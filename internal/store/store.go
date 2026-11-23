package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // register the pure-Go SQLite driver

	"github.com/coffeebreak-ai/coffee-opencode-llm/migrations"
)

type Options struct {
	BusyTimeoutMS int64
	MaxOpenConns  int
	MaxIdleConns  int
	WriteConns    int
}

type Store struct {
	rw  *sql.DB
	ro  *sql.DB
	now func() time.Time
}

func Open(ctx context.Context, path string, opts Options) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)",
		path, opts.BusyTimeoutMS)
	rw, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open write database: %w", err)
	}
	rw.SetMaxOpenConns(opts.WriteConns)
	rw.SetMaxIdleConns(opts.WriteConns)
	if err := rw.PingContext(ctx); err != nil {
		_ = rw.Close()
		return nil, fmt.Errorf("ping write database: %w", err)
	}
	ro, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = rw.Close()
		return nil, fmt.Errorf("open read database: %w", err)
	}
	ro.SetMaxOpenConns(opts.MaxOpenConns)
	ro.SetMaxIdleConns(opts.MaxIdleConns)
	if err := ro.PingContext(ctx); err != nil {
		_ = rw.Close()
		_ = ro.Close()
		return nil, fmt.Errorf("ping read database: %w", err)
	}
	return &Store{rw: rw, ro: ro, now: time.Now}, nil
}

func (s *Store) Close() error {
	return errors.Join(s.ro.Close(), s.rw.Close())
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.rw.PingContext(ctx); err != nil {
		return fmt.Errorf("ping write database: %w", err)
	}
	if err := s.ro.PingContext(ctx); err != nil {
		return fmt.Errorf("ping read database: %w", err)
	}
	return nil
}

func (s *Store) NowMS() int64 {
	return s.now().UTC().UnixMilli()
}

func NewID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("generate identifier: %v", err))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

var migrationName = regexp.MustCompile(`^(\d{4})_(.+)\.(sqlite|postgres)\.sql$`)

type migrationFile struct {
	version  int
	name     string
	content  string
	checksum string
}

func (s *Store) Migrate(ctx context.Context, dialect string) error {
	files, err := loadMigrations(dialect)
	if err != nil {
		return err
	}
	if _, err := s.rw.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migration (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at_ms BIGINT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migration: %w", err)
	}
	applied := make(map[int]string)
	rows, err := s.rw.QueryContext(ctx, `SELECT version, checksum FROM schema_migration`)
	if err != nil {
		return fmt.Errorf("read schema_migration: %w", err)
	}
	for rows.Next() {
		var version int
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan schema_migration: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate schema_migration: %w", err)
	}
	_ = rows.Close()
	for _, file := range files {
		if checksum, ok := applied[file.version]; ok {
			if checksum != file.checksum {
				return fmt.Errorf("migration %04d_%s was modified after it was applied", file.version, file.name)
			}
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(file.content), "-- no-transaction") {
			// A table rebuild needs foreign_keys off, which SQLite ignores inside a
			// transaction. Such a migration is applied statement by statement.
			if _, err := s.rw.ExecContext(ctx, file.content); err != nil {
				return fmt.Errorf("apply migration %04d_%s: %w", file.version, file.name, err)
			}
			if _, err := s.rw.ExecContext(ctx,
				`INSERT INTO schema_migration (version, name, checksum, applied_at_ms) VALUES (?, ?, ?, ?)`,
				file.version, file.name, file.checksum, s.NowMS()); err != nil {
				return fmt.Errorf("record migration %04d_%s: %w", file.version, file.name, err)
			}
			continue
		}
		tx, err := s.rw.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %04d_%s: %w", file.version, file.name, err)
		}
		if _, err := tx.ExecContext(ctx, file.content); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %04d_%s: %w", file.version, file.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migration (version, name, checksum, applied_at_ms) VALUES (?, ?, ?, ?)`,
			file.version, file.name, file.checksum, s.NowMS()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %04d_%s: %w", file.version, file.name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %04d_%s: %w", file.version, file.name, err)
		}
	}
	return nil
}

func loadMigrations(dialect string) ([]migrationFile, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var files []migrationFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := migrationName.FindStringSubmatch(entry.Name())
		if len(match) < 4 || match[3] != dialect {
			continue
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse migration version %s: %w", entry.Name(), err)
		}
		raw, err := migrations.FS.ReadFile(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(raw)
		files = append(files, migrationFile{
			version:  version,
			name:     match[2],
			content:  string(raw),
			checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	for i := 1; i < len(files); i++ {
		if files[i].version == files[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %04d", files[i].version)
		}
	}
	return files, nil
}

func (s *Store) exec(ctx context.Context, query string, args ...any) error {
	if _, err := s.rw.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("execute %q: %w", compact(query), err)
	}
	return nil
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.ro.QueryRowContext(ctx, query, args...)
}

func compact(query string) string {
	fields := strings.Fields(query)
	if len(fields) > 12 {
		fields = fields[:12]
	}
	return strings.Join(fields, " ")
}
