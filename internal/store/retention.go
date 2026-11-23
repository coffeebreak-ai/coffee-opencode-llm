package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type retentionSpec struct {
	name   string
	count  string
	remove string
}

var retentionSpecs = []retentionSpec{
	{
		name:   "quota_window",
		count:  `SELECT COUNT(*) FROM quota_window WHERE window_start_ms < ?`,
		remove: `DELETE FROM quota_window WHERE (owner_kind, owner_id, window_kind, window_start_ms) IN (SELECT owner_kind, owner_id, window_kind, window_start_ms FROM quota_window WHERE window_start_ms < ? LIMIT ?)`,
	},
	{
		name:   "quota_event",
		count:  `SELECT COUNT(*) FROM quota_event WHERE created_at_ms < ?`,
		remove: `DELETE FROM quota_event WHERE id IN (SELECT id FROM quota_event WHERE created_at_ms < ? LIMIT ?)`,
	},
	{
		name:   "model_call",
		count:  `SELECT COUNT(*) FROM model_call WHERE created_at_ms < ?`,
		remove: `DELETE FROM model_call WHERE id IN (SELECT id FROM model_call WHERE created_at_ms < ? LIMIT ?)`,
	},
	{
		name:   "journal_event",
		count:  `SELECT COUNT(*) FROM journal_event WHERE created_at_ms < ?`,
		remove: `DELETE FROM journal_event WHERE id IN (SELECT id FROM journal_event WHERE created_at_ms < ? LIMIT ?)`,
	},
	{
		name:   "dialogue",
		count:  `SELECT COUNT(*) FROM dialogue WHERE COALESCE(last_human_at_ms, updated_at_ms) < ?`,
		remove: `DELETE FROM dialogue WHERE id IN (SELECT id FROM dialogue WHERE COALESCE(last_human_at_ms, updated_at_ms) < ? LIMIT ?)`,
	},
}

// RetentionCounts reports how many rows each retention table would remove.
func (s *Store) RetentionCounts(ctx context.Context, cutoffMS int64) (map[string]int64, error) {
	out := make(map[string]int64, len(retentionSpecs))
	for _, spec := range retentionSpecs {
		var count int64
		if err := s.queryRow(ctx, spec.count, cutoffMS).Scan(&count); err != nil {
			return nil, fmt.Errorf("count retention rows in %s: %w", spec.name, err)
		}
		out[spec.name] = count
	}
	return out, nil
}

// RetentionDelete removes rows older than cutoffMS in batches, in the documented order.
func (s *Store) RetentionDelete(ctx context.Context, cutoffMS, batchSize int64) (map[string]int64, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("retention: batch size %d must be positive", batchSize)
	}
	out := make(map[string]int64, len(retentionSpecs))
	for _, spec := range retentionSpecs {
		var total int64
		for {
			result, err := s.rw.ExecContext(ctx, spec.remove, cutoffMS, batchSize)
			if err != nil {
				return out, fmt.Errorf("remove retention batch from %s: %w", spec.name, err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return out, fmt.Errorf("count deleted rows in %s: %w", spec.name, err)
			}
			total += affected
			if affected == 0 {
				break
			}
		}
		out[spec.name] = total
	}
	return out, nil
}

// Vacuum rewrites the SQLite file outside any transaction.
func (s *Store) Vacuum(ctx context.Context) error {
	if _, err := s.rw.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("vacuum database: %w", err)
	}
	return nil
}

// GroupSource is one rotation label with its origin and current state.
type GroupSource struct {
	Source string
	Active bool
}

// GroupSources reports every stored group row, including labels removed by a command.
func (s *Store) GroupSources(ctx context.Context) (map[string]map[string]GroupSource, error) {
	rows, err := s.ro.QueryContext(ctx,
		`SELECT model_id, group_name, source_kind, active FROM model_group ORDER BY model_id, group_name`)
	if err != nil {
		return nil, fmt.Errorf("read group sources: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]map[string]GroupSource)
	for rows.Next() {
		var modelID, group, source string
		var active int
		if err := rows.Scan(&modelID, &group, &source, &active); err != nil {
			return nil, fmt.Errorf("scan group source: %w", err)
		}
		if out[modelID] == nil {
			out[modelID] = make(map[string]GroupSource)
		}
		out[modelID][group] = GroupSource{Source: source, Active: active == 1}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group sources: %w", err)
	}
	return out, nil
}

func FormatRetentionCounts(counts map[string]int64) string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", name, counts[name]))
	}
	return strings.Join(parts, " ")
}
