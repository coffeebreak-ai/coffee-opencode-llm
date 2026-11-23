package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrNoQuota = errors.New("no quota")

// Quota is the plain limit set of one owner. The ledger maps its own domain
// type into these fields; the store stays domain-agnostic.
type Quota struct {
	Group string
	RPM   *int64
	Day   *int64
	Month *int64
}

type WindowKey struct {
	OwnerKind string
	OwnerID   string
	Kind      string
	StartMS   int64
	Limit     *int64
}

type Reservation struct {
	RequestID string
	ModelID   string
	Group     string
	Windows   []WindowKey
}

func WindowStart(kind string, now time.Time) int64 {
	now = now.UTC()
	switch kind {
	case "rpm":
		return now.UnixMilli() - now.UnixMilli()%60000
	case "day":
		return now.UnixMilli() - now.UnixMilli()%86400000
	case "month":
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	default:
		return now.UnixMilli()
	}
}

func (s *Store) Reserve(ctx context.Context, requestID, modelID string, quota Quota) (*Reservation, error) {
	now := s.now().UTC()
	windows := buildWindows(modelID, quota, now)
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin quota reserve: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	nowMS := now.UnixMilli()
	for _, window := range windows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO quota_window
			(owner_kind, owner_id, window_kind, window_start_ms, limit_value, used_value, reserved_value, updated_at_ms)
			VALUES (?, ?, ?, ?, ?, 0, 0, ?) ON CONFLICT DO NOTHING`,
			window.OwnerKind, window.OwnerID, window.Kind, window.StartMS, window.Limit, nowMS); err != nil {
			return nil, fmt.Errorf("ensure quota window %s/%s: %w", window.OwnerID, window.Kind, err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE quota_window
			SET reserved_value = reserved_value + 1, updated_at_ms = ?
			WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ?
			  AND (limit_value IS NULL OR used_value + reserved_value + 1 <= limit_value)`,
			nowMS, window.OwnerKind, window.OwnerID, window.Kind, window.StartMS)
		if err != nil {
			return nil, fmt.Errorf("reserve quota window %s/%s: %w", window.OwnerID, window.Kind, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("check quota reserve: %w", err)
		}
		if affected == 0 {
			return nil, fmt.Errorf("model %s window %s: %w", modelID, window.Kind, ErrNoQuota)
		}
		if err := insertQuotaEvent(ctx, tx, requestID, modelID, window, "reserved", 1, nil, 0, nowMS); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit quota reserve: %w", err)
	}
	return &Reservation{RequestID: requestID, ModelID: modelID, Group: quota.Group, Windows: windows}, nil
}

func buildWindows(modelID string, quota Quota, now time.Time) []WindowKey {
	var out []WindowKey
	add := func(kind string, limit *int64) {
		out = append(out, WindowKey{OwnerKind: "model", OwnerID: modelID, Kind: kind, StartMS: WindowStart(kind, now), Limit: limit})
	}
	add("rpm", quota.RPM)
	add("day", quota.Day)
	add("month", quota.Month)
	if quota.Group != "" {
		group := func(kind string, limit *int64) {
			out = append(out, WindowKey{OwnerKind: "group", OwnerID: quota.Group, Kind: kind, StartMS: WindowStart(kind, now), Limit: limit})
		}
		group("rpm", quota.RPM)
		group("day", quota.Day)
		group("month", quota.Month)
	}
	return out
}

func (s *Store) Commit(ctx context.Context, reservation Reservation) error {
	return s.finishReservation(ctx, reservation, true)
}

func (s *Store) Release(ctx context.Context, reservation Reservation) error {
	return s.finishReservation(ctx, reservation, false)
}

func (s *Store) finishReservation(ctx context.Context, reservation Reservation, commit bool) error {
	if len(reservation.Windows) == 0 {
		return nil
	}
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin quota finish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.NowMS()
	for _, window := range reservation.Windows {
		var result sql.Result
		if commit {
			result, err = tx.ExecContext(ctx, `UPDATE quota_window
				SET reserved_value = reserved_value - 1, used_value = used_value + 1, updated_at_ms = ?
				WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ? AND reserved_value >= 1`,
				now, window.OwnerKind, window.OwnerID, window.Kind, window.StartMS)
		} else {
			result, err = tx.ExecContext(ctx, `UPDATE quota_window
				SET reserved_value = reserved_value - 1, updated_at_ms = ?
				WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ? AND reserved_value >= 1`,
				now, window.OwnerKind, window.OwnerID, window.Kind, window.StartMS)
		}
		if err != nil {
			return fmt.Errorf("finish quota window %s/%s: %w", window.OwnerID, window.Kind, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("check quota finish: %w", err)
		}
		if affected == 0 {
			continue
		}
		kind := "released"
		if commit {
			kind = "committed"
		}
		if err := insertQuotaEvent(ctx, tx, reservation.RequestID, reservation.ModelID, window, kind, 1, nil, 0, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit quota finish: %w", err)
	}
	return nil
}

func insertQuotaEvent(ctx context.Context, tx *sql.Tx, _ string, modelID string, window WindowKey, kind string, amount int64,
	observed *int64, httpStatus int, nowMS int64) error {
	var observedValue any
	if observed != nil {
		observedValue = *observed
	}
	var status any
	if httpStatus != 0 {
		status = httpStatus
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO quota_event
		(id, owner_kind, owner_id, model_id, window_kind, event_kind, amount, observed_limit, retry_after_ms, http_status, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
		NewID(), window.OwnerKind, window.OwnerID, modelID, window.Kind, kind, amount, observedValue, status, nowMS); err != nil {
		return fmt.Errorf("insert quota event %s: %w", kind, err)
	}
	return nil
}

func (s *Store) SetCooldown(ctx context.Context, modelID, group string, retryAfterMS int64) error {
	now := s.NowMS()
	owners := []struct{ kind, id string }{{"model", modelID}}
	if group != "" {
		owners = append(owners, struct{ kind, id string }{"group", group})
	}
	for _, owner := range owners {
		if _, err := s.rw.ExecContext(ctx, `INSERT INTO quota_event
			(id, owner_kind, owner_id, model_id, window_kind, event_kind, amount, observed_limit, retry_after_ms, http_status, created_at_ms)
			VALUES (?, ?, ?, ?, 'rpm', 'cooldown_set', NULL, NULL, ?, 429, ?)`,
			NewID(), owner.kind, owner.id, modelID, retryAfterMS, now); err != nil {
			return fmt.Errorf("set cooldown for %s/%s: %w", owner.kind, owner.id, err)
		}
	}
	return nil
}

func (s *Store) CoolingUntil(ctx context.Context, modelID, group string) (int64, error) {
	now := s.NowMS()
	owners := []struct{ kind, id string }{{"model", modelID}}
	if group != "" {
		owners = append(owners, struct{ kind, id string }{"group", group})
	}
	var maxUntil int64
	for _, owner := range owners {
		var until sql.NullInt64
		if err := s.queryRow(ctx, `SELECT MAX(created_at_ms + retry_after_ms) FROM quota_event
			WHERE owner_kind = ? AND owner_id = ? AND event_kind = 'cooldown_set'
			  AND retry_after_ms IS NOT NULL AND created_at_ms + retry_after_ms > ?`, owner.kind, owner.id, now).Scan(&until); err != nil {
			return 0, fmt.Errorf("read cooldown for %s/%s: %w", owner.kind, owner.id, err)
		}
		if until.Valid && until.Int64 > maxUntil {
			maxUntil = until.Int64
		}
	}
	return maxUntil, nil
}

func (s *Store) Remaining(ctx context.Context, modelID string, quota Quota) (int64, bool, error) {
	now := s.now().UTC()
	windows := buildWindows(modelID, quota, now)
	var remaining int64
	var set bool
	unlimited := true
	for _, window := range windows {
		if window.Limit == nil {
			continue
		}
		unlimited = false
		var used, reserved int64
		err := s.queryRow(ctx, `SELECT used_value, reserved_value FROM quota_window
			WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ?`,
			window.OwnerKind, window.OwnerID, window.Kind, window.StartMS).Scan(&used, &reserved)
		left := *window.Limit
		if err == nil {
			left = *window.Limit - used - reserved
			if left < 0 {
				left = 0
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("read quota window %s/%s: %w", window.OwnerID, window.Kind, err)
		}
		if !set || left < remaining {
			remaining = left
			set = true
		}
	}
	if !set {
		return 0, true, nil
	}
	return remaining, unlimited, nil
}

func (s *Store) LearnLimit(ctx context.Context, ownerKind, ownerID, windowKind string, observed int64) error {
	now := s.now().UTC()
	start := WindowStart(windowKind, now)
	if _, err := s.rw.ExecContext(ctx, `INSERT INTO quota_window
		(owner_kind, owner_id, window_kind, window_start_ms, limit_value, used_value, reserved_value, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, 0, 0, ?) ON CONFLICT DO NOTHING`,
		ownerKind, ownerID, windowKind, start, observed, now.UnixMilli()); err != nil {
		return fmt.Errorf("learn quota limit: %w", err)
	}
	if _, err := s.rw.ExecContext(ctx, `UPDATE quota_window SET limit_value = ?, updated_at_ms = ?
		WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ?
		  AND (limit_value IS NULL OR limit_value > ?)`,
		observed, now.UnixMilli(), ownerKind, ownerID, windowKind, start, observed); err != nil {
		return fmt.Errorf("update learned quota limit: %w", err)
	}
	return nil
}

func (s *Store) ReleaseStaleReservations(ctx context.Context, ttlMS int64) (int64, error) {
	now := s.NowMS()
	cutoff := now - ttlMS
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin stale reservation cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT owner_kind, owner_id, window_kind, window_start_ms, reserved_value
		FROM quota_window WHERE reserved_value > 0 AND updated_at_ms < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("read stale reservations: %w", err)
	}
	type staleWindow struct {
		ownerKind  string
		ownerID    string
		windowKind string
		startMS    int64
		amount     int64
	}
	var stale []staleWindow
	for rows.Next() {
		var item staleWindow
		if err := rows.Scan(&item.ownerKind, &item.ownerID, &item.windowKind, &item.startMS,
			&item.amount); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan stale reservation: %w", err)
		}
		stale = append(stale, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate stale reservations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale reservations: %w", err)
	}
	var total int64
	for _, item := range stale {
		if _, err := tx.ExecContext(ctx, `UPDATE quota_window SET reserved_value = 0, updated_at_ms = ?
			WHERE owner_kind = ? AND owner_id = ? AND window_kind = ? AND window_start_ms = ?`,
			now, item.ownerKind, item.ownerID, item.windowKind, item.startMS); err != nil {
			return 0, fmt.Errorf("release stale reservation %s/%s: %w", item.ownerID, item.windowKind, err)
		}
		window := WindowKey{OwnerKind: item.ownerKind, OwnerID: item.ownerID, Kind: item.windowKind,
			StartMS: item.startMS}
		// A group window has no model association in the schema, so its release
		// event carries an empty model identifier; the journal note keeps the total.
		modelID := ""
		if item.ownerKind == "model" {
			modelID = item.ownerID
		}
		if err := insertQuotaEvent(ctx, tx, "", modelID, window, "released", item.amount, nil, 0, now); err != nil {
			return 0, err
		}
		total += item.amount
	}
	if total > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_event
			(id, dialogue_id, level, kind, role, request_id, note, created_at_ms)
			VALUES (?, NULL, 'warn', 'quota_reservation_expired', NULL, NULL, ?, ?)`,
			NewID(), fmt.Sprintf("released %d stale reservations", total), now); err != nil {
			return 0, fmt.Errorf("journal stale reservation cleanup: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit stale reservation cleanup: %w", err)
	}
	return total, nil
}
