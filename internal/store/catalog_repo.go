package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
)

type Availability struct {
	ModelID         string
	Enabled         bool
	DisabledUntilMS *int64
	DisabledReason  string
	ExcludedUntilMS *int64
	ManualExcluded  bool
	ErrorCount      int64
	LastErrorKind   string
}

func (s *Store) UpsertCatalog(ctx context.Context, models []catalog.Model) error {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin catalog upsert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := s.NowMS()
	ids := make([]string, 0, len(models))
	for _, model := range models {
		ids = append(ids, model.ID)
		var costIn, costOut any
		if model.CostInPerMTok != nil {
			costIn = *model.CostInPerMTok
		}
		if model.CostOutPerMTok != nil {
			costOut = *model.CostOutPerMTok
		}
		enabled := 0
		if model.Enabled {
			enabled = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO model (
			id, context_tokens, max_output_tokens, cost_in_per_mtok, cost_out_per_mtok,
			quality_tier, retention, quota_group, strengths, limits_text, enabled,
			created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			context_tokens = excluded.context_tokens,
			max_output_tokens = excluded.max_output_tokens,
			cost_in_per_mtok = excluded.cost_in_per_mtok,
			cost_out_per_mtok = excluded.cost_out_per_mtok,
			quality_tier = excluded.quality_tier,
			retention = excluded.retention,
			quota_group = excluded.quota_group,
			strengths = excluded.strengths,
			limits_text = excluded.limits_text,
			enabled = excluded.enabled,
			updated_at_ms = excluded.updated_at_ms`,
			model.ID, model.ContextTokens, model.MaxOutput, costIn, costOut,
			model.QualityTier, nullString(model.Retention), nullString(model.Quota.Group),
			model.Strengths, model.Limits, enabled, now, now); err != nil {
			return fmt.Errorf("upsert model %s: %w", model.ID, err)
		}
		for _, table := range []string{"model_role", "model_effort", "model_modality", "model_feature"} {
			//nolint:gosec // table is picked from a fixed internal list.
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE model_id = ?", model.ID); err != nil {
				return fmt.Errorf("reset %s for %s: %w", table, model.ID, err)
			}
		}
		for _, role := range model.Roles {
			if _, err := tx.ExecContext(ctx, `INSERT INTO model_role (model_id, role) VALUES (?, ?)`, model.ID, role); err != nil {
				return fmt.Errorf("insert role %s for %s: %w", role, model.ID, err)
			}
		}
		for _, effort := range model.Effort {
			if _, err := tx.ExecContext(ctx, `INSERT INTO model_effort (model_id, effort) VALUES (?, ?)`, model.ID, effort); err != nil {
				return fmt.Errorf("insert effort %s for %s: %w", effort, model.ID, err)
			}
		}
		for _, modality := range model.Modalities {
			if _, err := tx.ExecContext(ctx, `INSERT INTO model_modality (model_id, modality) VALUES (?, ?)`, model.ID, modality); err != nil {
				return fmt.Errorf("insert modality %s for %s: %w", modality, model.ID, err)
			}
		}
		for _, feature := range model.Features {
			if _, err := tx.ExecContext(ctx, `INSERT INTO model_feature (model_id, feature) VALUES (?, ?)`, model.ID, feature); err != nil {
				return fmt.Errorf("insert feature %s for %s: %w", feature, model.ID, err)
			}
		}
		for _, group := range model.Groups {
			source := model.GroupSources[group]
			if source == "" {
				source = "profile"
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO model_group (model_id, group_name, source_kind, active)
				VALUES (?, ?, ?, 1) ON CONFLICT(model_id, group_name) DO NOTHING`, model.ID, group, source); err != nil {
				return fmt.Errorf("insert group %s for %s: %w", group, model.ID, err)
			}
		}
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+1)
	args = append(args, now)
	for _, id := range ids {
		args = append(args, id)
	}
	if _, err := tx.ExecContext(ctx,
		//nolint:gosec // placeholders contains only repeated '?' marks.
		"UPDATE model SET enabled = 0, updated_at_ms = ? WHERE id NOT IN ("+placeholders+")", args...); err != nil {
		return fmt.Errorf("disable removed models: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit catalog upsert: %w", err)
	}
	return nil
}

func (s *Store) Availability(ctx context.Context) (map[string]Availability, error) {
	rows, err := s.ro.QueryContext(ctx, `SELECT m.id,
		COALESCE(rs.enabled_override, m.enabled),
		rs.disabled_until_ms, rs.disabled_reason,
		rot.excluded_until_ms, COALESCE(rot.manual_excluded, 0), COALESCE(rot.error_count, 0), rot.last_error_kind
		FROM model m
		LEFT JOIN model_runtime_state rs ON rs.model_id = m.id
		LEFT JOIN model_rotation_state rot ON rot.model_id = m.id`)
	if err != nil {
		return nil, fmt.Errorf("read availability: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]Availability)
	for rows.Next() {
		var item Availability
		var enabled int
		var disabledUntil, excludedUntil sql.NullInt64
		var reason, lastError sql.NullString
		var manual int
		if err := rows.Scan(&item.ModelID, &enabled, &disabledUntil, &reason, &excludedUntil, &manual, &item.ErrorCount, &lastError); err != nil {
			return nil, fmt.Errorf("scan availability: %w", err)
		}
		item.Enabled = enabled == 1
		if disabledUntil.Valid {
			value := disabledUntil.Int64
			item.DisabledUntilMS = &value
		}
		if excludedUntil.Valid {
			value := excludedUntil.Int64
			item.ExcludedUntilMS = &value
		}
		item.DisabledReason = reason.String
		item.ManualExcluded = manual == 1
		item.LastErrorKind = lastError.String
		out[item.ModelID] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate availability: %w", err)
	}
	return out, nil
}

func (s *Store) RecordModelSuccess(ctx context.Context, modelID string) error {
	return s.exec(ctx, `INSERT INTO model_rotation_state
		(model_id, excluded_until_ms, manual_excluded, error_count, last_error_kind, updated_at_ms)
		VALUES (?, NULL, 0, 0, NULL, ?)
		ON CONFLICT(model_id) DO UPDATE SET
			excluded_until_ms = NULL,
			error_count = 0,
			last_error_kind = NULL,
			updated_at_ms = excluded.updated_at_ms`, modelID, s.NowMS())
}

func (s *Store) RecordModelError(ctx context.Context, modelID, kind string, threshold, cooldownMS int64) (int64, error) {
	now := s.NowMS()
	var until int64
	err := s.rw.QueryRowContext(ctx, `INSERT INTO model_rotation_state
		(model_id, excluded_until_ms, manual_excluded, error_count, last_error_kind, updated_at_ms)
		VALUES (?, NULL, 0, 1, ?, ?)
		ON CONFLICT(model_id) DO UPDATE SET
			error_count = model_rotation_state.error_count + 1,
			last_error_kind = excluded.last_error_kind,
			updated_at_ms = excluded.updated_at_ms
		RETURNING error_count`, modelID, kind, now).Scan(&until)
	if err != nil {
		return 0, fmt.Errorf("record model error %s: %w", modelID, err)
	}
	if until >= threshold {
		until = now + cooldownMS
		if err := s.exec(ctx, `UPDATE model_rotation_state SET excluded_until_ms = ?, updated_at_ms = ? WHERE model_id = ?`,
			until, now, modelID); err != nil {
			return 0, err
		}
		return until, nil
	}
	return 0, nil
}

func (s *Store) SetManualExcluded(ctx context.Context, modelID string, excluded bool) error {
	value := 0
	if excluded {
		value = 1
	}
	return s.exec(ctx, `INSERT INTO model_rotation_state
		(model_id, excluded_until_ms, manual_excluded, error_count, last_error_kind, updated_at_ms)
		VALUES (?, NULL, ?, 0, NULL, ?)
		ON CONFLICT(model_id) DO UPDATE SET manual_excluded = excluded.manual_excluded, updated_at_ms = excluded.updated_at_ms`,
		modelID, value, s.NowMS())
}

func (s *Store) DisableModel(ctx context.Context, modelID, reason string, untilMS int64) error {
	return s.exec(ctx, `INSERT INTO model_runtime_state
		(model_id, enabled_override, disabled_until_ms, disabled_reason, updated_at_ms)
		VALUES (?, NULL, ?, ?, ?)
		ON CONFLICT(model_id) DO UPDATE SET
			disabled_until_ms = excluded.disabled_until_ms,
			disabled_reason = excluded.disabled_reason,
			updated_at_ms = excluded.updated_at_ms`, modelID, untilMS, reason, s.NowMS())
}

func (s *Store) SetEnabledOverride(ctx context.Context, modelID string, enabled bool) error {
	return s.exec(ctx, `INSERT INTO model_runtime_state (model_id, enabled_override, updated_at_ms)
		VALUES (?, ?, ?)
		ON CONFLICT(model_id) DO UPDATE SET
			enabled_override = excluded.enabled_override,
			updated_at_ms = excluded.updated_at_ms`, modelID, boolInt(enabled), s.NowMS())
}

func (s *Store) GroupMembers(ctx context.Context, group string) ([]string, error) {
	rows, err := s.ro.QueryContext(ctx,
		`SELECT model_id FROM model_group WHERE group_name = ? AND active = 1 ORDER BY model_id`, group)
	if err != nil {
		return nil, fmt.Errorf("read group %s: %w", group, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan group %s: %w", group, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group %s: %w", group, err)
	}
	return ids, nil
}

func (s *Store) AddGroup(ctx context.Context, group, modelID string) error {
	return s.exec(ctx, `INSERT INTO model_group (model_id, group_name, source_kind, active)
		VALUES (?, ?, 'command', 1)
		ON CONFLICT(model_id, group_name) DO UPDATE SET source_kind = 'command', active = 1`, modelID, group)
}

func (s *Store) RemoveGroup(ctx context.Context, group, modelID string) error {
	return s.exec(ctx, `INSERT INTO model_group (model_id, group_name, source_kind, active)
		VALUES (?, ?, 'command', 0)
		ON CONFLICT(model_id, group_name) DO UPDATE SET source_kind = 'command', active = 0`, modelID, group)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var errNotFound = errors.New("not found")

var ErrNotFound = errNotFound
