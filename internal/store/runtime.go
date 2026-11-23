package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type Setting struct {
	Key        string
	Value      string
	SourceKind string
	UpdatedMS  int64
}

func (s *Store) Setting(ctx context.Context, key string) (*Setting, error) {
	row := s.queryRow(ctx, `SELECT key, value, source_kind, updated_at_ms FROM runtime_setting WHERE key = ?`, key)
	var item Setting
	if err := row.Scan(&item.Key, &item.Value, &item.SourceKind, &item.UpdatedMS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("read runtime setting %s: %w", key, err)
	}
	return &item, nil
}

func (s *Store) SetSetting(ctx context.Context, key, value, source string) error {
	return s.exec(ctx, `INSERT INTO runtime_setting (key, value, source_kind, updated_at_ms)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, source_kind = excluded.source_kind,
			updated_at_ms = excluded.updated_at_ms`, key, value, source, s.NowMS())
}

type JournalEvent struct {
	DialogueID string
	Level      string
	Kind       string
	Role       string
	RequestID  string
	Note       string
}

func (s *Store) Journal(ctx context.Context, event JournalEvent) error {
	return s.exec(ctx, `INSERT INTO journal_event
		(id, dialogue_id, level, kind, role, request_id, note, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		NewID(), nullString(event.DialogueID), event.Level, event.Kind, nullString(event.Role),
		nullString(event.RequestID), event.Note, s.NowMS())
}

type ModelCall struct {
	DialogueID       string
	DelegationID     string
	Role             string
	ModelID          string
	Purpose          string
	RequestID        string
	InputTokens      *int64
	CachedTokens     *int64
	OutputTokens     *int64
	ReasoningTokens  *int64
	DurationMS       int64
	HTTPStatus       *int
	CostUSD          *float64
	ObjectiveMode    string
	ReasoningMode    string
	ReasoningEffort  string
	InGroupExpert    *bool
	InGroupAssistant *bool
	PipelineMode     string
}

func (s *Store) RecordModelCall(ctx context.Context, call ModelCall) error {
	return s.exec(ctx, `INSERT INTO model_call
		(id, dialogue_id, delegation_id, role, model_id, purpose, request_id,
		 input_tokens, cached_tokens, output_tokens, reasoning_tokens, duration_ms, http_status, cost_usd,
		 objective_mode, reasoning_mode, reasoning_effort, in_group_expert, in_group_assistant, pipeline_mode, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		NewID(), nullString(call.DialogueID), nullString(call.DelegationID), call.Role, call.ModelID,
		nullString(call.Purpose), call.RequestID, call.InputTokens, call.CachedTokens, call.OutputTokens,
		call.ReasoningTokens, call.DurationMS, call.HTTPStatus, call.CostUSD,
		nullString(call.ObjectiveMode), nullString(call.ReasoningMode), nullString(call.ReasoningEffort),
		nullBool(call.InGroupExpert), nullBool(call.InGroupAssistant), nullString(call.PipelineMode), s.NowMS())
}

type Delegation struct {
	ID               string
	DialogueID       string
	Seq              int
	Task             string
	ContextText      string
	Scope            string
	AssistantModelID string
	BudgetSteps      int64
	BudgetChars      int64
	MaxTokens        *int64
	Status           string
	CloseReason      string
	StartedMS        int64
	FinishedMS       int64
}

func (s *Store) CreateDelegation(ctx context.Context, item Delegation) (*Delegation, error) {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin delegation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var seq int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), -1) FROM delegation WHERE dialogue_id = ?`, item.DialogueID).Scan(&seq); err != nil {
		return nil, fmt.Errorf("read delegation seq: %w", err)
	}
	item.ID = NewID()
	item.Seq = seq + 1
	item.Status = "expert_to_assistant_2"
	item.StartedMS = s.NowMS()
	if _, err := tx.ExecContext(ctx, `INSERT INTO delegation
		(id, dialogue_id, seq, task, context_text, scope, assistant_model_id, budget_steps, budget_chars,
		 status, close_reason, started_at_ms, finished_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)`,
		item.ID, item.DialogueID, item.Seq, item.Task, nullString(item.ContextText), item.Scope,
		item.AssistantModelID, item.BudgetSteps, item.BudgetChars, item.Status, item.StartedMS); err != nil {
		return nil, fmt.Errorf("insert delegation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit delegation: %w", err)
	}
	return &item, nil
}

func (s *Store) FinishDelegation(ctx context.Context, id, closeReason string) error {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finish delegation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE delegation
		SET status = 'assistant_2_to_expert', close_reason = ?, finished_at_ms = ? WHERE id = ?`,
		nullString(closeReason), s.NowMS(), id); err != nil {
		return fmt.Errorf("update delegation %s: %w", id, err)
	}
	// Steps the client never answered stay issued; the delegation close reason
	// records why the material is partial.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finish delegation %s: %w", id, err)
	}
	return nil
}

// SetDelegationStatus moves the delegation to an intermediate status without
// closing it.
func (s *Store) SetDelegationStatus(ctx context.Context, id, status string) error {
	if err := s.exec(ctx, `UPDATE delegation SET status = ? WHERE id = ?`, status, id); err != nil {
		return fmt.Errorf("set delegation status %q for %s: %w", status, id, err)
	}
	return nil
}

func (s *Store) SetDelegationAssistant(ctx context.Context, id, modelID string) error {
	if err := s.exec(ctx, `UPDATE delegation SET assistant_model_id = ? WHERE id = ?`, modelID, id); err != nil {
		return fmt.Errorf("set delegation assistant %s for %s: %w", modelID, id, err)
	}
	return nil
}

func (s *Store) ActiveDelegation(ctx context.Context, dialogueID string) (*Delegation, error) {
	row := s.queryRow(ctx, `SELECT id, dialogue_id, seq, task, COALESCE(context_text, ''), scope,
		assistant_model_id, budget_steps, budget_chars, status, COALESCE(close_reason, ''),
		COALESCE(started_at_ms, 0), COALESCE(finished_at_ms, 0)
		FROM delegation WHERE dialogue_id = ?
		  AND status IN ('expert_to_assistant_2','assistant_2_to_client_tools','client_tools_to_assistant_2')
		ORDER BY seq DESC LIMIT 1`, dialogueID)
	var item Delegation
	if err := row.Scan(&item.ID, &item.DialogueID, &item.Seq, &item.Task, &item.ContextText, &item.Scope,
		&item.AssistantModelID, &item.BudgetSteps, &item.BudgetChars, &item.Status, &item.CloseReason,
		&item.StartedMS, &item.FinishedMS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("read active delegation: %w", err)
	}
	return &item, nil
}

func (s *Store) DelegationSteps(ctx context.Context, delegationID string) (int64, error) {
	var count int64
	if err := s.queryRow(ctx, `SELECT COUNT(*) FROM delegation_step WHERE delegation_id = ?`, delegationID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count delegation steps: %w", err)
	}
	return count, nil
}

func (s *Store) AddDelegationStep(ctx context.Context, delegationID, callID, toolName, arguments, status string) error {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delegation step: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var seq int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), -1) FROM delegation_step WHERE delegation_id = ?`, delegationID).Scan(&seq); err != nil {
		return fmt.Errorf("read delegation step seq: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO delegation_step
		(id, delegation_id, seq, call_id, tool_name, arguments_raw, status, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		NewID(), delegationID, seq+1, nullString(callID), nullString(toolName), nullString(arguments),
		status, s.NowMS()); err != nil {
		return fmt.Errorf("insert delegation step: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delegation step: %w", err)
	}
	return nil
}

// CompleteDelegationSteps closes the steps whose tool results the client sent
// back. Steps the client did not answer stay issued until the delegation ends.
func (s *Store) CompleteDelegationSteps(ctx context.Context, delegationID string, callIDs []string) error {
	if len(callIDs) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(callIDs)), ",")
	args := make([]any, 0, len(callIDs)+1)
	args = append(args, delegationID)
	for _, callID := range callIDs {
		args = append(args, callID)
	}
	//nolint:gosec // placeholders contains only repeated '?' marks.
	if _, err := s.rw.ExecContext(ctx, `UPDATE delegation_step SET status = 'client_tools_to_assistant_2'
		WHERE delegation_id = ? AND status = 'assistant_2_to_client_tools' AND call_id IN (`+placeholders+`)`, args...); err != nil {
		return fmt.Errorf("complete delegation steps %s: %w", delegationID, err)
	}
	return nil
}

func (s *Store) PinExpert(ctx context.Context, dialogueID, role, modelID, reason string) error {
	return s.exec(ctx, `INSERT INTO pin_event (id, dialogue_id, role, model_id, reason, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?)`, NewID(), dialogueID, role, modelID, reason, s.NowMS())
}

// Deflated reads a cached deflator result for a tool payload.
func (s *Store) Deflated(ctx context.Context, hash string) (string, bool, error) {
	var text string
	err := s.queryRow(ctx, `SELECT text FROM deflate_cache WHERE hash = ?`, hash).Scan(&text)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read deflate cache %s: %w", hash, err)
	}
	return text, true, nil
}

func (s *Store) SaveDeflated(ctx context.Context, hash, text string) error {
	return s.exec(ctx, `INSERT INTO deflate_cache (hash, text, created_at_ms) VALUES (?, ?, ?)
		ON CONFLICT(hash) DO UPDATE SET text = excluded.text, created_at_ms = excluded.created_at_ms`,
		hash, text, s.NowMS())
}

// Compaction is the cached summary of the covered dialogue prefix.
type Compaction struct {
	BorderHash        string
	CoveredCount      int64
	Summary           string
	ReasoningIncluded bool
}

func (s *Store) Compaction(ctx context.Context, dialogueID string) (Compaction, bool, error) {
	var item Compaction
	var reasoningIncluded int
	err := s.queryRow(ctx, `SELECT border_hash, covered_count, summary, reasoning_included
		FROM dialogue_compaction WHERE dialogue_id = ?`, dialogueID).
		Scan(&item.BorderHash, &item.CoveredCount, &item.Summary, &reasoningIncluded)
	if errors.Is(err, sql.ErrNoRows) {
		return Compaction{}, false, nil
	}
	if err != nil {
		return Compaction{}, false, fmt.Errorf("read compaction %s: %w", dialogueID, err)
	}
	item.ReasoningIncluded = reasoningIncluded == 1
	return item, true, nil
}

func (s *Store) SaveCompaction(
	ctx context.Context, dialogueID, borderHash string, covered int64, summary string, reasoningIncluded bool,
) error {
	now := s.NowMS()
	return s.exec(ctx, `INSERT INTO dialogue_compaction
		(dialogue_id, border_hash, covered_count, summary, reasoning_included, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(dialogue_id) DO UPDATE SET
			border_hash = excluded.border_hash,
			covered_count = excluded.covered_count,
			summary = excluded.summary,
			reasoning_included = excluded.reasoning_included,
			updated_at_ms = excluded.updated_at_ms`,
		dialogueID, borderHash, covered, summary, boolInt(reasoningIncluded), now, now)
}

func (s *Store) CompactionFailed(ctx context.Context, dialogueID string) (bool, error) {
	var exists int
	err := s.queryRow(ctx, `SELECT 1 FROM dialogue_compaction_failure WHERE dialogue_id = ?`,
		dialogueID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read compaction failure %s: %w", dialogueID, err)
	}
	return true, nil
}

func (s *Store) MarkCompactionFailure(ctx context.Context, dialogueID, borderHash string) error {
	return s.exec(ctx, `INSERT INTO dialogue_compaction_failure (dialogue_id, border_hash, failed_at_ms)
		VALUES (?, ?, ?) ON CONFLICT(dialogue_id) DO NOTHING`,
		dialogueID, borderHash, s.NowMS())
}

func (s *Store) GroupUsage(ctx context.Context, dialogueID string, turns int64) (int64, int64, error) {
	rows, err := s.ro.QueryContext(ctx, `SELECT in_group_expert, in_group_assistant FROM model_call
		WHERE dialogue_id = ? ORDER BY created_at_ms DESC LIMIT ?`, dialogueID, turns)
	if err != nil {
		return 0, 0, fmt.Errorf("read group usage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var expert, assistant int64
	for rows.Next() {
		var inExpert, inAssistant sql.NullInt64
		if err := rows.Scan(&inExpert, &inAssistant); err != nil {
			return 0, 0, fmt.Errorf("scan group usage: %w", err)
		}
		if inExpert.Valid && inExpert.Int64 == 1 {
			expert++
		}
		if inAssistant.Valid && inAssistant.Int64 == 1 {
			assistant++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate group usage: %w", err)
	}
	return expert, assistant, nil
}

func nullBool(value *bool) any {
	if value == nil {
		return nil
	}
	if *value {
		return 1
	}
	return 0
}
