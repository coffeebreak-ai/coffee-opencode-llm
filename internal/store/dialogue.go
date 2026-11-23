package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Dialogue struct {
	ID               string
	ScopeKey         string
	ScopeSource      string
	Level            int
	Owner            string
	Phase            string
	RequestID        string
	TurnSteps        int64
	ExpertModelID    string
	AssistantModelID string
	StateVersion     int
	Closed           bool
	CreatedAtMS      int64
	UpdatedAtMS      int64
	LastHumanAtMS    int64
}

type Part struct {
	Seq      int
	Type     string
	Text     string
	URI      string
	MimeType string
	ByteSize *int64
}

type Message struct {
	ID               string
	DialogueID       string
	Seq              int
	Role             string
	Kind             string
	Content          string
	ReasoningContent string
	ReasoningGroupID string
	CallID           string
	ToolName         string
	Truncated        bool
	CreatedAtMS      int64
	Parts            []Part
}

type Delivery struct {
	ID           string
	DialogueID   string
	Fingerprint  string
	Endpoint     string
	ResponseBody string
	CreatedAtMS  int64
}

// EnsureDialogue returns the dialogue bound to the session key. A dialogue is
// never regenerated: the session keeps one row for its whole life. A dialogue
// idle longer than the configured TTL is closed logically without touching its
// phase; the next request reopens the same row with its history and pins intact.
func (s *Store) EnsureDialogue(ctx context.Context, scopeKey, scopeSource string, idleTTLHours int64) (*Dialogue, error) {
	now := s.NowMS()
	dialogue, err := s.dialogueByScope(ctx, scopeKey)
	switch {
	case errors.Is(err, ErrNotFound):
		return s.createDialogue(ctx, scopeKey, scopeSource, now)
	case err != nil:
		return nil, err
	}
	idleMS := idleTTLHours * time.Hour.Milliseconds()
	lastSeen := dialogue.UpdatedAtMS
	if dialogue.LastHumanAtMS > 0 {
		lastSeen = dialogue.LastHumanAtMS
	}
	if now-lastSeen > idleMS && freePhase(dialogue.Phase) {
		if err := s.exec(ctx, `UPDATE dialogue
			SET closed = 1, updated_at_ms = ?
			WHERE id = ?`, now, dialogue.ID); err != nil {
			return nil, fmt.Errorf("close idle dialogue %s: %w", dialogue.ID, err)
		}
		dialogue.Closed = true
		dialogue.UpdatedAtMS = now
	}
	if dialogue.Closed {
		if err := s.exec(ctx, `UPDATE dialogue
			SET closed = 0, updated_at_ms = ?
			WHERE id = ?`, now, dialogue.ID); err != nil {
			return nil, fmt.Errorf("reopen dialogue %s: %w", dialogue.ID, err)
		}
		dialogue.Closed = false
		dialogue.UpdatedAtMS = now
	}
	return dialogue, nil
}

// freePhase reports whether the last status ended the request, so the dialogue
// is idle and may be closed by the TTL.
func freePhase(phase string) bool {
	return phase == "" || strings.HasSuffix(phase, "_to_user") || strings.HasSuffix(phase, "_stop")
}

func (s *Store) dialogueByScope(ctx context.Context, scopeKey string) (*Dialogue, error) {
	row := s.queryRow(ctx, `SELECT id, scope_key, scope_source, level, owner, phase,
		COALESCE(request_id, ''), COALESCE(expert_model_id, ''), COALESCE(assistant_model_id, ''),
		state_version, closed, created_at_ms, updated_at_ms, COALESCE(last_human_at_ms, 0), turn_steps
		FROM dialogue WHERE scope_key = ?`, scopeKey)
	return scanDialogue(row)
}

func (s *Store) createDialogue(ctx context.Context, scopeKey, scopeSource string, now int64) (*Dialogue, error) {
	dialogue := &Dialogue{
		ID:          NewID(),
		ScopeKey:    scopeKey,
		ScopeSource: scopeSource,
		Level:       1,
		Owner:       "assistant",
		Phase:       "user_to_adjuster_1",
		CreatedAtMS: now,
		UpdatedAtMS: now,
	}
	if err := s.exec(ctx, `INSERT INTO dialogue
		(id, scope_key, scope_source, level, owner, phase, request_id, expert_model_id, assistant_model_id,
		 state_version, closed, created_at_ms, updated_at_ms, last_human_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, 0, ?, ?, NULL)`,
		dialogue.ID, dialogue.ScopeKey, dialogue.ScopeSource, dialogue.Level, dialogue.Owner,
		dialogue.Phase, dialogue.StateVersion, dialogue.CreatedAtMS, dialogue.UpdatedAtMS); err != nil {
		return nil, fmt.Errorf("create dialogue for %q: %w", scopeKey, err)
	}
	return dialogue, nil
}

func scanDialogue(row *sql.Row) (*Dialogue, error) {
	var dialogue Dialogue
	var closed int
	if err := row.Scan(&dialogue.ID, &dialogue.ScopeKey, &dialogue.ScopeSource, &dialogue.Level,
		&dialogue.Owner, &dialogue.Phase, &dialogue.RequestID, &dialogue.ExpertModelID,
		&dialogue.AssistantModelID, &dialogue.StateVersion, &closed, &dialogue.CreatedAtMS,
		&dialogue.UpdatedAtMS, &dialogue.LastHumanAtMS, &dialogue.TurnSteps); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan dialogue: %w", err)
	}
	dialogue.Closed = closed == 1
	return &dialogue, nil
}

// UpdateDialogue persists the level, owner, pinned models and phase of a
// dialogue. The state version grows with every write.
func (s *Store) UpdateDialogue(ctx context.Context, dialogue *Dialogue) error {
	dialogue.StateVersion++
	dialogue.UpdatedAtMS = s.NowMS()
	if err := s.exec(ctx, `UPDATE dialogue
		SET level = ?, owner = ?, phase = ?, request_id = ?, expert_model_id = ?, assistant_model_id = ?,
		    state_version = ?, closed = ?, updated_at_ms = ?, turn_steps = ?
		WHERE id = ?`,
		dialogue.Level, dialogue.Owner, dialogue.Phase, nullString(dialogue.RequestID),
		nullString(dialogue.ExpertModelID), nullString(dialogue.AssistantModelID),
		dialogue.StateVersion, boolInt(dialogue.Closed), dialogue.UpdatedAtMS, dialogue.TurnSteps, dialogue.ID); err != nil {
		return fmt.Errorf("update dialogue %s: %w", dialogue.ID, err)
	}
	return nil
}

// dialoguePhases is the closed list of dialogue transitions. A value names the
// step the dialogue leaves and the step it enters next; the receiver of the
// edge is the state the dialogue is in.
var dialoguePhases = map[string]struct{}{
	"user_to_adjuster_1":                  {},
	"user_to_expert":                      {},
	"adjuster_1_to_assistant_1":           {},
	"adjuster_1_to_expert":                {},
	"assistant_1_to_client_tools":         {},
	"client_tools_to_assistant_1":         {},
	"assistant_1_to_adjuster_2":           {},
	"assistant_1_to_expert":               {},
	"adjuster_2_to_expert":                {},
	"client_tools_to_adjuster_3":          {},
	"adjuster_3_to_deflator":              {},
	"adjuster_3_to_expert":                {},
	"deflator_to_expert":                  {},
	"adjuster_3_to_assistant_1":           {},
	"deflator_to_assistant_1":             {},
	"adjuster_2_timeout_stop":             {},
	"adjuster_2_upstream_error_stop":      {},
	"adjuster_2_canceled_stop":            {},
	"adjuster_3_canceled_stop":            {},
	"deflator_canceled_stop":              {},
	"adjuster_2_to_user":                  {},
	"expert_to_client_tools":              {},
	"client_tools_to_expert":              {},
	"expert_to_user":                      {},
	"adjuster_1_no_available_model_stop":  {},
	"adjuster_1_timeout_stop":             {},
	"adjuster_1_upstream_error_stop":      {},
	"adjuster_1_canceled_stop":            {},
	"assistant_1_timeout_stop":            {},
	"assistant_1_upstream_error_stop":     {},
	"assistant_1_empty_answer_stop":       {},
	"assistant_1_no_available_model_stop": {},
	"assistant_1_canceled_stop":           {},
	"expert_timeout_stop":                 {},
	"expert_upstream_error_stop":          {},
	"expert_empty_answer_stop":            {},
	"expert_no_available_model_stop":      {},
	"expert_canceled_stop":                {},
	"client_tools_timeout_stop":           {},
	"client_tools_canceled_stop":          {},
	"assistant_2_canceled_stop":           {},
}

// SetDialoguePhase records the transition the dialogue leaves its current step
// by. A value outside the closed list is rejected before the write, so a typo
// never reaches the CHECK constraint.
func (s *Store) SetDialoguePhase(ctx context.Context, dialogueID, phase string) error {
	if _, ok := dialoguePhases[phase]; !ok {
		return fmt.Errorf("dialogue %s: phase %q is not in the transition dictionary", dialogueID, phase)
	}
	if err := s.exec(ctx, `UPDATE dialogue SET phase = ?, updated_at_ms = ? WHERE id = ?`,
		phase, s.NowMS(), dialogueID); err != nil {
		return fmt.Errorf("set dialogue phase %q for %s: %w", phase, dialogueID, err)
	}
	return nil
}

// TouchHuman records the moment the client delivered a human message. It is the
// anchor for the lazy idle check in EnsureDialogue.
func (s *Store) TouchHuman(ctx context.Context, dialogueID string) error {
	now := s.NowMS()
	if err := s.exec(ctx, `UPDATE dialogue SET last_human_at_ms = ?, updated_at_ms = ? WHERE id = ?`,
		now, now, dialogueID); err != nil {
		return fmt.Errorf("touch dialogue %s: %w", dialogueID, err)
	}
	return nil
}

// Messages returns the dialogue history in the order the model saw it.
func (s *Store) Messages(ctx context.Context, dialogueID string) ([]Message, error) {
	rows, err := s.ro.QueryContext(ctx, `SELECT id, dialogue_id, seq, role, kind,
		COALESCE(content, ''), COALESCE(reasoning_content, ''), COALESCE(reasoning_group_id, ''),
		COALESCE(call_id, ''),
		COALESCE(tool_name, ''), truncated, created_at_ms
		FROM message WHERE dialogue_id = ? ORDER BY seq`, dialogueID)
	if err != nil {
		return nil, fmt.Errorf("read messages: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Message
	index := make(map[string]int)
	for rows.Next() {
		var message Message
		var truncated int
		if err := rows.Scan(&message.ID, &message.DialogueID, &message.Seq, &message.Role,
			&message.Kind, &message.Content, &message.ReasoningContent, &message.ReasoningGroupID,
			&message.CallID, &message.ToolName,
			&truncated, &message.CreatedAtMS); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		message.Truncated = truncated == 1
		index[message.ID] = len(out)
		out = append(out, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	if len(out) == 0 {
		return out, nil
	}
	parts, err := s.ro.QueryContext(ctx, `SELECT p.message_id, p.seq, p.part_type,
		COALESCE(p.text_value, ''), COALESCE(p.uri, ''), COALESCE(p.mime_type, ''), p.byte_size
		FROM message_part p JOIN message m ON m.id = p.message_id
		WHERE m.dialogue_id = ? ORDER BY p.message_id, p.seq`, dialogueID)
	if err != nil {
		return nil, fmt.Errorf("read message parts: %w", err)
	}
	defer func() { _ = parts.Close() }()
	for parts.Next() {
		var messageID string
		var part Part
		var size sql.NullInt64
		if err := parts.Scan(&messageID, &part.Seq, &part.Type, &part.Text, &part.URI,
			&part.MimeType, &size); err != nil {
			return nil, fmt.Errorf("scan message part: %w", err)
		}
		if size.Valid {
			value := size.Int64
			part.ByteSize = &value
		}
		if idx, ok := index[messageID]; ok {
			out[idx].Parts = append(out[idx].Parts, part)
		}
	}
	if err := parts.Err(); err != nil {
		return nil, fmt.Errorf("iterate message parts: %w", err)
	}
	return out, nil
}

// AppendMessages writes one batch of messages and its parts in a single
// transaction. The returned value is the last sequence number in the dialogue.
func (s *Store) AppendMessages(ctx context.Context, dialogueID string, batch []Message) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin message batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var seq int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), -1) FROM message WHERE dialogue_id = ?`, dialogueID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("read last message seq: %w", err)
	}
	now := s.NowMS()
	for i := range batch {
		seq++
		message := &batch[i]
		if message.ID == "" {
			message.ID = NewID()
		}
		message.DialogueID = dialogueID
		message.Seq = int(seq)
		message.CreatedAtMS = now
		if _, err := tx.ExecContext(ctx, `INSERT INTO message
			(id, dialogue_id, seq, role, kind, content, reasoning_content, reasoning_group_id,
			 call_id, tool_name, truncated, created_at_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			message.ID, dialogueID, message.Seq, message.Role, message.Kind, nullString(message.Content),
			nullString(message.ReasoningContent), nullString(message.ReasoningGroupID),
			nullString(message.CallID), nullString(message.ToolName),
			boolInt(message.Truncated), message.CreatedAtMS); err != nil {
			return 0, fmt.Errorf("insert message %s: %w", message.ID, err)
		}
		for _, part := range message.Parts {
			if _, err := tx.ExecContext(ctx, `INSERT INTO message_part
				(message_id, seq, part_type, text_value, uri, mime_type, byte_size)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				message.ID, part.Seq, part.Type, nullString(part.Text), nullString(part.URI),
				nullString(part.MimeType), nullIntPtr(part.ByteSize)); err != nil {
				return 0, fmt.Errorf("insert message part for %s: %w", message.ID, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dialogue SET updated_at_ms = ? WHERE id = ?`, now, dialogueID); err != nil {
		return 0, fmt.Errorf("touch dialogue %s: %w", dialogueID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit message batch: %w", err)
	}
	return seq, nil
}

// Delivery returns the stored response for an idempotent repeat.
func (s *Store) Delivery(ctx context.Context, dialogueID, endpoint, fingerprint string) (*Delivery, bool, error) {
	row := s.queryRow(ctx, `SELECT id, dialogue_id, fingerprint, endpoint, response_body, created_at_ms
		FROM delivery WHERE dialogue_id = ? AND endpoint = ? AND fingerprint = ?`,
		dialogueID, endpoint, fingerprint)
	var item Delivery
	if err := row.Scan(&item.ID, &item.DialogueID, &item.Fingerprint, &item.Endpoint,
		&item.ResponseBody, &item.CreatedAtMS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read delivery: %w", err)
	}
	return &item, true, nil
}

func (s *Store) PutDelivery(ctx context.Context, item Delivery) error {
	return s.exec(ctx, `INSERT INTO delivery (id, dialogue_id, fingerprint, endpoint, response_body, created_at_ms)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(dialogue_id, endpoint, fingerprint) DO NOTHING`,
		item.ID, item.DialogueID, item.Fingerprint, item.Endpoint, item.ResponseBody, s.NowMS())
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullIntPtr(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
