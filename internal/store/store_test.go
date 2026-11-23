package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "coffee-opencode-llm.db"), Options{
		BusyTimeoutMS: 5000,
		MaxOpenConns:  4,
		MaxIdleConns:  2,
		WriteConns:    1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

func TestMigrationsApplyOnce(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	var count int
	if err := st.queryRow(ctx, `SELECT COUNT(*) FROM schema_migration`).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	files, err := loadMigrations("sqlite")
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if count != len(files) {
		t.Fatalf("schema_migration rows = %d, want %d", count, len(files))
	}
}

func TestReasoningContextMigrationPreservesRuntimeSettings(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "coffee-opencode-llm.db"), Options{
		BusyTimeoutMS: 5000,
		MaxOpenConns:  4,
		MaxIdleConns:  2,
		WriteConns:    1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if _, err := st.rw.ExecContext(ctx, `CREATE TABLE schema_migration (
		version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_ms BIGINT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	files, err := loadMigrations("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.version >= 7 {
			break
		}
		tx, err := st.rw.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin migration %d: %v", file.version, err)
		}
		if _, err := tx.ExecContext(ctx, file.content); err != nil {
			_ = tx.Rollback()
			t.Fatalf("apply migration %d: %v", file.version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migration
			(version, name, checksum, applied_at_ms) VALUES (?, ?, ?, ?)`,
			file.version, file.name, file.checksum, st.NowMS()); err != nil {
			_ = tx.Rollback()
			t.Fatalf("record migration %d: %v", file.version, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit migration %d: %v", file.version, err)
		}
	}
	if err := st.SetSetting(ctx, "objective_mode", "quality", "command"); err != nil {
		t.Fatalf("write pre-migration setting: %v", err)
	}
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		t.Fatalf("apply reasoning migration: %v", err)
	}
	item, err := st.Setting(ctx, "objective_mode")
	if err != nil {
		t.Fatalf("read preserved setting: %v", err)
	}
	if item.Value != "quality" || item.SourceKind != "command" {
		t.Fatalf("preserved setting = %+v", item)
	}
	if err := st.SetSetting(ctx, "reasoning_context_enabled", "on", "command"); err != nil {
		t.Fatalf("write reasoning setting: %v", err)
	}
	item, err = st.Setting(ctx, "reasoning_context_enabled")
	if err != nil {
		t.Fatalf("read reasoning setting: %v", err)
	}
	if item.Value != "on" || item.SourceKind != "command" {
		t.Fatalf("reasoning setting = %+v", item)
	}
	if err := st.SetSetting(ctx, "assistant2_reasoning_enabled", "off", "command"); err != nil {
		t.Fatalf("write assistant 2 setting: %v", err)
	}
	item, err = st.Setting(ctx, "assistant2_reasoning_enabled")
	if err != nil {
		t.Fatalf("read assistant 2 setting: %v", err)
	}
	if item.Value != "off" || item.SourceKind != "command" {
		t.Fatalf("assistant 2 setting = %+v", item)
	}
}

func TestQuotaReserveCommitRelease(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	limit := int64(2)
	quota := Quota{Day: &limit}

	reservation, err := st.Reserve(ctx, "req-1", "model-a", quota)
	if err != nil {
		t.Fatalf("reserve first: %v", err)
	}
	if err := st.Commit(ctx, *reservation); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	remaining, unlimited, err := st.Remaining(ctx, "model-a", quota)
	if err != nil {
		t.Fatalf("remaining after commit: %v", err)
	}
	if unlimited || remaining != 1 {
		t.Fatalf("remaining = %d, unlimited = %v, want 1 and false", remaining, unlimited)
	}

	released, err := st.Reserve(ctx, "req-2", "model-a", quota)
	if err != nil {
		t.Fatalf("reserve second: %v", err)
	}
	if err := st.Release(ctx, *released); err != nil {
		t.Fatalf("release second: %v", err)
	}
	remaining, _, err = st.Remaining(ctx, "model-a", quota)
	if err != nil {
		t.Fatalf("remaining after release: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining after release = %d, want 1", remaining)
	}

	held, err := st.Reserve(ctx, "req-3", "model-a", quota)
	if err != nil {
		t.Fatalf("reserve third: %v", err)
	}
	if _, err := st.Reserve(ctx, "req-4", "model-a", quota); !errors.Is(err, ErrNoQuota) {
		t.Fatalf("reserve over the limit: got %v, want ErrNoQuota", err)
	}
	if err := st.Commit(ctx, *held); err != nil {
		t.Fatalf("commit third: %v", err)
	}
	remaining, _, err = st.Remaining(ctx, "model-a", quota)
	if err != nil {
		t.Fatalf("remaining at the limit: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining at the limit = %d, want 0", remaining)
	}
}

func TestDelegationStepLifecycle(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	dialogue, err := st.EnsureDialogue(ctx, "session-1", "header", 72)
	if err != nil {
		t.Fatalf("ensure dialogue: %v", err)
	}
	now := st.NowMS()
	if err := st.exec(ctx, `INSERT INTO model
		(id, context_tokens, max_output_tokens, quality_tier, strengths, limits_text, enabled, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"model-a", 1000, 100, "high", "test", "test", 1, now, now); err != nil {
		t.Fatalf("insert model: %v", err)
	}
	delegation, err := st.CreateDelegation(ctx, Delegation{
		DialogueID: dialogue.ID, Task: "collect", Scope: "full",
		AssistantModelID: "model-a", BudgetSteps: 4, BudgetChars: 1000,
	})
	if err != nil {
		t.Fatalf("create delegation: %v", err)
	}
	for _, callID := range []string{"call_1", "call_2"} {
		if err := st.AddDelegationStep(ctx, delegation.ID, callID, "search", "{}", "assistant_2_to_client_tools"); err != nil {
			t.Fatalf("add step %s: %v", callID, err)
		}
	}
	assertStepStatuses(t, st, delegation.ID, map[string]int{"assistant_2_to_client_tools": 2})

	if err := st.CompleteDelegationSteps(ctx, delegation.ID, []string{"call_1", "call_unknown"}); err != nil {
		t.Fatalf("complete steps: %v", err)
	}
	assertStepStatuses(t, st, delegation.ID, map[string]int{"assistant_2_to_client_tools": 1, "client_tools_to_assistant_2": 1})

	if err := st.FinishDelegation(ctx, delegation.ID, "client_tools_missing"); err != nil {
		t.Fatalf("finish delegation: %v", err)
	}
	assertStepStatuses(t, st, delegation.ID, map[string]int{"client_tools_to_assistant_2": 1, "assistant_2_to_client_tools": 1})

	active, err := st.ActiveDelegation(ctx, dialogue.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("active delegation after stop = %+v, err = %v, want ErrNotFound", active, err)
	}
}

func assertStepStatuses(t *testing.T, st *Store, delegationID string, want map[string]int) {
	t.Helper()
	rows, err := st.ro.QueryContext(context.Background(),
		`SELECT status, COUNT(*) FROM delegation_step WHERE delegation_id = ? GROUP BY status`, delegationID)
	if err != nil {
		t.Fatalf("read step statuses: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			t.Fatalf("scan step status: %v", err)
		}
		got[status] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate step statuses: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("step statuses = %v, want %v", got, want)
	}
	for status, count := range want {
		if got[status] != count {
			t.Fatalf("step statuses = %v, want %v", got, want)
		}
	}
}

func TestDeliveryIsIdempotentPerEndpoint(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	dialogue, err := st.EnsureDialogue(ctx, "session-1", "header", 72)
	if err != nil {
		t.Fatalf("ensure dialogue: %v", err)
	}
	first := Delivery{ID: NewID(), DialogueID: dialogue.ID, Fingerprint: "fp-1", Endpoint: "chat",
		ResponseBody: `{"id":"first"}`}
	if err := st.PutDelivery(ctx, first); err != nil {
		t.Fatalf("put first delivery: %v", err)
	}
	second := Delivery{ID: NewID(), DialogueID: dialogue.ID, Fingerprint: "fp-1", Endpoint: "chat",
		ResponseBody: `{"id":"second"}`}
	if err := st.PutDelivery(ctx, second); err != nil {
		t.Fatalf("put duplicate delivery: %v", err)
	}
	found, ok, err := st.Delivery(ctx, dialogue.ID, "chat", "fp-1")
	if err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if !ok {
		t.Fatal("delivery not found")
	}
	if found.ID != first.ID || found.ResponseBody != first.ResponseBody {
		t.Fatalf("delivery was overwritten: %+v", found)
	}

	other := Delivery{ID: NewID(), DialogueID: dialogue.ID, Fingerprint: "fp-1", Endpoint: "responses",
		ResponseBody: `{"id":"other"}`}
	if err := st.PutDelivery(ctx, other); err != nil {
		t.Fatalf("put delivery for another endpoint: %v", err)
	}
	found, ok, err = st.Delivery(ctx, dialogue.ID, "responses", "fp-1")
	if err != nil {
		t.Fatalf("read responses delivery: %v", err)
	}
	if !ok || found.ID != other.ID {
		t.Fatalf("endpoint is not part of the delivery key: %+v", found)
	}
}
