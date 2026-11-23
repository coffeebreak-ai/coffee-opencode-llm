-- no-transaction
-- ADR 0009 §10: the jev_* statuses are replaced by adjuster_1, adjuster_2 and
-- adjuster_3. SQLite cannot change a CHECK constraint in place, and dropping a
-- parent table with foreign keys on would cascade into its children, so the
-- pragma is switched off outside a transaction.
PRAGMA foreign_keys=OFF;

CREATE TABLE dialogue_new (
    id                 TEXT PRIMARY KEY,
    scope_key          TEXT NOT NULL UNIQUE,
    scope_source       TEXT NOT NULL CHECK (scope_source IN ('header','metadata','user','fingerprint')),
    level              INTEGER NOT NULL CHECK (level IN (1,2)),
    owner              TEXT NOT NULL CHECK (owner IN ('assistant','expert')),
    phase              TEXT NOT NULL CHECK (phase IN (
        'user_to_adjuster_1','user_to_expert','adjuster_1_to_assistant_1','adjuster_1_to_expert',
        'assistant_1_to_client_tools','client_tools_to_assistant_1','assistant_1_to_adjuster_2','assistant_1_to_expert',
        'adjuster_2_to_user','adjuster_2_to_expert','expert_to_client_tools','client_tools_to_expert',
        'client_tools_to_adjuster_3','adjuster_3_to_deflator','adjuster_3_to_expert','deflator_to_expert',
        'adjuster_3_to_assistant_1','deflator_to_assistant_1','expert_to_user','expert_to_assistant_2',
        'assistant_2_to_client_tools','client_tools_to_assistant_2','assistant_2_to_expert','adjuster_1_no_available_model_stop',
        'adjuster_1_timeout_stop','adjuster_1_upstream_error_stop','adjuster_1_canceled_stop','adjuster_2_timeout_stop',
        'adjuster_2_upstream_error_stop','adjuster_2_canceled_stop','adjuster_3_canceled_stop','deflator_canceled_stop',
        'assistant_1_timeout_stop','assistant_1_upstream_error_stop','assistant_1_empty_answer_stop','assistant_1_no_available_model_stop',
        'assistant_1_canceled_stop','expert_timeout_stop','expert_upstream_error_stop','expert_empty_answer_stop',
        'expert_no_available_model_stop','expert_canceled_stop','client_tools_timeout_stop','client_tools_canceled_stop',
        'assistant_2_canceled_stop')),
    request_id         TEXT,
    expert_model_id    TEXT REFERENCES model(id),
    assistant_model_id TEXT REFERENCES model(id),
    state_version      INTEGER NOT NULL,
    closed             INTEGER NOT NULL CHECK (closed IN (0,1)),
    created_at_ms      BIGINT NOT NULL,
    updated_at_ms      BIGINT NOT NULL,
    last_human_at_ms   BIGINT
);

INSERT INTO dialogue_new (id, scope_key, scope_source, level, owner, phase, request_id,
    expert_model_id, assistant_model_id, state_version, closed, created_at_ms, updated_at_ms, last_human_at_ms)
SELECT id, scope_key, scope_source, level, owner,
    CASE phase
          WHEN 'user_to_jev' THEN 'user_to_adjuster_1'
          WHEN 'jev_to_assistant_1' THEN 'adjuster_1_to_assistant_1'
          WHEN 'jev_to_expert' THEN 'adjuster_1_to_expert'
          WHEN 'assistant_1_to_jev' THEN 'assistant_1_to_adjuster_2'
          WHEN 'jev_to_user' THEN 'adjuster_2_to_user'
          WHEN 'jev_no_available_model_stop' THEN 'adjuster_1_no_available_model_stop'
          WHEN 'jev_timeout_stop' THEN 'adjuster_1_timeout_stop'
          WHEN 'jev_upstream_error_stop' THEN 'adjuster_1_upstream_error_stop'
          WHEN 'jev_canceled_stop' THEN 'adjuster_1_canceled_stop'
          ELSE phase END,
    request_id,
    expert_model_id, assistant_model_id, state_version, closed, created_at_ms, updated_at_ms, last_human_at_ms
FROM dialogue;

DROP TABLE dialogue;

ALTER TABLE dialogue_new RENAME TO dialogue;

CREATE TABLE journal_event_new (
    id            TEXT PRIMARY KEY,
    dialogue_id   TEXT,
    level         TEXT NOT NULL CHECK (level IN ('debug','info','warn','error')),
    kind          TEXT NOT NULL CHECK (kind IN (
        'user_to_adjuster_1','user_to_expert','adjuster_1_to_assistant_1','adjuster_1_to_expert',
        'assistant_1_to_client_tools','client_tools_to_assistant_1','assistant_1_to_adjuster_2','assistant_1_to_expert',
        'adjuster_2_to_user','adjuster_2_to_expert','expert_to_client_tools','client_tools_to_expert',
        'client_tools_to_adjuster_3','adjuster_3_to_deflator','adjuster_3_to_expert','deflator_to_expert',
        'adjuster_3_to_assistant_1','deflator_to_assistant_1','expert_to_user','expert_to_assistant_2',
        'assistant_2_to_client_tools','client_tools_to_assistant_2','assistant_2_to_expert','adjuster_1_no_available_model_stop',
        'adjuster_1_timeout_stop','adjuster_1_upstream_error_stop','adjuster_1_canceled_stop','adjuster_2_timeout_stop',
        'adjuster_2_upstream_error_stop','adjuster_2_canceled_stop','adjuster_3_canceled_stop','deflator_canceled_stop',
        'assistant_1_timeout_stop','assistant_1_upstream_error_stop','assistant_1_empty_answer_stop','assistant_1_no_available_model_stop',
        'assistant_1_canceled_stop','expert_timeout_stop','expert_upstream_error_stop','expert_empty_answer_stop',
        'expert_no_available_model_stop','expert_canceled_stop','client_tools_timeout_stop','client_tools_canceled_stop',
        'assistant_2_canceled_stop','jev_failed','jev_fallback_applied','model_call_failed',
        'upstream_rate_limited','effort_degraded','invalid_scope_rejected','turn_canceled',
        'quota_reservation_expired','pin_changed','delivery_replayed','missing_upstream',
        'smoke_failed','retention_applied')),
    role          TEXT CHECK (role IS NULL OR role IN ('assistant','expert','adjuster','deflator')),
    request_id    TEXT,
    note          TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

INSERT INTO journal_event_new (id, dialogue_id, level, kind, role, request_id, note, created_at_ms)
SELECT id, dialogue_id, level,
    CASE kind
          WHEN 'user_to_jev' THEN 'user_to_adjuster_1'
          WHEN 'jev_to_assistant_1' THEN 'adjuster_1_to_assistant_1'
          WHEN 'jev_to_expert' THEN 'adjuster_1_to_expert'
          WHEN 'assistant_1_to_jev' THEN 'assistant_1_to_adjuster_2'
          WHEN 'jev_to_user' THEN 'adjuster_2_to_user'
          WHEN 'jev_no_available_model_stop' THEN 'adjuster_1_no_available_model_stop'
          WHEN 'jev_timeout_stop' THEN 'adjuster_1_timeout_stop'
          WHEN 'jev_upstream_error_stop' THEN 'adjuster_1_upstream_error_stop'
          WHEN 'jev_canceled_stop' THEN 'adjuster_1_canceled_stop'
          ELSE kind END,
    role, request_id, note, created_at_ms FROM journal_event;

DROP TABLE journal_event;

ALTER TABLE journal_event_new RENAME TO journal_event;

CREATE INDEX idx_journal_created ON journal_event (created_at_ms);
CREATE INDEX idx_journal_kind ON journal_event (kind, created_at_ms);

PRAGMA foreign_keys=ON;
