-- reasoning_mode may now be "client": the effort is chosen by the caller per
-- request instead of being fixed by the gateway. SQLite cannot alter a CHECK
-- constraint in place, so the table is rebuilt and its rows are copied.
CREATE TABLE model_call_new (
    id               TEXT PRIMARY KEY,
    dialogue_id      TEXT REFERENCES dialogue(id) ON DELETE SET NULL,
    delegation_id    TEXT REFERENCES delegation(id) ON DELETE SET NULL,
    role             TEXT NOT NULL CHECK (role IN ('assistant','expert','adjuster','deflator')),
    model_id         TEXT NOT NULL,
    purpose          TEXT,
    request_id       TEXT NOT NULL,
    input_tokens     BIGINT,
    cached_tokens    BIGINT,
    output_tokens    BIGINT,
    reasoning_tokens BIGINT,
    duration_ms      INTEGER NOT NULL,
    http_status      INTEGER,
    cost_usd         NUMERIC(14,6),
    objective_mode   TEXT CHECK (objective_mode IN ('balanced','quality','price')),
    reasoning_mode   TEXT CHECK (reasoning_mode IN ('min','medium','high','max','client')),
    reasoning_effort TEXT CHECK (reasoning_effort IN ('minimal','low','medium','high','max')),
    in_group_expert  INTEGER CHECK (in_group_expert IN (0,1)),
    in_group_assistant INTEGER CHECK (in_group_assistant IN (0,1)),
    pipeline_mode    TEXT CHECK (pipeline_mode IN ('pipeline','expert_only')),
    created_at_ms    BIGINT NOT NULL
);

INSERT INTO model_call_new (id, dialogue_id, delegation_id, role, model_id, purpose, request_id,
    input_tokens, cached_tokens, output_tokens, reasoning_tokens, duration_ms, http_status, cost_usd,
    objective_mode, reasoning_mode, reasoning_effort, in_group_expert, in_group_assistant, pipeline_mode,
    created_at_ms)
SELECT id, dialogue_id, delegation_id, role, model_id, purpose, request_id,
    input_tokens, cached_tokens, output_tokens, reasoning_tokens, duration_ms, http_status, cost_usd,
    objective_mode, reasoning_mode, reasoning_effort, in_group_expert, in_group_assistant, pipeline_mode,
    created_at_ms
FROM model_call;

DROP TABLE model_call;

ALTER TABLE model_call_new RENAME TO model_call;

CREATE INDEX idx_model_call_dialogue ON model_call (dialogue_id, created_at_ms);
CREATE INDEX idx_model_call_created ON model_call (created_at_ms);
