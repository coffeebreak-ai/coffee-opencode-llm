-- Extend runtime settings with the reasoning trace scope (ADR 0024).
CREATE TABLE runtime_setting_new (
    key           TEXT PRIMARY KEY
                  CHECK (key IN ('objective_mode','reasoning_mode','reasoning_context_enabled','reasoning_trace_scope','subscription_mode')),
    value         TEXT NOT NULL,
    source_kind   TEXT NOT NULL CHECK (source_kind IN ('config','env','command')),
    updated_at_ms BIGINT NOT NULL
);

INSERT INTO runtime_setting_new (key, value, source_kind, updated_at_ms)
SELECT key, value, source_kind, updated_at_ms FROM runtime_setting;

DROP TABLE runtime_setting;
ALTER TABLE runtime_setting_new RENAME TO runtime_setting;
