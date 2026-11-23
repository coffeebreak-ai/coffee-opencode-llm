-- Add opt-in reasoning context storage without changing existing dialogue rows.
CREATE TABLE runtime_setting_new (
    key           TEXT PRIMARY KEY
                  CHECK (key IN ('objective_mode','reasoning_mode','reasoning_context_enabled','subscription_mode')),
    value         TEXT NOT NULL,
    source_kind   TEXT NOT NULL CHECK (source_kind IN ('config','env','command')),
    updated_at_ms BIGINT NOT NULL
);

INSERT INTO runtime_setting_new (key, value, source_kind, updated_at_ms)
SELECT key, value, source_kind, updated_at_ms FROM runtime_setting;

DROP TABLE runtime_setting;
ALTER TABLE runtime_setting_new RENAME TO runtime_setting;

ALTER TABLE message ADD COLUMN reasoning_content TEXT;
ALTER TABLE message ADD COLUMN reasoning_group_id TEXT;

ALTER TABLE dialogue_compaction ADD COLUMN reasoning_included
    INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_included IN (0,1));

CREATE TABLE dialogue_compaction_failure (
    dialogue_id   TEXT PRIMARY KEY REFERENCES dialogue(id) ON DELETE CASCADE,
    border_hash   TEXT NOT NULL,
    failed_at_ms  BIGINT NOT NULL
);
