-- Add opt-in reasoning context storage without changing existing dialogue rows.
ALTER TABLE runtime_setting DROP CONSTRAINT runtime_setting_key_check;
ALTER TABLE runtime_setting ADD CONSTRAINT runtime_setting_key_check
    CHECK (key IN ('objective_mode','reasoning_mode','reasoning_context_enabled','subscription_mode'));

ALTER TABLE message ADD COLUMN reasoning_content TEXT;
ALTER TABLE message ADD COLUMN reasoning_group_id TEXT;

ALTER TABLE dialogue_compaction ADD COLUMN reasoning_included
    INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_included IN (0,1));

CREATE TABLE dialogue_compaction_failure (
    dialogue_id   TEXT PRIMARY KEY REFERENCES dialogue(id) ON DELETE CASCADE,
    border_hash   TEXT NOT NULL,
    failed_at_ms  BIGINT NOT NULL
);
