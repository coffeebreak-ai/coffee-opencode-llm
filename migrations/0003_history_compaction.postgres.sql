-- A dialogue that outgrows history_max_tokens is compacted once: the oldest
-- part is replaced by a summary for the prompt. The message log stays intact.
CREATE TABLE dialogue_compaction (
    dialogue_id   TEXT PRIMARY KEY REFERENCES dialogue(id) ON DELETE CASCADE,
    border_hash   TEXT NOT NULL,
    covered_count INTEGER NOT NULL,
    summary       TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
