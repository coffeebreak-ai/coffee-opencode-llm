CREATE TABLE model (
    id                TEXT PRIMARY KEY,
    context_tokens    INTEGER NOT NULL CHECK (context_tokens > 0),
    max_output_tokens INTEGER NOT NULL CHECK (max_output_tokens > 0),
    cost_in_per_mtok  NUMERIC(14,6),
    cost_out_per_mtok NUMERIC(14,6),
    quality_tier      TEXT NOT NULL CHECK (quality_tier IN ('high','medium','low')),
    retention         TEXT,
    quota_group       TEXT,
    strengths         TEXT NOT NULL,
    limits_text       TEXT NOT NULL,
    enabled           INTEGER NOT NULL CHECK (enabled IN (0,1)),
    created_at_ms     BIGINT NOT NULL,
    updated_at_ms     BIGINT NOT NULL
);

CREATE TABLE model_role (
    model_id TEXT NOT NULL REFERENCES model(id),
    role     TEXT NOT NULL CHECK (role IN ('expert','assistant')),
    PRIMARY KEY (model_id, role)
);

CREATE TABLE model_group (
    model_id    TEXT NOT NULL REFERENCES model(id),
    group_name  TEXT NOT NULL CHECK (group_name IN ('coffee-expert','coffee-assistant')),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('profile','user','command')),
    active      INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    PRIMARY KEY (model_id, group_name)
);

CREATE TABLE model_runtime_state (
    model_id          TEXT PRIMARY KEY REFERENCES model(id),
    enabled_override  INTEGER CHECK (enabled_override IN (0,1)),
    disabled_until_ms BIGINT,
    disabled_reason   TEXT CHECK (disabled_reason IN ('smoke_failed','missing_upstream','command','rotation')),
    updated_at_ms     BIGINT NOT NULL
);

CREATE TABLE model_rotation_state (
    model_id          TEXT PRIMARY KEY REFERENCES model(id),
    excluded_until_ms BIGINT,
    manual_excluded   INTEGER NOT NULL DEFAULT 0 CHECK (manual_excluded IN (0,1)),
    error_count       INTEGER NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    last_error_kind   TEXT,
    updated_at_ms     BIGINT NOT NULL
);

CREATE TABLE model_effort (
    model_id TEXT NOT NULL REFERENCES model(id),
    effort   TEXT NOT NULL CHECK (effort IN ('minimal','low','medium','high','max')),
    PRIMARY KEY (model_id, effort)
);

CREATE TABLE model_modality (
    model_id TEXT NOT NULL REFERENCES model(id),
    modality TEXT NOT NULL CHECK (modality IN ('text','image','audio')),
    PRIMARY KEY (model_id, modality)
);

CREATE TABLE model_feature (
    model_id TEXT NOT NULL REFERENCES model(id),
    feature  TEXT NOT NULL CHECK (feature IN ('tools','streaming')),
    PRIMARY KEY (model_id, feature)
);

CREATE TABLE quota_window (
    owner_kind      TEXT    NOT NULL CHECK (owner_kind IN ('model','group')),
    owner_id        TEXT    NOT NULL,
    window_kind     TEXT    NOT NULL CHECK (window_kind IN ('rpm','day','month')),
    window_start_ms BIGINT  NOT NULL,
    limit_value     INTEGER CHECK (limit_value IS NULL OR limit_value >= 0),
    used_value      INTEGER NOT NULL DEFAULT 0 CHECK (used_value >= 0),
    reserved_value  INTEGER NOT NULL DEFAULT 0 CHECK (reserved_value >= 0),
    updated_at_ms   BIGINT  NOT NULL,
    PRIMARY KEY (owner_kind, owner_id, window_kind, window_start_ms)
);

CREATE TABLE quota_event (
    id             TEXT PRIMARY KEY,
    owner_kind     TEXT NOT NULL CHECK (owner_kind IN ('model','group')),
    owner_id       TEXT NOT NULL,
    model_id       TEXT NOT NULL,
    window_kind    TEXT NOT NULL CHECK (window_kind IN ('rpm','day','month')),
    event_kind     TEXT NOT NULL CHECK (event_kind IN ('reserved','committed','released','limit_learned','rate_limited','cooldown_set')),
    amount         INTEGER,
    observed_limit INTEGER,
    retry_after_ms BIGINT,
    http_status    INTEGER,
    created_at_ms  BIGINT NOT NULL
);

CREATE INDEX idx_quota_event_owner ON quota_event (owner_kind, owner_id, created_at_ms);

CREATE TABLE runtime_setting (
    key           TEXT PRIMARY KEY
                  CHECK (key IN ('objective_mode','reasoning_mode','subscription_mode')),
    value         TEXT NOT NULL,
    source_kind   TEXT NOT NULL CHECK (source_kind IN ('config','env','command')),
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE dialogue (
    id                 TEXT PRIMARY KEY,
    scope_key          TEXT NOT NULL UNIQUE,
    scope_source       TEXT NOT NULL CHECK (scope_source IN ('header','metadata','user','fingerprint')),
    level              INTEGER NOT NULL CHECK (level IN (1,2)),
    owner              TEXT NOT NULL CHECK (owner IN ('assistant','expert')),
    phase              TEXT NOT NULL CHECK (phase IN (
        'user_to_jev','user_to_expert','jev_to_assistant_1','jev_to_expert',
        'assistant_1_to_client_tools','client_tools_to_assistant_1',
        'assistant_1_to_jev','assistant_1_to_expert','jev_to_user',
        'expert_to_client_tools','client_tools_to_expert','expert_to_user',
        'jev_no_available_model_stop','jev_timeout_stop','jev_upstream_error_stop','jev_canceled_stop',
        'assistant_1_timeout_stop','assistant_1_upstream_error_stop','assistant_1_empty_answer_stop',
        'assistant_1_no_available_model_stop','assistant_1_canceled_stop',
        'expert_timeout_stop','expert_upstream_error_stop','expert_empty_answer_stop',
        'expert_no_available_model_stop','expert_canceled_stop',
        'client_tools_timeout_stop','client_tools_canceled_stop','assistant_2_canceled_stop')),
    request_id         TEXT,
    expert_model_id    TEXT REFERENCES model(id),
    assistant_model_id TEXT REFERENCES model(id),
    state_version      INTEGER NOT NULL,
    closed             INTEGER NOT NULL CHECK (closed IN (0,1)),
    created_at_ms      BIGINT NOT NULL,
    updated_at_ms      BIGINT NOT NULL,
    last_human_at_ms   BIGINT
);

CREATE TABLE message (
    id            TEXT PRIMARY KEY,
    dialogue_id   TEXT NOT NULL REFERENCES dialogue(id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL CHECK (seq >= 0),
    role          TEXT NOT NULL CHECK (role IN ('system','developer','user','assistant','tool')),
    kind          TEXT NOT NULL CHECK (kind IN ('human','message','tool_call','tool_result','delegation','delegation_result','service')),
    content       TEXT,
    call_id       TEXT,
    tool_name     TEXT,
    truncated     INTEGER NOT NULL DEFAULT 0 CHECK (truncated IN (0,1)),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (dialogue_id, seq)
);

CREATE INDEX idx_message_dialogue_seq ON message (dialogue_id, seq);

CREATE TABLE message_part (
    message_id  TEXT NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL CHECK (seq >= 0),
    part_type   TEXT NOT NULL CHECK (part_type IN ('text','image_url','input_audio')),
    text_value  TEXT,
    uri         TEXT,
    mime_type   TEXT,
    byte_size   INTEGER CHECK (byte_size IS NULL OR byte_size >= 0),
    PRIMARY KEY (message_id, seq)
);

CREATE TABLE delivery (
    id            TEXT PRIMARY KEY,
    dialogue_id   TEXT NOT NULL REFERENCES dialogue(id) ON DELETE CASCADE,
    fingerprint   TEXT NOT NULL,
    endpoint      TEXT NOT NULL CHECK (endpoint IN ('chat','responses')),
    response_body TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    UNIQUE (dialogue_id, endpoint, fingerprint)
);

CREATE TABLE delegation (
    id                 TEXT PRIMARY KEY,
    dialogue_id        TEXT NOT NULL REFERENCES dialogue(id) ON DELETE CASCADE,
    seq                INTEGER NOT NULL,
    task               TEXT NOT NULL,
    context_text       TEXT,
    scope              TEXT NOT NULL CHECK (scope IN ('search','read','external','write','full')),
    assistant_model_id TEXT NOT NULL REFERENCES model(id),
    budget_steps       INTEGER NOT NULL CHECK (budget_steps > 0),
    budget_chars       INTEGER NOT NULL CHECK (budget_chars > 0),
    status             TEXT NOT NULL CHECK (status IN (
        'expert_to_assistant_2','assistant_2_to_client_tools',
        'client_tools_to_assistant_2','assistant_2_to_expert')),
    close_reason       TEXT CHECK (close_reason IS NULL OR close_reason IN (
        'ok','budget_exhausted','client_tools_missing','needs_expert',
        'no_available_model','canceled')),
    started_at_ms      BIGINT,
    finished_at_ms     BIGINT,
    UNIQUE (dialogue_id, seq)
);

CREATE TABLE delegation_step (
    id            TEXT PRIMARY KEY,
    delegation_id TEXT NOT NULL REFERENCES delegation(id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL,
    call_id       TEXT,
    tool_name     TEXT,
    arguments_raw TEXT,
    status        TEXT NOT NULL CHECK (status IN (
        'assistant_2_to_client_tools','client_tools_to_assistant_2')),
    created_at_ms BIGINT NOT NULL,
    UNIQUE (delegation_id, seq)
);

CREATE INDEX idx_delegation_step_call ON delegation_step (delegation_id, call_id);

CREATE TABLE pin_event (
    id            TEXT PRIMARY KEY,
    dialogue_id   TEXT NOT NULL REFERENCES dialogue(id) ON DELETE CASCADE,
    role          TEXT NOT NULL CHECK (role IN ('assistant','expert')),
    model_id      TEXT NOT NULL REFERENCES model(id),
    reason        TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE model_call (
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
    reasoning_mode   TEXT CHECK (reasoning_mode IN ('min','medium','high','max')),
    reasoning_effort TEXT CHECK (reasoning_effort IN ('minimal','low','medium','high','max')),
    in_group_expert  INTEGER CHECK (in_group_expert IN (0,1)),
    in_group_assistant INTEGER CHECK (in_group_assistant IN (0,1)),
    pipeline_mode    TEXT CHECK (pipeline_mode IN ('pipeline','expert_only')),
    created_at_ms    BIGINT NOT NULL
);

CREATE INDEX idx_model_call_dialogue ON model_call (dialogue_id, created_at_ms);
CREATE INDEX idx_model_call_created ON model_call (created_at_ms);

CREATE TABLE journal_event (
    id            TEXT PRIMARY KEY,
    dialogue_id   TEXT,
    level         TEXT NOT NULL CHECK (level IN ('debug','info','warn','error')),
    kind          TEXT NOT NULL CHECK (kind IN (
        'user_to_jev','user_to_expert','jev_to_assistant_1','jev_to_expert',
        'assistant_1_to_client_tools','client_tools_to_assistant_1',
        'assistant_1_to_jev','assistant_1_to_expert',
        'expert_to_client_tools','client_tools_to_expert','jev_to_user','expert_to_user',
        'expert_to_assistant_2','assistant_2_to_client_tools','client_tools_to_assistant_2','assistant_2_to_expert',
        'jev_no_available_model_stop','jev_timeout_stop','jev_upstream_error_stop','jev_canceled_stop',
        'assistant_1_timeout_stop','assistant_1_upstream_error_stop','assistant_1_empty_answer_stop',
        'assistant_1_no_available_model_stop','assistant_1_canceled_stop',
        'expert_timeout_stop','expert_upstream_error_stop','expert_empty_answer_stop',
        'expert_no_available_model_stop','expert_canceled_stop',
        'client_tools_timeout_stop','client_tools_canceled_stop','assistant_2_canceled_stop',
        'jev_failed','jev_fallback_applied','model_call_failed','upstream_rate_limited',
        'effort_degraded','invalid_scope_rejected','turn_canceled','quota_reservation_expired',
        'pin_changed','delivery_replayed','missing_upstream','smoke_failed','retention_applied')),
    role          TEXT CHECK (role IS NULL OR
        role IN ('assistant','expert','adjuster','deflator')),
    request_id    TEXT,
    note          TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE INDEX idx_journal_created ON journal_event (created_at_ms);
CREATE INDEX idx_journal_kind ON journal_event (kind, created_at_ms);
