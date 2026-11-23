-- Extend runtime settings with the reasoning trace scope (ADR 0024).
ALTER TABLE runtime_setting DROP CONSTRAINT runtime_setting_key_check;
ALTER TABLE runtime_setting ADD CONSTRAINT runtime_setting_key_check
    CHECK (key IN ('objective_mode','reasoning_mode','reasoning_context_enabled','reasoning_trace_scope','subscription_mode'));
