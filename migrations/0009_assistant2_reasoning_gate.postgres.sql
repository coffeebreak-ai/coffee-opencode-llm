-- Replace the reasoning trace scope with the assistant 2 gate (ADR 0025).
ALTER TABLE runtime_setting DROP CONSTRAINT runtime_setting_key_check;
DELETE FROM runtime_setting WHERE key = 'reasoning_trace_scope';
ALTER TABLE runtime_setting ADD CONSTRAINT runtime_setting_key_check
    CHECK (key IN ('objective_mode','reasoning_mode','reasoning_context_enabled','assistant2_reasoning_enabled','subscription_mode'));
