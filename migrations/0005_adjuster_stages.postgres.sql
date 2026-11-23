-- ADR 0009 §10: the jev_* statuses are replaced by adjuster_1, adjuster_2 and
-- adjuster_3.
ALTER TABLE dialogue DROP CONSTRAINT IF EXISTS dialogue_phase_check;
ALTER TABLE journal_event DROP CONSTRAINT IF EXISTS journal_event_kind_check;

UPDATE dialogue SET phase = CASE phase
  WHEN 'user_to_jev' THEN 'user_to_adjuster_1'
  WHEN 'jev_to_assistant_1' THEN 'adjuster_1_to_assistant_1'
  WHEN 'jev_to_expert' THEN 'adjuster_1_to_expert'
  WHEN 'assistant_1_to_jev' THEN 'assistant_1_to_adjuster_2'
  WHEN 'jev_to_user' THEN 'adjuster_2_to_user'
  WHEN 'jev_no_available_model_stop' THEN 'adjuster_1_no_available_model_stop'
  WHEN 'jev_timeout_stop' THEN 'adjuster_1_timeout_stop'
  WHEN 'jev_upstream_error_stop' THEN 'adjuster_1_upstream_error_stop'
  WHEN 'jev_canceled_stop' THEN 'adjuster_1_canceled_stop'
  ELSE phase END
WHERE phase IN ('user_to_jev', 'jev_to_assistant_1', 'jev_to_expert', 'assistant_1_to_jev', 'jev_to_user', 'jev_no_available_model_stop', 'jev_timeout_stop', 'jev_upstream_error_stop', 'jev_canceled_stop');
UPDATE journal_event SET kind = CASE kind
  WHEN 'user_to_jev' THEN 'user_to_adjuster_1'
  WHEN 'jev_to_assistant_1' THEN 'adjuster_1_to_assistant_1'
  WHEN 'jev_to_expert' THEN 'adjuster_1_to_expert'
  WHEN 'assistant_1_to_jev' THEN 'assistant_1_to_adjuster_2'
  WHEN 'jev_to_user' THEN 'adjuster_2_to_user'
  WHEN 'jev_no_available_model_stop' THEN 'adjuster_1_no_available_model_stop'
  WHEN 'jev_timeout_stop' THEN 'adjuster_1_timeout_stop'
  WHEN 'jev_upstream_error_stop' THEN 'adjuster_1_upstream_error_stop'
  WHEN 'jev_canceled_stop' THEN 'adjuster_1_canceled_stop'
  ELSE kind END
WHERE kind IN ('user_to_jev', 'jev_to_assistant_1', 'jev_to_expert', 'assistant_1_to_jev', 'jev_to_user', 'jev_no_available_model_stop', 'jev_timeout_stop', 'jev_upstream_error_stop', 'jev_canceled_stop');

ALTER TABLE dialogue DROP CONSTRAINT IF EXISTS dialogue_phase_check;
ALTER TABLE dialogue ADD CONSTRAINT dialogue_phase_check CHECK (phase IN (
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
        'assistant_2_canceled_stop'));

ALTER TABLE journal_event DROP CONSTRAINT IF EXISTS journal_event_kind_check;
ALTER TABLE journal_event ADD CONSTRAINT journal_event_kind_check CHECK (kind IN (
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
        'smoke_failed','retention_applied'));
