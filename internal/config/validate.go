package config

import (
	"fmt"
	"strings"
)

type bound struct {
	name  string
	value int64
	lower int64
	upper int64
}

func (c *Config) Validate() error {
	checks := []bound{
		{"max_request_bytes", c.Limits.MaxRequestBytes, 1024, 268435456},
		{"max_upstream_response_bytes", c.Limits.MaxUpstreamResponseBytes, 65536, 268435456},
		{"connect_timeout_ms", c.Limits.ConnectTimeoutMS, 100, 60000},
		{"upstream_timeout_ms", c.Limits.UpstreamTimeoutMS, 1000, 600000},
		{"client_total_timeout_ms", c.Limits.ClientTotalTimeoutMS, 1000, 3600000},
		{"client_tools_timeout_ms", c.Limits.ClientToolsTimeoutMS, 1000, 600000},
		{"retry_attempts", c.Limits.RetryAttempts, 0, 5},
		{"retry_backoff_ms", c.Limits.RetryBackoffMS, 0, 30000},
		{"retry_after_max_ms", c.Limits.RetryAfterMaxMS, 1000, 86400000},

		{"stream_first_chunk_timeout_ms", c.Limits.StreamFirstChunkTimeoutMS, 1000, 300000},
		{"stream_idle_timeout_ms", c.Limits.StreamIdleTimeoutMS, 1000, 600000},
		{"stream_max_chunk_bytes", c.Limits.StreamMaxChunkBytes, 1024, 16777216},
		{"stream_flush_each_chunk", c.Limits.StreamFlushEachChunk, 0, 1},
		{"stream_write_timeout_ms", c.Limits.StreamWriteTimeoutMS, 1000, 120000},

		{"reasoning_context_enabled", c.ReasoningContextEnabled, 0, 1},
		{"assistant2_reasoning_enabled", c.Assistant2Reasoning, 0, 1},
		{"progress_enabled", c.Limits.ProgressEnabled, 0, 1},
		{"progress_stage_enabled", c.Limits.ProgressStageEnabled, 0, 1},
		{"progress_decision_enabled", c.Limits.ProgressDecisionEnabled, 0, 1},
		{"progress_delegation_enabled", c.Limits.ProgressDelegationEnabled, 0, 1},
		{"progress_deflate_enabled", c.Limits.ProgressDeflateEnabled, 0, 1},
		{"progress_model_enabled", c.Limits.ProgressModelEnabled, 0, 1},
		{"progress_quota_enabled", c.Limits.ProgressQuotaEnabled, 0, 1},
		{"progress_tools_enabled", c.Limits.ProgressToolsEnabled, 0, 1},
		{"progress_error_enabled", c.Limits.ProgressErrorEnabled, 0, 1},
		{"progress_min_interval_ms", c.Limits.ProgressMinIntervalMS, 0, 60000},
		{"progress_max_events", c.Limits.ProgressMaxEvents, 1, 1024},
		{"progress_text_chars", c.Limits.ProgressTextChars, 32, 1024},
		{"progress_include_request_id", c.Limits.ProgressIncludeRequestID, 0, 1},

		{"max_gateway_turns", c.Limits.MaxGatewayTurns, 1, 128},
		{"max_delegation_steps", c.Limits.MaxDelegationSteps, 1, 32},
		{"max_output_tokens", c.Limits.MaxOutputTokens, 256, 1048576},
		{"compact_packet_chars", c.Limits.CompactPacketChars, 2000, 200000},
		{"compact_evidence_chars", c.Limits.CompactEvidenceChars, 1000, 200000},
		{"deflate_threshold_chars", c.Limits.DeflateThresholdChars, 1, 200000},
		{"deflate_min_confidence_pct", c.Limits.DeflateMinConfidencePct, 0, 100},
		{"deflator_reasoning_tokens", c.Limits.DeflatorReasoningTokens, 0, 1048576},
		{"expert_delegation_only", c.Limits.ExpertDelegationOnly, 0, 1},
		{"history_compact_pct", c.Limits.HistoryCompactPct, 1, 100},
		{"history_summary_pct", c.Limits.HistorySummaryPct, 1, 100},
		{"chars_per_token", c.Limits.CharsPerToken, 1, 8},
		{"max_tools", c.Limits.MaxTools, 1, 1024},
		{"max_message_chars", c.Limits.MaxMessageChars, 1000, 32000000},
		{"max_media_part_bytes", c.Limits.MaxMediaPartBytes, 1024, 268435456},
		{"objective_balance_window_turns", c.Limits.ObjectiveBalanceTurns, 1, 128},

		{"jev_timeout_ms", c.Limits.JevTimeoutMS, 1000, 300000},
		{"jev_state_chars", c.Limits.JevStateChars, 4000, 200000},
		{"jev_input_tokens", c.Limits.JevInputTokens, 1000, 32000},
		{"jev_bytes_per_token", c.Limits.JevBytesPerToken, 1, 8},
		{"jev_criteria_chars", c.Limits.JevCriteriaChars, 2000, 200000},
		{"jev_max_questions", c.Limits.JevMaxQuestions, 1, 8},
		{"jev_max_calls_per_turn", c.Limits.JevMaxCallsPerTurn, 1, 128},
		{"jev_retry_attempts", c.Limits.JevRetryAttempts, 0, 3},
		{"jev_min_confidence_pct", c.Limits.JevMinConfidencePct, 0, 100},

		{"quota_reserve_ttl_ms", c.Limits.QuotaReserveTTLMS, 1000, 3600000},
		{"quota_429_cooldown_ms", c.Limits.Quota429CooldownMS, 1000, 86400000},
		{"session_idle_ttl_hours", c.Limits.SessionIdleTTLHours, 1, 8760},
		{"rotation_cooldown_ms", c.Limits.RotationCooldownMS, 1000, 86400000},
		{"rotation_error_threshold", c.Limits.RotationErrorThresh, 1, 20},
		{"dialogue_lock_wait_ms", c.Limits.DialogueLockWaitMS, 100, 60000},

		{"db_busy_timeout_ms", c.Limits.DBBusyTimeoutMS, 100, 60000},
		{"db_max_open_conns", c.Limits.DBMaxOpenConns, 1, 64},
		{"db_max_idle_conns", c.Limits.DBMaxIdleConns, 0, 64},
		{"db_write_conns", c.Limits.DBWriteConns, 1, 4},
		{"retention_days", c.Limits.RetentionDays, 1, 3650},
		{"startup_smoke", c.Limits.StartupSmoke, 0, 1},
		{"smoke_disable_ms", c.Limits.SmokeDisableMS, 1000, 86400000},
		{"smoke_max_tokens", c.Limits.SmokeMaxTokens, 1, c.Limits.MaxOutputTokens},
		{"http_read_header_timeout_ms", c.Limits.HTTPReadHeaderMS, 1000, 60000},
		{"http_idle_timeout_ms", c.Limits.HTTPIdleMS, 1000, 600000},
		{"http_shutdown_timeout_ms", c.Limits.HTTPShutdownMS, 1000, 300000},
	}
	for _, item := range checks {
		if item.value < item.lower || item.value > item.upper {
			return fmt.Errorf("config: %s = %d is outside %d..%d", item.name, item.value, item.lower, item.upper)
		}
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("config: port = %d is outside 1..65535", c.Port)
	}
	if strings.TrimSpace(c.DataPath) == "" {
		return fmt.Errorf("config: data_path is required")
	}
	if strings.TrimSpace(c.PublicModel) == "" {
		return fmt.Errorf("config: public_model is required")
	}
	if strings.TrimSpace(c.PausePhrase) == "" {
		return fmt.Errorf("config: pause_phrase is required")
	}
	if err := oneOf("log_level", c.LogLevel, "debug", "info", "warn", "error"); err != nil {
		return err
	}
	if err := oneOf("stream_text_scope", c.Limits.StreamTextScope, "all", "final"); err != nil {
		return err
	}
	if err := validateProgressTexts(c.ProgressTexts); err != nil {
		return err
	}
	if err := validateProgressToolClasses(c.ProgressTools); err != nil {
		return err
	}
	if err := oneOf("output_limit_mode", c.OutputLimitMode, "client_first", "strict_min"); err != nil {
		return err
	}
	if err := oneOf("objective_mode", c.ObjectiveMode, "balanced", "quality", "price"); err != nil {
		return err
	}
	if err := oneOf("reasoning_mode", c.ReasoningMode, "min", "medium", "high", "max", "client"); err != nil {
		return err
	}
	if err := oneOf("subscription_mode", c.SubscriptionMode, "free", "go", "go-plus", "go-teams"); err != nil {
		return err
	}
	if c.Proxy != 0 && c.Proxy != 1 {
		return fmt.Errorf("config: proxy must be 0 or 1")
	}
	if !validHeaderName(c.Upstream.SessionHeader) {
		return fmt.Errorf("config: upstream.session_header must be an empty or a valid HTTP header name")
	}
	if c.Limits.ConnectTimeoutMS >= c.Limits.UpstreamTimeoutMS {
		return fmt.Errorf("config: connect_timeout_ms must be less than upstream_timeout_ms")
	}
	if c.Limits.UpstreamTimeoutMS >= c.Limits.ClientTotalTimeoutMS {
		return fmt.Errorf("config: upstream_timeout_ms must be less than client_total_timeout_ms")
	}
	if c.Limits.StreamFirstChunkTimeoutMS >= c.Limits.UpstreamTimeoutMS {
		return fmt.Errorf("config: stream_first_chunk_timeout_ms must be less than upstream_timeout_ms")
	}
	if c.Limits.DialogueLockWaitMS > c.Limits.ClientTotalTimeoutMS {
		return fmt.Errorf("config: dialogue_lock_wait_ms must not exceed client_total_timeout_ms")
	}
	if c.Limits.ClientToolsTimeoutMS > c.Limits.ClientTotalTimeoutMS {
		return fmt.Errorf("config: client_tools_timeout_ms must not exceed client_total_timeout_ms")
	}
	if c.Limits.RetentionDays*24 <= c.Limits.SessionIdleTTLHours {
		return fmt.Errorf("config: retention_days must outlive session_idle_ttl_hours")
	}
	if c.Limits.DBWriteConns != 1 {
		return fmt.Errorf("config: db_write_conns must be 1 for SQLite")
	}
	if strings.TrimSpace(c.Upstream.BaseURLEnv) == "" || strings.TrimSpace(c.Upstream.KeyEnv) == "" {
		return fmt.Errorf("config: upstream base_url_env and key_env are required")
	}
	if strings.TrimSpace(c.Jev.BaseURLEnv) == "" || strings.TrimSpace(c.Jev.ModelEnv) == "" {
		return fmt.Errorf("config: jev base_url_env and model_env are required")
	}
	allowedScopes := map[string]bool{"search": true, "read": true, "external": true, "write": true, "full": true}
	if len(c.ToolScopes) == 0 {
		return fmt.Errorf("config: tool_scopes is required")
	}
	for scope, names := range c.ToolScopes {
		if !allowedScopes[scope] {
			return fmt.Errorf("config: tool_scopes has unknown scope %q", scope)
		}
		if len(names) == 0 {
			return fmt.Errorf("config: tool_scopes.%s is empty", scope)
		}
	}
	for _, scope := range []string{"search", "read", "external", "write", "full"} {
		if _, ok := c.ToolScopes[scope]; !ok && scope != "full" {
			return fmt.Errorf("config: tool_scopes.%s is required", scope)
		}
	}
	return nil
}

func oneOf(name, value string, allowed ...string) error {
	for _, item := range allowed {
		if value == item {
			return nil
		}
	}
	return fmt.Errorf("config: %s = %q is not one of %s", name, value, strings.Join(allowed, ", "))
}

var progressKinds = []string{"transition", "decision", "model", "deflate", "wait", "stop", "error", "warning", "tool", "thought", "summary"}

var progressPlaceholders = map[string]bool{
	"{stage}": true, "{kind}": true, "{category}": true, "{role}": true,
	"{model_id}": true, "{level}": true, "{reason}": true, "{request_id}": true,
	"{class}": true, "{commands}": true, "{edits}": true, "{reads}": true,
	"{thoughts}": true, "{tools}": true,
}

var progressToolClasses = map[string]bool{"command": true, "edit": true, "read": true, "tool": true}

func validateProgressToolClasses(classes map[string]string) error {
	for name, class := range classes {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("config: progress_tool_classes has an empty tool name")
		}
		if !progressToolClasses[class] {
			return fmt.Errorf("config: progress_tool_classes.%s = %q is unknown", name, class)
		}
	}
	return nil
}

// validateProgressTexts keeps the visible progress text a config-owned
// dictionary: every event kind has a template and only known placeholders are
// substituted. A template cannot inject user content.
func validateProgressTexts(texts map[string]string) error {
	for _, kind := range progressKinds {
		template, ok := texts[kind]
		if !ok || strings.TrimSpace(template) == "" {
			return fmt.Errorf("config: progress_texts.%s is required", kind)
		}
		for _, token := range templatePlaceholders(template) {
			if !progressPlaceholders[token] {
				return fmt.Errorf("config: progress_texts.%s has unknown placeholder %s", kind, token)
			}
		}
	}
	return nil
}

func templatePlaceholders(template string) []string {
	var out []string
	for {
		start := strings.IndexByte(template, '{')
		if start < 0 {
			return out
		}
		end := strings.IndexByte(template[start:], '}')
		if end < 0 {
			return out
		}
		out = append(out, template[start:start+end+1])
		template = template[start+end+1:]
	}
}

// validHeaderName accepts an empty value, which turns the header off, or a
// token made of the characters RFC 7230 allows in a field name.
func validHeaderName(value string) bool {
	if value == "" {
		return true
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", r):
		default:
			return false
		}
	}
	return true
}
