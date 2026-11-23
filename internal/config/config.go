package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port                    int                 `yaml:"port" env:"PORT"`
	DataPath                string              `yaml:"data_path" env:"DATA_PATH"`
	LogLevel                string              `yaml:"log_level" env:"LOG_LEVEL"`
	PublicModel             string              `yaml:"public_model" env:"PUBLIC_MODEL"`
	PausePhrase             string              `yaml:"pause_phrase" env:"PAUSE_PHRASE"`
	Proxy                   int                 `yaml:"proxy" env:"PROXY"`
	OutputLimitMode         string              `yaml:"output_limit_mode" env:"OUTPUT_LIMIT_MODE"`
	ObjectiveMode           string              `yaml:"objective_mode" env:"OBJECTIVE_MODE"`
	ReasoningMode           string              `yaml:"reasoning_mode" env:"REASONING_MODE"`
	ReasoningContextEnabled int64               `yaml:"reasoning_context_enabled" env:"REASONING_CONTEXT_ENABLED"`
	ReasoningTraceScope     string              `yaml:"reasoning_trace_scope" env:"REASONING_TRACE_SCOPE"`
	SubscriptionMode        string              `yaml:"subscription_mode" env:"SUBSCRIPTION_MODE"`
	Limits                  Limits              `yaml:"limits"`
	ProgressTexts           map[string]string   `yaml:"progress_texts"`
	ProgressTools           map[string]string   `yaml:"progress_tool_classes"`
	Upstream                Upstream            `yaml:"upstream"`
	Jev                     Jev                 `yaml:"jev"`
	ToolScopes              map[string][]string `yaml:"tool_scopes"`
	Sources                 map[string]string   `yaml:"-"`
}

type Upstream struct {
	BaseURLEnv    string `yaml:"base_url_env"`
	KeyEnv        string `yaml:"key_env"`
	SessionHeader string `yaml:"session_header"`
}

type Jev struct {
	BaseURLEnv string `yaml:"base_url_env"`
	KeyEnv     string `yaml:"key_env"`
	ModelEnv   string `yaml:"model_env"`
}

type Limits struct {
	MaxRequestBytes          int64 `yaml:"max_request_bytes" env:"MAX_REQUEST_BYTES"`
	MaxUpstreamResponseBytes int64 `yaml:"max_upstream_response_bytes" env:"MAX_UPSTREAM_RESPONSE_BYTES"`
	ConnectTimeoutMS         int64 `yaml:"connect_timeout_ms" env:"CONNECT_TIMEOUT_MS"`
	UpstreamTimeoutMS        int64 `yaml:"upstream_timeout_ms" env:"UPSTREAM_TIMEOUT_MS"`
	ClientTotalTimeoutMS     int64 `yaml:"client_total_timeout_ms" env:"CLIENT_TOTAL_TIMEOUT_MS"`
	ClientToolsTimeoutMS     int64 `yaml:"client_tools_timeout_ms" env:"CLIENT_TOOLS_TIMEOUT_MS"`
	RetryAttempts            int64 `yaml:"retry_attempts" env:"RETRY_ATTEMPTS"`
	RetryBackoffMS           int64 `yaml:"retry_backoff_ms" env:"RETRY_BACKOFF_MS"`
	RetryAfterMaxMS          int64 `yaml:"retry_after_max_ms" env:"RETRY_AFTER_MAX_MS"`

	StreamFirstChunkTimeoutMS int64  `yaml:"stream_first_chunk_timeout_ms" env:"STREAM_FIRST_CHUNK_TIMEOUT_MS"`
	StreamIdleTimeoutMS       int64  `yaml:"stream_idle_timeout_ms" env:"STREAM_IDLE_TIMEOUT_MS"`
	StreamMaxChunkBytes       int64  `yaml:"stream_max_chunk_bytes" env:"STREAM_MAX_CHUNK_BYTES"`
	StreamFlushEachChunk      int64  `yaml:"stream_flush_each_chunk" env:"STREAM_FLUSH_EACH_CHUNK"`
	StreamWriteTimeoutMS      int64  `yaml:"stream_write_timeout_ms" env:"STREAM_WRITE_TIMEOUT_MS"`
	StreamTextScope           string `yaml:"stream_text_scope" env:"STREAM_TEXT_SCOPE"`

	ProgressEnabled           int64 `yaml:"progress_enabled" env:"PROGRESS_ENABLED"`
	ProgressStageEnabled      int64 `yaml:"progress_stage_enabled" env:"PROGRESS_STAGE_ENABLED"`
	ProgressDecisionEnabled   int64 `yaml:"progress_decision_enabled" env:"PROGRESS_DECISION_ENABLED"`
	ProgressDelegationEnabled int64 `yaml:"progress_delegation_enabled" env:"PROGRESS_DELEGATION_ENABLED"`
	ProgressDeflateEnabled    int64 `yaml:"progress_deflate_enabled" env:"PROGRESS_DEFLATE_ENABLED"`
	ProgressModelEnabled      int64 `yaml:"progress_model_enabled" env:"PROGRESS_MODEL_ENABLED"`
	ProgressQuotaEnabled      int64 `yaml:"progress_quota_enabled" env:"PROGRESS_QUOTA_ENABLED"`
	ProgressToolsEnabled      int64 `yaml:"progress_tools_enabled" env:"PROGRESS_TOOLS_ENABLED"`
	ProgressErrorEnabled      int64 `yaml:"progress_error_enabled" env:"PROGRESS_ERROR_ENABLED"`
	ProgressMinIntervalMS     int64 `yaml:"progress_min_interval_ms" env:"PROGRESS_MIN_INTERVAL_MS"`
	ProgressMaxEvents         int64 `yaml:"progress_max_events" env:"PROGRESS_MAX_EVENTS"`
	ProgressTextChars         int64 `yaml:"progress_text_chars" env:"PROGRESS_TEXT_CHARS"`
	ProgressIncludeRequestID  int64 `yaml:"progress_include_request_id" env:"PROGRESS_INCLUDE_REQUEST_ID"`

	MaxGatewayTurns         int64 `yaml:"max_gateway_turns" env:"MAX_GATEWAY_TURNS"`
	MaxDelegationSteps      int64 `yaml:"max_delegation_steps" env:"MAX_DELEGATION_STEPS"`
	MaxOutputTokens         int64 `yaml:"max_output_tokens" env:"MAX_OUTPUT_TOKENS"`
	CompactPacketChars      int64 `yaml:"compact_packet_chars" env:"COMPACT_PACKET_CHARS"`
	CompactEvidenceChars    int64 `yaml:"compact_evidence_chars" env:"COMPACT_EVIDENCE_CHARS"`
	DeflateThresholdChars   int64 `yaml:"deflate_threshold_chars" env:"DEFLATE_THRESHOLD_CHARS"`
	DeflateMinConfidencePct int64 `yaml:"deflate_min_confidence_pct" env:"DEFLATE_MIN_CONFIDENCE_PCT"`
	DeflatorReasoningTokens int64 `yaml:"deflator_reasoning_tokens" env:"DEFLATOR_REASONING_TOKENS"`
	ExpertDelegationOnly    int64 `yaml:"expert_delegation_only" env:"EXPERT_DELEGATION_ONLY"`
	DeflateAssistantResults int64 `yaml:"deflate_assistant_results" env:"DEFLATE_ASSISTANT_RESULTS"`
	HistoryCompactPct       int64 `yaml:"history_compact_pct" env:"HISTORY_COMPACT_PCT"`
	HistorySummaryPct       int64 `yaml:"history_summary_pct" env:"HISTORY_SUMMARY_PCT"`
	CharsPerToken           int64 `yaml:"chars_per_token" env:"CHARS_PER_TOKEN"`
	MaxTools                int64 `yaml:"max_tools" env:"MAX_TOOLS"`
	MaxMessageChars         int64 `yaml:"max_message_chars" env:"MAX_MESSAGE_CHARS"`
	MaxMediaPartBytes       int64 `yaml:"max_media_part_bytes" env:"MAX_MEDIA_PART_BYTES"`
	ObjectiveBalanceTurns   int64 `yaml:"objective_balance_window_turns" env:"OBJECTIVE_BALANCE_WINDOW_TURNS"`

	JevTimeoutMS        int64 `yaml:"jev_timeout_ms" env:"JEV_TIMEOUT_MS"`
	JevStateChars       int64 `yaml:"jev_state_chars" env:"JEV_STATE_CHARS"`
	JevInputTokens      int64 `yaml:"jev_input_tokens" env:"JEV_INPUT_TOKENS"`
	JevBytesPerToken    int64 `yaml:"jev_bytes_per_token" env:"JEV_BYTES_PER_TOKEN"`
	JevCriteriaChars    int64 `yaml:"jev_criteria_chars" env:"JEV_CRITERIA_CHARS"`
	JevMaxQuestions     int64 `yaml:"jev_max_questions" env:"JEV_MAX_QUESTIONS"`
	JevMaxCallsPerTurn  int64 `yaml:"jev_max_calls_per_turn" env:"JEV_MAX_CALLS_PER_TURN"`
	JevRetryAttempts    int64 `yaml:"jev_retry_attempts" env:"JEV_RETRY_ATTEMPTS"`
	JevMinConfidencePct int64 `yaml:"jev_min_confidence_pct" env:"JEV_MIN_CONFIDENCE_PCT"`

	QuotaReserveTTLMS   int64 `yaml:"quota_reserve_ttl_ms" env:"QUOTA_RESERVE_TTL_MS"`
	Quota429CooldownMS  int64 `yaml:"quota_429_cooldown_ms" env:"QUOTA_429_COOLDOWN_MS"`
	SessionIdleTTLHours int64 `yaml:"session_idle_ttl_hours" env:"SESSION_IDLE_TTL_HOURS"`
	RotationCooldownMS  int64 `yaml:"rotation_cooldown_ms" env:"ROTATION_COOLDOWN_MS"`
	RotationErrorThresh int64 `yaml:"rotation_error_threshold" env:"ROTATION_ERROR_THRESHOLD"`
	DialogueLockWaitMS  int64 `yaml:"dialogue_lock_wait_ms" env:"DIALOGUE_LOCK_WAIT_MS"`

	DBBusyTimeoutMS  int64 `yaml:"db_busy_timeout_ms" env:"DB_BUSY_TIMEOUT_MS"`
	DBMaxOpenConns   int64 `yaml:"db_max_open_conns" env:"DB_MAX_OPEN_CONNS"`
	DBMaxIdleConns   int64 `yaml:"db_max_idle_conns" env:"DB_MAX_IDLE_CONNS"`
	DBWriteConns     int64 `yaml:"db_write_conns" env:"DB_WRITE_CONNS"`
	RetentionDays    int64 `yaml:"retention_days" env:"RETENTION_DAYS"`
	StartupSmoke     int64 `yaml:"startup_smoke" env:"STARTUP_SMOKE"`
	SmokeDisableMS   int64 `yaml:"smoke_disable_ms" env:"SMOKE_DISABLE_MS"`
	SmokeMaxTokens   int64 `yaml:"smoke_max_tokens" env:"SMOKE_MAX_TOKENS"`
	HTTPReadHeaderMS int64 `yaml:"http_read_header_timeout_ms" env:"HTTP_READ_HEADER_TIMEOUT_MS"`
	HTTPIdleMS       int64 `yaml:"http_idle_timeout_ms" env:"HTTP_IDLE_TIMEOUT_MS"`
	HTTPShutdownMS   int64 `yaml:"http_shutdown_timeout_ms" env:"HTTP_SHUTDOWN_TIMEOUT_MS"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path comes from validated CLI arguments
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	cfg.Sources = make(map[string]string)
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	applied, err := applyEnv(reflect.ValueOf(&cfg).Elem())
	if err != nil {
		return nil, fmt.Errorf("apply environment: %w", err)
	}
	for _, name := range applied {
		cfg.Sources[name] = "env"
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyEnv(v reflect.Value) ([]string, error) {
	t := v.Type()
	var applied []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fv := v.Field(i)
		if !fv.CanSet() {
			continue
		}
		if fv.Kind() == reflect.Struct {
			nested, err := applyEnv(fv)
			if err != nil {
				return nil, err
			}
			applied = append(applied, nested...)
			continue
		}
		name := field.Tag.Get("env")
		if name == "" {
			continue
		}
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			continue
		}
		switch fv.Kind() {
		case reflect.Int, reflect.Int64:
			n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			fv.SetInt(n)
		case reflect.String:
			fv.SetString(raw)
		default:
			return nil, fmt.Errorf("%s: unsupported environment target", name)
		}
		applied = append(applied, strings.ToLower(name))
	}
	return applied, nil
}

func (c *Config) UpstreamURL() (string, error) {
	return requiredEnv(c.Upstream.BaseURLEnv)
}

func (c *Config) UpstreamKey() (string, error) {
	return requiredEnv(c.Upstream.KeyEnv)
}

func (c *Config) JevURL() (string, error) {
	return requiredEnv(c.Jev.BaseURLEnv)
}

func (c *Config) JevKey() string {
	return strings.TrimSpace(os.Getenv(c.Jev.KeyEnv))
}

func (c *Config) JevModel() (string, error) {
	return requiredEnv(c.Jev.ModelEnv)
}

func requiredEnv(name string) (string, error) {
	if name == "" {
		return "", errors.New("config: environment variable name is empty")
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("config: environment variable %s is required", name)
	}
	return value, nil
}

func (c *Config) IsProxy() bool {
	return c.Proxy == 1
}

// LimitEntry is one effective setting together with the source that provided it.
type LimitEntry struct {
	Value  any    `json:"value"`
	Source string `json:"source"`
}

// LimitEntries builds the startup map of limits and their sources. Secrets never
// reach it: only keys with an environment override tag are collected.
func (c *Config) LimitEntries() map[string]LimitEntry {
	out := make(map[string]LimitEntry)
	var collect func(reflect.Value)
	collect = func(v reflect.Value) {
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			value := v.Field(i)
			if value.Kind() == reflect.Struct {
				collect(value)
				continue
			}
			name := strings.ToLower(field.Tag.Get("env"))
			if name == "" {
				continue
			}
			source := "yaml"
			if c.Sources[name] == "env" {
				source = "env"
			}
			out[name] = LimitEntry{Value: value.Interface(), Source: source}
		}
	}
	collect(reflect.ValueOf(c).Elem())
	return out
}
