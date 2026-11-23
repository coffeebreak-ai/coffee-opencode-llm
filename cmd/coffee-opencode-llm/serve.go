package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/config"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/httpapi"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/jev"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/observe"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

const percentScale = 100

func runServe(args []string) error {
	var p paths
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	p.register(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := loadConfig(p.config)
	if err != nil {
		return err
	}
	log, err := observe.NewLogger(cfg.LogLevel)
	if err != nil {
		return &cliError{code: exitConfig, err: err}
	}
	ctx, cancel := signalContext()
	defer cancel()

	release, err := lockPID(filepath.Join(cfg.DataPath, "coffee-opencode-llm.pid"))
	if err != nil {
		return err
	}
	defer release()

	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	if released, err := st.ReleaseStaleReservations(ctx, cfg.Limits.QuotaReserveTTLMS); err != nil {
		log.Warn("release stale quota reservations", "error", err)
	} else if released > 0 {
		log.Info("released stale quota reservations", "rows", released)
	}

	settings, err := readEffective(ctx, cfg, st)
	if err != nil {
		return err
	}
	cat, err := loadCatalog(p, settings.Subscription.Value)
	if err != nil {
		return err
	}
	if err := st.UpsertCatalog(ctx, cat.Models); err != nil {
		return wrapDB(err, "load catalog")
	}

	baseURL, err := inferenceBaseURL(cfg, settings.Subscription.Value)
	if err != nil {
		return err
	}
	jevURL, err := cfg.JevURL()
	if err != nil {
		return &cliError{code: exitConfig, err: err}
	}
	jevModel, err := cfg.JevModel()
	if err != nil {
		return &cliError{code: exitConfig, err: err}
	}
	key, err := cfg.UpstreamKey()
	if err != nil {
		return &cliError{code: exitConfig, err: err}
	}

	tr := transport.New(transport.Options{
		BaseURL:           baseURL,
		Key:               key,
		SessionHeader:     cfg.Upstream.SessionHeader,
		ConnectTimeout:    time.Duration(cfg.Limits.ConnectTimeoutMS) * time.Millisecond,
		UpstreamTimeout:   time.Duration(cfg.Limits.UpstreamTimeoutMS) * time.Millisecond,
		MaxResponseBytes:  cfg.Limits.MaxUpstreamResponseBytes,
		MaxChunkBytes:     cfg.Limits.StreamMaxChunkBytes,
		FirstChunkTimeout: time.Duration(cfg.Limits.StreamFirstChunkTimeoutMS) * time.Millisecond,
		IdleTimeout:       time.Duration(cfg.Limits.StreamIdleTimeoutMS) * time.Millisecond,
		RetryAttempts:     int(cfg.Limits.RetryAttempts),
		RetryBackoff:      time.Duration(cfg.Limits.RetryBackoffMS) * time.Millisecond,
	})
	jevClient := jev.New(jev.Options{
		BaseURL:     jevURL,
		Key:         cfg.JevKey(),
		Model:       jevModel,
		Timeout:     time.Duration(cfg.Limits.JevTimeoutMS) * time.Millisecond,
		Retry:       int(cfg.Limits.JevRetryAttempts),
		Backoff:     time.Duration(cfg.Limits.RetryBackoffMS) * time.Millisecond,
		MaxBodySize: cfg.Limits.MaxUpstreamResponseBytes,
	})
	led := ledger.New(st, cfg.Limits.QuotaReserveTTLMS)
	rt := router.New(cat, st, led, jevClient, router.Options{
		ObjectiveMode:           settings.Objective.Value,
		ReasoningMode:           settings.Reasoning.Value,
		ReasoningContextEnabled: settings.ReasoningContext.Value == "on",
		ReasoningTraceScope:     settings.TraceScope.Value,
		SubscriptionMode:        settings.Subscription.Value,
		BalanceTurns:            cfg.Limits.ObjectiveBalanceTurns,
		CharsPerToken:           cfg.Limits.CharsPerToken,
		CriteriaChars:           cfg.Limits.JevCriteriaChars,
		StateChars:              cfg.Limits.JevStateChars,
		MaxRequestBytes:         cfg.Limits.JevInputTokens * cfg.Limits.JevBytesPerToken,
		DeflateMinConfidencePct: cfg.Limits.DeflateMinConfidencePct,
		MaxQuestions:            cfg.Limits.JevMaxQuestions,
		MaxCallsPerTurn:         cfg.Limits.JevMaxCallsPerTurn,
		MinStartConfidence:      float64(cfg.Limits.JevMinConfidencePct) / percentScale,
		DefaultExpert:           cat.Defaults.Expert,
		DefaultAssistant:        cat.Defaults.Assistant,
		DefaultScope:            cat.Defaults.Scope,
	})
	eng := engine.New(st, cat, rt, led, tr, log, engine.Options{
		PublicModel:             cfg.PublicModel,
		PausePhrase:             cfg.PausePhrase,
		StreamTextScope:         cfg.Limits.StreamTextScope,
		Proxy:                   cfg.IsProxy(),
		OutputLimitMode:         cfg.OutputLimitMode,
		MaxGatewayTurns:         cfg.Limits.MaxGatewayTurns,
		MaxDelegationSteps:      cfg.Limits.MaxDelegationSteps,
		MaxOutputTokens:         cfg.Limits.MaxOutputTokens,
		CompactPacketChars:      cfg.Limits.CompactPacketChars,
		CompactEvidenceChars:    cfg.Limits.CompactEvidenceChars,
		DeflateThresholdChars:   cfg.Limits.DeflateThresholdChars,
		DeflatorReasoningTokens: cfg.Limits.DeflatorReasoningTokens,
		ExpertDelegationOnly:    cfg.Limits.ExpertDelegationOnly == 1,
		HistoryCompactPct:       cfg.Limits.HistoryCompactPct,
		HistorySummaryPct:       cfg.Limits.HistorySummaryPct,
		CharsPerToken:           cfg.Limits.CharsPerToken,
		MaxTools:                cfg.Limits.MaxTools,
		Quota429CooldownMS:      cfg.Limits.Quota429CooldownMS,
		ClientToolsTimeoutMS:    cfg.Limits.ClientToolsTimeoutMS,
		RetryAfterMaxMS:         cfg.Limits.RetryAfterMaxMS,
		RotationCooldownMS:      cfg.Limits.RotationCooldownMS,
		RotationErrorThresh:     cfg.Limits.RotationErrorThresh,
		ObjectiveMode:           settings.Objective.Value,
		ReasoningMode:           settings.Reasoning.Value,
		SubscriptionMode:        settings.Subscription.Value,
		BalanceTurns:            cfg.Limits.ObjectiveBalanceTurns,
		JevMaxCallsPerTurn:      cfg.Limits.JevMaxCallsPerTurn,
		ToolScopes:              cfg.ToolScopes,
	})
	api := httpapi.New(eng, st, cat, log, httpapi.Options{
		PublicModel:          cfg.PublicModel,
		MaxRequestBytes:      cfg.Limits.MaxRequestBytes,
		MaxMessageChars:      cfg.Limits.MaxMessageChars,
		MaxMediaPartBytes:    cfg.Limits.MaxMediaPartBytes,
		MaxTools:             cfg.Limits.MaxTools,
		DialogueLockWaitMS:   cfg.Limits.DialogueLockWaitMS,
		ClientTotalTimeoutMS: cfg.Limits.ClientTotalTimeoutMS,
		StreamWriteTimeoutMS: cfg.Limits.StreamWriteTimeoutMS,
		SessionIdleTTLHours:  cfg.Limits.SessionIdleTTLHours,
		Progress: httpapi.ProgressOptions{
			Enabled: cfg.Limits.ProgressEnabled == 1,
			Categories: map[string]bool{
				"stage":      cfg.Limits.ProgressStageEnabled == 1,
				"decision":   cfg.Limits.ProgressDecisionEnabled == 1,
				"delegation": cfg.Limits.ProgressDelegationEnabled == 1,
				"deflate":    cfg.Limits.ProgressDeflateEnabled == 1,
				"model":      cfg.Limits.ProgressModelEnabled == 1,
				"quota":      cfg.Limits.ProgressQuotaEnabled == 1,
				"tools":      cfg.Limits.ProgressToolsEnabled == 1,
				"error":      cfg.Limits.ProgressErrorEnabled == 1,
			},
			MinIntervalMS:    cfg.Limits.ProgressMinIntervalMS,
			MaxEvents:        cfg.Limits.ProgressMaxEvents,
			TextChars:        cfg.Limits.ProgressTextChars,
			IncludeRequestID: cfg.Limits.ProgressIncludeRequestID == 1,
			Texts:            cfg.ProgressTexts,
			ToolClasses:      cfg.ProgressTools,
		},
	})

	log.Info("startup limits", "limits", cfg.LimitEntries())
	log.Info("startup settings",
		"objective_mode", settings.Objective.Value, "objective_source", settings.Objective.Source,
		"reasoning_mode", settings.Reasoning.Value, "reasoning_source", settings.Reasoning.Source,
		"subscription_mode", settings.Subscription.Value, "subscription_source", settings.Subscription.Source)
	log.Info("startup catalog", "models", len(cat.Models), "expert_default", cat.Defaults.Expert,
		"assistant_default", cat.Defaults.Assistant)
	applyUpstreamCatalog(ctx, cfg, cat, st, tr, log)
	runStartupSmoke(ctx, cfg, cat, st, tr, log)

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.Port),
		Handler:           api.Handler(),
		ReadHeaderTimeout: time.Duration(cfg.Limits.HTTPReadHeaderMS) * time.Millisecond,
		IdleTimeout:       time.Duration(cfg.Limits.HTTPIdleMS) * time.Millisecond,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-errCh:
		return &cliError{code: exitConfig, err: fmt.Errorf("serve: %w", err)}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(),
		time.Duration(cfg.Limits.HTTPShutdownMS)*time.Millisecond)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

type upstreamSnapshot struct {
	UpdatedAtMS int64    `yaml:"updated_at_ms"`
	Models      []string `yaml:"models"`
}

func snapshotPath(cfg *config.Config) string {
	return filepath.Join(cfg.DataPath, "upstream_models.yaml")
}

func readSnapshot(path string) (*upstreamSnapshot, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is resolved from the startup config
	if err != nil {
		return nil, fmt.Errorf("read upstream snapshot %s: %w", path, err)
	}
	var snap upstreamSnapshot
	if err := yaml.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("decode upstream snapshot %s: %w", path, err)
	}
	return &snap, nil
}

func writeSnapshot(path string, ids []string) error {
	raw, err := yaml.Marshal(upstreamSnapshot{UpdatedAtMS: time.Now().UTC().UnixMilli(), Models: ids})
	if err != nil {
		return fmt.Errorf("encode upstream snapshot: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write upstream snapshot: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace upstream snapshot: %w", err)
	}
	return nil
}

// applyUpstreamCatalog compares the catalog with GET /v1/models. A missing model
// is disabled for smoke_disable_ms instead of failing the start (Q14). Human
// configs are never rewritten: only the machine snapshot is updated.
func applyUpstreamCatalog(ctx context.Context, cfg *config.Config, cat *catalog.Catalog,
	st *store.Store, tr *transport.Transport, log *slog.Logger) {
	path := snapshotPath(cfg)
	ids, err := tr.ListModels(ctx)
	if err != nil {
		log.Warn("upstream catalog is unavailable, using files and the last snapshot",
			"error", err, "snapshot", path)
		if snap, readErr := readSnapshot(path); readErr == nil {
			log.Info("upstream snapshot loaded", "models", len(snap.Models),
				"updated_at_ms", snap.UpdatedAtMS)
		}
		return
	}
	if err := writeSnapshot(path, ids); err != nil {
		log.Warn("upstream snapshot is not written", "error", err, "snapshot", path)
	}
	served := make(map[string]bool, len(ids))
	for _, id := range ids {
		served[id] = true
	}
	until := st.NowMS() + cfg.Limits.SmokeDisableMS
	for _, model := range cat.Models {
		if !model.Enabled || served[model.ID] {
			continue
		}
		if err := st.DisableModel(ctx, model.ID, "missing_upstream", until); err != nil {
			log.Error("disable a model missing upstream", "model", model.ID, "error", err)
			continue
		}
		if err := st.Journal(ctx, store.JournalEvent{Level: "warn", Kind: "missing_upstream",
			Note: fmt.Sprintf("model %s is missing upstream, disabled until %d", model.ID, until)}); err != nil {
			log.Error("write the startup journal", "error", err)
		}
		log.Warn("model is missing upstream, disabled temporarily",
			"model", model.ID, "reason", "missing_upstream", "until_ms", until)
	}
	log.Info("upstream catalog checked", "models", len(ids))
}

// runStartupSmoke proves that the default assistant is callable before the first
// human turn. A failure disables the model for smoke_disable_ms and the gateway
// keeps starting (model-catalog.md §5).
func runStartupSmoke(ctx context.Context, cfg *config.Config, cat *catalog.Catalog,
	st *store.Store, tr *transport.Transport, log *slog.Logger) {
	if cfg.Limits.StartupSmoke != 1 {
		return
	}
	model, ok := cat.Get(cat.Defaults.Assistant)
	if !ok || !model.Enabled {
		return
	}
	availability, err := st.Availability(ctx)
	if err != nil {
		log.Warn("startup smoke skipped: availability is unavailable", "error", err)
		return
	}
	if state, ok := availability[model.ID]; ok && state.DisabledUntilMS != nil &&
		*state.DisabledUntilMS > st.NowMS() {
		log.Info("startup smoke skipped: the assistant model is disabled",
			"model", model.ID, "until_ms", *state.DisabledUntilMS)
		return
	}
	limit := cfg.Limits.SmokeMaxTokens
	request := transport.ChatRequest{
		Model:     model.ID,
		Messages:  []transport.Message{transport.TextMessage("user", "Reply with the single word ok.")},
		MaxTokens: &limit,
		Session:   "smoke",
	}
	response, err := tr.Do(ctx, request, true)
	if err == nil && (response == nil || len(response.Choices) == 0) {
		err = errors.New("the smoke response has no choices")
	}
	if err != nil {
		until := st.NowMS() + cfg.Limits.SmokeDisableMS
		if disableErr := st.DisableModel(ctx, model.ID, "smoke_failed", until); disableErr != nil {
			log.Error("disable the model after a failed smoke call", "model", model.ID, "error", disableErr)
		}
		if journalErr := st.Journal(ctx, store.JournalEvent{Level: "error", Kind: "smoke_failed",
			Note: fmt.Sprintf("startup smoke failed for %s, disabled until %d", model.ID, until)}); journalErr != nil {
			log.Error("write the startup journal", "error", journalErr)
		}
		log.Error("startup smoke failed, the assistant model is disabled temporarily",
			"model", model.ID, "until_ms", until, "error", err)
		return
	}
	if err := st.RecordModelSuccess(ctx, model.ID); err != nil {
		log.Error("record a successful smoke call", "model", model.ID, "error", err)
	}
	log.Info("startup smoke passed", "model", model.ID)
}

// inferenceBaseURL resolves the upstream base URL. The environment variable names
// the free-tier endpoint; paid plans use the /go path from model-limits.md §4.
func inferenceBaseURL(cfg *config.Config, mode string) (string, error) {
	base, err := cfg.UpstreamURL()
	if err != nil {
		return "", &cliError{code: exitConfig, err: err}
	}
	base = strings.TrimRight(base, "/")
	paid := mode != "free"
	switch {
	case paid && strings.HasSuffix(base, "/go/v1"):
		return base, nil
	case paid && strings.HasSuffix(base, "/v1"):
		return strings.TrimSuffix(base, "/v1") + "/go/v1", nil
	case !paid && strings.Contains(base, "/go/"):
		return strings.Replace(base, "/go/", "/", 1), nil
	default:
		return base, nil
	}
}

func lockPID(path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, wrapDB(err, "create data directory")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // path is the pid file resolved from the startup config
	if err != nil {
		return nil, wrapDB(err, "open pid file")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, failf("another coffee-opencode-llm instance holds %s", path)
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return nil, wrapDB(err, "truncate pid file")
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = file.Close()
		return nil, wrapDB(err, "seek pid file")
	}
	if _, err := fmt.Fprintf(file, "%d\n", os.Getpid()); err != nil {
		_ = file.Close()
		return nil, wrapDB(err, "write pid file")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, wrapDB(err, "sync pid file")
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		_ = os.Remove(path)
	}, nil
}
