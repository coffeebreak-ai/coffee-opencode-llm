package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/config"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

const serveModelsFixture = `defaults:
  expert: free-a
  assistant: free-a
  scope: full
models:
  - id: free-a
    roles: [expert, assistant]
    groups: [coffee-expert, coffee-assistant]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: high
    quota: {}
    strengths: "test"
    limits: "test"
    enabled: true
  - id: free-b
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: {}
    strengths: "test"
    limits: "test"
    enabled: true
`

const serveExpertsFixture = `coffee-expert: []
coffee-assistant: []
enabled: []
disabled: []
`

type startupFixture struct {
	cfg *config.Config
	cat *catalog.Catalog
	st  *store.Store
	log *slog.Logger
}

func newStartupFixture(t *testing.T) *startupFixture {
	t.Helper()
	dir := t.TempDir()
	modelsPath := filepath.Join(dir, "models.yaml")
	expertsPath := filepath.Join(dir, "experts.yaml")
	if err := os.WriteFile(modelsPath, []byte(serveModelsFixture), 0o600); err != nil {
		t.Fatalf("write models: %v", err)
	}
	if err := os.WriteFile(expertsPath, []byte(serveExpertsFixture), 0o600); err != nil {
		t.Fatalf("write experts: %v", err)
	}
	cat, err := catalog.Load(modelsPath, expertsPath, "free")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(dir, "coffee-opencode-llm.db"), store.Options{
		BusyTimeoutMS: 5000,
		MaxOpenConns:  4,
		MaxIdleConns:  2,
		WriteConns:    1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.UpsertCatalog(ctx, catalogRows(cat.Models)); err != nil {
		t.Fatalf("upsert catalog: %v", err)
	}
	cfg := &config.Config{
		DataPath: dir,
		Limits: config.Limits{
			ConnectTimeoutMS:         1000,
			UpstreamTimeoutMS:        5000,
			MaxUpstreamResponseBytes: 1 << 20,
			StreamMaxChunkBytes:      1 << 16,
			SmokeDisableMS:           3600000,
			SmokeMaxTokens:           8,
			StartupSmoke:             1,
		},
	}
	return &startupFixture{
		cfg: cfg,
		cat: cat,
		st:  st,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func (f *startupFixture) transport(url string) *transport.Transport {
	return transport.New(transport.Options{
		BaseURL:           url,
		ConnectTimeout:    time.Second,
		UpstreamTimeout:   5 * time.Second,
		MaxResponseBytes:  1 << 20,
		MaxChunkBytes:     1 << 16,
		FirstChunkTimeout: 2 * time.Second,
		IdleTimeout:       2 * time.Second,
	})
}

func (f *startupFixture) availability(t *testing.T, modelID string) store.Availability {
	t.Helper()
	state, err := f.st.Availability(context.Background())
	if err != nil {
		t.Fatalf("availability: %v", err)
	}
	return state[modelID]
}

func TestApplyUpstreamCatalogDisablesMissingModel(t *testing.T) {
	f := newStartupFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"free-a"}]}`)
	}))
	t.Cleanup(server.Close)

	applyUpstreamCatalog(context.Background(), f.cfg, f.cat, f.st, f.transport(server.URL), f.log)

	snapshot, err := readSnapshot(snapshotPath(f.cfg))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(snapshot.Models) != 1 || snapshot.Models[0] != "free-a" {
		t.Fatalf("snapshot models = %v, want [free-a]", snapshot.Models)
	}
	missing := f.availability(t, "free-b")
	if missing.DisabledUntilMS == nil {
		t.Fatalf("free-b must be disabled, state = %+v", missing)
	}
	if missing.DisabledReason != "missing_upstream" {
		t.Fatalf("reason = %q, want missing_upstream", missing.DisabledReason)
	}
	served := f.availability(t, "free-a")
	if served.DisabledUntilMS != nil {
		t.Fatalf("free-a must stay enabled, state = %+v", served)
	}
}

func TestApplyUpstreamCatalogKeepsWorkingOnFailure(t *testing.T) {
	f := newStartupFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	applyUpstreamCatalog(context.Background(), f.cfg, f.cat, f.st, f.transport(server.URL), f.log)

	if _, err := os.Stat(snapshotPath(f.cfg)); !os.IsNotExist(err) {
		t.Fatalf("snapshot must not be written on failure, stat error = %v", err)
	}
	for _, id := range []string{"free-a", "free-b"} {
		if state := f.availability(t, id); state.DisabledUntilMS != nil {
			t.Fatalf("%s must stay enabled after an upstream failure: %+v", id, state)
		}
	}
}

func TestStartupSmokeDisablesModelOnFailure(t *testing.T) {
	f := newStartupFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	runStartupSmoke(context.Background(), f.cfg, f.cat, f.st, f.transport(server.URL), f.log)

	state := f.availability(t, "free-a")
	if state.DisabledUntilMS == nil {
		t.Fatalf("free-a must be disabled after a failed smoke call: %+v", state)
	}
	if state.DisabledReason != "smoke_failed" {
		t.Fatalf("reason = %q, want smoke_failed", state.DisabledReason)
	}
}

func TestStartupSmokePassesAndResetsErrors(t *testing.T) {
	f := newStartupFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transport.ChatResponse{
			ID:      "smoke-1",
			Object:  "chat.completion",
			Created: 1,
			Model:   "free-a",
			Choices: []transport.ChatChoice{{
				Index:        0,
				Message:      transport.TextMessage("assistant", "ok"),
				FinishReason: "stop",
			}},
		})
	}))
	t.Cleanup(server.Close)

	runStartupSmoke(context.Background(), f.cfg, f.cat, f.st, f.transport(server.URL), f.log)

	if state := f.availability(t, "free-a"); state.DisabledUntilMS != nil {
		t.Fatalf("free-a must stay enabled after a successful smoke call: %+v", state)
	}
}

func TestStartupSmokeSkipsDisabledModel(t *testing.T) {
	f := newStartupFixture(t)
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"smoke-1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
	}))
	t.Cleanup(server.Close)

	until := f.st.NowMS() + f.cfg.Limits.SmokeDisableMS
	if err := f.st.DisableModel(context.Background(), "free-a", "smoke_failed", until); err != nil {
		t.Fatalf("disable model: %v", err)
	}
	runStartupSmoke(context.Background(), f.cfg, f.cat, f.st, f.transport(server.URL), f.log)
	if called {
		t.Fatal("the smoke call must be skipped while the model is disabled")
	}
}
