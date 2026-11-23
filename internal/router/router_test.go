package router

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

const modelsYAML = `defaults:
  expert: model-b
  assistant: model-b
  scope: full
models:
  - id: model-a
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
    quota: { group: free, day: 1 }
    strengths: "test"
    limits: "test"
    enabled: true
  - id: model-b
    roles: [expert, assistant]
    groups: [coffee-expert, coffee-assistant]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: { group: free }
    strengths: "test"
    limits: "test"
    enabled: true
`

const expertsYAML = `coffee-expert: []
coffee-assistant: []
enabled: []
disabled: []
`

const filterModelsYAML = `defaults:
  expert: filter-plain
  assistant: filter-plain
  scope: full
models:
  - id: filter-plain
    roles: [expert, assistant]
    groups: [coffee-expert, coffee-assistant]
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
  - id: filter-rpm
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
    quota: { group: free-rpm, rpm: 1 }
    strengths: "test"
    limits: "test"
    enabled: true
  - id: filter-month
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
    quota: { group: free-month, month: 1 }
    strengths: "test"
    limits: "test"
    enabled: true
  - id: filter-paid
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 1
    cost_out_per_mtok: 2
    quality_tier: high
    quota: {}
    strengths: "test"
    limits: "test"
    enabled: true
  - id: filter-vision
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text, image]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: {}
    strengths: "test"
    limits: "test"
    enabled: true
  - id: filter-notools
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 10000
    max_output_tokens: 1000
    effort: []
    modalities: [text]
    features: []
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: {}
    strengths: "test"
    limits: "test"
    enabled: true
`

func testRouter(t *testing.T) *Router {
	t.Helper()
	return testRouterWith(t, modelsYAML, "free")
}

func testRouterWith(t *testing.T, models, profile string) *Router {
	t.Helper()
	dir := t.TempDir()
	modelsPath := filepath.Join(dir, "models.yaml")
	expertsPath := filepath.Join(dir, "experts.yaml")
	if err := os.WriteFile(modelsPath, []byte(models), 0o600); err != nil {
		t.Fatalf("write models: %v", err)
	}
	if err := os.WriteFile(expertsPath, []byte(expertsYAML), 0o600); err != nil {
		t.Fatalf("write experts: %v", err)
	}
	cat, err := catalog.Load(modelsPath, expertsPath, profile)
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
	if err := st.UpsertCatalog(ctx, storeRows(cat.Models)); err != nil {
		t.Fatalf("upsert catalog: %v", err)
	}
	return New(cat, st, ledger.New(st, time.Hour.Milliseconds()), nil, Options{
		DefaultExpert:    "model-b",
		DefaultAssistant: "model-b",
		DefaultScope:     "full",
	})
}

func TestCandidatesExcludeExhaustedQuota(t *testing.T) {
	ctx := context.Background()
	rt := testRouter(t)
	model, ok := rt.catalog.Get("model-a")
	if !ok {
		t.Fatal("model-a is missing")
	}
	reservation, err := rt.store.Reserve(ctx, "req-1", model.ID, storeQuota(model.Quota))
	if err != nil {
		t.Fatalf("reserve model-a: %v", err)
	}
	if err := rt.store.Commit(ctx, *reservation); err != nil {
		t.Fatalf("commit model-a: %v", err)
	}
	candidates, err := rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Model.ID != "model-b" {
		t.Fatalf("candidates = %+v, want only model-b", candidates)
	}
}

func TestCandidatesExcludeManuallyExcluded(t *testing.T) {
	ctx := context.Background()
	rt := testRouter(t)
	if err := rt.store.SetManualExcluded(ctx, "model-b", true); err != nil {
		t.Fatalf("exclude model-b: %v", err)
	}
	candidates, err := rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.Model.ID == "model-b" {
			t.Fatalf("manually excluded model is still a candidate: %+v", candidate)
		}
	}
}

func exhaust(t *testing.T, rt *Router, modelID string) {
	t.Helper()
	ctx := context.Background()
	model, ok := rt.catalog.Get(modelID)
	if !ok {
		t.Fatalf("%s is missing", modelID)
	}
	reservation, err := rt.store.Reserve(ctx, "req-"+modelID, model.ID, storeQuota(model.Quota))
	if err != nil {
		t.Fatalf("reserve %s: %v", modelID, err)
	}
	if err := rt.store.Commit(ctx, *reservation); err != nil {
		t.Fatalf("commit %s: %v", modelID, err)
	}
}

func candidateIDs(candidates []Candidate) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.Model.ID)
	}
	return out
}

func TestCandidatesRespectQuotaWindows(t *testing.T) {
	ctx := context.Background()
	rt := testRouterWith(t, filterModelsYAML, "go")

	candidates, err := rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	windows := make(map[string]Candidate)
	for _, candidate := range candidates {
		windows[candidate.Model.ID] = candidate
	}
	if item, ok := windows["filter-rpm"]; !ok || item.Window != "rpm" || item.Limit == nil || *item.Limit != 1 {
		t.Fatalf("filter-rpm window = %+v", item)
	}
	if item, ok := windows["filter-month"]; !ok || item.Window != "month" || item.Limit == nil || *item.Limit != 1 {
		t.Fatalf("filter-month window = %+v", item)
	}

	exhaust(t, rt, "filter-rpm")
	candidates, err = rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates after rpm: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.Model.ID == "filter-rpm" {
			t.Fatalf("exhausted rpm window is still a candidate: %+v", candidate)
		}
	}

	exhaust(t, rt, "filter-month")
	candidates, err = rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates after month: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.Model.ID == "filter-month" {
			t.Fatalf("exhausted month window is still a candidate: %+v", candidate)
		}
	}
}

func TestCandidatesFreeOnly(t *testing.T) {
	ctx := context.Background()
	rt := testRouterWith(t, filterModelsYAML, "go")

	all, err := rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	found := false
	for _, candidate := range all {
		if candidate.Model.ID == "filter-paid" {
			found = true
		}
	}
	if !found {
		t.Fatalf("filter-paid must be a candidate without FreeOnly: %v", candidateIDs(all))
	}

	free, err := rt.Candidates(ctx, Filter{Role: "expert", FreeOnly: true})
	if err != nil {
		t.Fatalf("free candidates: %v", err)
	}
	for _, candidate := range free {
		if candidate.Model.ID == "filter-paid" {
			t.Fatalf("paid model is a candidate under FreeOnly: %v", candidateIDs(free))
		}
	}
}

func TestCandidatesModalities(t *testing.T) {
	ctx := context.Background()
	rt := testRouterWith(t, filterModelsYAML, "go")

	candidates, err := rt.Candidates(ctx, Filter{Role: "expert", Modalities: map[string]bool{"image": true}})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Model.ID != "filter-vision" {
		t.Fatalf("candidates = %v, want only filter-vision", candidateIDs(candidates))
	}
}

func TestCandidatesNeedTools(t *testing.T) {
	ctx := context.Background()
	rt := testRouterWith(t, filterModelsYAML, "go")

	candidates, err := rt.Candidates(ctx, Filter{Role: "expert", NeedTools: true})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.Model.ID == "filter-notools" {
			t.Fatalf("model without tools is a candidate under NeedTools: %v", candidateIDs(candidates))
		}
	}
}

func TestCandidatesUseRequestedDeflatorOutputReservation(t *testing.T) {
	rt := testRouter(t)
	candidates, err := rt.Candidates(context.Background(), Filter{
		Role: "expert", InputToken: 9500, OutputToken: 400,
	})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) == 0 {
		t.Fatal("candidate with the requested output reservation was rejected")
	}
}

func TestCandidatesOrdering(t *testing.T) {
	ctx := context.Background()
	rt := testRouterWith(t, filterModelsYAML, "go")

	candidates, err := rt.Candidates(ctx, Filter{Role: "expert"})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	want := []string{"filter-notools", "filter-paid", "filter-plain", "filter-vision",
		"filter-month", "filter-rpm"}
	got := candidateIDs(candidates)
	if len(got) != len(want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("candidates = %v, want %v", got, want)
		}
	}
}

// storeRows maps catalog models into plain storage rows for the fixture.
func storeRows(models []catalog.Model) []store.CatalogModel {
	rows := make([]store.CatalogModel, 0, len(models))
	for _, model := range models {
		rows = append(rows, store.CatalogModel{
			ID:             model.ID,
			ContextTokens:  int64(model.ContextTokens),
			MaxOutput:      int64(model.MaxOutput),
			CostInPerMTok:  model.CostInPerMTok,
			CostOutPerMTok: model.CostOutPerMTok,
			QualityTier:    model.QualityTier,
			Retention:      model.Retention,
			QuotaGroup:     model.Quota.Group,
			Strengths:      model.Strengths,
			Limits:         model.Limits,
			Enabled:        model.Enabled,
			Roles:          model.Roles,
			Efforts:        model.Effort,
			Modalities:     model.Modalities,
			Features:       model.Features,
			Groups:         model.GroupSources,
		})
	}
	return rows
}

// storeQuota maps a catalog quota into plain store fields for the fixture.
func storeQuota(quota catalog.Quota) store.Quota {
	return store.Quota{
		Group: quota.Group,
		RPM:   int64Ptr(quota.RPM),
		Day:   int64Ptr(quota.Day),
		Month: int64Ptr(quota.Month),
	}
}

func int64Ptr(value *int) *int64 {
	if value == nil {
		return nil
	}
	converted := int64(*value)
	return &converted
}
