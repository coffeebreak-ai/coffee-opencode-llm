package adapter_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
)

func TestCanonicalEffort(t *testing.T) {
	want := []string{"minimal", "low", "medium", "high", "max"}
	if got := adapter.CanonicalEffort(); !reflect.DeepEqual(got, want) {
		t.Fatalf("CanonicalEffort() = %v, want %v", got, want)
	}
}

func TestNearestEffort(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		mode    string
		chosen  string
		want    string
		degrade bool
	}{
		{name: "exact match", values: []string{"minimal", "high"}, mode: "client", chosen: "high", want: "high"},
		{name: "nearest above", values: []string{"minimal", "high"}, mode: "client", chosen: "medium", want: "high", degrade: true},
		{name: "nearest below", values: []string{"low", "high", "max"}, mode: "client", chosen: "minimal", want: "low", degrade: true},
		{name: "mode bounds the set", values: []string{"minimal", "low", "medium", "high", "max"}, mode: "min", chosen: "max", want: "minimal", degrade: true},
		{name: "empty list", values: nil, mode: "client", chosen: "high", want: ""},
		{name: "no client choice", values: []string{"minimal", "high"}, mode: "client", want: "minimal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, degraded := adapter.NearestEffort(test.values, test.mode, test.chosen)
			if got != test.want || degraded != test.degrade {
				t.Fatalf("NearestEffort(%v, %q, %q) = (%q, %v), want (%q, %v)",
					test.values, test.mode, test.chosen, got, degraded, test.want, test.degrade)
			}
		})
	}
}

func TestUnionCapabilities(t *testing.T) {
	models := []catalog.Model{
		{ID: "a", Enabled: true, Modalities: []string{"text", "image"}, Features: []string{"tools"}, ContextTokens: 100, MaxOutput: 20},
		{ID: "b", Enabled: true, Modalities: []string{"text", "audio"}, Features: []string{"temperature"}, ContextTokens: 300, MaxOutput: 40},
		{ID: "c", Enabled: false, Modalities: []string{"video"}, ContextTokens: 9999, MaxOutput: 9999},
	}
	manifest := adapter.UnionCapabilities(models, "rev-1", 1234, "event-1")
	if manifest.SchemaVersion != 1 || manifest.CatalogRevision != "rev-1" || manifest.UpdatedAtMS != 1234 {
		t.Fatalf("manifest metadata = %+v", manifest)
	}
	if !manifest.Capabilities.Tools || !manifest.Capabilities.Temperature {
		t.Fatalf("capabilities = %+v", manifest.Capabilities)
	}
	if !reflect.DeepEqual(manifest.Capabilities.Input, []string{"audio", "image", "text"}) {
		t.Fatalf("input = %v", manifest.Capabilities.Input)
	}
	if !reflect.DeepEqual(manifest.Capabilities.Reasoning.Effort, adapter.CanonicalEffort()) {
		t.Fatalf("effort = %v", manifest.Capabilities.Reasoning.Effort)
	}
	if manifest.Limit.Context != 300 || manifest.Limit.Output != 40 {
		t.Fatalf("limit = %+v", manifest.Limit)
	}
	if len(manifest.Variants) != len(adapter.CanonicalEffort()) {
		t.Fatalf("variants = %v", manifest.Variants)
	}
}

func TestFilterAvailable(t *testing.T) {
	paid := 1.0
	free := 0.0
	models := []catalog.Model{
		{ID: "free", Enabled: true, CostInPerMTok: &free, CostOutPerMTok: &free},
		{ID: "paid", Enabled: true, CostInPerMTok: &paid, CostOutPerMTok: &paid},
		{ID: "off", Enabled: false, CostInPerMTok: &free, CostOutPerMTok: &free},
	}
	got := adapter.FilterAvailable(models, "free", map[string]bool{"free": true, "paid": true, "off": true}, nil)
	if len(got) != 1 || got[0].ID != "free" {
		t.Fatalf("free plan = %+v", got)
	}
	got = adapter.FilterAvailable(models, "go", nil, map[string]bool{"off": true})
	if len(got) != 2 {
		t.Fatalf("go plan = %+v", got)
	}
}

func TestValidateToolChoice(t *testing.T) {
	for _, raw := range []string{`null`, `"auto"`, `"none"`, `"required"`,
		`{"type":"function","function":{"name":"read"}}`,
		`{"type":"custom","custom":{"name":"read"}}`} {
		if err := adapter.ValidateToolChoice(json.RawMessage(raw)); err != nil {
			t.Fatalf("ValidateToolChoice(%s) = %v", raw, err)
		}
	}
	for _, raw := range []string{`""`, `"bogus"`, `{"type":"function"}`, `{"type":"custom"}`, `{"type":"other"}`, `{`} {
		if err := adapter.ValidateToolChoice(json.RawMessage(raw)); err == nil {
			t.Fatalf("ValidateToolChoice(%s) = nil, want error", raw)
		}
	}
}
