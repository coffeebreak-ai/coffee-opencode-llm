package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

const expertsFixture = `coffee-expert: []
coffee-assistant: []
enabled: []
disabled: []
`

func modelsFixture(paidEnabled bool) string {
	enabled := "false"
	if paidEnabled {
		enabled = "true"
	}
	return `defaults:
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
  - id: paid-a
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
    enabled: ` + enabled + `
`
}

func loadFixture(t *testing.T, paidEnabled bool, profile string) (*Catalog, error) {
	t.Helper()
	dir := t.TempDir()
	modelsPath := filepath.Join(dir, "models.yaml")
	expertsPath := filepath.Join(dir, "experts.yaml")
	if err := os.WriteFile(modelsPath, []byte(modelsFixture(paidEnabled)), 0o600); err != nil {
		t.Fatalf("write models: %v", err)
	}
	if err := os.WriteFile(expertsPath, []byte(expertsFixture), 0o600); err != nil {
		t.Fatalf("write experts: %v", err)
	}
	return Load(modelsPath, expertsPath, profile)
}

func TestFreeProfileAcceptsExplicitlyEnabledPaidModel(t *testing.T) {
	cat, err := loadFixture(t, true, "free")
	if err != nil {
		t.Fatalf("an explicitly enabled paid model must not abort startup: %v", err)
	}
	if len(cat.Models) != 3 {
		t.Fatalf("models = %d, want 3", len(cat.Models))
	}
}

func TestFreeProfileAcceptsDisabledPaidModel(t *testing.T) {
	cat, err := loadFixture(t, false, "free")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if len(cat.FreePool()) != 2 {
		t.Fatalf("free pool = %d, want 2", len(cat.FreePool()))
	}
}

func TestPaidProfileAcceptsEnabledPaidModel(t *testing.T) {
	if _, err := loadFixture(t, true, "go"); err != nil {
		t.Fatalf("load catalog: %v", err)
	}
}
