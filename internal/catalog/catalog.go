package catalog

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type Quota struct {
	Group string `json:"group" yaml:"group"`
	RPM   *int   `json:"rpm" yaml:"rpm"`
	Day   *int   `json:"day" yaml:"day"`
	Month *int   `json:"month" yaml:"month"`
}

type Model struct {
	ID             string   `json:"id" yaml:"id"`
	Roles          []string `json:"roles" yaml:"roles"`
	Groups         []string `json:"groups" yaml:"groups"`
	ContextTokens  int      `json:"context_tokens" yaml:"context_tokens"`
	MaxOutput      int      `json:"max_output_tokens" yaml:"max_output_tokens"`
	Effort         []string `json:"effort" yaml:"effort"`
	Modalities     []string `json:"modalities" yaml:"modalities"`
	Features       []string `json:"features" yaml:"features"`
	CostInPerMTok  *float64 `json:"cost_in_per_mtok" yaml:"cost_in_per_mtok"`
	CostOutPerMTok *float64 `json:"cost_out_per_mtok" yaml:"cost_out_per_mtok"`
	QualityTier    string   `json:"quality_tier" yaml:"quality_tier"`
	Retention      string   `json:"retention" yaml:"retention"`
	Quota          Quota    `json:"quota" yaml:"quota"`
	Strengths      string   `json:"strengths" yaml:"strengths"`
	Limits         string   `json:"limits" yaml:"limits"`
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	// GroupSources records where each rotation label came from: profile or user.
	GroupSources map[string]string `json:"-" yaml:"-"`
}

type Defaults struct {
	Expert    string `yaml:"expert"`
	Assistant string `yaml:"assistant"`
	Scope     string `yaml:"scope"`
}

type file struct {
	Upstream struct {
		BaseURLEnv string `yaml:"base_url_env"`
		KeyEnv     string `yaml:"key_env"`
	} `yaml:"upstream"`
	Defaults      Defaults                    `yaml:"defaults"`
	QuotaProfiles map[string]map[string]Quota `yaml:"quota_profiles"`
	Models        []Model                     `yaml:"models"`
}

type expertsFile struct {
	CoffeeExpert    []string `yaml:"coffee-expert"`
	CoffeeAssistant []string `yaml:"coffee-assistant"`
	Enabled         []string `yaml:"enabled"`
	Disabled        []string `yaml:"disabled"`
}

type Catalog struct {
	Models   []Model
	Defaults Defaults
	byID     map[string]int
}

func Load(modelsPath, expertsPath, profile string) (*Catalog, error) {
	raw, err := os.ReadFile(modelsPath) //nolint:gosec // modelsPath comes from validated startup config
	if err != nil {
		return nil, fmt.Errorf("read models %s: %w", modelsPath, err)
	}
	var src file
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&src); err != nil {
		return nil, fmt.Errorf("decode models %s: %w", modelsPath, err)
	}
	if len(src.Models) == 0 {
		return nil, errors.New("catalog: models list is empty")
	}
	overrides := src.QuotaProfiles[profile]
	for i := range src.Models {
		model := &src.Models[i]
		if quota, ok := overrides[model.ID]; ok {
			model.Quota = quota
		}
		if err := validateModel(model); err != nil {
			return nil, err
		}
	}
	// ADR 0015: an explicitly enabled paid model no longer aborts startup.
	// Availability for the current plan is filtered by internal/adapter.FilterAvailable.
	cat := &Catalog{Models: src.Models, Defaults: src.Defaults, byID: make(map[string]int)}
	for i := range cat.Models {
		model := &cat.Models[i]
		if _, exists := cat.byID[model.ID]; exists {
			return nil, fmt.Errorf("catalog: duplicate model id %q", model.ID)
		}
		cat.byID[model.ID] = i
		model.GroupSources = make(map[string]string, len(model.Groups))
		for _, group := range model.Groups {
			model.GroupSources[group] = "profile"
		}
	}
	if err := cat.applyExperts(expertsPath); err != nil {
		return nil, err
	}
	if err := cat.validate(); err != nil {
		return nil, err
	}
	return cat, nil
}

func validateModel(model *Model) error {
	if strings.TrimSpace(model.ID) == "" {
		return errors.New("catalog: model id is required")
	}
	if len(model.Roles) == 0 {
		return fmt.Errorf("catalog: model %q has empty roles", model.ID)
	}
	for _, role := range model.Roles {
		if role != "expert" && role != "assistant" {
			return fmt.Errorf("catalog: model %q has unknown role %q", model.ID, role)
		}
	}
	for _, group := range model.Groups {
		if group != "coffee-expert" && group != "coffee-assistant" {
			return fmt.Errorf("catalog: model %q has unknown group %q", model.ID, group)
		}
	}
	if model.ContextTokens <= 0 {
		return fmt.Errorf("catalog: model %q context_tokens must be positive", model.ID)
	}
	if model.MaxOutput <= 0 {
		return fmt.Errorf("catalog: model %q max_output_tokens must be positive", model.ID)
	}
	for _, effort := range model.Effort {
		switch effort {
		case "minimal", "low", "medium", "high", "max":
		default:
			return fmt.Errorf("catalog: model %q has unknown effort %q", model.ID, effort)
		}
	}
	if !contains(model.Modalities, "text") {
		return fmt.Errorf("catalog: model %q must declare text modality", model.ID)
	}
	for _, modality := range model.Modalities {
		if modality != "text" && modality != "image" && modality != "audio" {
			return fmt.Errorf("catalog: model %q has unknown modality %q", model.ID, modality)
		}
	}
	for _, feature := range model.Features {
		if feature != "tools" && feature != "streaming" {
			return fmt.Errorf("catalog: model %q has unknown feature %q", model.ID, feature)
		}
	}
	if model.CostInPerMTok != nil && *model.CostInPerMTok < 0 {
		return fmt.Errorf("catalog: model %q cost_in_per_mtok must be non-negative", model.ID)
	}
	if model.CostOutPerMTok != nil && *model.CostOutPerMTok < 0 {
		return fmt.Errorf("catalog: model %q cost_out_per_mtok must be non-negative", model.ID)
	}
	switch model.QualityTier {
	case "high", "medium", "low":
	default:
		return fmt.Errorf("catalog: model %q quality_tier is invalid", model.ID)
	}
	if strings.TrimSpace(model.Strengths) == "" || strings.TrimSpace(model.Limits) == "" {
		return fmt.Errorf("catalog: model %q strengths and limits are required", model.ID)
	}
	if len(model.Strengths) > 240 || len(model.Limits) > 240 {
		return fmt.Errorf("catalog: model %q strengths or limits exceed 240 characters", model.ID)
	}
	return nil
}

func (c *Catalog) applyExperts(path string) error {
	raw, err := os.ReadFile(path) //nolint:gosec // path is a trusted, validated catalog path
	if err != nil {
		return fmt.Errorf("read experts %s: %w", path, err)
	}
	var src expertsFile
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&src); err != nil {
		return fmt.Errorf("decode experts %s: %w", path, err)
	}
	for _, id := range src.CoffeeExpert {
		if err := c.addGroup(id, "coffee-expert", "user"); err != nil {
			return err
		}
	}
	for _, id := range src.CoffeeAssistant {
		if err := c.addGroup(id, "coffee-assistant", "user"); err != nil {
			return err
		}
	}
	enabled := make(map[string]bool)
	for _, id := range src.Enabled {
		if _, ok := c.byID[id]; !ok {
			return fmt.Errorf("catalog: experts enabled has unknown id %q", id)
		}
		enabled[id] = true
	}
	disabled := make(map[string]bool)
	for _, id := range src.Disabled {
		if _, ok := c.byID[id]; !ok {
			return fmt.Errorf("catalog: experts disabled has unknown id %q", id)
		}
		if enabled[id] {
			return fmt.Errorf("catalog: model %q is both enabled and disabled", id)
		}
		disabled[id] = true
	}
	for i := range c.Models {
		model := &c.Models[i]
		if enabled[model.ID] {
			model.Enabled = true
		}
		if disabled[model.ID] {
			model.Enabled = false
		}
	}
	return nil
}

func (c *Catalog) addGroup(id, group, source string) error {
	idx, ok := c.byID[id]
	if !ok {
		return fmt.Errorf("catalog: experts file has unknown id %q", id)
	}
	if c.Models[idx].GroupSources == nil {
		c.Models[idx].GroupSources = make(map[string]string)
	}
	if !contains(c.Models[idx].Groups, group) {
		c.Models[idx].Groups = append(c.Models[idx].Groups, group)
	}
	c.Models[idx].GroupSources[group] = source
	return nil
}

func (c *Catalog) validate() error {
	experts := 0
	for _, model := range c.Models {
		if model.Enabled && contains(model.Groups, "coffee-expert") {
			experts++
		}
	}
	if experts < 2 {
		return fmt.Errorf("catalog: at least two active coffee-expert models are required, got %d", experts)
	}
	expert, ok := c.Get(c.Defaults.Expert)
	if !ok || !contains(expert.Roles, "expert") {
		return fmt.Errorf("catalog: defaults.expert %q is missing or has no expert role", c.Defaults.Expert)
	}
	assistant, ok := c.Get(c.Defaults.Assistant)
	if !ok || !contains(assistant.Roles, "assistant") {
		return fmt.Errorf("catalog: defaults.assistant %q is missing or has no assistant role", c.Defaults.Assistant)
	}
	switch c.Defaults.Scope {
	case "search", "read", "external", "write", "full":
	default:
		return fmt.Errorf("catalog: defaults.scope %q is invalid", c.Defaults.Scope)
	}
	return nil
}

func (c *Catalog) Get(id string) (Model, bool) {
	idx, ok := c.byID[id]
	if !ok {
		var zero Model
		return zero, false
	}
	return c.Models[idx], true
}

func (c *Catalog) ByRole(role string) []Model {
	out := make([]Model, 0, len(c.Models))
	for _, model := range c.Models {
		if contains(model.Roles, role) {
			out = append(out, model)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Catalog) HasGroup(id, group string) bool {
	model, ok := c.Get(id)
	if !ok {
		return false
	}
	return contains(model.Groups, group)
}

func (c *Catalog) FreePool() []Model {
	out := make([]Model, 0, len(c.Models))
	for _, model := range c.Models {
		if model.Enabled && model.IsFree() {
			out = append(out, model)
		}
	}
	return out
}

// IsFree reports whether the model costs nothing on both input and output.
func (m Model) IsFree() bool {
	return m.CostInPerMTok != nil && m.CostOutPerMTok != nil &&
		*m.CostInPerMTok == 0 && *m.CostOutPerMTok == 0
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
