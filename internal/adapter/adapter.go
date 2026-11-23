package adapter

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
)

const (
	QuestionTool           = "question"
	ToolAssistantNeedsHelp = "assistant_needs_expert"
	ToolGatewayAssistant   = "gateway_assistant"
)

type Reasoning struct {
	Supported bool     `json:"supported"`
	Effort    []string `json:"effort"`
}

type Capabilities struct {
	Tools       bool      `json:"tools"`
	Input       []string  `json:"input"`
	Output      []string  `json:"output"`
	Temperature bool      `json:"temperature"`
	Reasoning   Reasoning `json:"reasoning"`
}

type Limit struct {
	Context int64 `json:"context"`
	Output  int64 `json:"output"`
}

type Variant struct {
	ID string `json:"id"`
}

type Manifest struct {
	SchemaVersion   int          `json:"schema_version"`
	CatalogRevision string       `json:"catalog_revision"`
	UpdatedAtMS     int64        `json:"updated_at_ms"`
	EventID         string       `json:"event_id,omitempty"`
	Capabilities    Capabilities `json:"capabilities"`
	Limit           Limit        `json:"limit"`
	Variants        []Variant    `json:"variants"`
}

// CanonicalEffort is the only effort vocabulary the gateway publishes.
func CanonicalEffort() []string {
	return []string{"minimal", "low", "medium", "high", "max"}
}

func AllowedEfforts(values []string, mode string) []string {
	order := CanonicalEffort()
	sorted := make([]string, 0, len(values))
	for _, name := range order {
		for _, value := range values {
			if value == name {
				sorted = append(sorted, value)
			}
		}
	}
	if len(sorted) == 0 {
		return nil
	}
	switch mode {
	case "client":
		return sorted
	case "min":
		return sorted[:1]
	case "max":
		return sorted[len(sorted)-1:]
	case "high":
		return sorted[len(sorted)/2:]
	default:
		return sorted[len(sorted)/2 : len(sorted)/2+1]
	}
}

// NearestEffort returns the allowed value closest to the client's choice.
// The second result reports that a substitution happened.
func NearestEffort(values []string, mode, chosen string) (string, bool) {
	allowed := AllowedEfforts(values, mode)
	if len(allowed) == 0 {
		return "", false
	}
	if chosen == "" {
		return allowed[0], false
	}
	for _, value := range allowed {
		if value == chosen {
			return chosen, false
		}
	}
	order := CanonicalEffort()
	target := indexOf(order, chosen)
	best := allowed[0]
	bestDistance := distance(order, best, target)
	for _, value := range allowed[1:] {
		if d := distance(order, value, target); d < bestDistance {
			best, bestDistance = value, d
		}
	}
	return best, true
}

func indexOf(order []string, value string) int {
	for i, item := range order {
		if item == value {
			return i
		}
	}
	return -1
}

func distance(order []string, value string, target int) int {
	index := indexOf(order, value)
	if index < 0 || target < 0 {
		return len(order) + 1
	}
	delta := index - target
	if delta < 0 {
		return -delta
	}
	return delta
}

// UnionCapabilities builds the public manifest from the active model subset.
// It never publishes model identifiers, roles, groups, quotas or prices.
func UnionCapabilities(models []catalog.Model, revision string, updatedAtMS int64, eventID string) Manifest {
	input := make(map[string]bool)
	tools := false
	temperature := false
	var maxContext, maxOutput int64
	for _, model := range models {
		if !model.Enabled {
			continue
		}
		for _, modality := range model.Modalities {
			input[modality] = true
		}
		if has(model.Features, "tools") {
			tools = true
		}
		if has(model.Features, "temperature") {
			temperature = true
		}
		if int64(model.ContextTokens) > maxContext {
			maxContext = int64(model.ContextTokens)
		}
		if int64(model.MaxOutput) > maxOutput {
			maxOutput = int64(model.MaxOutput)
		}
	}
	inputs := make([]string, 0, len(input))
	for name := range input {
		inputs = append(inputs, name)
	}
	sort.Strings(inputs)
	if len(inputs) == 0 {
		inputs = []string{"text"}
	}
	variants := make([]Variant, 0, len(CanonicalEffort()))
	for _, effort := range CanonicalEffort() {
		variants = append(variants, Variant{ID: effort})
	}
	return Manifest{
		SchemaVersion:   1,
		CatalogRevision: revision,
		UpdatedAtMS:     updatedAtMS,
		EventID:         eventID,
		Capabilities: Capabilities{
			Tools:       tools,
			Input:       inputs,
			Output:      []string{"text"},
			Temperature: temperature,
			Reasoning:   Reasoning{Supported: true, Effort: CanonicalEffort()},
		},
		Limit:    Limit{Context: maxContext, Output: maxOutput},
		Variants: variants,
	}
}

// FilterAvailable keeps models that are visible and selectable for the plan.
func FilterAvailable(models []catalog.Model, plan string, upstreamIDs, disabled map[string]bool) []catalog.Model {
	out := make([]catalog.Model, 0, len(models))
	for _, model := range models {
		if !model.Enabled || disabled[model.ID] {
			continue
		}
		if len(upstreamIDs) > 0 && !upstreamIDs[model.ID] {
			continue
		}
		if plan == "free" && !model.IsFree() {
			continue
		}
		out = append(out, model)
	}
	return out
}

// ValidateToolChoice checks the OpenAI-compatible tool_choice shape.
func ValidateToolChoice(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		switch value {
		case "auto", "none", "required":
			return nil
		default:
			return fmt.Errorf("adapter: invalid tool_choice %q", value)
		}
	}
	var object struct {
		Type     string `json:"type"`
		Function *struct {
			Name string `json:"name"`
		} `json:"function"`
		Custom *struct {
			Name string `json:"name"`
		} `json:"custom"`
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("adapter: decode tool_choice: %w", err)
	}
	switch object.Type {
	case "function":
		if object.Function == nil || object.Function.Name == "" {
			return fmt.Errorf("adapter: tool_choice function requires name")
		}
	case "custom":
		if object.Custom == nil || object.Custom.Name == "" {
			return fmt.Errorf("adapter: tool_choice custom requires name")
		}
	default:
		return fmt.Errorf("adapter: invalid tool_choice type %q", object.Type)
	}
	return nil
}

func IsVirtualTool(name string) bool {
	return name == ToolAssistantNeedsHelp || name == ToolGatewayAssistant
}

func has(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
