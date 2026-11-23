package transport

import (
	"encoding/json"
	"testing"
)

func numPtr(value int64) *int64 { return &value }

func TestUsageReasoningTokens(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int64
	}{
		{"top level field", `{"prompt_tokens":10,"completion_tokens":5,"reasoning_tokens":3}`, 3},
		{"completion details", `{"completion_tokens":5,"completion_tokens_details":{"reasoning_tokens":7}}`, 7},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var usage Usage
			if err := json.Unmarshal([]byte(test.raw), &usage); err != nil {
				t.Fatalf("decode usage: %v", err)
			}
			got := usage.Reasoning()
			if got == nil || *got != test.want {
				t.Fatalf("Reasoning() = %v, want %d", got, test.want)
			}
		})
	}
}

func TestUsageReasoningAbsent(t *testing.T) {
	var usage Usage
	if err := json.Unmarshal([]byte(`{"completion_tokens":5}`), &usage); err != nil {
		t.Fatalf("decode usage: %v", err)
	}
	if usage.Reasoning() != nil {
		t.Fatalf("Reasoning() = %v, want nil", usage.Reasoning())
	}
}

func TestUsageMergeReasoning(t *testing.T) {
	first := &Usage{ReasoningTokens: numPtr(2)}
	second := &Usage{CompletionTokensDetails: &CompletionTokensDetails{ReasoningTokens: numPtr(3)}}
	first.Merge(second)
	if first.Reasoning() == nil || *first.Reasoning() != 5 {
		t.Fatalf("merged reasoning = %v, want 5", first.Reasoning())
	}
	if first.ReasoningTokens == nil || *first.ReasoningTokens != 5 {
		t.Fatalf("merged top-level reasoning = %v, want 5", first.ReasoningTokens)
	}
}
