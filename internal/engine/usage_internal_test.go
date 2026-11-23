package engine

import (
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

func TestUsageRowsOrderAndOmittedFields(t *testing.T) {
	input := int64(10)
	output := int64(5)
	cached := int64(3)
	tm := &turn{}
	tm.mergeUsage("assistant", &transport.Usage{
		PromptTokens:        &input,
		CompletionTokens:    &output,
		PromptTokensDetails: &transport.UsageDetails{CachedTokens: &cached},
	})
	tm.mergeUsage("deflator", nil)

	rows := tm.usageRows()
	if len(rows) != len(usageRoles) {
		t.Fatalf("rows = %d, want %d", len(rows), len(usageRoles))
	}
	for i, role := range []string{"assistant", "expert", "deflator"} {
		if rows[i].Role != role {
			t.Fatalf("row %d role = %q, want %q", i, rows[i].Role, role)
		}
	}
	if rows[0].InputTokens == nil || *rows[0].InputTokens != input {
		t.Fatalf("assistant input = %v, want %d", rows[0].InputTokens, input)
	}
	if rows[0].OutputTokens == nil || *rows[0].OutputTokens != output {
		t.Fatalf("assistant output = %v, want %d", rows[0].OutputTokens, output)
	}
	if rows[0].CachedTokens == nil || *rows[0].CachedTokens != cached {
		t.Fatalf("assistant cached = %v, want %d", rows[0].CachedTokens, cached)
	}
	for _, i := range []int{1, 2} {
		if rows[i].InputTokens != nil || rows[i].OutputTokens != nil || rows[i].CachedTokens != nil {
			t.Fatalf("row %d must omit absent provider fields: %+v", i, rows[i])
		}
	}
}

func TestMergeUsageAccumulatesByRole(t *testing.T) {
	first := int64(2)
	second := int64(3)
	tm := &turn{}
	tm.mergeUsage("expert", &transport.Usage{PromptTokens: &first})
	tm.mergeUsage("expert", &transport.Usage{PromptTokens: &second})

	rows := tm.usageRows()
	if rows[1].InputTokens == nil || *rows[1].InputTokens != first+second {
		t.Fatalf("expert input = %v, want %d", rows[1].InputTokens, first+second)
	}
	if rows[0].InputTokens != nil || rows[2].InputTokens != nil {
		t.Fatal("usage leaked into another role")
	}
}

func TestUsageRowsIncludeReasoning(t *testing.T) {
	reasoning := int64(9)
	tm := &turn{}
	tm.mergeUsage("expert", &transport.Usage{
		CompletionTokensDetails: &transport.CompletionTokensDetails{ReasoningTokens: &reasoning},
	})
	rows := tm.usageRows()
	if rows[1].ReasoningTokens == nil || *rows[1].ReasoningTokens != reasoning {
		t.Fatalf("expert reasoning = %v, want %d", rows[1].ReasoningTokens, reasoning)
	}
	if rows[0].ReasoningTokens != nil || rows[2].ReasoningTokens != nil {
		t.Fatal("reasoning leaked into another role")
	}
}
