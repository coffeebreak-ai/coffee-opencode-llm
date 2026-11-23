package engine

import (
	"strings"
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

func TestCompactTail(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		limit int64
		want  string
	}{
		{"keeps the end", "abcdef", 3, "def"},
		{"shorter than limit", "abc", 5, "abc"},
		{"equal to limit", "abc", 3, "abc"},
		{"empty", "", 3, ""},
		{"non-positive limit", "abc", 0, "abc"},
		{"runes, not bytes", "привет", 3, "вет"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := compactTail(test.in, test.limit); got != test.want {
				t.Fatalf("compactTail(%q, %d) = %q, want %q", test.in, test.limit, got, test.want)
			}
		})
	}
}

func TestCompactTextStillKeepsHead(t *testing.T) {
	if got := compactText("abcdef", 3); got != "abc" {
		t.Fatalf("compactText = %q, want %q", got, "abc")
	}
}

func TestJevContextKeepsHumanContextAndDropsAssistantWorkText(t *testing.T) {
	tm := &turn{
		input: Input{Messages: []transport.Message{
			transport.TextMessage("user", strings.Repeat("A", 200)+"FIRST-TAIL"),
			transport.TextMessage("assistant", strings.Repeat("B", 200)+"SECOND-TAIL"),
		}},
		engine: &Engine{opts: Options{CompactEvidenceChars: 40, CompactPacketChars: 90}},
	}
	got := tm.jevContext()
	if !strings.HasSuffix(got, "FIRST-TAIL") {
		t.Fatalf("the most recent human context is lost: %q", got)
	}
	if strings.Contains(got, "SECOND-TAIL") || strings.Contains(got, strings.Repeat("B", 100)) {
		t.Fatalf("assistant work text reached Jev: %q", got)
	}
}

func TestLatestHumanKeepsRecentTail(t *testing.T) {
	tm := &turn{
		input: Input{Messages: []transport.Message{
			transport.TextMessage("user", strings.Repeat("A", 300)+"LATEST-TAIL"),
		}},
		engine: &Engine{opts: Options{CompactPacketChars: 60}},
	}
	got := tm.latestHuman()
	if !strings.HasSuffix(got, "LATEST-TAIL") {
		t.Fatalf("the latest human message end is lost: %q", got)
	}
	if strings.Contains(got, strings.Repeat("A", 100)) {
		t.Fatalf("the stale head survived: %q", got)
	}
}

func TestDeflateDecisionDropsAssistantWorkTextButKeepsActionEvidence(t *testing.T) {
	tm := &turn{
		input: Input{Messages: []transport.Message{
			transport.TextMessage("user", "human request"),
			{
				Role: "assistant",
				ToolCalls: []transport.ToolCall{{
					ID: "call_1", Type: "function",
					Function: transport.FunctionCall{Name: "read", Arguments: `{"path":"a"}`},
				}},
				Content: transport.TextContent("internal reasoning"),
			},
			{Role: "tool", ToolCallID: "call_1", Content: transport.TextContent("current result")},
		}},
		engine: &Engine{opts: Options{CompactPacketChars: 100}},
	}
	state := tm.deflateDecisionState("call_1", "current result")
	evidence, _ := state["evidence"].(string)
	if strings.Contains(evidence, "internal reasoning") {
		t.Fatalf("assistant work text reached the adjuster: %q", evidence)
	}
	for _, want := range []string{"human request", `read({"path":"a"})`, "current result"} {
		if !strings.Contains(evidence, want) {
			t.Fatalf("deflation evidence is missing %q: %q", want, evidence)
		}
	}
}
