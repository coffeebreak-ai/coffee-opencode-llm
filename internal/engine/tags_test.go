package engine_test

import (
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
)

const (
	closingMarker = "\u003c/think:6124c78e\u003e"
	openingMarker = "\u003cthink:6124c78e\u003e"
)

func TestStripCommandTags(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"stray closing marker", "hello " + closingMarker + " world", "hello  world"},
		{"repeated closing markers", "a" + closingMarker + closingMarker + "b", "ab"},
		{"complete block", openingMarker + "reasoning" + closingMarker + "answer", "answer"},
		{"unclosed block", "answer " + openingMarker + "private reasoning", "answer "},
		{"complete then unclosed block", "answer " + openingMarker + "private" + closingMarker +
			" visible " + openingMarker + "private reasoning", "answer  visible "},
		{"bare tags", "\u003cthink\u003e\u003c/think\u003eanswer", "answer"},
		{"uppercase", "a\u003c/THINK:ABC\u003eb", "ab"},
		{"marker with payload", "a\u003c/think:hash-2\u003eb", "ab"},
		{"legitimate client tags are kept", "\u003cuser_query\u003e hello \u003c/shell\u003e \u003csystem-update\u003e", "\u003cuser_query\u003e hello \u003c/shell\u003e \u003csystem-update\u003e"},
		{"plain text is unchanged", "ordinary answer", "ordinary answer"},
		{"empty", "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := engine.StripCommandTags(test.in); got != test.want {
				t.Fatalf("StripCommandTags(%q) = %q, want %q", test.in, got, test.want)
			}
		})
	}
}

func TestHasCommandTag(t *testing.T) {
	if !engine.HasCommandTag("answer " + closingMarker) {
		t.Fatal("closing marker must be detected")
	}
	if !engine.HasCommandTag(openingMarker + "reasoning") {
		t.Fatal("opening marker must be detected")
	}
	for _, text := range []string{"plain answer", "\u003cuser_query\u003e task", "\u003c/shell\u003e", "\u003ctool call\u003e"} {
		if engine.HasCommandTag(text) {
			t.Fatalf("legitimate text %q must not be flagged", text)
		}
	}
}

func TestSanitizeModelTextKeepsReasoningAndStripsReports(t *testing.T) {
	in := "real thinking\n[coffee-progress] adjuster_1_to_assistant_1\n" + closingMarker + "answer"
	want := "real thinking\nanswer"
	if got := engine.SanitizeModelText(in); got != want {
		t.Fatalf("SanitizeModelText(%q) = %q, want %q", in, got, want)
	}
}
