package engine

import (
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
)

func TestReasoningGate(t *testing.T) {
	cases := []struct {
		name    string
		turn    *turn
		kind    actorKind
		want    bool
		wantArc bool
	}{
		{"master on assistant", &turn{settings: router.Settings{ReasoningContextEnabled: true}}, actorAssistant, true, true},
		{"master on expert", &turn{settings: router.Settings{ReasoningContextEnabled: true}}, actorExpert, true, true},
		{"master on helper allowed", &turn{settings: router.Settings{ReasoningContextEnabled: true, Assistant2Reasoning: true}}, actorHelper, true, true},
		{"master on helper disabled", &turn{settings: router.Settings{ReasoningContextEnabled: true}}, actorHelper, false, false},
		{"master off assistant", &turn{settings: router.Settings{}}, actorAssistant, false, false},
		{"master off helper allowed", &turn{settings: router.Settings{Assistant2Reasoning: true}}, actorHelper, false, false},
		{"deflator never", &turn{settings: router.Settings{ReasoningContextEnabled: true, Assistant2Reasoning: true}}, actorDeflator, false, false},
	}
	for _, test := range cases {
		if got := test.turn.reasoningEnabled(test.kind); got != test.want {
			t.Errorf("%s: enabled = %v, want %v", test.name, got, test.want)
		}
		got := test.turn.reasoningTraceable(test.kind, &actorOutcome{FinishReason: "stop"})
		if got != test.wantArc {
			t.Errorf("%s: traceable = %v, want %v", test.name, got, test.wantArc)
		}
	}
	complete := &turn{settings: router.Settings{ReasoningContextEnabled: true}}
	for _, finish := range []string{"length", "content_filter"} {
		if complete.reasoningTraceable(actorExpert, &actorOutcome{FinishReason: finish}) {
			t.Errorf("%s finish must not be archived", finish)
		}
	}
}
