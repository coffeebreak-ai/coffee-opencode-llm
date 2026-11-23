package engine

import (
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
)

func TestReasoningTraceableGate(t *testing.T) {
	base := &turn{settings: router.Settings{ReasoningTraceScope: "full"}}
	final := &turn{settings: router.Settings{ReasoningTraceScope: "final"}}
	cases := []struct {
		name    string
		turn    *turn
		kind    actorKind
		outcome *actorOutcome
		want    bool
	}{
		{"full assistant escalates", base, actorAssistant, &actorOutcome{NeedsExpert: true}, true},
		{"full helper", base, actorHelper, &actorOutcome{}, true},
		{"full deflator", base, actorDeflator, &actorOutcome{}, false},
		{"full length finish", base, actorAssistant, &actorOutcome{FinishReason: "length"}, false},
		{"full content filter", base, actorAssistant, &actorOutcome{FinishReason: "content_filter"}, false},
		{"final assistant final", final, actorAssistant, &actorOutcome{}, true},
		{"final assistant escalates", final, actorAssistant, &actorOutcome{NeedsExpert: true}, false},
		{"final assistant delegates", final, actorAssistant, &actorOutcome{Delegation: &delegationRequest{}}, false},
		{"final helper", final, actorHelper, &actorOutcome{}, false},
		{"final expert final", final, actorExpert, &actorOutcome{}, true},
		{"final expert delegates", final, actorExpert, &actorOutcome{Delegation: &delegationRequest{}}, false},
		{"final deflator", final, actorDeflator, &actorOutcome{}, false},
	}
	for _, test := range cases {
		if got := test.turn.reasoningTraceable(test.kind, test.outcome); got != test.want {
			t.Errorf("%s: traceable = %v, want %v", test.name, got, test.want)
		}
	}
}
