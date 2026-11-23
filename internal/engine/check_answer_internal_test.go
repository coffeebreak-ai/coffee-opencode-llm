package engine

import (
	"strings"
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
)

func TestMarkMissingParts(t *testing.T) {
	tests := []struct {
		name    string
		state   map[string]any
		context bool
		request bool
		answer  bool
	}{
		{
			name:    "all parts present",
			state:   map[string]any{"context": "c", "request": "r", "answer": "a"},
			context: false, request: false, answer: false,
		},
		{
			name:    "context and request omitted",
			state:   map[string]any{"answer": "a"},
			context: true, request: true, answer: false,
		},
		{
			name:    "answer omitted",
			state:   map[string]any{"context": "c", "request": "r"},
			context: false, request: false, answer: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markMissingParts(test.state)
			for key, want := range map[string]bool{
				"context_missing": test.context,
				"request_missing": test.request,
				"answer_missing":  test.answer,
			} {
				got, _ := test.state[key].(bool)
				if got != want {
					t.Fatalf("%s = %v, want %v", key, got, want)
				}
			}
		})
	}
}

func TestMarkMissingPartsAfterFitState(t *testing.T) {
	rt := router.New(nil, nil, nil, nil, router.Options{StateChars: 200})
	state := map[string]any{
		"context": strings.Repeat("c", 400),
		"request": strings.Repeat("r", 400),
		"answer":  strings.Repeat("a", 100),
	}
	markMissingParts(state)
	fit, ok := rt.FitState(state, "context", "request")
	if !ok {
		t.Fatal("FitState could not fit even the answer")
	}
	markMissingParts(fit)
	context, _ := fit["context_missing"].(bool)
	request, _ := fit["request_missing"].(bool)
	answer, _ := fit["answer_missing"].(bool)
	if !context || !request || answer {
		t.Fatalf("missing flags = context %v, request %v, answer %v; want true, true, false",
			context, request, answer)
	}
}
