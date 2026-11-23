package engine

import (
	"strings"
	"testing"
)

func TestDelegationMaterial(t *testing.T) {
	got := delegationMaterial("asst-a", "read", 8, "digest")
	for _, want := range []string{
		"assistant_model: asst-a",
		"assistant_role: assistant",
		"scope: read",
		"max_steps: 8",
		"digest",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("delegation material does not contain %q:\n%s", want, got)
		}
	}
}
