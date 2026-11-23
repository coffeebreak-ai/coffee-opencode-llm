package engine

import "testing"

func TestReasoningStreamSanitizerHandlesSplitTags(t *testing.T) {
	var sanitizer reasoningStreamSanitizer
	var got string
	for _, part := range []string{
		"before <thi",
		"nk:broken>private</thi",
		"nk:broken> after",
	} {
		got += sanitizer.Push(part)
	}
	got += sanitizer.Flush()
	if want := "before  after"; got != want {
		t.Fatalf("sanitized stream = %q, want %q", got, want)
	}
}

func TestReasoningStreamSanitizerDropsUnclosedTail(t *testing.T) {
	var sanitizer reasoningStreamSanitizer
	got := sanitizer.Push("safe <think>private")
	got += sanitizer.Flush()
	if want := "safe "; got != want {
		t.Fatalf("sanitized stream = %q, want %q", got, want)
	}
}

func TestReasoningStreamSanitizerRemovesSplitStrayClosingTag(t *testing.T) {
	var sanitizer reasoningStreamSanitizer
	got := sanitizer.Push("before </thi")
	got += sanitizer.Push("nk:broken> after")
	got += sanitizer.Flush()
	if want := "before  after"; got != want {
		t.Fatalf("sanitized stream = %q, want %q", got, want)
	}
}

func TestReasoningStreamSanitizerRemovesClosingTagAfterPrefix(t *testing.T) {
	var sanitizer reasoningStreamSanitizer
	got := sanitizer.Push("before </think:broken> after")
	got += sanitizer.Flush()
	if want := "before  after"; got != want {
		t.Fatalf("sanitized stream = %q, want %q", got, want)
	}
	if !sanitizer.Changed() {
		t.Fatal("sanitizer did not report the removed marker")
	}
}
