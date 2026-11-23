package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type sanitizedDelivery struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Choices []struct {
		Message struct {
			Role      string `json:"role"`
			Content   string `json:"content"`
			ToolCalls []struct {
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Output []struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"output"`
}

func TestSanitizeDeliveryBody(t *testing.T) {
	const marker = "\u003c/think:6124c78e\u003e"
	original := map[string]any{
		"id":      "chatcmpl-1",
		"created": json.Number("1795187760127"),
		"choices": []any{map[string]any{
			"message": map[string]any{
				"role":    "assistant",
				"content": "hello " + marker + " world",
				"tool_calls": []any{map[string]any{
					"function": map[string]any{"arguments": "keep " + marker},
				}},
			},
		}},
		"output": []any{map[string]any{
			"content": []any{map[string]any{"type": "output_text", "text": "answer " + marker}},
		}},
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	cleaned, err := sanitizeDeliveryBody(raw)
	if err != nil {
		t.Fatalf("sanitize: %v", err)
	}

	var decoded sanitizedDelivery
	if err := json.Unmarshal(cleaned, &decoded); err != nil {
		t.Fatalf("decode cleaned body: %v", err)
	}
	if len(decoded.Choices) != 1 {
		t.Fatalf("choices = %d", len(decoded.Choices))
	}
	if got := decoded.Choices[0].Message.Content; got != "hello  world" {
		t.Fatalf("message content = %q, want %q", got, "hello  world")
	}
	if len(decoded.Choices[0].Message.ToolCalls) != 1 {
		t.Fatal("tool calls are missing")
	}
	if got := decoded.Choices[0].Message.ToolCalls[0].Function.Arguments; got != "keep "+marker {
		t.Fatalf("tool arguments were changed: %q", got)
	}
	if len(decoded.Output) != 1 || len(decoded.Output[0].Content) != 1 {
		t.Fatal("responses output is missing")
	}
	if got := decoded.Output[0].Content[0].Text; got != "answer " {
		t.Fatalf("responses output text = %q, want %q", got, "answer ")
	}
	if !strings.Contains(string(cleaned), "1795187760127") {
		t.Fatalf("numeric literal was changed: %s", cleaned)
	}
}

func TestSanitizeDeliveryBodyRejectsInvalidJSON(t *testing.T) {
	if _, err := sanitizeDeliveryBody([]byte("not json")); err == nil {
		t.Fatal("invalid stored body must return an error")
	}
}

func TestWriteStoredDeliverySanitizes(t *testing.T) {
	const marker = "\u003c/think:6124c78e\u003e"
	stale, err := json.Marshal(map[string]any{
		"id":      "chatcmpl-1",
		"created": 1795187760127,
		"choices": []any{map[string]any{
			"message": map[string]any{
				"role":    "assistant",
				"content": "hello " + marker + " world",
			},
		}},
	})
	if err != nil {
		t.Fatalf("marshal stale delivery: %v", err)
	}
	s := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w := httptest.NewRecorder()
	s.writeStoredDelivery(context.Background(), w, string(stale))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	if strings.Contains(w.Body.String(), "6124c78e") {
		t.Fatalf("stored marker leaked on replay: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "hello  world") {
		t.Fatalf("answer is missing: %s", w.Body.String())
	}
}
