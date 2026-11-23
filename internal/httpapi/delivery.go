package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/observe"
)

// deliveryTextKeys limits sanitization to answer text. Tool-call arguments,
// ids and model names are data from the client and must stay byte-for-byte.
var deliveryTextKeys = map[string]bool{
	"content":           true,
	"text":              true,
	"reasoning_content": true,
	"reasoning":         true,
	"reasoning_text":    true,
}

// sanitizeDeliveryBody cleans a stored response on read. Old delivery rows may
// still hold upstream command tags; rewriting them would be a destructive data
// change, so the row stays as it is and only the emitted copy is cleaned
// (contract.md §9.2.3). Numbers are kept as json.Number so the replayed body
// keeps its original scalar literals.
func sanitizeDeliveryBody(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode stored delivery: %w", err)
	}
	cleaned, changed := sanitizeDeliveryValue(value, "")
	if !changed {
		return raw, nil
	}
	body, err := json.Marshal(cleaned)
	if err != nil {
		return nil, fmt.Errorf("encode stored delivery: %w", err)
	}
	return body, nil
}

// sanitizeDeliveryValue reports whether any answer text was changed, so a
// clean delivery is replayed byte-for-byte (contract.md §10).
func sanitizeDeliveryValue(value any, key string) (any, bool) {
	switch item := value.(type) {
	case string:
		if !deliveryTextKeys[key] {
			return item, false
		}
		cleaned := engine.SanitizeModelText(item)
		return cleaned, cleaned != item
	case []any:
		out := make([]any, len(item))
		changed := false
		for i := range item {
			var nested bool
			out[i], nested = sanitizeDeliveryValue(item[i], key)
			changed = changed || nested
		}
		return out, changed
	case map[string]any:
		out := make(map[string]any, len(item))
		changed := false
		for nestedKey, nested := range item {
			var nestedChanged bool
			out[nestedKey], nestedChanged = sanitizeDeliveryValue(nested, nestedKey)
			changed = changed || nestedChanged
		}
		return out, changed
	default:
		return value, false
	}
}

func (s *Server) writeStoredDelivery(ctx context.Context, w http.ResponseWriter, raw string) {
	body, err := sanitizeDeliveryBody([]byte(raw))
	if err != nil {
		s.log.Error("sanitize stored delivery", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
