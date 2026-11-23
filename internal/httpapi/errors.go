package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
)

// statusClientClosedRequest is the non-standard 499 code used when the client
// abandons a request. It is only reported in logs; nothing is sent back.
const statusClientClosedRequest = 499

type errorBody struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Code    string  `json:"code"`
	Param   *string `json:"param"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type failure struct {
	status       int
	code         string
	message      string
	retryAfterMS int64
}

func newFailure(status int, code string) *failure {
	return &failure{status: status, code: code, message: messageFor(code)}
}

func messageFor(code string) string {
	switch code {
	case "request_too_large":
		return "Request body is too large"
	case "invalid_json":
		return "Request body is not valid JSON"
	case "invalid_request":
		return "Request structure is invalid"
	case "invalid_session_key":
		return "Session key is too long"
	case "too_many_tools":
		return "Tool catalog is too large"
	case "not_found":
		return "Path is not found"
	case "method_not_allowed":
		return "Method is not allowed"
	case "conflict":
		return "Another request is in progress for this dialogue"
	case "message_too_large":
		return "Message or content part is too large"
	case "unsupported_content":
		return "Content part type is not supported"
	case "upstream_rate_limited":
		return "Upstream rate limit is reached"
	case "upstream_failed":
		return "Upstream request failed"
	case "upstream_timeout":
		return "Upstream request timed out"
	case "empty_answer":
		return "The model returned an empty answer"
	case "timeout":
		return "The client did not return tool results in time"
	case "no_available_model":
		return "No model is available"
	case "canceled":
		return "Request was canceled"
	default:
		return "Gateway error"
	}
}

// clientCanceled reports a request the client abandoned. It is not a gateway
// failure: nothing is written back and no model state is changed.
func clientCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

func errorType(status int) string {
	switch {
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status >= http.StatusInternalServerError:
		return "server_error"
	default:
		return "invalid_request_error"
	}
}

func writeFailure(w http.ResponseWriter, fail *failure) {
	writeErrorBody(w, fail.status, fail.code, fail.message, fail.retryAfterMS)
}

func writeErrorBody(w http.ResponseWriter, status int, code, message string, retryAfterMS int64) {
	if message == "" {
		message = messageFor(code)
	}
	if status == http.StatusTooManyRequests && retryAfterMS > 0 {
		seconds := (retryAfterMS + time.Second.Milliseconds() - 1) / time.Second.Milliseconds()
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: errorBody{
		Message: message,
		Type:    errorType(status),
		Code:    code,
	}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// engineFailure converts an engine error into the public envelope. A quota refusal
// is reported as no_available_model: the contract has no separate code for it.
func engineFailure(err error) *failure {
	var typed *engine.Error
	if !errors.As(err, &typed) {
		return newFailure(http.StatusInternalServerError, "internal")
	}
	code := typed.Code
	status := typed.Status
	if code == "no_quota" {
		code = "no_available_model"
		status = http.StatusServiceUnavailable
	}
	if status == 0 {
		status = http.StatusInternalServerError
	}
	message := typed.Message
	if message == "" {
		message = messageFor(code)
	}
	return &failure{status: status, code: code, message: message, retryAfterMS: typed.RetryAfterMS}
}
