package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testTransport(url string) *Transport {
	return New(Options{
		BaseURL:           url,
		ConnectTimeout:    time.Second,
		UpstreamTimeout:   5 * time.Second,
		MaxResponseBytes:  1 << 20,
		MaxChunkBytes:     1 << 16,
		FirstChunkTimeout: 2 * time.Second,
		IdleTimeout:       2 * time.Second,
		RetryAttempts:     0,
	})
}

func sseServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %q, want /chat/completions", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestStreamAssemblesToolCalls(t *testing.T) {
	server := sseServer(t, `data: {"id":"c1","choices":[{"index":0,"delta":{"reasoning_content":"plan "}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{"reasoning_text":"search"}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{"content":"hel"}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{"content":"lo"}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}

data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: {"id":"c1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}

data: [DONE]

`)
	result, err := testTransport(server.URL).Stream(context.Background(),
		ChatRequest{Model: "m", Stream: true}, func(Chunk) error { return nil })
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if result.Content != "hello" {
		t.Fatalf("content = %q, want hello", result.Content)
	}
	if result.ReasoningContent != "plan search" {
		t.Fatalf("reasoning = %q, want %q", result.ReasoningContent, "plan search")
	}
	if result.FinishReason != "tool_calls" {
		t.Fatalf("finish reason = %q, want tool_calls", result.FinishReason)
	}
	if len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(result.ToolCalls))
	}
	call := result.ToolCalls[0]
	if call.ID != "call_1" || call.Function.Name != "search" || call.Function.Arguments != `{"q":"x"}` {
		t.Fatalf("assembled call = %+v", call)
	}
	if result.Usage == nil || result.Usage.TotalTokens == nil || *result.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %+v, want total 7", result.Usage)
	}
}

func TestStreamRequiresDoneMarker(t *testing.T) {
	server := sseServer(t, `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"partial"}}]}

`)
	_, err := testTransport(server.URL).Stream(context.Background(),
		ChatRequest{Model: "m", Stream: true}, func(Chunk) error { return nil })
	if !errors.Is(err, ErrBrokenStream) {
		t.Fatalf("stream error = %v, want ErrBrokenStream", err)
	}
}

func TestDoCanceledIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-release
	}))
	t.Cleanup(server.Close)

	tr := testTransport(server.URL)
	tr.retryAttempts = 3
	tr.retryBackoff = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := tr.Do(ctx, ChatRequest{Model: "m"}, true)
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream call did not start")
	}
	cancel()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("the canceled call did not return")
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "canceled" {
		t.Fatalf("code = %q, want canceled", typed.Code)
	}
	if IsRetryable(err) {
		t.Fatalf("a canceled call must not be retryable")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestStreamCanceledIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"par"}}]}`+"\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		started <- struct{}{}
		<-release
	}))
	t.Cleanup(server.Close)

	tr := testTransport(server.URL)
	tr.retryAttempts = 3
	tr.retryBackoff = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := tr.Stream(ctx, ChatRequest{Model: "m"}, func(Chunk) error { return nil })
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream call did not start")
	}
	cancel()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("the canceled stream did not return")
	}
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "canceled" {
		t.Fatalf("code = %q, want canceled", typed.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1: a canceled stream must not be retried", got)
	}
}

func TestStreamFirstChunkTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}))
	t.Cleanup(server.Close)

	tr := testTransport(server.URL)
	tr.firstChunkTimeout = 50 * time.Millisecond
	tr.idleTimeout = time.Second

	_, err := tr.Stream(context.Background(), ChatRequest{Model: "m"}, func(Chunk) error { return nil })
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "upstream_timeout" {
		t.Fatalf("code = %q, want upstream_timeout", typed.Code)
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"par"}}]}`+"\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}))
	t.Cleanup(server.Close)

	tr := testTransport(server.URL)
	tr.firstChunkTimeout = time.Second
	tr.idleTimeout = 50 * time.Millisecond

	_, err := tr.Stream(context.Background(), ChatRequest{Model: "m"}, func(Chunk) error { return nil })
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "upstream_timeout" {
		t.Fatalf("code = %q, want upstream_timeout", typed.Code)
	}
}

func TestStreamRejectsOversizedChunk(t *testing.T) {
	server := sseServer(t, "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\""+
		strings.Repeat("a", 2048)+"\"}}]}\n\n")
	tr := testTransport(server.URL)
	tr.maxChunkBytes = 256

	_, err := tr.Stream(context.Background(), ChatRequest{Model: "m"}, func(Chunk) error { return nil })
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "upstream_failed" {
		t.Fatalf("code = %q, want upstream_failed", typed.Code)
	}
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Fatalf("error = %v, want ErrChunkTooLarge", err)
	}
}

func TestListModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			t.Errorf("path = %q, want /models", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"object":"list","data":[{"id":"model-a"},{"id":"model-b"},{"id":""}]}`)
	}))
	t.Cleanup(server.Close)

	ids, err := testTransport(server.URL).ListModels(context.Background())
	if err != nil {
		t.Fatalf("list models: %v", err)
	}
	if len(ids) != 2 || ids[0] != "model-a" || ids[1] != "model-b" {
		t.Fatalf("ids = %v, want [model-a model-b]", ids)
	}
}

func TestListModelsReportsUpstreamFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	_, err := testTransport(server.URL).ListModels(context.Background())
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatalf("error = %v, want *Error", err)
	}
	if typed.Code != "upstream_failed" {
		t.Fatalf("code = %q, want upstream_failed", typed.Code)
	}
}
