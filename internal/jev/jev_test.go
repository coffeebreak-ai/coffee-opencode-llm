package jev

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(url string) *Client {
	return New(Options{
		BaseURL:     url,
		Model:       "jev-test",
		Timeout:     2 * time.Second,
		Retry:       1,
		Backoff:     time.Millisecond,
		MaxBodySize: 1 << 20,
	})
}

func TestAskParsesAnswers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/systemone" {
			t.Errorf("path = %q, want /v1/systemone", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"answers":{"route":{"choice":"expert","confidence":0.8,"reason":"needs synthesis"}}}`))
	}))
	defer server.Close()

	answers, err := testClient(server.URL).Ask(context.Background(), map[string]any{"task": "x"},
		map[string]Question{"route": {
			Type:     "choice",
			Criteria: map[string]string{"assistant": "cheap", "expert": "expensive"},
		}})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if answers["route"].Choice != "expert" {
		t.Fatalf("choice = %q, want expert", answers["route"].Choice)
	}
	if answers["route"].Confidence != 0.8 {
		t.Fatalf("confidence = %v, want 0.8", answers["route"].Confidence)
	}
}

func TestAskRetriesServerErrors(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			http.Error(w, "temporary", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"answers":{"route":{"choice":"assistant","confidence":1}}}`))
	}))
	defer server.Close()

	answers, err := testClient(server.URL).Ask(context.Background(), make(map[string]any),
		map[string]Question{"route": {
			Type:     "choice",
			Criteria: map[string]string{"assistant": "cheap", "expert": "expensive"},
		}})
	if err != nil {
		t.Fatalf("ask with retry: %v", err)
	}
	if answers["route"].Choice != "assistant" {
		t.Fatalf("choice = %q, want assistant", answers["route"].Choice)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestAskRejectsChoiceOutsideCriteria(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"answers":{"route":{"choice":"unknown","confidence":0.5}}}`))
	}))
	defer server.Close()

	_, err := testClient(server.URL).Ask(context.Background(), make(map[string]any),
		map[string]Question{"route": {
			Type:     "choice",
			Criteria: map[string]string{"assistant": "cheap", "expert": "expensive"},
		}})
	if err == nil {
		t.Fatal("expected an error for a choice outside criteria")
	}
}
