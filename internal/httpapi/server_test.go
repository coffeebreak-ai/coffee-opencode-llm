package httpapi_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/httpapi"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/jev"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

const testModelsYAML = `defaults:
  expert: exp-a
  assistant: asst-a
  scope: full
models:
  - id: exp-a
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 32768
    max_output_tokens: 4096
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: high
    quota: {}
    strengths: "test expert"
    limits: "test"
    enabled: true
  - id: exp-b
    roles: [expert]
    groups: [coffee-expert]
    context_tokens: 32768
    max_output_tokens: 4096
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: {}
    strengths: "test expert"
    limits: "test"
    enabled: true
  - id: asst-a
    roles: [assistant]
    groups: [coffee-assistant]
    context_tokens: 200000
    max_output_tokens: 4096
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: high
    quota: {}
    strengths: "test assistant"
    limits: "test"
    enabled: true
  - id: asst-b
    roles: [assistant]
    groups: [coffee-assistant]
    context_tokens: 200000
    max_output_tokens: 4096
    effort: []
    modalities: [text]
    features: [tools]
    cost_in_per_mtok: 0
    cost_out_per_mtok: 0
    quality_tier: medium
    quota: {}
    strengths: "test assistant"
    limits: "test"
    enabled: true
`

const testExpertsYAML = `coffee-expert: []
coffee-assistant: []
enabled: []
disabled: []
`

type fakeUpstream struct {
	mu       sync.Mutex
	calls    int
	requests []transport.ChatRequest
	respond  func(call int, request transport.ChatRequest, w http.ResponseWriter)
}

func (f *fakeUpstream) handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/chat/completions" {
		http.NotFound(w, r)
		return
	}
	var request transport.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	f.respond(call, request, w)
}

func (f *fakeUpstream) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeUpstream) request(index int) transport.ChatRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index < 0 || index >= len(f.requests) {
		return transport.ChatRequest{}
	}
	return f.requests[index]
}

func writeJSONResponse(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transport.ChatResponse{
		ID:      "upstream-1",
		Object:  "chat.completion",
		Created: 1,
		Model:   "asst-a",
		Choices: []transport.ChatChoice{{
			Index:        0,
			Message:      transport.TextMessage("assistant", content),
			FinishReason: "stop",
		}},
	})
}

func writeSSE(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	for _, event := range events {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", event); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func newGateway(t *testing.T, upstreamURL string) *httptest.Server {
	t.Helper()
	return newGatewayFixture(t, upstreamURL, testModelsYAML).Server
}

type gatewayFixture struct {
	*httptest.Server
	store  *store.Store
	dbPath string
}

func newGatewayFixture(t *testing.T, upstreamURL, modelsYAML string) *gatewayFixture {
	t.Helper()
	return newGatewayFixtureWith(t, upstreamURL, modelsYAML, testProgressOptions(), "final")
}

func testProgressOptions() httpapi.ProgressOptions {
	return httpapi.ProgressOptions{
		Enabled: true,
		Categories: map[string]bool{
			"stage": true, "decision": true, "delegation": true, "deflate": true,
			"model": true, "quota": true, "tools": true, "error": true,
		},
		MinIntervalMS: 0,
		MaxEvents:     128,
		TextChars:     200,
		Texts: map[string]string{
			"transition": "{stage}",
			"decision":   "{stage}: {reason}",
			"model":      "{stage}: {role} {model_id} {reason}",
			"deflate":    "{stage}: {reason}",
			"wait":       "{stage}: waiting for client tools",
			"stop":       "{stage}: {reason}",
			"error":      "{stage}: {reason}",
			"warning":    "Warning: {reason}",
			"tool":       "{class}: {reason}",
			"thought":    "thinking ({role})",
			"summary":    "turn: commands {commands}, edits {edits}, reads {reads}, thoughts {thoughts}, tools {tools}",
		},
		ToolClasses: map[string]string{"bash": "command", "edit": "edit", "write": "edit", "read": "read"},
	}
}

func newGatewayFixtureWith(t *testing.T, upstreamURL, modelsYAML string,
	progress httpapi.ProgressOptions, scope string, solvers ...*jev.Client) *gatewayFixture {
	t.Helper()
	return newGatewayFixtureOpts(t, upstreamURL, modelsYAML, progress, scope, nil, solvers...)
}

func newGatewayFixtureOpts(t *testing.T, upstreamURL, modelsYAML string,
	progress httpapi.ProgressOptions, scope string, mutate func(*engine.Options), solvers ...*jev.Client) *gatewayFixture {
	t.Helper()
	var solver *jev.Client
	if len(solvers) > 0 {
		solver = solvers[0]
	}
	ctx := context.Background()
	dir := t.TempDir()
	modelsPath := filepath.Join(dir, "models.yaml")
	expertsPath := filepath.Join(dir, "experts.yaml")
	if err := os.WriteFile(modelsPath, []byte(modelsYAML), 0o600); err != nil {
		t.Fatalf("write models: %v", err)
	}
	if err := os.WriteFile(expertsPath, []byte(testExpertsYAML), 0o600); err != nil {
		t.Fatalf("write experts: %v", err)
	}
	cat, err := catalog.Load(modelsPath, expertsPath, "free")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	st, err := store.Open(ctx, filepath.Join(dir, "coffee-opencode-llm.db"), store.Options{
		BusyTimeoutMS: 5000,
		MaxOpenConns:  4,
		MaxIdleConns:  2,
		WriteConns:    1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := st.Migrate(ctx, "sqlite"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.UpsertCatalog(ctx, storeRows(cat.Models)); err != nil {
		t.Fatalf("upsert catalog: %v", err)
	}
	tr := transport.New(transport.Options{
		BaseURL:           upstreamURL,
		ConnectTimeout:    time.Second,
		UpstreamTimeout:   5 * time.Second,
		MaxResponseBytes:  1 << 20,
		MaxChunkBytes:     1 << 20,
		FirstChunkTimeout: 2 * time.Second,
		IdleTimeout:       2 * time.Second,
		RetryAttempts:     1,
		RetryBackoff:      10 * time.Millisecond,
	})
	led := ledger.New(st, time.Hour.Milliseconds())
	rt := router.New(cat, st, led, solver, router.Options{
		ObjectiveMode:           "balanced",
		ReasoningMode:           "medium",
		SubscriptionMode:        "free",
		BalanceTurns:            10,
		CharsPerToken:           4,
		CriteriaChars:           4000,
		StateChars:              8000,
		MaxRequestBytes:         48000,
		DeflateMinConfidencePct: 50,
		MaxQuestions:            4,
		MaxCallsPerTurn:         4,
		MinStartConfidence:      0.6,
		DefaultExpert:           "exp-a",
		DefaultAssistant:        "asst-a",
		DefaultScope:            "full",
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := engine.Options{
		PublicModel:             "coffee-opencode-llm",
		PausePhrase:             "Paused by the gateway limit.",
		StreamTextScope:         scope,
		OutputLimitMode:         "client_first",
		MaxGatewayTurns:         4,
		MaxDelegationSteps:      4,
		MaxOutputTokens:         4096,
		CompactPacketChars:      4000,
		CompactEvidenceChars:    2000,
		DeflateThresholdChars:   2000,
		DeflatorReasoningTokens: 500,
		HistoryCompactPct:       80,
		HistorySummaryPct:       15,
		CharsPerToken:           4,
		MaxTools:                64,
		JevMaxCallsPerTurn:      4,
		BalanceTurns:            10,
		ToolScopes:              make(map[string][]string),
	}
	if mutate != nil {
		mutate(&opts)
	}
	eng := engine.New(st, cat, rt, led, tr, log, opts)
	api := httpapi.New(eng, st, cat, log, httpapi.Options{
		PublicModel:          "coffee-opencode-llm",
		MaxRequestBytes:      1 << 20,
		MaxMessageChars:      100000,
		MaxMediaPartBytes:    1 << 20,
		MaxTools:             64,
		DialogueLockWaitMS:   500,
		ClientTotalTimeoutMS: 30000,
		StreamWriteTimeoutMS: 5000,
		SessionIdleTTLHours:  24,
		Progress:             progress,
	})
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return &gatewayFixture{Server: server, store: st, dbPath: filepath.Join(dir, "coffee-opencode-llm.db")}
}

func startUpstream(t *testing.T, upstream *fakeUpstream) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(upstream.handler))
	t.Cleanup(server.Close)
	return server.URL
}

type fakeJev struct {
	mu         sync.Mutex
	requests   []map[string]any
	choice     string
	overrides  map[string]string
	confidence float64
}

func (f *fakeJev) checkAnswerRequest() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.requests) - 1; i >= 0; i-- {
		questions, _ := f.requests[i]["questions"].(map[string]any)
		if _, ok := questions["check_answer"]; ok {
			return f.requests[i]
		}
	}
	return nil
}

func (f *fakeJev) deflateRequest() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.requests) - 1; i >= 0; i-- {
		questions, _ := f.requests[i]["questions"].(map[string]any)
		if _, ok := questions["deflate"]; ok {
			return f.requests[i]
		}
	}
	return nil
}

func startJev(t *testing.T, choice string) (*jev.Client, *fakeJev) {
	return startJevWith(t, choice, nil)
}

func startJevWith(t *testing.T, choice string, overrides map[string]string) (*jev.Client, *fakeJev) {
	t.Helper()
	return startJevWithConfidence(t, choice, overrides, 0.9)
}

func startJevWithConfidence(t *testing.T, choice string, overrides map[string]string, confidence float64) (*jev.Client, *fakeJev) {
	t.Helper()
	mock := &fakeJev{choice: choice, overrides: overrides, confidence: confidence}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mock.mu.Lock()
		mock.requests = append(mock.requests, body)
		mock.mu.Unlock()
		questions, _ := body["questions"].(map[string]any)
		answers := make(map[string]any, len(questions))
		for name, raw := range questions {
			question, _ := raw.(map[string]any)
			criteria, _ := question["criteria"].(map[string]any)
			keys := make([]string, 0, len(criteria))
			for key := range criteria {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			picked := ""
			if len(keys) > 0 {
				picked = keys[0]
			}
			if name == "check_answer" {
				picked = mock.choice
			}
			if name == "start" {
				picked = "yes"
			}
			if override, ok := mock.overrides[name]; ok {
				picked = override
			}
			answers[name] = map[string]any{
				"choice": picked, "confidence": mock.confidence, "reason": "test decision",
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": answers})
	}))
	t.Cleanup(server.Close)
	return jev.New(jev.Options{
		BaseURL:     server.URL,
		Model:       "jev-mock",
		Timeout:     time.Second,
		MaxBodySize: 1 << 20,
	}), mock
}

func suspectBody() string {
	return `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"check"}],"stream":false}`
}

func TestSuspectAnswerEscalatesToExpert(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeJSONResponse(w, "")
			return
		}
		writeJSONResponse(w, "expert answer")
	}}
	solver, mock := startJev(t, "expert")
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)

	response, raw := post(t, gateway.URL, "ses_suspect_empty", suspectBody())
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if response.Header.Get("X-Coffee-Level") != "2" {
		t.Fatalf("level header = %q, want 2", response.Header.Get("X-Coffee-Level"))
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	text, _ := parsed.Choices[0].Message.Text()
	if text != "expert answer" {
		t.Fatalf("content = %q, want the expert answer", text)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d, want 2", upstream.callCount())
	}
	if got := upstream.request(1).Model; got != "exp-a" {
		t.Fatalf("expert model = %q, want exp-a", got)
	}
	request := mock.checkAnswerRequest()
	if request == nil {
		t.Fatal("check_answer was not asked")
	}
	state, _ := request["state"].(map[string]any)
	suspect, _ := state["answer_suspect"].(bool)
	if !suspect {
		t.Fatalf("answer_suspect = %v, want true; state = %+v", state["answer_suspect"], state)
	}
	if state["suspect_reason"] != "empty_answer" {
		t.Fatalf("suspect_reason = %v, want empty_answer", state["suspect_reason"])
	}
	questions, _ := request["questions"].(map[string]any)
	check, _ := questions["check_answer"].(map[string]any)
	criteria, _ := check["criteria"].(map[string]any)
	if _, ok := criteria["expert"]; !ok {
		t.Fatalf("check_answer criteria lack expert: %+v", criteria)
	}
	for _, key := range []string{"context_missing", "request_missing", "answer_missing"} {
		got, _ := state[key].(bool)
		if got {
			t.Fatalf("%s = true on a request that fits the state limit", key)
		}
	}
	instructions, _ := check["instructions"].(string)
	for _, key := range []string{"context_missing", "request_missing", "answer_missing"} {
		if !strings.Contains(instructions, key) {
			t.Fatalf("check_answer instructions do not explain %s: %q", key, instructions)
		}
	}
}

func TestSanitizedAnswerEscalatesToExpert(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeJSONResponse(w, "kept text\n```coffee-progress\nuser_to_adjuster_1\n```")
			return
		}
		writeJSONResponse(w, "expert answer")
	}}
	solver, mock := startJev(t, "expert")
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)

	response, raw := post(t, gateway.URL, "ses_suspect_sanitized", suspectBody())
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "coffee-progress") {
		t.Fatalf("service block reached the client: %s", raw)
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	text, _ := parsed.Choices[0].Message.Text()
	if text != "expert answer" {
		t.Fatalf("content = %q, want the expert answer", text)
	}
	request := mock.checkAnswerRequest()
	if request == nil {
		t.Fatal("check_answer was not asked")
	}
	state, _ := request["state"].(map[string]any)
	suspect, _ := state["answer_suspect"].(bool)
	if !suspect {
		t.Fatalf("answer_suspect = %v, want true", state["answer_suspect"])
	}
	if state["suspect_reason"] != "sanitized" {
		t.Fatalf("suspect_reason = %v, want sanitized", state["suspect_reason"])
	}
}

func TestEmptyAnswerAcceptedByAdjusterStillEscalates(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeJSONResponse(w, "")
			return
		}
		writeJSONResponse(w, "expert answer")
	}}
	solver, _ := startJev(t, "accept")
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)

	response, raw := post(t, gateway.URL, "ses_suspect_accepted", suspectBody())
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d, want 2 (empty answer must not be delivered)", upstream.callCount())
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	text, _ := parsed.Choices[0].Message.Text()
	if text == "" {
		t.Fatal("empty answer was delivered to the human")
	}
}

func TestEmptyAnswerWithoutAdjusterStops(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))

	response, raw := post(t, gateway.URL, "ses_suspect_no_jev", suspectBody())
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body = %s", response.StatusCode, raw)
	}
	if !strings.Contains(raw, "The model returned an empty answer") {
		t.Fatalf("empty_answer reason is missing: %s", raw)
	}
}

func post(t *testing.T, url, session, body string) (*http.Response, string) {
	return postWithClient(t, url, session, "", body)
}

func postWithClient(t *testing.T, url, session, client, body string) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-opencode-session", session)
	if client != "" {
		request.Header.Set("x-opencode-client", client)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response, string(raw)
}

func TestBufferedChatAndReplay(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "hello from assistant")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"system","content":"client system"},{"role":"user","content":"hello"}],"stream":false}`
	response, raw := post(t, gateway.URL, "ses_buffered", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if response.Header.Get("X-Coffee-Level") != "1" {
		t.Fatalf("level header = %q, want 1", response.Header.Get("X-Coffee-Level"))
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].Message.Content == nil {
		t.Fatalf("unexpected response: %s", raw)
	}
	text, _ := parsed.Choices[0].Message.Text()
	if text != "hello from assistant" {
		t.Fatalf("content = %q", text)
	}

	first := upstream.request(0)
	if first.Model != "asst-a" {
		t.Fatalf("upstream model = %q, want asst-a", first.Model)
	}
	if len(first.Messages) < 2 || first.Messages[0].Role != "system" {
		t.Fatalf("upstream messages = %+v", first.Messages)
	}
	prompt, _ := first.Messages[0].Text()
	if !strings.Contains(prompt, "You are the assistant") {
		t.Fatalf("assistant prompt is missing: %q", prompt)
	}
	instructions, _ := first.Messages[1].Text()
	if instructions != "client system" {
		t.Fatalf("client instructions = %q", instructions)
	}
	names := make(map[string]bool)
	for _, tool := range first.Tools {
		names[tool.Function.Name] = true
	}
	if !names["assistant_needs_expert"] {
		t.Fatalf("assistant tools do not include assistant_needs_expert: %+v", names)
	}
	if names["gateway_source"] || names["gateway_assistant"] {
		t.Fatalf("assistant must not receive gateway tools: %+v", names)
	}

	replayed, replayedRaw := post(t, gateway.URL, "ses_buffered", body)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayed.StatusCode, replayedRaw)
	}
	if upstream.callCount() != 1 {
		t.Fatalf("upstream calls = %d, want 1 (replay must not call the provider)", upstream.callCount())
	}
	if replayedRaw != raw {
		t.Fatalf("replay body differs:\nfirst: %s\nreplay: %s", raw, replayedRaw)
	}
}

func TestStreamingToolCalls(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"Searching first"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"role":"assistant","content":"working"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read","arguments":"{\"path\":\"a\"}"}}]}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`,
			`[DONE]`,
		)
	}}
	gateway := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)
	enableReasoningContext(t, gateway)

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"read a"}],"stream":true}`
	response, raw := postWithClient(t, gateway.URL, "ses_stream_tools", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content type = %q", response.Header.Get("Content-Type"))
	}
	for _, want := range []string{
		`"reasoning_content":"Searching first"`,
		`"name":"read"`,
		`"finish_reason":"tool_calls"`,
		`"x-coffee-participants"`,
		`"kind":"tool"`,
		`"class":"read"`,
		`"kind":"summary"`,
		`"reads":1`,
		"commands 0, edits 0, reads 1, thoughts 0, tools 0",
		"data: [DONE]",
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("stream does not contain %q:\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "working") {
		t.Fatalf("intermediate tool-call narration leaked into the response:\n%s", raw)
	}
	if strings.Contains(raw, `"usage"`) {
		t.Fatalf("usage must be suppressed without include_usage:\n%s", raw)
	}
	if strings.Index(raw, `"x-coffee-participants"`) > strings.Index(raw, `"finish_reason":"tool_calls"`) {
		t.Fatalf("participants must be emitted before finish_reason:\n%s", raw)
	}
	replayed, replayedRaw := postWithClient(t, gateway.URL, "ses_stream_tools", "cli", body)
	if replayed.StatusCode != http.StatusOK || upstream.callCount() != 1 ||
		strings.Contains(replayedRaw, `"reasoning_content"`) {
		t.Fatalf("reasoning replay status=%d calls=%d body=%s",
			replayed.StatusCode, upstream.callCount(), replayedRaw)
	}
	otherClient, otherRaw := post(t, gateway.URL, "ses_stream_tools_other_client", body)
	if otherClient.StatusCode != http.StatusOK || strings.Contains(otherRaw, `"reasoning_content"`) {
		t.Fatalf("reasoning reached a non-OpenCode stream client: status=%d body=%s",
			otherClient.StatusCode, otherRaw)
	}
}

func TestAssistantEscalatesToExpert(t *testing.T) {
	internalReasoning := "assistant internal reasoning"
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(transport.ChatResponse{
				ID:      "up-1",
				Object:  "chat.completion",
				Created: 1,
				Model:   "asst-a",
				Choices: []transport.ChatChoice{{
					Index: 0,
					Message: transport.Message{
						Role:             "assistant",
						Content:          transport.TextContent("draft"),
						ReasoningContent: &internalReasoning,
						ToolCalls: []transport.ToolCall{{
							ID:   "call_virtual",
							Type: "function",
							Function: transport.FunctionCall{
								Name:      "assistant_needs_expert",
								Arguments: `{"reason":"needs review"}`,
							},
						}},
					},
					FinishReason: "tool_calls",
				}},
			})
			return
		}
		writeJSONResponse(w, "final answer")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"review this"}],"stream":false}`
	response, raw := post(t, gateway.URL, "ses_escalate", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if response.Header.Get("X-Coffee-Level") != "2" {
		t.Fatalf("level header = %q, want 2", response.Header.Get("X-Coffee-Level"))
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	text, _ := parsed.Choices[0].Message.Text()
	if text != "final answer" {
		t.Fatalf("content = %q, want the expert answer", text)
	}
	if strings.Contains(raw, internalReasoning) {
		t.Fatalf("reasoning from an internal escalation reached the client: %s", raw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d, want 2", upstream.callCount())
	}
	expert := upstream.request(1)
	if expert.Model != "exp-a" {
		t.Fatalf("expert model = %q, want exp-a", expert.Model)
	}
	foundMaterial := false
	for _, message := range expert.Messages {
		text, err := message.Text()
		if err != nil {
			continue
		}
		if strings.Contains(text, "Assistant material:") {
			foundMaterial = true
		}
		if strings.Contains(text, "draft") {
			t.Fatal("intermediate assistant work text reached the expert prompt")
		}
	}
	if !foundMaterial {
		t.Fatalf("expert request does not contain the assistant material: %+v", expert.Messages)
	}
	names := make(map[string]bool)
	for _, tool := range expert.Tools {
		names[tool.Function.Name] = true
	}
	if !names["gateway_assistant"] {
		t.Fatalf("expert tools do not include gateway_assistant: %+v", names)
	}
	if names["gateway_source"] || names["assistant_needs_expert"] {
		t.Fatalf("expert must not receive assistant tools: %+v", names)
	}

	// The delivery key must not depend on the level: after the escalation the
	// dialogue is on level 2, and a retry of the same input must still replay
	// the stored answer instead of calling the provider again.
	replayed, replayedRaw := post(t, gateway.URL, "ses_escalate", body)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayed.StatusCode, replayedRaw)
	}
	if replayedRaw != raw {
		t.Fatalf("replay body differs:\nfirst: %s\nreplay: %s", raw, replayedRaw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d after replay, want 2", upstream.callCount())
	}
}

func TestClientCancelStopsUpstreamAndReleasesQuota(t *testing.T) {
	var calls atomic.Int32
	upstreamStarted := make(chan struct{}, 1)
	upstreamCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if calls.Add(1) > 1 {
			writeJSONResponse(w, "after cancel")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		upstreamStarted <- struct{}{}
		<-r.Context().Done()
		upstreamCanceled <- struct{}{}
	}))
	t.Cleanup(upstream.Close)

	models := strings.ReplaceAll(testModelsYAML, "quota: {}", "quota: {day: 1}")
	gateway := newGatewayFixture(t, upstream.URL, models)

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"hello"}],"stream":true}`
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/v1/chat/completions",
		strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-opencode-session", "ses_cancel")
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		done <- err
	}()

	select {
	case <-upstreamStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream call did not start")
	}
	cancel()
	select {
	case <-upstreamCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("the upstream call was not canceled")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the client request did not finish after cancellation")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls after cancel = %d, want 1: a canceled request must not be retried", got)
	}

	one := int64(1)
	deadline := time.Now().Add(2 * time.Second)
	for {
		remaining, unlimited, err := gateway.store.Remaining(context.Background(), "asst-a", store.Quota{Day: &one})
		if err != nil {
			t.Fatalf("read remaining quota: %v", err)
		}
		if !unlimited && remaining == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("quota reservation was not released: remaining = %d, unlimited = %v", remaining, unlimited)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The follow-up is buffered on purpose: the fake upstream answers with a
	// single JSON document, and a replayed delivery would never reach it.
	buffered := strings.Replace(body, `"stream":true`, `"stream":false`, 1)
	response, raw := post(t, gateway.URL, "ses_cancel", buffered)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("second request status = %d, body = %s", response.StatusCode, raw)
	}
	if !strings.Contains(raw, "after cancel") {
		t.Fatalf("second request body = %s, want a fresh upstream answer", raw)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2: a canceled stream must not write a delivery", got)
	}

	state, err := gateway.store.Availability(context.Background())
	if err != nil {
		t.Fatalf("read availability: %v", err)
	}
	if item := state["asst-a"]; item.ErrorCount != 0 || item.DisabledUntilMS != nil {
		t.Fatalf("a canceled request must not penalize the model: %+v", item)
	}
}

func TestConcurrentTurnOnSameSessionConflicts(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	releaseAll := func() { once.Do(func() { close(release) }) }
	defer releaseAll()
	started := make(chan struct{}, 1)
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			started <- struct{}{}
			<-release
		}
		writeJSONResponse(w, "slow answer")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"hello"}],"stream":false}`
	first := make(chan string, 1)
	go func() {
		_, raw := post(t, gateway.URL, "ses_lock", body)
		first <- raw
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the first turn did not reach the upstream")
	}

	response, raw := post(t, gateway.URL, "ses_lock", body)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("second turn status = %d, body = %s, want 409", response.StatusCode, raw)
	}
	if !strings.Contains(raw, `"conflict"`) {
		t.Fatalf("second turn body = %s, want the conflict code", raw)
	}

	releaseAll()
	select {
	case raw := <-first:
		if !strings.Contains(raw, "slow answer") {
			t.Fatalf("first turn body = %s", raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first turn did not finish")
	}
	if got := upstream.callCount(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1: the conflicting turn must not call the provider", got)
	}
}

func TestParticipantsReportIsNotRepeatedBack(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"Связь есть.\n[coffee-opencode-llm] assistant: asst-a\n"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	ctx := context.Background()
	const session = "ses_participants_report"
	dialogue, err := fixture.store.EnsureDialogue(ctx, session, "header", 24)
	if err != nil {
		t.Fatalf("ensure dialogue: %v", err)
	}
	// A row written before the fix: the report sits in the append-only log.
	if _, err := fixture.store.AppendMessages(ctx, dialogue.ID, []store.Message{{
		ID: store.NewID(), Role: "assistant", Kind: "message",
		Content: "Старый ответ.\n[coffee-opencode-llm] assistant: asst-a\n",
	}}); err != nil {
		t.Fatalf("append legacy message: %v", err)
	}
	before, err := fixture.store.Messages(ctx, dialogue.ID)
	if err != nil {
		t.Fatalf("read baseline messages: %v", err)
	}

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[` +
		`{"role":"user","content":"первый запрос"},` +
		"{\"role\":\"assistant\",\"content\":\"Ответ.\\n```coffee-progress\\nuser_to_adjuster_1\\n```\\n\"}," +
		`{"role":"user","content":"проверка связи"}]}`
	response, raw := post(t, fixture.URL, session, body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if got := strings.Count(raw, "x-coffee-participants"); got != 1 {
		t.Fatalf("participants chunks = %d, want 1:\n%s", got, raw)
	}
	if got := strings.Count(raw, "```coffee-participants"); got != 1 {
		t.Fatalf("report blocks = %d, want 1:\n%s", got, raw)
	}
	for _, message := range upstream.request(0).Messages {
		text, err := message.Text()
		if err != nil {
			continue
		}
		if strings.Contains(text, "coffee-progress") || strings.Contains(text, "[coffee-opencode-llm]") {
			t.Fatalf("report leaked into the upstream prompt: %q", text)
		}
	}
	stored, err := fixture.store.Messages(ctx, dialogue.ID)
	if err != nil {
		t.Fatalf("read messages: %v", err)
	}
	for _, message := range stored[len(before):] {
		if strings.Contains(message.Content, "coffee-progress") ||
			strings.Contains(message.Content, "[coffee-opencode-llm]") {
			t.Fatalf("report leaked into the stored history: %q", message.Content)
		}
	}
}

func TestProgressBeforeFirstModelChunk(t *testing.T) {
	release := make(chan struct{})
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		<-release
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"hello"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	request, err := http.NewRequest(http.MethodPost, fixture.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-opencode-session", "ses_progress_first")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}

	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read the first chunk: %v", err)
	}
	if !strings.Contains(first, "x-coffee-progress") || !strings.Contains(first, "```coffee-progress") {
		t.Fatalf("the first chunk is not a progress event: %q", first)
	}
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read the chunk separator: %v", err)
	}
	close(release)
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read the rest of the stream: %v", err)
	}
	raw := first + string(rest)
	if strings.Count(raw, "x-coffee-participants") != 1 {
		t.Fatalf("participants chunks = %d, want 1:\n%s", strings.Count(raw, "x-coffee-participants"), raw)
	}
	if !strings.Contains(raw, `"content":"hello"`) {
		t.Fatalf("the answer is missing:\n%s", raw)
	}

	dialogue, err := fixture.store.EnsureDialogue(context.Background(), "ses_progress_first", "header", 24)
	if err != nil {
		t.Fatalf("read dialogue: %v", err)
	}
	stored, err := fixture.store.Messages(context.Background(), dialogue.ID)
	if err != nil {
		t.Fatalf("read messages: %v", err)
	}
	for _, message := range stored {
		if strings.Contains(message.Content, "coffee-progress") || strings.Contains(message.Content, "coffee-opencode-llm") {
			t.Fatalf("progress leaked into the stored history: %q", message.Content)
		}
	}
}

func TestProgressDisabledByConfig(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"hello"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	options := testProgressOptions()
	options.Enabled = false
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML, options, "all")

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_progress_off", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "coffee-progress") || strings.Contains(raw, "x-coffee-progress") {
		t.Fatalf("progress must be disabled:\n%s", raw)
	}
}

func TestIncomingReasoningIsExcludedFromPrompt(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[` +
		`{"role":"user","content":"first"},` +
		`{"role":"assistant","content":"answer","reasoning_content":"real thinking\n[coffee-progress] adjuster_1_to_assistant_1\n"},` +
		`{"role":"user","content":"second"}]}`
	response, raw := post(t, fixture.URL, "ses_reasoning_roundtrip", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, message := range upstream.request(0).Messages {
		if message.ReasoningContent != nil || message.Reasoning != nil || message.ReasoningText != nil {
			t.Fatalf("reasoning reached the upstream prompt: %+v", message)
		}
	}
}

func TestFinalScopeStreamsFinalModelReasoning(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"model thinking"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML, testProgressOptions(), "final")
	enableReasoningContext(t, fixture)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := postWithClient(t, fixture.URL, "ses_final_scope", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if !strings.Contains(raw, `"reasoning_content":"model thinking"`) {
		t.Fatalf("final reasoning is missing in final scope:\n%s", raw)
	}
	if !strings.Contains(raw, `"content":"answer"`) {
		t.Fatalf("the final answer is missing:\n%s", raw)
	}
	if !strings.Contains(raw, "```coffee-progress") {
		t.Fatalf("progress is missing in final scope:\n%s", raw)
	}
}

func TestFinalScopeStreamsReasoningFromInternalModelCalls(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeSSE(w,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"assistant thought"}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"internal draft"}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":`+
					`{"tool_calls":[{"index":0,"id":"call_needs_expert","type":"function","function":{"name":"assistant_needs_expert",`+
					`"arguments":"{\"reason\":\"needs review\"}"}}]}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
			return
		}
		writeSSE(w,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{"reasoning_content":"expert thought"}}]}`,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{"content":"final answer"}}]}`,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "final")
	enableReasoningContext(t, fixture)
	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"review"}]}`
	response, raw := postWithClient(t, fixture.URL, "ses_live_internal_reasoning", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, want := range []string{
		`"reasoning_content":"assistant thought"`,
		`"reasoning_content":"expert thought"`,
		`"content":"final answer"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("stream is missing %q:\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "internal draft") {
		t.Fatalf("intermediate content leaked in final scope:\n%s", raw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d, want assistant and expert", upstream.callCount())
	}
}

const (
	commandMarkerJSON = `\u003c/think:6124c78e\u003e`
	commandMarkerGo   = "\u003c/think:6124c78e\u003e"
)

func TestReasoningAndCommandTagsAreStrippedFromStream(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"hello `+commandMarkerJSON+` world"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"real thinking `+commandMarkerJSON+`"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)
	enableReasoningContext(t, fixture)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := postWithClient(t, fixture.URL, "ses_command_stream", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "6124c78e") {
		t.Fatalf("command marker leaked into the stream:\n%s", raw)
	}
	if !strings.Contains(raw, "hello  world") {
		t.Fatalf("clean answer is missing:\n%s", raw)
	}
	if !strings.Contains(raw, `"reasoning_content":"real thinking "`) {
		t.Fatalf("clean reasoning is missing from the stream:\n%s", raw)
	}
	if got := strings.Count(raw, `"reason":"reasoning_sanitized"`); got != 1 {
		t.Fatalf("sanitization warnings = %d, want one:\n%s", got, raw)
	}
}

func TestReasoningContextDisabledHidesAllReasoning(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeSSE(w,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"assistant internal thought"}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":`+
					`{"tool_calls":[{"index":0,"id":"call_needs_expert","type":"function","function":{"name":"assistant_needs_expert",`+
					`"arguments":"{\"reason\":\"needs review\"}"}}]}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
			return
		}
		writeSSE(w,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{"reasoning_content":"expert thought"}}]}`,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{"content":"final answer"}}]}`,
			`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "final")
	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"review"}]}`
	response, raw := postWithClient(t, fixture.URL, "ses_reasoning_disabled", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "reasoning_content") {
		t.Fatalf("reasoning leaked with the master gate off:\n%s", raw)
	}
	if !strings.Contains(raw, `"content":"final answer"`) {
		t.Fatalf("the final answer is missing:\n%s", raw)
	}

	db, err := sql.Open("sqlite", fixture.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	var reasoningRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM message WHERE reasoning_content IS NOT NULL`).
		Scan(&reasoningRows); err != nil {
		t.Fatal(err)
	}
	if reasoningRows != 0 {
		t.Fatalf("master-off reasoning rows = %d, want none", reasoningRows)
	}
}

func enableReasoningContext(t *testing.T, fixture *gatewayFixture) {
	t.Helper()
	if err := fixture.store.SetSetting(context.Background(),
		"reasoning_context_enabled", "on", "command"); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedCommandTagsAndReplay(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "hello "+commandMarkerGo+" world")
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_command_buffered", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "6124c78e") {
		t.Fatalf("command marker leaked into the answer: %s", raw)
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	text, err := parsed.Choices[0].Message.Text()
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if text != "hello  world" {
		t.Fatalf("content = %q", text)
	}

	replayed, replayRaw := post(t, fixture.URL, "ses_command_buffered", body)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayed.StatusCode, replayRaw)
	}
	if upstream.callCount() != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.callCount())
	}
	if replayRaw != raw {
		t.Fatalf("replay differs:\nfirst: %s\nreplay: %s", raw, replayRaw)
	}
}

func TestBufferedReasoningIsTransientAndOpenCodeOnly(t *testing.T) {
	reasoning := "model reasoning"
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
			Message: transport.Message{
				Role:             "assistant",
				Content:          transport.TextContent("answer"),
				ReasoningContent: &reasoning,
			},
			FinishReason: "stop",
		}}}); err != nil {
			t.Error(err)
		}
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)
	enableReasoningContext(t, fixture)
	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[{"role":"user","content":"hello"}]}`

	response, raw := postWithClient(t, fixture.URL, "ses_buffered_reasoning", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	message := parsed.Choices[0].Message
	text, err := message.Text()
	if err != nil {
		t.Fatal(err)
	}
	if text != "answer" || message.ReasoningContent == nil || *message.ReasoningContent != reasoning {
		t.Fatalf("message content=%q reasoning=%v", text, message.ReasoningContent)
	}

	db, err := sql.Open("sqlite", fixture.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	var archivedReasoning int
	if err := db.QueryRow(`SELECT COUNT(*) FROM message
		WHERE reasoning_content = ? AND content IS NULL`, reasoning).
		Scan(&archivedReasoning); err != nil {
		t.Fatal(err)
	}
	if archivedReasoning != 1 {
		t.Fatalf("trace-scope-full reasoning rows = %d, want one even with context flag off", archivedReasoning)
	}
	var storedBody string
	if err := db.QueryRow(`SELECT response_body FROM delivery WHERE endpoint = 'chat'
		ORDER BY created_at_ms DESC LIMIT 1`).Scan(&storedBody); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(storedBody, "reasoning_content") || strings.Contains(storedBody, reasoning) {
		t.Fatalf("reasoning persisted in delivery: %s", storedBody)
	}

	legacyResponse := parsed
	legacyResponse.Choices = append([]transport.ChatChoice(nil), parsed.Choices...)
	legacyResponse.Choices[0].Message.ReasoningContent = &reasoning
	legacyBody, err := json.Marshal(legacyResponse)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE delivery SET response_body = ? WHERE endpoint = 'chat'`,
		string(legacyBody)); err != nil {
		t.Fatal(err)
	}

	replayed, replayRaw := post(t, fixture.URL, "ses_buffered_reasoning", body)
	if replayed.StatusCode != http.StatusOK || strings.Contains(replayRaw, `"reasoning_content"`) ||
		!strings.Contains(replayRaw, `"content":"answer"`) || upstream.callCount() != 1 {
		t.Fatalf("legacy replay status=%d calls=%d body=%s",
			replayed.StatusCode, upstream.callCount(), replayRaw)
	}

	otherClient, otherRaw := post(t, fixture.URL, "ses_non_opencode_reasoning", body)
	if otherClient.StatusCode != http.StatusOK || strings.Contains(otherRaw, `"reasoning_content"`) {
		t.Fatalf("reasoning reached a non-OpenCode client: status=%d body=%s",
			otherClient.StatusCode, otherRaw)
	}
}

func TestReasoningContextUsesOnlyExactArchivedReasoning(t *testing.T) {
	for _, test := range []struct {
		name             string
		clientReasoning  string
		wantPromptReason bool
	}{
		{name: "exact_match", clientReasoning: "trusted thought", wantPromptReason: true},
		{name: "changed_reasoning", clientReasoning: "client supplied thought"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reasoning := "trusted thought"
			upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
				message := transport.TextMessage("assistant", "first answer")
				if call == 1 {
					message.ReasoningContent = &reasoning
				} else {
					message = transport.TextMessage("assistant", "next answer")
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
					Message: message, FinishReason: "stop",
				}}}); err != nil {
					t.Error(err)
				}
			}}
			fixture := newGatewayFixtureOpts(t, startUpstream(t, upstream), testModelsYAML,
				testProgressOptions(), "all", func(opts *engine.Options) {
					opts.Proxy = true
				})
			if err := fixture.store.SetSetting(context.Background(),
				"reasoning_context_enabled", "on", "command"); err != nil {
				t.Fatal(err)
			}

			first := `{"model":"coffee-opencode-llm","stream":false,` +
				`"messages":[{"role":"user","content":"first question"}]}`
			response, raw := postWithClient(t, fixture.URL,
				"ses_reasoning_archive_"+test.name, "tui", first)
			if response.StatusCode != http.StatusOK ||
				!strings.Contains(raw, `"reasoning_content":"trusted thought"`) {
				t.Fatalf("first response status=%d body=%s", response.StatusCode, raw)
			}

			body := `{"model":"coffee-opencode-llm","stream":false,"messages":[` +
				`{"role":"user","content":"first question"},` +
				`{"role":"assistant","content":"first answer","reasoning_content":"` +
				test.clientReasoning + `"},` +
				`{"role":"user","content":"next question"}]}`
			response, raw = postWithClient(t, fixture.URL,
				"ses_reasoning_archive_"+test.name, "tui", body)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("second response status=%d body=%s", response.StatusCode, raw)
			}

			foundArchivedReasoning := false
			for _, message := range upstream.request(1).Messages {
				if message.ReasoningContent != nil && *message.ReasoningContent == reasoning {
					foundArchivedReasoning = true
				}
				if message.ReasoningContent != nil && *message.ReasoningContent == test.clientReasoning &&
					test.clientReasoning != reasoning {
					t.Fatal("client-provided reasoning reached the model")
				}
			}
			if foundArchivedReasoning != test.wantPromptReason {
				t.Fatalf("archived reasoning in prompt = %v, want %v",
					foundArchivedReasoning, test.wantPromptReason)
			}

			db, err := sql.Open("sqlite", fixture.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Errorf("close database: %v", err)
				}
			}()
			var reasoningRows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM message
				WHERE reasoning_content = ? AND content IS NULL`, reasoning).
				Scan(&reasoningRows); err != nil {
				t.Fatal(err)
			}
			if reasoningRows != 1 {
				t.Fatalf("archived reasoning rows = %d, want one", reasoningRows)
			}
		})
	}
}

func TestReasoningContextDoesNotArchiveIncompleteCall(t *testing.T) {
	for _, finish := range []string{"length", "content_filter"} {
		t.Run(finish, func(t *testing.T) {
			reasoning := "partial reasoning"
			upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
				message := transport.TextMessage("assistant", "partial answer")
				message.ReasoningContent = &reasoning
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
					Message: message, FinishReason: finish,
				}}}); err != nil {
					t.Error(err)
				}
			}}
			fixture := newGatewayFixtureOpts(t, startUpstream(t, upstream), testModelsYAML,
				testProgressOptions(), "all", func(opts *engine.Options) {
					opts.Proxy = true
				})
			if err := fixture.store.SetSetting(context.Background(),
				"reasoning_context_enabled", "on", "command"); err != nil {
				t.Fatal(err)
			}
			body := `{"model":"coffee-opencode-llm","stream":false,` +
				`"messages":[{"role":"user","content":"question"}]}`
			response, raw := postWithClient(t, fixture.URL, "ses_reasoning_partial_"+finish, "tui", body)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("response status=%d body=%s", response.StatusCode, raw)
			}

			db, err := sql.Open("sqlite", fixture.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Errorf("close database: %v", err)
				}
			}()
			var archivedReasoning int
			if err := db.QueryRow(`SELECT COUNT(*) FROM message WHERE reasoning_content IS NOT NULL`).
				Scan(&archivedReasoning); err != nil {
				t.Fatal(err)
			}
			if archivedReasoning != 0 {
				t.Fatalf("%s reasoning rows = %d, want none", finish, archivedReasoning)
			}
		})
	}
}

func TestBrokenAnswerIsNotInheritedInRequest(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "clean answer")
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[` +
		`{"role":"system","content":"\u003cuser_query\u003e client system"},` +
		`{"role":"assistant","content":"foreign answer ` + commandMarkerJSON + `",` +
		`"reasoning_content":"real thinking ` + commandMarkerJSON + `"},` +
		`{"role":"user","content":"my question ` + commandMarkerJSON + `"}]}`
	response, raw := post(t, fixture.URL, "ses_foreign_answer", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if strings.Contains(raw, "6124c78e") {
		t.Fatalf("command marker leaked into the answer: %s", raw)
	}

	request := upstream.request(0)
	legitimateTag := false
	for _, message := range request.Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatalf("read upstream message: %v", err)
		}
		if strings.Contains(text, "6124c78e") {
			t.Fatalf("foreign broken answer reached the prompt: %q", text)
		}
		if strings.Contains(text, "\u003cuser_query\u003e") {
			legitimateTag = true
		}
		for _, value := range []*string{message.ReasoningContent, message.Reasoning, message.ReasoningText} {
			if value != nil && strings.Contains(*value, "6124c78e") {
				t.Fatalf("foreign broken reasoning reached the prompt: %q", *value)
			}
		}
	}
	if !legitimateTag {
		t.Fatal("legitimate client tag was stripped from the prompt")
	}
}

func TestUsageReportInStream(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[],`+
				`"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,`+
				`"prompt_tokens_details":{"cached_tokens":3},`+
				`"completion_tokens_details":{"reasoning_tokens":7}}}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_usage_stream", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, want := range []string{
		`"x-coffee-usage"`,
		"```coffee-usage",
		`"role":"assistant"`,
		`"input_tokens":10`,
		`"output_tokens":5`,
		`"cached_tokens":3`,
		`"reasoning_tokens":7`,
		`"role":"expert"`,
		`"role":"deflator"`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("stream does not contain %q:\n%s", want, raw)
		}
	}
	reportAt := strings.Index(raw, `"x-coffee-usage"`)
	finishAt := strings.LastIndex(raw, `"finish_reason":"stop"`)
	if reportAt < 0 || finishAt < 0 || reportAt > finishAt {
		t.Fatalf("usage report must precede the final finish_reason:\n%s", raw)
	}

	replayBody := `{"model":"coffee-opencode-llm","stream":false,"messages":[{"role":"user","content":"hello"}]}`
	replayed, replayRaw := post(t, fixture.URL, "ses_usage_stream", replayBody)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayed.StatusCode, replayRaw)
	}
	if strings.Contains(replayRaw, "coffee-usage") {
		t.Fatalf("transient report was stored in delivery: %s", replayRaw)
	}
}

func TestUsageReportInBufferedResponse(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"up-1","object":"chat.completion","created":1,"model":"asst-a",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`))
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_usage_buffered", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, want := range []string{`"x-coffee-usage"`, `"role":"assistant"`, `"input_tokens":7`, `"output_tokens":2`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("buffered response does not contain %q: %s", want, raw)
		}
	}
	replayed, replayRaw := post(t, fixture.URL, "ses_usage_buffered", body)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, body = %s", replayed.StatusCode, replayRaw)
	}
	if replayRaw != raw {
		t.Fatalf("replay differs:\nfirst: %s\nreplay: %s", raw, replayRaw)
	}
}

func TestUsageReportIsStrippedFromIncomingHistory(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "answer")
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[` +
		`{"role":"system","content":"client system"},` +
		`{"role":"assistant","content":"answer\n` + "```coffee-usage" + `\nassistant: input 10\n` + "```" + `\n"},` +
		`{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_usage_echo", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, message := range upstream.request(0).Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatalf("read upstream message: %v", err)
		}
		if strings.Contains(text, "coffee-usage") {
			t.Fatalf("usage report reached the prompt: %q", text)
		}
	}
}

func TestDiscardedIntermediateToolCallsAreNotEmitted(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		switch call {
		case 1:
			writeSSE(w,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a",`+
					`"choices":[{"index":0,"delta":{"tool_calls":[`+
					`{"index":0,"id":"call_need","type":"function",`+
					`"function":{"name":"assistant_needs_expert","arguments":"{\"reason\":\"need expert\"}"}}]}}]}`,
				`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a",`+
					`"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
		case 2:
			writeSSE(w,
				`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a",`+
					`"choices":[{"index":0,"delta":{"tool_calls":[`+
					`{"index":0,"id":"call_gw","type":"function",`+
					`"function":{"name":"gateway_assistant","arguments":"{\"task\":\"t\",\"context\":\"c\",\"scope\":\"read\"}"}},`+
					`{"index":1,"id":"call_discarded","type":"function",`+
					`"function":{"name":"read","arguments":"{\"path\":\"discarded\"}"}}]}}]}`,
				`{"id":"up-2","object":"chat.completion.chunk","created":1,"model":"exp-a",`+
					`"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
		case 3:
			writeSSE(w,
				`{"id":"up-3","object":"chat.completion.chunk","created":1,"model":"asst-a",`+
					`"choices":[{"index":0,"delta":{"tool_calls":[`+
					`{"index":0,"id":"call_need2","type":"function",`+
					`"function":{"name":"assistant_needs_expert","arguments":"{\"reason\":\"no shell\"}"}}]}}]}`,
				`{"id":"up-3","object":"chat.completion.chunk","created":1,"model":"asst-a",`+
					`"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
		default:
			writeSSE(w,
				`{"id":"up-4","object":"chat.completion.chunk","created":1,"model":"exp-a",`+
					`"choices":[{"index":0,"delta":{"tool_calls":[`+
					`{"index":0,"id":"call_final","type":"function",`+
					`"function":{"name":"read","arguments":"{\"path\":\"final\"}"}}]}}]}`,
				`{"id":"up-4","object":"chat.completion.chunk","created":1,"model":"exp-a",`+
					`"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
		}
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_discarded_tools", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if got := strings.Count(raw, `"tool_calls":[`); got != 1 {
		t.Fatalf("tool_calls chunks = %d, want 1:\n%s", got, raw)
	}
	if !strings.Contains(raw, "call_final") || !strings.Contains(raw, `{\"path\":\"final\"}`) {
		t.Fatalf("final tool call is missing:\n%s", raw)
	}
	if strings.Contains(raw, "call_discarded") || strings.Contains(raw, "call_gw") || strings.Contains(raw, "discarded") {
		t.Fatalf("discarded or virtual tool call leaked into the stream:\n%s", raw)
	}
	if strings.Contains(raw, `}{`) {
		t.Fatalf("tool arguments were concatenated:\n%s", raw)
	}
}

func TestClientCheckpointIsHoisted(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[` +
		`{"role":"user","content":"<conversation-checkpoint>earlier summary</conversation-checkpoint>"},` +
		`{"role":"user","content":"hello"}]}`
	response, raw := post(t, fixture.URL, "ses_checkpoint", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}

	request := upstream.request(0)
	checkpoints := 0
	question := false
	for _, message := range request.Messages {
		text, err := message.Text()
		if err != nil {
			continue
		}
		if strings.Contains(text, "conversation-checkpoint") {
			checkpoints++
			if message.Role != "system" {
				t.Fatalf("checkpoint must be a leading system instruction, role = %q", message.Role)
			}
		}
		if message.Role == "user" && strings.Contains(text, "hello") {
			question = true
		}
	}
	if checkpoints != 1 {
		t.Fatalf("checkpoint copies = %d, want 1: %+v", checkpoints, request.Messages)
	}
	if !question {
		t.Fatal("the human question is missing from the prompt")
	}
}

func TestReasoningStreamsSeparatelyAndActionSummaryRemains(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeSSE(w,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"reasoning_content":"model thinking"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{"content":"answer"}}]}`,
			`{"id":"up-1","object":"chat.completion.chunk","created":1,"model":"asst-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", nil)
	enableReasoningContext(t, fixture)

	body := `{"model":"coffee-opencode-llm","stream":true,"messages":[{"role":"user","content":"hello"}]}`
	response, raw := postWithClient(t, fixture.URL, "ses_thought", "cli", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	for _, unwanted := range []string{`"kind":"thought"`, "thinking (assistant)"} {
		if strings.Contains(raw, unwanted) {
			t.Fatalf("reasoning details leaked through %q:\n%s", unwanted, raw)
		}
	}
	if !strings.Contains(raw, `"reasoning_content":"model thinking"`) {
		t.Fatalf("reasoning is missing as a separate stream field:\n%s", raw)
	}
	if !strings.Contains(raw, `"thoughts":0`) || !strings.Contains(raw, `"content":"answer"`) {
		t.Fatalf("final answer or action summary is missing:\n%s", raw)
	}
}

func TestLowStartConfidenceGoesToExpert(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "expert answer")
	}}
	solver, _ := startJevWithConfidence(t, "accept", nil, 0.3)
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)

	body := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"hello"}],"stream":false}`
	response, raw := post(t, gateway.URL, "ses_low_confidence", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if response.Header.Get("X-Coffee-Level") != "2" {
		t.Fatalf("level header = %q, want 2", response.Header.Get("X-Coffee-Level"))
	}
	if upstream.callCount() == 0 || upstream.request(0).Model != "exp-a" {
		t.Fatalf("upstream model = %q, want exp-a", upstream.request(0).Model)
	}
}

func TestDeflatorGetsOutputLimitAndLowEffort(t *testing.T) {
	models := strings.ReplaceAll(testModelsYAML, "effort: []", "effort: [minimal, high]")
	upstream := &fakeUpstream{respond: func(call int, request transport.ChatRequest, w http.ResponseWriter) {
		switch call {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(transport.ChatResponse{
				ID: "up-1", Object: "chat.completion", Created: 1, Model: "asst-a",
				Choices: []transport.ChatChoice{{
					Index: 0,
					Message: transport.Message{
						Role: "assistant",
						ToolCalls: []transport.ToolCall{{
							ID: "call_1", Type: "function",
							Function: transport.FunctionCall{Name: "bash", Arguments: `{"command":"ls"}`},
						}},
					},
					FinishReason: "tool_calls",
				}},
			})
		case 2:
			if request.MaxTokens == nil || *request.MaxTokens != 1750 {
				t.Errorf("deflator max_tokens = %v, want 1750", request.MaxTokens)
			}
			if request.ReasoningEffort != "minimal" {
				t.Errorf("deflator reasoning_effort = %q, want minimal", request.ReasoningEffort)
			}
			writeJSONResponse(w, "condensed")
		default:
			writeJSONResponse(w, "done")
		}
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "yes"})
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), models,
		testProgressOptions(), "all", solver)

	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	response, raw := post(t, gateway.URL, "ses_deflate_limit", first)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	long := strings.Repeat("x", 5000)
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"run"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"}],"stream":false}`
	response, raw = post(t, gateway.URL, "ses_deflate_limit", second)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() < 3 {
		t.Fatalf("upstream calls = %d, want at least 3", upstream.callCount())
	}
}

func TestDeflateDecisionKeepsOriginal(t *testing.T) {
	models := strings.ReplaceAll(testModelsYAML, "effort: []", "effort: [minimal, high]")
	upstream := &fakeUpstream{respond: func(call int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			writeJSONResponse(w, "condensed")
			return
		}
		if call == 1 {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(transport.ChatResponse{
				ID: "up-1", Object: "chat.completion", Created: 1, Model: "exp-a",
				Choices: []transport.ChatChoice{{
					Index: 0,
					Message: transport.Message{
						Role: "assistant",
						ToolCalls: []transport.ToolCall{{
							ID: "call_1", Type: "function",
							Function: transport.FunctionCall{Name: "bash", Arguments: `{"command":"ls"}`},
						}},
					},
					FinishReason: "tool_calls",
				}},
			})
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "no"})
	gateway := newGatewayFixtureWith(t, startUpstream(t, upstream), models,
		testProgressOptions(), "all", solver)

	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	if response, raw := post(t, gateway.URL, "ses_deflate_keep", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	long := strings.Repeat("x", 5000)
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"run"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"}],"stream":false}`
	if response, raw := post(t, gateway.URL, "ses_deflate_keep", second); response.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("upstream calls = %d, want 2: the deflator must not run when Jev keeps the original", upstream.callCount())
	}
	found := false
	for _, message := range upstream.request(1).Messages {
		if message.Role != "tool" {
			continue
		}
		text, _ := message.Text()
		if text != strings.Repeat("x", 5000) {
			t.Fatalf("tool result = %q, want the original", text)
		}
		found = true
	}
	if !found {
		t.Fatal("the tool result is missing from the expert prompt")
	}
}

func TestModelsManifestIsAdditive(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "unused")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))
	response, err := http.Get(gateway.URL + "/v1/models")
	if err != nil {
		t.Fatalf("get models: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var payload struct {
		Object string `json:"object"`
		Data   []struct {
			ID              string `json:"id"`
			Object          string `json:"object"`
			OwnedBy         string `json:"owned_by"`
			SchemaVersion   int    `json:"schema_version"`
			CatalogRevision string `json:"catalog_revision"`
			UpdatedAtMS     int64  `json:"updated_at_ms"`
			EventID         string `json:"event_id"`
			Capabilities    struct {
				Tools     bool     `json:"tools"`
				Input     []string `json:"input"`
				Output    []string `json:"output"`
				Reasoning struct {
					Supported bool     `json:"supported"`
					Effort    []string `json:"effort"`
				} `json:"reasoning"`
			} `json:"x-coffee-capabilities"`
			Limit    map[string]int64 `json:"x-coffee-limit"`
			Variants []struct {
				ID string `json:"id"`
			} `json:"x-coffee-variants"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode models: %v", err)
	}
	if len(payload.Data) != 1 || payload.Data[0].ID != "coffee-opencode-llm" {
		t.Fatalf("data = %+v", payload.Data)
	}
	entry := payload.Data[0]
	if entry.Object != "model" || entry.OwnedBy != "coffeebreak" {
		t.Fatalf("legacy fields changed: %+v", entry)
	}
	if entry.SchemaVersion != 1 || entry.CatalogRevision == "" || entry.UpdatedAtMS == 0 || entry.EventID == "" {
		t.Fatalf("manifest metadata = %+v", entry)
	}
	if !entry.Capabilities.Reasoning.Supported || len(entry.Capabilities.Reasoning.Effort) != 5 {
		t.Fatalf("reasoning = %+v", entry.Capabilities.Reasoning)
	}
	if entry.Capabilities.Reasoning.Effort[0] != "minimal" || entry.Capabilities.Reasoning.Effort[4] != "max" {
		t.Fatalf("effort = %v", entry.Capabilities.Reasoning.Effort)
	}
	if len(entry.Variants) != 5 || entry.Limit["context"] == 0 || entry.Limit["output"] == 0 {
		t.Fatalf("variants/limit = %+v / %+v", entry.Variants, entry.Limit)
	}
}

// storeRows maps catalog models into plain storage rows for the fixture.
func storeRows(models []catalog.Model) []store.CatalogModel {
	rows := make([]store.CatalogModel, 0, len(models))
	for _, model := range models {
		rows = append(rows, store.CatalogModel{
			ID:             model.ID,
			ContextTokens:  int64(model.ContextTokens),
			MaxOutput:      int64(model.MaxOutput),
			CostInPerMTok:  model.CostInPerMTok,
			CostOutPerMTok: model.CostOutPerMTok,
			QualityTier:    model.QualityTier,
			Retention:      model.Retention,
			QuotaGroup:     model.Quota.Group,
			Strengths:      model.Strengths,
			Limits:         model.Limits,
			Enabled:        model.Enabled,
			Roles:          model.Roles,
			Efforts:        model.Effort,
			Modalities:     model.Modalities,
			Features:       model.Features,
			Groups:         model.GroupSources,
		})
	}
	return rows
}
