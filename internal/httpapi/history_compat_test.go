package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

func TestClientToolBatchIsPreservedAndReasoningIsDropped(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "done")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))
	body := `{"model":"coffee-opencode-llm","stream":false,"messages":[` +
		`{"role":"user","content":"inspect"},` +
		`{"role":"assistant","content":null,"reasoning_content":"reasoning preserved",` +
		`"tool_calls":[{"id":"call_a","type":"function","function":{"name":"read","arguments":"{}"}},` +
		`{"id":"call_b","type":"function","function":{"name":"read","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_a","content":"result a"},` +
		`{"role":"tool","tool_call_id":"call_b","content":"result b"},` +
		`{"role":"user","content":"continue"}]}`
	response, raw := post(t, gateway.URL, "ses_history_compat", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	request := upstream.request(0)
	var batch *transport.Message
	for i := range request.Messages {
		message := &request.Messages[i]
		for _, call := range message.ToolCalls {
			if call.ID == "call_a" {
				batch = message
			}
		}
	}
	if batch == nil || len(batch.ToolCalls) != 2 ||
		batch.ToolCalls[0].ID != "call_a" || batch.ToolCalls[1].ID != "call_b" {
		t.Fatalf("parallel client tool calls were not preserved together: %+v", request.Messages)
	}
	if batch.ReasoningContent != nil || batch.Reasoning != nil || batch.ReasoningText != nil {
		t.Fatalf("reasoning fields must not reach the model: %+v", batch)
	}
}

func TestAssistantHistoryTextIsExcludedFromModelContext(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "done")
	}}
	gateway := newGateway(t, startUpstream(t, upstream))
	body := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"first request"},` +
		`{"role":"assistant","content":"intermediate reasoning and final answer"},` +
		`{"role":"user","content":"continue"}]}`
	response, raw := post(t, gateway.URL, "ses_assistant_text_filter", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	request := upstream.request(0)
	foundFirst, foundCurrent := false, false
	for _, message := range request.Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(text, "intermediate reasoning and final answer") {
			t.Fatal("ambiguous assistant history text reached the model")
		}
		foundFirst = foundFirst || text == "first request"
		foundCurrent = foundCurrent || text == "continue"
	}
	if !foundFirst || !foundCurrent {
		t.Fatalf("human history missing: first=%v current=%v", foundFirst, foundCurrent)
	}
}

func longHistory(t *testing.T) string {
	return historyWithMessages(t, 50)
}

func historyWithMessages(t *testing.T, count int) string {
	t.Helper()
	messages := make([]transport.Message, 0, count+1)
	for i := 0; i < count; i++ {
		messages = append(messages, transport.TextMessage("user", fmt.Sprintf("entry-%02d %s", i, strings.Repeat("x", 5000))))
	}
	messages = append(messages, transport.TextMessage("user", "latest request"))
	body, err := json.Marshal(transport.ChatRequest{
		Model: "coffee-opencode-llm", Messages: messages,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestCompactionUsesConfiguredInputThreshold(t *testing.T) {
	for _, test := range []struct {
		name        string
		messages    int
		wantCompact bool
	}{
		{name: "small earlier turns stay intact", messages: 1},
		{name: "large earlier turn folds into a summary", messages: 20, wantCompact: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
				system, err := request.Messages[0].Text()
				if err != nil {
					t.Error(err)
				}
				if strings.Contains(system, "You are the deflator") {
					writeJSONResponse(w, "summary")
				} else {
					writeJSONResponse(w, "done")
				}
			}}
			models := strings.ReplaceAll(testModelsYAML, "context_tokens: 200000", "context_tokens: 32768")
			fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), models,
				testProgressOptions(), "all", nil)
			response, raw := post(t, fixture.URL, "ses_threshold", historyWithMessages(t, test.messages))
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
			}
			if compacted := upstream.callCount() > 1; compacted != test.wantCompact {
				t.Fatalf("compacted = %v, want %v", compacted, test.wantCompact)
			}
		})
	}
}

func TestHistoryOverModelWindowIsCompactedAfterSelection(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		if len(request.Messages) > 0 {
			system, err := request.Messages[0].Text()
			if err != nil {
				t.Error(err)
			}
			if strings.Contains(system, "You are the deflator") {
				// Return a summary that mentions the newest entries so the test
				// can verify they survived compaction.
				writeJSONResponse(w, "compacted: entry-45, entry-46, entry-47, entry-48, entry-49")
				return
			}
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	response, raw := post(t, fixture.URL, "ses_long_history", longHistory(t))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() < 2 {
		t.Fatalf("calls = %d, expected compaction before the answer", upstream.callCount())
	}
	deflatorCalls := 0
	for call := 0; call < upstream.callCount(); call++ {
		request := upstream.request(call)
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(system, "You are the deflator") {
			continue
		}
		deflatorCalls++
		payload, err := request.Messages[len(request.Messages)-1].Text()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(payload, "entry-00") || !strings.Contains(payload, "entry-49") {
			t.Fatal("one-shot compaction did not send the complete history prefix")
		}
	}
	if deflatorCalls != 1 {
		t.Fatalf("deflator calls = %d, want one request for the complete history", deflatorCalls)
	}
	// The newest entries and the summary must reach the expert.
	inspected := upstream.request(upstream.callCount() - 1)
	joined := strings.Join(func() []string {
		var out []string
		for _, message := range inspected.Messages {
			text, err := message.Text()
			if err == nil {
				out = append(out, text)
			}
		}
		return out
	}(), " ")
	// The tail message must be present.
	if !strings.Contains(joined, "latest request") {
		t.Fatal("latest request was lost during compaction")
	}
	// The compacted summary must contain at least one entry reference.
	if !strings.Contains(joined, "entry-") {
		t.Fatal("no entries survived compaction")
	}
}

func TestHistoryCompactionStripsReasoningFromDeflatorInput(t *testing.T) {
	reasoning := "<think>PRIVATE-REASONING</think>"
	messages := make([]transport.Message, 0, 53)
	for index := 0; index < 50; index++ {
		content := fmt.Sprintf("entry-%02d %s", index, strings.Repeat("x", 5000))
		if index == 0 {
			content += "<think>PRIVATE-USER</think>"
		}
		messages = append(messages, transport.TextMessage("user", content))
		if index == 0 {
			messages = append(messages, transport.Message{
				Role:             "assistant",
				ReasoningContent: &reasoning,
				ToolCalls: []transport.ToolCall{{
					ID: "call_reasoning", Type: "function",
					Function: transport.FunctionCall{
						Name: "read", Arguments: `{"note":"<think>PRIVATE-ARG</think>"}`,
					},
				}},
			})
		}
	}
	messages = append(messages, transport.TextMessage("user", "latest request"))
	body, err := json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
	if err != nil {
		t.Fatal(err)
	}

	var deflatorPayload string
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			deflatorPayload, err = request.Messages[len(request.Messages)-1].Text()
			if err != nil {
				t.Error(err)
			}
			writeJSONResponse(w, "<think>PRIVATE-SUMMARY</think>summary: entry-00")
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	response, raw := post(t, fixture.URL, "ses_compaction_reasoning", string(body))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if deflatorPayload == "" {
		t.Fatal("history was not sent to the deflator")
	}
	for _, private := range []string{"<think>", "PRIVATE-USER", "PRIVATE-REASONING", "PRIVATE-ARG"} {
		if strings.Contains(deflatorPayload, private) {
			t.Fatalf("reasoning reached the deflator as %q", private)
		}
	}

	expertRequest, err := json.Marshal(upstream.request(upstream.callCount() - 1).Messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"<think>", "PRIVATE-SUMMARY", "PRIVATE-REASONING", "PRIVATE-ARG"} {
		if strings.Contains(string(expertRequest), private) {
			t.Fatalf("reasoning reached the expert as %q", private)
		}
	}
	if !strings.Contains(string(expertRequest), "summary: entry-00") {
		t.Fatal("the cleaned summary did not reach the expert")
	}
}

func TestCompactionUsesSelectedModelWindow(t *testing.T) {
	models := strings.Replace(testModelsYAML, "context_tokens: 32768", "context_tokens: 12288", 2)
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			writeJSONResponse(w, "summary")
		} else {
			writeJSONResponse(w, "done")
		}
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), models, testProgressOptions(), "all", solver)
	response, raw := post(t, fixture.URL, "ses_expert_window", historyWithMessages(t, 12))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() < 2 {
		t.Fatal("the selected expert's smaller window did not trigger compaction")
	}
	if got := upstream.request(upstream.callCount() - 1).Model; got != "exp-a" {
		t.Fatalf("final model = %q, want exp-a", got)
	}
}

func TestCompactionFailsWhenDeflatorWindowIsTooSmall(t *testing.T) {
	models := strings.ReplaceAll(testModelsYAML, "context_tokens: 200000", "context_tokens: 32768")
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), models,
		testProgressOptions(), "all", solver)
	response, raw := post(t, fixture.URL, "ses_small_deflator_window", longHistory(t))
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("status = %d, want compaction failure: %s", response.StatusCode, raw)
	}
	if upstream.callCount() != 0 {
		t.Fatalf("upstream calls = %d, want no tail-fallback model call", upstream.callCount())
	}
}

func TestCompactionReusesSummaryWhileEffectivePromptFits(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			writeJSONResponse(w, "summary")
		} else {
			writeJSONResponse(w, "done")
		}
	}}
	models := strings.Replace(testModelsYAML, "context_tokens: 200000", "context_tokens: 32768", 1)
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), models,
		testProgressOptions(), "all", nil)
	body := longHistory(t)
	if response, raw := post(t, fixture.URL, "ses_incremental_summary", body); response.StatusCode != http.StatusOK {
		t.Fatalf("first request: status = %d, body = %s", response.StatusCode, raw)
	}
	firstDeflatorCalls := 0
	for i := 0; i < upstream.callCount(); i++ {
		req := upstream.request(i)
		for _, message := range req.Messages {
			text, err := message.Text()
			if err == nil && strings.Contains(text, "You are the deflator") {
				firstDeflatorCalls++
				break
			}
		}
	}
	var request transport.ChatRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		t.Fatal(err)
	}
	request.Messages = append(request.Messages,
		transport.TextMessage("assistant", "done"),
		transport.TextMessage("user", strings.Repeat("y", 3000)),
	)
	rawBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if response, raw := post(t, fixture.URL, "ses_incremental_summary", string(rawBody)); response.StatusCode != http.StatusOK {
		t.Fatalf("second request: status = %d, body = %s", response.StatusCode, raw)
	}
	// The cached summary plus the new tail fits the selected model, so another
	// deflator call is unnecessary even though the raw archive remains large.
	secondDeflatorCalls := 0
	for i := firstDeflatorCalls; i < upstream.callCount(); i++ {
		req := upstream.request(i)
		for _, message := range req.Messages {
			text, err := message.Text()
			if err == nil && strings.Contains(text, "You are the deflator") {
				secondDeflatorCalls++
				break
			}
		}
	}
	if secondDeflatorCalls != 0 {
		t.Fatalf("second turn made %d unnecessary deflator calls", secondDeflatorCalls)
	}
	var prompt strings.Builder
	for _, message := range upstream.request(upstream.callCount() - 1).Messages {
		text, err := message.Text()
		if err == nil {
			prompt.WriteString(text)
			prompt.WriteByte('\n')
		}
	}
	if !strings.Contains(prompt.String(), "Summary of the earlier conversation:\nsummary") ||
		!strings.Contains(prompt.String(), strings.Repeat("y", 100)) ||
		strings.Contains(prompt.String(), "entry-00") {
		t.Fatalf("cached summary and new tail were not used:\n%s", prompt.String()[:min(prompt.Len(), 300)])
	}
}

func TestCompactionProgressOnlyAppearsWhenSummaryIsRecomputed(t *testing.T) {
	streamCalls := 0
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			writeJSONResponse(w, "history summary")
			return
		}
		if !request.Stream {
			writeJSONResponse(w, "done")
			return
		}
		streamCalls++
		if streamCalls == 1 {
			writeSSE(w,
				`{"id":"up-tool","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[`+
					`{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_progress",`+
					`"type":"function","function":{"name":"bash","arguments":"{}"}}]}}]}`,
				`{"id":"up-tool","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`[DONE]`,
			)
			return
		}
		writeSSE(w,
			`{"id":"up-final","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{"role":"assistant","content":"done"}}]}`,
			`{"id":"up-final","object":"chat.completion.chunk","created":1,"model":"exp-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	var initial transport.ChatRequest
	if err := json.Unmarshal([]byte(longHistory(t)), &initial); err != nil {
		t.Fatal(err)
	}
	initial.Stream = true
	firstBody, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse, firstRaw := post(t, fixture.URL, "ses_compaction_progress", string(firstBody))
	if firstResponse.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", firstResponse.StatusCode, firstRaw)
	}
	if !strings.Contains(firstRaw, "compaction:") {
		t.Fatalf("first response did not announce compaction:\n%s", firstRaw)
	}
	second := initial
	second.Messages = append(append([]transport.Message(nil), initial.Messages...),
		transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
			ID: "call_progress", Type: "function",
			Function: transport.FunctionCall{Name: "bash", Arguments: "{}"},
		}}},
		transport.Message{Role: "tool", ToolCallID: "call_progress", Content: transport.TextContent("tool result")},
	)
	secondBody, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	secondResponse, secondRaw := post(t, fixture.URL, "ses_compaction_progress", string(secondBody))
	if secondResponse.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", secondResponse.StatusCode, secondRaw)
	}
	if strings.Contains(secondRaw, "compaction:") {
		t.Fatalf("reused summary emitted another compaction status:\n%s", secondRaw)
	}
}

func TestFailedHistoryCompactionStopsAndRecordsDeflatorUsage(t *testing.T) {
	for _, test := range []struct {
		name   string
		text   string
		finish string
	}{
		{name: "empty", finish: "stop"},
		{name: "truncated", text: "partial", finish: "content_filter"},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
				system, err := request.Messages[0].Text()
				if err != nil {
					t.Error(err)
				}
				if strings.Contains(system, "You are the deflator") {
					w.Header().Set("Content-Type", "application/json")
					promptTokens, completionTokens := int64(7), int64(3)
					response := transport.ChatResponse{Choices: []transport.ChatChoice{{
						Message:      transport.TextMessage("assistant", test.text),
						FinishReason: test.finish,
					}}, Usage: &transport.Usage{PromptTokens: &promptTokens, CompletionTokens: &completionTokens}}
					if err := json.NewEncoder(w).Encode(response); err != nil {
						t.Error(err)
					}
					return
				}
				writeJSONResponse(w, "done")
			}}
			solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
			fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
				testProgressOptions(), "all", solver)
			response, raw := post(t, fixture.URL, "ses_compaction_failure", longHistory(t))
			if response.StatusCode != http.StatusUnprocessableEntity ||
				!strings.Contains(raw, "start a new OpenCode session") {
				t.Fatalf("status = %d, want compaction failure: %s", response.StatusCode, raw)
			}
			if upstream.callCount() != 1 {
				t.Fatalf("upstream calls = %d, want only one deflator call", upstream.callCount())
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
			var calls, input, output int
			err = db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0)
				FROM model_call WHERE role = 'deflator'`).Scan(&calls, &input, &output)
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || input != 7 || output != 3 {
				t.Fatalf("failed deflator usage not recorded: calls=%d input=%d output=%d", calls, input, output)
			}
		})
	}
}

func TestCompactionFailureBlocksDialogueUntilNewSession(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			writeJSONResponse(w, "")
			return
		}
		writeJSONResponse(w, "tail-reached")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	body := longHistory(t)
	response, raw := post(t, fixture.URL, "ses_compaction_failure_block", body)
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("status = %d, want compaction failure: %s", response.StatusCode, raw)
	}
	callsAfterFailure := upstream.callCount()
	response, raw = post(t, fixture.URL, "ses_compaction_failure_block", body)
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("retry status = %d, want blocked session: %s", response.StatusCode, raw)
	}
	if upstream.callCount() != callsAfterFailure {
		t.Fatalf("blocked retry made upstream calls: before=%d after=%d",
			callsAfterFailure, upstream.callCount())
	}
	newSession := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"new session"}]}`
	response, raw = post(t, fixture.URL, "ses_compaction_failure_new", newSession)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("new session status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() <= callsAfterFailure {
		t.Fatalf("new session did not reach the model: calls=%d", upstream.callCount())
	}
}

func TestOversizedSingleMessageFailsWithoutTailFallback(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "done")
	}}
	models := strings.ReplaceAll(testModelsYAML, "context_tokens: 200000", "context_tokens: 32768")
	gateway := newGatewayFixture(t, startUpstream(t, upstream), models)
	body, err := json.Marshal(transport.ChatRequest{
		Model:    "coffee-opencode-llm",
		Messages: []transport.Message{transport.TextMessage("user", strings.Repeat("x", 99999))},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, raw := post(t, gateway.URL, "ses_single_long_message", string(body))
	if response.StatusCode != http.StatusUnprocessableEntity || upstream.callCount() != 0 ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("status=%d calls=%d body=%s", response.StatusCode, upstream.callCount(), raw)
	}
}

func TestClientCheckpointReplacesOldPrompt(t *testing.T) {
	upstream := &fakeUpstream{respond: func(_ int, _ transport.ChatRequest, w http.ResponseWriter) {
		writeJSONResponse(w, "done")
	}}
	fixture := newGatewayFixture(t, startUpstream(t, upstream), testModelsYAML)
	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"obsolete history"}]}`
	if response, raw := post(t, fixture.URL, "ses_checkpoint_replace", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first request: status = %d, body = %s", response.StatusCode, raw)
	}
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"<conversation-checkpoint>summary</conversation-checkpoint>"},` +
		`{"role":"user","content":"new request"}]}`
	if response, raw := post(t, fixture.URL, "ses_checkpoint_replace", second); response.StatusCode != http.StatusOK {
		t.Fatalf("second request: status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() != 2 {
		t.Fatalf("calls = %d, want 2", upstream.callCount())
	}
	hasCheckpoint, hasNew := false, false
	for _, message := range upstream.request(1).Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatal(err)
		}
		if text == "obsolete history" {
			t.Fatal("old history reached the model after the client checkpoint")
		}
		hasCheckpoint = hasCheckpoint || strings.Contains(text, "<conversation-checkpoint>")
		hasNew = hasNew || text == "new request"
	}
	if !hasCheckpoint || !hasNew {
		t.Fatalf("checkpoint=%v, new request=%v", hasCheckpoint, hasNew)
	}
	dialogue, err := fixture.store.EnsureDialogue(context.Background(), "ses_checkpoint_replace", "header", 24)
	if err != nil {
		t.Fatal(err)
	}
	records, err := fixture.store.Messages(context.Background(), dialogue.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved := make(map[string]int)
	for _, record := range records {
		if record.Role == "user" {
			saved[record.Content]++
		}
	}
	for _, text := range []string{"obsolete history", "new request", "<conversation-checkpoint>summary</conversation-checkpoint>"} {
		if saved[text] != 1 {
			t.Fatalf("archived message %q: got %d, want 1", text, saved[text])
		}
	}
}

func TestExpertDelegationOnlyCatalog(t *testing.T) {
	var gotTools []transport.Tool
	var gotPrompt string
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		gotTools = request.Tools
		system, err := request.Messages[0].Text()
		if err == nil {
			gotPrompt = system
		}
		writeJSONResponse(w, "done")
	}}
	tools := `[` +
		`{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}},` +
		`{"type":"function","function":{"name":"edit","parameters":{"type":"object"}}},` +
		`{"type":"function","function":{"name":"question","parameters":{"type":"object"}}}]`
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureOpts(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", func(opts *engine.Options) { opts.ExpertDelegationOnly = true }, solver)
	body := `{"model":"coffee-opencode-llm","tools":` + tools +
		`,"messages":[{"role":"user","content":"run"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_delegation_only", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	names := make(map[string]bool, len(gotTools))
	for _, tool := range gotTools {
		names[tool.Function.Name] = true
	}
	if names["bash"] || names["edit"] {
		t.Fatalf("client tools reached the expert: %v", names)
	}
	if !names["gateway_assistant"] || !names["question"] {
		t.Fatalf("expert tools = %v, want gateway_assistant and question", names)
	}
	if !strings.Contains(gotPrompt, "Delegation is your only way to run client tools") {
		t.Fatalf("the delegation-only prompt block is missing:\n%s", gotPrompt)
	}
}

func TestExpertDelegationOnlyKeepsFullCatalogWhenDisabled(t *testing.T) {
	var gotTools []transport.Tool
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		gotTools = request.Tools
		writeJSONResponse(w, "done")
	}}
	tools := `[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}]`
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	body := `{"model":"coffee-opencode-llm","tools":` + tools +
		`,"messages":[{"role":"user","content":"run"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_delegation_full", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.StatusCode, raw)
	}
	names := make(map[string]bool, len(gotTools))
	for _, tool := range gotTools {
		names[tool.Function.Name] = true
	}
	if !names["bash"] || !names["gateway_assistant"] {
		t.Fatalf("expert tools = %v, want the full client catalog and gateway_assistant", names)
	}
}

func TestNewTurnCompactsPreviousHistory(t *testing.T) {
	sawSubject := false
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			user, err := request.Messages[len(request.Messages)-1].Text()
			if err != nil {
				t.Error(err)
			}
			if strings.Contains(user, "The current human request, for context:") &&
				strings.Contains(user, "second task") {
				sawSubject = true
			}
			writeJSONResponse(w, "summary of the earlier work")
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	long := strings.Repeat("x", 50000)
	first := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"first task"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_2","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_2","content":"` + long + `"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_turn_boundary", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"first task"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_2","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_2","content":"` + long + `"},` +
		`{"role":"assistant","content":"done"},` +
		`{"role":"user","content":"second task"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_turn_boundary", second); response.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", response.StatusCode, raw)
	}
	if upstream.callCount() < 3 {
		t.Fatalf("upstream calls = %d, want the deflator to run before the expert", upstream.callCount())
	}
	request := upstream.request(upstream.callCount() - 1)
	joined := ""
	for _, message := range request.Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatal(err)
		}
		joined += text + "\n"
	}
	if strings.Contains(joined, long) {
		t.Fatal("the previous turn history reached the expert uncompacted")
	}
	if !strings.Contains(joined, "Summary of the earlier conversation:\nsummary of the earlier work") {
		t.Fatalf("the summary is missing from the expert prompt:\n%s", joined[:min(len(joined), 400)])
	}
	if !sawSubject {
		t.Fatal("the deflator did not receive the current human request as context")
	}
	if !strings.Contains(joined, "second task") {
		t.Fatal("the new human turn is missing from the expert prompt")
	}
}

func TestDeflationAdjusterReceivesTailWithinRequestBudget(t *testing.T) {
	result := "<think>PRIVATE-THOUGHT</think>" + strings.Repeat("x", 60000) + "LATEST-RESULT-TAIL"
	callArguments := `{"command":"inspect","note":"<think>PRIVATE-ARG</think>"}`
	upstream := &fakeUpstream{respond: func(call int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			user, err := request.Messages[len(request.Messages)-1].Text()
			if err != nil {
				t.Error(err)
			}
			if strings.Contains(user, "<think>") || strings.Contains(user, "PRIVATE-THOUGHT") ||
				strings.Contains(user, "PRIVATE-ARG") {
				t.Error("reasoning tags or content reached the deflator")
			}
			writeJSONResponse(w, "condensed result")
			return
		}
		if call == 1 {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
				Message: transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
					ID: "call_tail", Type: "function",
					Function: transport.FunctionCall{Name: "bash", Arguments: callArguments},
				}}},
				FinishReason: "tool_calls",
			}}}); err != nil {
				t.Error(err)
			}
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, adjuster := startJevWith(t, "accept", map[string]string{
		"start": "no", "deflate": "yes",
	})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_deflate_tail", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	second, err := json.Marshal(transport.ChatRequest{
		Model: "coffee-opencode-llm",
		Messages: []transport.Message{
			transport.TextMessage("user", "run"),
			{Role: "assistant", ToolCalls: []transport.ToolCall{{
				ID: "call_tail", Type: "function",
				Function: transport.FunctionCall{Name: "bash", Arguments: callArguments},
			}}},
			{Role: "tool", ToolCallID: "call_tail", Content: transport.TextContent(result)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response, raw := post(t, fixture.URL, "ses_deflate_tail", string(second)); response.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, body = %s", response.StatusCode, raw)
	}
	request := adjuster.deflateRequest()
	if request == nil {
		t.Fatal("Jev did not receive a deflation decision")
	}
	state, _ := request["state"].(map[string]any)
	evidence, _ := state["evidence"].(string)
	truncated, _ := state["truncated"].(bool)
	if !truncated {
		t.Fatal("oversized Jev evidence was not marked truncated")
	}
	if !strings.HasSuffix(evidence, "LATEST-RESULT-TAIL") {
		t.Fatalf("Jev evidence lost the newest result suffix: %q", evidence[len(evidence)-min(len(evidence), 80):])
	}
	if strings.Contains(evidence, "<think>") || strings.Contains(evidence, "PRIVATE-THOUGHT") ||
		strings.Contains(evidence, "PRIVATE-ARG") {
		t.Fatalf("reasoning tags or content reached Jev: %q", evidence[len(evidence)-min(len(evidence), 120):])
	}
	rawRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(rawRequest) > 48000 {
		t.Fatalf("Jev request = %d bytes, want at most 48000", len(rawRequest))
	}
	expertRequest := upstream.request(upstream.callCount() - 1)
	for _, message := range expertRequest.Messages {
		if message.Role != "tool" {
			continue
		}
		text, err := message.Text()
		if err != nil {
			t.Fatal(err)
		}
		if text != "condensed result" {
			t.Fatalf("expert tool result = %q, want condensed result", text)
		}
		return
	}
	t.Fatal("expert tool result is missing")
}

func TestDeflationSkipsShortResultsAndLowConfidence(t *testing.T) {
	for _, test := range []struct {
		name        string
		resultSize  int
		choice      string
		confidence  float64
		wantJev     bool
		wantFailure bool
	}{
		{name: "at_threshold", resultSize: 2000, choice: "yes", confidence: 0.9},
		{name: "confidence_at_fifty_percent", resultSize: 2001, choice: "yes", confidence: 0.5, wantJev: true},
		{name: "invalid_jev_choice", resultSize: 2001, choice: "invalid", confidence: 0.9,
			wantJev: true, wantFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
				if call == 1 {
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
						Message: transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
							ID: "call_threshold", Type: "function",
							Function: transport.FunctionCall{Name: "bash", Arguments: `{}`},
						}}},
						FinishReason: "tool_calls",
					}}}); err != nil {
						t.Error(err)
					}
					return
				}
				writeJSONResponse(w, "done")
			}}
			solver, adjuster := startJevWithConfidence(t, "accept",
				map[string]string{"start": "no", "deflate": test.choice}, test.confidence)
			fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
				testProgressOptions(), "all", solver)
			first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
			if response, raw := post(t, fixture.URL, "ses_deflate_threshold_"+test.name, first); response.StatusCode != http.StatusOK {
				t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
			}
			result := strings.Repeat("x", test.resultSize)
			second := `{"model":"coffee-opencode-llm","messages":[` +
				`{"role":"user","content":"run"},` +
				`{"role":"assistant","tool_calls":[{"id":"call_threshold","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
				`{"role":"tool","tool_call_id":"call_threshold","content":"` + result + `"}],"stream":false}`
			response, raw := post(t, fixture.URL, "ses_deflate_threshold_"+test.name, second)
			if test.wantFailure {
				if response.StatusCode != http.StatusUnprocessableEntity ||
					!strings.Contains(raw, "start a new OpenCode session") {
					t.Fatalf("second status = %d, want compaction failure: %s", response.StatusCode, raw)
				}
				calls := upstream.callCount()
				response, raw = post(t, fixture.URL, "ses_deflate_threshold_"+test.name, second)
				if response.StatusCode != http.StatusUnprocessableEntity || upstream.callCount() != calls {
					t.Fatalf("blocked retry status=%d calls=%d want calls=%d body=%s",
						response.StatusCode, upstream.callCount(), calls, raw)
				}
			} else if response.StatusCode != http.StatusOK {
				t.Fatalf("second status = %d, body = %s", response.StatusCode, raw)
			}
			hasDecision := adjuster.deflateRequest() != nil
			if hasDecision != test.wantJev {
				t.Fatalf("Jev called = %v, want %v", hasDecision, test.wantJev)
			}
			wantCalls := 2
			if test.wantFailure {
				wantCalls = 1
			}
			if upstream.callCount() != wantCalls {
				t.Fatalf("upstream calls = %d, want %d", upstream.callCount(), wantCalls)
			}
		})
	}
}

func TestUnshrunkDeflationStopsAndBlocksDialogue(t *testing.T) {
	bloat := strings.Repeat("f", 3500)
	upstream := &fakeUpstream{respond: func(call int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			user, err := request.Messages[len(request.Messages)-1].Text()
			if err != nil {
				t.Error(err)
			}
			if !strings.Contains(user, "The human requested: run") ||
				!strings.Contains(user, "the result of the tool call bash(") {
				t.Errorf("the deflator got no compression context: %.120s", user)
			}
			writeJSONResponse(w, bloat)
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
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "yes"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)

	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_unshrunk_deflation", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	original := strings.Repeat("x", 3000)
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"run"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + original + `"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_unshrunk_deflation", second)
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("second status = %d, want deflation failure: %s", response.StatusCode, raw)
	}
	calls := upstream.callCount()
	response, raw = post(t, fixture.URL, "ses_unshrunk_deflation", second)
	if response.StatusCode != http.StatusUnprocessableEntity || upstream.callCount() != calls {
		t.Fatalf("blocked retry status=%d calls=%d want calls=%d body=%s",
			response.StatusCode, upstream.callCount(), calls, raw)
	}
}

func TestOversizedSummaryIsFittedToBudget(t *testing.T) {
	oversized := strings.Repeat("f", 30000)
	sawSubject := false
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			user, err := request.Messages[len(request.Messages)-1].Text()
			if err != nil {
				t.Error(err)
			}
			if strings.Contains(user, "The current human request, for context:") &&
				strings.Contains(user, "second task") {
				sawSubject = true
			}
			writeJSONResponse(w, oversized)
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML,
		testProgressOptions(), "all", solver)
	long := strings.Repeat("x", 50000)
	first := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"first task"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_2","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_2","content":"` + long + `"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_oversized_summary", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	second := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"first task"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_2","type":"function","function":{"name":"bash","arguments":"{}"}}]},` +
		`{"role":"tool","tool_call_id":"call_2","content":"` + long + `"},` +
		`{"role":"assistant","content":"done"},` +
		`{"role":"user","content":"second task"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_oversized_summary", second)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want the turn to continue after trimming: %s", response.StatusCode, raw)
	}
	request := upstream.request(upstream.callCount() - 1)
	for _, message := range request.Messages {
		text, err := message.Text()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(text, "Summary of the earlier conversation") {
			summary := strings.TrimPrefix(text, "Summary of the earlier conversation:\n")
			summaryTokens := int64(len(summary)+3) / 4
			usableTokens := int64(32768 - 4096)
			summaryBudget := usableTokens * 80 / 100 * 15 / 100
			if summaryTokens > summaryBudget {
				t.Fatalf("summary used %d estimated tokens, budget is %d", summaryTokens, summaryBudget)
			}
			if !strings.HasSuffix(summary, "f") {
				t.Fatal("summary did not preserve the newest suffix")
			}
			return
		}
	}
	if !sawSubject {
		t.Fatal("the deflator did not receive the current human request as context")
	}
	t.Fatal("the summary is missing from the expert prompt")
}

func TestDeflatorLengthFinishStopsTheDialogue(t *testing.T) {
	oversized := strings.Repeat("d", 5000)
	upstream := &fakeUpstream{respond: func(_ int, request transport.ChatRequest, w http.ResponseWriter) {
		system, err := request.Messages[0].Text()
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(system, "You are the deflator") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
				Message:      transport.TextMessage("assistant", oversized),
				FinishReason: "length",
			}}})
			return
		}
		writeJSONResponse(w, "done")
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "yes"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML, testProgressOptions(), "all", solver)

	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_length_finish", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	long := strings.Repeat("x", 5000)
	body := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"run"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_length_finish", body)
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("status = %d, want deflation failure: %s", response.StatusCode, raw)
	}
}

func ptrInt64(value int64) *int64 { return &value }

func TestTruncatedDeflatorAnswerIsNotStored(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		if call == 1 {
			writeJSONResponse(w, "done")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transport.ChatResponse{Choices: []transport.ChatChoice{{
			Message:      transport.TextMessage("assistant", "half-written digest"),
			FinishReason: "content_filter",
		}}})
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no", "deflate": "yes"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML, testProgressOptions(), "all", solver)
	first := `{"model":"coffee-opencode-llm","messages":[{"role":"user","content":"run"}],"stream":false}`
	if response, raw := post(t, fixture.URL, "ses_truncated_deflator", first); response.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", response.StatusCode, raw)
	}
	long := strings.Repeat("x", 5000)
	body := `{"model":"coffee-opencode-llm","messages":[` +
		`{"role":"user","content":"run"},` +
		`{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"command\":\"ls\"}"}}]},` +
		`{"role":"tool","tool_call_id":"call_1","content":"` + long + `"}],"stream":false}`
	response, raw := post(t, fixture.URL, "ses_truncated_deflator", body)
	if response.StatusCode != http.StatusUnprocessableEntity ||
		!strings.Contains(raw, "start a new OpenCode session") {
		t.Fatalf("status = %d, want deflation failure: %s", response.StatusCode, raw)
	}
	dialogue, err := fixture.store.EnsureDialogue(context.Background(), "ses_truncated_deflator", "header", 24)
	if err != nil {
		t.Fatal(err)
	}
	records, err := fixture.store.Messages(context.Background(), dialogue.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Kind == "tool_result" && record.CallID == "call_1" {
			t.Fatal("tool result was persisted after deflation failed")
		}
		if record.Content == "half-written digest" {
			t.Fatal("partial deflator output was persisted")
		}
	}
}

func TestTurnStepLimitSurvivesClientToolRoundTrips(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		response := transport.ChatResponse{Choices: []transport.ChatChoice{{
			Message: transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
				ID: fmt.Sprintf("call-%d", call), Type: "function",
				Function: transport.FunctionCall{Name: "read", Arguments: "{}"},
			}}},
			FinishReason: "tool_calls",
		}}}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}}
	gateway := newGateway(t, startUpstream(t, upstream))
	messages := []transport.Message{transport.TextMessage("user", "inspect")}
	for step := 0; step < 4; step++ {
		body, err := json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
		if err != nil {
			t.Fatal(err)
		}
		response, raw := post(t, gateway.URL, "ses_turn_limit", string(body))
		if response.StatusCode != http.StatusOK {
			t.Fatalf("step %d: status = %d, body = %s", step, response.StatusCode, raw)
		}
		var parsed transport.ChatResponse
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			t.Fatal(err)
		}
		if len(parsed.Choices) != 1 || len(parsed.Choices[0].Message.ToolCalls) != 1 {
			t.Fatalf("step %d did not return a tool call", step)
		}
		call := parsed.Choices[0].Message.ToolCalls[0]
		messages = append(messages, parsed.Choices[0].Message,
			transport.Message{Role: "tool", ToolCallID: call.ID, Content: transport.TextContent("result")})
	}
	body, err := json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	response, raw := post(t, gateway.URL, "ses_turn_limit", string(body))
	if response.StatusCode != http.StatusOK {
		t.Fatalf("pause status = %d, body = %s", response.StatusCode, raw)
	}
	var paused transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &paused); err != nil {
		t.Fatal(err)
	}
	if len(paused.Choices) != 1 || paused.Choices[0].FinishReason != "length" || upstream.callCount() != 4 {
		t.Fatalf("turn was not paused after four calls: calls=%d, body=%s", upstream.callCount(), raw)
	}
	messages = append(messages, paused.Choices[0].Message, transport.TextMessage("user", "continue"))
	body, err = json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	response, raw = post(t, gateway.URL, "ses_turn_limit", string(body))
	if response.StatusCode != http.StatusOK || upstream.callCount() != 5 {
		t.Fatalf("new turn did not reset the budget: status=%d calls=%d body=%s",
			response.StatusCode, upstream.callCount(), raw)
	}
}

func TestTurnStepLimitClosesActiveDelegation(t *testing.T) {
	upstream := &fakeUpstream{respond: func(call int, _ transport.ChatRequest, w http.ResponseWriter) {
		function := transport.FunctionCall{Name: "read", Arguments: "{}"}
		if call == 1 {
			function = transport.FunctionCall{
				Name: "gateway_assistant", Arguments: `{"task":"inspect","scope":"full"}`,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		response := transport.ChatResponse{Choices: []transport.ChatChoice{{
			Message: transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
				ID: fmt.Sprintf("call-%d", call), Type: "function", Function: function,
			}}},
			FinishReason: "tool_calls",
		}}}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	}}
	solver, _ := startJevWith(t, "accept", map[string]string{"start": "no"})
	fixture := newGatewayFixtureWith(t, startUpstream(t, upstream), testModelsYAML, testProgressOptions(), "all", solver)
	messages := []transport.Message{transport.TextMessage("user", "inspect")}
	for step := 0; step < 3; step++ {
		body, err := json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
		if err != nil {
			t.Fatal(err)
		}
		response, raw := post(t, fixture.URL, "ses_delegation_limit", string(body))
		if response.StatusCode != http.StatusOK {
			t.Fatalf("step %d: status=%d body=%s", step, response.StatusCode, raw)
		}
		var parsed transport.ChatResponse
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			t.Fatal(err)
		}
		if len(parsed.Choices) != 1 || len(parsed.Choices[0].Message.ToolCalls) != 1 {
			t.Fatalf("step %d did not return the delegated tool call: %s", step, raw)
		}
		call := parsed.Choices[0].Message.ToolCalls[0]
		messages = append(messages, parsed.Choices[0].Message,
			transport.Message{Role: "tool", ToolCallID: call.ID, Content: transport.TextContent("result")})
	}
	body, err := json.Marshal(transport.ChatRequest{Model: "coffee-opencode-llm", Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	response, raw := post(t, fixture.URL, "ses_delegation_limit", string(body))
	if response.StatusCode != http.StatusOK || upstream.callCount() != 4 {
		t.Fatalf("pause: status=%d calls=%d body=%s", response.StatusCode, upstream.callCount(), raw)
	}
	var parsed transport.ChatResponse
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.Choices) != 1 || parsed.Choices[0].FinishReason != "length" {
		t.Fatalf("delegation was not paused: %s", raw)
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
	var status, reason string
	if err := db.QueryRow(`SELECT status, close_reason FROM delegation`).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "assistant_2_to_expert" || reason != "budget_exhausted" {
		t.Fatalf("delegation status=%q reason=%q", status, reason)
	}
	var pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM delegation_step WHERE status = 'assistant_2_to_client_tools'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("answered delegation steps still pending: %d", pending)
	}
}
