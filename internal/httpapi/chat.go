package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/observe"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type chatRequest struct {
	Model               string          `json:"model"`
	Messages            []chatMessage   `json:"messages"`
	Tools               []chatTool      `json:"tools"`
	ToolChoice          json.RawMessage `json:"tool_choice"`
	Stream              bool            `json:"stream"`
	StreamOptions       *streamOptions  `json:"stream_options"`
	MaxTokens           *int64          `json:"max_tokens"`
	MaxCompletionTokens *int64          `json:"max_completion_tokens"`
	ReasoningEffort     string          `json:"reasoning_effort"`
	Temperature         *float64        `json:"temperature"`
	TopP                *float64        `json:"top_p"`
	FrequencyPenalty    *float64        `json:"frequency_penalty"`
	PresencePenalty     *float64        `json:"presence_penalty"`
	Seed                *int64          `json:"seed"`
	Stop                json.RawMessage `json:"stop"`
	N                   *int64          `json:"n"`
	ResponseFormat      json.RawMessage `json:"response_format"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls"`
	Metadata            chatMetadata    `json:"metadata"`
	User                string          `json:"user"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMetadata struct {
	SessionID string `json:"session_id"`
}

type chatMessage struct {
	Role             string               `json:"role"`
	Content          json.RawMessage      `json:"content"`
	ReasoningContent *string              `json:"reasoning_content"`
	Reasoning        *string              `json:"reasoning"`
	ReasoningText    *string              `json:"reasoning_text"`
	ToolCalls        []transport.ToolCall `json:"tool_calls"`
	ToolCallID       string               `json:"tool_call_id"`
}

type chatTool struct {
	Type         string          `json:"type"`
	CacheControl json.RawMessage `json:"cache_control"`
	Function     struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
		Strict      *bool           `json:"strict"`
	} `json:"function"`
}

var allowedRoles = map[string]bool{
	"system":    true,
	"developer": true,
	"user":      true,
	"assistant": true,
	"tool":      true,
}

func (s *Server) validateChat(req *chatRequest) *failure {
	if len(req.Messages) == 0 {
		return newFailure(http.StatusBadRequest, "invalid_request")
	}
	if int64(len(req.Tools)) > s.opts.MaxTools {
		return newFailure(http.StatusBadRequest, "too_many_tools")
	}
	if err := adapter.ValidateToolChoice(req.ToolChoice); err != nil {
		return newFailure(http.StatusBadRequest, "invalid_request")
	}
	for i := range req.Tools {
		if req.Tools[i].Function.Name == "" {
			return newFailure(http.StatusBadRequest, "invalid_request")
		}
		if req.Tools[i].Type != "" && req.Tools[i].Type != "function" {
			return newFailure(http.StatusBadRequest, "invalid_request")
		}
	}
	hasHuman := false
	for i := range req.Messages {
		message := &req.Messages[i]
		if !allowedRoles[message.Role] {
			return newFailure(http.StatusBadRequest, "invalid_request")
		}
		if message.Role == "user" {
			hasHuman = true
		}
		info, fail := analyzeContent(message.Content)
		if fail != nil {
			return fail
		}
		if info.chars > s.opts.MaxMessageChars {
			return newFailure(http.StatusRequestEntityTooLarge, "message_too_large")
		}
		if info.mediaBytes > s.opts.MaxMediaPartBytes {
			return newFailure(http.StatusRequestEntityTooLarge, "message_too_large")
		}
		for j := range message.ToolCalls {
			if message.ToolCalls[j].Function.Name == "" {
				return newFailure(http.StatusBadRequest, "invalid_request")
			}
			if message.ToolCalls[j].Type == "" {
				message.ToolCalls[j].Type = "function"
			}
		}
	}
	if !hasHuman {
		return newFailure(http.StatusBadRequest, "invalid_request")
	}
	return nil
}

func includeUsage(req *chatRequest) bool {
	return req.StreamOptions != nil && req.StreamOptions.IncludeUsage
}

func requestMaxTokens(req *chatRequest) *int64 {
	if req.MaxCompletionTokens != nil {
		return req.MaxCompletionTokens
	}
	return req.MaxTokens
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.opts.ClientTotalTimeoutMS)*time.Millisecond)
	defer cancel()

	raw, fail := s.readBody(r)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	var req chatRequest
	if fail := decodeJSON(raw, &req); fail != nil {
		writeFailure(w, fail)
		return
	}
	if fail := s.validateChat(&req); fail != nil {
		writeFailure(w, fail)
		return
	}
	prefix, fail := fingerprintPrefix(req.Messages)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	key, source, fail := s.sessionKey(r, &req, prefix)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	unlock, ok := s.locks.acquire(ctx, key, time.Duration(s.opts.DialogueLockWaitMS)*time.Millisecond)
	if !ok {
		if ctx.Err() != nil {
			return
		}
		writeFailure(w, newFailure(http.StatusConflict, "conflict"))
		return
	}
	defer unlock()

	dialogue, err := s.store.EnsureDialogue(ctx, key, source, s.opts.SessionIdleTTLHours)
	if err != nil {
		s.log.Error("ensure dialogue", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	fingerprint, fail := idempotencyFingerprint(&req)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if delivery, found, err := s.store.Delivery(ctx, dialogue.ID, "chat", fingerprint); err != nil {
		s.log.Error("read delivery", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	} else if found {
		s.replayChat(ctx, w, delivery, req.Stream, includeUsage(&req),
			isOpenCodeClient(r.Header.Get("x-opencode-client")))
		return
	}
	if len(req.Messages) > 0 && req.Messages[len(req.Messages)-1].Role == "user" {
		if err := s.store.TouchHuman(ctx, dialogue.ID); err != nil {
			s.log.Warn("touch dialogue", "request_id", observe.RequestID(ctx), "error", err)
		}
	}

	input := engine.Input{
		Endpoint:          "chat",
		Dialogue:          dialogue,
		Messages:          toTransportMessages(&req),
		Tools:             toTransportTools(req.Tools),
		ToolChoice:        req.ToolChoice,
		Stream:            req.Stream,
		MaxTokens:         requestMaxTokens(&req),
		ReasoningEffort:   req.ReasoningEffort,
		Temperature:       req.Temperature,
		TopP:              req.TopP,
		FrequencyPenalty:  req.FrequencyPenalty,
		PresencePenalty:   req.PresencePenalty,
		Seed:              req.Seed,
		Stop:              req.Stop,
		N:                 req.N,
		ResponseFormat:    req.ResponseFormat,
		ParallelToolCalls: req.ParallelToolCalls,
		RequestID:         observe.RequestID(ctx),
		Client:            r.Header.Get("x-opencode-client"),
		Fingerprint:       fingerprint,
	}
	if req.Stream {
		s.streamChat(ctx, w, input, dialogue, &req, fingerprint)
		return
	}
	s.bufferedChat(ctx, w, input, dialogue, fingerprint)
}

func (s *Server) bufferedChat(ctx context.Context, w http.ResponseWriter, input engine.Input,
	dialogue *store.Dialogue, fingerprint string) {
	result, err := s.engine.Run(ctx, input, engine.Sink{})
	if err != nil {
		if clientCanceled(ctx) {
			s.log.Info("client canceled the request", "request_id", observe.RequestID(ctx), "error", err)
			return
		}
		fail := engineFailure(err)
		s.log.Error("chat turn failed", "request_id", observe.RequestID(ctx), "code", fail.code, "error", err)
		writeFailure(w, fail)
		return
	}
	turnID := "chatcmpl-" + store.NewID()
	reasoning := ""
	if isOpenCodeClient(input.Client) {
		reasoning = result.ReasoningContent
	}
	response := buildChatResponse(turnID, s.now().Unix(), s.opts.PublicModel,
		result, result.Content, reasoning)
	response.UsageReport = result.UsageReport
	body, err := json.Marshal(response)
	if err != nil {
		s.log.Error("encode chat response", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	delivered := true
	storedBody, err := json.Marshal(responseWithoutReasoning(response))
	if err == nil {
		err = s.store.PutDelivery(ctx, store.Delivery{
			ID:           store.NewID(),
			DialogueID:   dialogue.ID,
			Fingerprint:  fingerprint,
			Endpoint:     "chat",
			ResponseBody: string(storedBody),
		})
	}
	if err != nil {
		s.log.Error("store delivery", "request_id", observe.RequestID(ctx), "error", err)
		delivered = false
	}
	s.recordDeliveryPhase(ctx, dialogue, result, delivered)
	setDialogueHeaders(w, dialogue)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) streamChat(ctx context.Context, w http.ResponseWriter, input engine.Input,
	dialogue *store.Dialogue, req *chatRequest, fingerprint string) {
	turnID := "chatcmpl-" + store.NewID()
	created := s.now().Unix()
	sink := newStreamSink(w, s.log, turnID, s.opts.PublicModel, created,
		includeUsage(req), isOpenCodeClient(input.Client),
		time.Duration(s.opts.StreamWriteTimeoutMS)*time.Millisecond,
		func() { setDialogueHeaders(w, dialogue) }, s.opts.Progress)
	result, err := s.engine.Run(ctx, input, sink.sink())
	if err != nil {
		if clientCanceled(ctx) {
			s.log.Info("client canceled the stream", "request_id", observe.RequestID(ctx), "error", err)
			return
		}
		if sink.writeErr != nil {
			s.log.Warn("stream write failed", "request_id", observe.RequestID(ctx), "error", sink.writeErr)
			return
		}
		if sink.wrote {
			sink.writeError(err)
			if doneErr := sink.done(); doneErr != nil {
				s.log.Warn("stream terminator failed", "request_id", observe.RequestID(ctx), "error", doneErr)
			}
			s.log.Error("stream turn failed", "request_id", observe.RequestID(ctx), "error", err)
			return
		}
		fail := engineFailure(err)
		s.log.Error("stream turn failed before the first chunk", "request_id", observe.RequestID(ctx),
			"code", fail.code, "error", err)
		writeFailure(w, fail)
		return
	}
	if err := sink.writeUsage(result.Usage); err != nil {
		s.log.Warn("stream usage failed", "request_id", observe.RequestID(ctx), "error", err)
		return
	}
	response := buildChatResponse(turnID, created, s.opts.PublicModel, result, sink.emitted(result.Content), "")
	if body, err := json.Marshal(response); err == nil {
		delivered := true
		if err := s.store.PutDelivery(ctx, store.Delivery{
			ID:           store.NewID(),
			DialogueID:   dialogue.ID,
			Fingerprint:  fingerprint,
			Endpoint:     "chat",
			ResponseBody: string(body),
		}); err != nil {
			s.log.Error("store stream delivery", "request_id", observe.RequestID(ctx), "error", err)
			delivered = false
		}
		s.recordDeliveryPhase(ctx, dialogue, result, delivered)
	} else {
		s.log.Error("encode stream response", "request_id", observe.RequestID(ctx), "error", err)
	}
	if err := sink.done(); err != nil {
		s.log.Warn("stream terminator failed", "request_id", observe.RequestID(ctx), "error", err)
	}
}

func (s *Server) replayChat(
	ctx context.Context, w http.ResponseWriter, delivery *store.Delivery, stream, withUsage, emitReasoning bool,
) {
	if !stream {
		body := []byte(delivery.ResponseBody)
		if !emitReasoning {
			sanitized, err := stripChatReasoningFields(body)
			if err != nil {
				writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
				return
			}
			body = sanitized
		}
		s.writeStoredDelivery(ctx, w, string(body))
		return
	}
	var response transport.ChatResponse
	if err := json.Unmarshal([]byte(delivery.ResponseBody), &response); err != nil {
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	sink := newStreamSink(w, s.log, response.ID, s.opts.PublicModel, response.Created, withUsage, emitReasoning,
		time.Duration(s.opts.StreamWriteTimeoutMS)*time.Millisecond, nil, ProgressOptions{})
	if err := sink.replay(response); err != nil {
		return
	}
	_ = sink.done()
}

func setDialogueHeaders(w http.ResponseWriter, dialogue *store.Dialogue) {
	w.Header().Set("X-Coffee-Level", strconv.Itoa(dialogue.Level))
	model := dialogue.ExpertModelID
	if model == "" {
		model = dialogue.AssistantModelID
	}
	if model != "" {
		w.Header().Set("X-Coffee-Model", model)
	}
}

func isOpenCodeClient(client string) bool {
	switch strings.ToLower(strings.TrimSpace(client)) {
	case "app", "cli", "tui":
		return true
	default:
		return false
	}
}

func responseWithoutReasoning(response transport.ChatResponse) transport.ChatResponse {
	out := response
	out.Choices = append([]transport.ChatChoice(nil), response.Choices...)
	for i := range out.Choices {
		message := out.Choices[i].Message
		message.ReasoningContent = nil
		message.Reasoning = nil
		message.ReasoningText = nil
		out.Choices[i].Message = message
	}
	return out
}

func stripChatReasoningFields(raw []byte) ([]byte, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode stored chat response: %w", err)
	}
	rawChoices, ok := response["choices"]
	if !ok {
		return raw, nil
	}
	var choices []json.RawMessage
	if err := json.Unmarshal(rawChoices, &choices); err != nil {
		return nil, fmt.Errorf("decode stored chat choices: %w", err)
	}
	changed := false
	for i, rawChoice := range choices {
		var choice map[string]json.RawMessage
		if err := json.Unmarshal(rawChoice, &choice); err != nil {
			return nil, fmt.Errorf("decode stored chat choice: %w", err)
		}
		rawMessage, ok := choice["message"]
		if !ok {
			continue
		}
		var message map[string]json.RawMessage
		if err := json.Unmarshal(rawMessage, &message); err != nil {
			return nil, fmt.Errorf("decode stored assistant message: %w", err)
		}
		messageChanged := false
		for _, field := range []string{"reasoning_content", "reasoning", "reasoning_text"} {
			if _, exists := message[field]; exists {
				delete(message, field)
				messageChanged = true
			}
		}
		if !messageChanged {
			continue
		}
		changed = true
		encodedMessage, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("encode stored assistant message: %w", err)
		}
		choice["message"] = encodedMessage
		encodedChoice, err := json.Marshal(choice)
		if err != nil {
			return nil, fmt.Errorf("encode stored chat choice: %w", err)
		}
		choices[i] = encodedChoice
	}
	if !changed {
		return raw, nil
	}
	encodedChoices, err := json.Marshal(choices)
	if err != nil {
		return nil, fmt.Errorf("encode stored chat choices: %w", err)
	}
	response["choices"] = encodedChoices
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("encode stored chat response: %w", err)
	}
	return encodedResponse, nil
}

func buildChatResponse(
	id string, created int64, publicModel string, result engine.Result, content, reasoning string,
) transport.ChatResponse {
	finish := result.FinishReason
	if finish == "" {
		finish = "stop"
	}
	message := transport.Message{Role: "assistant"}
	if reasoning != "" {
		message.ReasoningContent = &reasoning
	}
	if len(result.ToolCalls) > 0 {
		message.ToolCalls = result.ToolCalls
		message.Content = json.RawMessage("null")
	} else {
		message.Content = transport.TextContent(content)
	}
	return transport.ChatResponse{
		ID:      id,
		Object:  "chat.completion",
		Created: created,
		Model:   publicModel,
		Choices: []transport.ChatChoice{{
			Index:        0,
			Message:      message,
			FinishReason: finish,
		}},
		Usage: result.Usage,
	}
}

func toTransportMessages(req *chatRequest) []transport.Message {
	out := make([]transport.Message, 0, len(req.Messages))
	for _, message := range req.Messages {
		item := transport.Message{Role: message.Role, Content: message.Content, ToolCallID: message.ToolCallID,
			ReasoningContent: message.ReasoningContent, Reasoning: message.Reasoning,
			ReasoningText: message.ReasoningText}
		if len(message.ToolCalls) > 0 {
			item.ToolCalls = message.ToolCalls
		}
		out = append(out, item)
	}
	return out
}

func toTransportTools(tools []chatTool) []transport.Tool {
	out := make([]transport.Tool, 0, len(tools))
	for _, tool := range tools {
		kind := tool.Type
		if kind == "" {
			kind = "function"
		}
		out = append(out, transport.Tool{
			Type:         kind,
			CacheControl: tool.CacheControl,
			Function: transport.ToolFunction{
				Name:        tool.Function.Name,
				Description: tool.Function.Description,
				Parameters:  tool.Function.Parameters,
				Strict:      tool.Function.Strict,
			},
		})
	}
	return out
}

// recordDeliveryPhase writes the delivery status after the response body is
// stored. A failed store call turns the turn into a canceled stop, so the
// request never stays active forever.
func (s *Server) recordDeliveryPhase(ctx context.Context, dialogue *store.Dialogue, result engine.Result, delivered bool) {
	bookkeeping := context.WithoutCancel(ctx)
	phase := result.DeliverPhase
	if !delivered {
		phase = "expert_canceled_stop"
		if strings.HasPrefix(result.DeliverPhase, "adjuster_") {
			phase = "adjuster_2_canceled_stop"
		}
	}
	if phase == "" {
		return
	}
	if err := s.store.SetDialoguePhase(bookkeeping, dialogue.ID, phase); err != nil {
		s.log.Error("write delivery status", "error", err, "phase", phase)
	}
}
