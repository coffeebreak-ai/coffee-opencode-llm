package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/observe"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type responsesRequest struct {
	Model           string          `json:"model"`
	Input           json.RawMessage `json:"input"`
	Instructions    string          `json:"instructions"`
	Tools           []responsesTool `json:"tools"`
	Stream          bool            `json:"stream"`
	MaxOutputTokens *int64          `json:"max_output_tokens"`
}

type responsesTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type responsesItem struct {
	Type      string          `json:"type"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	CallID    string          `json:"call_id"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Output    string          `json:"output"`
}

type responsesResponse struct {
	ID          string               `json:"id"`
	Object      string               `json:"object"`
	Status      string               `json:"status"`
	Output      []any                `json:"output"`
	Usage       *responsesUsage      `json:"usage,omitempty"`
	UsageReport []transport.UsageRow `json:"x-coffee-usage,omitempty"`
}

type responsesUsage struct {
	InputTokens  *int64 `json:"input_tokens,omitempty"`
	OutputTokens *int64 `json:"output_tokens,omitempty"`
	TotalTokens  *int64 `json:"total_tokens,omitempty"`
}

func (s *Server) responses(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.opts.ClientTotalTimeoutMS)*time.Millisecond)
	defer cancel()

	raw, fail := s.readBody(r)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	var req responsesRequest
	if fail := decodeJSON(raw, &req); fail != nil {
		writeFailure(w, fail)
		return
	}
	messages, fail := convertResponsesInput(req.Input)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if req.Instructions != "" {
		messages = append([]transport.Message{transport.TextMessage("system", req.Instructions)}, messages...)
	}
	if len(messages) == 0 {
		writeFailure(w, newFailure(http.StatusBadRequest, "invalid_request"))
		return
	}
	hasHuman := false
	for _, message := range messages {
		if message.Role == "user" || message.Role == "tool" {
			hasHuman = true
			break
		}
	}
	if !hasHuman {
		writeFailure(w, newFailure(http.StatusBadRequest, "invalid_request"))
		return
	}
	tools, fail := convertResponsesTools(req.Tools)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	chatMessages := make([]chatMessage, 0, len(messages))
	for _, message := range messages {
		chatMessages = append(chatMessages, chatMessage{
			Role:       message.Role,
			Content:    message.Content,
			ToolCalls:  message.ToolCalls,
			ToolCallID: message.ToolCallID,
		})
	}
	prefix, fail := fingerprintPrefix(chatMessages)
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	key, source, fail := s.sessionKey(r, &chatRequest{Messages: chatMessages}, prefix)
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
	fingerprint, fail := idempotencyFingerprint(&chatRequest{Messages: chatMessages, Tools: responsesAsChatTools(req.Tools)})
	if fail != nil {
		writeFailure(w, fail)
		return
	}
	if delivery, found, err := s.store.Delivery(ctx, dialogue.ID, "responses", fingerprint); err != nil {
		s.log.Error("read delivery", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	} else if found {
		s.writeStoredDelivery(ctx, w, delivery.ResponseBody)
		return
	}
	if len(messages) > 0 && messages[len(messages)-1].Role == "user" {
		if err := s.store.TouchHuman(ctx, dialogue.ID); err != nil {
			s.log.Warn("touch dialogue", "request_id", observe.RequestID(ctx), "error", err)
		}
	}

	input := engine.Input{
		Endpoint:    "responses",
		Dialogue:    dialogue,
		Messages:    messages,
		Tools:       tools,
		MaxTokens:   req.MaxOutputTokens,
		RequestID:   observe.RequestID(ctx),
		Client:      r.Header.Get("x-opencode-client"),
		Fingerprint: fingerprint,
	}
	result, err := s.engine.Run(ctx, input, engine.Sink{})
	if err != nil {
		if clientCanceled(ctx) {
			s.log.Info("client canceled the request", "request_id", observe.RequestID(ctx), "error", err)
			return
		}
		fail := engineFailure(err)
		s.log.Error("responses turn failed", "request_id", observe.RequestID(ctx), "code", fail.code, "error", err)
		writeFailure(w, fail)
		return
	}
	response := buildResponsesResponse("resp_"+store.NewID(), result)
	body, err := json.Marshal(response)
	if err != nil {
		s.log.Error("encode responses body", "request_id", observe.RequestID(ctx), "error", err)
		writeFailure(w, newFailure(http.StatusInternalServerError, "internal"))
		return
	}
	delivered := true
	if err := s.store.PutDelivery(ctx, store.Delivery{
		ID:           store.NewID(),
		DialogueID:   dialogue.ID,
		Fingerprint:  fingerprint,
		Endpoint:     "responses",
		ResponseBody: string(body),
	}); err != nil {
		s.log.Error("store responses delivery", "request_id", observe.RequestID(ctx), "error", err)
		delivered = false
	}
	s.recordDeliveryPhase(ctx, dialogue, result, delivered)
	setDialogueHeaders(w, dialogue)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func convertResponsesInput(raw json.RawMessage) ([]transport.Message, *failure) {
	if len(raw) == 0 {
		return nil, newFailure(http.StatusBadRequest, "invalid_request")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []transport.Message{transport.TextMessage("user", text)}, nil
	}
	var items []responsesItem
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, newFailure(http.StatusBadRequest, "invalid_request")
	}
	out := make([]transport.Message, 0, len(items))
	for _, item := range items {
		switch item.Type {
		case "message", "":
			role := item.Role
			if role == "" {
				role = "user"
			}
			content, fail := responsesText(item.Content)
			if fail != nil {
				return nil, fail
			}
			out = append(out, transport.Message{Role: role, Content: transport.TextContent(content)})
		case "function_call":
			id := item.CallID
			if id == "" {
				id = item.ID
			}
			out = append(out, transport.Message{
				Role: "assistant",
				ToolCalls: []transport.ToolCall{{
					ID:   id,
					Type: "function",
					Function: transport.FunctionCall{
						Name:      item.Name,
						Arguments: item.Arguments,
					},
				}},
			})
		case "function_call_output":
			out = append(out, transport.Message{
				Role:       "tool",
				ToolCallID: item.CallID,
				Content:    transport.TextContent(item.Output),
			})
		default:
			return nil, newFailure(http.StatusBadRequest, "invalid_request")
		}
	}
	return out, nil
}

func responsesText(raw json.RawMessage) (string, *failure) {
	if len(raw) == 0 {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", newFailure(http.StatusBadRequest, "invalid_request")
	}
	var builder strings.Builder
	for _, part := range parts {
		switch part.Type {
		case "input_text", "output_text", "text":
			builder.WriteString(part.Text)
		default:
			return "", newFailure(http.StatusUnprocessableEntity, "unsupported_content")
		}
	}
	return builder.String(), nil
}

func convertResponsesTools(tools []responsesTool) ([]transport.Tool, *failure) {
	out := make([]transport.Tool, 0, len(tools))
	for _, tool := range tools {
		if tool.Name == "" {
			return nil, newFailure(http.StatusBadRequest, "invalid_request")
		}
		out = append(out, transport.Tool{
			Type: "function",
			Function: transport.ToolFunction{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  tool.Parameters,
			},
		})
	}
	return out, nil
}

func responsesAsChatTools(tools []responsesTool) []chatTool {
	out := make([]chatTool, 0, len(tools))
	for _, tool := range tools {
		var item chatTool
		item.Type = "function"
		item.Function.Name = tool.Name
		item.Function.Description = tool.Description
		item.Function.Parameters = tool.Parameters
		out = append(out, item)
	}
	return out
}

func buildResponsesResponse(id string, result engine.Result) responsesResponse {
	output := make([]any, 0, 1)
	if len(result.ToolCalls) > 0 {
		for _, call := range result.ToolCalls {
			output = append(output, map[string]any{
				"type":      "function_call",
				"id":        call.ID,
				"call_id":   call.ID,
				"name":      call.Function.Name,
				"arguments": call.Function.Arguments,
			})
		}
	} else {
		output = append(output, map[string]any{
			"type":   "message",
			"id":     "msg_" + store.NewID(),
			"role":   "assistant",
			"status": "completed",
			"content": []any{map[string]any{
				"type": "output_text",
				"text": result.Content,
			}},
		})
	}
	response := responsesResponse{
		ID:          id,
		Object:      "response",
		Status:      "completed",
		Output:      output,
		UsageReport: result.UsageReport,
	}
	if result.Usage != nil {
		response.Usage = &responsesUsage{
			InputTokens:  result.Usage.PromptTokens,
			OutputTokens: result.Usage.CompletionTokens,
			TotalTokens:  result.Usage.TotalTokens,
		}
	}
	return response
}
