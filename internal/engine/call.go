package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/ledger"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type actorKind int

const (
	actorAssistant actorKind = iota
	actorExpert
	actorHelper
	actorDeflator
)

var errTurnStepLimit = errors.New("gateway turn step limit reached")

type delegationRequest struct {
	Task      string `json:"task"`
	Context   string `json:"context"`
	Scope     string `json:"scope"`
	MaxTokens *int64 `json:"max_tokens,omitempty"`
}

type actorOutcome struct {
	Text               string
	ReasoningContent   string
	ToolCalls          []transport.ToolCall
	FinishReason       string
	Usage              *transport.Usage
	NeedsExpert        bool
	NeedsExpertReason  string
	Sanitized          bool
	ReasoningSanitized bool
	Delegation         *delegationRequest
	HTTPStatus         int
	Duration           time.Duration
}

// reasoningEnabled reports whether the actor's reasoning exists at all: the
// master gate covers every consumer (TUI, archive, prompts, compaction), the
// assistant 2 gate narrows the helper alone (ADR 0025). The deflator is a
// service call and never has reasoning.
func (t *turn) reasoningEnabled(kind actorKind) bool {
	if kind == actorDeflator || !t.settings.ReasoningContextEnabled {
		return false
	}
	return kind != actorHelper || t.settings.Assistant2Reasoning
}

// reasoningTraceable reports whether the reasoning of one completed call is
// archived and reused: enabled actors only, and incomplete calls (length,
// content_filter) are never archived.
func (t *turn) reasoningTraceable(kind actorKind, outcome *actorOutcome) bool {
	if kind == actorDeflator || !t.reasoningEnabled(kind) {
		return false
	}
	return outcome.FinishReason != "length" && outcome.FinishReason != "content_filter"
}

func (t *turn) callActor(ctx context.Context, kind actorKind, model catalog.Model, messages []transport.Message,
	tools []transport.Tool, allowDelegation bool) (*actorOutcome, error) {
	if t.dialogue.TurnSteps >= t.engine.opts.MaxGatewayTurns {
		return nil, errTurnStepLimit
	}
	effort, degraded := t.engine.router.Effort(model, t.settings.ReasoningMode, t.input.ReasoningEffort)
	if degraded {
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "effort_degraded", Role: roleName(kind), RequestID: t.dialogue.RequestID,
			Note: fmt.Sprintf("client reasoning_effort %q mapped to the nearest allowed value for mode %q", t.input.ReasoningEffort, t.settings.ReasoningMode)})
	}
	// Expert has no output ceiling: the full answer reaches the user (ADR 0017).
	// Assistant 1 also has no ceiling — if the answer is too long, the expert
	// will handle it.  Assistant 2 (helper) gets a ceiling only when the expert
	// explicitly passes one via delegation.
	var maxTokens *int64
	switch kind {
	case actorExpert, actorAssistant, actorHelper:
		maxTokens = nil
	default:
		maxTokens = t.outputLimit(model)
	}
	request := transport.ChatRequest{
		Model:             model.ID,
		Messages:          messages,
		Tools:             tools,
		ToolChoice:        t.input.ToolChoice,
		MaxTokens:         maxTokens,
		ReasoningEffort:   effort,
		Temperature:       t.input.Temperature,
		TopP:              t.input.TopP,
		FrequencyPenalty:  t.input.FrequencyPenalty,
		PresencePenalty:   t.input.PresencePenalty,
		Seed:              t.input.Seed,
		Stop:              t.input.Stop,
		N:                 t.input.N,
		ResponseFormat:    t.input.ResponseFormat,
		ParallelToolCalls: t.input.ParallelToolCalls,
		Session:           t.dialogue.ID,
	}
	requestID := store.NewID()
	reservation, err := t.engine.ledger.Reserve(ctx, requestID, model.ID, model.Quota)
	if err != nil {
		if ledger.IsNoQuota(err) {
			t.writeStop(ctx, kind, "no_available_model")
			return nil, fail("no_quota", http.StatusServiceUnavailable, "Model quota is exhausted", err)
		}
		return nil, fail("internal", http.StatusInternalServerError, "Quota reservation failed", err)
	}
	t.dialogue.TurnSteps++
	if err := t.engine.store.UpdateDialogue(ctx, t.dialogue); err != nil {
		releaseErr := t.engine.ledger.Release(context.WithoutCancel(ctx), *reservation)
		return nil, fail("internal", http.StatusInternalServerError, "Turn state is unavailable",
			errors.Join(fmt.Errorf("persist turn step: %w", err), releaseErr))
	}
	started := time.Now()
	t.actors.add(model.ID, participantRole(kind))
	t.modelCall++
	t.engine.log.Info("model request diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"upstream_request_id", requestID,
		"phase", t.dialogue.Phase,
		"role", roleName(kind),
		"model", model.ID,
		"reasoning_effort", effort,
		"max_tokens", maxTokens,
		"client_history", t.clientHistory,
		"stream", t.input.Stream,
		"tools", len(tools),
		"history", diagnosePrompt(t.input.Messages, messages),
	)
	var outcome *actorOutcome
	if t.input.Stream {
		outcome, err = t.callStreaming(ctx, kind, request, allowDelegation)
	} else {
		outcome, err = t.callBuffered(ctx, request, allowDelegation)
	}
	outcome = ensureOutcome(outcome)
	if !t.reasoningEnabled(kind) {
		outcome.ReasoningContent = ""
		outcome.ReasoningSanitized = false
	}
	cleaned := SanitizeModelText(outcome.Text)
	cleanedReasoning := SanitizeModelText(outcome.ReasoningContent)
	outcome.Sanitized = cleaned != outcome.Text || cleanedReasoning != outcome.ReasoningContent
	outcome.ReasoningSanitized = outcome.ReasoningSanitized ||
		cleanedReasoning != outcome.ReasoningContent
	outcome.Text = cleaned
	outcome.ReasoningContent = cleanedReasoning
	t.reasoningSanitized = t.reasoningSanitized || outcome.ReasoningSanitized
	outcome.Duration = time.Since(started)
	t.engine.log.Info("model response diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"upstream_request_id", requestID,
		"phase", t.dialogue.Phase,
		"role", roleName(kind),
		"model", model.ID,
		"duration_ms", outcome.Duration.Milliseconds(),
		"failed", err != nil,
		"finish", outcome.FinishReason,
		"output_chars", len([]rune(outcome.Text)),
		"tool_calls", len(outcome.ToolCalls),
		"needs_expert", outcome.NeedsExpert,
		"delegation", outcome.Delegation != nil,
		"sanitized", outcome.Sanitized,
		"usage", outcome.Usage,
	)
	if err != nil {
		// The client may already be gone: quota and diagnostics must still be
		// written, so bookkeeping does not inherit the canceled context.
		bookkeeping := context.WithoutCancel(ctx)
		if releaseErr := t.engine.ledger.Release(bookkeeping, *reservation); releaseErr != nil {
			t.engine.log.Error("release quota", "error", releaseErr, "request_id", t.input.RequestID)
		}
		t.handleCallError(bookkeeping, kind, model, err)
		return nil, err
	}
	if commitErr := t.engine.ledger.Commit(ctx, *reservation); commitErr != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Quota commit failed", commitErr)
	}
	if outcome.Usage != nil {
		t.usage.Merge(outcome.Usage)
		t.mergeUsage(roleName(kind), outcome.Usage)
	}
	if err := t.recordCall(ctx, kind, model, requestID, outcome); err != nil {
		t.engine.log.Error("record model call", "error", err, "request_id", t.input.RequestID)
	}
	if err := t.engine.store.RecordModelSuccess(ctx, model.ID); err != nil {
		t.engine.log.Error("record model success", "error", err, "model", model.ID)
	}
	if outcome.ReasoningContent != "" && t.reasoningTraceable(kind, outcome) {
		if err := t.appendReasoning(ctx, outcome.ReasoningContent); err != nil {
			return nil, fail("internal", http.StatusInternalServerError,
				"Reasoning history is unavailable", err)
		}
	}
	if outcome.NeedsExpert || outcome.Delegation != nil {
		t.logCall(kind, model, messages, outcome, requestID)
		return outcome, nil
	}
	outcome.ToolCalls = filterClientCalls(outcome.ToolCalls)
	for _, call := range outcome.ToolCalls {
		t.progress(Progress{Kind: "tool", Stage: t.dialogue.Phase, Category: "tools",
			Role: roleName(kind), Reason: call.Function.Name})
	}
	t.logCall(kind, model, messages, outcome, requestID)
	return outcome, nil
}

// promptTokens estimates the size of the prompt that is actually sent, not of the
// client request: after compaction they differ.
func promptTokens(messages []transport.Message) int64 {
	chars := int64(0)
	for _, message := range messages {
		chars += int64(len(message.Content))
		for _, call := range message.ToolCalls {
			chars += int64(len(call.Function.Name) + len(call.Function.Arguments))
		}
	}
	charsPerToken := int64(3)
	return chars / charsPerToken
}

// logCall writes one line per model call so a repeating turn is visible without
// reading the database: who was called, how big the prompt was, what came back.
func (t *turn) logCall(kind actorKind, model catalog.Model, messages []transport.Message, outcome *actorOutcome, requestID string) {
	t.engine.log.Info("model call",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"upstream_request_id", requestID,
		"phase", t.dialogue.Phase,
		"dialogue_level", t.dialogue.Level,
		"step", t.modelCall,
		"role", roleName(kind),
		"model", model.ID,
		"prompt_tokens", promptTokens(messages),
		"output_chars", len([]rune(outcome.Text)),
		"sanitized", outcome.Sanitized,
		"tool_calls", len(outcome.ToolCalls),
		"needs_expert", outcome.NeedsExpert,
		"delegation", outcome.Delegation != nil,
		"finish", outcome.FinishReason,
		"http_status", outcome.HTTPStatus,
		"duration_ms", outcome.Duration.Milliseconds(),
	)
}

func (t *turn) callBuffered(ctx context.Context, request transport.ChatRequest,
	allowDelegation bool) (*actorOutcome, error) {
	response, err := t.engine.transport.Do(ctx, request, len(request.Tools) == 0)
	if err != nil {
		return nil, mapTransportError(err)
	}
	outcome := &actorOutcome{FinishReason: "stop", Usage: response.Usage}
	if len(response.Choices) > 0 {
		choice := response.Choices[0]
		text, textErr := choice.Message.Text()
		if textErr != nil {
			return nil, fail("upstream_failed", http.StatusBadGateway, "Upstream response is invalid", textErr)
		}
		outcome.Text = text
		outcome.ReasoningContent = messageReasoningText(choice.Message)
		outcome.ToolCalls = choice.Message.ToolCalls
		if choice.FinishReason != "" {
			outcome.FinishReason = choice.FinishReason
		}
	}
	t.inspectVirtual(outcome, allowDelegation)
	return outcome, nil
}

func (t *turn) callStreaming(ctx context.Context, kind actorKind, request transport.ChatRequest,
	allowDelegation bool) (*actorOutcome, error) {
	emitText := t.engine.opts.StreamTextScope == "all"
	emitReasoning := t.reasoningEnabled(kind)
	var reasoningSanitizer reasoningStreamSanitizer
	result, err := t.engine.transport.Stream(ctx, request, func(chunk transport.Chunk) error {
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != nil && emitText {
				if err := t.sink.text(SanitizeModelText(*choice.Delta.Content)); err != nil {
					return fmt.Errorf("%w: %w", errSinkClosed, err)
				}
			}
			if emitReasoning {
				if reasoning := choice.Delta.ReasoningText(); reasoning != "" {
					if safe := reasoningSanitizer.Push(reasoning); safe != "" {
						if err := t.sink.reasoning(safe); err != nil {
							return fmt.Errorf("%w: %w", errSinkClosed, err)
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errSinkClosed) {
			return nil, fail("canceled", statusClientClosedRequest, "Client interrupted the request", err)
		}
		return nil, mapTransportError(err)
	}
	if emitReasoning {
		if reasoning := reasoningSanitizer.Flush(); reasoning != "" {
			if err := t.sink.reasoning(reasoning); err != nil {
				return nil, fail("canceled", statusClientClosedRequest, "Client interrupted the request",
					fmt.Errorf("%w: %w", errSinkClosed, err))
			}
		}
	}
	outcome := &actorOutcome{Text: result.Content, ReasoningContent: result.ReasoningContent,
		ToolCalls:          result.ToolCalls,
		ReasoningSanitized: reasoningSanitizer.Changed(),
		FinishReason:       result.FinishReason, Usage: result.Usage}
	if outcome.FinishReason == "" {
		outcome.FinishReason = "stop"
	}
	t.inspectVirtual(outcome, allowDelegation)
	return outcome, nil
}

func messageReasoningText(message transport.Message) string {
	for _, value := range []*string{message.ReasoningContent, message.Reasoning, message.ReasoningText} {
		if value != nil && *value != "" {
			return *value
		}
	}
	return ""
}

func (t *turn) inspectVirtual(outcome *actorOutcome, allowDelegation bool) {
	var client []transport.ToolCall
	for _, call := range outcome.ToolCalls {
		switch call.Function.Name {
		case toolAssistantNeedsHelp:
			outcome.NeedsExpert = true
			var args struct {
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err == nil {
				outcome.NeedsExpertReason = args.Reason
			}
		case toolGatewayAssistant:
			if !allowDelegation {
				client = append(client, call)
				continue
			}
			var args delegationRequest
			if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
				outcome.Delegation = &delegationRequest{Task: "", Scope: "invalid"}
				continue
			}
			outcome.Delegation = &args
		default:
			client = append(client, call)
		}
	}
	outcome.ToolCalls = client
}

func filterClientCalls(calls []transport.ToolCall) []transport.ToolCall {
	out := calls[:0]
	for _, call := range calls {
		switch call.Function.Name {
		case toolAssistantNeedsHelp, toolGatewayAssistant:
			continue
		default:
			out = append(out, call)
		}
	}
	return out
}

func (t *turn) outputLimit(model catalog.Model) *int64 {
	limit := t.engine.opts.MaxOutputTokens
	client := t.input.MaxTokens
	if t.engine.opts.OutputLimitMode == "strict_min" {
		if client != nil && *client < limit {
			limit = *client
		}
		if int64(model.MaxOutput) < limit {
			limit = int64(model.MaxOutput)
		}
	} else {
		if client != nil {
			limit = *client
		}
		if int64(model.MaxOutput) > 0 && int64(model.MaxOutput) < limit {
			limit = int64(model.MaxOutput)
		}
		if limit > t.engine.opts.MaxOutputTokens {
			limit = t.engine.opts.MaxOutputTokens
		}
	}
	if limit <= 0 {
		return nil
	}
	return &limit
}

func (t *turn) handleCallError(ctx context.Context, kind actorKind, model catalog.Model, err error) {
	var typed *Error
	if !errors.As(err, &typed) {
		return
	}
	// A canceled client request says nothing about the model: no cooldown and
	// no rotation penalty, only a journal record of the interruption.
	if typed.Code == "canceled" || errors.Is(err, context.Canceled) {
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "turn_canceled", Role: roleName(kind), RequestID: t.dialogue.RequestID,
			Note: fmt.Sprintf("client closed the connection during %s", model.ID)})
		t.writeStop(ctx, kind, "canceled")
		return
	}
	if typed.Code == "upstream_rate_limited" {
		until, coolErr := t.engine.ledger.OnRateLimit(ctx, model.ID, model.Quota.Group,
			typed.RetryAfterMS, t.engine.opts.Quota429CooldownMS, t.engine.opts.RetryAfterMaxMS)
		if coolErr != nil {
			t.engine.log.Error("set cooldown", "error", coolErr, "model", model.ID)
		}
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "upstream_rate_limited", Role: roleName(kind), RequestID: t.dialogue.RequestID,
			Note: fmt.Sprintf("model %s is cooling until %d", model.ID, until)})
		t.progress(Progress{Kind: "model", Stage: t.dialogue.Phase, Category: "quota",
			Role: roleName(kind), ModelID: model.ID, Reason: "excluded"})
		t.writeStop(ctx, kind, "upstream_error")
		return
	}
	if typed.Code == "upstream_failed" || typed.Code == "upstream_timeout" {
		until, recErr := t.engine.store.RecordModelError(ctx, model.ID, typed.Code,
			t.engine.opts.RotationErrorThresh, t.engine.opts.RotationCooldownMS)
		if recErr != nil {
			t.engine.log.Error("record model error", "error", recErr, "model", model.ID)
		}
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "model_call_failed", Role: roleName(kind), RequestID: t.dialogue.RequestID,
			Note: fmt.Sprintf("model %s failed: %s, excluded until %d", model.ID, typed.Code, until)})
		t.progress(Progress{Kind: "model", Stage: t.dialogue.Phase, Category: "model",
			Role: roleName(kind), ModelID: model.ID, Reason: "excluded"})
		if typed.Code == "upstream_timeout" {
			t.writeStop(ctx, kind, "timeout")
		} else {
			t.writeStop(ctx, kind, "upstream_error")
		}
	}
}

func (t *turn) recordCall(ctx context.Context, kind actorKind, model catalog.Model, requestID string, outcome *actorOutcome) error {
	var input, cached, output, reasoning *int64
	if outcome.Usage != nil {
		input = outcome.Usage.PromptTokens
		output = outcome.Usage.CompletionTokens
		reasoning = outcome.Usage.Reasoning()
		if outcome.Usage.PromptTokensDetails != nil {
			cached = outcome.Usage.PromptTokensDetails.CachedTokens
		}
	}
	status := outcome.HTTPStatus
	if status == 0 {
		status = 200
	}
	inExpert := t.engine.catalog.HasGroup(model.ID, "coffee-expert")
	inAssistant := t.engine.catalog.HasGroup(model.ID, "coffee-assistant")
	pipeline := "pipeline"
	if t.engine.opts.Proxy {
		pipeline = "expert_only"
	}
	return t.engine.store.RecordModelCall(ctx, store.ModelCall{
		DialogueID: t.dialogue.ID, Role: roleName(kind), ModelID: model.ID, Purpose: stageName(kind),
		RequestID:   requestID,
		InputTokens: input, CachedTokens: cached, OutputTokens: output, ReasoningTokens: reasoning,
		DurationMS: outcome.Duration.Milliseconds(), HTTPStatus: &status, CostUSD: costOf(model, outcome.Usage),
		ObjectiveMode: t.settings.ObjectiveMode, ReasoningMode: t.settings.ReasoningMode,
		ReasoningEffort: t.engine.router.EffortOrEmpty(model, t.settings.ReasoningMode, t.input.ReasoningEffort),
		InGroupExpert:   &inExpert, InGroupAssistant: &inAssistant, PipelineMode: pipeline,
	})
}

func costOf(model catalog.Model, usage *transport.Usage) *float64 {
	if usage == nil || model.CostInPerMTok == nil || model.CostOutPerMTok == nil {
		return nil
	}
	if usage.PromptTokens == nil || usage.CompletionTokens == nil {
		return nil
	}
	cost := float64(*usage.PromptTokens)/1_000_000*(*model.CostInPerMTok) +
		float64(*usage.CompletionTokens)/1_000_000*(*model.CostOutPerMTok)
	return &cost
}

func roleName(kind actorKind) string {
	switch kind {
	case actorExpert:
		return "expert"
	case actorDeflator:
		return "deflator"
	default:
		return "assistant"
	}
}

func stageName(kind actorKind) string {
	switch kind {
	case actorHelper:
		return "assistant_2"
	case actorAssistant:
		return "assistant_1"
	default:
		return ""
	}
}

func participantRole(kind actorKind) string {
	switch kind {
	case actorExpert:
		return "expert"
	case actorDeflator:
		return "deflator"
	default:
		return "assistant"
	}
}

func mapTransportError(err error) error {
	var typed *transport.Error
	if errors.As(err, &typed) {
		switch typed.Code {
		case "canceled":
			return &Error{Code: "canceled", Status: statusClientClosedRequest, Message: "Request was canceled", Err: typed.Err}
		case "upstream_rate_limited":
			return &Error{Code: typed.Code, Status: http.StatusTooManyRequests, Message: "Upstream rate limit is reached",
				RetryAfterMS: typed.RetryAfterMS, Err: typed.Err}
		case "upstream_timeout":
			return &Error{Code: typed.Code, Status: http.StatusGatewayTimeout, Message: "Upstream request timed out", Err: typed.Err}
		default:
			return &Error{Code: "upstream_failed", Status: http.StatusBadGateway, Message: "Upstream request failed", Err: typed.Err}
		}
	}
	return &Error{Code: "upstream_failed", Status: http.StatusBadGateway, Message: "Upstream request failed", Err: err}
}

func ensureOutcome(outcome *actorOutcome) *actorOutcome {
	if outcome == nil {
		return &actorOutcome{FinishReason: "stop"}
	}
	return outcome
}

// errSinkClosed marks a failure to write to the client stream. It is not an
// upstream failure and must not put the model into rotation.
var errSinkClosed = errors.New("client stream is unavailable")
