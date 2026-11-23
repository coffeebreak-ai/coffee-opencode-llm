package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/adapter"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/router"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type delegationOutcome struct {
	text        string
	assistantID string
	scope       string
	budgetSteps int64
	pending     bool
	result      Result
}

func delegationMaterial(modelID, scope string, budgetSteps int64, text string) string {
	if modelID == "" {
		modelID = "none"
	}
	if scope == "" {
		scope = "unknown"
	}
	return fmt.Sprintf("assistant_model: %s\nassistant_role: assistant\nscope: %s\nmax_steps: %d\n\n%s",
		modelID, scope, budgetSteps, text)
}

func (t *turn) runExpertWithDelegation(ctx context.Context) (Result, error) {
	delegation, err := t.engine.store.ActiveDelegation(ctx, t.dialogue.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return t.runExpert(ctx)
		}
		return Result{}, fail("internal", http.StatusInternalServerError, "Delegation state is unavailable", err)
	}
	model, ok := t.engine.catalog.Get(t.dialogue.ExpertModelID)
	if !ok {
		return t.runExpert(ctx)
	}
	outcome, err := t.resumeDelegation(ctx, delegation)
	if err != nil {
		return Result{}, err
	}
	if outcome.pending {
		return outcome.result, nil
	}
	if err := t.appendDelegationResult(ctx, delegationMaterial(outcome.assistantID, outcome.scope, outcome.budgetSteps, outcome.text)); err != nil {
		return Result{}, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	return t.expertLoop(ctx, model)
}

func (t *turn) runDelegation(ctx context.Context, request *delegationRequest) (*delegationOutcome, error) {
	scope := strings.TrimSpace(request.Scope)
	if !validScope(scope) {
		_ = t.engine.store.Journal(ctx, store.JournalEvent{DialogueID: t.dialogue.ID, Level: "warn",
			Kind: "invalid_scope_rejected", Role: "expert", RequestID: t.dialogue.RequestID,
			Note: fmt.Sprintf("delegation scope %q is not allowed", request.Scope)})
		return &delegationOutcome{text: "invalid_scope: the requested scope is not allowed", scope: request.Scope}, nil
	}
	candidates, err := t.assistantCandidates(ctx, true)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return &delegationOutcome{text: "no_available_model: no assistant can take the delegation", scope: scope}, nil
	}
	chosen := candidates[0]
	delegation, err := t.engine.store.CreateDelegation(ctx, store.Delegation{
		DialogueID: t.dialogue.ID, Task: request.Task, ContextText: request.Context, Scope: scope,
		AssistantModelID: chosen.Model.ID, BudgetSteps: t.engine.opts.MaxDelegationSteps,
		BudgetChars: t.engine.opts.CompactEvidenceChars,
	})
	if err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
	}
	outcome, err := t.continueDelegation(ctx, delegation)
	if err != nil {
		return nil, err
	}
	outcome.assistantID = delegation.AssistantModelID
	outcome.scope = delegation.Scope
	outcome.budgetSteps = delegation.BudgetSteps
	return outcome, nil
}

func (t *turn) resumeDelegation(ctx context.Context, delegation *store.Delegation) (*delegationOutcome, error) {
	if err := t.engine.store.CompleteDelegationSteps(ctx, delegation.ID, t.incomingToolCallIDs()); err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation steps are unavailable", err)
	}
	if err := t.engine.store.SetDelegationStatus(ctx, delegation.ID, "client_tools_to_assistant_2"); err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
	}
	steps, err := t.engine.store.DelegationSteps(ctx, delegation.ID)
	if err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation steps are unavailable", err)
	}
	if steps >= delegation.BudgetSteps {
		if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "budget_exhausted"); err != nil {
			return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		return &delegationOutcome{
			text:        "delegation stopped: max_delegation_steps reached",
			assistantID: delegation.AssistantModelID, scope: delegation.Scope,
			budgetSteps: delegation.BudgetSteps,
		}, nil
	}
	outcome, err := t.continueDelegation(ctx, delegation)
	if err != nil {
		return nil, err
	}
	outcome.assistantID = delegation.AssistantModelID
	outcome.scope = delegation.Scope
	outcome.budgetSteps = delegation.BudgetSteps
	return outcome, nil
}

func (t *turn) continueDelegation(ctx context.Context, delegation *store.Delegation) (*delegationOutcome, error) {
	model, ok := t.engine.catalog.Get(delegation.AssistantModelID)
	if !ok {
		if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "no_available_model"); err != nil {
			return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		return &delegationOutcome{text: "delegation stopped: assistant is no longer in the catalog"}, nil
	}
	messages, err := t.helperMessages(ctx, delegation)
	if err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	tools := append(filterTools(t.input.Tools, delegation.Scope, t.engine.opts.ToolScopes),
		virtualTools(adapter.ToolAssistantNeedsHelp)...)
	outcome, err := t.callActor(ctx, actorHelper, model, messages, tools, false)
	if err != nil {
		if isNoQuota(err) || isUpstreamFailure(err) {
			return t.rotateDelegation(ctx, delegation)
		}
		return nil, err
	}
	if outcome.NeedsExpert {
		if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "needs_expert"); err != nil {
			return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		text := strings.TrimSpace(outcome.Text)
		if reason := strings.TrimSpace(outcome.NeedsExpertReason); reason != "" {
			text = strings.TrimSpace(text + "\nassistant_needs_expert: " + reason)
		}
		if text == "" {
			text = "assistant_needs_expert: the assistant stopped without a result"
		}
		return &delegationOutcome{text: text}, nil
	}
	if len(outcome.ToolCalls) > 0 {
		if err := t.engine.store.SetDelegationStatus(ctx, delegation.ID, "assistant_2_to_client_tools"); err != nil {
			return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		for _, call := range outcome.ToolCalls {
			if err := t.engine.store.AddDelegationStep(ctx, delegation.ID, call.ID, call.Function.Name,
				call.Function.Arguments, "assistant_2_to_client_tools"); err != nil {
				return nil, fail("internal", http.StatusInternalServerError, "Delegation step is unavailable", err)
			}
		}
		result, err := t.returnToolCalls(ctx, outcome)
		if err != nil {
			return nil, err
		}
		return &delegationOutcome{pending: true, result: result}, nil
	}
	text := outcome.Text
	if int64(len([]rune(text))) > t.engine.opts.CompactEvidenceChars {
		text = compactText(text, t.engine.opts.CompactEvidenceChars)
	}
	if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "ok"); err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
	}
	return &delegationOutcome{text: text}, nil
}

func (t *turn) rotateDelegation(ctx context.Context, delegation *store.Delegation) (*delegationOutcome, error) {
	candidates, err := t.assistantCandidates(ctx, true)
	if err != nil {
		return nil, err
	}
	replacement := candidates[:0]
	for _, item := range candidates {
		if item.Model.ID != delegation.AssistantModelID {
			replacement = append(replacement, item)
		}
	}
	if len(replacement) == 0 {
		if err := t.engine.store.FinishDelegation(ctx, delegation.ID, "no_available_model"); err != nil {
			return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
		}
		return &delegationOutcome{text: "delegation stopped: no_available_model"}, nil
	}
	chosen, _, err := t.decideAssistant(ctx, replacement)
	if err != nil {
		return nil, err
	}
	if err := t.engine.store.SetDelegationAssistant(ctx, delegation.ID, chosen.Model.ID); err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
	}
	if err := t.engine.store.SetDelegationStatus(ctx, delegation.ID, "expert_to_assistant_2"); err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Delegation is unavailable", err)
	}
	delegation.AssistantModelID = chosen.Model.ID
	return t.continueDelegation(ctx, delegation)
}

func (t *turn) helperMessages(ctx context.Context, delegation *store.Delegation) ([]transport.Message, error) {
	stored, err := t.engine.store.Messages(ctx, t.dialogue.ID)
	if err != nil {
		return nil, err
	}
	out := []transport.Message{
		transport.TextMessage("system", helperPrompt(PromptOptions{
			CompactEvidenceChars: t.engine.opts.CompactEvidenceChars,
			MaxDelegationSteps:   t.engine.opts.MaxDelegationSteps,
		}, delegation.Scope)),
	}
	out = append(out, t.instructions...)
	out = append(out, transport.TextMessage("user", "Task: "+delegation.Task+"\nContext: "+delegation.ContextText))
	for _, message := range stored {
		if message.CreatedAtMS < delegation.StartedMS {
			continue
		}
		switch message.Kind {
		case "tool_call":
			out = append(out, transport.Message{Role: "assistant", ToolCalls: []transport.ToolCall{{
				ID: message.CallID, Type: "function",
				Function: transport.FunctionCall{Name: message.ToolName, Arguments: message.Content},
			}}})
		case "tool_result":
			out = append(out, transport.Message{Role: "tool", ToolCallID: message.CallID,
				Content: transport.TextContent(message.Content)})
		case "message":
			out = append(out, transport.Message{Role: message.Role, Content: transport.TextContent(message.Content)})
		}
	}
	return out, nil
}

func deflatorTokenLimit(outputBudgetTokens, reasoningReserveTokens int64, modelOutputTokens int) int64 {
	limit := outputBudgetTokens + reasoningReserveTokens
	if int64(modelOutputTokens) > 0 && int64(modelOutputTokens) < limit {
		limit = int64(modelOutputTokens)
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func (t *turn) deflatorMaxTokens(model catalog.Model, outputBudgetTokens int64) *int64 {
	limit := deflatorTokenLimit(outputBudgetTokens, t.engine.opts.DeflatorReasoningTokens, model.MaxOutput)
	return &limit
}

func (t *turn) compress(
	ctx context.Context, text, purpose, subject string, outputBudgetTokens int64,
) (string, error) {
	outputBudgetChars := outputBudgetTokens * t.engine.opts.CharsPerToken
	if outputBudgetChars <= 0 {
		return "", errors.New("deflator output budget is not usable")
	}
	prompt := deflatorPrompt(PromptOptions{CompactEvidenceChars: outputBudgetChars})
	payload := text
	if subject != "" {
		payload = "Compression context — what this work is about:\n" + subject +
			"\n\nThe fragment to condense:\n" + text
	}
	payload = SanitizeModelText(payload)
	promptTokens := t.estimatedTextTokens(prompt)
	inputTokens := t.estimatedTextTokens(prompt + payload)
	outputReservation := outputBudgetTokens + t.engine.opts.DeflatorReasoningTokens
	filter := router.Filter{
		Role: "assistant", InputToken: inputTokens, OutputToken: outputReservation,
		Modalities: map[string]bool{"text": true}, FreeOnly: t.settings.SubscriptionMode == "free",
	}
	candidates, err := t.engine.router.Candidates(ctx, filter)
	if err != nil {
		return "", fmt.Errorf("deflator is unavailable: %w", err)
	}
	if len(candidates) == 0 {
		if purpose != "deflate" {
			return "", errors.New("no available deflator can accept the full history for compaction")
		}
		filter.InputToken = promptTokens
		candidates, err = t.engine.router.Candidates(ctx, filter)
		if err != nil {
			return "", fmt.Errorf("find a deflator for the tool result tail: %w", err)
		}
		if len(candidates) == 0 {
			return "", errors.New("no available deflator can accept the compression instructions")
		}
		modelIndex := 0
		for index := 1; index < len(candidates); index++ {
			if deflatorInputTokens(candidates[index].Model, outputReservation) >
				deflatorInputTokens(candidates[modelIndex].Model, outputReservation) {
				modelIndex = index
			}
		}
		model := candidates[modelIndex].Model
		maxPayloadTokens := deflatorInputTokens(model, outputReservation) - promptTokens
		if maxPayloadTokens <= 0 {
			return "", errors.New("deflator has no input budget for the tool result")
		}
		maxPayloadBytes := maxPayloadTokens * t.engine.opts.CharsPerToken
		payload = compactTailBytes(payload, maxPayloadBytes)
		inputTokens = t.estimatedTextTokens(prompt + payload)
		filter.InputToken = inputTokens
		candidates, err = t.engine.router.Candidates(ctx, filter)
		if err != nil {
			return "", fmt.Errorf("select a deflator for the tool result tail: %w", err)
		}
		if len(candidates) == 0 {
			return "", errors.New("no available deflator can accept the tool result tail")
		}
	}
	model := candidates[0].Model
	request := transport.ChatRequest{
		Model: model.ID,
		Messages: []transport.Message{
			transport.TextMessage("system", prompt),
			transport.TextMessage("user", payload),
		},
		MaxTokens:       t.deflatorMaxTokens(model, outputBudgetTokens),
		ReasoningEffort: t.engine.router.EffortOrEmpty(model, "min", ""),
		Session:         t.dialogue.ID,
	}
	requestID := store.NewID()
	reservation, err := t.engine.ledger.Reserve(ctx, requestID, model.ID, model.Quota)
	if err != nil {
		return "", fmt.Errorf("reserve deflator quota: %w", err)
	}
	started := time.Now()
	t.engine.log.Info("deflator request diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"upstream_request_id", requestID,
		"purpose", purpose,
		"model", model.ID,
		"input_bytes", len(payload),
		"output_budget_tokens", outputBudgetTokens,
		"max_tokens", request.MaxTokens,
		"reasoning_effort", request.ReasoningEffort,
	)
	response, callErr := t.engine.transport.Do(ctx, request, false)
	if callErr != nil {
		t.engine.log.Info("deflator response diagnostics",
			"dialogue_id", t.dialogue.ID,
			"request_id", t.input.RequestID,
			"turn_request_id", t.dialogue.RequestID,
			"upstream_request_id", requestID,
			"purpose", purpose,
			"model", model.ID,
			"duration_ms", time.Since(started).Milliseconds(),
			"failed", true,
		)
		if releaseErr := t.engine.ledger.Release(context.WithoutCancel(ctx), *reservation); releaseErr != nil {
			t.engine.log.Error("release deflator quota", "error", releaseErr)
		}
		mapped := mapTransportError(callErr)
		t.handleCallError(context.WithoutCancel(ctx), actorDeflator, model, mapped)
		return "", mapped
	}
	if err := t.engine.ledger.Commit(ctx, *reservation); err != nil {
		return "", err
	}
	content := ""
	finish := ""
	var textErr error
	if len(response.Choices) > 0 {
		content, textErr = response.Choices[0].Message.Text()
		finish = response.Choices[0].FinishReason
	}
	content = SanitizeModelText(content)
	t.engine.log.Info("deflator response diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"upstream_request_id", requestID,
		"purpose", purpose,
		"model", model.ID,
		"duration_ms", time.Since(started).Milliseconds(),
		"failed", false,
		"finish", finish,
		"output_bytes", len(content),
		"usage", response.Usage,
	)
	t.actors.add(model.ID, participantRole(actorDeflator))
	if response.Usage != nil {
		t.usage.Merge(response.Usage)
		t.mergeUsage("deflator", response.Usage)
	}
	status := 200
	call := store.ModelCall{
		DialogueID: t.dialogue.ID, Role: "deflator", ModelID: model.ID, Purpose: purpose,
		RequestID: requestID, DurationMS: time.Since(started).Milliseconds(), HTTPStatus: &status,
		ObjectiveMode: t.settings.ObjectiveMode, ReasoningMode: t.settings.ReasoningMode,
		PipelineMode: "pipeline",
	}
	if response.Usage != nil {
		call.InputTokens = response.Usage.PromptTokens
		call.OutputTokens = response.Usage.CompletionTokens
		call.ReasoningTokens = response.Usage.Reasoning()
		if response.Usage.PromptTokensDetails != nil {
			call.CachedTokens = response.Usage.PromptTokensDetails.CachedTokens
		}
	}
	if err := t.engine.store.RecordModelCall(ctx, call); err != nil {
		t.engine.log.Error("record deflator call", "error", err, "request_id", requestID)
	}
	if textErr != nil {
		return "", fmt.Errorf("decode deflator response: %w", textErr)
	}
	switch finish {
	case "", "stop", "length":
	default:
		return "", fmt.Errorf("incomplete deflator result: finish reason %q", finish)
	}
	if strings.TrimSpace(content) == "" {
		return "", fmt.Errorf("deflator returned an empty result")
	}
	actualTextTokens := int64(0)
	if actual := response.Usage; actual != nil && actual.CompletionTokens != nil {
		actualTextTokens = *actual.CompletionTokens
		if reasoning := actual.Reasoning(); reasoning != nil {
			actualTextTokens -= *reasoning
		}
	}
	estimatedOutputTokens := t.estimatedTextTokens(content)
	if actualTextTokens < estimatedOutputTokens {
		actualTextTokens = estimatedOutputTokens
	}
	if actualTextTokens > outputBudgetTokens {
		content = compactTailBytes(content, outputBudgetTokens*t.engine.opts.CharsPerToken)
	}
	return content, nil
}

func deflatorInputTokens(model catalog.Model, outputReservation int64) int64 {
	if int64(model.MaxOutput) < outputReservation {
		outputReservation = int64(model.MaxOutput)
	}
	return int64(model.ContextTokens) - outputReservation
}

func compactTailBytes(text string, limit int64) string {
	if limit <= 0 {
		return ""
	}
	if int64(len(text)) <= limit {
		return text
	}
	start := len(text) - int(limit)
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	return text[start:]
}

// compressionSubject tells the deflator what the fragment belongs to: the human
// request of the current turn and, for a tool result, the call that produced it.
// Without the subject the condenser is a lossy archiver, not a helper (ADR 0017).
func (t *turn) compressionSubject(toolCallID string) string {
	var parts []string
	if request := t.latestHumanFull(); request != "" {
		parts = append(parts, "The human requested: "+request)
	}
	if call := t.toolCallDescription(toolCallID); call != "" {
		parts = append(parts, "The fragment is the result of the tool call "+call+".")
	}
	return strings.Join(parts, "\n")
}

func (t *turn) latestHumanFull() string {
	for i := len(t.input.Messages) - 1; i >= 0; i-- {
		message := t.input.Messages[i]
		if message.Role != "user" {
			continue
		}
		text, err := message.Text()
		if err == nil && text != "" && !isClientCheckpoint(text) {
			return SanitizeModelText(text)
		}
	}
	return ""
}

func (t *turn) toolCallDescription(toolCallID string) string {
	if toolCallID == "" {
		return ""
	}
	for _, message := range t.input.Messages {
		for _, call := range message.ToolCalls {
			if call.ID == toolCallID {
				return fmt.Sprintf("%s(%s)", call.Function.Name,
					SanitizeModelText(call.Function.Arguments))
			}
		}
	}
	return ""
}

// incomingToolCallIDs lists the tool results the client returned in this request.
func (t *turn) incomingToolCallIDs() []string {
	var out []string
	for _, message := range t.input.Messages {
		if message.Role == "tool" && message.ToolCallID != "" {
			out = append(out, message.ToolCallID)
		}
	}
	return out
}

func validScope(scope string) bool {
	switch scope {
	case "search", "read", "external", "write", "full":
		return true
	default:
		return false
	}
}

func filterTools(tools []transport.Tool, scope string, scopes map[string][]string) []transport.Tool {
	if scope == "full" {
		return tools
	}
	allowed, ok := scopes[scope]
	if !ok {
		return tools
	}
	set := make(map[string]bool)
	for _, name := range allowed {
		set[name] = true
	}
	out := make([]transport.Tool, 0, len(tools))
	for _, tool := range tools {
		if set[tool.Function.Name] {
			out = append(out, tool)
		}
	}
	return out
}

func isNoQuota(err error) bool {
	var typed *Error
	return errors.As(err, &typed) && typed.Code == "no_quota"
}

func isUpstreamFailure(err error) bool {
	var typed *Error
	if !errors.As(err, &typed) {
		return false
	}
	switch typed.Code {
	case "upstream_failed", "upstream_timeout", "upstream_rate_limited":
		return true
	default:
		return false
	}
}
