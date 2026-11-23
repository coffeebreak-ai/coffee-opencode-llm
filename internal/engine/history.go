package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

const percentScale = 100

var errCompactionFailed = errors.New("history compaction failed")

func (t *turn) syncHistory(ctx context.Context, incoming []transport.Message) error {
	stored, err := t.engine.store.Messages(ctx, t.dialogue.ID)
	if err != nil {
		return err
	}
	if checkpointFrom(incoming) != "" {
		t.clientHistory = true
		t.engine.log.Info("history synchronization diagnostics",
			"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
			"turn_request_id", t.dialogue.RequestID, "source", "client_checkpoint",
			"stored_messages", len(stored), "client_messages", len(incoming),
		)
		if len(stored) == 0 {
			return t.appendIncoming(ctx, incoming)
		}
		return t.appendClientViewTail(ctx, stored, incoming)
	}
	if len(stored) == 0 {
		t.engine.log.Info("history synchronization diagnostics",
			"dialogue_id", t.dialogue.ID,
			"request_id", t.input.RequestID,
			"turn_request_id", t.dialogue.RequestID,
			"source", "initial",
			"stored_messages", 0,
			"client_messages", len(incoming),
		)
		return t.appendIncoming(ctx, incoming)
	}
	storedMarkers := historyMarkers(stored)
	incomingMarkers := incomingMarkers(incoming)
	matched := 0
	for matched < len(storedMarkers) && matched < len(incomingMarkers) &&
		storedMarkers[matched] == incomingMarkers[matched] {
		matched++
	}
	source := "stored"
	if matched < len(storedMarkers) {
		source = "client"
	}
	t.engine.log.Info("history synchronization diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"source", source,
		"stored_messages", len(stored),
		"client_messages", len(incoming),
		"stored_markers", len(storedMarkers),
		"client_markers", len(incomingMarkers),
		"matched_markers", matched,
		"client_history", matched < len(storedMarkers),
	)
	if matched < len(storedMarkers) {
		// The client compacted or rewrote its history: the stored log stays the
		// record of the dialogue, but the prompt follows the client view so the
		// context does not grow without bound.
		t.clientHistory = true
		t.engine.log.Warn("client history does not match the stored dialogue, using the client view for the prompt",
			"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
			"stored_markers", len(storedMarkers), "incoming_markers", len(incomingMarkers),
			"matched", matched)
		return t.appendClientViewTail(ctx, stored, incoming)
	}
	if matched >= len(incomingMarkers) {
		return nil
	}
	return t.appendNewTail(ctx, incoming, matched)
}

func (t *turn) appendClientViewTail(ctx context.Context, stored []store.Message, incoming []transport.Message) error {
	toolResults := make(map[string]bool)
	checkpoints := make(map[string]bool)
	for _, message := range stored {
		if message.Kind == "tool_result" && message.CallID != "" {
			toolResults[message.CallID] = true
		}
		if message.Kind == "human" && isClientCheckpoint(message.Content) {
			checkpoints[message.Content] = true
		}
	}
	var batch []store.Message
	for i, message := range incoming {
		switch message.Role {
		case "user":
			text, err := message.Text()
			if err != nil {
				return fmt.Errorf("read client history: %w", err)
			}
			if isClientCheckpoint(text) {
				if !checkpoints[text] {
					batch = append(batch, messageRecords(message)...)
					checkpoints[text] = true
				}
			} else if t.newRequest && i == len(incoming)-1 {
				batch = append(batch, messageRecords(message)...)
			}
		case "tool":
			if message.ToolCallID != "" && !toolResults[message.ToolCallID] {
				batch = append(batch, messageRecords(message)...)
				toolResults[message.ToolCallID] = true
			}
		}
	}
	if len(batch) == 0 {
		return nil
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, batch)
	if err != nil {
		return fmt.Errorf("append client history: %w", err)
	}
	return nil
}

// appendIncoming stores the first request of a dialogue. System and developer
// instructions are per-request policy and are not part of the dialogue history.
func (t *turn) appendIncoming(ctx context.Context, incoming []transport.Message) error {
	var batch []store.Message
	for _, message := range incoming {
		if message.Role == "system" || message.Role == "developer" {
			continue
		}
		compacted, err := t.compactToolResult(ctx, message)
		if err != nil {
			return err
		}
		batch = append(batch, messageRecords(compacted)...)
	}
	if len(batch) == 0 {
		return nil
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, batch)
	return err
}

// appendNewTail stores human messages and client tool results that the dialogue
// does not contain yet. Assistant messages are the gateway's own output.
func (t *turn) appendNewTail(ctx context.Context, incoming []transport.Message, matched int) error {
	var batch []store.Message
	position := 0
	for _, message := range incoming {
		switch message.Role {
		case "user", "tool":
		default:
			continue
		}
		index := position
		position++
		if index < matched {
			continue
		}
		compacted, err := t.compactToolResult(ctx, message)
		if err != nil {
			return err
		}
		batch = append(batch, messageRecords(compacted)...)
	}
	if len(batch) == 0 {
		return nil
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, batch)
	return err
}

func deflateNeeded(text string, thresholdChars int64) bool {
	size := int64(len([]rune(text)))
	return size > thresholdChars
}

func (t *turn) compactToolResult(
	ctx context.Context, message transport.Message,
) (transport.Message, error) {
	if message.Role != "tool" {
		return message, nil
	}
	text, err := message.Text()
	if err != nil {
		t.engine.log.Warn("результат инструмента не текстовый; дефляция пропущена",
			"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID, "error", err)
		return message, nil
	}
	text = SanitizeModelText(text)
	message.Content = transport.TextContent(text)
	eligible := deflateNeeded(text, t.engine.opts.DeflateThresholdChars) &&
		t.dialogue.Owner == "expert"
	t.engine.log.Info("deflate eligibility diagnostics",
		"dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID,
		"turn_request_id", t.dialogue.RequestID,
		"owner", t.dialogue.Owner,
		"result_chars", len([]rune(text)),
		"threshold_chars", t.engine.opts.DeflateThresholdChars,
		"eligible", eligible,
	)
	if !eligible {
		return message, nil
	}
	t.recordStage(ctx, "client_tools_to_adjuster_3")
	inputTokens := t.estimatedTextTokens(text)
	subject := t.compressionSubject(message.ToolCallID)
	decisionState := t.deflateDecisionState(message.ToolCallID, text)
	evidence, _ := decisionState["evidence"].(string)
	hash := deflateCacheKey(evidence, subject, inputTokens)
	if cached, ok, err := t.engine.store.Deflated(ctx, hash); err == nil && ok {
		if t.estimatedTextTokens(cached) < inputTokens {
			message.Content = transport.TextContent(cached)
			t.recordStage(ctx, "deflator_to_expert")
			return message, nil
		}
	} else if err != nil {
		t.engine.log.Warn("read the deflate cache", "error", err, "dialogue_id", t.dialogue.ID)
	}
	compress, answer, err := t.engine.router.CheckDeflate(ctx, decisionState)
	if err != nil {
		t.engine.log.Warn("Jev не смог решить, сжимать ли результат инструмента",
			"error", err, "dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID)
		t.recordStage(ctx, "adjuster_3_to_expert")
		return message, fmt.Errorf("%w: tool-result compression decision: %w", errCompactionFailed, err)
	}
	t.engine.log.Info("решение о сжатии результата инструмента",
		"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
		"choice", answer.Choice, "confidence", answer.Confidence, "compress", compress)
	if !compress {
		t.recordStage(ctx, "adjuster_3_to_expert")
		return message, nil
	}
	t.recordStage(ctx, "adjuster_3_to_deflator")
	condensed, err := t.compress(ctx, text, "deflate", subject, inputTokens)
	if err != nil {
		t.engine.log.Warn("дефлятор не смог сжать результат инструмента",
			"error", err, "dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID)
		t.recordStage(ctx, "adjuster_3_to_expert")
		return message, fmt.Errorf("%w: tool-result deflator: %w", errCompactionFailed, err)
	}
	if t.estimatedTextTokens(condensed) >= inputTokens {
		t.engine.log.Warn("дефлятор не уменьшил результат инструмента",
			"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
			"original_tokens", inputTokens, "compressed_tokens", t.estimatedTextTokens(condensed))
		t.recordStage(ctx, "adjuster_3_to_expert")
		return message, fmt.Errorf("%w: tool-result summary did not reduce the input", errCompactionFailed)
	}
	if err := t.engine.store.SaveDeflated(ctx, hash, condensed); err != nil {
		t.engine.log.Warn("store the deflate cache", "error", err, "dialogue_id", t.dialogue.ID)
	}
	message.Content = transport.TextContent(condensed)
	t.recordStage(ctx, "deflator_to_expert")
	return message, nil
}

func deflateCacheKey(evidence, subject string, limit int64) string {
	return contentHash("tool-deflate-v2\x00" + evidence + "\x00" + subject +
		"\x00" + strconv.FormatInt(limit, 10))
}

func (t *turn) deflateDecisionState(toolCallID, result string) map[string]any {
	var contextText strings.Builder
	for _, message := range t.input.Messages {
		if message.Role == "system" || message.Role == "developer" ||
			(message.Role == "tool" && message.ToolCallID == toolCallID) {
			continue
		}
		if message.Role != "assistant" {
			text, err := message.Text()
			if err != nil {
				t.engine.log.Debug("skip non-text content in deflation context",
					"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID, "role", message.Role)
			} else if text != "" {
				fmt.Fprintf(&contextText, "%s: %s\n", message.Role, SanitizeModelText(text))
			}
		}
		for _, call := range message.ToolCalls {
			fmt.Fprintf(&contextText, "tool call %s(%s)\n",
				call.Function.Name, SanitizeModelText(call.Function.Arguments))
		}
	}
	evidence := fmt.Sprintf("Context:\n%s\nRequest:\n%s\nTool call:\n%s\nResult:\n%s",
		contextText.String(), t.latestHumanFull(), t.toolCallDescription(toolCallID), result)
	return map[string]any{"evidence": evidence, "truncated": false}
}

func (t *turn) estimatedTextTokens(text string) int64 {
	size := int64(len(text))
	per := t.engine.opts.CharsPerToken
	if per <= 0 {
		return size
	}
	return (size + per - 1) / per
}

func contentHash(text string) string {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(text))
	return fmt.Sprintf("%x", hash.Sum64())
}

// historyMarkers identifies stored messages by structure, not by text: clients
// rewrite their opening preamble on every request, so comparing content would
// report a mismatch forever and the dialogue would stop growing.
func historyMarkers(messages []store.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		switch message.Kind {
		case "human":
			out = append(out, "user")
		case "tool_result":
			out = append(out, "tool\x00"+message.CallID)
		}
	}
	return out
}

func incomingMarkers(messages []transport.Message) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		switch message.Role {
		case "user":
			out = append(out, "user")
		case "tool":
			out = append(out, "tool\x00"+message.ToolCallID)
		}
	}
	return out
}

func (t *turn) historyMessages(
	ctx context.Context, model catalog.Model, prompt string, tools []transport.Tool,
) ([]transport.Message, error) {
	dialog, checkpoint, err := t.dialogueMessages(ctx)
	if err != nil {
		if errors.Is(err, errCompactionFailed) {
			return nil, t.markCompactionFailed(ctx, err)
		}
		return nil, fail("internal", http.StatusInternalServerError, "History is unavailable", err)
	}
	dialog = sanitizeHistoryMessages(dialog)
	out := make([]transport.Message, 0, len(t.instructions)+len(dialog)+3)
	out = append(out, transport.TextMessage("system", prompt))
	out = append(out, t.instructions...)
	if current := checkpointFrom(t.input.Messages); current != "" {
		checkpoint = current
	}
	if checkpoint != "" {
		out = append(out, transport.TextMessage("system", checkpoint))
	}
	toolBytes, err := json.Marshal(tools)
	if err != nil {
		return nil, fail("internal", http.StatusInternalServerError, "Tool catalog is unavailable", err)
	}
	fixedChars := len(toolBytes)
	for _, message := range out {
		fixedChars += messageChars(message)
	}
	dialog, summary, err := t.compactHistory(ctx, dialog, model, int64(fixedChars))
	if err != nil {
		return nil, t.markCompactionFailed(ctx, err)
	}
	if summary != "" {
		out = append(out, transport.TextMessage("system", "Summary of the earlier conversation:\n"+summary))
	}
	out = append(out, dialog...)
	return out, nil
}

const clientCheckpointPrefix = "<conversation-checkpoint>"

func isClientCheckpoint(text string) bool {
	return strings.HasPrefix(strings.TrimSpace(text), clientCheckpointPrefix)
}

func checkpointFrom(messages []transport.Message) string {
	latest := ""
	for _, message := range messages {
		if message.Role != "user" {
			continue
		}
		text, err := message.Text()
		if err == nil && isClientCheckpoint(text) {
			latest = text
		}
	}
	return latest
}

func (t *turn) dialogueMessages(ctx context.Context) ([]transport.Message, string, error) {
	stored, err := t.engine.store.Messages(ctx, t.dialogue.ID)
	if err != nil {
		return nil, "", err
	}
	reasoningByGroup, representedReasoning := archivedReasoning(stored, t.clientMessages)
	if t.clientHistory {
		return t.clientDialogueMessages(ctx, stored, reasoningByGroup, representedReasoning)
	}
	return t.storedDialogueMessages(stored, reasoningByGroup, representedReasoning)
}

func (t *turn) clientDialogueMessages(
	ctx context.Context, stored []store.Message,
	reasoningByGroup map[string]string, representedReasoning map[string]bool,
) ([]transport.Message, string, error) {
	out := make([]transport.Message, 0, len(t.input.Messages))
	checkpoint := ""
	for index, message := range t.input.Messages {
		clientMessage := t.clientMessages[index]
		if message.Role == "system" || message.Role == "developer" {
			continue
		}
		if message.Role == "assistant" {
			if len(message.ToolCalls) == 0 {
				if reasoning, ok := archivedAssistantReasoning(
					stored, clientMessage, reasoningByGroup, representedReasoning,
				); ok {
					out = append(out, reasoningMessage(reasoning))
				}
				continue
			}
			message.Content = nil
			if reasoning, ok := archivedToolReasoning(
				stored, clientMessage, reasoningByGroup, representedReasoning,
			); ok {
				message.ReasoningContent = &reasoning
			}
		}
		if message.Role == "user" {
			text, err := message.Text()
			if err == nil && isClientCheckpoint(text) {
				checkpoint = text
				continue
			}
		}
		compacted, err := t.compactToolResult(ctx, message)
		if err != nil {
			return nil, checkpoint, err
		}
		out = append(out, compacted)
	}
	return out, checkpoint, nil
}

func (t *turn) storedDialogueMessages(
	stored []store.Message, reasoningByGroup map[string]string, representedReasoning map[string]bool,
) ([]transport.Message, string, error) {
	out := make([]transport.Message, 0, len(stored))
	checkpoint := ""
	batches := clientToolBatches(t.input.Messages)
	for i := 0; i < len(stored); i++ {
		message := stored[i]
		if message.Role == "system" || message.Role == "developer" {
			continue
		}
		if message.Kind == "human" && isClientCheckpoint(message.Content) {
			checkpoint = message.Content
			continue
		}
		if message.Kind == "tool_call" {
			if batch, count, ok := matchingToolBatch(stored[i:], t.input.Messages, batches); ok {
				clientIndex := batches[stored[i].CallID]
				if clientIndex >= 0 && clientIndex < len(t.clientMessages) {
					if reasoning, matched := archivedToolReasoning(
						stored, t.clientMessages[clientIndex], reasoningByGroup, representedReasoning,
					); matched {
						batch.ReasoningContent = &reasoning
					}
				}
				if i > 0 && stored[i-1].Role == "assistant" && stored[i-1].Kind == "message" &&
					len(out) > 0 && out[len(out)-1].Role == "assistant" {
					text, err := batch.Text()
					if err == nil && text != "" && text == stored[i-1].Content {
						out = out[:len(out)-1]
					}
				}
				out = append(out, batch)
				i += count - 1
				continue
			}
		}
		item := transport.Message{Role: message.Role, ToolCallID: message.CallID}
		if len(message.Parts) > 0 {
			raw, err := partsJSON(message.Parts)
			if err != nil {
				return nil, "", err
			}
			item.Content = raw
		} else if message.Content != "" {
			item.Content = transport.TextContent(message.Content)
		}
		switch message.Kind {
		case "tool_call":
			item.Role = "assistant"
			item.ToolCallID = ""
			item.ToolCalls = []transport.ToolCall{{
				ID: message.CallID, Type: "function",
				Function: transport.FunctionCall{Name: message.ToolName, Arguments: message.Content},
			}}
			item.Content = nil
		case "tool_result":
			item.Role = "tool"
			item.ToolCallID = message.CallID
		case "message":
			if message.Role == "assistant" {
				// A reasoning row of the current turn group was written by this
				// turn seconds ago: it is trusted by construction and enters the
				// next in-turn prompt without waiting for the client echo.
				// Older groups stay behind the exact client-history check.
				if message.ReasoningContent != "" && message.ReasoningGroupID == t.reasoningGroup {
					out = append(out, reasoningMessage(message.ReasoningContent))
					continue
				}
				if reasoning, ok := archivedAssistantReasoning(
					stored, matchingClientAssistant(t.clientMessages, message.Content),
					reasoningByGroup, representedReasoning,
				); ok {
					out = append(out, reasoningMessage(reasoning))
				}
				continue
			}
		case "delegation", "delegation_result":
			item.Role = "user"
			item.Content = transport.TextContent(message.Content)
		case "service":
			continue
		}
		out = append(out, item)
	}
	return out, checkpoint, nil
}

func archivedReasoning(
	stored []store.Message, client []transport.Message,
) (map[string]string, map[string]bool) {
	textByGroup := make(map[string]string)
	for _, message := range stored {
		if message.ReasoningGroupID != "" && message.ReasoningContent != "" {
			textByGroup[message.ReasoningGroupID] += message.ReasoningContent
		}
	}
	represented := make(map[string]bool)
	for _, message := range stored {
		group := message.ReasoningGroupID
		reasoning := textByGroup[group]
		if group == "" || reasoning == "" {
			continue
		}
		switch {
		case message.Kind == "message" && message.Role == "assistant" && message.Content != "":
			for _, clientMessage := range client {
				text, err := clientMessage.Text()
				if err == nil && clientMessage.Role == "assistant" && len(clientMessage.ToolCalls) == 0 &&
					SanitizeModelText(text) == message.Content && SanitizeModelText(messageReasoningText(clientMessage)) == reasoning {
					represented[group] = true
					break
				}
			}
		case message.Kind == "tool_call":
			for _, clientMessage := range client {
				if clientMessage.Role == "assistant" && SanitizeModelText(messageReasoningText(clientMessage)) == reasoning &&
					matchesStoredToolBatch(stored, group, clientMessage.ToolCalls) {
					represented[group] = true
					break
				}
			}
		}
	}
	return textByGroup, represented
}

func archivedAssistantReasoning(
	stored []store.Message, clientMessage transport.Message, textByGroup map[string]string,
	represented map[string]bool,
) (string, bool) {
	if clientMessage.Role != "assistant" || len(clientMessage.ToolCalls) != 0 ||
		SanitizeModelText(messageReasoningText(clientMessage)) == "" {
		return "", false
	}
	text, err := clientMessage.Text()
	if err != nil {
		return "", false
	}
	for _, message := range stored {
		group := message.ReasoningGroupID
		if message.Kind == "message" && message.Role == "assistant" && message.Content != "" &&
			message.Content == SanitizeModelText(text) &&
			group != "" && represented[group] &&
			SanitizeModelText(messageReasoningText(clientMessage)) == textByGroup[group] {
			return textByGroup[group], true
		}
	}
	return "", false
}

func archivedToolReasoning(
	stored []store.Message, clientMessage transport.Message, textByGroup map[string]string,
	represented map[string]bool,
) (string, bool) {
	if clientMessage.Role != "assistant" || len(clientMessage.ToolCalls) == 0 {
		return "", false
	}
	clientReasoning := SanitizeModelText(messageReasoningText(clientMessage))
	for _, message := range stored {
		group := message.ReasoningGroupID
		if message.Kind == "tool_call" && group != "" && represented[group] &&
			textByGroup[group] == clientReasoning &&
			matchesStoredToolBatch(stored, group, clientMessage.ToolCalls) {
			return textByGroup[group], true
		}
	}
	return "", false
}

func matchesStoredToolBatch(stored []store.Message, group string, calls []transport.ToolCall) bool {
	var archived []store.Message
	for _, message := range stored {
		if message.Kind == "tool_call" && message.ReasoningGroupID == group {
			archived = append(archived, message)
		}
	}
	if len(archived) != len(calls) {
		return false
	}
	for index, call := range calls {
		if archived[index].CallID != call.ID || archived[index].ToolName != call.Function.Name ||
			archived[index].Content != call.Function.Arguments {
			return false
		}
	}
	return true
}

func matchingClientAssistant(messages []transport.Message, content string) transport.Message {
	for _, message := range messages {
		if message.Role != "assistant" || len(message.ToolCalls) != 0 {
			continue
		}
		text, err := message.Text()
		if err == nil && SanitizeModelText(text) == content {
			return message
		}
	}
	return transport.Message{}
}

func reasoningMessage(text string) transport.Message {
	return transport.Message{Role: "assistant", Content: json.RawMessage("null"), ReasoningContent: &text}
}

func clientToolBatches(incoming []transport.Message) map[string]int {
	batches := make(map[string]int)
	for i, message := range incoming {
		if message.Role != "assistant" {
			continue
		}
		for _, call := range message.ToolCalls {
			if call.ID == "" {
				continue
			}
			if _, exists := batches[call.ID]; exists {
				batches[call.ID] = -1
			} else {
				batches[call.ID] = i
			}
		}
	}
	return batches
}

func matchingToolBatch(stored []store.Message, incoming []transport.Message, batches map[string]int) (transport.Message, int, bool) {
	if len(stored) == 0 {
		return transport.Message{}, 0, false
	}
	index, ok := batches[stored[0].CallID]
	if !ok || index < 0 {
		return transport.Message{}, 0, false
	}
	message := incoming[index]
	if len(message.ToolCalls) == 0 || len(stored) < len(message.ToolCalls) {
		return transport.Message{}, 0, false
	}
	for i, call := range message.ToolCalls {
		item := stored[i]
		batchIndex, exists := batches[call.ID]
		if !exists || call.ID == "" || batchIndex != index || item.Kind != "tool_call" ||
			item.CallID != call.ID || item.ToolName != call.Function.Name || item.Content != call.Function.Arguments {
			return transport.Message{}, 0, false
		}
	}
	return message, len(message.ToolCalls), true
}

func (t *turn) compactHistory(
	ctx context.Context, dialog []transport.Message, model catalog.Model, fixedChars int64,
) ([]transport.Message, string, error) {
	hard := t.promptBudget(model)
	if hard <= 0 || t.engine.opts.CharsPerToken <= 0 ||
		t.engine.opts.HistoryCompactPct <= 0 || t.engine.opts.HistorySummaryPct <= 0 {
		return nil, "", fmt.Errorf("%w: selected model has no usable input budget", errCompactionFailed)
	}
	trigger := hard * t.engine.opts.HistoryCompactPct / percentScale
	summaryBudget := trigger * t.engine.opts.HistorySummaryPct / percentScale
	currentTurnStart := lastUserIndex(dialog)
	if currentTurnStart <= 0 {
		promptDialog := promptHistoryMessages(dialog, t.settings.ReasoningContextEnabled)
		if estimatedTokens(fixedChars+dialogBytes(promptDialog), t.engine.opts.CharsPerToken) <= trigger {
			return promptDialog, "", nil
		}
		return nil, "", fmt.Errorf("%w: no earlier history prefix can be compacted", errCompactionFailed)
	}
	head, tail := dialog[:currentTurnStart], dialog[currentTurnStart:]
	border, err := borderHash(head)
	if err != nil {
		return nil, "", fmt.Errorf("%w: identify history boundary: %w", errCompactionFailed, err)
	}
	stored, found, err := t.engine.store.Compaction(ctx, t.dialogue.ID)
	if err != nil {
		return nil, "", fmt.Errorf("%w: read previous summary: %w", errCompactionFailed, err)
	}
	summary := ""
	covered := 0
	if found && stored.Summary != "" && stored.CoveredCount > 0 && stored.CoveredCount <= int64(len(head)) {
		previousHash, hashErr := borderHash(head[:stored.CoveredCount])
		if hashErr != nil {
			return nil, "", fmt.Errorf("%w: verify previous summary: %w", errCompactionFailed, hashErr)
		} else if previousHash == stored.BorderHash {
			cleanSummary := SanitizeModelText(stored.Summary)
			if cleanSummary != "" {
				covered, summary = int(stored.CoveredCount), cleanSummary
			}
		}
	}
	if t.settings.ReasoningContextEnabled && covered > 0 && !stored.ReasoningIncluded {
		covered = 0
		summary = ""
	}
	if covered == len(head) && t.estimatedTextTokens(summary) > summaryBudget {
		covered = 0
		summary = ""
	}
	delta := head[covered:]
	promptDelta := promptHistoryMessages(delta, t.settings.ReasoningContextEnabled)
	promptTail := promptHistoryMessages(tail, t.settings.ReasoningContextEnabled)
	effectiveChars := fixedChars + int64(messageChars(
		transport.TextMessage("system", "Summary of the earlier conversation:\n"+summary),
	))
	if summary == "" {
		effectiveChars = fixedChars
	}
	effectiveChars += dialogBytes(promptDelta) + dialogBytes(promptTail)
	effectiveTokens := estimatedTokens(effectiveChars, t.engine.opts.CharsPerToken)
	if effectiveTokens <= trigger {
		return appendHistory(promptDelta, promptTail), summary, nil
	}
	if len(promptDelta) == 0 {
		if effectiveTokens <= hard {
			return appendHistory(promptDelta, promptTail), summary, nil
		}
		return nil, "", fmt.Errorf("%w: compacted summary and current turn exceed model budget",
			errCompactionFailed)
	}
	if summaryBudget <= 0 {
		return nil, "", fmt.Errorf("%w: summary budget is not usable", errCompactionFailed)
	}
	t.engine.log.Info("compaction check",
		"dialogue_id", t.dialogue.ID, "request_id", t.input.RequestID,
		"model", model.ID, "archive_messages", len(dialog), "uncovered_messages", len(delta),
		"boundary", "current_turn", "effective_tokens", effectiveTokens,
		"hard_tokens", hard, "trigger_tokens", trigger, "summary_budget_tokens", summaryBudget)
	t.progress(Progress{Kind: "deflate", Stage: "compaction", Category: "stage",
		Role: "deflator", Reason: fmt.Sprintf("%d messages, current_turn", len(delta))})
	summary, err = t.summarize(ctx, delta, summary, t.latestHumanFull(), summaryBudget)
	if err != nil {
		return nil, "", fmt.Errorf("%w: summarize history: %w", errCompactionFailed, err)
	}
	summary = compactTailBytes(summary, summaryBudget*t.engine.opts.CharsPerToken)
	headBytes := dialogBytes(promptHistoryMessages(head, t.settings.ReasoningContextEnabled))
	if t.estimatedTextTokens(summary) >= estimatedTokens(headBytes, t.engine.opts.CharsPerToken) {
		return nil, "", fmt.Errorf("%w: summary did not reduce the covered history", errCompactionFailed)
	}
	resultChars := fixedChars + int64(messageChars(
		transport.TextMessage("system", "Summary of the earlier conversation:\n"+summary),
	)) + dialogBytes(promptTail)
	if estimatedTokens(resultChars, t.engine.opts.CharsPerToken) > hard {
		return nil, "", fmt.Errorf("%w: compacted history exceeds selected model budget", errCompactionFailed)
	}
	if err := t.engine.store.SaveCompaction(ctx, t.dialogue.ID, border, int64(len(head)),
		summary, t.settings.ReasoningContextEnabled); err != nil {
		return nil, "", fmt.Errorf("%w: save summary: %w", errCompactionFailed, err)
	}
	t.engine.log.Info("compaction applied", "dialogue_id", t.dialogue.ID,
		"request_id", t.input.RequestID, "covered", len(head), "tail_messages", len(tail),
		"summary_tokens", t.estimatedTextTokens(summary))
	return promptTail, summary, nil
}

func (t *turn) markCompactionFailed(ctx context.Context, cause error) error {
	bookkeeping := context.WithoutCancel(ctx)
	requestID := t.input.RequestID
	if requestID == "" {
		requestID = t.dialogue.RequestID
	}
	failureHash := contentHash("compaction-failed\x00" + t.dialogue.ID + "\x00" + requestID)
	if err := t.engine.store.MarkCompactionFailure(bookkeeping, t.dialogue.ID, failureHash); err != nil {
		return fail("internal", http.StatusInternalServerError,
			"Context compaction failed and the session state could not be saved",
			errors.Join(fmt.Errorf("compaction: %w", cause), fmt.Errorf("record failure: %w", err)))
	}
	actor := actorAssistant
	if t.dialogue.Owner == "expert" {
		actor = actorExpert
	}
	t.writeStop(bookkeeping, actor, "upstream_error")
	t.engine.log.Warn("compaction failed; dialogue is blocked until a new OpenCode session",
		"dialogue_id", t.dialogue.ID, "request_id", requestID, "error", cause)
	return fail("invalid_request", http.StatusUnprocessableEntity,
		"Context compaction failed; start a new OpenCode session", cause)
}

func appendHistory(delta, tail []transport.Message) []transport.Message {
	out := make([]transport.Message, 0, len(delta)+len(tail))
	out = append(out, delta...)
	out = append(out, tail...)
	return out
}

func dialogBytes(messages []transport.Message) int64 {
	var total int64
	for _, message := range messages {
		total += int64(messageChars(message))
	}
	return total
}

func (t *turn) promptBudget(model catalog.Model) int64 {
	reservation := int64(model.MaxOutput)
	if limit := t.outputLimit(model); limit != nil && *limit > reservation {
		reservation = *limit
	}
	budget := int64(model.ContextTokens) - reservation
	return budget
}

// lastUserIndex returns the index of the newest human message, or -1.
func lastUserIndex(dialog []transport.Message) int {
	for i := len(dialog) - 1; i >= 0; i-- {
		if dialog[i].Role == "user" {
			return i
		}
	}
	return -1
}

// recordStage writes a transient compression stage to the journal. The stage runs
// inside a model step, so dialogue.phase keeps the turn status and only the
// journal shows the sequence (ADR 0009 §10).
func (t *turn) recordStage(ctx context.Context, kind string) {
	if err := t.engine.store.Journal(context.WithoutCancel(ctx), store.JournalEvent{
		DialogueID: t.dialogue.ID, Level: "info", Kind: kind,
		Role: t.dialogue.Owner, RequestID: t.input.RequestID,
		Note: "tool result compression",
	}); err != nil {
		t.engine.log.Warn("write the compression stage", "error", err, "kind", kind)
	}
	t.progress(Progress{Kind: "deflate", Stage: kind, Category: "deflate", Role: "deflator"})
}

// summarize sends the full uncovered history prefix and prior summary in one
// deflator request. The caller treats every failure as a terminal compaction error.
func (t *turn) summarize(
	ctx context.Context, messages []transport.Message, summary, subject string, outputBudgetTokens int64,
) (string, error) {
	if len(messages) == 0 {
		return "", errors.New("history has no messages to summarize")
	}
	summary = SanitizeModelText(summary)
	subject = SanitizeModelText(subject)
	safeMessages := make([]transport.Message, len(messages))
	copy(safeMessages, messages)
	for i := range safeMessages {
		safeMessages[i].Content = cleanMessageContent(safeMessages[i].Content)
		if !t.settings.ReasoningContextEnabled {
			safeMessages[i].ReasoningContent = nil
			safeMessages[i].Reasoning = nil
			safeMessages[i].ReasoningText = nil
		} else {
			safeMessages[i].ReasoningContent = cleanStringPointer(safeMessages[i].ReasoningContent)
			safeMessages[i].Reasoning = cleanStringPointer(safeMessages[i].Reasoning)
			safeMessages[i].ReasoningText = cleanStringPointer(safeMessages[i].ReasoningText)
		}
		safeMessages[i].ToolCalls = append([]transport.ToolCall(nil), safeMessages[i].ToolCalls...)
		for callIndex := range safeMessages[i].ToolCalls {
			safeMessages[i].ToolCalls[callIndex].Function.Arguments =
				SanitizeModelText(safeMessages[i].ToolCalls[callIndex].Function.Arguments)
		}
	}
	history, err := json.Marshal(safeMessages)
	if err != nil {
		return "", fmt.Errorf("encode history for compaction: %w", err)
	}
	var payload strings.Builder
	if subject != "" {
		payload.WriteString("The current human request, for context:\n")
		payload.WriteString(subject)
		payload.WriteString("\n\n")
	}
	if summary != "" {
		payload.WriteString("Previously compacted summary:\n")
		payload.WriteString(summary)
		payload.WriteString("\n\n")
	}
	payload.WriteString("Complete history to compact:\n")
	payload.Write(history)
	next, err := t.compress(ctx, payload.String(), "summary", "", outputBudgetTokens)
	if err != nil {
		return "", fmt.Errorf("compress complete history: %w", err)
	}
	if t.estimatedTextTokens(next) >= t.estimatedTextTokens(payload.String()) {
		return "", errors.New("history summary did not reduce the input")
	}
	return next, nil
}

func borderHash(messages []transport.Message) (string, error) {
	raw, err := json.Marshal(messages)
	if err != nil {
		return "", fmt.Errorf("encode compacted history: %w", err)
	}
	versioned := append([]byte("model-budget-compaction-v1\x00"), raw...)
	hash := sha256.Sum256(versioned)
	return fmt.Sprintf("%x", hash), nil
}

func estimatedTokens(size, bytesPerToken int64) int64 {
	if size <= 0 {
		return 0
	}
	if bytesPerToken <= 0 {
		return size
	}
	return (size + bytesPerToken - 1) / bytesPerToken
}

func messageChars(message transport.Message) int {
	chars := len(message.Content) + reasoningBytes(message)
	for _, call := range message.ToolCalls {
		chars += len(call.Function.Name) + len(call.Function.Arguments)
	}
	return chars
}

func (t *turn) appendAssistantText(ctx context.Context, text, reasoningGroup string) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, []store.Message{{
		Role: "assistant", Kind: "message", Content: text, ReasoningGroupID: reasoningGroup,
	}})
	return err
}

func (t *turn) appendReasoning(ctx context.Context, text string) error {
	if t.reasoningGroup == "" {
		return errors.New("reasoning archive has no group id")
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, []store.Message{{
		Role: "assistant", Kind: "message", ReasoningContent: text, ReasoningGroupID: t.reasoningGroup,
	}})
	if err != nil {
		return fmt.Errorf("append reasoning archive: %w", err)
	}
	return nil
}

func (t *turn) reasoningGroupID() string {
	return t.reasoningGroup
}

func (t *turn) appendInternal(ctx context.Context, message store.Message) error {
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, []store.Message{message})
	return err
}

func (t *turn) appendToolCalls(ctx context.Context, calls []transport.ToolCall, reasoningGroup string) error {
	var batch []store.Message
	for _, call := range calls {
		batch = append(batch, store.Message{Role: "assistant", Kind: "tool_call", Content: call.Function.Arguments,
			CallID: call.ID, ToolName: call.Function.Name, ReasoningGroupID: reasoningGroup})
	}
	if len(batch) == 0 {
		return nil
	}
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, batch)
	return err
}

func (t *turn) appendDelegationResult(ctx context.Context, text string) error {
	_, err := t.engine.store.AppendMessages(ctx, t.dialogue.ID, []store.Message{{
		Role: "user", Kind: "delegation_result", Content: text,
	}})
	return err
}

func messageRecords(message transport.Message) []store.Message {
	content, parts := parseContent(message.Content)
	switch message.Role {
	case "assistant":
		var batch []store.Message
		if content != "" {
			batch = append(batch, store.Message{Role: "assistant", Kind: "message", Content: content, Parts: parts})
		}
		for _, call := range message.ToolCalls {
			batch = append(batch, store.Message{Role: "assistant", Kind: "tool_call",
				Content: call.Function.Arguments, CallID: call.ID, ToolName: call.Function.Name})
		}
		return batch
	case "tool":
		return []store.Message{{Role: "tool", Kind: "tool_result", Content: content,
			CallID: message.ToolCallID, Parts: parts}}
	case "user":
		return []store.Message{{Role: "user", Kind: "human", Content: content, Parts: parts}}
	default:
		return []store.Message{{Role: message.Role, Kind: "message", Content: content, Parts: parts}}
	}
}

func parseContent(raw json.RawMessage) (string, []store.Part) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		ImageURL *struct {
			URL string `json:"url"`
		} `json:"image_url"`
		InputAudio *struct {
			Data   string `json:"data"`
			Format string `json:"format"`
		} `json:"input_audio"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", nil
	}
	out := make([]store.Part, 0, len(parts))
	var content strings.Builder
	for index, part := range parts {
		item := store.Part{Seq: index, Type: part.Type}
		switch part.Type {
		case "text":
			item.Text = part.Text
			content.WriteString(part.Text)
		case "image_url":
			if part.ImageURL != nil {
				item.URI = part.ImageURL.URL
			}
		case "input_audio":
			if part.InputAudio != nil {
				item.URI = part.InputAudio.Data
				item.MimeType = part.InputAudio.Format
			}
		}
		out = append(out, item)
	}
	return content.String(), out
}

func partsJSON(parts []store.Part) (json.RawMessage, error) {
	out := make([]map[string]any, 0, len(parts))
	for _, part := range parts {
		switch part.Type {
		case "text":
			out = append(out, map[string]any{"type": "text", "text": part.Text})
		case "image_url":
			out = append(out, map[string]any{"type": "image_url", "image_url": map[string]any{"url": part.URI}})
		case "input_audio":
			out = append(out, map[string]any{"type": "input_audio",
				"input_audio": map[string]any{"data": part.URI, "format": part.MimeType}})
		}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode message parts: %w", err)
	}
	return raw, nil
}

// sanitizePromptMessages keeps synthetic service reports and upstream command
// tags out of the model prompt. A client echoes previous assistant answers and
// may paste a broken answer from another session as a user message, so every
// role is cleaned, not only assistant (contract.md §5, §9.2.3).
func sanitizePromptMessages(messages []transport.Message) []transport.Message {
	out := make([]transport.Message, len(messages))
	copy(out, messages)
	for i := range out {
		out[i].Content = cleanMessageContent(out[i].Content)
		out[i].ReasoningContent = nil
		out[i].Reasoning = nil
		out[i].ReasoningText = nil
	}
	return out
}

func sanitizeHistoryMessages(messages []transport.Message) []transport.Message {
	out := make([]transport.Message, len(messages))
	copy(out, messages)
	for i := range out {
		out[i].Content = cleanMessageContent(out[i].Content)
		if out[i].ReasoningContent != nil {
			cleaned := SanitizeModelText(*out[i].ReasoningContent)
			out[i].ReasoningContent = &cleaned
		}
		if out[i].Reasoning != nil {
			cleaned := SanitizeModelText(*out[i].Reasoning)
			out[i].Reasoning = &cleaned
		}
		if out[i].ReasoningText != nil {
			cleaned := SanitizeModelText(*out[i].ReasoningText)
			out[i].ReasoningText = &cleaned
		}
	}
	return out
}

func cleanStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cleaned := SanitizeModelText(*value)
	return &cleaned
}

func promptHistoryMessages(messages []transport.Message, includeReasoning bool) []transport.Message {
	out := make([]transport.Message, 0, len(messages))
	for _, message := range messages {
		hasReasoning := message.ReasoningContent != nil || message.Reasoning != nil ||
			message.ReasoningText != nil
		if includeReasoning {
			out = append(out, message)
			continue
		}
		message.ReasoningContent = nil
		message.Reasoning = nil
		message.ReasoningText = nil
		contentEmpty := len(message.Content) == 0 || strings.TrimSpace(string(message.Content)) == "null"
		if hasReasoning && contentEmpty && len(message.ToolCalls) == 0 && message.Role == "assistant" {
			continue
		}
		out = append(out, message)
	}
	return out
}

// hasCommandTags reports whether any client message carries an upstream
// reasoning marker. Only the fact is logged: the tag and message bodies never
// reach the journal (AGENTS.md rule 7).
func hasCommandTags(messages []transport.Message) bool {
	for _, message := range messages {
		text, err := message.Text()
		if err == nil && HasCommandTag(text) {
			return true
		}
		for _, value := range []*string{message.ReasoningContent, message.Reasoning, message.ReasoningText} {
			if value != nil && HasCommandTag(*value) {
				return true
			}
		}
	}
	return false
}

func cleanMessageContent(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return raw
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		cleaned := SanitizeModelText(text)
		if cleaned == text {
			return raw
		}
		return transport.TextContent(cleaned)
	}
	content, parts := parseContent(raw)
	if content == "" || SanitizeModelText(content) == content {
		return raw
	}
	cleanParts := make([]store.Part, len(parts))
	copy(cleanParts, parts)
	for i := range cleanParts {
		if cleanParts[i].Type == "text" {
			cleanParts[i].Text = SanitizeModelText(cleanParts[i].Text)
		}
	}
	cleaned, err := partsJSON(cleanParts)
	if err != nil {
		return raw
	}
	return cleaned
}
