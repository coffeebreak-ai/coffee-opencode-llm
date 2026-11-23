package engine

import "github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"

type promptDiagnostics struct {
	ClientMessages                      int `json:"client_messages"`
	ClientToolCalls                     int `json:"client_tool_calls"`
	ClientParallelBatches               int `json:"client_parallel_batches"`
	ClientToolCallsWithReasoning        int `json:"client_tool_calls_with_reasoning"`
	ClientReasoningBytes                int `json:"client_reasoning_bytes"`
	PromptMessages                      int `json:"prompt_messages"`
	PromptToolCalls                     int `json:"prompt_tool_calls"`
	PromptParallelBatches               int `json:"prompt_parallel_batches"`
	PromptReasoningMessages             int `json:"prompt_reasoning_messages"`
	PromptContentBytes                  int `json:"prompt_content_bytes"`
	PromptReasoningBytes                int `json:"prompt_reasoning_bytes"`
	PromptToolArgumentBytes             int `json:"prompt_tool_argument_bytes"`
	MatchedToolCalls                    int `json:"matched_tool_calls"`
	MatchedToolCallsMissingReasoning    int `json:"matched_tool_calls_missing_reasoning"`
	UnmatchedOrAmbiguousToolCalls       int `json:"unmatched_or_ambiguous_tool_calls"`
	ParallelBatchesSplit                int `json:"parallel_batches_split"`
	ParallelBatchesUnmatchedOrAmbiguous int `json:"parallel_batches_unmatched_or_ambiguous"`
}

type promptToolLocation struct {
	messageIndex int
	hasReasoning bool
	ambiguous    bool
}

func hasReasoning(message transport.Message) bool {
	return message.ReasoningContent != nil && *message.ReasoningContent != "" ||
		message.Reasoning != nil && *message.Reasoning != "" ||
		message.ReasoningText != nil && *message.ReasoningText != ""
}

func reasoningBytes(message transport.Message) int {
	size := 0
	for _, value := range []*string{message.ReasoningContent, message.Reasoning, message.ReasoningText} {
		if value != nil {
			size += len(*value)
		}
	}
	return size
}

func diagnosePrompt(client, prompt []transport.Message) promptDiagnostics {
	stats := promptDiagnostics{ClientMessages: len(client), PromptMessages: len(prompt)}
	locations := make(map[string]promptToolLocation)
	for i, message := range prompt {
		stats.PromptContentBytes += len(message.Content)
		stats.PromptReasoningBytes += reasoningBytes(message)
		if hasReasoning(message) {
			stats.PromptReasoningMessages++
		}
		if message.Role != "assistant" {
			continue
		}
		stats.PromptToolCalls += len(message.ToolCalls)
		if len(message.ToolCalls) > 1 {
			stats.PromptParallelBatches++
		}
		for _, call := range message.ToolCalls {
			stats.PromptToolArgumentBytes += len(call.Function.Arguments)
			if call.ID == "" {
				continue
			}
			location, exists := locations[call.ID]
			if exists {
				location.ambiguous = true
			} else {
				location = promptToolLocation{messageIndex: i, hasReasoning: hasReasoning(message)}
			}
			locations[call.ID] = location
		}
	}
	for _, message := range client {
		stats.ClientReasoningBytes += reasoningBytes(message)
		if message.Role != "assistant" {
			continue
		}
		stats.ClientToolCalls += len(message.ToolCalls)
		parallel := len(message.ToolCalls) > 1
		if parallel {
			stats.ClientParallelBatches++
		}
		reasoning := hasReasoning(message)
		var firstIndex int
		found := false
		split := false
		unmatched := false
		ids := make(map[string]bool)
		for _, call := range message.ToolCalls {
			if reasoning {
				stats.ClientToolCallsWithReasoning++
			}
			location, ok := locations[call.ID]
			if call.ID == "" || !ok || location.ambiguous || ids[call.ID] {
				stats.UnmatchedOrAmbiguousToolCalls++
				unmatched = true
				continue
			}
			ids[call.ID] = true
			stats.MatchedToolCalls++
			if reasoning && !location.hasReasoning {
				stats.MatchedToolCallsMissingReasoning++
			}
			if !found {
				firstIndex, found = location.messageIndex, true
			} else if firstIndex != location.messageIndex {
				split = true
			}
		}
		if parallel && unmatched {
			stats.ParallelBatchesUnmatchedOrAmbiguous++
		} else if parallel && split {
			stats.ParallelBatchesSplit++
		}
	}
	return stats
}
