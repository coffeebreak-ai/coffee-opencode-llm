package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type ToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      *bool           `json:"strict,omitempty"`
}

type Tool struct {
	Type         string          `json:"type"`
	Function     ToolFunction    `json:"function"`
	CacheControl json.RawMessage `json:"cache_control,omitempty"`
}

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

type ToolCallDelta struct {
	Index    int           `json:"index"`
	ID       string        `json:"id,omitempty"`
	Type     string        `json:"type,omitempty"`
	Function *FunctionCall `json:"function,omitempty"`
}

type Message struct {
	Role             string          `json:"role"`
	Content          json.RawMessage `json:"content,omitempty"`
	ReasoningContent *string         `json:"reasoning_content,omitempty"`
	Reasoning        *string         `json:"reasoning,omitempty"`
	ReasoningText    *string         `json:"reasoning_text,omitempty"`
	ToolCalls        []ToolCall      `json:"tool_calls,omitempty"`
	ToolCallID       string          `json:"tool_call_id,omitempty"`
}

func TextMessage(role, text string) Message {
	return Message{Role: role, Content: TextContent(text)}
}

func TextContent(text string) json.RawMessage {
	raw, err := json.Marshal(text)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return raw
}

func (m Message) Text() (string, error) {
	if len(m.Content) == 0 || bytes.Equal(m.Content, []byte("null")) {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(m.Content, &text); err == nil {
		return text, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(m.Content, &parts); err != nil {
		return "", fmt.Errorf("decode message content: %w", err)
	}
	var out bytes.Buffer
	for _, part := range parts {
		if part.Type == "text" {
			out.WriteString(part.Text)
		}
	}
	return out.String(), nil
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type ChatRequest struct {
	Model               string          `json:"model"`
	Messages            []Message       `json:"messages"`
	Tools               []Tool          `json:"tools,omitempty"`
	ToolChoice          json.RawMessage `json:"tool_choice,omitempty"`
	Stream              bool            `json:"stream,omitempty"`
	StreamOptions       *StreamOptions  `json:"stream_options,omitempty"`
	MaxTokens           *int64          `json:"max_tokens,omitempty"`
	MaxCompletionTokens *int64          `json:"max_completion_tokens,omitempty"`
	ReasoningEffort     string          `json:"reasoning_effort,omitempty"`
	Temperature         *float64        `json:"temperature,omitempty"`
	TopP                *float64        `json:"top_p,omitempty"`
	FrequencyPenalty    *float64        `json:"frequency_penalty,omitempty"`
	PresencePenalty     *float64        `json:"presence_penalty,omitempty"`
	Seed                *int64          `json:"seed,omitempty"`
	Stop                json.RawMessage `json:"stop,omitempty"`
	N                   *int64          `json:"n,omitempty"`
	ResponseFormat      json.RawMessage `json:"response_format,omitempty"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls,omitempty"`
	// Session is sent as the configured session header and never as a body field.
	Session string `json:"-"`
}

type UsageDetails struct {
	CachedTokens *int64 `json:"cached_tokens,omitempty"`
}

type CompletionTokensDetails struct {
	ReasoningTokens *int64 `json:"reasoning_tokens,omitempty"`
}

type Usage struct {
	PromptTokens            *int64                   `json:"prompt_tokens,omitempty"`
	CompletionTokens        *int64                   `json:"completion_tokens,omitempty"`
	TotalTokens             *int64                   `json:"total_tokens,omitempty"`
	ReasoningTokens         *int64                   `json:"reasoning_tokens,omitempty"`
	PromptTokensDetails     *UsageDetails            `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *CompletionTokensDetails `json:"completion_tokens_details,omitempty"`
}

// Reasoning returns the reasoning token count in whichever field the provider
// uses. Reasoning tokens are part of completion_tokens, not an extra charge.
func (u Usage) Reasoning() *int64 {
	if u.ReasoningTokens != nil {
		return u.ReasoningTokens
	}
	if u.CompletionTokensDetails != nil {
		return u.CompletionTokensDetails.ReasoningTokens
	}
	return nil
}

func (u *Usage) Merge(other *Usage) {
	if other == nil {
		return
	}
	u.PromptTokens = addPtr(u.PromptTokens, other.PromptTokens)
	u.CompletionTokens = addPtr(u.CompletionTokens, other.CompletionTokens)
	u.TotalTokens = addPtr(u.TotalTokens, other.TotalTokens)
	u.ReasoningTokens = addPtr(u.Reasoning(), other.Reasoning())
	if other.PromptTokensDetails != nil && other.PromptTokensDetails.CachedTokens != nil {
		if u.PromptTokensDetails == nil {
			u.PromptTokensDetails = &UsageDetails{}
		}
		u.PromptTokensDetails.CachedTokens = addPtr(u.PromptTokensDetails.CachedTokens, other.PromptTokensDetails.CachedTokens)
	}
}

func addPtr(a, b *int64) *int64 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	sum := *a + *b
	return &sum
}

// UsageRow is the per-role token summary of one turn. A field the provider did
// not report is omitted, never replaced with zero (contract.md §5).
type UsageRow struct {
	Role            string `json:"role"`
	InputTokens     *int64 `json:"input_tokens,omitempty"`
	OutputTokens    *int64 `json:"output_tokens,omitempty"`
	CachedTokens    *int64 `json:"cached_tokens,omitempty"`
	ReasoningTokens *int64 `json:"reasoning_tokens,omitempty"`
}

type ChatChoice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type ChatResponse struct {
	ID          string       `json:"id"`
	Object      string       `json:"object"`
	Created     int64        `json:"created"`
	Model       string       `json:"model"`
	Choices     []ChatChoice `json:"choices"`
	Usage       *Usage       `json:"usage,omitempty"`
	UsageReport []UsageRow   `json:"x-coffee-usage,omitempty"`
}

type Delta struct {
	Role               string                     `json:"role,omitempty"`
	Content            *string                    `json:"content,omitempty"`
	ReasoningContent   *string                    `json:"reasoning_content,omitempty"`
	Reasoning          *string                    `json:"reasoning,omitempty"`
	ReasoningTextDelta *string                    `json:"reasoning_text,omitempty"`
	ToolCalls          []ToolCallDelta            `json:"tool_calls,omitempty"`
	Extra              map[string]json.RawMessage `json:"-"`
}

// ReasoningText returns the reasoning delta in whichever field the upstream uses.
func (d Delta) ReasoningText() string {
	if d.ReasoningContent != nil && *d.ReasoningContent != "" {
		return *d.ReasoningContent
	}
	if d.Reasoning != nil && *d.Reasoning != "" {
		return *d.Reasoning
	}
	if d.ReasoningTextDelta != nil && *d.ReasoningTextDelta != "" {
		return *d.ReasoningTextDelta
	}
	return ""
}

func (d *Delta) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("decode delta: %w", err)
	}
	if value, ok := fields["role"]; ok {
		if err := json.Unmarshal(value, &d.Role); err != nil {
			return fmt.Errorf("decode delta role: %w", err)
		}
		delete(fields, "role")
	}
	if value, ok := fields["content"]; ok {
		if !bytes.Equal(value, []byte("null")) {
			var text string
			if err := json.Unmarshal(value, &text); err != nil {
				return fmt.Errorf("decode delta content: %w", err)
			}
			d.Content = &text
		}
		delete(fields, "content")
	}
	if value, ok := fields["reasoning_content"]; ok {
		if !bytes.Equal(value, []byte("null")) {
			var text string
			if err := json.Unmarshal(value, &text); err != nil {
				return fmt.Errorf("decode delta reasoning_content: %w", err)
			}
			d.ReasoningContent = &text
		}
		delete(fields, "reasoning_content")
	}
	if value, ok := fields["reasoning"]; ok {
		if !bytes.Equal(value, []byte("null")) {
			var text string
			if err := json.Unmarshal(value, &text); err != nil {
				return fmt.Errorf("decode delta reasoning: %w", err)
			}
			d.Reasoning = &text
		}
		delete(fields, "reasoning")
	}
	if value, ok := fields["reasoning_text"]; ok {
		if !bytes.Equal(value, []byte("null")) {
			var text string
			if err := json.Unmarshal(value, &text); err != nil {
				return fmt.Errorf("decode delta reasoning_text: %w", err)
			}
			d.ReasoningTextDelta = &text
		}
		delete(fields, "reasoning_text")
	}
	if value, ok := fields["tool_calls"]; ok {
		if err := json.Unmarshal(value, &d.ToolCalls); err != nil {
			return fmt.Errorf("decode delta tool_calls: %w", err)
		}
		delete(fields, "tool_calls")
	}
	if len(fields) > 0 {
		d.Extra = fields
	}
	return nil
}

func (d Delta) MarshalJSON() ([]byte, error) {
	fields := make(map[string]json.RawMessage, len(d.Extra)+3)
	for key, value := range d.Extra {
		fields[key] = value
	}
	if d.Role != "" {
		raw, err := json.Marshal(d.Role)
		if err != nil {
			return nil, fmt.Errorf("encode delta role: %w", err)
		}
		fields["role"] = raw
	}
	if d.Content != nil {
		raw, err := json.Marshal(*d.Content)
		if err != nil {
			return nil, fmt.Errorf("encode delta content: %w", err)
		}
		fields["content"] = raw
	}
	if d.ReasoningContent != nil {
		raw, err := json.Marshal(*d.ReasoningContent)
		if err != nil {
			return nil, fmt.Errorf("encode delta reasoning_content: %w", err)
		}
		fields["reasoning_content"] = raw
	}
	if d.Reasoning != nil {
		raw, err := json.Marshal(*d.Reasoning)
		if err != nil {
			return nil, fmt.Errorf("encode delta reasoning: %w", err)
		}
		fields["reasoning"] = raw
	}
	if d.ReasoningTextDelta != nil {
		raw, err := json.Marshal(*d.ReasoningTextDelta)
		if err != nil {
			return nil, fmt.Errorf("encode delta reasoning_text: %w", err)
		}
		fields["reasoning_text"] = raw
	}
	if len(d.ToolCalls) > 0 {
		raw, err := json.Marshal(d.ToolCalls)
		if err != nil {
			return nil, fmt.Errorf("encode delta tool_calls: %w", err)
		}
		fields["tool_calls"] = raw
	}
	return json.Marshal(fields)
}

type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type Chunk struct {
	ID      string                     `json:"id"`
	Object  string                     `json:"object"`
	Created int64                      `json:"created"`
	Model   string                     `json:"model"`
	Choices []ChunkChoice              `json:"choices"`
	Usage   *Usage                     `json:"usage,omitempty"`
	Extra   map[string]json.RawMessage `json:"-"`
}

func (c *Chunk) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("decode chunk: %w", err)
	}
	if value, ok := fields["id"]; ok {
		if err := json.Unmarshal(value, &c.ID); err != nil {
			return fmt.Errorf("decode chunk id: %w", err)
		}
		delete(fields, "id")
	}
	if value, ok := fields["object"]; ok {
		if err := json.Unmarshal(value, &c.Object); err != nil {
			return fmt.Errorf("decode chunk object: %w", err)
		}
		delete(fields, "object")
	}
	if value, ok := fields["created"]; ok {
		if err := json.Unmarshal(value, &c.Created); err != nil {
			return fmt.Errorf("decode chunk created: %w", err)
		}
		delete(fields, "created")
	}
	if value, ok := fields["model"]; ok {
		if err := json.Unmarshal(value, &c.Model); err != nil {
			return fmt.Errorf("decode chunk model: %w", err)
		}
		delete(fields, "model")
	}
	if value, ok := fields["choices"]; ok {
		if err := json.Unmarshal(value, &c.Choices); err != nil {
			return fmt.Errorf("decode chunk choices: %w", err)
		}
		delete(fields, "choices")
	}
	if value, ok := fields["usage"]; ok {
		if !bytes.Equal(value, []byte("null")) {
			var usage Usage
			if err := json.Unmarshal(value, &usage); err != nil {
				return fmt.Errorf("decode chunk usage: %w", err)
			}
			c.Usage = &usage
		}
		delete(fields, "usage")
	}
	if len(fields) > 0 {
		c.Extra = fields
	}
	return nil
}

func (c Chunk) MarshalJSON() ([]byte, error) {
	fields := make(map[string]json.RawMessage, len(c.Extra)+6)
	for key, value := range c.Extra {
		fields[key] = value
	}
	set := func(key string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode chunk %s: %w", key, err)
		}
		fields[key] = raw
		return nil
	}
	if err := set("id", c.ID); err != nil {
		return nil, err
	}
	if err := set("object", c.Object); err != nil {
		return nil, err
	}
	if err := set("created", c.Created); err != nil {
		return nil, err
	}
	if err := set("model", c.Model); err != nil {
		return nil, err
	}
	if err := set("choices", c.Choices); err != nil {
		return nil, err
	}
	if c.Usage != nil {
		if err := set("usage", c.Usage); err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}
