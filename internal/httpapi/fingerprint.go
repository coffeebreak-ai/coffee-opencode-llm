package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// maxSessionKeyChars is a contract constant (contract.md §4), not a tunable limit.
const maxSessionKeyChars = 200

type contentInfo struct {
	normalized any
	text       string
	chars      int64
	mediaBytes int64
}

type rawPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	ImageURL *struct {
		URL    string `json:"url"`
		Detail string `json:"detail"`
	} `json:"image_url"`
	InputAudio *struct {
		Data   string `json:"data"`
		Format string `json:"format"`
	} `json:"input_audio"`
}

func analyzeContent(raw json.RawMessage) (contentInfo, *failure) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return contentInfo{}, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return contentInfo{normalized: text, text: text, chars: int64(len([]rune(text)))}, nil
	}
	var parts []rawPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return contentInfo{}, newFailure(http.StatusBadRequest, "invalid_request")
	}
	info := contentInfo{}
	normalized := make([]map[string]any, 0, len(parts))
	var textBuilder strings.Builder
	for _, part := range parts {
		switch part.Type {
		case "text":
			normalized = append(normalized, map[string]any{"type": "text", "text": part.Text})
			info.chars += int64(len([]rune(part.Text)))
			textBuilder.WriteString(part.Text)
		case "image_url":
			if part.ImageURL == nil || part.ImageURL.URL == "" {
				return contentInfo{}, newFailure(http.StatusBadRequest, "invalid_request")
			}
			value := map[string]any{"url": part.ImageURL.URL}
			if part.ImageURL.Detail != "" {
				value["detail"] = part.ImageURL.Detail
			}
			normalized = append(normalized, map[string]any{"type": "image_url", "image_url": value})
			info.mediaBytes += estimateMediaBytes(part.ImageURL.URL)
		case "input_audio":
			if part.InputAudio == nil || part.InputAudio.Data == "" || part.InputAudio.Format == "" {
				return contentInfo{}, newFailure(http.StatusBadRequest, "invalid_request")
			}
			normalized = append(normalized, map[string]any{
				"type": "input_audio",
				"input_audio": map[string]any{
					"data":   part.InputAudio.Data,
					"format": part.InputAudio.Format,
				},
			})
			info.mediaBytes += estimateMediaBytes(part.InputAudio.Data)
		default:
			return contentInfo{}, newFailure(http.StatusUnprocessableEntity, "unsupported_content")
		}
	}
	info.normalized = normalized
	info.text = textBuilder.String()
	return info, nil
}

func estimateMediaBytes(value string) int64 {
	if strings.HasPrefix(value, "data:") {
		if index := strings.Index(value, ","); index >= 0 {
			return int64(len(value[index+1:])) * 3 / 4
		}
	}
	return int64(len(value))
}

type normToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function normFunction `json:"function"`
}

type normFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type normMessage struct {
	Role       string         `json:"role"`
	Content    any            `json:"content,omitempty"`
	ToolCalls  []normToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type normTool struct {
	Type     string     `json:"type"`
	Function normToolFn `json:"function"`
}

type normToolFn struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func normalizeMessage(message chatMessage) (normMessage, *failure) {
	info, fail := analyzeContent(message.Content)
	if fail != nil {
		return normMessage{}, fail
	}
	out := normMessage{Role: message.Role, Content: info.normalized, ToolCallID: message.ToolCallID}
	for _, call := range message.ToolCalls {
		kind := call.Type
		if kind == "" {
			kind = "function"
		}
		out.ToolCalls = append(out.ToolCalls, normToolCall{
			ID:       call.ID,
			Type:     kind,
			Function: normFunction{Name: call.Function.Name, Arguments: call.Function.Arguments},
		})
	}
	return out, nil
}

func normalizeTools(tools []chatTool) []normTool {
	out := make([]normTool, 0, len(tools))
	for _, tool := range tools {
		kind := tool.Type
		if kind == "" {
			kind = "function"
		}
		out = append(out, normTool{
			Type: kind,
			Function: normToolFn{
				Name:        tool.Function.Name,
				Description: tool.Function.Description,
				Parameters:  compactRaw(tool.Function.Parameters),
			},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Function.Name < out[j].Function.Name })
	return out
}

func compactRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}
	return json.RawMessage(buf.Bytes())
}

func hashJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func fingerprintPrefix(messages []chatMessage) (string, *failure) {
	prefix := make([]normMessage, 0, len(messages))
	for _, message := range messages {
		normalized, fail := normalizeMessage(message)
		if fail != nil {
			return "", fail
		}
		prefix = append(prefix, normalized)
		if message.Role == "user" {
			break
		}
	}
	hash, err := hashJSON(prefix)
	if err != nil {
		return "", newFailure(http.StatusInternalServerError, "internal")
	}
	return hash, nil
}

// idempotencyFingerprint is the delivery key of a request. It covers the
// normalized input and tools only: routing decisions (level, owner, pinned
// model) are not part of the request identity, so a retry is recognized even
// when the dialogue moved from the assistant to the expert.
func idempotencyFingerprint(req *chatRequest) (string, *failure) {
	messages := make([]normMessage, 0, len(req.Messages))
	for _, message := range req.Messages {
		normalized, fail := normalizeMessage(message)
		if fail != nil {
			return "", fail
		}
		messages = append(messages, normalized)
	}
	value := struct {
		Messages []normMessage `json:"messages"`
		Tools    []normTool    `json:"tools"`
	}{
		Messages: messages,
		Tools:    normalizeTools(req.Tools),
	}
	hash, err := hashJSON(value)
	if err != nil {
		return "", newFailure(http.StatusInternalServerError, "internal")
	}
	return hash, nil
}

func (s *Server) sessionKey(r *http.Request, req *chatRequest, prefix string) (string, string, *failure) {
	candidates := []struct {
		value  string
		source string
	}{
		{r.Header.Get("X-Coffee-Llm-Session"), "header"},
		{r.Header.Get("x-opencode-session"), "header"},
		{req.Metadata.SessionID, "metadata"},
		{req.User, "user"},
	}
	for _, candidate := range candidates {
		if candidate.value == "" {
			continue
		}
		if len([]rune(candidate.value)) > maxSessionKeyChars {
			return "", "", newFailure(http.StatusBadRequest, "invalid_session_key")
		}
		return candidate.value, candidate.source, nil
	}
	return prefix, "fingerprint", nil
}
