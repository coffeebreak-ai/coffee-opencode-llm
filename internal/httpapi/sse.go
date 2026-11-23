package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/engine"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/transport"
)

type streamSink struct {
	w             http.ResponseWriter
	rc            *http.ResponseController
	log           *slog.Logger
	turnID        string
	model         string
	created       int64
	includeUsage  bool
	emitReasoning bool
	writeTimeout  time.Duration
	dynamic       func()
	wrote         bool
	writeErr      error
	text          strings.Builder
	progress      ProgressOptions
	progressCount int64
	progressLast  time.Time
	cappedLogged  bool
	counts        engine.ProgressCounts
}

func newStreamSink(w http.ResponseWriter, log *slog.Logger, turnID, model string, created int64,
	includeUsage, emitReasoning bool, writeTimeout time.Duration, dynamic func(),
	progress ProgressOptions) *streamSink {
	return &streamSink{
		w:             w,
		rc:            http.NewResponseController(w),
		log:           log,
		turnID:        turnID,
		model:         model,
		created:       created,
		includeUsage:  includeUsage,
		emitReasoning: emitReasoning,
		writeTimeout:  writeTimeout,
		dynamic:       dynamic,
		progress:      progress,
	}
}

func (s *streamSink) sink() engine.Sink {
	return engine.Sink{
		Text:         s.onText,
		Reasoning:    s.onReasoning,
		ToolCall:     s.onToolCall,
		Finish:       s.finish,
		Ping:         s.ping,
		Participants: s.participants,
		UsageReport:  s.usageReport,
		Progress:     s.onProgress,
	}
}

func (s *streamSink) onText(text string) error {
	text = engine.SanitizeModelText(text)
	if text == "" {
		return nil
	}
	s.text.WriteString(text)
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: transport.Delta{Content: &text}}}
	return s.writeChunk(chunk)
}

func (s *streamSink) onReasoning(text string) error {
	if !s.emitReasoning {
		return nil
	}
	text = engine.SanitizeModelText(text)
	if text == "" {
		return nil
	}
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: transport.Delta{ReasoningContent: &text}}}
	return s.writeChunk(chunk)
}

func (s *streamSink) onToolCall(delta transport.ToolCallDelta) error {
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: transport.Delta{
		ToolCalls: []transport.ToolCallDelta{delta},
	}}}
	return s.writeChunk(chunk)
}

func (s *streamSink) participants(list []engine.Participant) error {
	raw, err := json.Marshal(list)
	if err != nil {
		return err
	}
	chunk := s.baseChunk()
	delta := transport.Delta{}
	if text := participantsText(list); text != "" {
		delta.Content = &text
	}
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: delta}}
	chunk.Extra = map[string]json.RawMessage{"x-coffee-participants": raw}
	return s.writeChunk(chunk)
}

// usageReport emits the transient per-role token report. It bypasses onText,
// so the block never reaches s.text, delivery or the append-only history
// (contract.md §9.8 rule 5).
func (s *streamSink) usageReport(rows []transport.UsageRow) error {
	raw, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	chunk := s.baseChunk()
	text := usageReportText(rows)
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: transport.Delta{Content: &text}}}
	chunk.Extra = map[string]json.RawMessage{"x-coffee-usage": raw}
	return s.writeChunk(chunk)
}

func (s *streamSink) onProgress(event engine.Progress) error {
	if event.Kind != "warning" && !s.progress.enabled(event.Category) {
		return nil
	}
	switch event.Kind {
	case "tool":
		event.Class = toolClass(s.progress.ToolClasses, event.Reason)
		s.countTool(event.Class)
	case "thought":
		s.counts.Thoughts++
	case "summary":
		event.Counts = &s.counts
	}
	exempt := event.Kind == "error" || event.Kind == "warning" || event.Kind == "stop" || event.Kind == "summary" ||
		event.Stage == "compaction"
	if !exempt {
		if s.progress.MinIntervalMS > 0 && !s.progressLast.IsZero() &&
			time.Since(s.progressLast) < time.Duration(s.progress.MinIntervalMS)*time.Millisecond {
			s.log.Debug("progress_skipped", "request_id", event.RequestID, "stage", event.Stage,
				"category", event.Category, "reason", "throttled")
			return nil
		}
		if s.progress.MaxEvents > 0 {
			remaining := s.progress.MaxEvents - s.progressCount
			if dropProgressAtCap(event, remaining) {
				if !s.cappedLogged {
					s.log.Debug("progress_capped", "request_id", event.RequestID, "stage", event.Stage)
					s.cappedLogged = true
				}
				return nil
			}
		}
	}
	line := progressText(s.progress, event)
	if line == "" {
		s.log.Warn("progress template is missing", "kind", event.Kind, "category", event.Category)
		return nil
	}
	line = truncateRunes(line, s.progress.TextChars)
	text := "\n```" + engine.ProgressBlockLanguage + "\n" + line + "\n```\n"
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: transport.Delta{Content: &text}}}
	raw, err := json.Marshal(event)
	if err != nil {
		s.log.Warn("progress event is not encoded", "request_id", event.RequestID,
			"stage", event.Stage, "error", err)
		return nil
	}
	chunk.Extra = map[string]json.RawMessage{"x-coffee-progress": raw}
	if err := s.writeChunk(chunk); err != nil {
		return err
	}
	if !exempt {
		s.progressCount++
		s.progressLast = time.Now()
	}
	return nil
}

func (p ProgressOptions) enabled(category string) bool {
	if !p.Enabled {
		return false
	}
	if p.Categories == nil {
		return true
	}
	return p.Categories[category]
}

func toolClass(classes map[string]string, name string) string {
	if class, ok := classes[name]; ok && class != "" {
		return class
	}
	return "tool"
}

func (s *streamSink) countTool(class string) {
	switch class {
	case "command":
		s.counts.Commands++
	case "edit":
		s.counts.Edits++
	case "read":
		s.counts.Reads++
	default:
		s.counts.Tools++
	}
}

// progressDropOrder is the low-to-high priority order used when the event budget
// runs low (progress-stream.md §5).
var progressDropOrder = []string{"tools", "model", "deflate", "delegation", "decision", "stage"}

func dropProgressAtCap(event engine.Progress, remaining int64) bool {
	if event.Kind == "error" || event.Kind == "warning" || event.Kind == "stop" {
		return false
	}
	if remaining <= 0 {
		return true
	}
	index := -1
	for i, category := range progressDropOrder {
		if category == event.Category {
			index = i
			break
		}
	}
	if index < 0 {
		return false
	}
	return int64(index) < int64(len(progressDropOrder))-remaining
}

func progressText(opts ProgressOptions, event engine.Progress) string {
	template, ok := opts.Texts[event.Kind]
	if !ok || strings.TrimSpace(template) == "" {
		return ""
	}
	requestID := ""
	if opts.IncludeRequestID {
		requestID = event.RequestID
	}
	counts := engine.ProgressCounts{}
	if event.Counts != nil {
		counts = *event.Counts
	}
	replacer := strings.NewReplacer(
		"{stage}", event.Stage,
		"{kind}", event.Kind,
		"{category}", event.Category,
		"{role}", event.Role,
		"{model_id}", event.ModelID,
		"{level}", strconv.Itoa(event.Level),
		"{reason}", event.Reason,
		"{class}", event.Class,
		"{commands}", strconv.FormatInt(counts.Commands, 10),
		"{edits}", strconv.FormatInt(counts.Edits, 10),
		"{reads}", strconv.FormatInt(counts.Reads, 10),
		"{thoughts}", strconv.FormatInt(counts.Thoughts, 10),
		"{tools}", strconv.FormatInt(counts.Tools, 10),
		"{request_id}", requestID,
	)
	return strings.TrimSpace(replacer.Replace(template))
}

func truncateRunes(text string, limit int64) string {
	if limit <= 0 {
		return text
	}
	runes := []rune(text)
	if int64(len(runes)) <= limit {
		return text
	}
	return string(runes[:limit])
}

func (s *streamSink) finish(reason string) error {
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{{Index: 0, FinishReason: &reason}}
	return s.writeChunk(chunk)
}

func (s *streamSink) ping() error {
	return s.writeRaw([]byte(": ping\n\n"))
}

func (s *streamSink) writeUsage(usage *transport.Usage) error {
	if !s.includeUsage || usage == nil {
		return nil
	}
	chunk := s.baseChunk()
	chunk.Choices = []transport.ChunkChoice{}
	chunk.Usage = usage
	return s.writeChunk(chunk)
}

func (s *streamSink) writeError(err error) {
	fail := engineFailure(err)
	body, marshalErr := json.Marshal(errorEnvelope{Error: errorBody{
		Message: fail.message,
		Type:    errorType(fail.status),
		Code:    fail.code,
	}})
	if marshalErr != nil {
		return
	}
	_ = s.writeRaw(append(append([]byte("data: "), body...), '\n', '\n'))
}

func (s *streamSink) done() error {
	return s.writeRaw([]byte("data: [DONE]\n\n"))
}

func (s *streamSink) emitted(fallback string) string {
	if s.text.Len() > 0 {
		return engine.SanitizeModelText(s.text.String())
	}
	return engine.SanitizeModelText(fallback)
}

func (s *streamSink) replay(response transport.ChatResponse) error {
	chunk := s.baseChunk()
	chunk.ID = response.ID
	chunk.Created = response.Created
	if len(response.Choices) == 0 {
		return s.writeChunk(chunk)
	}
	choice := response.Choices[0]
	if reasoning := choice.Message.ReasoningContent; reasoning != nil && *reasoning != "" {
		if err := s.onReasoning(*reasoning); err != nil {
			return err
		}
	}
	delta := transport.Delta{Role: "assistant"}
	if len(choice.Message.ToolCalls) > 0 {
		for index, call := range choice.Message.ToolCalls {
			function := call.Function
			delta.ToolCalls = append(delta.ToolCalls, transport.ToolCallDelta{
				Index:    index,
				ID:       call.ID,
				Type:     call.Type,
				Function: &function,
			})
		}
	} else {
		text, err := choice.Message.Text()
		if err != nil {
			return err
		}
		text = engine.SanitizeModelText(text)
		delta.Content = &text
	}
	finish := choice.FinishReason
	if finish == "" {
		finish = "stop"
	}
	chunk.Choices = []transport.ChunkChoice{{Index: 0, Delta: delta, FinishReason: &finish}}
	if s.includeUsage && response.Usage != nil {
		chunk.Usage = response.Usage
	}
	return s.writeChunk(chunk)
}

func (s *streamSink) baseChunk() transport.Chunk {
	return transport.Chunk{
		ID:      s.turnID,
		Object:  "chat.completion.chunk",
		Created: s.created,
		Model:   s.model,
	}
}

func (s *streamSink) writeChunk(chunk transport.Chunk) error {
	raw, err := json.Marshal(chunk)
	if err != nil {
		s.writeErr = err
		return err
	}
	buf := make([]byte, 0, len(raw)+8)
	buf = append(buf, "data: "...)
	buf = append(buf, raw...)
	buf = append(buf, '\n', '\n')
	return s.writeRaw(buf)
}

func (s *streamSink) writeRaw(data []byte) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	if !s.wrote {
		header := s.w.Header()
		header.Set("Content-Type", "text/event-stream; charset=utf-8")
		header.Set("Cache-Control", "no-cache")
		header.Set("Connection", "keep-alive")
		header.Set("X-Accel-Buffering", "no")
		if s.dynamic != nil {
			s.dynamic()
		}
		s.w.WriteHeader(http.StatusOK)
		s.wrote = true
	}
	if s.writeTimeout > 0 {
		_ = s.rc.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	}
	if _, err := s.w.Write(data); err != nil {
		s.writeErr = err
		return err
	}
	if flusher, ok := s.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func usageReportText(rows []transport.UsageRow) string {
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		parts := []string{row.Role + ":"}
		if row.InputTokens != nil {
			parts = append(parts, "input "+strconv.FormatInt(*row.InputTokens, 10))
		}
		if row.OutputTokens != nil {
			parts = append(parts, "output "+strconv.FormatInt(*row.OutputTokens, 10))
		}
		if row.CachedTokens != nil {
			parts = append(parts, "cached "+strconv.FormatInt(*row.CachedTokens, 10))
		}
		if row.ReasoningTokens != nil {
			parts = append(parts, "reasoning "+strconv.FormatInt(*row.ReasoningTokens, 10))
		}
		if len(parts) == 1 {
			parts = append(parts, "no usage reported")
		}
		lines = append(lines, strings.Join(parts, " "))
	}
	return "\n```" + engine.UsageBlockLanguage + "\n" + strings.Join(lines, "\n") + "\n```\n"
}

func participantsText(list []engine.Participant) string {
	var experts, assistants, deflators []string
	for _, item := range list {
		switch item.Role {
		case "expert":
			experts = append(experts, item.ID)
		case "deflator":
			deflators = append(deflators, item.ID)
		default:
			assistants = append(assistants, item.ID)
		}
	}
	var parts []string
	if len(experts) > 0 {
		parts = append(parts, "expert: "+strings.Join(experts, ", "))
	}
	if len(assistants) > 0 {
		parts = append(parts, "assistant: "+strings.Join(assistants, ", "))
	}
	if len(deflators) > 0 {
		parts = append(parts, "deflator: "+strings.Join(deflators, ", "))
	}
	if len(parts) == 0 {
		return ""
	}
	return "\n```" + engine.ParticipantsBlockLanguage + "\n" + strings.Join(parts, "; ") + "\n```\n"
}
