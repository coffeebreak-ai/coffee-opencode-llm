package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrTimeout        = errors.New("upstream timeout")
	ErrBrokenStream   = errors.New("upstream stream ended without done marker")
	ErrChunkTooLarge  = errors.New("upstream stream chunk is too large")
	ErrResponseTooBig = errors.New("upstream response is too large")
)

type Error struct {
	Code         string
	Status       int
	RetryAfterMS int64
	Err          error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: status %d", e.Code, e.Status)
	}
	return fmt.Sprintf("%s: status %d: %v", e.Code, e.Status, e.Err)
}

func (e *Error) Unwrap() error {
	return e.Err
}

type Options struct {
	BaseURL           string
	Key               string
	SessionHeader     string
	ConnectTimeout    time.Duration
	UpstreamTimeout   time.Duration
	MaxResponseBytes  int64
	MaxChunkBytes     int64
	FirstChunkTimeout time.Duration
	IdleTimeout       time.Duration
	RetryAttempts     int
	RetryBackoff      time.Duration
}

type Transport struct {
	http              *http.Client
	baseURL           string
	key               string
	sessionHeader     string
	upstreamTimeout   time.Duration
	maxResponseBytes  int64
	maxChunkBytes     int64
	firstChunkTimeout time.Duration
	idleTimeout       time.Duration
	retryAttempts     int
	retryBackoff      time.Duration
}

// userAgent identifies the gateway to the upstream, as the upstream contract
// asks clients to do instead of presenting a generic HTTP-library name.
const userAgent = "coffee-opencode-llm"

func New(opts Options) *Transport {
	dialer := &net.Dialer{Timeout: opts.ConnectTimeout}
	return &Transport{
		http: &http.Client{
			Transport: &http.Transport{
				DialContext:         dialer.DialContext,
				MaxIdleConnsPerHost: 1,
			},
		},
		baseURL:           strings.TrimRight(opts.BaseURL, "/"),
		key:               opts.Key,
		sessionHeader:     opts.SessionHeader,
		upstreamTimeout:   opts.UpstreamTimeout,
		maxResponseBytes:  opts.MaxResponseBytes,
		maxChunkBytes:     opts.MaxChunkBytes,
		firstChunkTimeout: opts.FirstChunkTimeout,
		idleTimeout:       opts.IdleTimeout,
		retryAttempts:     opts.RetryAttempts,
		retryBackoff:      opts.RetryBackoff,
	}
}

// headers applies the credentials and the session identity the upstream uses
// for routing and prompt-cache affinity. The session value is the dialogue ID,
// so it stays stable for every request of one conversation.
func (t *Transport) headers(req *http.Request, accept, session string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", userAgent)
	if t.key != "" {
		req.Header.Set("Authorization", "Bearer "+t.key)
	}
	if t.sessionHeader != "" && session != "" {
		req.Header.Set(t.sessionHeader, session)
	}
}

type StreamResult struct {
	Content          string
	ReasoningContent string
	ToolCalls        []ToolCall
	FinishReason     string
	Usage            *Usage
}

func (t *Transport) Do(ctx context.Context, req ChatRequest, retryable bool) (*ChatResponse, error) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 && t.retryBackoff > 0 {
			if err := wait(ctx, time.Duration(attempt)*t.retryBackoff); err != nil {
				return nil, err
			}
		}
		resp, err := t.once(ctx, req)
		if err == nil {
			return resp, nil
		}
		if !retryable || attempt >= t.retryAttempts || !IsRetryable(err) {
			return nil, err
		}
	}
}

func (t *Transport) once(ctx context.Context, req ChatRequest) (*ChatResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, t.upstreamTimeout)
	defer cancel()
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode chat request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, t.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build chat request: %w", err)
	}
	t.headers(httpReq, "application/json", req.Session)
	resp, err := t.http.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, &Error{Code: "canceled", Err: err}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &Error{Code: "upstream_timeout", Err: ErrTimeout}
		}
		return nil, &Error{Code: "upstream_failed", Err: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, statusError(resp)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, t.maxResponseBytes+1))
	if err != nil {
		return nil, &Error{Code: "upstream_failed", Err: fmt.Errorf("read chat response: %w", err)}
	}
	if int64(len(raw)) > t.maxResponseBytes {
		return nil, &Error{Code: "upstream_failed", Err: ErrResponseTooBig}
	}
	var out ChatResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, &Error{Code: "upstream_failed", Err: fmt.Errorf("decode chat response: %w", err)}
	}
	return &out, nil
}

// ListModels reads the upstream model ids from GET /models. The startup check
// uses it to find catalog entries the upstream no longer serves.
func (t *Transport) ListModels(ctx context.Context) ([]string, error) {
	callCtx, cancel := context.WithTimeout(ctx, t.upstreamTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, t.baseURL+"/models", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build models request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if t.key != "" {
		req.Header.Set("Authorization", "Bearer "+t.key)
	}
	resp, err := t.http.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &Error{Code: "upstream_timeout", Err: ErrTimeout}
		}
		return nil, &Error{Code: "upstream_failed", Err: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, statusError(resp)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, t.maxResponseBytes+1))
	if err != nil {
		return nil, &Error{Code: "upstream_failed", Err: fmt.Errorf("read models response: %w", err)}
	}
	if int64(len(raw)) > t.maxResponseBytes {
		return nil, &Error{Code: "upstream_failed", Err: ErrResponseTooBig}
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, &Error{Code: "upstream_failed", Err: fmt.Errorf("decode models response: %w", err)}
	}
	ids := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		if item.ID != "" {
			ids = append(ids, item.ID)
		}
	}
	return ids, nil
}

func (t *Transport) Stream(ctx context.Context, req ChatRequest, onChunk func(Chunk) error) (StreamResult, error) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 && t.retryBackoff > 0 {
			if err := wait(ctx, time.Duration(attempt)*t.retryBackoff); err != nil {
				return StreamResult{}, err
			}
		}
		result, emitted, err := t.streamOnce(ctx, req, onChunk)
		if err == nil {
			return result, nil
		}
		if emitted || attempt >= t.retryAttempts || !IsRetryable(err) {
			return result, err
		}
	}
}

func (t *Transport) streamOnce(ctx context.Context, req ChatRequest, onChunk func(Chunk) error) (StreamResult, bool, error) {
	req.Stream = true
	req.StreamOptions = &StreamOptions{IncludeUsage: true}
	body, err := json.Marshal(req)
	if err != nil {
		return StreamResult{}, false, fmt.Errorf("encode stream request: %w", err)
	}
	callCtx, cancel := context.WithTimeout(ctx, t.upstreamTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, t.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return StreamResult{}, false, fmt.Errorf("build stream request: %w", err)
	}
	t.headers(httpReq, "text/event-stream", req.Session)
	resp, err := t.http.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return StreamResult{}, false, &Error{Code: "canceled", Err: err}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return StreamResult{}, false, &Error{Code: "upstream_timeout", Err: ErrTimeout}
		}
		return StreamResult{}, false, &Error{Code: "upstream_failed", Err: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return StreamResult{}, false, statusError(resp)
	}

	var timedOut atomic.Bool
	timer := time.AfterFunc(t.firstChunkTimeout, func() {
		timedOut.Store(true)
		cancel()
	})
	defer timer.Stop()

	reader := bufio.NewReaderSize(resp.Body, int(t.maxChunkBytes)+1)
	result := StreamResult{}
	acc := make(map[int]*toolAccumulator)
	emitted := false
	done := false
	for {
		event, err := readEvent(reader, t.maxChunkBytes)
		if err != nil {
			if timedOut.Load() {
				return result, emitted, &Error{Code: "upstream_timeout", Err: ErrTimeout}
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return result, emitted, &Error{Code: "canceled", Err: ctxErr}
			}
			if errors.Is(err, io.EOF) {
				if done {
					return result, emitted, nil
				}
				return result, emitted, &Error{Code: "upstream_failed", Err: ErrBrokenStream}
			}
			return result, emitted, &Error{Code: "upstream_failed", Err: err}
		}
		if event == "" {
			continue
		}
		if event == "[DONE]" {
			done = true
			break
		}
		var chunk Chunk
		if err := json.Unmarshal([]byte(event), &chunk); err != nil {
			return result, emitted, &Error{Code: "upstream_failed", Err: fmt.Errorf("decode stream chunk: %w", err)}
		}
		timer.Reset(t.idleTimeout)
		if chunk.Usage != nil {
			result.Usage = chunk.Usage
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != nil {
				result.Content += *choice.Delta.Content
			}
			result.ReasoningContent += choice.Delta.ReasoningText()
			for _, delta := range choice.Delta.ToolCalls {
				item, ok := acc[delta.Index]
				if !ok {
					item = &toolAccumulator{}
					acc[delta.Index] = item
				}
				item.merge(delta)
			}
			if choice.FinishReason != nil {
				result.FinishReason = *choice.FinishReason
			}
		}
		if err := onChunk(chunk); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return result, true, &Error{Code: "canceled", Err: ctxErr}
			}
			return result, true, fmt.Errorf("emit stream chunk: %w", err)
		}
		emitted = true
	}
	if !done {
		return result, emitted, &Error{Code: "upstream_failed", Err: ErrBrokenStream}
	}
	result.ToolCalls = assembleToolCalls(acc)
	return result, emitted, nil
}

type toolAccumulator struct {
	id   string
	kind string
	name string
	args strings.Builder
}

func (a *toolAccumulator) merge(delta ToolCallDelta) {
	if delta.ID != "" {
		a.id = delta.ID
	}
	if delta.Type != "" {
		a.kind = delta.Type
	}
	if delta.Function != nil {
		if delta.Function.Name != "" {
			a.name = delta.Function.Name
		}
		if delta.Function.Arguments != "" {
			a.args.WriteString(delta.Function.Arguments)
		}
	}
}

func assembleToolCalls(acc map[int]*toolAccumulator) []ToolCall {
	if len(acc) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(acc))
	for index := range acc {
		indexes = append(indexes, index)
	}
	sortInts(indexes)
	out := make([]ToolCall, 0, len(indexes))
	for _, index := range indexes {
		item := acc[index]
		if item.name == "" {
			continue
		}
		kind := item.kind
		if kind == "" {
			kind = "function"
		}
		out = append(out, ToolCall{ID: item.id, Type: kind, Function: FunctionCall{Name: item.name, Arguments: item.args.String()}})
	}
	return out
}

func sortInts(values []int) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func readEvent(reader *bufio.Reader, maxBytes int64) (string, error) {
	var data strings.Builder
	started := false
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) && started {
				return data.String(), nil
			}
			return "", err
		}
		if isPrefix {
			return "", ErrChunkTooLarge
		}
		text := string(line)
		if text == "" {
			if started {
				return data.String(), nil
			}
			continue
		}
		if strings.HasPrefix(text, ":") {
			continue
		}
		if strings.HasPrefix(text, "data:") {
			started = true
			value := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
			if data.Len() > 0 {
				data.WriteString("\n")
			}
			data.WriteString(value)
			if int64(data.Len()) > maxBytes {
				return "", ErrChunkTooLarge
			}
		}
	}
}

// Bounds for a diagnostic body: only a short prefix is copied into an error.
const (
	maxErrorBodyBytes = 4096
	maxErrorTextChars = 200
)

func statusError(resp *http.Response) error {
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return &Error{Code: "upstream_rate_limited", Status: resp.StatusCode, RetryAfterMS: retryAfter,
			Err: fmt.Errorf("upstream rate limited: %s", compactBody(body))}
	case resp.StatusCode >= http.StatusInternalServerError:
		return &Error{Code: "upstream_failed", Status: resp.StatusCode,
			Err: fmt.Errorf("upstream error: %s", compactBody(body))}
	default:
		return &Error{Code: "upstream_failed", Status: resp.StatusCode,
			Err: fmt.Errorf("upstream rejected request: %s", compactBody(body))}
	}
}

func parseRetryAfter(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0
		}
		return seconds * time.Second.Milliseconds()
	}
	if when, err := http.ParseTime(value); err == nil {
		wait := time.Until(when).Milliseconds()
		if wait < 0 {
			return 0
		}
		return wait
	}
	return 0
}

func compactBody(body []byte) string {
	text := strings.Join(strings.Fields(string(body)), " ")
	if len(text) > maxErrorTextChars {
		text = text[:maxErrorTextChars]
	}
	return text
}

func IsRetryable(err error) bool {
	var typed *Error
	if !errors.As(err, &typed) {
		return false
	}
	if typed.Code == "canceled" {
		return false
	}
	if typed.Code == "upstream_timeout" {
		return true
	}
	if typed.Status == http.StatusTooManyRequests {
		return false
	}
	return typed.Status == 0 || typed.Status >= http.StatusInternalServerError
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return &Error{Code: "canceled", Err: ctx.Err()}
	case <-timer.C:
		return nil
	}
}
