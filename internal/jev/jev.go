package jev

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Question struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

type Answer struct {
	Choice     string  `json:"choice"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason"`
}

type Client struct {
	http        *http.Client
	url         string
	key         string
	model       string
	timeout     time.Duration
	retry       int
	backoff     time.Duration
	maxBodySize int64
}

type Options struct {
	BaseURL     string
	Key         string
	Model       string
	Timeout     time.Duration
	Retry       int
	Backoff     time.Duration
	MaxBodySize int64
}

func New(opts Options) *Client {
	return &Client{
		http:        &http.Client{},
		url:         strings.TrimRight(opts.BaseURL, "/") + "/v1/systemone",
		key:         opts.Key,
		model:       opts.Model,
		timeout:     opts.Timeout,
		retry:       opts.Retry,
		backoff:     opts.Backoff,
		maxBodySize: opts.MaxBodySize,
	}
}

type request struct {
	Model     string              `json:"model"`
	State     any                 `json:"state"`
	Questions map[string]Question `json:"questions"`
}

type response struct {
	Answers map[string]struct {
		Choice      string  `json:"choice"`
		Confidence  float64 `json:"confidence"`
		Reason      string  `json:"reason"`
		Explanation string  `json:"explanation"`
	} `json:"answers"`
}

func (c *Client) Ask(ctx context.Context, state any, questions map[string]Question) (map[string]Answer, error) {
	if len(questions) == 0 {
		return nil, errors.New("jev: questions are empty")
	}
	body, err := c.requestBody(state, questions)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt <= c.retry; attempt++ {
		if attempt > 0 && c.backoff > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("jev request canceled: %w", ctx.Err())
			case <-time.After(time.Duration(attempt) * c.backoff):
			}
		}
		answers, retryable, err := c.once(ctx, body, questions)
		if err == nil {
			return answers, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, fmt.Errorf("jev request failed after %d attempts: %w", c.retry+1, lastErr)
}

func (c *Client) RequestSizeBytes(state any, questions map[string]Question) (int, error) {
	body, err := c.requestBody(state, questions)
	if err != nil {
		return 0, err
	}
	return len(body), nil
}

func (c *Client) requestBody(state any, questions map[string]Question) ([]byte, error) {
	body, err := json.Marshal(request{Model: c.model, State: state, Questions: questions})
	if err != nil {
		return nil, fmt.Errorf("encode jev request: %w", err)
	}
	return body, nil
}

func (c *Client) once(ctx context.Context, body []byte, questions map[string]Question) (map[string]Answer, bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("build jev request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("call jev: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("jev responded %d", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("jev responded %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBodySize))
	if err != nil {
		return nil, true, fmt.Errorf("read jev response: %w", err)
	}
	var parsed response
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&parsed); err != nil {
		return nil, false, fmt.Errorf("decode jev response: %w", err)
	}
	out := make(map[string]Answer, len(parsed.Answers))
	for name, answer := range parsed.Answers {
		question, ok := questions[name]
		if !ok {
			return nil, false, fmt.Errorf("jev: answer for unknown question %q", name)
		}
		if _, ok := question.Criteria[answer.Choice]; !ok {
			return nil, false, fmt.Errorf("jev: answer %q for %q is outside criteria", answer.Choice, name)
		}
		if answer.Confidence < 0 || answer.Confidence > 1 {
			return nil, false, fmt.Errorf("jev: confidence %f for %q is outside 0..1", answer.Confidence, name)
		}
		reason := answer.Reason
		if reason == "" {
			reason = answer.Explanation
		}
		out[name] = Answer{Choice: answer.Choice, Confidence: answer.Confidence, Reason: reason}
	}
	for name := range questions {
		if _, ok := out[name]; !ok {
			return nil, false, fmt.Errorf("jev: answer for %q is missing", name)
		}
	}
	return out, false, nil
}
