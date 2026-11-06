package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Config selects an OpenAI-compatible provider: Groq, OpenRouter, Ollama, Gemini, OpenAI.
type Config struct {
	BaseURL    string        `env:"LLM_BASE_URL" envDefault:"https://api.groq.com/openai/v1"`
	APIKey     string        `env:"LLM_API_KEY,unset"`
	Model      string        `env:"LLM_MODEL" envDefault:"openai/gpt-oss-120b"`
	Timeout    time.Duration `env:"LLM_TIMEOUT" envDefault:"60s"`
	MaxRetries int           `env:"LLM_MAX_RETRIES" envDefault:"4"`
	MaxBackoff time.Duration `env:"LLM_MAX_BACKOFF" envDefault:"60s"`
}

// OpenAI talks to any /chat/completions endpoint.
type OpenAI struct {
	cfg  Config
	http *http.Client
}

// NewOpenAI builds the client.
func NewOpenAI(cfg Config) (*OpenAI, error) {
	if cfg.BaseURL == "" || cfg.Model == "" {
		return nil, errors.New("llm: LLM_BASE_URL and LLM_MODEL are required")
	}
	return &OpenAI{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}, nil
}

type wireMessage struct {
	Role       string         `json:"role"`
	Content    *string        `json:"content"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type wireTool struct {
	Type     string `json:"type"`
	Function Tool   `json:"function"`
}

type wireRequest struct {
	Model       string        `json:"model"`
	Messages    []wireMessage `json:"messages"`
	Tools       []wireTool    `json:"tools,omitempty"`
	Temperature float64       `json:"temperature"`
}

type wireResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      wireMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage Usage `json:"usage"`
}

// StatusError is a non-2xx answer from the provider.
type StatusError struct {
	Code       int
	Body       string
	RetryAfter time.Duration
}

func (e *StatusError) Error() string { return fmt.Sprintf("llm: HTTP %d: %s", e.Code, e.Body) }

func (e *StatusError) retryable() bool { return e.Code == http.StatusTooManyRequests || e.Code >= 500 }

// Chat sends one turn, retrying rate limits and server errors.
func (c *OpenAI) Chat(ctx context.Context, req Request) (Response, error) {
	body, err := json.Marshal(c.wire(req))
	if err != nil {
		return Response{}, err
	}
	backoff := time.Second
	for attempt := 0; ; attempt++ {
		resp, err := c.do(ctx, body)
		var se *StatusError
		if err == nil || !errors.As(err, &se) || !se.retryable() || attempt >= c.cfg.MaxRetries {
			return resp, err
		}
		wait := min(max(se.RetryAfter, backoff), c.cfg.MaxBackoff)
		backoff *= 2
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (c *OpenAI) wire(req Request) wireRequest {
	w := wireRequest{Model: c.cfg.Model}
	for _, m := range req.Messages {
		wm := wireMessage{Role: m.Role, ToolCallID: m.ToolCallID}
		if m.Content != "" || len(m.ToolCalls) == 0 {
			content := m.Content
			wm.Content = &content
		}
		for _, tc := range m.ToolCalls {
			var wt wireToolCall
			wt.ID, wt.Type = tc.ID, "function"
			wt.Function.Name, wt.Function.Arguments = tc.Name, tc.Arguments
			wm.ToolCalls = append(wm.ToolCalls, wt)
		}
		w.Messages = append(w.Messages, wm)
	}
	for _, t := range req.Tools {
		w.Tools = append(w.Tools, wireTool{Type: "function", Function: t})
	}
	return w
}

func (c *OpenAI) do(ctx context.Context, body []byte) (Response, error) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.cfg.BaseURL, "/")+"/chat/completions",
		bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	hresp, err := c.http.Do(hreq)
	if err != nil {
		return Response{}, fmt.Errorf("llm: %w", err)
	}
	defer func() { _ = hresp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(hresp.Body, 4<<20))
	if err != nil {
		return Response{}, fmt.Errorf("llm: read body: %w", err)
	}
	if hresp.StatusCode/100 != 2 {
		se := &StatusError{Code: hresp.StatusCode, Body: truncate(string(raw), 300)}
		if s, err := strconv.Atoi(hresp.Header.Get("Retry-After")); err == nil {
			se.RetryAfter = time.Duration(s) * time.Second
		}
		return Response{}, se
	}

	var wr wireResponse
	if err := json.Unmarshal(raw, &wr); err != nil {
		return Response{}, fmt.Errorf("llm: decode response: %w", err)
	}
	if len(wr.Choices) == 0 {
		return Response{}, errors.New("llm: response has no choices")
	}
	ch := wr.Choices[0]
	msg := Message{Role: RoleAssistant}
	if ch.Message.Content != nil {
		msg.Content = *ch.Message.Content
	}
	for _, tc := range ch.Message.ToolCalls {
		msg.ToolCalls = append(msg.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	return Response{Message: msg, FinishReason: ch.FinishReason, Usage: wr.Usage, Model: wr.Model}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
