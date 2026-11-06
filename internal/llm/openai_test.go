package llm

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAI_ToolCallRoundTrip(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("path %s auth %q", r.URL.Path, r.Header.Get("Authorization"))
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		_, _ = io.WriteString(w, `{"model":"m","choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":null,
			"tool_calls":[{"id":"c1","type":"function","function":{"name":"card_history","arguments":"{}"}}]}}],
			"usage":{"prompt_tokens":10,"completion_tokens":3}}`)
	}))
	defer srv.Close()

	c, err := NewOpenAI(Config{BaseURL: srv.URL + "/v1/", APIKey: "k", Model: "m", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Chat(t.Context(), Request{
		Messages: []Message{
			{Role: RoleSystem, Content: "s"},
			{Role: RoleAssistant, ToolCalls: []ToolCall{{ID: "c0", Name: "x", Arguments: "{}"}}},
			{Role: RoleTool, ToolCallID: "c0", Content: "[]"},
		},
		Tools: []Tool{{Name: "card_history", Description: "d", Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Message.ToolCalls) != 1 || resp.Message.ToolCalls[0].Name != "card_history" || resp.Usage.PromptTokens != 10 {
		t.Fatalf("response = %+v", resp)
	}

	msgs := got["messages"].([]any)
	asst := msgs[1].(map[string]any)
	if _, has := asst["content"]; !has || asst["content"] != nil {
		t.Errorf("assistant tool-call message must send content: null, got %v", asst["content"])
	}
	call := asst["tool_calls"].([]any)[0].(map[string]any)
	if call["type"] != "function" || call["function"].(map[string]any)["name"] != "x" {
		t.Errorf("tool call wire format = %v", call)
	}
	if msgs[2].(map[string]any)["tool_call_id"] != "c0" {
		t.Errorf("tool result = %v", msgs[2])
	}
	tool := got["tools"].([]any)[0].(map[string]any)
	if tool["type"] != "function" || tool["function"].(map[string]any)["name"] != "card_history" {
		t.Errorf("tool wire format = %v", tool)
	}
}

func TestOpenAI_RetriesRateLimits(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`)
	}))
	defer srv.Close()

	c, _ := NewOpenAI(Config{BaseURL: srv.URL, Model: "m", Timeout: time.Second, MaxRetries: 4, MaxBackoff: 10 * time.Millisecond})
	resp, err := c.Chat(t.Context(), Request{Messages: []Message{{Role: RoleUser, Content: "hi"}}})
	if err != nil || resp.Message.Content != "ok" || calls.Load() != 3 {
		t.Fatalf("resp %+v err %v calls %d", resp, err, calls.Load())
	}
}

func TestOpenAI_ClientErrorsAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, strings.Repeat("x", 1000), http.StatusBadRequest)
	}))
	defer srv.Close()

	c, _ := NewOpenAI(Config{BaseURL: srv.URL, Model: "m", Timeout: time.Second, MaxRetries: 4})
	_, err := c.Chat(t.Context(), Request{})
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 400 || calls.Load() != 1 || len(se.Body) > 310 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
}
