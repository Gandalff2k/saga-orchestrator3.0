package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Turn is one recorded request and its response.
type Turn struct {
	Request  Request  `json:"request"`
	Response Response `json:"response"`
}

// Recorder passes calls through to a real client and keeps every turn.
type Recorder struct {
	Next  Client
	mu    sync.Mutex
	turns []Turn
}

// Chat implements Client.
func (r *Recorder) Chat(ctx context.Context, req Request) (Response, error) {
	resp, err := r.Next.Chat(ctx, req)
	if err == nil {
		r.mu.Lock()
		r.turns = append(r.turns, Turn{Request: req, Response: resp})
		r.mu.Unlock()
	}
	return resp, err
}

// Turns returns a copy of what was recorded so far.
func (r *Recorder) Turns() []Turn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Turn(nil), r.turns...)
}

// Save writes the recorded turns as a cassette and clears them.
func (r *Recorder) Save(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := json.MarshalIndent(r.turns, "", "  ")
	if err != nil {
		return err
	}
	r.turns = nil
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

// ErrCassetteExhausted means the code made more calls than were recorded.
var ErrCassetteExhausted = errors.New("llm: cassette has no more turns")

// Replay answers calls from a cassette, in order, without a network.
type Replay struct {
	mu    sync.Mutex
	turns []Turn
}

// LoadCassette reads a cassette written by Recorder.Save.
func LoadCassette(path string) (*Replay, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: test fixture path
	if err != nil {
		return nil, err
	}
	var turns []Turn
	if err := json.Unmarshal(b, &turns); err != nil {
		return nil, fmt.Errorf("cassette %s: %w", path, err)
	}
	return &Replay{turns: turns}, nil
}

// Script is a Replay built from responses in code, for unit tests.
func Script(responses ...Response) *Replay {
	r := &Replay{}
	for _, resp := range responses {
		r.turns = append(r.turns, Turn{Response: resp})
	}
	return r
}

// Chat implements Client.
func (r *Replay) Chat(_ context.Context, _ Request) (Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.turns) == 0 {
		return Response{}, ErrCassetteExhausted
	}
	t := r.turns[0]
	r.turns = r.turns[1:]
	return t.Response, nil
}
