//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/orchestrator"
)

// The public API end to end over real HTTP and a real database.
func TestAPI_PlaceAndGetOrder_WithIdempotencyKey(t *testing.T) {
	svc, pool := newOrchestrator(t)
	srv := httptest.NewServer(orchestrator.NewHTTPHandler(svc, env.Log))
	t.Cleanup(srv.Close)

	key := "it-http-" + uuid.NewString()
	post := func(body string) (int, http.Header, orchestrator.Order) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/orders", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		return do(t, req)
	}

	status, hdr, first := post(`{"seat_id": 5, "amount_cents": 2500}`)
	if status != http.StatusAccepted {
		t.Fatalf("first POST: %d", status)
	}
	if loc := hdr.Get("Location"); loc != "/orders/"+first.ID.String() {
		t.Errorf("Location = %q", loc)
	}

	status, _, second := post(`{"seat_id": 5, "amount_cents": 2500}`)
	if status != http.StatusOK || second.ID != first.ID {
		t.Fatalf("replay: %d, order %v (want 200, %v)", status, second.ID, first.ID)
	}

	status, _, _ = post(`{"seat_id": 6, "amount_cents": 2500}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("same key, other body: %d, want 422", status)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/orders/"+first.ID.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	status, _, got := do(t, req)
	if status != http.StatusOK || got.Status != orchestrator.OrderPending || got.SagaState != orchestrator.StateReserving {
		t.Fatalf("GET: %d %+v", status, got)
	}

	if n := count(t, pool, `SELECT count(*) FROM sagas WHERE order_id = $1`, first.ID); n != 1 {
		t.Fatalf("sagas for order: %d, want 1", n)
	}
}

// do sends req and returns status, headers and the decoded order (on 2xx).
// The body is consumed and closed here; the *http.Response never escapes.
func do(t *testing.T, req *http.Request) (int, http.Header, orchestrator.Order) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var o orchestrator.Order
	if resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
	return resp.StatusCode, resp.Header, o
}
