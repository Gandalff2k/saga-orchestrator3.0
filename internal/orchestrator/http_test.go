package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type fakeService struct {
	placeIn PlaceOrder
	order   Order
	created bool
	err     error
}

func (f *fakeService) PlaceOrder(_ context.Context, in PlaceOrder) (Order, bool, error) {
	f.placeIn = in
	if err := in.Validate(); err != nil {
		return Order{}, false, err
	}
	return f.order, f.created, f.err
}

func (f *fakeService) GetOrder(_ context.Context, id uuid.UUID) (Order, error) {
	if f.err != nil {
		return Order{}, f.err
	}
	return f.order, nil
}

func TestHTTP_PlaceOrder(t *testing.T) {
	order := Order{ID: uuid.New(), Status: OrderPending}

	tests := []struct {
		name       string
		body       string
		svc        *fakeService
		wantStatus int
	}{
		{"created", `{"seat_id":1,"amount_cents":100}`, &fakeService{order: order, created: true}, http.StatusAccepted},
		{"idempotent replay", `{"seat_id":1,"amount_cents":100}`, &fakeService{order: order, created: false}, http.StatusOK},
		{"bad json", `{`, &fakeService{}, http.StatusBadRequest},
		{"unknown field", `{"seat_id":1,"amount_cents":100,"x":1}`, &fakeService{}, http.StatusBadRequest},
		{"invalid seat", `{"seat_id":0,"amount_cents":100}`, &fakeService{}, http.StatusBadRequest},
		{"invalid amount", `{"seat_id":1,"amount_cents":-5}`, &fakeService{}, http.StatusBadRequest},
		{"internal error hidden", `{"seat_id":1,"amount_cents":100}`, &fakeService{err: errors.New("db password leaked")}, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHTTPHandler(tt.svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
			req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(tt.body))
			req.Header.Set("Idempotency-Key", "k-1")
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body: %s", rec.Code, tt.wantStatus, rec.Body)
			}
			if strings.Contains(rec.Body.String(), "password") {
				t.Fatal("internal error details leaked to the client")
			}
			if tt.wantStatus == http.StatusAccepted {
				if tt.svc.placeIn.IdempotencyKey != "k-1" {
					t.Errorf("idempotency key not forwarded: %q", tt.svc.placeIn.IdempotencyKey)
				}
				if loc := rec.Header().Get("Location"); loc != "/orders/"+order.ID.String() {
					t.Errorf("Location = %q", loc)
				}
			}
		})
	}
}

func TestHTTP_GetOrder(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		svc        *fakeService
		wantStatus int
	}{
		{"found", "/orders/" + uuid.NewString(), &fakeService{order: Order{ID: uuid.New()}}, http.StatusOK},
		{"not found", "/orders/" + uuid.NewString(), &fakeService{err: ErrNotFound}, http.StatusNotFound},
		{"bad id", "/orders/nope", &fakeService{}, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHTTPHandler(tt.svc, slog.New(slog.NewTextHandler(io.Discard, nil)))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}
