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

var testKey = []byte("0123456789abcdef0123456789abcdef")

func testHandler(t *testing.T, svc OrderService) http.Handler {
	t.Helper()
	cards, err := NewCardHasher(testKey)
	if err != nil {
		t.Fatal(err)
	}
	return NewHTTPHandler(svc, cards, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func orderBody(override string) string {
	base := `"seat_id":1,"amount_cents":100,"customer_id":"cus_1","card_number":"4242424242424242",` +
		`"ip_address":"203.0.113.7","country":"UA"`
	if override == "" {
		return "{" + base + "}"
	}
	return "{" + base + "," + override + "}"
}

func TestHTTP_PlaceOrder(t *testing.T) {
	order := Order{ID: uuid.New(), Status: OrderPending}

	tests := []struct {
		name       string
		body       string
		svc        *fakeService
		wantStatus int
	}{
		{"created", orderBody(""), &fakeService{order: order, created: true}, http.StatusAccepted},
		{"idempotent replay", orderBody(""), &fakeService{order: order, created: false}, http.StatusOK},
		{"bad json", `{`, &fakeService{}, http.StatusBadRequest},
		{"unknown field", orderBody(`"x":1`), &fakeService{}, http.StatusBadRequest},
		{"invalid seat", `{"seat_id":0,"amount_cents":100,"customer_id":"c","card_number":"4242424242424242","ip_address":"1.2.3.4","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"invalid amount", `{"seat_id":1,"amount_cents":-5,"customer_id":"c","card_number":"4242424242424242","ip_address":"1.2.3.4","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"card with letters", `{"seat_id":1,"amount_cents":100,"customer_id":"c","card_number":"4242abcd42424242","ip_address":"1.2.3.4","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"card too short", `{"seat_id":1,"amount_cents":100,"customer_id":"c","card_number":"4242","ip_address":"1.2.3.4","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"bad ip", `{"seat_id":1,"amount_cents":100,"customer_id":"c","card_number":"4242424242424242","ip_address":"999.1.1.1","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"bad country", `{"seat_id":1,"amount_cents":100,"customer_id":"c","card_number":"4242424242424242","ip_address":"1.2.3.4","country":"ukr"}`, &fakeService{}, http.StatusBadRequest},
		{"no customer", `{"seat_id":1,"amount_cents":100,"card_number":"4242424242424242","ip_address":"1.2.3.4","country":"UA"}`, &fakeService{}, http.StatusBadRequest},
		{"key reused with another body", orderBody(""), &fakeService{err: ErrIdempotencyKeyReused}, http.StatusUnprocessableEntity},
		{"internal error hidden", orderBody(""), &fakeService{err: errors.New("db password leaked")}, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testHandler(t, tt.svc)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders", strings.NewReader(tt.body))
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
				if h := tt.svc.placeIn.CardHash; len(h) != 64 || strings.Contains(h, "4242") {
					t.Errorf("card must reach the service only as an HMAC, got %q", h)
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
			h := testHandler(t, tt.svc)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}
