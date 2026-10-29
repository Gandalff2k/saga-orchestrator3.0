package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Gandalff2k/saga3/internal/platform/todo"
)

// OrderService is what the HTTP layer needs (Service implements it; tests fake it).
type OrderService interface {
	PlaceOrder(ctx context.Context, in PlaceOrder) (Order, bool, error)
	GetOrder(ctx context.Context, id uuid.UUID) (Order, error)
}

// NewHTTPHandler exposes the public API:
//
//	POST /orders        {"seat_id":1,"amount_cents":10000}  [Idempotency-Key: <key>]
//	                    -> 202 Accepted (saga started) | 200 OK (same key replayed)
//	                    -> 422 (same key, different body) | 400 (validation)
//	GET  /orders/{id}   -> 200 | 404
func NewHTTPHandler(svc OrderService, log *slog.Logger) http.Handler {
	h := &httpHandler{svc: svc, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", h.placeOrder)
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	return otelhttp.NewHandler(mux, "orchestrator-api", otelhttp.WithSpanNameFormatter(spanName))
}

// spanName names server spans by route ("POST /orders/{id}"), never by raw
// path: ids in span names explode cardinality and break grouping in Jaeger.
// The mux sets r.Pattern (method included) on the request it matches, which
// the formatter sees once the handler has run.
func spanName(_ string, r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return r.Method + " unmatched"
}

type httpHandler struct {
	svc OrderService
	log *slog.Logger
}

type placeOrderRequest struct {
	SeatID      int   `json:"seat_id"`
	AmountCents int64 `json:"amount_cents"`
}

func (h *httpHandler) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, "invalid JSON body")
		return
	}

	order, created, err := h.svc.PlaceOrder(r.Context(), PlaceOrder{
		SeatID:         req.SeatID,
		AmountCents:    req.AmountCents,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	w.Header().Set("Location", "/orders/"+order.ID.String())
	status := http.StatusAccepted // saga runs asynchronously; poll GET /orders/{id}
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, order)
}

func (h *httpHandler) getOrder(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, "order id must be a UUID")
		return
	}
	order, err := h.svc.GetOrder(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (h *httpHandler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrInvalidInput):
		h.writeError(w, r, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		h.writeError(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrIdempotencyKeyReused):
		// 422 per the IETF Idempotency-Key draft: same key, different payload.
		h.writeError(w, r, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, todo.ErrNotImplemented):
		h.writeError(w, r, http.StatusNotImplemented, err.Error())
	default:
		h.log.ErrorContext(r.Context(), "request failed", "err", err, "path", r.URL.Path)
		h.writeError(w, r, http.StatusInternalServerError, "internal error")
	}
}

func (h *httpHandler) writeError(w http.ResponseWriter, _ *http.Request, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
