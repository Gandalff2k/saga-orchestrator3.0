//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gandalff2k/saga3/internal/inventory"
	"github.com/Gandalff2k/saga3/internal/messaging"
	"github.com/Gandalff2k/saga3/internal/orchestrator"
	"github.com/Gandalff2k/saga3/internal/payment"
	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/faults"
	"github.com/Gandalff2k/saga3/internal/platform/inbox"
	"github.com/Gandalff2k/saga3/internal/platform/kafka"
	"github.com/Gandalff2k/saga3/internal/platform/outbox"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
)

// system is the whole application running in-process for one test, in its
// own Kafka namespace (topic prefix) with its own three databases, so
// full-flow tests never see each other's messages.
type system struct {
	t    *testing.T
	ns   kafka.Namespace
	opts sysOpts

	orchDB, invDB, payDB *pgxpool.Pool
	svc                  *orchestrator.Service
	api                  *httptest.Server
}

type sysOpts struct {
	paymentLimit     int64
	inventoryFaults  faults.Config
	paymentFaults    faults.Config
	inventoryWorkers int // 0 with noInventory: start them later with addInventoryWorker
	noInventory      bool
	noPayment        bool
	noSweeper        bool // tests that drive Sweeper.SweepOnce themselves
	stepTimeout      time.Duration
	retry            retry.Policy
}

func defaultOpts() sysOpts {
	return sysOpts{
		paymentLimit:     100_000,
		inventoryWorkers: 1,
		stepTimeout:      30 * time.Second,
		retry:            retry.Policy{MaxAttempts: 5, Initial: 20 * time.Millisecond, Max: 200 * time.Millisecond, Multiplier: 2},
	}
}

func startSystem(t *testing.T, opts sysOpts) *system {
	t.Helper()
	s := &system{t: t, opts: opts, ns: kafka.Namespace("it-" + uuid.NewString()[:8] + "-")}
	s.orchDB = env.FreshDB(t, "orchestrator")
	s.invDB = env.FreshDB(t, "inventory")
	s.payDB = env.FreshDB(t, "payment")

	cl, err := kafka.NewClient(env.Brokers, "it-admin")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := kafka.EnsureTopics(t.Context(), cl, s.ns.Specs(messaging.Topics(3, 1)), env.Log); err != nil {
		t.Fatal(err)
	}

	// One relay per service database.
	s.startRelay(s.orchDB, "relay-orch")
	s.startRelay(s.invDB, "relay-inv")
	s.startRelay(s.payDB, "relay-pay")

	// Workers.
	s.svc, err = orchestrator.NewService(s.orchDB, env.Log)
	if err != nil {
		t.Fatal(err)
	}
	s.startWorker("orchestrator", messaging.TopicOrchestratorReplies, s.orchDB, s.svc.HandleReply)
	if !opts.noSweeper {
		s.startSweeper()
	}
	if !opts.noInventory {
		for range max(1, opts.inventoryWorkers) {
			s.addInventoryWorker()
		}
	}
	if !opts.noPayment {
		s.addPaymentWorker()
	}

	s.api = httptest.NewServer(orchestrator.NewHTTPHandler(s.svc, env.Log))
	t.Cleanup(s.api.Close)
	return s
}

func (s *system) group(service string) string { return string(s.ns) + service }

func (s *system) startRelay(pool *pgxpool.Pool, name string) func() {
	return startRelayNS(s.t, pool, relayCfg(), s.ns, name)
}

func (s *system) startWorker(service, logicalTopic string, pool *pgxpool.Pool, h inbox.Handler) *runningConsumer {
	cfg := consumerCfg(s.group(service), s.ns.Topic(logicalTopic))
	cfg.Retry = s.opts.retry
	return startConsumer(s.t, pool, cfg, h)
}

// startSweeper runs the saga timeout sweeper like orchestrator-worker does.
func (s *system) startSweeper() {
	sw := orchestrator.NewSweeper(s.orchDB, s.svc, s.opts.stepTimeout, 200*time.Millisecond, 100, env.Log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sw.Run(ctx)
	}()
	s.t.Cleanup(func() { cancel(); <-done })
}

func (s *system) addPaymentWorker() *runningConsumer {
	pay := payment.NewService(payment.Config{LimitCents: s.opts.paymentLimit, Faults: s.opts.paymentFaults}, env.Log)
	return s.startWorker("payment", messaging.TopicPaymentCommands, s.payDB, pay.Handle)
}

func (s *system) addInventoryWorker() *runningConsumer {
	inv := inventory.NewService(s.opts.inventoryFaults, env.Log)
	return s.startWorker("inventory", messaging.TopicInventoryCommands, s.invDB, inv.Handle)
}

// placeOrder POSTs an order; traceparent may be "" (no incoming trace).
func (s *system) placeOrder(seat int, amountCents int64, traceparent string) orchestrator.Order {
	s.t.Helper()
	body, _ := json.Marshal(map[string]any{"seat_id": seat, "amount_cents": amountCents})
	req, err := http.NewRequestWithContext(s.t.Context(), http.MethodPost, s.api.URL+"/orders", bytes.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	if traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	status, _, o := do(s.t, req)
	if status != http.StatusAccepted {
		s.t.Fatalf("POST /orders: %d", status)
	}
	return o
}

// waitDone waits until every order's saga reached a terminal state and
// returns the final orders by id.
func (s *system) waitDone(timeout time.Duration, ids ...uuid.UUID) map[uuid.UUID]orchestrator.Order {
	s.t.Helper()
	out := map[uuid.UUID]orchestrator.Order{}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if _, done := out[id]; done {
				continue
			}
			o, err := s.svc.GetOrder(s.t.Context(), id)
			if err != nil {
				s.t.Fatal(err)
			}
			if o.SagaState.Terminal() {
				out[id] = o
			}
		}
		if len(out) == len(ids) {
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
	var stuck []string
	for _, id := range ids {
		if _, done := out[id]; !done {
			o, _ := s.svc.GetOrder(s.t.Context(), id)
			stuck = append(stuck, fmt.Sprintf("%s=%s/%s", id.String()[:8], o.Status, o.SagaState))
		}
	}
	s.t.Fatalf("%d of %d sagas not finished after %s: %v", len(stuck), len(ids), timeout, stuck)
	return nil
}

// dlqCount reads a namespaced DLQ topic.
func (s *system) dlqCount(logicalTopic string) int {
	s.t.Helper()
	return len(consumeAll(s.t, kafka.DLQTopic(s.ns.Topic(logicalTopic)), 0, time.Second, 10*time.Second))
}

func (s *system) assertNoDLQ() {
	s.t.Helper()
	for _, topic := range []string{messaging.TopicInventoryCommands, messaging.TopicPaymentCommands, messaging.TopicOrchestratorReplies} {
		if n := s.dlqCount(topic); n != 0 {
			s.t.Errorf("%s DLQ has %d records", topic, n)
		}
	}
}

// startRelayNS is startRelay with a namespace.
func startRelayNS(t *testing.T, pool *pgxpool.Pool, cfg config.Relay, ns kafka.Namespace, name string) (stop func()) {
	t.Helper()
	client, err := kafka.NewClient(env.Brokers, name, kafka.ProducerOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	r, err := outbox.NewRelay(pool, client, cfg, ns, name, env.Log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
		client.Close()
	}()
	stopped := false
	stop = func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

// startCrashableRelay runs a relay on its OWN pool. crash() closes that pool
// first and then stops the relay: like a killed process, it can neither
// finish its batch bookkeeping nor release its leases, so they must expire.
func startCrashableRelay(t *testing.T, dsnFrom *pgxpool.Pool, ns kafka.Namespace, name string) (crash func()) {
	t.Helper()
	cc := dsnFrom.Config().ConnConfig
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cc.User, cc.Password, cc.Host, cc.Port, cc.Database)
	pool, err := postgres.NewPool(t.Context(), dsn, 5)
	if err != nil {
		t.Fatal(err)
	}
	stop := startRelayNS(t, pool, relayCfg(), ns, name)
	return func() {
		pool.Close()
		stop()
	}
}
