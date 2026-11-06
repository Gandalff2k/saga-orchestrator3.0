// fraudgen sends labelled synthetic orders and scores the risk service against the labels.
//
//	fraudgen run    [-url http://localhost:8080] [-seed 1] [-seat-offset N] [-rate 20] [-out labels.jsonl]
//	fraudgen report [-labels labels.jsonl]                                          (RISK_DSN)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/Gandalff2k/saga3/internal/fraudgen"
	"github.com/Gandalff2k/saga3/internal/platform/postgres"
)

type label struct {
	SagaID   uuid.UUID `json:"saga_id"`
	OrderID  uuid.UUID `json:"order_id"`
	Scenario string    `json:"scenario"`
	Fraud    bool      `json:"fraud"`
}

func main() {
	if err := dispatch(); err != nil {
		fmt.Fprintln(os.Stderr, "fraudgen:", err)
		os.Exit(1)
	}
}

func dispatch() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 {
		return errors.New("usage: fraudgen run | report")
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(ctx, os.Args[2:])
	case "report":
		err = report(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	return err
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	url := fs.String("url", "http://localhost:8080", "orchestrator API")
	seed := fs.Uint64("seed", 1, "plan seed")
	offset := fs.Int("seat-offset", 1001+rand.IntN(90_000), "first seat id") //nolint:gosec // G404: test data
	rate := fs.Int("rate", 20, "orders per second")
	out := fs.String("out", "labels.jsonl", "labels file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	history, live := fraudgen.Plan(*seed, fraudgen.DefaultMix)
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)

	client := &http.Client{Timeout: 10 * time.Second}
	tick := time.NewTicker(time.Second / time.Duration(max(1, *rate)))
	defer tick.Stop()
	seat := *offset
	send := func(orders []fraudgen.Order) error {
		for _, o := range orders {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
			}
			l, err := place(ctx, client, *url, seat, o)
			if err != nil {
				return err
			}
			seat++
			if err := enc.Encode(l); err != nil {
				return err
			}
		}
		return nil
	}

	if err := send(history); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)
	if err := send(live); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Printf("sent %d history + %d live orders, labels in %s\n", len(history), len(live), *out)
	return nil
}

func place(ctx context.Context, c *http.Client, url string, seat int, o fraudgen.Order) (label, error) {
	body, err := json.Marshal(map[string]any{"seat_id": seat, "amount_cents": o.AmountCents,
		"customer_id": o.CustomerID, "card_number": o.CardNumber, "ip_address": o.IP, "country": o.Country})
	if err != nil {
		return label{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/orders", bytes.NewReader(body))
	if err != nil {
		return label{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return label{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		return label{}, fmt.Errorf("POST /orders: %s", resp.Status)
	}
	var placed struct {
		OrderID uuid.UUID `json:"order_id"`
		SagaID  uuid.UUID `json:"saga_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&placed); err != nil {
		return label{}, err
	}
	return label{SagaID: placed.SagaID, OrderID: placed.OrderID, Scenario: o.Scenario, Fraud: o.Fraud}, nil
}

func report(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	path := fs.String("labels", "labels.jsonl", "labels file written by run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dsn := os.Getenv("RISK_DSN")
	if dsn == "" {
		return errors.New("RISK_DSN is not set")
	}

	labels, err := readLabels(*path)
	if err != nil {
		return err
	}
	pool, err := postgres.NewPool(ctx, dsn, 2)
	if err != nil {
		return err
	}
	defer pool.Close()

	ids := make([]uuid.UUID, len(labels))
	for i, l := range labels {
		ids[i] = l.SagaID
	}
	rows, err := pool.Query(ctx, `SELECT saga_id, decision FROM risk_decisions WHERE saga_id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	decisions := map[uuid.UUID]string{}
	for rows.Next() {
		var (
			id uuid.UUID
			d  string
		)
		if err := rows.Scan(&id, &d); err != nil {
			rows.Close()
			return err
		}
		decisions[id] = d
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	outcomes := make([]fraudgen.Outcome, len(labels))
	for i, l := range labels {
		outcomes[i] = fraudgen.Outcome{Scenario: l.Scenario, Fraud: l.Fraud, Decision: decisions[l.SagaID]}
	}
	return fraudgen.Score(outcomes).Print(os.Stdout)
}

func readLabels(path string) ([]label, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the operator names the file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []label
	dec := json.NewDecoder(f)
	for dec.More() {
		var l label
		if err := dec.Decode(&l); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}
