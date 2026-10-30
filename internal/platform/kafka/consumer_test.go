package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/retry"
	"github.com/Gandalff2k/saga3/internal/platform/telemetry"
)

var (
	errTransient = errors.New("transient")
	errPoison    = retry.Permanent(errors.New("poison"))
)


type fakeProc struct {
	mu         sync.Mutex
	batchErrs  []error
	one        func(offset int64) error
	healthy    error
	batchCalls int
	oneCalls   []int64
}

func (f *fakeProc) ProcessBatch(context.Context, []*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := min(f.batchCalls, len(f.batchErrs)-1)
	f.batchCalls++
	if i < 0 {
		return nil
	}
	return f.batchErrs[i]
}

func (f *fakeProc) ProcessOne(_ context.Context, rec *kgo.Record) error {
	f.mu.Lock()
	f.oneCalls = append(f.oneCalls, rec.Offset)
	f.mu.Unlock()
	if f.one == nil {
		return nil
	}
	return f.one(rec.Offset)
}

func (f *fakeProc) Healthy(context.Context) error { return f.healthy }

type fakeDLQ struct {
	errs     []error
	calls    int
	onCall   func()
	parked   []int64
	attempts []int
}

func (d *fakeDLQ) DeadLetter(_ context.Context, rec *kgo.Record, _ error, attempts int) error {
	d.calls++
	if d.onCall != nil {
		d.onCall()
	}
	if d.calls <= len(d.errs) && d.errs[d.calls-1] != nil {
		return d.errs[d.calls-1]
	}
	d.parked = append(d.parked, rec.Offset)
	d.attempts = append(d.attempts, attempts)
	return nil
}

func testConsumer(t *testing.T, proc Processor, dlq DeadLetterer) *Consumer {
	t.Helper()
	m, err := newConsumerMetrics("test")
	if err != nil {
		t.Fatal(err)
	}
	return &Consumer{
		proc: proc, dlq: dlq, m: m,
		cfg:    config.Consumer{Retry: retry.Policy{MaxAttempts: 3, Initial: time.Microsecond, Multiplier: 1}},
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		tracer: otel.Tracer(telemetry.ScopeName),
	}
}

func records(n int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = &kgo.Record{Topic: "t", Partition: 0, Offset: int64(i)}
	}
	return out
}

func failAt(offset int64, err error) func(int64) error {
	return func(o int64) error {
		if o == offset {
			return err
		}
		return nil
	}
}

func TestHandlePartition(t *testing.T) {
	tests := []struct {
		name         string
		proc         *fakeProc
		dlq          *fakeDLQ
		wantHandled  int
		wantErr      error
		wantBatches  int
		wantOneCalls []int64
		wantParked   []int64
		wantAttempts []int
	}{
		{
			name:        "batch succeeds: one transaction, nothing else",
			proc:        &fakeProc{batchErrs: []error{nil}},
			dlq:         &fakeDLQ{},
			wantHandled: 5, wantBatches: 1,
		},
		{
			name:        "transient batch failure is retried",
			proc:        &fakeProc{batchErrs: []error{errTransient, errTransient, nil}},
			dlq:         &fakeDLQ{},
			wantHandled: 5, wantBatches: 3,
		},
		{
			name:        "permanent failure: no batch retries, poison isolated, the rest applied",
			proc:        &fakeProc{batchErrs: []error{errPoison}, one: failAt(2, errPoison)},
			dlq:         &fakeDLQ{},
			wantHandled: 5, wantBatches: 1,
			wantOneCalls: []int64{0, 1, 2, 3, 4},
			wantParked:   []int64{2}, wantAttempts: []int{1},
		},
		{
			name:        "record failing transiently forever while DB is healthy = poison",
			proc:        &fakeProc{batchErrs: []error{errTransient}, one: failAt(3, errTransient)},
			dlq:         &fakeDLQ{},
			wantHandled: 5, wantBatches: 3,
			wantOneCalls: []int64{0, 1, 2, 3, 3, 3, 4},
			wantParked:   []int64{3}, wantAttempts: []int{3},
		},
		{
			name:        "DB down: stop at once, nothing dead-lettered, nothing handled",
			proc:        &fakeProc{batchErrs: []error{errTransient}, healthy: errors.New("conn refused")},
			dlq:         &fakeDLQ{},
			wantHandled: 0, wantErr: ErrDependencyDown, wantBatches: 3,
		},
		{
			name: "DB goes down mid slow path: earlier poison parked, prefix handled, then stop",
			proc: &fakeProc{batchErrs: []error{errPoison}, healthy: errors.New("conn refused"),
				one: func(o int64) error {
					switch o {
					case 1:
						return errPoison
					case 3:
						return errTransient
					}
					return nil
				}},
			dlq:         &fakeDLQ{},
			wantHandled: 3, wantErr: ErrDependencyDown, wantBatches: 1,
			wantOneCalls: []int64{0, 1, 2, 3, 3, 3},
			wantParked:   []int64{1}, wantAttempts: []int{1},
		},
		{
			name:        "DLQ write is retried until it succeeds",
			proc:        &fakeProc{batchErrs: []error{errPoison}, one: failAt(0, errPoison)},
			dlq:         &fakeDLQ{errs: []error{errTransient, errTransient}},
			wantHandled: 5, wantBatches: 1,
			wantOneCalls: []int64{0, 1, 2, 3, 4},
			wantParked:   []int64{0}, wantAttempts: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testConsumer(t, tt.proc, tt.dlq)
			handled, err := c.handlePartition(context.Background(), records(5))

			if handled != tt.wantHandled {
				t.Errorf("handled = %d, want %d", handled, tt.wantHandled)
			}
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.proc.batchCalls != tt.wantBatches {
				t.Errorf("batch calls = %d, want %d", tt.proc.batchCalls, tt.wantBatches)
			}
			if !slices.Equal(tt.proc.oneCalls, tt.wantOneCalls) {
				t.Errorf("one-by-one calls = %v, want %v (order matters)", tt.proc.oneCalls, tt.wantOneCalls)
			}
			if !slices.Equal(tt.dlq.parked, tt.wantParked) || !slices.Equal(tt.dlq.attempts, tt.wantAttempts) {
				t.Errorf("parked = %v attempts %v, want %v %v", tt.dlq.parked, tt.dlq.attempts, tt.wantParked, tt.wantAttempts)
			}
		})
	}
}


func TestHandlePartition_StopReturnsHandledPrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	proc := &fakeProc{batchErrs: []error{errPoison}, one: func(o int64) error {
		if o == 2 {
			cancel()
		}
		return nil
	}}
	c := testConsumer(t, proc, &fakeDLQ{})

	handled, err := c.handlePartition(ctx, records(5))
	if handled != 3 || err != nil {
		t.Fatalf("handled = %d, err = %v; want 3, nil", handled, err)
	}
}


func TestHandlePartition_UnparkableRecordIsNotHandled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dlq := &fakeDLQ{errs: slices.Repeat([]error{errTransient}, 100)}
	dlq.onCall = func() {
		if dlq.calls == 4 {
			cancel()
		}
	}
	c := testConsumer(t, &fakeProc{batchErrs: []error{errPoison}, one: failAt(1, errPoison)}, dlq)

	handled, err := c.handlePartition(ctx, records(5))
	if handled != 1 || err != nil || len(dlq.parked) != 0 {
		t.Fatalf("handled = %d, err = %v, parked = %v; want 1, nil, none", handled, err, dlq.parked)
	}
}

func TestByPartition_MergesFetchesInOrder(t *testing.T) {
	rec := func(p int32, o int64) *kgo.Record { return &kgo.Record{Topic: "t", Partition: p, Offset: o} }
	fetch := func(parts ...kgo.FetchPartition) kgo.Fetch {
		return kgo.Fetch{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: parts}}}
	}
	fetches := kgo.Fetches{
		fetch(kgo.FetchPartition{Partition: 0, Records: []*kgo.Record{rec(0, 1), rec(0, 2)}},
			kgo.FetchPartition{Partition: 1, Records: []*kgo.Record{rec(1, 7)}}),
		fetch(kgo.FetchPartition{Partition: 0, Records: []*kgo.Record{rec(0, 3)}},
			kgo.FetchPartition{Partition: 2}),
	}

	got := byPartition(fetches)
	if len(got) != 2 {
		t.Fatalf("partitions = %d, want 2", len(got))
	}
	var offsets []int64
	for _, r := range got[0] {
		offsets = append(offsets, r.Offset)
	}
	if !slices.Equal(offsets, []int64{1, 2, 3}) {
		t.Fatalf("partition 0 offsets = %v, want [1 2 3]", offsets)
	}
}

type fakeOffsets struct {
	committed []*kgo.Record
	rewound   map[string]map[int32]kgo.EpochOffset
}

func (f *fakeOffsets) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.committed = append(f.committed, rs...)
	return nil
}

func (f *fakeOffsets) SetOffsets(o map[string]map[int32]kgo.EpochOffset) { f.rewound = o }

func TestSettle_CommitsHandledPrefixAndRewindsTheRest(t *testing.T) {
	part := func(p int32, n int) []*kgo.Record {
		out := make([]*kgo.Record, n)
		for i := range out {
			out[i] = &kgo.Record{Topic: "t", Partition: p, Offset: int64(10 + i), LeaderEpoch: 3}
		}
		return out
	}
	parts := [][]*kgo.Record{part(0, 5), part(1, 5), part(2, 5)}
	handled := []int{5, 2, 0} // fully, partly, not at all

	off := &fakeOffsets{}
	c := testConsumer(t, &fakeProc{}, &fakeDLQ{})
	c.offsets = off
	c.settle(context.Background(), parts, handled)

	var committed []string
	for _, r := range off.committed {
		committed = append(committed, fmt.Sprintf("p%d@%d", r.Partition, r.Offset))
	}
	if !slices.Equal(committed, []string{"p0@14", "p1@11"}) {
		t.Errorf("committed %v, want last handled record of p0 and p1 only", committed)
	}
	want := map[int32]kgo.EpochOffset{1: {Epoch: 3, Offset: 12}, 2: {Epoch: 3, Offset: 10}}
	if got := off.rewound["t"]; len(got) != 2 || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("rewound %v, want %v (first unhandled record; fully handled p0 untouched)", got, want)
	}
}

func TestSettle_NothingToRewindWhenAllHandled(t *testing.T) {
	off := &fakeOffsets{}
	c := testConsumer(t, &fakeProc{}, &fakeDLQ{})
	c.offsets = off
	c.settle(context.Background(), [][]*kgo.Record{records(3)}, []int{3})
	if off.rewound != nil {
		t.Fatalf("rewound %v on a fully handled poll", off.rewound)
	}
}
