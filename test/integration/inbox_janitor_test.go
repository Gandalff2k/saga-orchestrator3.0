//go:build integration

package integration

import (
	"sync"
	"testing"
	"time"

	"github.com/Gandalff2k/saga3/internal/platform/config"
	"github.com/Gandalff2k/saga3/internal/platform/inbox"
)

// Closes the D-311 gap: old inbox rows are deleted in chunks, recent ones
// kept, and concurrent janitors (one per worker replica) don't double-delete.
func TestInboxJanitor_DeletesOnlyExpiredRows(t *testing.T) {
	pool := env.FreshDB(t, "orchestrator")
	mustExec(t, pool, `INSERT INTO inbox (consumer, message_id, received_at)
	                   SELECT 'g', gen_random_uuid(), now() - interval '20 days' FROM generate_series(1, 2500)`)
	mustExec(t, pool, `INSERT INTO inbox (consumer, message_id, received_at)
	                   SELECT 'g', gen_random_uuid(), now() - interval '1 day' FROM generate_series(1, 300)`)

	cfg := config.Inbox{Retention: 14 * 24 * time.Hour, JanitorInterval: time.Hour}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		deleted int64
	)
	for range 3 {
		j := inbox.NewJanitor(pool, cfg, env.Log)
		wg.Go(func() {
			// Like replicas on a timer: each keeps trying until nothing is left.
			for range 10 {
				n, err := j.Clean(t.Context())
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				deleted += n
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
	wg.Wait()

	if deleted != 2500 {
		t.Errorf("janitors reported %d deleted, want 2500 (more = double counting, less = left behind)", deleted)
	}
	if n := count(t, pool, `SELECT count(*) FROM inbox WHERE received_at < now() - interval '14 days'`); n != 0 {
		t.Errorf("%d expired rows left", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM inbox`); n != 300 {
		t.Errorf("%d rows left, want the 300 recent ones", n)
	}
}
