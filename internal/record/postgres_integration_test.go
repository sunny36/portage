//go:build integration

package record_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/record/pgtest"
)

func newStore(t *testing.T) (*record.PGStore, *pgxpool.Pool) {
	t.Helper()
	pool, _ := pgtest.NewPool(t)
	if err := record.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return record.NewPGStore(pool), pool
}

var t0 = time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)

func cand(key, version string, mtime time.Time) record.Candidate {
	return record.Candidate{PipelineID: "p1", Key: key, Version: version, Size: 100, MTime: mtime, EventTime: mtime}
}

func mustClaim(t *testing.T, s *record.PGStore, c record.Candidate, lease time.Duration, want record.Decision) record.Record {
	t.Helper()
	d, r, err := s.Claim(context.Background(), c, lease)
	if err != nil {
		t.Fatalf("Claim(%s@%s): %v", c.Key, c.Version, err)
	}
	if d != want {
		t.Fatalf("Claim(%s@%s) = %q, want %q", c.Key, c.Version, d, want)
	}
	return r
}

func TestMigrateIdempotentAndConcurrent(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.NewPool(t)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- record.Migrate(ctx, pool) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Migrate: %v", err)
		}
	}
	if err := record.Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM portage_schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("applied migrations = %d, want 2", n)
	}
}

func TestClaimCompleteLifecycle(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	if _, err := s.Get(ctx, "p1", "a"); !errors.Is(err, record.ErrNotFound) {
		t.Fatalf("Get missing = %v, want ErrNotFound", err)
	}

	c := cand("a", "v1", t0)
	c.Sequencer = "0A"
	r := mustClaim(t, s, c, time.Minute, record.DecisionCopy)
	if r.Status != record.StatusCopying || r.Attempts != 1 || r.SourceVersion != "v1" || r.ClaimedUntil.IsZero() {
		t.Fatalf("claimed record = %+v", r)
	}
	if err := s.SetUpload(ctx, "p1", "a", "v1", "upload-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.ExtendLease(ctx, "p1", "a", "v1", 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "p1", "a")
	if !got.ClaimedUntil.After(r.ClaimedUntil) || got.UploadID != "upload-1" || got.UploadVersion != "v1" {
		t.Fatalf("after extend/setupload: %+v", got)
	}
	// Wrong version is not the lease holder.
	if err := s.Complete(ctx, "p1", "a", "v0", record.SyncResult{}); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("Complete wrong version = %v, want ErrLeaseLost", err)
	}

	syncedAt := t0.Add(time.Hour)
	if err := s.Complete(ctx, "p1", "a", "v1", record.SyncResult{SHA256: []byte{1, 2, 3}, DestVersion: "d1", SyncedAt: syncedAt}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, "p1", "a")
	if got.Status != record.StatusSynced || got.SyncedVersion != "v1" || got.SyncedSequencer != "0A" ||
		!got.SyncedMTime.Equal(t0) || got.DestVersion != "d1" || !got.SyncedAt.Equal(syncedAt) ||
		!got.ClaimedUntil.IsZero() || got.UploadID != "" || len(got.SHA256) != 3 {
		t.Fatalf("after Complete: %+v", got)
	}
	if err := s.Complete(ctx, "p1", "a", "v1", record.SyncResult{}); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("second Complete = %v, want ErrLeaseLost", err)
	}

	mustClaim(t, s, c, time.Minute, record.DecisionAlreadySynced)
	older := cand("a", "v0", t0.Add(-time.Hour))
	older.Sequencer = "09"
	mustClaim(t, s, older, time.Minute, record.DecisionStale)
	newer := cand("a", "v2", t0.Add(-time.Hour)) // older mtime but newer sequencer
	newer.Sequencer = "0B"
	r = mustClaim(t, s, newer, time.Minute, record.DecisionCopy)
	if r.Attempts != 1 {
		t.Fatalf("attempts for a new version = %d, want 1", r.Attempts)
	}
	// Same version while held → busy (no parallel duplicate copy).
	mustClaim(t, s, newer, time.Minute, record.DecisionBusy)
}

func TestClaimConcurrentExactlyOneCopy(t *testing.T) {
	s, _ := newStore(t)
	c := cand("hot", "v1", t0)
	const n = 20
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		count = map[record.Decision]int{}
		start = make(chan struct{})
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, _, err := s.Claim(context.Background(), c, time.Minute)
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			mu.Lock()
			count[d]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if count[record.DecisionCopy] != 1 || count[record.DecisionBusy] != n-1 {
		t.Fatalf("decisions = %v, want 1 copy and %d busy", count, n-1)
	}
}

func TestLeaseExpiryTakeover(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	c := cand("k", "v1", t0)

	mustClaim(t, s, c, 300*time.Millisecond, record.DecisionCopy)
	if err := s.SetUpload(ctx, "p1", "k", "v1", "mpu-1"); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, s, c, time.Minute, record.DecisionBusy)
	time.Sleep(400 * time.Millisecond)

	// Worker A's lease expired: everything it tries now fails.
	if err := s.ExtendLease(ctx, "p1", "k", "v1", time.Minute); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("ExtendLease after expiry = %v, want ErrLeaseLost", err)
	}

	// Worker B takes over and sees the upload session to resume.
	r := mustClaim(t, s, c, time.Minute, record.DecisionCopy)
	if r.Attempts != 2 || r.UploadID != "mpu-1" || r.UploadVersion != "v1" {
		t.Fatalf("takeover record = %+v", r)
	}

	// Note: the lease is identified by (key, version) only, so a stale holder
	// of the SAME version is indistinguishable from the taker-over (the
	// interface has no claim token). Both copy the same bytes, so that is
	// safe. A lease that expired and was not taken over is rejected:
	mustClaim(t, s, cand("k2", "v1", t0), 200*time.Millisecond, record.DecisionCopy)
	time.Sleep(300 * time.Millisecond)
	if err := s.Complete(ctx, "p1", "k2", "v1", record.SyncResult{}); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("Complete after expiry = %v, want ErrLeaseLost", err)
	}
	if err := s.Fail(ctx, "p1", "k2", "v1", errors.New("x"), true); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("Fail after expiry = %v, want ErrLeaseLost", err)
	}
	if err := s.SetUpload(ctx, "p1", "k2", "v1", "u"); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("SetUpload after expiry = %v, want ErrLeaseLost", err)
	}

	// A takeover for a newer version makes the old holder's Complete fail.
	mustClaim(t, s, cand("k3", "v1", t0), 200*time.Millisecond, record.DecisionCopy)
	time.Sleep(300 * time.Millisecond)
	mustClaim(t, s, cand("k3", "v2", t0.Add(time.Minute)), time.Minute, record.DecisionCopy)
	if err := s.Complete(ctx, "p1", "k3", "v1", record.SyncResult{}); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("stale Complete after newer takeover = %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, "p1", "k3", "v2", record.SyncResult{}); err != nil {
		t.Fatal(err)
	}
}

func TestObserveNeverRegresses(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	get := func() record.Record {
		r, err := s.Get(ctx, "p1", "o")
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	if err := s.Observe(ctx, cand("o", "v2", t0.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.SourceVersion != "v2" || r.Status != record.StatusPending || r.SourceSize != 100 {
		t.Fatalf("after first Observe: %+v", r)
	}
	if err := s.Observe(ctx, cand("o", "v1", t0)); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.SourceVersion != "v2" {
		t.Fatalf("Observe regressed to %s", r.SourceVersion)
	}

	// Sequencers outrank mtime once both sides have one.
	a := cand("o", "v3", t0.Add(2*time.Minute)) // newer mtime than v2 (no sequencer there)
	a.Sequencer = "100"
	if err := s.Observe(ctx, a); err != nil {
		t.Fatal(err)
	}
	b := cand("o", "v4", t0.Add(time.Hour))
	b.Sequencer = "FF" // 0FF < 100
	if err := s.Observe(ctx, b); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.SourceVersion != "v3" {
		t.Fatalf("source = %s, want v3 (sequencer ordering)", r.SourceVersion)
	}
	// Lag keeps the oldest unsynced event time.
	if r := get(); !r.EventTime.Equal(t0.Add(time.Minute)) {
		t.Fatalf("event_time = %v, want oldest pending %v", r.EventTime, t0.Add(time.Minute))
	}

	// While copying, Observe leaves the lease holder's fields alone.
	mustClaim(t, s, a, time.Minute, record.DecisionCopy)
	c := cand("o", "v5", t0)
	c.Sequencer = "200"
	if err := s.Observe(ctx, c); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.SourceVersion != "v3" || r.Status != record.StatusCopying {
		t.Fatalf("Observe during copy changed record: %+v", r)
	}
	if err := s.Complete(ctx, "p1", "o", "v3", record.SyncResult{}); err != nil {
		t.Fatal(err)
	}
	// Re-observing the synced version keeps it synced; a newer one → pending.
	if err := s.Observe(ctx, a); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != record.StatusSynced {
		t.Fatalf("status = %s, want synced", r.Status)
	}
	if err := s.Observe(ctx, c); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Status != record.StatusPending || r.SourceVersion != "v5" || r.SyncedVersion != "v3" {
		t.Fatalf("after newer Observe: %+v", r)
	}
}

func TestFailDeleteAndStats(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	mustClaim(t, s, cand("ok", "v1", t0), time.Minute, record.DecisionCopy)
	if err := s.Complete(ctx, "p1", "ok", "v1", record.SyncResult{SyncedAt: t0.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	mustClaim(t, s, cand("bad", "v1", t0), time.Minute, record.DecisionCopy)
	if err := s.Fail(ctx, "p1", "bad", "v1", errors.New("permission denied"), false); err != nil {
		t.Fatal(err)
	}
	r := mustClaim(t, s, cand("bad", "v1", t0), time.Minute, record.DecisionCopy) // failed retries
	if r.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", r.Attempts)
	}
	if err := s.Fail(ctx, "p1", "bad", "v1", errors.New("still denied"), false); err != nil {
		t.Fatal(err)
	}

	mustClaim(t, s, cand("flaky", "v1", t0.Add(-time.Minute)), time.Minute, record.DecisionCopy)
	if err := s.Fail(ctx, "p1", "flaky", "v1", errors.New("throttled"), true); err != nil {
		t.Fatal(err)
	}

	mustClaim(t, s, cand("busy", "v1", t0), time.Minute, record.DecisionCopy)
	if err := s.Observe(ctx, cand("pend", "v1", t0)); err != nil {
		t.Fatal(err)
	}

	st, err := s.Stats(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Synced != 1 || st.Failed != 1 || st.Pending != 2 || st.Copying != 1 || st.BytesSynced != 100 {
		t.Fatalf("stats = %+v", st)
	}
	if !st.LastSyncedAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("LastSyncedAt = %v", st.LastSyncedAt)
	}
	if !st.OldestPendingEvent.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("OldestPendingEvent = %v", st.OldestPendingEvent)
	}
	if len(st.RecentErrors) != 2 || st.RecentErrors[0].Key != "flaky" || st.RecentErrors[1].LastError != "still denied" {
		t.Fatalf("RecentErrors = %+v", st.RecentErrors)
	}
	if other, _ := s.Stats(ctx, "other"); other.Synced != 0 || len(other.RecentErrors) != 0 {
		t.Fatalf("stats leaked across pipelines: %+v", other)
	}

	// Delete, then re-create with the same version (S3 ETag of identical
	// content): copied again.
	if err := s.MarkDeleted(ctx, "p1", "ok", t0.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Get(ctx, "p1", "ok"); r.Status != record.StatusDeleted {
		t.Fatalf("status = %s, want deleted", r.Status)
	}
	mustClaim(t, s, cand("ok", "v1", t0), time.Minute, record.DecisionCopy)

	// A delete older than the known version is ignored; one during a copy
	// revokes the lease.
	if err := s.MarkDeleted(ctx, "p1", "busy", t0.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Get(ctx, "p1", "busy"); r.Status != record.StatusCopying {
		t.Fatalf("old delete applied: %s", r.Status)
	}
	if err := s.MarkDeleted(ctx, "p1", "busy", t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, "p1", "busy", "v1", record.SyncResult{}); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("Complete after delete = %v, want ErrLeaseLost", err)
	}
	// Delete of a never-seen key is tracked.
	if err := s.MarkDeleted(ctx, "p1", "ghost", t0); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Get(ctx, "p1", "ghost"); err != nil || r.Status != record.StatusDeleted {
		t.Fatalf("ghost = %+v, %v", r, err)
	}
}

func TestListByteOrder(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	keys := []string{"b", "a/b", "a", "B", "a-b", "é", "Z", "a b", "aa"}
	for _, k := range keys {
		if err := s.Observe(ctx, cand(k, "v1", t0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Observe(ctx, record.Candidate{PipelineID: "p2", Key: "a0", Version: "v1"}); err != nil {
		t.Fatal(err)
	}
	want := append([]string(nil), keys...)
	sort.Strings(want) // Go sorts by bytes, like object stores

	var got []string
	after := ""
	for {
		page, err := s.List(ctx, "p1", after, 4)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			got = append(got, r.Key)
		}
		after = page[len(page)-1].Key
	}
	if len(got) != len(want) {
		t.Fatalf("List = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List = %q, want %q", got, want)
		}
	}
}
