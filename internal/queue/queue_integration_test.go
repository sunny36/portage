//go:build integration

package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/queue"
	"github.com/sunny36/portage/internal/record/pgtest"
)

type ran struct{ queue, key, version string }

// testCopyWorker records what ran where; jobs for version "block" wait until
// release is closed.
type testCopyWorker struct {
	river.WorkerDefaults[queue.CopyArgs]
	mu      sync.Mutex
	ran     []ran
	started chan struct{}
	release chan struct{}
}

func (w *testCopyWorker) Work(ctx context.Context, job *river.Job[queue.CopyArgs]) error {
	if job.Args.Version == "block" {
		close(w.started)
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.mu.Lock()
	w.ran = append(w.ran, ran{job.Queue, job.Args.Key, job.Args.Version})
	w.mu.Unlock()
	return nil
}

type testReconcileWorker struct {
	river.WorkerDefaults[queue.ReconcileArgs]
	done chan string
}

func (w *testReconcileWorker) Work(ctx context.Context, job *river.Job[queue.ReconcileArgs]) error {
	w.done <- job.Queue
	return nil
}

func countJobs(t *testing.T, pool *pgxpool.Pool, queueName string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM river_job WHERE queue = $1`, queueName).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestInsertDedupeAndRouting(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.NewPool(t)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := queue.Migrate(ctx, pool); err != nil {
				t.Errorf("Migrate: %v", err)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	pipelines := []config.Pipeline{{Name: "p1", Concurrency: 2}, {Name: "p-2", Concurrency: 1}}
	workers := river.NewWorkers()
	cw := &testCopyWorker{started: make(chan struct{}), release: make(chan struct{})}
	rw := &testReconcileWorker{done: make(chan string, 1)}
	river.AddWorker(workers, cw)
	river.AddWorker(workers, rw)
	client, err := queue.NewClient(pool, workers, queue.QueuesFor(pipelines))
	if err != nil {
		t.Fatal(err)
	}

	ins := func(pipeline, key, version string) {
		t.Helper()
		if err := queue.InsertCopy(ctx, client, queue.CopyArgs{
			PipelineID: pipeline, Key: key, Version: version,
			EventTime: time.Now(), DetectedAt: time.Now(), Origin: "test",
		}); err != nil {
			t.Fatalf("InsertCopy: %v", err)
		}
	}

	// Before the client starts, everything stays available.
	ins("p1", "k", "v1")
	ins("p1", "k", "v1") // duplicate: collapsed
	ins("p1", "k", "v2") // newer version: its own job
	ins("p1", "other", "v1")
	ins("p-2", "k", "v1") // another pipeline: its own queue
	if n := countJobs(t, pool, "copy_p1"); n != 3 {
		t.Fatalf("copy_p1 jobs = %d, want 3", n)
	}
	if n := countJobs(t, pool, "copy_p-2"); n != 1 {
		t.Fatalf("copy_p-2 jobs = %d, want 1", n)
	}

	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	})

	// A running job blocks a duplicate of its own version but not a newer one.
	ins("p1", "big", "block")
	select {
	case <-cw.started:
	case <-time.After(15 * time.Second):
		t.Fatal("blocking job never started")
	}
	ins("p1", "big", "block") // duplicate of the running job
	ins("p1", "big", "v9")    // newer version while the old one runs
	var runningDupes int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM river_job WHERE args->>'key' = 'big' AND args->>'version' = 'block'`).Scan(&runningDupes); err != nil {
		t.Fatal(err)
	}
	if runningDupes != 1 {
		t.Fatalf("jobs for running version = %d, want 1", runningDupes)
	}

	// The same key, version and generation in a later dedupe window is
	// queued even though the first job is still 'running': a job stranded
	// before any claim must not block its key indefinitely (soak finding).
	if err := queue.InsertCopy(ctx, client, queue.CopyArgs{
		PipelineID: "p1", Key: "big", Version: "block", Window: 1 << 40,
		EventTime: time.Now(), DetectedAt: time.Now(), Origin: "test",
	}); err != nil {
		t.Fatalf("InsertCopy next window: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM river_job WHERE args->>'key' = 'big' AND args->>'version' = 'block'`).Scan(&runningDupes); err != nil {
		t.Fatal(err)
	}
	if runningDupes != 2 {
		t.Fatalf("jobs for running version after the window moved = %d, want 2", runningDupes)
	}

	if _, err := client.Insert(ctx, queue.ReconcileArgs{PipelineID: "p1"}, &river.InsertOpts{Queue: queue.QueueReconcile}); err != nil {
		t.Fatal(err)
	}
	select {
	case q := <-rw.done:
		if q != queue.QueueReconcile {
			t.Fatalf("reconcile ran on %q", q)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconcile job never ran")
	}

	close(cw.release)

	want := map[ran]bool{
		{"copy_p1", "k", "v1"}: true, {"copy_p1", "k", "v2"}: true, {"copy_p1", "other", "v1"}: true,
		{"copy_p-2", "k", "v1"}: true, {"copy_p1", "big", "block"}: true, {"copy_p1", "big", "v9"}: true,
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		cw.mu.Lock()
		got := append([]ran(nil), cw.ran...)
		cw.mu.Unlock()
		if len(got) >= len(want) || time.Now().After(deadline) {
			if len(got) != len(want) {
				t.Fatalf("ran %d jobs %v, want %d", len(got), got, len(want))
			}
			for _, r := range got {
				if !want[r] {
					t.Fatalf("unexpected job %+v (all: %v)", r, got)
				}
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Once a version's job has completed, the same version may be enqueued
	// again (the file record then answers already_synced). River marks jobs
	// completed asynchronously, so wait for that first.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		var state string
		if err := pool.QueryRow(ctx,
			`SELECT state FROM river_job WHERE args->>'key' = 'k' AND args->>'version' = 'v1' AND queue = 'copy_p1'`).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job state = %s, never completed", state)
		}
	}
	ins("p1", "k", "v1")
	var avail int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM river_job WHERE args->>'key' = 'k' AND args->>'version' = 'v1' AND queue = 'copy_p1'`).Scan(&avail); err != nil {
		t.Fatal(err)
	}
	if avail != 2 {
		t.Fatalf("jobs for completed version after re-insert = %d, want 2", avail)
	}
}
