//go:build integration

package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/testenv"
)

// Failure-mode tests: the real engine against Azurite -> SeaweedFS with a
// fresh database each, and faults injected through connectorFactory (see
// faults_test.go). Configs are built directly as config.File, bypassing
// validation, so reconcile intervals can be seconds.
//
// Every test runs in parallel: each has its own database, container, bucket
// and queue, and fault injection is keyed by container/bucket name.

// realLease makes the crash/resume test wait for the 2-minute claim lease to
// lapse on its own instead of fast-forwarding it in the database.
var realLease = os.Getenv("PORTAGE_TEST_REAL_LEASE") != ""

// --- harness ----------------------------------------------------------------

type harness struct {
	t                 *testing.T
	dbURL             string
	container, bucket string
	queue             string              // "" when the test uses no event queue
	src               connector.Connector // container-scoped: keys carry "in/"
	dst               connector.Connector // scoped to "out/": keys as synced
	srcF, dstF        *faults
	logs              *logSink

	poolOnce sync.Once
	pool     *pgxpool.Pool
}

func newHarness(t *testing.T, withQueue bool) *harness {
	t.Helper()
	h := &harness{t: t, logs: newLogSink(t)}
	h.dbURL = freshDatabase(t)
	h.container, h.bucket = newContainer(t), newBucket(t)
	if withQueue {
		h.queue = newEventQueue(t)
	}
	h.srcF = injectFaults(t, h.container)
	h.dstF = injectFaults(t, h.bucket)
	ctx := context.Background()
	var err error
	if h.src, err = azure.New(ctx, h.azure(), ""); err != nil {
		t.Fatal(err)
	}
	if h.dst, err = s3.New(ctx, h.s3(h.bucket), "out/"); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) azure() config.AzureConfig {
	return config.AzureConfig{
		AccountURL: testenv.AzuriteBlobURL(), Container: h.container,
		Auth: "connection_string", ConnectionString: testenv.AzuriteConnectionString(),
	}
}

func (h *harness) s3(bucket string) config.S3Config {
	return config.S3Config{
		Bucket: bucket, Region: testenv.S3Region, Endpoint: testenv.S3Endpoint(),
		PathStyle: true, Flavor: "generic",
		AccessKeyID: testenv.S3AccessKeyID, SecretAccessKey: testenv.S3SecretAccessKey,
	}
}

// pipeline returns a pipeline named name syncing <container>/in/ to
// <bucket>/out/, with events from the harness queue if it has one.
func (h *harness) pipeline(name string, mod func(*config.Pipeline)) config.Pipeline {
	az, s := h.azure(), h.s3(h.bucket)
	p := config.Pipeline{
		Name:              name,
		Source:            config.Endpoint{Prefix: "in/", Azure: &az},
		Destination:       config.Endpoint{Prefix: "out/", S3: &s},
		Events:            config.Events{Type: "none"},
		ReconcileInterval: 2 * time.Second,
		ExistingFiles:     "copy",
		Concurrency:       4,
		PartConcurrency:   2,
		PartSize:          5 << 20,
	}
	if h.queue != "" {
		p.Events = config.Events{Type: "azure_queue", QueueName: h.queue}
	}
	if mod != nil {
		mod(&p)
	}
	return p
}

func (h *harness) config(ps ...config.Pipeline) *config.File {
	return &config.File{Version: 1, DatabaseURL: h.dbURL, Pipelines: ps}
}

func (h *harness) db() *pgxpool.Pool {
	h.poolOnce.Do(func() {
		h.pool = mustPool(h.t, h.dbURL)
		h.t.Cleanup(h.pool.Close)
	})
	return h.pool
}

// record returns the file record, or ok=false if there is none yet.
func (h *harness) record(pipelineID, key string) (record.Record, bool) {
	r, err := record.NewPGStore(h.db()).Get(context.Background(), pipelineID, key)
	if err != nil {
		return record.Record{}, false // no row, or tables not migrated yet
	}
	return r, true
}

func (h *harness) stats(pipelineID string) record.Stats {
	h.t.Helper()
	st, err := record.NewPGStore(h.db()).Stats(context.Background(), pipelineID)
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// riverJob is one row of river_job for a key.
type riverJob struct {
	ID       int64
	State    string
	Attempt  int
	Version  string
	Finished bool
}

func (h *harness) copyJobs(key string) []riverJob {
	h.t.Helper()
	rows, err := h.db().Query(context.Background(), `
		SELECT id, state, attempt, coalesce(args->>'version', ''), finalized_at IS NOT NULL
		FROM river_job WHERE kind = 'copy' AND args->>'key' = $1 ORDER BY id`, key)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rows.Close()
	var out []riverJob
	for rows.Next() {
		var j riverJob
		if err := rows.Scan(&j.ID, &j.State, &j.Attempt, &j.Version, &j.Finished); err != nil {
			h.t.Fatal(err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		h.t.Fatal(err)
	}
	return out
}

// unfinishedJobs counts copy/delete jobs River has not finalised.
func (h *harness) unfinishedJobs() int {
	h.t.Helper()
	var n int
	err := h.db().QueryRow(context.Background(), `
		SELECT count(*) FROM river_job WHERE kind IN ('copy', 'delete') AND finalized_at IS NULL`).Scan(&n)
	if err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) queueDepth() int32 {
	h.t.Helper()
	p, err := queueService(h.t).NewQueueClient(h.queue).GetProperties(context.Background(), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if p.ApproximateMessagesCount == nil {
		return 0
	}
	return *p.ApproximateMessagesCount
}

// destContent returns the destination object's bytes, or nil, false if absent.
func destContent(t *testing.T, dst connector.Connector, key string) ([]byte, bool) {
	t.Helper()
	rc, err := dst.OpenRange(context.Background(), key, "", 0, -1)
	if errors.Is(err, connector.ErrNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("read dest %q: %v", key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read dest %q: %v", key, err)
	}
	return b, true
}

// --- engine -----------------------------------------------------------------

type engine struct {
	t      *testing.T
	cancel context.CancelFunc
	exited chan struct{} // closed when Run has returned
	err    error         // Run's result, valid once exited is closed
	once   sync.Once
}

// start runs the engine in the background. Cleanup cancels it and waits.
func (h *harness) start(cfg *config.File) *engine {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e := &engine{t: h.t, cancel: cancel, exited: make(chan struct{})}
	log := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	go func() {
		e.err = Run(ctx, cfg, nil, log)
		close(e.exited)
	}()
	h.t.Cleanup(func() { e.wait(false) })
	return e
}

// stop cancels the engine and waits for Run to return nil (graceful stop).
func (e *engine) stop() {
	e.t.Helper()
	e.wait(true)
}

// abandon cancels the engine without waiting for it: the test carries on as
// if the process had died. Cleanup still waits for Run to return.
func (e *engine) abandon() { e.cancel() }

// engineStopTimeout covers Run's shutdown: shutdownTimeout for running
// copies, then up to 10s for StopAndCancel.
const engineStopTimeout = shutdownTimeout + 30*time.Second

func (e *engine) wait(check bool) {
	e.once.Do(func() {
		e.cancel()
		select {
		case <-e.exited:
			if check && e.err != nil {
				e.t.Errorf("Run returned %v on shutdown, want nil", e.err)
			}
		case <-time.After(engineStopTimeout):
			e.t.Error("engine did not stop")
		}
	})
}

// --- logs -------------------------------------------------------------------

// logSink tees engine logs to the test output and keeps them for assertions.
type logSink struct {
	t    *testing.T
	mu   sync.Mutex
	buf  bytes.Buffer
	done bool
}

func newLogSink(t *testing.T) *logSink {
	s := &logSink{t: t}
	// Registered first, so it runs after every engine has stopped.
	t.Cleanup(func() {
		s.mu.Lock()
		s.done = true
		s.mu.Unlock()
	})
	return s
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Write(p)
	if !s.done {
		_, _ = s.t.Output().Write(p)
	}
	return len(p), nil
}

// lines returns the log lines containing every one of subs.
func (s *logSink) lines(subs ...string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for l := range strings.SplitSeq(s.buf.String(), "\n") {
		ok := true
		for _, sub := range subs {
			if !strings.Contains(l, sub) {
				ok = false
				break
			}
		}
		if ok && l != "" {
			out = append(out, l)
		}
	}
	return out
}

// --- events -----------------------------------------------------------------

type blobEvent struct {
	Type      string // "Microsoft.Storage.BlobCreated" / "...BlobDeleted"
	Name      string // blob name in the container
	ETag      string
	Size      int64
	Sequencer string
	Time      time.Time
	ID        string
}

var eventSeq atomic.Int64

// newSequencer returns an Azure-style sequencer that orders after every
// earlier call in this process (what the service assigns at write time).
func newSequencer() string {
	return fmt.Sprintf("%032x", time.Now().UnixNano()*100+eventSeq.Add(1)%100)
}

// sendEvent enqueues ev exactly as Event Grid delivers it to a Storage Queue.
func (h *harness) sendEvent(ev blobEvent) {
	h.t.Helper()
	if ev.ID == "" {
		ev.ID = fmt.Sprintf("fail-%d", eventSeq.Add(1))
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	blobURL := testenv.AzuriteBlobURL() + "/" + h.container + "/" + (&url.URL{Path: ev.Name}).EscapedPath()
	data := map[string]any{"url": blobURL, "sequencer": ev.Sequencer, "blobType": "BlockBlob"}
	if ev.Type == "Microsoft.Storage.BlobCreated" {
		data["api"], data["eTag"], data["contentLength"] = "PutBlob", ev.ETag, ev.Size
	} else {
		data["api"] = "DeleteBlob"
	}
	raw, err := json.Marshal(map[string]any{
		"id":        ev.ID,
		"topic":     "/subscriptions/x/resourceGroups/x/providers/Microsoft.Storage/storageAccounts/devstoreaccount1",
		"subject":   "/blobServices/default/containers/" + h.container + "/blobs/" + ev.Name,
		"eventType": ev.Type,
		"eventTime": ev.Time.UTC().Format(time.RFC3339Nano),
		"data":      data, "dataVersion": "", "metadataVersion": "1",
	})
	if err != nil {
		h.t.Fatal(err)
	}
	msg := base64.StdEncoding.EncodeToString(raw)
	if _, err := queueService(h.t).NewQueueClient(h.queue).EnqueueMessage(context.Background(), msg, nil); err != nil {
		h.t.Fatal(err)
	}
}

func created(name, etag string, size int64, seq string) blobEvent {
	return blobEvent{Type: "Microsoft.Storage.BlobCreated", Name: name, ETag: etag, Size: size, Sequencer: seq}
}

// --- polling ----------------------------------------------------------------

// eventually polls cond every interval until it returns true or timeout
// passes, then fails the test with cond's last message.
func eventually(t *testing.T, timeout, interval time.Duration, cond func() (bool, string)) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	var msg string
	for {
		var ok bool
		if ok, msg = cond(); ok {
			return time.Since(start)
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met after %s: %s", timeout, msg)
		}
		time.Sleep(interval)
	}
}

func syncedTo(t *testing.T, dst connector.Connector, key string, want []byte) func() (bool, string) {
	return func() (bool, string) {
		got, ok := destContent(t, dst, key)
		if !ok {
			return false, key + " absent at destination"
		}
		if sha256.Sum256(got) != sha256.Sum256(want) {
			return false, fmt.Sprintf("%s at destination has the wrong content (%d bytes, want %d)", key, len(got), len(want))
		}
		return true, ""
	}
}

// recordSynced is a condition: the key's file record is synced.
func (h *harness) recordSynced(pipelineID, key string) func() (bool, string) {
	return func() (bool, string) {
		r, _ := h.record(pipelineID, key)
		return r.Status == record.StatusSynced, fmt.Sprintf("%s record status %q", key, r.Status)
	}
}

// ============================================================================
// a. Crash mid-multipart -> resume
// ============================================================================

// TestFailureCrashMidMultipartResumes hangs the destination's 4th UploadPart
// of a 6-part object, takes the engine down there, starts a new one and
// checks that it finishes the same multipart session, re-uploading only the
// missing parts.
//
//   - graceful-stop: ctx cancelled (SIGTERM) while the part upload hangs.
//     Run waits shutdownTimeout for the copy, then cancels it; the
//     interrupted copy releases its lease, so the restart resumes at once.
//   - abandoned: the in-flight request never returns, even when cancelled
//     (kill -9 / node loss), so the dead worker never releases its lease and
//     its River job stays 'running'. The restart must wait for the lease to
//     lapse; then a reconcile pass re-queues the key (a new CopyArgs
//     generation, so the orphaned job does not dedupe it) and the copy
//     resumes. By default the test waits for the dead engine to be fully
//     gone (no more heartbeats), proves the key is still blocked, and
//     fast-forwards the lease in the database; PORTAGE_TEST_REAL_LEASE=1
//     waits for it to lapse on its own and logs the real latency.
func TestFailureCrashMidMultipartResumes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		abandon bool
	}{
		{name: "graceful-stop"},
		{name: "abandoned", abandon: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCrashResume(t, tc.abandon)
		})
	}
}

func testCrashResume(t *testing.T, abandon bool) {
	h := newHarness(t, false)
	const (
		pipe  = "crash"
		key   = "big.bin"
		size  = 30 << 20
		parts = 6
		done  = 3 // parts uploaded before the crash
	)
	data := randBytes(t, size)
	putBlob(t, h.src, "in/"+key, data)
	cfg := h.config(h.pipeline(pipe, func(p *config.Pipeline) {
		p.Concurrency, p.PartConcurrency = 1, 1
	}))

	// The hang is never released: in the abandoned case the blocked call
	// belongs to a "dead" process and must never complete.
	hung, _ := h.dstF.HangAfter(opUploadPart, done, !abandon)
	e1 := h.start(cfg)
	select {
	case <-hung:
	case <-time.After(90 * time.Second):
		t.Fatal("copy never reached the hanging UploadPart")
	}
	rec, _ := h.record(pipe, key)
	if rec.Status != record.StatusCopying || rec.UploadID == "" {
		t.Fatalf("record before crash = status %q upload %q; want copying with an upload id", rec.Status, rec.UploadID)
	}
	uploadID := rec.UploadID
	callsRun1 := h.dstF.Calls(opUploadPart)
	jobsBefore := h.copyJobs(key)
	h.dstF.StopHanging(opUploadPart) // the next "process" has a healthy destination

	crashedAt := time.Now()
	if abandon {
		e1.abandon()
	} else {
		e1.stop()
		t.Logf("graceful stop took %s (shutdownTimeout %s)", time.Since(crashedAt).Round(time.Millisecond), shutdownTimeout)
		rec, _ = h.record(pipe, key)
		if rec.Status == record.StatusCopying {
			t.Errorf("after a graceful stop the key is still claimed (lease for another %s); want the lease released",
				time.Until(rec.ClaimedUntil).Round(time.Second))
		}
		if rec.UploadID != uploadID {
			t.Errorf("upload session after stop = %q, want %q kept for resume", rec.UploadID, uploadID)
		}
	}

	// Run 1 is stopped (graceful) or can never finish another part (its
	// hung call never returns), so later successful parts are run 2's.
	partsRun1 := h.dstF.PartsUploaded()
	h.start(cfg)
	restartedAt := time.Now()

	timeout := 60 * time.Second
	if abandon {
		if realLease {
			timeout = engineStopTimeout + leaseDuration + time.Minute
		} else {
			// Wait until the dead engine is gone for good, so no heartbeat
			// can renew the lease after it is fast-forwarded.
			select {
			case <-e1.exited:
			case <-time.After(engineStopTimeout):
				t.Fatal("abandoned engine never returned")
			}
			rec, _ = h.record(pipe, key)
			if rec.Status != record.StatusCopying {
				t.Fatalf("abandoned copy: record is %s, want still copying (the dead worker never released it)", rec.Status)
			}
			if ok, _ := syncedTo(t, h.dst, key, data)(); ok {
				t.Fatal("synced while the dead worker's lease was live")
			}
			t.Logf("%s after the crash the key is still claimed by the dead worker (lease for another %s); fast-forwarding it",
				time.Since(crashedAt).Round(time.Second), time.Until(rec.ClaimedUntil).Round(time.Second))
			if _, err := h.db().Exec(context.Background(), `UPDATE file_record SET claimed_until = clock_timestamp()
				WHERE pipeline_id = $1 AND key = $2 AND status = 'copying'`, pipe, key); err != nil {
				t.Fatal(err)
			}
		}
	}
	resumeFrom := time.Now()
	eventually(t, timeout, 250*time.Millisecond, syncedTo(t, h.dst, key, data))
	eventually(t, 10*time.Second, 50*time.Millisecond, h.recordSynced(pipe, key))
	t.Logf("synced %s after the crash, %s after restart, %s after the lease was free (lease %s, reconcile every %s)",
		time.Since(crashedAt).Round(100*time.Millisecond), time.Since(restartedAt).Round(100*time.Millisecond),
		time.Since(resumeFrom).Round(100*time.Millisecond), leaseDuration, cfg.Pipelines[0].ReconcileInterval)
	if !abandon {
		// Nothing to wait for: the lease was released on shutdown. Typically
		// 1-4s; the bound only has to tell that apart from waiting out the
		// lease, and tolerate a stalled shared Postgres.
		if took := time.Since(restartedAt); took > leaseDuration/2 {
			t.Errorf("restart took %s to resume after a graceful stop; want well under the %s lease", took, leaseDuration)
		}
	}

	rec, _ = h.record(pipe, key)
	if rec.Status != record.StatusSynced {
		t.Errorf("record status = %s, want synced", rec.Status)
	}
	if want := sha256.Sum256(data); !bytes.Equal(rec.SHA256, want[:]) {
		t.Errorf("record sha256 = %x, want %x", rec.SHA256, want)
	}
	total := h.dstF.Calls(opUploadPart)
	all := h.dstF.PartsUploaded()
	run2 := all[len(partsRun1):]
	t.Logf("UploadPart: %d calls in all (run 1: %d before the crash, %d ok + 1 hung); parts uploaded: run 1 %v, run 2 %v; "+
		"begin=%d resume=%d; jobs before %+v after %+v",
		total, callsRun1, done, partsRun1, run2,
		h.dstF.Calls(opBeginUpload), h.dstF.Calls(opResumeUpload), jobsBefore, h.copyJobs(key))
	if fmt.Sprint(partsRun1) != "[1 2 3]" {
		t.Errorf("run 1 uploaded parts %v, want [1 2 3]", partsRun1)
	}
	if fmt.Sprint(run2) != "[4 5 6]" {
		t.Errorf("restart uploaded parts %v, want [4 5 6] (parts 1-3 reused from the session)", run2)
	}
	// Count successful uploads, not calls: the shared emulator sometimes
	// answers 500 under parallel load, and those retried calls are not
	// re-uploads of finished parts.
	if len(all) != parts {
		t.Errorf("parts uploaded successfully = %d (%v), want %d: each part exactly once", len(all), all, parts)
	}
	if h.dstF.Calls(opResumeUpload) == 0 {
		t.Error("restart never resumed the multipart session")
	}
	if n := h.dstF.Calls(opBeginUpload); n != 1 {
		t.Errorf("BeginUpload called %d times, want 1 (the original session reused)", n)
	}
	if abandon {
		// The dead worker's job is orphaned in 'running'; the copy above
		// must have come from a newer job.
		orphaned := false
		for _, j := range h.copyJobs(key) {
			if j.State == "running" {
				orphaned = true
			}
		}
		if !orphaned {
			t.Log("note: no orphaned 'running' job left behind")
		}
	}
}

// ============================================================================
// b. Dropped events -> reconciler catches up
// ============================================================================

func TestFailureDroppedEventsReconcilerCatchesUp(t *testing.T) {
	t.Parallel()
	for _, withQueue := range []bool{false, true} {
		name := "events-none"
		if withQueue {
			name = "queue-event-dropped"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, withQueue)
			const interval = 2 * time.Second
			cfg := h.config(h.pipeline("dropped", func(p *config.Pipeline) { p.ReconcileInterval = interval }))
			first := randBytes(t, 1000)
			putBlob(t, h.src, "in/first.txt", first)
			h.start(cfg)
			eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "first.txt", first))

			// No event is sent for this one (or it was lost in transit).
			late := randBytes(t, 2000)
			putBlob(t, h.src, "in/late.txt", late)
			took := eventually(t, 30*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "late.txt", late))
			t.Logf("blob with no event synced after %s (reconcile interval %s)", took.Round(10*time.Millisecond), interval)
			// Two intervals plus the copy itself; generous for -race and
			// parallel tests sharing the emulators.
			if limit := 2*interval + 5*time.Second; took > limit {
				t.Errorf("reconciler took %s to pick up a missed blob, want <= %s", took, limit)
			}
			// The object lands before the record is completed.
			eventually(t, 10*time.Second, 50*time.Millisecond, h.recordSynced("dropped", "late.txt"))
		})
	}
}

// ============================================================================
// c. Rapid overwrites, newest wins
// ============================================================================

func TestFailureRapidOverwritesNewestWins(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	const key = "hot.bin"
	cfg := h.config(h.pipeline("overwrite", func(p *config.Pipeline) {
		// Only the (empty) on-start reconcile: ordering is up to the events.
		p.ReconcileInterval = time.Hour
	}))
	h.start(cfg)
	eventually(t, 30*time.Second, 100*time.Millisecond, func() (bool, string) {
		return len(h.logs.lines("reconcile finished", "overwrite")) > 0, "initial reconcile not finished"
	})

	// Versions alternate between multipart (6 MiB, 2 parts) and single put,
	// so copies of different lengths race.
	const n = 5
	versions := make([][]byte, n+1) // 1-based
	byHash := map[[32]byte]int{}
	for i := 1; i <= n; i++ {
		size := 6<<20 + i
		if i%2 == 0 {
			size = 300_000 + i
		}
		versions[i] = randBytes(t, size)
		byHash[sha256.Sum256(versions[i])] = i
	}

	// Watch the destination the whole time.
	var (
		mu       sync.Mutex
		observed []int // version index per change seen at the destination
	)
	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		last := -1
		for {
			select {
			case <-stopWatch:
				return
			default:
			}
			rc, err := h.dst.OpenRange(context.Background(), key, "", 0, -1)
			v := 0
			if err == nil {
				b, rerr := io.ReadAll(rc)
				rc.Close()
				if rerr != nil {
					continue
				}
				var ok bool
				if v, ok = byHash[sha256.Sum256(b)]; !ok {
					v = -2 // torn or unknown content
				}
			} else if !errors.Is(err, connector.ErrNotFound) {
				continue
			}
			if v != last {
				mu.Lock()
				observed = append(observed, v)
				mu.Unlock()
				last = v
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	etags := make([]string, n+1)
	seqs := make([]string, n+1)
	write := func(i int) {
		etags[i] = putBlob(t, h.src, "in/"+key, versions[i])
		seqs[i] = newSequencer()
	}
	send := func(i int) {
		h.sendEvent(created("in/"+key, etags[i], int64(len(versions[i])), seqs[i]))
	}
	// Writes in quick succession; events delivered out of order, with old
	// events redelivered after newer ones.
	// The pauses let copies of intermediate versions start (or land), so
	// newer writes race in-flight copies rather than all collapsing to v5.
	pause := func(d time.Duration) { time.Sleep(d) }
	write(1)
	write(2)
	send(2)
	send(1)
	pause(1500 * time.Millisecond)
	write(3)
	send(3)
	pause(300 * time.Millisecond)
	write(4)
	send(4)
	send(3)
	pause(1500 * time.Millisecond)
	write(5)
	send(5)
	send(1)
	send(4)
	send(2)

	eventually(t, 90*time.Second, 200*time.Millisecond, func() (bool, string) {
		r, _ := h.record("overwrite", key)
		got, _ := destContent(t, h.dst, key)
		return r.Status == record.StatusSynced && r.SyncedVersion == etags[n] && bytes.Equal(got, versions[n]),
			fmt.Sprintf("record %s synced_version=%s (want v5 %s)", r.Status, r.SyncedVersion, etags[n])
	})
	// Let every stale event and job drain, then look again.
	eventually(t, 60*time.Second, 200*time.Millisecond, func() (bool, string) {
		return h.queueDepth() == 0 && h.unfinishedJobs() == 0, "events or jobs still pending"
	})
	time.Sleep(time.Second)
	close(stopWatch)
	<-watchDone

	if got, _ := destContent(t, h.dst, key); !bytes.Equal(got, versions[n]) {
		t.Errorf("final destination content is v%d, want v%d", byHash[sha256.Sum256(got)], n)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("destination versions observed in order: %v (0 = absent)", observed)
	high := 0
	for _, v := range observed {
		if v == -2 {
			t.Errorf("observed content matching no written version: %v", observed)
			continue
		}
		if v < high {
			t.Errorf("destination went back from v%d to v%d after the newer version was visible: %v", high, v, observed)
		}
		high = max(high, v)
	}
	if r, _ := h.record("overwrite", key); r.SyncedVersion != etags[n] {
		t.Errorf("record synced_version = %s, want %s (v5)", r.SyncedVersion, etags[n])
	}
}

// ============================================================================
// d. Throttling
// ============================================================================

func TestFailureThrottlingStillSyncs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, false)
	want := map[string][]byte{
		"a.txt":     randBytes(t, 100_000),
		"b.txt":     randBytes(t, 200_000),
		"c.txt":     randBytes(t, 300_000),
		"multi.bin": randBytes(t, 16<<20+7), // 4 parts
	}
	for k, b := range want {
		putBlob(t, h.src, "in/"+k, b)
	}
	h.srcF.FailProb(connector.ErrThrottled, 0.5, opOpenRange)
	h.dstF.FailProb(connector.ErrThrottled, 0.5, opUploadPart, opPutObject)

	cfg := h.config(h.pipeline("throttle", func(p *config.Pipeline) { p.ReconcileInterval = time.Hour }))
	start := time.Now()
	h.start(cfg)
	for k, b := range want {
		eventually(t, 4*time.Minute, 250*time.Millisecond, syncedTo(t, h.dst, k, b))
		eventually(t, 30*time.Second, 50*time.Millisecond, h.recordSynced("throttle", k))
	}
	took := time.Since(start)

	var jobs, attempts, maxAttempt int
	for k, b := range want {
		r, _ := h.record("throttle", k)
		if r.Status != record.StatusSynced {
			t.Errorf("%s: status %s, want synced", k, r.Status)
		}
		if sum := sha256.Sum256(b); !bytes.Equal(r.SHA256, sum[:]) {
			t.Errorf("%s: record sha256 %x, want %x", k, r.SHA256, sum)
		}
		for _, j := range h.copyJobs(k) {
			jobs++
			attempts += j.Attempt
			maxAttempt = max(maxAttempt, j.Attempt)
		}
	}
	if st := h.stats("throttle"); st.Failed != 0 {
		t.Errorf("stats = %+v; want no failures", st)
	}
	t.Logf("synced %d files in %s under 50%% throttling: source OpenRange %d calls (%d throttled), "+
		"dest UploadPart %d calls (%d throttled), dest PutObject %d calls (%d throttled); "+
		"%d copy jobs, %d job attempts (max %d per job)",
		len(want), took.Round(time.Millisecond),
		h.srcF.Calls(opOpenRange), h.srcF.Injected(opOpenRange),
		h.dstF.Calls(opUploadPart), h.dstF.Injected(opUploadPart),
		h.dstF.Calls(opPutObject), h.dstF.Injected(opPutObject),
		jobs, attempts, maxAttempt)
	if h.srcF.Injected(opOpenRange)+h.dstF.Injected(opUploadPart)+h.dstF.Injected(opPutObject) == 0 {
		t.Error("no throttling was injected; the test proved nothing")
	}
}

// ============================================================================
// e. Permission revoked mid-sync
// ============================================================================

func TestFailurePermissionRevoked(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	const interval = 3 * time.Second
	other := newBucket(t) // second pipeline's destination: unaffected
	otherDst, err := s3.New(context.Background(), h.s3(other), "out/")
	if err != nil {
		t.Fatal(err)
	}
	perm := h.pipeline("perm", func(p *config.Pipeline) { p.ReconcileInterval = interval })
	sibling := h.pipeline("sibling", func(p *config.Pipeline) {
		p.Source.Prefix = "in2/"
		s := h.s3(other)
		p.Destination.S3 = &s
		p.Events = config.Events{Type: "none"}
		p.ReconcileInterval = interval
	})
	h.start(h.config(perm, sibling))

	first := randBytes(t, 1000)
	putBlob(t, h.src, "in/first.txt", first)
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "first.txt", first))

	// Revoke destination writes.
	h.dstF.FailAlways(connector.ErrPermission, destWriteOps...)
	revokedAt := time.Now()
	blocked := randBytes(t, 2000)
	v := putBlob(t, h.src, "in/blocked.txt", blocked)
	h.sendEvent(created("in/blocked.txt", v, int64(len(blocked)), newSequencer()))

	tookFail := eventually(t, 30*time.Second, 100*time.Millisecond, func() (bool, string) {
		r, ok := h.record("perm", "blocked.txt")
		return ok && r.Status == record.StatusFailed, fmt.Sprintf("record = %s %q", r.Status, r.LastError)
	})
	r, _ := h.record("perm", "blocked.txt")
	t.Logf("blocked.txt marked failed %s after the write; last_error: %s", tookFail.Round(10*time.Millisecond), r.LastError)
	if !strings.Contains(r.LastError, "status=403") {
		t.Errorf("last_error = %q, want it to mention the permission problem", r.LastError)
	}

	// Watch a few reconcile intervals: each job attempt must give up at once
	// (no in-job retry loop); failed keys are re-tried only once per
	// reconcile pass.
	callsBefore := h.dstF.Calls(opPutObject)
	time.Sleep(3 * interval)
	jobs := h.copyJobs("blocked.txt")
	callsDuring := h.dstF.Calls(opPutObject) - callsBefore
	cancelled := 0
	for _, j := range jobs {
		if j.Attempt > 1 {
			t.Errorf("copy job %d retried %d times on a permission error; want it to give up at once", j.ID, j.Attempt)
		}
		if j.State == "cancelled" {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Errorf("no cancelled copy job for blocked.txt: %+v", jobs)
	}
	t.Logf("while revoked: %d copy jobs (%d cancelled), %d PutObject attempts in %s (one per reconcile pass at most)",
		len(jobs), cancelled, callsDuring, 3*interval)
	if maxCalls := 5; callsDuring > maxCalls {
		t.Errorf("%d PutObject attempts in %s; want at most ~1 per reconcile interval", callsDuring, 3*interval)
	}

	// The operator signal: an error log naming the key and the cause, and the
	// failure in the status stats.
	if l := h.logs.lines("level=ERROR", "copy failed permanently", "blocked.txt", "AccessDenied"); len(l) == 0 {
		t.Error(`no ERROR log "copy failed permanently" naming blocked.txt and the permission error`)
	}
	st := h.stats("perm")
	if st.Failed < 1 || len(st.RecentErrors) == 0 || st.RecentErrors[0].Key != "blocked.txt" {
		t.Errorf("stats = failed %d, recent errors %v; want blocked.txt reported", st.Failed, st.RecentErrors)
	}

	// The engine keeps running: the other pipeline still syncs, and so does
	// a new key once access is back.
	sib := randBytes(t, 3000)
	putBlob(t, h.src, "in2/sibling.txt", sib)
	eventually(t, 30*time.Second, 100*time.Millisecond, syncedTo(t, otherDst, "sibling.txt", sib))

	h.dstF.Clear()
	later := randBytes(t, 500)
	putBlob(t, h.src, "in/later.txt", later)
	tookRecover := eventually(t, 30*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "blocked.txt", blocked))
	eventually(t, 30*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "later.txt", later))
	t.Logf("access restored: blocked.txt synced %s later (reconcile interval %s); outage to recovery %s",
		tookRecover.Round(10*time.Millisecond), interval, time.Since(revokedAt).Round(time.Millisecond))
	if limit := 2*interval + 5*time.Second; tookRecover > limit {
		t.Errorf("recovery took %s after access was restored, want <= %s", tookRecover, limit)
	}
	eventually(t, 10*time.Second, 100*time.Millisecond, func() (bool, string) {
		st := h.stats("perm")
		return st.Failed == 0, fmt.Sprintf("stats still report %d failed", st.Failed)
	})
}

// ============================================================================
// f. Source delete
// ============================================================================

func TestFailureSourceDelete(t *testing.T) {
	t.Parallel()
	for _, deletes := range []bool{false, true} {
		t.Run(fmt.Sprintf("deletes=%v", deletes), func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, true)
			cfg := h.config(h.pipeline("del", func(p *config.Pipeline) {
				p.Deletes = deletes
				p.ReconcileInterval = time.Hour // the event path only
			}))
			h.start(cfg)
			body := randBytes(t, 4000)
			keep := randBytes(t, 100)
			putBlob(t, h.src, "in/gone.txt", body)
			putBlob(t, h.src, "in/stays.txt", keep)
			eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "gone.txt", body))
			eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "stays.txt", keep))

			if err := h.src.Delete(context.Background(), "in/gone.txt"); err != nil {
				t.Fatal(err)
			}
			h.sendEvent(blobEvent{Type: "Microsoft.Storage.BlobDeleted", Name: "in/gone.txt", Sequencer: newSequencer()})

			eventually(t, 30*time.Second, 100*time.Millisecond, func() (bool, string) {
				r, _ := h.record("del", "gone.txt")
				return r.Status == record.StatusDeleted, "record status " + string(r.Status)
			})
			if deletes {
				eventually(t, 10*time.Second, 100*time.Millisecond, func() (bool, string) {
					_, ok := destContent(t, h.dst, "gone.txt")
					return !ok, "gone.txt still at destination"
				})
			} else {
				if ok, msg := syncedTo(t, h.dst, "gone.txt", body)(); !ok {
					t.Errorf("deletes off but destination lost the file: %s", msg)
				}
				if n := h.dstF.Calls(opDelete); n != 0 {
					t.Errorf("deletes off but destination Delete was called %d times", n)
				}
			}
			if ok, msg := syncedTo(t, h.dst, "stays.txt", keep)(); !ok {
				t.Errorf("unrelated key affected: %s", msg)
			}
			if r, _ := h.record("del", "stays.txt"); r.Status != record.StatusSynced {
				t.Errorf("stays.txt status %s, want synced", r.Status)
			}
		})
	}
}

// ============================================================================
// g. Empty-source safety net
// ============================================================================

func TestFailureEmptySourceSafetyNet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// empty makes the source listing empty after the initial sync.
		empty func(t *testing.T, h *harness, keys []string, p *config.Pipeline)
	}{
		{"all-blobs-deleted", func(t *testing.T, h *harness, keys []string, _ *config.Pipeline) {
			for _, k := range keys {
				if err := h.src.Delete(context.Background(), "in/"+k); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"prefix-typo", func(_ *testing.T, _ *harness, _ []string, p *config.Pipeline) {
			p.Source.Prefix = "in-typo/"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, false)
			p := h.pipeline("empty", func(p *config.Pipeline) { p.Deletes = true })
			want := map[string][]byte{}
			var keys []string
			for i := range 5 {
				k := fmt.Sprintf("f%d.txt", i)
				keys = append(keys, k)
				want[k] = randBytes(t, 1000+i)
				putBlob(t, h.src, "in/"+k, want[k])
			}
			e := h.start(h.config(p))
			waitSynced(t, h.dst, want, 60*time.Second)
			e.stop()

			tc.empty(t, h, keys, &p)
			h.start(h.config(p))
			// The on-start pass and at least two periodic ones must refuse.
			eventually(t, 30*time.Second, 100*time.Millisecond, func() (bool, string) {
				n := len(h.logs.lines("level=ERROR", "reconcile refused", "pipeline=empty"))
				return n >= 3, fmt.Sprintf("%d refusals logged", n)
			})
			if l := h.logs.lines("reconcile refused"); len(l) > 0 {
				t.Logf("refusal: %s", l[0])
			}
			if n := h.dstF.Calls(opDelete); n != 0 {
				t.Errorf("destination Delete called %d times; the reconciler must not delete on an empty listing", n)
			}
			for _, k := range keys {
				if r, _ := h.record("empty", k); r.Status != record.StatusSynced {
					t.Errorf("%s: record status %s, want synced (no delete recorded)", k, r.Status)
				}
			}
			waitSynced(t, h.dst, want, time.Second)
		})
	}
}

// ============================================================================
// h. Duplicate events
// ============================================================================

func TestFailureDuplicateEvents(t *testing.T) {
	t.Parallel()
	h := newHarness(t, true)
	cfg := h.config(h.pipeline("dup", func(p *config.Pipeline) { p.ReconcileInterval = time.Hour }))
	h.start(cfg)
	eventually(t, 30*time.Second, 100*time.Millisecond, func() (bool, string) {
		return len(h.logs.lines("reconcile finished", "pipeline=dup")) > 0, "initial reconcile not finished"
	})

	small := randBytes(t, 5000)
	big := randBytes(t, 12<<20) // 3 parts
	files := []struct {
		key  string
		data []byte
	}{{"small.txt", small}, {"big.bin", big}}
	for _, f := range files {
		v := putBlob(t, h.src, "in/"+f.key, f.data)
		ev := created("in/"+f.key, v, int64(len(f.data)), newSequencer())
		ev.ID = "dup-" + f.key
		ev.Time = time.Now()
		for range 10 {
			h.sendEvent(ev)
		}
	}
	for _, f := range files {
		eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, f.key, f.data))
	}
	eventually(t, 60*time.Second, 200*time.Millisecond, func() (bool, string) {
		return h.queueDepth() == 0 && h.unfinishedJobs() == 0, "duplicate events or jobs still pending"
	})

	jobs := len(h.copyJobs("small.txt")) + len(h.copyJobs("big.bin"))
	// Count writes that reached the destination and succeeded: a call that
	// failed (a transient emulator 500 under load) and was retried inside
	// the same copy is not a second upload.
	d := h.dstF
	t.Logf("20 events -> %d copy jobs; succeeded/called: PutObject %d/%d, BeginUpload %d/%d, UploadPart %d/%d, Complete %d/%d",
		jobs, d.Succeeded(opPutObject), d.Calls(opPutObject), d.Succeeded(opBeginUpload), d.Calls(opBeginUpload),
		d.Succeeded(opUploadPart), d.Calls(opUploadPart), d.Succeeded(opComplete), d.Calls(opComplete))
	if n := d.Succeeded(opPutObject); n != 1 {
		t.Errorf("%d successful PutObjects, want 1 (small.txt once)", n)
	}
	if n := d.Succeeded(opBeginUpload); n != 1 {
		t.Errorf("%d successful BeginUploads, want 1", n)
	}
	if n := d.Succeeded(opUploadPart); n != 3 {
		t.Errorf("%d successful UploadParts, want 3 (big.bin's parts once)", n)
	}
	if n := d.Succeeded(opComplete); n != 1 {
		t.Errorf("%d successful Completes, want 1", n)
	}
	for _, f := range files {
		r, _ := h.record("dup", f.key)
		// One claim per copy attempt: duplicates must not add any. Attempts
		// that failed on a transient emulator error (logged) are retries of
		// the same copy, not duplicates.
		want := 1 + len(h.logs.lines("copy failed; will retry", "pipeline=dup", "key="+f.key))
		if r.Status != record.StatusSynced || r.Attempts != want {
			t.Errorf("%s: status %s attempts %d; want synced after %d attempt(s)", f.key, r.Status, r.Attempts, want)
		}
	}
}
