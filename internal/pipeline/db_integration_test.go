//go:build integration

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/metrics"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/testenv"
)

// Pipelines from the database (ADR 0004): the real engine against Azurite
// and SeaweedFS with a fresh database per test, driven only through the
// pipeline_spec table.

func init() {
	// Long enough that a change applied well within it must have come
	// through NOTIFY, short enough to test a missed notification.
	specRefreshInterval = 10 * time.Second
}

// dbConfig is an engine config with pipelines from the database. The table
// is migrated up front so tests can write specs before the engine starts.
func (h *harness) dbConfig() *config.File {
	h.t.Helper()
	if err := record.Migrate(context.Background(), h.db()); err != nil {
		h.t.Fatal(err)
	}
	return &config.File{Version: 1, DatabaseURL: h.dbURL, PipelinesFrom: config.PipelinesFromDatabase}
}

// spec is a pipeline_spec spec for the harness: <container>/in/ to
// <bucket>/<dstPrefix>, events from the harness queue if it has one,
// credentials inline unless mod replaces them.
func (h *harness) spec(dstPrefix string, mod func(map[string]any)) []byte {
	h.t.Helper()
	sp := map[string]any{
		"source": map[string]any{"prefix": "in/", "azure": map[string]any{
			"account_url": testenv.AzuriteBlobURL(), "container": h.container,
			"auth": "connection_string", "connection_string": testenv.AzuriteConnectionString(),
		}},
		"destination": map[string]any{"prefix": dstPrefix, "s3": map[string]any{
			"bucket": h.bucket, "region": testenv.S3Region, "endpoint": testenv.S3Endpoint(),
			"path_style": true, "flavor": "generic",
			"access_key_id": testenv.S3AccessKeyID, "secret_access_key": testenv.S3SecretAccessKey,
		}},
		"events":             map[string]any{"type": "none"},
		"reconcile_interval": "1m",
		"concurrency":        4,
		"part_concurrency":   2,
		"part_size":          "5MiB",
	}
	if h.queue != "" {
		sp["events"] = map[string]any{"type": "azure_queue", "queue_account_url": testenv.AzuriteQueueURL(), "queue_name": h.queue}
	}
	if mod != nil {
		mod(sp)
	}
	b, err := json.Marshal(sp)
	if err != nil {
		h.t.Fatal(err)
	}
	return b
}

func (h *harness) putSpec(name string, spec []byte) int64 {
	h.t.Helper()
	rev, _, err := record.UpsertPipelineSpec(context.Background(), h.db(), name, spec)
	if err != nil {
		h.t.Fatal(err)
	}
	return rev
}

func (h *harness) setEnabled(name string, enabled bool) {
	h.t.Helper()
	if _, _, err := record.SetPipelineEnabled(context.Background(), h.db(), name, enabled); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) revision(name string) int64 {
	h.t.Helper()
	var rev int64
	if err := h.db().QueryRow(context.Background(), `SELECT revision FROM pipeline_spec WHERE name = $1`, name).Scan(&rev); err != nil {
		h.t.Fatal(err)
	}
	return rev
}

// logged waits for a log line containing every one of subs.
func (h *harness) logged(timeout time.Duration, subs ...string) time.Duration {
	h.t.Helper()
	return eventually(h.t, timeout, 50*time.Millisecond, func() (bool, string) {
		return len(h.logs.lines(subs...)) > 0, fmt.Sprintf("no log line with %q", subs)
	})
}

// putAndNotify writes in/<key> and sends its BlobCreated event.
func (h *harness) putAndNotify(key string, b []byte) {
	h.t.Helper()
	v := putBlob(h.t, h.src, "in/"+key, b)
	h.sendEvent(created("in/"+key, v, int64(len(b)), newSequencer()))
}

// TestDatabasePipelineLifecycle drives one pipeline through insert, update,
// disable, enable and delete. Not parallel: it sets the secrets directory
// and environment (secret:// credentials).
func TestDatabasePipelineLifecycle(t *testing.T) {
	h := newHarness(t, true)
	cfg := h.dbConfig()

	// Credentials by reference: the connection string from a mounted file,
	// the S3 keys from the environment.
	secrets := t.TempDir()
	if err := os.WriteFile(filepath.Join(secrets, "azurite-conn"), []byte(testenv.AzuriteConnectionString()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(config.SecretsDirEnv, secrets)
	t.Setenv("PORTAGE_SECRET_S3_ID", testenv.S3AccessKeyID)
	t.Setenv("PORTAGE_SECRET_S3_KEY", testenv.S3SecretAccessKey)
	withSecrets := func(sp map[string]any) {
		sp["source"].(map[string]any)["azure"].(map[string]any)["connection_string"] = "secret://azurite-conn"
		s := sp["destination"].(map[string]any)["s3"].(map[string]any)
		s["access_key_id"], s["secret_access_key"] = "secret://s3-id", "secret://s3.key"
	}
	out2, err := s3.New(context.Background(), h.s3(h.bucket), "out2/")
	if err != nil {
		t.Fatal(err)
	}

	// The engine starts with an empty table.
	h.start(cfg)
	h.logged(30*time.Second, "engine started", "pipelines=0", "pipelines_from=database")

	// Insert: the on-start reconcile copies what's there. Applied through
	// NOTIFY, well before the first periodic re-read.
	a := randBytes(t, 1000)
	putBlob(t, h.src, "in/a.txt", a)
	inserted := time.Now()
	h.putSpec("dbp", h.spec("out/", withSecrets))
	took := h.logged(specRefreshInterval/2, "pipeline started", "pipeline=dbp", "revision=1")
	t.Logf("insert applied %s after the write (refresh interval %s)", took.Round(time.Millisecond), specRefreshInterval)
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "a.txt", a))
	t.Logf("a.txt synced %s after the insert", time.Since(inserted).Round(time.Millisecond))

	// Update to another destination prefix: the old instance stops, the
	// new one syncs new files under the new prefix.
	h.putSpec("dbp", h.spec("out2/", withSecrets))
	h.logged(30*time.Second, "pipeline stopped", "pipeline=dbp", "revision=1", "replaced by revision 2")
	h.logged(30*time.Second, "pipeline started", "pipeline=dbp", "revision=2", "s3:"+h.bucket+"/out2/")
	b := randBytes(t, 2000)
	h.putAndNotify("b.txt", b)
	eventually(t, 30*time.Second, 100*time.Millisecond, syncedTo(t, out2, "b.txt", b))
	if _, ok := destContent(t, h.dst, "b.txt"); ok {
		t.Error("b.txt was copied under the old prefix out/")
	}

	// Disable: nothing syncs, neither events nor existing files.
	h.setEnabled("dbp", false)
	disabledRev := h.revision("dbp")
	h.logged(30*time.Second, "pipeline stopped", "pipeline=dbp", "reason=disabled")
	c := randBytes(t, 300)
	h.putAndNotify("c.txt", c) // the event waits in the queue
	d := randBytes(t, 400)
	putBlob(t, h.src, "in/d.txt", d) // no event: only a reconcile finds it
	time.Sleep(5 * time.Second)
	for _, k := range []string{"c.txt", "d.txt"} {
		if _, ok := destContent(t, out2, k); ok {
			t.Errorf("%s synced while the pipeline was disabled", k)
		}
	}

	// Re-enable: the queued event is consumed and the on-start reconcile
	// finds d.txt.
	h.setEnabled("dbp", true)
	h.logged(30*time.Second, "pipeline started", "pipeline=dbp", fmt.Sprintf("revision=%d", disabledRev+1))
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, out2, "c.txt", c))
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, out2, "d.txt", d))

	// Delete: the pipeline stops; its file records stay.
	if _, err := record.DeletePipelineSpec(context.Background(), h.db(), "dbp"); err != nil {
		t.Fatal(err)
	}
	h.logged(30*time.Second, "pipeline stopped", "pipeline=dbp", "reason=deleted")
	e := randBytes(t, 500)
	h.putAndNotify("e.txt", e)
	time.Sleep(5 * time.Second)
	if _, ok := destContent(t, out2, "e.txt"); ok {
		t.Error("e.txt synced after the pipeline was deleted")
	}
	if st := h.stats("dbp"); st.Synced < 4 {
		t.Errorf("file records after delete: %+v; want the 4 synced files kept", st)
	}
	if l := h.logs.lines("did not stop cleanly"); len(l) > 0 {
		t.Errorf("a pipeline did not stop cleanly: %v", l)
	}
}

// TestDatabaseBadSpecIsolated: invalid specs next to a valid one are
// reported and retried, and never stop the valid one; a bad change to a
// running pipeline leaves the running revision alone.
func TestDatabaseBadSpecIsolated(t *testing.T) {
	// Not parallel: SeaweedFS (-volume.max=50, ~7 volumes per bucket) only
	// takes writes for a handful of buckets at once, and the failure tests
	// already use them.
	h := newHarness(t, false)
	cfg := h.dbConfig()
	m, metricsHandler, err := metrics.New()
	if err != nil {
		t.Fatal(err)
	}
	missingBucket := testenv.UniqueName(t, "e2e-later")

	h.putSpec("good", h.spec("out/", nil))
	h.putSpec("bad-field", h.spec("out/", func(sp map[string]any) { sp["concurency"] = 4 }))
	h.putSpec("bad-secret", h.spec("out/", func(sp map[string]any) {
		sp["destination"].(map[string]any)["s3"].(map[string]any)["secret_access_key"] = "secret://no-such-secret-xyz"
	}))
	h.putSpec("bad-bucket", h.spec("out/", func(sp map[string]any) {
		sp["destination"].(map[string]any)["s3"].(map[string]any)["bucket"] = missingBucket
	}))
	h.startWithMetrics(cfg, m)

	f := randBytes(t, 1000)
	putBlob(t, h.src, "in/f.txt", f)
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "f.txt", f))

	for name, want := range map[string]string{
		"bad-field":  "concurency",
		"bad-secret": "destination.s3.secret_access_key: secret://no-such-secret-xyz",
		"bad-bucket": "destination check",
	} {
		h.logged(10*time.Second, "level=ERROR", "pipeline="+name, want)
		if v := scrapeCounter(t, metricsHandler, metrics.NamePipelineConfigErrors, name); v < 1 {
			t.Errorf("%s{pipeline=%q} = %v, want >= 1", metrics.NamePipelineConfigErrors, name, v)
		}
	}
	if v := scrapeCounter(t, metricsHandler, metrics.NamePipelineConfigErrors, "good"); v != 0 {
		t.Errorf("config errors for the good pipeline = %v", v)
	}

	// Failing pipelines are retried: create the missing bucket and the
	// pipeline starts on a later refresh.
	newBucketNamed(t, missingBucket)
	h.logged(3*specRefreshInterval, "pipeline started", "pipeline=bad-bucket")

	// A bad change to the running good pipeline is not applied: revision 1
	// keeps syncing.
	h.putSpec("good", h.spec("out/", func(sp map[string]any) { sp["part_size"] = "1MiB" }))
	h.logged(30*time.Second, "level=ERROR", "pipeline=good", "revision=2", "revision 1 keeps running", "part_size")
	g := randBytes(t, 1500)
	putBlob(t, h.src, "in/g.txt", g)
	// No events: the next reconcile (1m) finds it.
	eventually(t, 90*time.Second, 200*time.Millisecond, syncedTo(t, h.dst, "g.txt", g))
	if l := h.logs.lines("pipeline stopped", "pipeline=good"); len(l) > 0 {
		t.Errorf("the good pipeline was stopped by a bad change: %v", l)
	}
}

// TestDatabaseMissedNotification: with the trigger disabled no NOTIFY is
// sent, and the periodic re-read still applies the change.
func TestDatabaseMissedNotification(t *testing.T) {
	// Not parallel: see TestDatabaseBadSpecIsolated.
	h := newHarness(t, false)
	cfg := h.dbConfig()
	if _, err := h.db().Exec(context.Background(), `ALTER TABLE pipeline_spec DISABLE TRIGGER pipeline_spec_notify`); err != nil {
		t.Fatal(err)
	}
	h.start(cfg)
	h.logged(30*time.Second, "engine started", "pipelines=0")

	f := randBytes(t, 1000)
	putBlob(t, h.src, "in/f.txt", f)
	h.putSpec("quiet", h.spec("out/", nil))
	took := h.logged(specRefreshInterval+20*time.Second, "pipeline started", "pipeline=quiet")
	t.Logf("applied %s after the write without a notification (refresh interval %s)", took.Round(time.Millisecond), specRefreshInterval)
	eventually(t, 60*time.Second, 100*time.Millisecond, syncedTo(t, h.dst, "f.txt", f))
}

// scrapeCounter returns the value of name{pipeline=pipeline} (0 if absent).
func scrapeCounter(t *testing.T, h http.Handler, name, pipeline string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, name+"{") && strings.Contains(line, `pipeline="`+pipeline+`"`) {
			var v float64
			if _, err := fmt.Sscan(line[strings.LastIndex(line, " ")+1:], &v); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return 0
}
