package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func line(key string, size int64, sha string, at time.Time) string {
	return fmt.Sprintf(`{"key":%q,"size":%d,"sha256":%q,"written_at":%q}`+"\n", key, size, sha, at.Format(time.RFC3339Nano))
}

func TestReadManifestsKeepsLastWritePerKey(t *testing.T) {
	t0 := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	a, b, c, d := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)
	m1 := writeFile(t, "m1.jsonl",
		line("x", 1, a, t0)+
			line("y", 2, a, t0)+
			line("x", 3, b, t0.Add(time.Second))+ // later write of x in the same file
			"\n") // blank lines are fine
	m2 := writeFile(t, "m2.jsonl",
		line("x", 4, c, t0.Add(-time.Hour))+ // older write in a later file loses
			line("y", 5, d, t0)+ // same written_at: the one read last wins
			line("z", 6, a, t0))

	set, err := readManifests([]string{m1, m2})
	if err != nil {
		t.Fatal(err)
	}
	if set.Lines != 6 || set.Superseded != 3 || len(set.Entries) != 3 {
		t.Fatalf("lines %d superseded %d entries %d; want 6, 3, 3", set.Lines, set.Superseded, len(set.Entries))
	}
	got := map[string]manifestEntry{}
	for _, e := range set.Entries {
		got[e.Key] = e
	}
	if e := got["x"]; e.Size != 3 || e.SHA256 != b {
		t.Errorf("x = %+v; want the size-3 write", e)
	}
	if e := got["y"]; e.Size != 5 || e.SHA256 != d {
		t.Errorf("y = %+v; want the later-read size-5 write", e)
	}
	if set.Entries[0].Key != "x" || set.Entries[2].Key != "z" {
		t.Errorf("entries not sorted by key: %v", set.Entries)
	}

	load := summarizeLoad(set.Entries)
	if load.Files != 3 || load.Bytes != 14 || load.MaxSize != 6 || !load.LastWrite.Equal(t0.Add(time.Second)) {
		t.Errorf("load = %+v", load)
	}
}

func TestReadManifestsRejectsBadLines(t *testing.T) {
	for name, content := range map[string]string{
		"truncated": `{"key":"a","size":1,"sha256":"` + strings.Repeat("a", 64),
		"no sha":    `{"key":"a","size":1}`,
		"no key":    `{"size":1,"sha256":"` + strings.Repeat("a", 64) + `"}`,
	} {
		if _, err := readManifests([]string{writeFile(t, "m.jsonl", content)}); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	if _, err := readManifests([]string{filepath.Join(t.TempDir(), "absent")}); err == nil {
		t.Error("missing file: want error")
	}
}

func TestHistogramQuantile(t *testing.T) {
	inf := math.Inf(1)
	// 100 observations: 10 in (0,1], 40 in (1,2], 40 in (2,5], 9 in (5,10], 1 above 10.
	bs := []bucket{{10, 99}, {1, 10}, {inf, 100}, {2, 50}, {5, 90}} // unsorted on purpose
	cases := []struct {
		q, want float64
	}{
		{0, 0},             // rank 0 lands in the first bucket, at its start
		{0.05, 0.5},        // rank 5 of 10 in (0,1]
		{0.1, 1},           // exactly the first bucket's count
		{0.3, 1.5},         // rank 30: 20 of 40 into (1,2]
		{0.5, 2},           // rank 50: top of (1,2]
		{0.7, 3.5},         // rank 70: 20 of 40 into (2,5]
		{0.95, 5 + 5*5/9.}, // rank 95: 5 of 9 into (5,10]
		{0.995, 10},        // rank 99.5 lands in +Inf: highest finite bound
		{1, 10},
	}
	for _, c := range cases {
		if got := histogramQuantile(c.q, bs); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("q=%v: got %v, want %v", c.q, got, c.want)
		}
	}
	if !math.IsInf(histogramQuantile(-0.1, bs), -1) || !math.IsInf(histogramQuantile(1.1, bs), 1) {
		t.Error("out-of-range q should give ±Inf like PromQL")
	}
	for name, b := range map[string][]bucket{
		"empty":        {{1, 0}, {inf, 0}},
		"no +Inf":      {{1, 3}, {2, 5}},
		"single":       {{inf, 3}},
		"no buckets":   nil,
		"all in first": {{1, 4}, {inf, 4}},
	} {
		got := histogramQuantile(0.5, b)
		if name == "all in first" {
			if got != 0.5 {
				t.Errorf("%s: got %v, want 0.5", name, got)
			}
			continue
		}
		if !math.IsNaN(got) {
			t.Errorf("%s: got %v, want NaN", name, got)
		}
	}
	// Non-monotonic counts (a scrape racing an observation) are clamped.
	// Clamped to {1,6},{2,6},{+Inf,10}: rank 5 is 5/6 into (0,1].
	if got := histogramQuantile(0.5, []bucket{{1, 6}, {2, 5}, {inf, 10}}); math.Abs(got-5/6.) > 1e-9 {
		t.Errorf("non-monotonic: got %v, want %v", got, 5/6.)
	}
}

const sampleMetrics = `# HELP portage_sync_lag_seconds Time from the source change.
# TYPE portage_sync_lag_seconds histogram
portage_sync_lag_seconds_bucket{pipeline="soak",le="1"} 10
portage_sync_lag_seconds_bucket{pipeline="soak",le="2"} 50
portage_sync_lag_seconds_bucket{pipeline="soak",le="5"} 90
portage_sync_lag_seconds_bucket{pipeline="soak",le="10"} 99
portage_sync_lag_seconds_bucket{pipeline="soak",le="+Inf"} 100
portage_sync_lag_seconds_sum{pipeline="soak"} 300
portage_sync_lag_seconds_count{pipeline="soak"} 100
portage_sync_lag_seconds_bucket{pipeline="other",le="1"} 1000
portage_sync_lag_seconds_bucket{pipeline="other",le="+Inf"} 1000
portage_sync_lag_seconds_count{pipeline="other"} 1000
portage_files_total{outcome="synced",pipeline="soak"} 100
portage_files_total{outcome="already_synced",pipeline="soak"} 7
portage_files_total{outcome="synced",pipeline="other"} 1000
portage_bytes_copied_total{pipeline="soak"} 3.6e+09
portage_events_total{kind="created",pipeline="soak",source="event"} 95
portage_events_total{kind="created",pipeline="soak",source="reconcile"} 12
portage_queue_depth{pipeline="soak"} 3
portage_oldest_pending_seconds{pipeline="soak"} 4.5
label_escapes{a="x\"y,z}",b="\\"} 1 1700000000000
go_goroutines 42
process_resident_memory_bytes 1.5e+08
process_start_time_seconds 1.7e+09
`

func TestParsePromTextAndReport(t *testing.T) {
	samples, err := parsePromText(strings.NewReader(sampleMetrics))
	if err != nil {
		t.Fatal(err)
	}
	var esc *sample
	for i := range samples {
		if samples[i].Name == "label_escapes" {
			esc = &samples[i]
		}
	}
	if esc == nil || esc.Labels["a"] != `x"y,z}` || esc.Labels["b"] != `\` || esc.Value != 1 {
		t.Errorf("escaped labels parsed as %+v", esc)
	}

	at := time.Unix(1_700_003_600, 0)
	r := buildMetricsReport(samples, "soak", at)
	lag := r.SyncLagSeconds
	if lag.Count != 100 || lag.Mean == nil || *lag.Mean != 3 {
		t.Errorf("lag count/mean = %v/%v", lag.Count, lag.Mean)
	}
	if lag.P50 == nil || *lag.P50 != 2 || lag.P95 == nil || math.Abs(*lag.P95-(5+25/9.)) > 1e-9 || lag.P99 == nil || *lag.P99 != 10 {
		t.Errorf("lag quantiles = %v %v %v", *lag.P50, *lag.P95, *lag.P99)
	}
	if r.CopyDurationSeconds.P50 != nil {
		t.Error("absent histogram should have no quantiles")
	}
	if r.FilesByOutcome["synced"] != 100 || r.FilesByOutcome["already_synced"] != 7 {
		t.Errorf("outcomes = %v (other pipeline must not leak in)", r.FilesByOutcome)
	}
	if r.EventsBySource["event"] != 95 || r.EventsBySource["reconcile"] != 12 {
		t.Errorf("events = %v", r.EventsBySource)
	}
	if r.BytesCopied != 3.6e9 || r.QueueDepth != 3 || r.OldestPendingSeconds != 4.5 || r.Goroutines != 42 {
		t.Errorf("report = %+v", r)
	}
	if r.EngineUptimeSecond != 3600 || r.AvgBytesPerSecond != 1e6 {
		t.Errorf("uptime %v avg %v; want 3600s, 1MB/s", r.EngineUptimeSecond, r.AvgBytesPerSecond)
	}

	all := buildMetricsReport(samples, "", at)
	if all.SyncLagSeconds.Count != 1100 || all.FilesByOutcome["synced"] != 1100 {
		t.Errorf("all pipelines: count %v synced %v", all.SyncLagSeconds.Count, all.FilesByOutcome["synced"])
	}

	if _, err := parsePromText(strings.NewReader("broken{a=\"x\" 1\n")); err == nil {
		t.Error("unterminated labels: want error")
	}
}

func TestMapToDestination(t *testing.T) {
	f, err := change.NewFilter(config.Filters{Exclude: []string{"**/*.tmp"}})
	if err != nil {
		t.Fatal(err)
	}
	entries := []manifestEntry{
		{Key: "in/a.bin"}, {Key: "in/sub/b.tmp"}, {Key: "other/c"}, {Key: "in/"}, {Key: "inx/d"},
	}
	m := mapToDestination(entries, "in/", f)
	if len(m.Targets) != 1 || m.Targets[0].DestKey != "a.bin" || m.Excluded != 1 || m.OutOfScope != 3 {
		t.Errorf("mapping = %+v", m)
	}
	if m := mapToDestination(entries, "", nil); len(m.Targets) != 5 {
		t.Errorf("no prefix/filter: %d targets", len(m.Targets))
	}
}

// fakeDest is an in-memory destination.
type fakeDest struct {
	mu    sync.Mutex
	objs  map[string][]byte
	meta  map[string]map[string]string
	flaky map[string]int // Stat errors to return before succeeding
}

func (f *fakeDest) Stat(_ context.Context, key string) (connector.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flaky[key] > 0 {
		f.flaky[key]--
		return connector.ObjectInfo{}, errors.New("connection reset")
	}
	b, ok := f.objs[key]
	if !ok {
		return connector.ObjectInfo{}, connector.ErrNotFound
	}
	return connector.ObjectInfo{Key: key, Size: int64(len(b)), Version: shaHex(b)[:8],
		ModTime: time.Unix(100, 0), Metadata: f.meta[key]}, nil
}

func (f *fakeDest) OpenRange(_ context.Context, key, _ string, _, _ int64) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objs[key]
	if !ok {
		return nil, connector.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeDest) put(key string, b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = b
}

func TestCheck(t *testing.T) {
	good, other := []byte("hello world"), []byte("hello WORLD")
	written := time.Unix(90, 0)
	dst := &fakeDest{
		objs: map[string][]byte{
			"ok":      good,
			"empty":   {},
			"badsha":  other,
			"badsize": good[:5],
			"flaky":   good,
			"meta":    other,
		},
		meta:  map[string]map[string]string{"meta": {connector.MetaSHA256: shaHex(other)}},
		flaky: map[string]int{"flaky": 2},
	}
	tg := func(key string, b []byte) target {
		return target{Entry: manifestEntry{Key: "src/" + key, Size: int64(len(b)), SHA256: shaHex(b), WrittenAt: written}, DestKey: key}
	}
	targets := []target{
		tg("ok", good), tg("empty", nil), tg("badsha", good), tg("badsize", good),
		tg("missing", good), tg("flaky", good), tg("meta", good),
	}
	res := check(context.Background(), dst, targets, checkOptions{Concurrency: 3, DestPrefix: "out/"})
	if res.Files != 7 || res.OK != 3 || res.Missing != 1 || res.SizeMismatch != 1 || res.SHAMismatch != 2 || res.Errors != 0 {
		t.Fatalf("result = %+v", res)
	}
	if !res.Failed() {
		t.Error("Failed() = false")
	}
	if res.DestLag == nil || res.DestLag.Count != 3 || res.DestLag.P50 != 10 {
		t.Errorf("dest lag = %+v", res.DestLag)
	}
	byKey := map[string]failure{}
	for _, f := range res.Failures {
		byKey[f.DestKey] = f
	}
	if f := byKey["out/badsha"]; f.Problem != problemSHAMismatch || f.ActualSHA256 != shaHex(other) {
		t.Errorf("badsha failure = %+v", f)
	}
	if f := byKey["out/badsize"]; f.Problem != problemSizeMismatch || f.ActualSize != 5 {
		t.Errorf("badsize failure = %+v", f)
	}
	if res.BytesHashed != int64(len(good)*2+len(other)*2) {
		t.Errorf("hashed %d bytes", res.BytesHashed)
	}

	// Size-only mode can't see content corruption unless metadata says so.
	res = check(context.Background(), dst, targets, checkOptions{SizeOnly: true})
	if res.OK != 4 || res.SHAMismatch != 1 || res.BytesHashed != 0 {
		t.Errorf("size-only result = %+v", res)
	}

	// Persistent errors are reported as errors, not passes.
	dst.flaky["ok"] = 100
	res = check(context.Background(), dst, targets[:1], checkOptions{Attempts: 2})
	if res.Errors != 1 || res.OK != 0 {
		t.Errorf("persistent error result = %+v", res)
	}
}

func TestCheckRetryMissing(t *testing.T) {
	b := []byte("late arrival")
	dst := &fakeDest{objs: map[string][]byte{}}
	targets := []target{{Entry: manifestEntry{Key: "k", Size: int64(len(b)), SHA256: shaHex(b)}, DestKey: "k"}}
	go func() {
		time.Sleep(150 * time.Millisecond)
		dst.put("k", b)
	}()
	var out bytes.Buffer
	res := check(context.Background(), dst, targets, checkOptions{
		RetryMissing: 5 * time.Second, RetryEvery: 50 * time.Millisecond, Out: &out,
	})
	if res.OK != 1 || res.Failed() {
		t.Fatalf("result = %+v", res)
	}
	if !strings.Contains(out.String(), "re-checking") {
		t.Errorf("no retry progress in %q", out.String())
	}

	// And it gives up at the deadline.
	targets[0].DestKey = "never"
	res = check(context.Background(), dst, targets, checkOptions{RetryMissing: 100 * time.Millisecond, RetryEvery: 20 * time.Millisecond})
	if res.Missing != 1 {
		t.Errorf("result = %+v", res)
	}
}

func TestWriteSummary(t *testing.T) {
	lag := 2.5
	r := &report{
		Pipeline: "soak", Manifests: []string{"a"}, UniqueKeys: 2,
		Verify: &checkResult{Files: 2, OK: 1, Missing: 1, Hashed: true,
			Failures: []failure{{DestKey: "out/x", Problem: problemMissing}}},
		Metrics: &metricsReport{SyncLagSeconds: quantiles{Count: 1, P50: &lag}},
	}
	var b bytes.Buffer
	writeSummary(&b, r, 10)
	s := b.String()
	for _, want := range []string{"FAIL", "1 missing", "out/x", "p50 2.5s", "p95 n/a"} {
		if !strings.Contains(s, want) {
			t.Errorf("summary lacks %q:\n%s", want, s)
		}
	}
}
