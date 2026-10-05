package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func scrape(t *testing.T, h http.Handler) map[string]*dto.MetricFamily {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape: status %d: %s", rec.Code, rec.Body)
	}
	p := expfmt.NewTextParser(model.LegacyValidation)
	fams, err := p.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("parse scrape: %v", err)
	}
	return fams
}

func labelsOf(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, l := range m.GetLabel() {
		out[l.GetName()] = l.GetValue()
	}
	return out
}

// find returns the series of family name whose labels include want.
func find(t *testing.T, fams map[string]*dto.MetricFamily, name string, want map[string]string) *dto.Metric {
	t.Helper()
	f, ok := fams[name]
	if !ok {
		t.Fatalf("metric %s not exposed", name)
	}
	var found *dto.Metric
	for _, m := range f.GetMetric() {
		ls := labelsOf(m)
		match := true
		for k, v := range want {
			if ls[k] != v {
				match = false
				break
			}
		}
		if match {
			if found != nil {
				t.Fatalf("%s%v: matches several series", name, want)
			}
			found = m
		}
	}
	if found == nil {
		t.Fatalf("%s%v: no such series", name, want)
	}
	return found
}

func counterValue(t *testing.T, fams map[string]*dto.MetricFamily, name string, want map[string]string) float64 {
	t.Helper()
	m := find(t, fams, name, want)
	if m.GetCounter() == nil {
		t.Fatalf("%s is %v, want counter", name, fams[name].GetType())
	}
	return m.GetCounter().GetValue()
}

func TestMetricsScrape(t *testing.T) {
	m, h, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	ctx := context.Background()

	m.CopyFinished(ctx, "p1", OutcomeSynced, 1000, 45*time.Second, 2*time.Second)
	m.CopyFinished(ctx, "p1", OutcomeSynced, 500, 90*time.Second, 3*time.Second)
	m.CopyFinished(ctx, "p1", OutcomeAlreadySynced, 999, time.Hour, time.Hour) // not lag/bytes
	m.CopyFinished(ctx, "p2", OutcomeFailed, 0, 0, time.Second)
	m.BytesUploaded(ctx, "p1", 4096)
	m.BytesUploaded(ctx, "p1", 4096)
	m.EventReceived(ctx, "p1", "azure_queue", "created")
	m.EventReceived(ctx, "p1", "azure_queue", "created")
	m.EventReceived(ctx, "p1", "webhook", "deleted")
	m.Retry(ctx, "p1", "upload_part", "throttled")
	m.ReconcileFinished(ctx, "p1", 100, 7, 2, 30*time.Second, nil)
	m.ReconcileFinished(ctx, "p1", 10, 0, 0, time.Second, errors.New("boom"))
	m.PipelineConfigError(ctx, "p2")
	m.PipelineConfigError(ctx, "p2")
	if err := m.RegisterQueueDepth(func(context.Context) (map[string]int64, error) {
		return map[string]int64{"p1": 12, "p2": 0}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterOldestPending(func(context.Context) (map[string]time.Duration, error) {
		return map[string]time.Duration{"p1": 75 * time.Second}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterQueueDepth(func(context.Context) (map[string]int64, error) { return nil, nil }); err == nil {
		t.Error("second RegisterQueueDepth: want error")
	}

	fams := scrape(t, h)

	// Every advertised name is exposed, with the expected type.
	for _, n := range Names {
		f, ok := fams[n]
		if !ok {
			t.Errorf("%s not exposed", n)
			continue
		}
		wantType := dto.MetricType_COUNTER
		switch {
		case contains(Histograms, n):
			wantType = dto.MetricType_HISTOGRAM
		case n == NameQueueDepth || n == NameOldestPendingSeconds:
			wantType = dto.MetricType_GAUGE
		}
		if f.GetType() != wantType {
			t.Errorf("%s type = %v, want %v", n, f.GetType(), wantType)
		}
	}
	// No surprise portage_ metrics (e.g. an exporter-added suffix).
	for n := range fams {
		if strings.HasPrefix(n, "portage_") && !contains(Names, n) {
			t.Errorf("unexpected metric %s", n)
		}
	}

	checks := []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{NameFilesTotal, map[string]string{"pipeline": "p1", "outcome": "synced"}, 2},
		{NameFilesTotal, map[string]string{"pipeline": "p1", "outcome": "already_synced"}, 1},
		{NameFilesTotal, map[string]string{"pipeline": "p2", "outcome": "failed"}, 1},
		{NameBytesCopiedTotal, map[string]string{"pipeline": "p1"}, 1500},
		{NameBytesUploadedTotal, map[string]string{"pipeline": "p1"}, 8192},
		{NameEventsTotal, map[string]string{"pipeline": "p1", "source": "azure_queue", "kind": "created"}, 2},
		{NameEventsTotal, map[string]string{"pipeline": "p1", "source": "webhook", "kind": "deleted"}, 1},
		{NameRetriesTotal, map[string]string{"pipeline": "p1", "op": "upload_part", "reason": "throttled"}, 1},
		{NameReconcileRunsTotal, map[string]string{"pipeline": "p1", "result": "ok"}, 1},
		{NameReconcileRunsTotal, map[string]string{"pipeline": "p1", "result": "error"}, 1},
		{NameReconcileListed, map[string]string{"pipeline": "p1"}, 110},
		{NameReconcileEmitted, map[string]string{"pipeline": "p1"}, 7},
		{NameReconcileDeletes, map[string]string{"pipeline": "p1"}, 2},
		{NamePipelineConfigErrors, map[string]string{"pipeline": "p2"}, 2},
	}
	for _, c := range checks {
		if got := counterValue(t, fams, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if _, ok := fams[NameBytesCopiedTotal]; ok {
		for _, s := range fams[NameBytesCopiedTotal].GetMetric() {
			if labelsOf(s)["pipeline"] == "p2" {
				t.Error("failed copy must not add bytes")
			}
		}
	}

	lag := find(t, fams, NameSyncLagSeconds, map[string]string{"pipeline": "p1"}).GetHistogram()
	if lag.GetSampleCount() != 2 || lag.GetSampleSum() != 135 {
		t.Errorf("lag count/sum = %d/%v, want 2/135", lag.GetSampleCount(), lag.GetSampleSum())
	}
	var le60 uint64
	var bounds []float64
	for _, b := range lag.GetBucket() {
		bounds = append(bounds, b.GetUpperBound())
		if b.GetUpperBound() == 60 {
			le60 = b.GetCumulativeCount()
		}
	}
	if le60 != 1 {
		t.Errorf("lag le=60 bucket = %d, want 1 (buckets %v)", le60, bounds)
	}
	if bounds[0] != 0.5 || !contains(bounds, 3600) {
		t.Errorf("lag buckets %v: want 0.5..3600", bounds)
	}
	dur := find(t, fams, NameCopyDurationSeconds, map[string]string{"pipeline": "p1"}).GetHistogram()
	if dur.GetSampleCount() != 2 || dur.GetSampleSum() != 5 {
		t.Errorf("duration count/sum = %d/%v, want 2/5", dur.GetSampleCount(), dur.GetSampleSum())
	}

	if v := find(t, fams, NameQueueDepth, map[string]string{"pipeline": "p1"}).GetGauge().GetValue(); v != 12 {
		t.Errorf("queue depth p1 = %v, want 12", v)
	}
	if v := find(t, fams, NameOldestPendingSeconds, map[string]string{"pipeline": "p1"}).GetGauge().GetValue(); v != 75 {
		t.Errorf("oldest pending p1 = %v, want 75", v)
	}
	// No otel_scope_* labels leak onto series.
	for k := range labelsOf(find(t, fams, NameQueueDepth, map[string]string{"pipeline": "p1"})) {
		if strings.HasPrefix(k, "otel_") {
			t.Errorf("unexpected label %s", k)
		}
	}
}

func contains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestNilMetricsIsNoop(t *testing.T) {
	var m *Metrics
	ctx := context.Background()
	m.CopyFinished(ctx, "p", OutcomeSynced, 1, time.Second, time.Second)
	m.BytesUploaded(ctx, "p", 1)
	m.EventReceived(ctx, "p", "s", "k")
	m.ReconcileFinished(ctx, "p", 1, 1, 1, time.Second, nil)
	m.Retry(ctx, "p", "op", "other")
	m.PipelineConfigError(ctx, "p")
	if err := m.RegisterQueueDepth(func(context.Context) (map[string]int64, error) { return nil, nil }); err != nil {
		t.Error(err)
	}
	if err := m.RegisterOldestPending(func(context.Context) (map[string]time.Duration, error) { return nil, nil }); err != nil {
		t.Error(err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Error(err)
	}
}

func TestGaugeCallbackErrorDoesNotBreakScrape(t *testing.T) {
	m, h, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterQueueDepth(func(context.Context) (map[string]int64, error) {
		return nil, errors.New("db down")
	}); err != nil {
		t.Fatal(err)
	}
	m.CopyFinished(context.Background(), "p1", OutcomeSynced, 1, time.Second, time.Second)
	fams := scrape(t, h)
	if counterValue(t, fams, NameFilesTotal, map[string]string{"pipeline": "p1"}) != 1 {
		t.Error("files_total missing while gauge callback fails")
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestServe(t *testing.T) {
	_, h, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var isReady atomic.Bool
	ready := func(context.Context) error {
		if isReady.Load() {
			return nil
		}
		return errors.New("db unreachable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, h, ready, nil) }()
	base := "http://" + ln.Addr().String()

	if code, _ := get(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}
	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "db unreachable") {
		t.Errorf("/readyz = %d %q, want 503", code, body)
	}
	isReady.Store(true)
	if code, _ := get(t, base+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", code)
	}
	if code, body := get(t, base+"/metrics"); code != http.StatusOK || !strings.Contains(body, "go_goroutines") {
		t.Errorf("/metrics = %d", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve after cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}

func TestServeListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := Serve(context.Background(), ln.Addr().String(), nil, nil, nil); err == nil {
		t.Fatal("want error for address in use")
	}
}
