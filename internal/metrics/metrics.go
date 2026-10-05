// Package metrics exposes Portage's OpenTelemetry instruments via a
// Prometheus /metrics endpoint.
//
// Metric names are pinned: the exporter runs with
// otlptranslator.UnderscoreEscapingWithoutSuffixes, so every name below is
// exactly what Prometheus sees (the exporter adds no unit or _total suffix;
// histograms still expose the usual _bucket/_sum/_count series). Names are
// listed in [Names] and checked against the Grafana dashboard by a test.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Exposed metric names (as scraped).
const (
	NameFilesTotal           = "portage_files_total"
	NameBytesCopiedTotal     = "portage_bytes_copied_total"
	NameBytesUploadedTotal   = "portage_bytes_uploaded_total"
	NameSyncLagSeconds       = "portage_sync_lag_seconds"
	NameCopyDurationSeconds  = "portage_copy_duration_seconds"
	NameEventsTotal          = "portage_events_total"
	NameRetriesTotal         = "portage_retries_total"
	NameReconcileRunsTotal   = "portage_reconcile_runs_total"
	NameReconcileListed      = "portage_reconcile_listed_total"
	NameReconcileEmitted     = "portage_reconcile_emitted_total"
	NameReconcileDeletes     = "portage_reconcile_deletes_total"
	NameReconcileDuration    = "portage_reconcile_duration_seconds"
	NameQueueDepth           = "portage_queue_depth"
	NameOldestPendingSeconds = "portage_oldest_pending_seconds"
	NamePipelineConfigErrors = "portage_pipeline_config_errors"
)

// Names lists every metric this package emits; histograms additionally
// expose <name>_bucket, <name>_sum and <name>_count.
var Names = []string{
	NameFilesTotal, NameBytesCopiedTotal, NameBytesUploadedTotal,
	NameSyncLagSeconds, NameCopyDurationSeconds, NameEventsTotal,
	NameRetriesTotal, NameReconcileRunsTotal, NameReconcileListed,
	NameReconcileEmitted, NameReconcileDeletes, NameReconcileDuration,
	NameQueueDepth, NameOldestPendingSeconds, NamePipelineConfigErrors,
}

// Histograms lists the members of Names that are histograms.
var Histograms = []string{NameSyncLagSeconds, NameCopyDurationSeconds, NameReconcileDuration}

// Copy outcomes accepted by CopyFinished.
const (
	OutcomeSynced        = "synced"
	OutcomeAlreadySynced = "already_synced"
	OutcomeStale         = "stale"
	OutcomeFailed        = "failed"
	OutcomeRetry         = "retry"
)

var (
	// lagBuckets: 0.5s..1h, dense around the 60s p95 SLO so
	// histogram_quantile is accurate where it matters.
	lagBuckets = []float64{0.5, 1, 2, 5, 10, 15, 20, 30, 40, 50, 60, 75, 90, 120, 180, 300, 600, 1200, 1800, 3600}
	// durationBuckets: copies range from sub-second small files to
	// hour-long multi-GB objects.
	durationBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}
	// reconcileBuckets: one listing pass of a container.
	reconcileBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200}
)

// observeTimeout bounds a gauge callback (e.g. a Postgres query) so a slow
// database can't hang a scrape.
const observeTimeout = 5 * time.Second

// Metrics holds every instrument. A nil *Metrics is safe to call methods on
// (no-op), so packages and tests can run without metrics.
type Metrics struct {
	provider *sdkmetric.MeterProvider
	meter    metric.Meter

	files         metric.Int64Counter
	bytesCopied   metric.Int64Counter
	bytesUploaded metric.Int64Counter
	syncLag       metric.Float64Histogram
	copyDuration  metric.Float64Histogram
	events        metric.Int64Counter
	retries       metric.Int64Counter

	reconcileRuns     metric.Int64Counter
	reconcileListed   metric.Int64Counter
	reconcileEmitted  metric.Int64Counter
	reconcileDeletes  metric.Int64Counter
	reconcileDuration metric.Float64Histogram

	configErrors metric.Int64Counter

	mu         sync.Mutex
	registered map[string]bool
}

// New builds the instruments on a private Prometheus registry (plus Go
// runtime and process collectors) and returns the /metrics handler.
func New() (*Metrics, http.Handler, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	exp, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithoutSuffixes),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, nil, err
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))
	m := &Metrics{
		provider:   provider,
		meter:      provider.Meter("github.com/sunny36/portage"),
		registered: map[string]bool{},
	}
	if err := m.init(); err != nil {
		return nil, nil, err
	}
	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
	return m, h, nil
}

func (m *Metrics) init() error {
	var errs []error
	counter := func(name, unit, desc string) metric.Int64Counter {
		c, err := m.meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		errs = append(errs, err)
		return c
	}
	hist := func(name, desc string, buckets []float64) metric.Float64Histogram {
		h, err := m.meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc),
			metric.WithExplicitBucketBoundaries(buckets...))
		errs = append(errs, err)
		return h
	}
	m.files = counter(NameFilesTotal, "{file}", "Files processed by copy workers, by outcome (synced, already_synced, stale, failed, retry).")
	m.bytesCopied = counter(NameBytesCopiedTotal, "By", "Bytes of files successfully synced and verified.")
	m.bytesUploaded = counter(NameBytesUploadedTotal, "By", "Bytes uploaded to the destination as parts complete (live throughput, includes retried parts).")
	m.syncLag = hist(NameSyncLagSeconds, "Time from the source change (event time) to the verified copy at the destination.", lagBuckets)
	m.copyDuration = hist(NameCopyDurationSeconds, "Wall time of one successful copy (transfer + verify).", durationBuckets)
	m.events = counter(NameEventsTotal, "{event}", "Change events received, by source and kind.")
	m.retries = counter(NameRetriesTotal, "{retry}", "Retried provider operations, by operation and reason (throttled, other).")
	m.reconcileRuns = counter(NameReconcileRunsTotal, "{run}", "Reconcile passes, by result (ok, error).")
	m.reconcileListed = counter(NameReconcileListed, "{object}", "Source objects listed by the reconciler.")
	m.reconcileEmitted = counter(NameReconcileEmitted, "{object}", "Copies the reconciler enqueued because the destination was missing or behind.")
	m.reconcileDeletes = counter(NameReconcileDeletes, "{object}", "Source deletes detected by the reconciler.")
	m.reconcileDuration = hist(NameReconcileDuration, "Wall time of one reconcile pass.", reconcileBuckets)
	m.configErrors = counter(NamePipelineConfigErrors, "{error}", "Pipelines that could not be (re)started: invalid spec, unresolvable secret or failed start check. Retried on the next refresh.")
	return errors.Join(errs...)
}

// Shutdown flushes and stops the meter provider.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return m.provider.Shutdown(ctx)
}

func pipelineAttr(pipeline string) attribute.KeyValue { return attribute.String("pipeline", pipeline) }

// CopyFinished records one copy attempt. Every call counts towards
// portage_files_total{outcome}; bytes, sync lag and copy duration are
// recorded only for outcome "synced" (lag is skipped when <= 0, i.e. the
// event time was unknown).
func (m *Metrics) CopyFinished(ctx context.Context, pipeline, outcome string, bytes int64, lag, dur time.Duration) {
	if m == nil {
		return
	}
	p := pipelineAttr(pipeline)
	m.files.Add(ctx, 1, metric.WithAttributes(p, attribute.String("outcome", outcome)))
	if outcome != OutcomeSynced {
		return
	}
	set := metric.WithAttributes(p)
	if bytes > 0 {
		m.bytesCopied.Add(ctx, bytes, set)
	}
	if lag > 0 {
		m.syncLag.Record(ctx, lag.Seconds(), set)
	}
	m.copyDuration.Record(ctx, dur.Seconds(), set)
}

// BytesUploaded adds n bytes to the live upload throughput counter; call it
// as each part completes.
func (m *Metrics) BytesUploaded(ctx context.Context, pipeline string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesUploaded.Add(ctx, n, metric.WithAttributes(pipelineAttr(pipeline)))
}

// EventReceived counts one change event. source is e.g. "azure_queue",
// "webhook" or "reconcile"; kind is e.g. "created" or "deleted".
func (m *Metrics) EventReceived(ctx context.Context, pipeline, source, kind string) {
	if m == nil {
		return
	}
	m.events.Add(ctx, 1, metric.WithAttributes(pipelineAttr(pipeline),
		attribute.String("source", source), attribute.String("kind", kind)))
}

// ReconcileFinished records one reconcile pass. A non-nil err counts the run
// as result="error"; the counts are still added (partial passes do work).
func (m *Metrics) ReconcileFinished(ctx context.Context, pipeline string, listed, emitted, deletes int64, dur time.Duration, err error) {
	if m == nil {
		return
	}
	p := pipelineAttr(pipeline)
	set := metric.WithAttributes(p)
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.reconcileRuns.Add(ctx, 1, metric.WithAttributes(p, attribute.String("result", result)))
	if listed > 0 {
		m.reconcileListed.Add(ctx, listed, set)
	}
	if emitted > 0 {
		m.reconcileEmitted.Add(ctx, emitted, set)
	}
	if deletes > 0 {
		m.reconcileDeletes.Add(ctx, deletes, set)
	}
	m.reconcileDuration.Record(ctx, dur.Seconds(), set)
}

// PipelineConfigError counts one failed attempt to start a pipeline from its
// config (invalid spec, unresolvable secret, failed start check).
func (m *Metrics) PipelineConfigError(ctx context.Context, pipeline string) {
	if m == nil {
		return
	}
	m.configErrors.Add(ctx, 1, metric.WithAttributes(pipelineAttr(pipeline)))
}

// Retry counts one retried operation. op is e.g. "upload_part", "stat";
// reason is "throttled" or "other".
func (m *Metrics) Retry(ctx context.Context, pipeline, op, reason string) {
	if m == nil {
		return
	}
	m.retries.Add(ctx, 1, metric.WithAttributes(pipelineAttr(pipeline),
		attribute.String("op", op), attribute.String("reason", reason)))
}

// RegisterQueueDepth exposes portage_queue_depth{pipeline}: jobs waiting to
// run, as reported by fn (pipeline -> count). fn runs on every scrape with a
// short timeout. Register once; a second call returns an error.
func (m *Metrics) RegisterQueueDepth(fn func(ctx context.Context) (map[string]int64, error)) error {
	if m == nil {
		return nil
	}
	if err := m.claim(NameQueueDepth); err != nil {
		return err
	}
	g, err := m.meter.Int64ObservableGauge(NameQueueDepth, metric.WithUnit("{job}"),
		metric.WithDescription("Jobs waiting in the copy queue."))
	if err != nil {
		return err
	}
	_, err = m.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, observeTimeout)
		defer cancel()
		vals, err := fn(ctx)
		if err != nil {
			return err
		}
		for p, v := range vals {
			o.ObserveInt64(g, v, metric.WithAttributes(pipelineAttr(p)))
		}
		return nil
	}, g)
	return err
}

// RegisterOldestPending exposes portage_oldest_pending_seconds{pipeline}: the
// age of the oldest change not yet synced, i.e. the lag building up right
// now (portage_sync_lag_seconds only sees a file once it finishes). fn
// returns pipeline -> age; omit a pipeline (or return 0) when nothing is
// pending.
func (m *Metrics) RegisterOldestPending(fn func(ctx context.Context) (map[string]time.Duration, error)) error {
	if m == nil {
		return nil
	}
	if err := m.claim(NameOldestPendingSeconds); err != nil {
		return err
	}
	g, err := m.meter.Float64ObservableGauge(NameOldestPendingSeconds, metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest pending change (current sync lag)."))
	if err != nil {
		return err
	}
	_, err = m.meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, observeTimeout)
		defer cancel()
		vals, err := fn(ctx)
		if err != nil {
			return err
		}
		for p, v := range vals {
			o.ObserveFloat64(g, max(v, 0).Seconds(), metric.WithAttributes(pipelineAttr(p)))
		}
		return nil
	}, g)
	return err
}

func (m *Metrics) claim(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.registered[name] {
		return errors.New("metrics: " + name + " callback already registered")
	}
	m.registered[name] = true
	return nil
}
