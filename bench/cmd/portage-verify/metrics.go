package main

import (
	"context"
	"math"
	"time"
)

// Engine metric names (internal/metrics). Duplicated as literals so this
// tool also reads a /metrics endpoint of a different engine build.
const (
	mSyncLag       = "portage_sync_lag_seconds"
	mCopyDuration  = "portage_copy_duration_seconds"
	mFiles         = "portage_files_total"
	mBytesCopied   = "portage_bytes_copied_total"
	mBytesUploaded = "portage_bytes_uploaded_total"
	mEvents        = "portage_events_total"
	mRetries       = "portage_retries_total"
	mQueueDepth    = "portage_queue_depth"
	mOldestPending = "portage_oldest_pending_seconds"
	mProcessStart  = "process_start_time_seconds"
	mRSS           = "process_resident_memory_bytes"
	mGoroutines    = "go_goroutines"
)

// quantiles is a p50/p95/p99 summary. Nil fields mean "no data" (JSON has
// no NaN).
type quantiles struct {
	Count float64  `json:"count"`
	Mean  *float64 `json:"mean,omitempty"`
	P50   *float64 `json:"p50,omitempty"`
	P95   *float64 `json:"p95,omitempty"`
	P99   *float64 `json:"p99,omitempty"`
}

// metricsReport is what one or two scrapes of the engine say. Counters are
// since the engine process started (they reset on restart).
type metricsReport struct {
	URL       string    `json:"url"`
	ScrapedAt time.Time `json:"scraped_at"`
	Pipeline  string    `json:"pipeline,omitempty"`
	Note      string    `json:"note"`

	SyncLagSeconds      quantiles `json:"sync_lag_seconds"`
	CopyDurationSeconds quantiles `json:"copy_duration_seconds"`

	FilesByOutcome     map[string]float64 `json:"files_by_outcome"`
	EventsBySource     map[string]float64 `json:"events_by_source"`
	RetriesByReason    map[string]float64 `json:"retries_by_reason,omitempty"`
	BytesCopied        float64            `json:"bytes_copied"`
	BytesUploaded      float64            `json:"bytes_uploaded"`
	EngineUptimeSecond float64            `json:"engine_uptime_seconds,omitempty"`
	// AvgBytesPerSecond is BytesCopied over the engine's uptime.
	AvgBytesPerSecond float64 `json:"avg_bytes_per_second,omitempty"`
	// WindowBytesPerSecond is the bytes_copied rate between two scrapes
	// -metrics-window apart (0 when not measured).
	WindowBytesPerSecond float64 `json:"window_bytes_per_second,omitempty"`
	WindowSeconds        float64 `json:"window_seconds,omitempty"`

	QueueDepth           float64 `json:"queue_depth"`
	OldestPendingSeconds float64 `json:"oldest_pending_seconds"`
	RSSBytes             float64 `json:"rss_bytes,omitempty"`
	Goroutines           float64 `json:"goroutines,omitempty"`
}

const bucketNote = "Quantiles are estimated from histogram buckets exactly like PromQL histogram_quantile " +
	"(linear interpolation inside the bucket the rank falls in), so they are only as precise as the " +
	"bucket boundaries (sync lag: 0.5,1,2,5,10,15,20,30,40,50,60,75,90,120,180,300,600,1200,1800,3600s). " +
	"Counters are since the engine process last started."

func summarizeHistogram(samples []sample, name string, want map[string]string) quantiles {
	bs := histogramBuckets(samples, name, want)
	q := quantiles{Count: sum(samples, name+"_count", want)}
	if q.Count > 0 {
		q.Mean = num(sum(samples, name+"_sum", want) / q.Count)
	}
	q.P50 = num(histogramQuantile(0.50, bs))
	q.P95 = num(histogramQuantile(0.95, bs))
	q.P99 = num(histogramQuantile(0.99, bs))
	return q
}

func num(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

// collectMetrics scrapes url (twice, window apart, when window > 0) and
// summarises the engine's view of pipeline ("" = all pipelines).
func collectMetrics(ctx context.Context, url, pipeline string, window time.Duration, now func() time.Time) (*metricsReport, error) {
	var first []sample
	var firstAt time.Time
	if window > 0 {
		var err error
		if first, err = scrape(ctx, url); err != nil {
			return nil, err
		}
		firstAt = now()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(window):
		}
	}
	samples, err := scrape(ctx, url)
	if err != nil {
		return nil, err
	}
	at := now()
	r := buildMetricsReport(samples, pipeline, at)
	r.URL = url
	if first != nil {
		r.WindowSeconds = at.Sub(firstAt).Seconds()
		if r.WindowSeconds > 0 {
			want := pipelineLabel(pipeline)
			r.WindowBytesPerSecond = (sum(samples, mBytesCopied, want) - sum(first, mBytesCopied, want)) / r.WindowSeconds
		}
	}
	return r, nil
}

func pipelineLabel(pipeline string) map[string]string {
	if pipeline == "" {
		return nil
	}
	return map[string]string{"pipeline": pipeline}
}

func buildMetricsReport(samples []sample, pipeline string, at time.Time) *metricsReport {
	want := pipelineLabel(pipeline)
	r := &metricsReport{
		ScrapedAt:            at.UTC(),
		Pipeline:             pipeline,
		Note:                 bucketNote,
		SyncLagSeconds:       summarizeHistogram(samples, mSyncLag, want),
		CopyDurationSeconds:  summarizeHistogram(samples, mCopyDuration, want),
		FilesByOutcome:       sumBy(samples, mFiles, "outcome", want),
		EventsBySource:       sumBy(samples, mEvents, "source", want),
		RetriesByReason:      sumBy(samples, mRetries, "reason", want),
		BytesCopied:          sum(samples, mBytesCopied, want),
		BytesUploaded:        sum(samples, mBytesUploaded, want),
		QueueDepth:           sum(samples, mQueueDepth, want),
		OldestPendingSeconds: sum(samples, mOldestPending, want),
		RSSBytes:             sum(samples, mRSS, nil),
		Goroutines:           sum(samples, mGoroutines, nil),
	}
	if start := sum(samples, mProcessStart, nil); start > 0 {
		r.EngineUptimeSecond = float64(at.UnixNano())/1e9 - start
		if r.EngineUptimeSecond > 0 {
			r.AvgBytesPerSecond = r.BytesCopied / r.EngineUptimeSecond
		}
	}
	return r
}
