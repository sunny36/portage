// Package pipeline wires change sources, the job queue, transfer, verify and
// the file record into a running sync.
//
//	change source ─┐                       ┌─ CopyWorker ── transfer.Copy ── record.Complete
//	               ├─ emit ── Observe ── River ┤
//	reconciler  ───┘   (record + enqueue)    └─ DeleteWorker ── record.MarkDeleted
//
// Every step is idempotent: events are at-least-once, jobs dedupe on
// (pipeline, key, version), and the file record decides whether a copy is
// still needed.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"golang.org/x/sync/errgroup"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/change/azqueue"
	"github.com/sunny36/portage/internal/change/webhook"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/metrics"
	"github.com/sunny36/portage/internal/queue"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/transfer"
)

const (
	// maxBufferBytes caps memory held by in-flight part buffers across all
	// pipelines. Copies wait for buffers beyond this (backpressure).
	maxBufferBytes = 2 << 30
	// sourceRestartBackoff is how long a failed change source waits before
	// restarting. The reconciler keeps the pipeline syncing meanwhile.
	sourceRestartBackoff = time.Minute
	shutdownTimeout      = 30 * time.Second
)

// Engine holds what workers share. One per process.
type Engine struct {
	store     record.Store
	pipelines map[string]*pipelineRuntime
	pool      *transfer.BufferPool
	db        *pgxpool.Pool
	client    *river.Client[pgx.Tx]
	m         *metrics.Metrics
	log       *slog.Logger
}

type pipelineRuntime struct {
	cfg    config.Pipeline
	src    connector.Connector
	dst    connector.Connector
	filter *change.Filter
	source change.Source // nil when events.type is none
	// ignoreBefore implements existing_files: skip.
	ignoreBefore time.Time
}

// Run starts every pipeline in cfg and blocks until ctx is cancelled (clean
// shutdown, returns nil) or a fatal error occurs. m may be nil.
func Run(ctx context.Context, cfg *config.File, m *metrics.Metrics, log *slog.Logger) error {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := record.Migrate(ctx, db); err != nil {
		return err
	}
	if err := queue.Migrate(ctx, db); err != nil {
		return err
	}

	e := &Engine{
		store:     record.NewPGStore(db),
		pipelines: map[string]*pipelineRuntime{},
		db:        db,
		m:         m,
		log:       log,
	}
	mux := http.NewServeMux()
	var maxPart int64
	for _, pc := range cfg.Pipelines {
		p, err := e.buildPipeline(ctx, pc, mux)
		if err != nil {
			return fmt.Errorf("pipeline %s: %w", pc.Name, err)
		}
		e.pipelines[pc.Name] = p
		maxPart = max(maxPart, int64(pc.PartSize)*int64(pc.PartConcurrency+1))
	}
	e.pool = transfer.NewBufferPool(max(maxBufferBytes, maxPart))

	workers := river.NewWorkers()
	river.AddWorker(workers, &CopyWorker{e: e})
	river.AddWorker(workers, &DeleteWorker{e: e})
	river.AddWorker(workers, &ReconcileWorker{e: e})
	e.client, err = queue.NewClient(db, workers, queue.QueuesFor(cfg.Pipelines), e.periodicReconciles(cfg)...)
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}
	if err := e.registerGauges(); err != nil {
		return err
	}

	if err := e.client.Start(ctx); err != nil {
		return fmt.Errorf("start workers: %w", err)
	}
	log.Info("engine started", "pipelines", len(e.pipelines))

	g, gctx := errgroup.WithContext(ctx)
	for _, p := range e.pipelines {
		if p.source != nil {
			g.Go(func() error { e.superviseSource(gctx, p); return nil })
		}
	}
	if hasWebhook(cfg) {
		g.Go(func() error { return serveWebhooks(gctx, cfg.WebhookAddr, mux, log) })
	}
	<-gctx.Done()
	runErr := g.Wait()

	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	log.Info("stopping workers")
	if err := e.client.Stop(stopCtx); err != nil {
		log.Warn("workers did not stop cleanly; unfinished copies resume on next start", "err", err)
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

func openDB(ctx context.Context, cfg *config.File) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database_url: %w", err)
	}
	// Each copy worker uses a connection briefly (claim, heartbeat,
	// complete); River and sources need a few more.
	var conns int32 = 8
	for _, p := range cfg.Pipelines {
		conns += int32(p.Concurrency)
	}
	pc.MaxConns = max(pc.MaxConns, min(conns, 64))
	db, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return db, nil
}

func (e *Engine) buildPipeline(ctx context.Context, pc config.Pipeline, mux *http.ServeMux) (*pipelineRuntime, error) {
	src, err := newConnector(ctx, pc.Source)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	dst, err := newConnector(ctx, pc.Destination)
	if err != nil {
		return nil, fmt.Errorf("destination: %w", err)
	}
	// Fail fast on a missing bucket or bad credentials rather than on the
	// first copy. Listing the destination also proves read access, which
	// verification needs.
	if _, err := src.List(ctx, "", "", 1); err != nil {
		return nil, fmt.Errorf("source check: %w", err)
	}
	if _, err := dst.List(ctx, "", "", 1); err != nil {
		return nil, fmt.Errorf("destination check: %w", err)
	}
	filter, err := change.NewFilter(pc.Filters)
	if err != nil {
		return nil, fmt.Errorf("filters: %w", err)
	}
	p := &pipelineRuntime{cfg: pc, src: src, dst: dst, filter: filter}

	if pc.ExistingFiles == "skip" {
		if p.ignoreBefore, err = e.firstStarted(ctx, pc.Name); err != nil {
			return nil, err
		}
	}

	log := e.log.With("pipeline", pc.Name)
	switch pc.Events.Type {
	case "azure_queue":
		p.source, err = azqueue.New(azqueue.Options{
			PipelineID:      pc.Name,
			QueueAccountURL: pc.Events.QueueAccountURL,
			QueueName:       pc.Events.QueueName,
			Auth:            *pc.Source.Azure,
			Container:       pc.Source.Azure.Container,
			Prefix:          pc.Source.Prefix,
			Filter:          filter,
			Log:             log,
		})
		if err != nil {
			return nil, fmt.Errorf("events: %w", err)
		}
	case "webhook":
		if pc.Source.Azure == nil {
			return nil, errors.New("events: webhook currently supports azure sources only")
		}
		src := webhook.NewSource(webhook.HandlerOptions{
			PipelineID: pc.Name,
			Secret:     pc.Events.WebhookSecret,
			Container:  pc.Source.Azure.Container,
			Prefix:     pc.Source.Prefix,
			Filter:     filter,
			Log:        log,
		})
		mux.Handle(pc.Events.WebhookPath, src.Handler())
		p.source = src
	}
	return p, nil
}

func newConnector(ctx context.Context, ep config.Endpoint) (connector.Connector, error) {
	switch ep.Provider() {
	case "azure":
		return azure.New(ctx, *ep.Azure, ep.Prefix)
	case "s3":
		return s3.New(ctx, *ep.S3, ep.Prefix)
	}
	return nil, errors.New("no provider configured")
}

// firstStarted returns when this pipeline first ran, recording now if never.
func (e *Engine) firstStarted(ctx context.Context, pipelineID string) (time.Time, error) {
	var t time.Time
	err := e.db.QueryRow(ctx, `
		INSERT INTO pipeline_state (pipeline_id) VALUES ($1)
		ON CONFLICT (pipeline_id) DO UPDATE SET pipeline_id = EXCLUDED.pipeline_id
		RETURNING first_started_at`, pipelineID).Scan(&t)
	if err != nil {
		return time.Time{}, fmt.Errorf("pipeline state: %w", err)
	}
	return t, nil
}

// emit records a change and queues the work for it. It returns only once
// the job is durably queued, so sources may then ack their message.
func (e *Engine) emit(ctx context.Context, c change.ObjectChanged) error {
	p, ok := e.pipelines[c.PipelineID]
	if !ok {
		return fmt.Errorf("unknown pipeline %q", c.PipelineID)
	}
	e.m.EventReceived(ctx, c.PipelineID, string(c.Origin), string(c.Kind))
	if c.Kind == change.KindDelete {
		_, err := e.client.Insert(ctx, queue.DeleteArgs{
			PipelineID: c.PipelineID, Key: c.Key, EventTime: c.EventTime,
		}, &river.InsertOpts{Queue: queue.CopyQueue(p.cfg.Name)})
		return err
	}
	if c.Version != "" {
		// Track the pending change so status and lag reflect it before a
		// worker picks it up.
		if err := e.store.Observe(ctx, record.Candidate{
			PipelineID: c.PipelineID, Key: c.Key, Version: c.Version, Size: c.Size,
			MTime: c.EventTime, Sequencer: c.Sequencer, EventTime: c.EventTime,
		}); err != nil {
			return fmt.Errorf("observe: %w", err)
		}
	}
	return queue.InsertCopy(ctx, e.client, queue.CopyArgs{
		PipelineID: c.PipelineID,
		Key:        c.Key,
		Version:    c.Version,
		Size:       c.Size,
		Sequencer:  c.Sequencer,
		EventTime:  c.EventTime,
		DetectedAt: c.DetectedAt,
		Origin:     string(c.Origin),
	})
}

// superviseSource runs a change source, restarting it after failures. A
// source error (expired credentials, deleted queue) must be loud but must
// not stop the engine: the reconciler keeps the pipeline syncing.
func (e *Engine) superviseSource(ctx context.Context, p *pipelineRuntime) {
	log := e.log.With("pipeline", p.cfg.Name, "source", p.source.Name())
	for {
		err := p.source.Run(ctx, e.emit)
		if ctx.Err() != nil {
			return
		}
		log.Error("change source stopped; restarting (reconciler continues meanwhile)", "err", err, "in", sourceRestartBackoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(sourceRestartBackoff):
		}
	}
}

func hasWebhook(cfg *config.File) bool {
	for _, p := range cfg.Pipelines {
		if p.Events.Type == "webhook" {
			return true
		}
	}
	return false
}

func serveWebhooks(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("webhook listener started", "addr", addr)
	select {
	case err := <-errc:
		return fmt.Errorf("webhook listener: %w", err)
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		return nil
	}
}

// registerGauges exposes queue depth and the oldest pending change (the lag
// building up right now) as scrape-time gauges.
func (e *Engine) registerGauges() error {
	if e.m == nil {
		return nil
	}
	err := e.m.RegisterQueueDepth(func(ctx context.Context) (map[string]int64, error) {
		rows, err := e.db.Query(ctx, `
			SELECT queue, count(*) FROM river_job
			WHERE state IN ('available', 'scheduled', 'retryable', 'running')
			GROUP BY queue`)
		if err != nil {
			return nil, err
		}
		byQueue, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
			Queue string
			N     int64
		}])
		if err != nil {
			return nil, err
		}
		out := map[string]int64{}
		for name := range e.pipelines {
			out[name] = 0
		}
		for _, q := range byQueue {
			for name := range e.pipelines {
				if q.Queue == queue.CopyQueue(name) {
					out[name] = q.N
				}
			}
		}
		return out, nil
	})
	if err != nil {
		return err
	}
	return e.m.RegisterOldestPending(func(ctx context.Context) (map[string]time.Duration, error) {
		rows, err := e.db.Query(ctx, `
			SELECT pipeline_id, now() - min(event_time) FROM file_record
			WHERE status IN ('pending', 'copying') AND event_time IS NOT NULL
			GROUP BY pipeline_id`)
		if err != nil {
			return nil, err
		}
		out := map[string]time.Duration{}
		for name := range e.pipelines {
			out[name] = 0
		}
		var (
			id  string
			age time.Duration
		)
		_, err = pgx.ForEachRow(rows, []any{&id, &age}, func() error {
			if _, ok := out[id]; ok {
				out[id] = age
			}
			return nil
		})
		return out, err
	})
}
