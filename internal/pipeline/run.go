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
//
// Pipelines can be added, replaced and removed while the engine runs
// (manager.go): from the config file they are started once; from the
// pipeline_spec table (pipelines_from: database) a watcher applies changes
// as they happen (watch.go).
package pipeline

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/sunny36/portage/internal/change"
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
	// shutdownTimeout is how long running copies get to finish when the
	// engine or one pipeline stops, before they are cancelled (and release
	// their lease, keeping their upload session for the next start).
	shutdownTimeout = 30 * time.Second
	// dbReconcileWorkers sizes the shared reconcile queue with pipelines
	// from the database, whose number changes at runtime.
	dbReconcileWorkers = 16
	// dbMaxConns is the connection pool size with pipelines from the
	// database (with a file it is sized from the pipelines' concurrency).
	dbMaxConns = 64
)

// Engine holds what workers share. One per process.
type Engine struct {
	store  record.Store
	pool   *transfer.BufferPool
	db     *pgxpool.Pool
	client *river.Client[pgx.Tx]
	m      *metrics.Metrics
	log    *slog.Logger

	// ctx is the parent of every pipeline's run context; cancelled when the
	// engine shuts down.
	ctx context.Context
	// wg tracks change sources, the spec watcher and the webhook server.
	wg sync.WaitGroup
	// fatal receives an error that must stop the engine (webhook listener).
	fatal chan error

	webhookAddr string
	hooks       webhookRouter
	hookServer  sync.Once

	mu sync.RWMutex
	// pipelines are the running pipelines by name.
	pipelines map[string]*pipelineRuntime
	// wanted are the pipelines that are configured and enabled, running or
	// not (being restarted, or failing their start checks). Jobs for other
	// pipelines are cancelled.
	wanted map[string]bool
}

// Run starts the pipelines (from cfg, or from the pipeline_spec table when
// cfg.FromDatabase()) and blocks until ctx is cancelled (clean shutdown,
// returns nil) or a fatal error occurs. m may be nil.
//
// With pipelines from the file, a pipeline that fails its start checks
// stops Run with an error. With pipelines from the database, it is logged,
// counted in portage_pipeline_config_errors and retried on the next refresh;
// it never affects other pipelines.
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

	poolSize := bufferPoolSize(cfg)
	setMemoryLimit(poolSize, log)
	e := &Engine{
		store:       record.NewPGStore(db),
		pool:        transfer.NewBufferPool(poolSize),
		db:          db,
		m:           m,
		log:         log,
		fatal:       make(chan error, 1),
		webhookAddr: cfg.WebhookAddr,
		pipelines:   map[string]*pipelineRuntime{},
		wanted:      map[string]bool{},
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &CopyWorker{e: e})
	river.AddWorker(workers, &DeleteWorker{e: e})
	river.AddWorker(workers, &ReconcileWorker{e: e})
	reconcileWorkers := max(1, len(cfg.Pipelines))
	if cfg.FromDatabase() {
		reconcileWorkers = dbReconcileWorkers
	}
	// Copy queues and periodic reconciles are added per pipeline as it
	// starts (manager.go).
	e.client, err = queue.NewClient(db, workers, map[string]river.QueueConfig{
		queue.QueueReconcile: {MaxWorkers: reconcileWorkers},
	})
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}
	if err := e.registerGauges(); err != nil {
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	e.ctx = runCtx
	abort := func(err error) error {
		cancelRun()
		e.wg.Wait()
		return err
	}

	// Start what's configured before the workers, so leftover jobs from a
	// previous run find their pipeline.
	var w *watcher
	if cfg.FromDatabase() {
		w = newWatcher(e)
		if err := w.refresh(ctx); err != nil {
			return abort(err)
		}
	} else {
		e.mu.Lock()
		for _, pc := range cfg.Pipelines {
			e.wanted[pc.Name] = true
		}
		e.mu.Unlock()
		for _, pc := range cfg.Pipelines {
			if err := e.startPipeline(ctx, pc, 0); err != nil {
				// A destination that's down or a bucket not created yet must
				// not take the whole engine (and its other pipelines) down,
				// nor need an external supervisor: retry in the background.
				log.Error("pipeline failed its start checks; retrying in the background", "pipeline", pc.Name, "err", err)
				e.m.PipelineConfigError(ctx, pc.Name)
				e.wg.Go(func() { e.retryStart(runCtx, pc) })
			}
		}
	}

	// Workers get a context that outlives the shutdown signal: on shutdown
	// running copies get shutdownTimeout to finish and record their result,
	// instead of being cut off mid-write.
	if err := e.client.Start(context.WithoutCancel(ctx)); err != nil {
		return abort(fmt.Errorf("start workers: %w", err))
	}
	if w != nil {
		e.wg.Go(func() { w.run(runCtx) })
	}
	log.Info("engine started", "pipelines", len(e.running()), "pipelines_from", cmp.Or(cfg.PipelinesFrom, config.PipelinesFromFile))

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-e.fatal:
	}
	cancelRun()
	e.wg.Wait()

	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	log.Info("stopping workers; waiting for running copies", "timeout", shutdownTimeout)
	if err := e.client.Stop(stopCtx); err != nil {
		// Cancel what's left. Interrupted copies release their lease and keep
		// their upload session, so the next start resumes them.
		log.Warn("copies still running after timeout; cancelling them (they resume on next start)")
		hardCtx, hardCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer hardCancel()
		if err := e.client.StopAndCancel(hardCtx); err != nil {
			log.Warn("workers did not stop cleanly", "err", err)
		}
	}
	for _, p := range e.running() {
		p.stopWork()
	}
	return runErr
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
	if cfg.FromDatabase() {
		conns = dbMaxConns
	}
	pc.MaxConns = max(pc.MaxConns, min(conns, dbMaxConns))
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

// bufferPoolSize: maxBufferBytes, or more if a configured pipeline's parts
// need it. Pipelines from the database must fit maxBufferBytes.
func bufferPoolSize(cfg *config.File) int64 {
	var need int64
	for _, pc := range cfg.Pipelines {
		need = max(need, bufferNeed(pc))
	}
	return max(maxBufferBytes, need)
}

// bufferNeed is the most buffer memory one copy of pc holds at once.
func bufferNeed(pc config.Pipeline) int64 {
	return int64(pc.PartSize) * int64(pc.PartConcurrency+1)
}

// connectorFactory builds endpoint connectors. Tests in this package swap
// it to wrap connectors with fault injection.
var connectorFactory = newConnector

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

// lookup returns the running pipeline named name.
func (e *Engine) lookup(name string) (*pipelineRuntime, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.pipelines[name]
	return p, ok
}

// running returns the running pipelines, ordered by name.
func (e *Engine) running() []*pipelineRuntime {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*pipelineRuntime, 0, len(e.pipelines))
	for _, name := range slices.Sorted(maps.Keys(e.pipelines)) {
		out = append(out, e.pipelines[name])
	}
	return out
}

// emit records a change and queues the work for it. It returns only once
// the job is durably queued, so sources may then ack their message.
func (e *Engine) emit(ctx context.Context, c change.ObjectChanged) error {
	p, ok := e.lookup(c.PipelineID)
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
	gen := 0
	switch rec, err := e.store.Get(ctx, c.PipelineID, c.Key); {
	case err == nil:
		gen = rec.Attempts
	case !errors.Is(err, record.ErrNotFound):
		return fmt.Errorf("read record: %w", err)
	}
	return queue.InsertCopy(ctx, e.client, queue.CopyArgs{
		Generation: gen,
		Window:     queue.WindowAt(time.Now(), queue.CopyDedupeWindow),
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

// superviseSource runs a change source until the pipeline stops, restarting
// it after failures. A source error (expired credentials, deleted queue)
// must be loud but must not stop the pipeline: the reconciler keeps it
// syncing.
func (e *Engine) superviseSource(p *pipelineRuntime) {
	ctx := p.runCtx
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

// ensureWebhookServer starts the shared webhook listener the first time a
// pipeline with webhook events starts. A listener failure stops the engine.
func (e *Engine) ensureWebhookServer() {
	e.hookServer.Do(func() {
		e.wg.Go(func() {
			if err := serveWebhooks(e.ctx, e.webhookAddr, &e.hooks, e.log); err != nil {
				select {
				case e.fatal <- err:
				default:
				}
			}
		})
	})
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
// building up right now) as scrape-time gauges, for the pipelines running
// at scrape time.
func (e *Engine) registerGauges() error {
	if e.m == nil {
		return nil
	}
	names := func() []string {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return slices.Collect(maps.Keys(e.pipelines))
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
		depth := make(map[string]int64, len(byQueue))
		for _, q := range byQueue {
			depth[q.Queue] = q.N
		}
		out := map[string]int64{}
		for _, name := range names() {
			out[name] = depth[queue.CopyQueue(name)]
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
		for _, name := range names() {
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

// memoryOverhead is the headroom above the part-buffer pool for everything
// else (SDK clients, River, connection pools, goroutine stacks).
const memoryOverhead = 1 << 30

// setMemoryLimit sets a soft Go memory limit of pool + overhead unless the
// operator set GOMEMLIMIT. Buffers dropped by the pool are garbage the GC
// otherwise collects lazily; the limit makes it collect before the heap
// grows far past what the engine actually holds.
func setMemoryLimit(poolSize int64, log *slog.Logger) {
	if os.Getenv("GOMEMLIMIT") != "" {
		log.Info("memory limit from GOMEMLIMIT", "limit", os.Getenv("GOMEMLIMIT"))
		return
	}
	limit := poolSize + memoryOverhead
	debug.SetMemoryLimit(limit)
	log.Info("soft memory limit set (override with GOMEMLIMIT)", "limit_mib", limit>>20, "buffer_pool_mib", poolSize>>20)
}

// startRetryBase and startRetryMax bound the backoff for a file-mode
// pipeline that failed its start checks (variables so tests can shorten them).
var (
	startRetryBase = 15 * time.Second
	startRetryMax  = 5 * time.Minute
)

// retryStart keeps trying to start pc until it starts or ctx ends. Its
// queued jobs wait (snooze) meanwhile, so nothing is lost.
func (e *Engine) retryStart(ctx context.Context, pc config.Pipeline) {
	log := e.log.With("pipeline", pc.Name)
	delay := startRetryBase
	for attempt := 2; ; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		err := e.startPipeline(ctx, pc, 0)
		if err == nil {
			log.Info("pipeline started after failed start checks", "attempt", attempt)
			return
		}
		if ctx.Err() != nil {
			return
		}
		delay = min(2*delay, startRetryMax)
		log.Error("pipeline still failing its start checks", "attempt", attempt, "retry_in", delay, "err", err)
		e.m.PipelineConfigError(ctx, pc.Name)
	}
}
