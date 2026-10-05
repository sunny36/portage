package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/change/azqueue"
	"github.com/sunny36/portage/internal/change/webhook"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/queue"
)

// restartSnooze is how long a job waits when its pipeline is configured but
// not running right now: being restarted, or failing its start checks.
const restartSnooze = 30 * time.Second

// pipelineRuntime is one running instance of a pipeline. A changed pipeline
// gets a new instance; the old one is stopped first.
type pipelineRuntime struct {
	cfg config.Pipeline
	// revision is the pipeline_spec revision (0 for pipelines from a file).
	revision int64
	src      connector.Connector
	dst      connector.Connector
	filter   *change.Filter
	source   change.Source // nil when events.type is none
	webhook  http.Handler  // set when events.type is webhook
	// ignoreBefore implements existing_files: skip.
	ignoreBefore time.Time

	// runCtx stops the change source and running reconciles. workCtx
	// cancels running copies and deletes; it is cancelled only once they
	// had shutdownTimeout to finish.
	runCtx   context.Context
	stopRun  context.CancelFunc
	workCtx  context.Context
	stopWork context.CancelFunc
	// sourceDone is closed once the change source has returned.
	sourceDone chan struct{}
}

func periodicJobID(name string) string { return "reconcile:" + name }

// buildPipeline creates a pipeline's connectors and change source and runs
// its start checks. Nothing is started.
func (e *Engine) buildPipeline(ctx context.Context, pc config.Pipeline, revision int64) (*pipelineRuntime, error) {
	if need := bufferNeed(pc); need > e.pool.Cap() {
		return nil, fmt.Errorf("part_size × (part_concurrency+1) = %d bytes exceeds the engine's buffer pool (%d bytes)", need, e.pool.Cap())
	}
	src, err := connectorFactory(ctx, pc.Source)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	dst, err := connectorFactory(ctx, pc.Destination)
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
	p := &pipelineRuntime{cfg: pc, revision: revision, src: src, dst: dst, filter: filter}

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
		p.webhook = src.Handler()
		p.source = src
	}
	return p, nil
}

// startPipeline builds, checks and starts a pipeline.
func (e *Engine) startPipeline(ctx context.Context, pc config.Pipeline, revision int64) error {
	p, err := e.buildPipeline(ctx, pc, revision)
	if err != nil {
		return err
	}
	return e.launch(p)
}

// launch starts a built pipeline: registers it for workers and emit, adds
// its copy queue and periodic reconcile (whose first run starts at once)
// and starts its change source. On error nothing is left running.
func (e *Engine) launch(p *pipelineRuntime) (err error) {
	name := p.cfg.Name
	p.runCtx, p.stopRun = context.WithCancel(e.ctx)
	p.workCtx, p.stopWork = context.WithCancel(context.Background())
	p.sourceDone = make(chan struct{})
	var undo []func()
	defer func() {
		if err != nil {
			for _, f := range undo {
				f()
			}
			p.stopRun()
			p.stopWork()
		}
	}()

	if p.webhook != nil {
		if err := e.hooks.add(p.cfg.Events.WebhookPath, p.webhook); err != nil {
			return err
		}
		undo = append(undo, func() { e.hooks.remove(p.cfg.Events.WebhookPath) })
		e.ensureWebhookServer()
	}

	e.mu.Lock()
	if cur, ok := e.pipelines[name]; ok {
		e.mu.Unlock()
		return fmt.Errorf("already running (revision %d)", cur.revision)
	}
	e.pipelines[name] = p
	e.mu.Unlock()
	undo = append(undo, func() {
		e.mu.Lock()
		if e.pipelines[name] == p {
			delete(e.pipelines, name)
		}
		e.mu.Unlock()
	})

	if err := e.addQueue(p); err != nil {
		return err
	}
	// Runs on the elected leader only; every engine adds it so whichever
	// is leader schedules it.
	if _, err := e.client.PeriodicJobs().AddSafely(e.periodicReconcile(p)); err != nil {
		e.removeQueue(p)
		return fmt.Errorf("schedule reconcile: %w", err)
	}

	if p.source != nil {
		e.wg.Go(func() {
			defer close(p.sourceDone)
			e.superviseSource(p)
		})
	} else {
		close(p.sourceDone)
	}
	e.log.Info("pipeline started", "pipeline", name, "revision", p.revision,
		"source", endpointName(p.cfg.Source), "destination", endpointName(p.cfg.Destination), "events", p.cfg.Events.Type)
	return nil
}

// addQueue adds the pipeline's copy queue to this client, so its jobs start
// being worked here (it is removed again on stop).
func (e *Engine) addQueue(p *pipelineRuntime) error {
	name := queue.CopyQueue(p.cfg.Name)
	err := e.client.Queues().Add(name, queue.CopyQueueConfig(p.cfg))
	var exists *river.QueueAlreadyAddedError
	if errors.As(err, &exists) {
		// A previous stop could not finish removing it; try once more.
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if rerr := e.client.Queues().Remove(rctx, name); rerr != nil {
			return fmt.Errorf("add queue: %w (removing the previous one: %w)", err, rerr)
		}
		err = e.client.Queues().Add(name, queue.CopyQueueConfig(p.cfg))
	}
	if err != nil {
		return fmt.Errorf("add queue: %w", err)
	}
	return nil
}

func (e *Engine) removeQueue(p *pipelineRuntime) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.client.Queues().Remove(ctx, queue.CopyQueue(p.cfg.Name)); err != nil {
		e.log.Warn("removing copy queue", "pipeline", p.cfg.Name, "err", err)
	}
}

// stopPipeline stops one running pipeline gracefully: its change source and
// reconciles stop, its periodic reconcile is removed, and its copy queue
// stops taking jobs on this engine. Running copies get shutdownTimeout to
// finish; then they are cancelled, release their lease and keep their
// upload session, so a later instance of the pipeline resumes them. Its
// file records stay. Waiting jobs stay queued for the next instance.
func (e *Engine) stopPipeline(p *pipelineRuntime, reason string) {
	name := p.cfg.Name
	log := e.log.With("pipeline", name, "revision", p.revision)
	log.Info("stopping pipeline", "reason", reason)
	e.mu.Lock()
	if e.pipelines[name] == p {
		delete(e.pipelines, name)
	}
	e.mu.Unlock()

	e.client.PeriodicJobs().RemoveByID(periodicJobID(name))
	p.stopRun()
	if p.webhook != nil {
		e.hooks.remove(p.cfg.Events.WebhookPath)
	}
	<-p.sourceDone

	// Queues().Remove stops fetching at once and waits for running jobs.
	cancelCopies := time.AfterFunc(shutdownTimeout, func() {
		log.Warn("copies still running after timeout; cancelling them (they resume when the pipeline runs again)", "timeout", shutdownTimeout)
		p.stopWork()
	})
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout+15*time.Second)
	err := e.client.Queues().Remove(ctx, queue.CopyQueue(name))
	cancel()
	cancelCopies.Stop()
	p.stopWork()
	if err != nil {
		log.Error("pipeline's copy queue did not stop cleanly", "err", err)
	}
	log.Info("pipeline stopped", "reason", reason)
}

// runtimeFor returns the running pipeline a job belongs to, or the error the
// job must return: snooze while the pipeline is configured but not running
// (restarting, or failing its start checks), cancel once it is no longer
// configured (deleted or disabled).
func (e *Engine) runtimeFor(name string) (*pipelineRuntime, error) {
	e.mu.RLock()
	p, ok := e.pipelines[name]
	wanted := e.wanted[name]
	e.mu.RUnlock()
	switch {
	case ok:
		return p, nil
	case wanted:
		return nil, river.JobSnooze(restartSnooze)
	}
	return nil, river.JobCancel(fmt.Errorf("pipeline %q is not configured", name))
}

// withStop returns a copy of ctx that is also cancelled when stop is done.
func withStop(ctx, stop context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	unregister := context.AfterFunc(stop, cancel)
	return ctx, func() {
		unregister()
		cancel()
	}
}

// periodicReconcile schedules a pipeline's reconcile at its interval,
// starting immediately (that first run is the initial copy, or catches up
// after the pipeline was stopped).
func (e *Engine) periodicReconcile(p *pipelineRuntime) *river.PeriodicJob {
	args := queue.ReconcileArgs{PipelineID: p.cfg.Name, Revision: p.revision}
	return river.NewPeriodicJob(
		river.PeriodicInterval(p.cfg.ReconcileInterval),
		func() (river.JobArgs, *river.InsertOpts) {
			return args, &river.InsertOpts{
				Queue: queue.QueueReconcile,
				// Never stack reconciles for one pipeline. Completed jobs
				// must not count, or every later run would be deduped away.
				UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
					rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
					rivertype.JobStateRetryable, rivertype.JobStateScheduled,
				}},
			}
		},
		&river.PeriodicJobOpts{ID: periodicJobID(p.cfg.Name), RunOnStart: true},
	)
}

func endpointName(ep config.Endpoint) string {
	switch ep.Provider() {
	case "azure":
		return "azure:" + ep.Azure.Container + "/" + ep.Prefix
	case "s3":
		return "s3:" + ep.S3.Bucket + "/" + ep.Prefix
	}
	return ""
}

// webhookRouter serves the webhook paths of the running pipelines; paths
// come and go as pipelines start and stop.
type webhookRouter struct {
	mu     sync.Mutex
	routes map[string]http.Handler
	mux    atomic.Pointer[http.ServeMux]
}

func (r *webhookRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	mux := r.mux.Load()
	if mux == nil {
		http.NotFound(w, req)
		return
	}
	mux.ServeHTTP(w, req)
}

func (r *webhookRouter) add(path string, h http.Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.routes[path]; ok {
		return fmt.Errorf("events.webhook_path %s is already used by another pipeline", path)
	}
	next := make(map[string]http.Handler, len(r.routes)+1)
	for p, h := range r.routes {
		next[p] = h
	}
	next[path] = h
	if err := r.rebuild(next); err != nil {
		return fmt.Errorf("events.webhook_path %s: %w", path, err)
	}
	return nil
}

func (r *webhookRouter) remove(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]http.Handler, len(r.routes))
	for p, h := range r.routes {
		if p != path {
			next[p] = h
		}
	}
	_ = r.rebuild(next) // every remaining path was accepted before
}

// rebuild swaps in a mux serving routes. http.ServeMux panics on a pattern
// it can't parse; that becomes an error.
func (r *webhookRouter) rebuild(routes map[string]http.Handler) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("invalid path: %v", v)
		}
	}()
	mux := http.NewServeMux()
	for p, h := range routes {
		mux.Handle(p, h)
	}
	r.routes = routes
	r.mux.Store(mux)
	return nil
}
