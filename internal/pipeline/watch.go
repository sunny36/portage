package pipeline

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/record"
)

// Pipelines from the database (pipelines_from: database): the watcher keeps
// the running pipelines in line with the pipeline_spec table. A LISTEN on
// record.PipelinesChannel triggers a re-read as soon as a row changes, and
// the table is re-read every specRefreshInterval anyway, so a missed
// notification (or a lost LISTEN connection) only delays a change.
// See docs/adr/0004-pipelines-from-database.md.

var (
	// specRefreshInterval is the full re-read period. A variable so tests
	// can shorten it.
	specRefreshInterval = 30 * time.Second
	// listenBackoffMax caps the wait between LISTEN reconnects.
	listenBackoffMax = 30 * time.Second
)

type watcher struct {
	e *Engine
	// wake asks for a refresh; buffered so bursts of notifications
	// coalesce into one re-read.
	wake chan struct{}
}

func newWatcher(e *Engine) *watcher {
	return &watcher{e: e, wake: make(chan struct{}, 1)}
}

func (w *watcher) poke() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run applies changes until ctx is cancelled.
func (w *watcher) run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() { w.listen(ctx) })

	tick := time.NewTicker(specRefreshInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-w.wake:
		}
		if err := w.refresh(ctx); err != nil && ctx.Err() == nil {
			w.e.log.Error("reading pipelines from the database failed; running pipelines are unchanged, retrying", "err", err, "in", specRefreshInterval)
		}
	}
}

// listen holds a dedicated connection LISTENing for pipeline_spec changes,
// reconnecting with backoff.
func (w *watcher) listen(ctx context.Context) {
	backoff := time.Second
	for {
		err := w.listenOnce(ctx, func() { backoff = time.Second })
		if ctx.Err() != nil {
			return
		}
		w.e.log.Warn("lost the pipeline_spec LISTEN connection; reconnecting (changes still apply within the refresh interval)",
			"err", err, "in", backoff, "refresh_interval", specRefreshInterval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenBackoffMax)
	}
}

func (w *watcher) listenOnce(ctx context.Context, connected func()) error {
	conn, err := pgx.ConnectConfig(ctx, w.e.db.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+record.PipelinesChannel); err != nil {
		return err
	}
	connected()
	// Catch up on anything that changed while not listening.
	w.poke()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		w.e.log.Debug("pipeline_spec changed", "pipeline", n.Payload)
		w.poke()
	}
}

// refresh reads pipeline_spec and applies it.
func (w *watcher) refresh(ctx context.Context) error {
	rows, err := record.ListPipelineSpecs(ctx, w.e.db)
	if err != nil {
		return err
	}
	w.e.applySpecs(ctx, rows)
	return nil
}

// applySpecs makes the running pipelines match rows, diffing by (name,
// revision, enabled): new or enabled rows start, rows with a new revision
// restart, removed or disabled ones stop. Pipelines are handled
// concurrently and independently: one that can't start is reported and
// retried on the next refresh, and never affects the others.
func (e *Engine) applySpecs(ctx context.Context, rows []record.PipelineSpec) {
	wanted := make(map[string]bool, len(rows))
	known := make(map[string]bool, len(rows))
	for _, r := range rows {
		known[r.Name] = true
		if r.Enabled {
			wanted[r.Name] = true
		}
	}
	e.mu.Lock()
	e.wanted = wanted
	running := maps.Clone(e.pipelines)
	e.mu.Unlock()

	var wg sync.WaitGroup
	for _, r := range rows {
		old := running[r.Name]
		if !r.Enabled || (old != nil && old.revision == r.Revision) {
			continue
		}
		wg.Go(func() { e.applySpec(ctx, r, old) })
	}
	for name, p := range running {
		if wanted[name] {
			continue
		}
		reason := "deleted"
		if known[name] {
			reason = "disabled"
		}
		wg.Go(func() { e.stopPipeline(p, reason) })
	}
	wg.Wait()
}

// applySpec starts the pipeline in r, replacing old (if any). The new spec
// is parsed and its start checks run before old is touched, so a bad
// change leaves the running instance alone.
func (e *Engine) applySpec(ctx context.Context, r record.PipelineSpec, old *pipelineRuntime) {
	log := e.log.With("pipeline", r.Name, "revision", r.Revision)
	fail := func(err error) {
		if ctx.Err() != nil {
			return // shutting down
		}
		msg := "pipeline not started; retrying on the next refresh"
		if old != nil {
			msg = fmt.Sprintf("pipeline change not applied; revision %d keeps running, retrying on the next refresh", old.revision)
		}
		log.Error(msg, "err", err)
		e.m.PipelineConfigError(ctx, r.Name)
	}
	pc, err := config.ParsePipelineSpec(r.Name, r.Spec)
	if err != nil {
		fail(fmt.Errorf("invalid spec: %w", err))
		return
	}
	p, err := e.buildPipeline(ctx, pc, r.Revision)
	if err != nil {
		fail(err)
		return
	}
	if old != nil {
		e.stopPipeline(old, fmt.Sprintf("replaced by revision %d", r.Revision))
	}
	if err := e.launch(p); err != nil {
		fail(err)
	}
}
