package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"

	"github.com/sunny36/portage/internal/queue"
	"github.com/sunny36/portage/internal/reconcile"
)

// ReconcileWorker runs one listing diff for a pipeline: the safety net for
// lost events, and how the initial copy happens.
type ReconcileWorker struct {
	river.WorkerDefaults[queue.ReconcileArgs]
	e *Engine
}

// A full listing of a large bucket can take a while.
func (w *ReconcileWorker) Timeout(*river.Job[queue.ReconcileArgs]) time.Duration {
	return 2 * time.Hour
}

func (w *ReconcileWorker) Work(ctx context.Context, job *river.Job[queue.ReconcileArgs]) error {
	p, err := w.e.runtimeFor(job.Args.PipelineID)
	if err != nil {
		return err
	}
	// A stopping pipeline (changed, disabled, deleted, engine shutdown)
	// abandons its listing; the next instance reconciles on start.
	ctx, done := withStop(ctx, p.runCtx)
	defer done()
	log := w.e.log.With("pipeline", p.cfg.Name)
	st, err := reconcile.Run(ctx, reconcile.Options{
		PipelineID:   p.cfg.Name,
		Source:       p.src,
		Store:        w.e.store,
		Filter:       p.filter,
		Emit:         w.e.emit,
		IgnoreBefore: p.ignoreBefore,
		Log:          w.e.log, // reconcile adds the pipeline attribute itself
	})
	w.e.m.ReconcileFinished(ctx, p.cfg.Name, st.Listed, st.Emitted, st.Deleted, st.Duration, err)
	if p.runCtx.Err() != nil {
		return river.JobCancel(fmt.Errorf("pipeline %s stopped during reconcile: %w", p.cfg.Name, context.Cause(p.runCtx)))
	}
	if errors.Is(err, reconcile.ErrEmptySource) || errors.Is(err, reconcile.ErrMassDelete) {
		// Likely a wrong prefix or revoked access. Don't retry in a loop;
		// the next scheduled run tries again.
		log.Error("reconcile refused: source listing is empty but files were synced before; check the source prefix and access", "err", err)
		return river.JobCancel(err)
	}
	if err != nil {
		return err
	}
	log.Info("reconcile finished", "listed", st.Listed, "emitted", st.Emitted, "deleted", st.Deleted,
		"in_flight", st.InFlight, "ignored", st.Ignored, "took", st.Duration.Round(time.Millisecond))
	return nil
}
