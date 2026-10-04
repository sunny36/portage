package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/sunny36/portage/internal/config"
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
	p, ok := w.e.pipelines[job.Args.PipelineID]
	if !ok {
		return river.JobCancel(fmt.Errorf("unknown pipeline %q", job.Args.PipelineID))
	}
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

// periodicReconciles schedules a reconcile per pipeline at its interval,
// starting immediately (that first run is the initial copy).
func (e *Engine) periodicReconciles(cfg *config.File) []*river.PeriodicJob {
	jobs := make([]*river.PeriodicJob, 0, len(cfg.Pipelines))
	for _, p := range cfg.Pipelines {
		name := p.Name
		jobs = append(jobs, river.NewPeriodicJob(
			river.PeriodicInterval(p.ReconcileInterval),
			func() (river.JobArgs, *river.InsertOpts) {
				return queue.ReconcileArgs{PipelineID: name}, &river.InsertOpts{
					Queue: queue.QueueReconcile,
					// Never stack reconciles for one pipeline. Completed jobs
					// must not count, or every later run would be deduped away.
					UniqueOpts: river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
						rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
						rivertype.JobStateRetryable, rivertype.JobStateScheduled,
					}},
				}
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
	return jobs
}
