package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river"

	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/metrics"
	"github.com/sunny36/portage/internal/queue"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/transfer"
)

const (
	// leaseDuration bounds how long a crashed worker blocks a key. Heartbeats
	// renew it every heartbeatEvery while a copy runs.
	leaseDuration  = 2 * time.Minute
	heartbeatEvery = 30 * time.Second
	// busySnooze is how long a job waits when another worker holds the key.
	busySnooze = 15 * time.Second
	// copyTimeout caps one attempt. Large objects are fine: the lease is
	// renewed by heartbeat, and a timed-out attempt resumes its upload.
	copyTimeout = 6 * time.Hour
)

// CopyWorker brings one key at the destination up to date with the source.
type CopyWorker struct {
	river.WorkerDefaults[queue.CopyArgs]
	e *Engine
}

func (w *CopyWorker) Timeout(*river.Job[queue.CopyArgs]) time.Duration { return copyTimeout }

func (w *CopyWorker) Work(ctx context.Context, job *river.Job[queue.CopyArgs]) error {
	args := job.Args
	p, err := w.e.runtimeFor(args.PipelineID)
	if err != nil {
		return err
	}
	// Stopping the pipeline (changed, disabled, deleted) cancels the copy
	// once it had shutdownTimeout to finish; it then releases its lease.
	ctx, done := withStop(ctx, p.workCtx)
	defer done()
	log := w.e.log.With("pipeline", args.PipelineID, "key", args.Key, "attempt", job.Attempt)
	start := time.Now()

	// Always copy the source's current version, never the event's.
	info, err := p.src.Stat(ctx, args.Key)
	if errors.Is(err, connector.ErrNotFound) {
		// Deleted (or renamed) since the event; the delete has its own path.
		w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeStale, 0, 0, 0)
		return nil
	}
	if err != nil {
		return w.e.sourceError(ctx, p, "Stat", err)
	}

	cand := record.Candidate{
		PipelineID: p.cfg.Name,
		Key:        args.Key,
		Version:    info.Version,
		Size:       info.Size,
		MTime:      info.ModTime,
		EventTime:  args.EventTime,
	}
	if args.Version == info.Version {
		// The event's sequencer describes this exact version.
		cand.Sequencer = args.Sequencer
	}

	dec, rec, err := w.e.store.Claim(ctx, cand, leaseDuration)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	switch dec {
	case record.DecisionAlreadySynced:
		w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeAlreadySynced, 0, 0, 0)
		return nil
	case record.DecisionStale:
		w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeStale, 0, 0, 0)
		return nil
	case record.DecisionBusy:
		return river.JobSnooze(busySnooze)
	}

	resume := ""
	if rec.UploadVersion == info.Version {
		resume = rec.UploadID
	}
	res, err := transfer.Copy(ctx,
		transfer.Request{
			Src: p.src, SrcKey: args.Key, Info: info,
			Dst: p.dst, DstKey: args.Key,
			ResumeUploadID: resume,
			ContentType:    contentType(info),
		},
		transfer.Options{
			PartSize:        int64(p.cfg.PartSize),
			PartConcurrency: p.cfg.PartConcurrency,
			Pool:            w.e.pool,
			HeartbeatEvery:  heartbeatEvery,
		},
		transfer.Hooks{
			OnUploadStarted: func(ctx context.Context, id string) error {
				return w.e.store.SetUpload(ctx, p.cfg.Name, args.Key, info.Version, id)
			},
			Heartbeat: leaseKeeper(func(ctx context.Context) error {
				return w.e.store.ExtendLease(ctx, p.cfg.Name, args.Key, info.Version, leaseDuration)
			}, log, time.Now),
			OnBytes: func(n int64) { w.e.m.BytesUploaded(ctx, p.cfg.Name, n) },
		})
	if err != nil {
		return w.copyFailed(ctx, p, log, info, err)
	}

	now := time.Now()
	// The object is written and verified: record it even if we're being
	// cancelled, or the next run would copy it again.
	doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err = w.e.store.Complete(doneCtx, p.cfg.Name, args.Key, info.Version, record.SyncResult{
		SHA256: res.SHA256, DestVersion: res.DestVersion, SyncedAt: now,
	})
	if errors.Is(err, record.ErrLeaseLost) {
		// Another worker took over this version and owns the outcome.
		log.Warn("lease lost before completing; leaving the result to the new owner")
		return nil
	}
	if err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	var lag time.Duration
	if !rec.EventTime.IsZero() {
		lag = now.Sub(rec.EventTime)
	}
	w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeSynced, res.Bytes, lag, now.Sub(start))
	log.Debug("synced", "bytes", res.Bytes, "lag", lag, "parts_reused", res.PartsReused, "verify", res.Verified.Method)
	return nil
}

// copyFailed records the failure and tells River what to do next.
func (w *CopyWorker) copyFailed(ctx context.Context, p *pipelineRuntime, log *slog.Logger, info connector.ObjectInfo, cause error) error {
	if errors.Is(cause, record.ErrLeaseLost) {
		// Heartbeat found someone else owns the key; don't touch the record.
		log.Warn("lease lost during copy; abandoning attempt")
		return nil
	}
	if ctx.Err() != nil {
		// Shutdown or timeout. Release the lease now (keeping the upload
		// session) so a restart resumes immediately instead of waiting for
		// the lease to lapse.
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := w.e.store.Fail(relCtx, p.cfg.Name, info.Key, info.Version, cause, true); err != nil &&
			!errors.Is(err, record.ErrLeaseLost) {
			log.Warn("releasing lease after interruption", "err", err)
		}
		return cause
	}
	retryable := connector.IsRetryable(cause) || errors.Is(cause, connector.ErrVersionChanged)
	if err := w.e.store.Fail(context.WithoutCancel(ctx), p.cfg.Name, info.Key, info.Version, cause, retryable); err != nil &&
		!errors.Is(err, record.ErrLeaseLost) {
		log.Error("recording failure", "err", err)
	}
	switch {
	case errors.Is(cause, connector.ErrVersionChanged):
		// The source changed mid-copy; go again right away for the new version.
		w.e.m.Retry(ctx, p.cfg.Name, "copy", "version_changed")
		return river.JobSnooze(time.Second)
	case !retryable:
		// Permission/auth problems need a human; stop retrying and say so.
		w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeFailed, 0, 0, 0)
		log.Error("copy failed permanently", "err", cause)
		return river.JobCancel(cause)
	default:
		reason := "other"
		if errors.Is(cause, connector.ErrThrottled) {
			reason = "throttled"
		}
		w.e.m.Retry(ctx, p.cfg.Name, "copy", reason)
		w.e.m.CopyFinished(ctx, p.cfg.Name, metrics.OutcomeRetry, 0, 0, 0)
		log.Warn("copy failed; will retry", "err", cause)
		return cause
	}
}

// DeleteWorker records a source delete and, when the pipeline has deletes
// enabled, removes the key at the destination.
type DeleteWorker struct {
	river.WorkerDefaults[queue.DeleteArgs]
	e *Engine
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[queue.DeleteArgs]) error {
	args := job.Args
	p, err := w.e.runtimeFor(args.PipelineID)
	if err != nil {
		return err
	}
	ctx, done := withStop(ctx, p.workCtx)
	defer done()
	// Confirm it's really gone: an event can arrive after a re-create.
	if _, err := p.src.Stat(ctx, args.Key); err == nil {
		return nil
	} else if !errors.Is(err, connector.ErrNotFound) {
		return w.e.sourceError(ctx, p, "Stat", err)
	}
	if p.cfg.Deletes {
		if err := p.dst.Delete(ctx, args.Key); err != nil {
			if !connector.IsRetryable(err) {
				return river.JobCancel(err)
			}
			return err
		}
	}
	at := args.EventTime
	if at.IsZero() {
		at = time.Now()
	}
	return w.e.store.MarkDeleted(ctx, p.cfg.Name, args.Key, at)
}

// sourceError classifies an error reading the source outside a copy.
func (e *Engine) sourceError(ctx context.Context, p *pipelineRuntime, op string, err error) error {
	if !connector.IsRetryable(err) {
		e.log.Error("source error", "pipeline", p.cfg.Name, "op", op, "err", err)
		return river.JobCancel(err)
	}
	reason := "other"
	if errors.Is(err, connector.ErrThrottled) {
		reason = "throttled"
	}
	e.m.Retry(ctx, p.cfg.Name, op, reason)
	return err
}

func contentType(info connector.ObjectInfo) string {
	if info.ContentType != "" {
		return info.ContentType
	}
	return "application/octet-stream"
}

// leaseKeeper wraps a lease renewal for use as a transfer heartbeat. A lost
// lease stops the copy at once (someone else owns the key). Any other error
// (a database blip) is tolerated while the lease is still safely valid, so a
// multi-gigabyte copy isn't thrown away over one failed renewal; once less
// than a heartbeat interval of lease remains, the error stops the copy.
func leaseKeeper(extend func(context.Context) error, log *slog.Logger, now func() time.Time) func(context.Context) error {
	lastOK := now()
	return func(ctx context.Context) error {
		err := extend(ctx)
		switch {
		case err == nil:
			lastOK = now()
			return nil
		case errors.Is(err, record.ErrLeaseLost):
			return err
		case now().Sub(lastOK) < leaseDuration-heartbeatEvery:
			log.Warn("lease renewal failed; retrying at the next heartbeat", "err", err,
				"lease_left", (leaseDuration - now().Sub(lastOK)).Round(time.Second))
			return nil
		default:
			return fmt.Errorf("lease renewal failing for %s: %w", now().Sub(lastOK).Round(time.Second), err)
		}
	}
}
