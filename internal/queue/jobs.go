// Package queue defines the background jobs the engine runs on River
// (Postgres-backed). Job arg types are the contract between change sources,
// the reconciler and the copy workers.
package queue

import "time"

// CopyArgs asks a worker to bring one key at the destination up to date with
// the source. The worker always Stats the source and copies the *current*
// version; Version/Sequencer here are hints used for ordering and dedupe.
type CopyArgs struct {
	PipelineID string    `json:"pipeline_id" river:"unique"`
	Key        string    `json:"key" river:"unique"`
	Version    string    `json:"version,omitempty" river:"unique"`
	Size       int64     `json:"size,omitempty"`
	Sequencer  string    `json:"sequencer,omitempty"`
	EventTime  time.Time `json:"event_time"`
	DetectedAt time.Time `json:"detected_at"`
	Origin     string    `json:"origin"`
	// Generation is the file record's attempt count when this job was
	// enqueued. It is part of the dedupe key: repeats of an event collapse,
	// but once a worker has claimed the key (bumping the count) a new job
	// can be queued even if the claiming worker died with its job still
	// 'running' in River.
	Generation int `json:"generation" river:"unique"`
	// Window is a coarse time bucket (see CopyWindow) and part of the dedupe
	// key. A job can be stranded as 'running' before any worker claims the
	// key (a DB stall between River's fetch and our claim, or a crash there),
	// leaving Generation unchanged; without a window it would swallow every
	// re-queue of that key until River's stuck-job rescue hours later. With
	// it, repeats still collapse within a window, and a stranded job blocks
	// the key for at most one window. Extra jobs are harmless: the file
	// record's claim answers busy or already-synced.
	Window int64 `json:"window" river:"unique"`
}

func (CopyArgs) Kind() string { return "copy" }

// CopyDedupeWindow bounds how long a stranded copy job can block its key.
const CopyDedupeWindow = 5 * time.Minute

// WindowAt returns the dedupe bucket containing t for a window length.
func WindowAt(t time.Time, window time.Duration) int64 {
	return t.Unix() / int64(window/time.Second)
}

// DeleteArgs propagates a source delete. Only enqueued when the pipeline has
// deletes enabled (off by default).
type DeleteArgs struct {
	PipelineID string    `json:"pipeline_id" river:"unique"`
	Key        string    `json:"key" river:"unique"`
	EventTime  time.Time `json:"event_time"`
}

func (DeleteArgs) Kind() string { return "delete" }

// ReconcileArgs runs one listing diff for a pipeline.
type ReconcileArgs struct {
	PipelineID string `json:"pipeline_id"`
	// Revision is the pipeline_spec revision that scheduled the run (0 for
	// pipelines from the config file). Reconciles dedupe on their args, so
	// a restarted pipeline's first reconcile is not swallowed by the
	// previous instance's run that is still being finalised.
	Revision int64 `json:"revision,omitempty"`
	// Window (a bucket of twice the reconcile interval) is part of the
	// dedupe key for the same reason as CopyArgs.Window: a reconcile job
	// stranded as 'running' must not suppress reconciles for hours. At most
	// two reconciles of one pipeline can overlap, which is harmless.
	Window int64 `json:"window,omitempty"`
}

func (ReconcileArgs) Kind() string { return "reconcile" }

// Queue names. Copy jobs for one pipeline share a queue so per-pipeline
// concurrency can be set by sizing that queue's worker count.
const QueueReconcile = "reconcile"

// CopyQueue returns the River queue name for a pipeline's copy/delete jobs.
func CopyQueue(pipelineID string) string { return "copy_" + pipelineID }
