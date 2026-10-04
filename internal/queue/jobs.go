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
}

func (CopyArgs) Kind() string { return "copy" }

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
}

func (ReconcileArgs) Kind() string { return "reconcile" }

// Queue names. Copy jobs for one pipeline share a queue so per-pipeline
// concurrency can be set by sizing that queue's worker count.
const QueueReconcile = "reconcile"

// CopyQueue returns the River queue name for a pipeline's copy/delete jobs.
func CopyQueue(pipelineID string) string { return "copy_" + pipelineID }
