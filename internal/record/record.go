// Package record owns the file record: the durable answer to "what has been
// synced, at which version". All copy decisions go through it so that
// retries are safe and an older version never overwrites a newer one.
// See docs/adr/0002-connector-and-file-record.md.
package record

import (
	"context"
	"errors"
	"time"
)

// Status of one key in one pipeline.
type Status string

const (
	StatusPending Status = "pending"
	StatusCopying Status = "copying"
	StatusSynced  Status = "synced"
	StatusFailed  Status = "failed"
	StatusDeleted Status = "deleted"
)

var ErrNotFound = errors.New("record: not found")

// ErrLeaseLost is returned by Complete/Fail when the caller's claim expired
// and another worker took the key over. The caller must abandon its work
// (without aborting the shared upload session).
var ErrLeaseLost = errors.New("record: lease lost")

// Record mirrors one row of file_record.
type Record struct {
	PipelineID string
	Key        string

	SourceVersion   string
	SourceSize      int64
	SourceMTime     time.Time
	SourceSequencer string

	SyncedVersion   string
	SyncedMTime     time.Time
	SyncedSequencer string
	SHA256          []byte
	DestVersion     string

	Status    Status
	Attempts  int
	LastError string

	UploadID      string
	UploadVersion string
	ClaimedUntil  time.Time

	EventTime time.Time
	SyncedAt  time.Time
	UpdatedAt time.Time
}

// Candidate is a source version a worker wants to copy, taken from a fresh
// Stat of the source (plus the event's sequencer/time when there was one).
type Candidate struct {
	PipelineID string
	Key        string
	Version    string
	Size       int64
	MTime      time.Time
	Sequencer  string
	EventTime  time.Time
}

// Decision is the outcome of Decide/Claim.
type Decision string

const (
	// DecisionCopy: the caller now holds the lease and must copy Candidate.
	DecisionCopy Decision = "copy"
	// DecisionAlreadySynced: this exact version is already at the destination.
	DecisionAlreadySynced Decision = "already_synced"
	// DecisionStale: a newer version is already synced or being copied.
	DecisionStale Decision = "stale"
	// DecisionBusy: another worker holds an unexpired lease on this key; the
	// job should be retried later (snooze), not counted as a failure.
	DecisionBusy Decision = "busy"
)

// SyncResult is written when a copy is verified.
type SyncResult struct {
	SHA256      []byte
	DestVersion string
	SyncedAt    time.Time
}

// Stats summarises a pipeline for `portage status`.
type Stats struct {
	Synced, Pending, Copying, Failed int64
	BytesSynced                      int64
	LastSyncedAt                     time.Time
	OldestPendingEvent               time.Time // zero when nothing pending
	RecentErrors                     []Record  // newest first, small N
}

// Store is the Postgres-backed file record. Implementations must make Claim
// atomic (row lock / single UPDATE ... RETURNING), so two workers can never
// both get DecisionCopy for the same key at the same time.
type Store interface {
	Get(ctx context.Context, pipelineID, key string) (Record, error)

	// Observe records that a source version exists (from an event or the
	// reconciler) without claiming it. Must not regress source_* fields to an
	// older version (same ordering rules as Decide).
	Observe(ctx context.Context, c Candidate) error

	// Claim applies Decide under a row lock. On DecisionCopy it sets
	// status=copying, claimed_until=now+lease, attempts+=1, and returns the
	// updated record (whose UploadID/UploadVersion the worker uses to resume
	// if UploadVersion == c.Version).
	Claim(ctx context.Context, c Candidate, lease time.Duration) (Decision, Record, error)

	// ExtendLease renews the caller's claim during a long copy.
	ExtendLease(ctx context.Context, pipelineID, key, version string, lease time.Duration) error

	// SetUpload persists the multipart session for resume after a crash.
	SetUpload(ctx context.Context, pipelineID, key, version, uploadID string) error

	// Complete marks version synced. Returns ErrLeaseLost if the key is no
	// longer claimed for this version.
	Complete(ctx context.Context, pipelineID, key, version string, res SyncResult) error

	// Fail releases the claim and records the error. retryable=false sets
	// status=failed; otherwise status=pending.
	Fail(ctx context.Context, pipelineID, key, version string, cause error, retryable bool) error

	// MarkDeleted records a source delete (deletes off by default; this only
	// tracks state).
	MarkDeleted(ctx context.Context, pipelineID, key string, at time.Time) error

	// List returns records ordered by key, after afterKey, for the reconciler.
	List(ctx context.Context, pipelineID, afterKey string, limit int) ([]Record, error)

	Stats(ctx context.Context, pipelineID string) (Stats, error)
}
