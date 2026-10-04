// Package change turns provider-specific change notifications into one
// normalised ObjectChanged message. See docs/adr/0003-change-sources.md.
package change

import (
	"context"
	"time"
)

// Kind of change observed at the source.
type Kind string

const (
	KindUpsert Kind = "upsert" // created or overwritten
	KindDelete Kind = "delete"
)

// Origin records how a change was discovered, for metrics and debugging.
type Origin string

const (
	OriginEvent     Origin = "event"     // pushed/queued notification
	OriginReconcile Origin = "reconcile" // found by a listing diff
	OriginPoll      Origin = "poll"      // SFTP-style polling (Phase 2)
)

// ObjectChanged is the single message every change source produces.
type ObjectChanged struct {
	PipelineID string
	Kind       Kind
	// Key is relative to the pipeline's source prefix.
	Key string
	// Version, Size and Sequencer as reported by the event (may be empty/zero
	// for reconcile-origin changes until the worker Stats the object).
	Version   string
	Size      int64
	Sequencer string
	// EventTime is when the provider says the change happened. Sync lag is
	// measured from here to verified-at-destination.
	EventTime time.Time
	// DetectedAt is when Portage received/observed it.
	DetectedAt time.Time
	Origin     Origin
}

// Emit hands a change to the engine (which enqueues a copy job). It returns
// only after the change is durably queued; a source must not ack/delete the
// provider message until Emit returns nil.
type Emit func(ctx context.Context, c ObjectChanged) error

// Source produces changes until ctx is cancelled.
type Source interface {
	Name() string
	// Run blocks, calling emit for each change, and returns ctx.Err() on
	// shutdown or a non-retryable error.
	Run(ctx context.Context, emit Emit) error
}
