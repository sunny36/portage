// Package reconcile diffs a pipeline's source listing against its file
// record and emits the changes events missed. It also performs the initial
// copy. See docs/adr/0003-change-sources.md.
//
// # Merge join and key order
//
// The two sides are merge-joined in key order, one page at a time, so memory
// is O(PageSize + deleted keys) whatever the bucket size. Store.List orders
// by key COLLATE "C", i.e. UTF-8 byte order, which is also Go's string order
// and the order S3 ListObjectsV2 documents. Azure is NOT guaranteed to agree:
// Azurite lists in UTF-16 code-unit order, which differs from UTF-8 byte
// order when a key has a supplementary-plane character (e.g. an emoji)
// where another key has a character in U+E000–U+FFFF. So the join does not
// trust the source order:
//
//   - The record stream (which Portage controls) must be strictly increasing
//     in byte order, or the run aborts.
//   - The source stream is checked as it is read. Once it goes backwards the
//     run is marked out of order, and from then on a source key with no
//     match in the join is looked up with Store.Get before being treated as
//     new.
//   - Every delete candidate (a live record with no matching source key) is
//     held until both listings are exhausted, then confirmed with a Stat on
//     the source; only ErrNotFound produces a delete. A misaligned join can
//     therefore cost an extra lookup, never a false delete.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/record"
)

// DefaultPageSize is used when Options.PageSize <= 0.
const DefaultPageSize = 1000

// Options configures Run.
type Options struct {
	PipelineID string
	// Source is already scoped to the pipeline's source prefix.
	Source connector.Connector
	Store  record.Store
	Filter *change.Filter
	Emit   change.Emit
	// IgnoreBefore: objects with no record and ModTime before this are not
	// emitted (existing_files: skip). Zero = copy everything.
	IgnoreBefore time.Time
	PageSize     int
	Log          *slog.Logger

	// Now defaults to time.Now (for tests).
	Now func() time.Time
}

// Stats summarises one reconcile pass.
type Stats struct {
	// Listed counts source objects that pass the filter.
	Listed int64
	// Emitted counts upserts emitted; Deleted counts deletes emitted.
	Emitted, Deleted int64
	// AlreadySynced: the record's synced version matches the source.
	AlreadySynced int64
	// InFlight: a worker holds an unexpired lease on a different version.
	InFlight int64
	// Ignored: new objects skipped because of IgnoreBefore.
	Ignored  int64
	Duration time.Duration
}

// ErrEmptySource is returned (wrapped) when the source lists nothing while
// the record still has live keys: a misconfigured prefix or credentials
// that silently see an empty bucket must not turn into a mass delete.
var ErrEmptySource = errors.New("reconcile: source listing empty")

// ErrMassDelete is returned instead of emitting deletes when a single pass
// finds more than massDeleteMin files, and more than 1/massDeleteDenom of
// all synced files, missing from the source. Like ErrEmptySource it guards
// against a wrong prefix or a partially visible source, not real deletes.
var ErrMassDelete = errors.New("reconcile: too many files missing from source")

const (
	massDeleteMin   = 100
	massDeleteDenom = 4
)

// Run performs one reconcile pass. It returns the stats so far and the first
// error from listing, the store or Emit.
func Run(ctx context.Context, opts Options) (Stats, error) {
	if opts.Source == nil || opts.Store == nil || opts.Emit == nil {
		return Stats{}, errors.New("reconcile: Source, Store and Emit are required")
	}
	if opts.PageSize <= 0 {
		opts.PageSize = DefaultPageSize
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	start := opts.Now()
	r := &run{opts: opts, start: start, log: opts.Log.With("pipeline", opts.PipelineID, "component", "reconcile")}
	err := r.join(ctx)
	if err == nil {
		err = r.confirmDeletes(ctx)
	}
	r.stats.Duration = opts.Now().Sub(start)
	if err != nil {
		return r.stats, err
	}
	r.log.Info("reconcile: done",
		"listed", r.stats.Listed, "emitted", r.stats.Emitted, "deleted", r.stats.Deleted,
		"already_synced", r.stats.AlreadySynced, "in_flight", r.stats.InFlight,
		"ignored", r.stats.Ignored, "duration", r.stats.Duration)
	return r.stats, nil
}

type run struct {
	opts  Options
	start time.Time
	log   *slog.Logger
	stats Stats
	src   *sourceIter
	// candidates are live records with no source match in the join, held
	// until the join ends and then confirmed with a Stat.
	candidates []record.Record
	// live counts records not marked deleted, for the mass-delete guard.
	live int64
}

func (r *run) join(ctx context.Context) error {
	r.src = &sourceIter{c: r.opts.Source, limit: r.opts.PageSize, filter: r.opts.Filter, log: r.log}
	recs := &recordIter{s: r.opts.Store, pipelineID: r.opts.PipelineID, limit: r.opts.PageSize, filter: r.opts.Filter}

	obj, haveObj, err := r.src.next(ctx)
	if err != nil {
		return err
	}
	if !haveObj {
		return r.emptySource(ctx, recs)
	}
	rec, haveRec, err := recs.next(ctx)
	if err != nil {
		return err
	}
	for haveObj || haveRec {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch {
		case haveObj && (!haveRec || obj.Key < rec.Key):
			r.stats.Listed++
			if err := r.sourceOnly(ctx, obj); err != nil {
				return err
			}
			obj, haveObj, err = r.src.next(ctx)
		case haveRec && (!haveObj || rec.Key < obj.Key):
			if rec.Status != record.StatusDeleted {
				r.candidates = append(r.candidates, rec)
				r.live++
			}
			rec, haveRec, err = recs.next(ctx)
		default: // same key
			r.stats.Listed++
			if rec.Status != record.StatusDeleted {
				r.live++
			}
			if err := r.both(ctx, obj, rec); err != nil {
				return err
			}
			if obj, haveObj, err = r.src.next(ctx); err != nil {
				return err
			}
			rec, haveRec, err = recs.next(ctx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// emptySource handles a source that lists no (matching) objects. Deleting
// every live record on that evidence is too dangerous: refuse if there is
// anything to delete.
func (r *run) emptySource(ctx context.Context, recs *recordIter) error {
	var live int64
	for {
		rec, ok, err := recs.next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if rec.Status != record.StatusDeleted {
			live++
		}
	}
	if live > 0 {
		return fmt.Errorf("%w; refusing to emit %d deletes (check the source prefix and credentials)", ErrEmptySource, live)
	}
	return nil
}

func (r *run) upsert(ctx context.Context, obj connector.ObjectInfo) error {
	err := r.opts.Emit(ctx, change.ObjectChanged{
		PipelineID: r.opts.PipelineID,
		Kind:       change.KindUpsert,
		Key:        obj.Key,
		Version:    obj.Version,
		Size:       obj.Size,
		Sequencer:  obj.Sequencer,
		EventTime:  obj.ModTime,
		DetectedAt: r.opts.Now(),
		Origin:     change.OriginReconcile,
	})
	if err != nil {
		return fmt.Errorf("reconcile: emit upsert %q: %w", obj.Key, err)
	}
	r.stats.Emitted++
	return nil
}

func (r *run) sourceOnly(ctx context.Context, obj connector.ObjectInfo) error {
	if r.src.outOfOrder {
		// The join may have passed this key's record already.
		rec, err := r.opts.Store.Get(ctx, r.opts.PipelineID, obj.Key)
		switch {
		case err == nil:
			return r.both(ctx, obj, rec)
		case !errors.Is(err, record.ErrNotFound):
			return fmt.Errorf("reconcile: get record %q: %w", obj.Key, err)
		}
	}
	if !r.opts.IgnoreBefore.IsZero() && obj.ModTime.Before(r.opts.IgnoreBefore) {
		r.stats.Ignored++
		return nil
	}
	return r.upsert(ctx, obj)
}

func (r *run) both(ctx context.Context, obj connector.ObjectInfo, rec record.Record) error {
	if rec.Status != record.StatusDeleted && rec.SyncedVersion != "" && sameVersion(rec.SyncedVersion, obj.Version) {
		r.stats.AlreadySynced++
		return nil
	}
	if rec.Status == record.StatusCopying && rec.ClaimedUntil.After(r.opts.Now()) {
		// Usually a live copy, but it may be a worker that died holding the
		// lease (kill -9). Emit anyway: the job is deduped per claim
		// generation, so at most one extra job per claim is queued; it waits
		// while the lease is held and resumes the upload if it lapses,
		// instead of the key waiting for the next reconcile pass.
		r.stats.InFlight++
	}
	return r.upsert(ctx, obj)
}

// sameVersion compares ETags ignoring surrounding quotes. The contract says
// connectors strip them, but a quoted value from one code path (e.g. a Stat
// that forgot to) must not make every object look changed on every pass.
func sameVersion(a, b string) bool {
	return strings.Trim(a, `"`) == strings.Trim(b, `"`)
}

// confirmDeletes Stats each delete candidate and emits a delete only for
// keys the source reports as not found. A key that exists was either
// misaligned in the join (and handled through Store.Get) or re-created after
// the listing (its event, or the next pass, handles it).
func (r *run) confirmDeletes(ctx context.Context) error {
	var gone []record.Record
	for _, rec := range r.candidates {
		if _, err := r.opts.Source.Stat(ctx, rec.Key); err == nil {
			r.log.Debug("reconcile: delete candidate still exists; skipping", "key", rec.Key)
			continue
		} else if !errors.Is(err, connector.ErrNotFound) {
			return fmt.Errorf("reconcile: confirm delete %q: %w", rec.Key, err)
		}
		gone = append(gone, rec)
	}
	if n := int64(len(gone)); n > massDeleteMin && n*massDeleteDenom > r.live {
		return fmt.Errorf("%w: %d of %d synced files are gone from the source; refusing to emit deletes "+
			"(check the source prefix and credentials; if this is intended, deletes are applied by events)",
			ErrMassDelete, n, r.live)
	}
	for _, rec := range gone {
		err := r.opts.Emit(ctx, change.ObjectChanged{
			PipelineID: r.opts.PipelineID,
			Kind:       change.KindDelete,
			Key:        rec.Key,
			// The exact time is unknown. The pass start is a safe bound: an
			// object re-created after it has a later ModTime, so the record
			// ignores this delete for it (see Store.MarkDeleted).
			EventTime:  r.start,
			DetectedAt: r.opts.Now(),
			Origin:     change.OriginReconcile,
		})
		if err != nil {
			return fmt.Errorf("reconcile: emit delete %q: %w", rec.Key, err)
		}
		r.stats.Deleted++
	}
	return nil
}

// sourceIter pages through Connector.List, applying the filter and noting
// (once) if keys stop increasing in byte order.
type sourceIter struct {
	c      connector.Connector
	limit  int
	filter *change.Filter
	log    *slog.Logger

	buf        []connector.ObjectInfo
	cursor     string
	done       bool
	last       string
	hasLast    bool
	outOfOrder bool
}

func (it *sourceIter) next(ctx context.Context) (connector.ObjectInfo, bool, error) {
	for {
		for len(it.buf) > 0 {
			o := it.buf[0]
			it.buf = it.buf[1:]
			if it.hasLast && o.Key <= it.last && !it.outOfOrder {
				it.outOfOrder = true
				it.log.Warn("reconcile: source listing is not in UTF-8 byte order; verifying unmatched keys individually",
					"key", o.Key, "after", it.last)
			}
			it.last, it.hasLast = o.Key, true
			if it.filter.Match(o.Key) {
				return o, true, nil
			}
		}
		if it.done {
			return connector.ObjectInfo{}, false, nil
		}
		page, err := it.c.List(ctx, "", it.cursor, it.limit)
		if err != nil {
			return connector.ObjectInfo{}, false, fmt.Errorf("reconcile: list source: %w", err)
		}
		// The buffer drains before done is checked again.
		it.buf, it.cursor, it.done = page.Objects, page.NextCursor, page.NextCursor == ""
	}
}

// recordIter pages through Store.List, applying the filter and checking that
// keys strictly increase.
type recordIter struct {
	s          record.Store
	pipelineID string
	limit      int
	filter     *change.Filter

	buf   []record.Record
	after string
	done  bool
	last  string
	seen  bool
}

func (it *recordIter) next(ctx context.Context) (record.Record, bool, error) {
	for {
		for len(it.buf) > 0 {
			rec := it.buf[0]
			it.buf = it.buf[1:]
			if it.seen && rec.Key <= it.last {
				return record.Record{}, false, fmt.Errorf(
					"reconcile: file record not in byte order (%q after %q); refusing to diff", rec.Key, it.last)
			}
			it.last, it.seen = rec.Key, true
			if it.filter.Match(rec.Key) {
				return rec, true, nil
			}
		}
		if it.done {
			return record.Record{}, false, nil
		}
		page, err := it.s.List(ctx, it.pipelineID, it.after, it.limit)
		if err != nil {
			return record.Record{}, false, fmt.Errorf("reconcile: list records: %w", err)
		}
		if len(page) < it.limit {
			it.done = true
		}
		if len(page) > 0 {
			it.after = page[len(page)-1].Key
		}
		it.buf = page
	}
}
