package record

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore is the Postgres implementation of Store (pgx v5). Tables come from
// Migrate. All lease checks use the database clock (clock_timestamp()), so
// engines on hosts with skewed clocks agree on who holds a key.
type PGStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PGStore)(nil)

// NewPGStore returns a Store backed by pool.
func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

// recentErrorsLimit bounds Stats.RecentErrors.
const recentErrorsLimit = 5

const recordColumns = `pipeline_id, key,
	source_version, source_size, source_mtime, source_sequencer,
	synced_version, synced_mtime, synced_sequencer, sha256, dest_version,
	status, attempts, last_error,
	upload_id, upload_version, claimed_until,
	event_time, synced_at, updated_at`

func scanRecord(row pgx.Row) (Record, error) {
	var (
		r                                        Record
		status                                   string
		srcMTime, syncMTime, claimed, ev, syncAt *time.Time
	)
	err := row.Scan(&r.PipelineID, &r.Key,
		&r.SourceVersion, &r.SourceSize, &srcMTime, &r.SourceSequencer,
		&r.SyncedVersion, &syncMTime, &r.SyncedSequencer, &r.SHA256, &r.DestVersion,
		&status, &r.Attempts, &r.LastError,
		&r.UploadID, &r.UploadVersion, &claimed,
		&ev, &syncAt, &r.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Record{}, ErrNotFound
		}
		return Record{}, err
	}
	r.Status = Status(status)
	r.SourceMTime = deref(srcMTime)
	r.SyncedMTime = deref(syncMTime)
	r.ClaimedUntil = deref(claimed)
	r.EventTime = deref(ev)
	r.SyncedAt = deref(syncAt)
	return r, nil
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// nullTime maps the zero time to SQL NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// normalize truncates times to Postgres' microsecond precision so values
// compare the same before and after a round trip.
func normalize(c Candidate) Candidate {
	c.MTime = c.MTime.Truncate(time.Microsecond)
	c.EventTime = c.EventTime.Truncate(time.Microsecond)
	return c
}

// lockRow ensures the (pipeline, key) row exists and locks it for the rest of
// tx. It returns the row and the database clock read after the lock was
// acquired.
func lockRow(ctx context.Context, tx pgx.Tx, pipelineID, key string) (Record, time.Time, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO file_record (pipeline_id, key) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		pipelineID, key); err != nil {
		return Record{}, time.Time{}, fmt.Errorf("record: insert: %w", err)
	}
	r, err := scanRecord(tx.QueryRow(ctx,
		`SELECT `+recordColumns+` FROM file_record WHERE pipeline_id = $1 AND key = $2 FOR UPDATE`,
		pipelineID, key))
	if err != nil {
		return Record{}, time.Time{}, fmt.Errorf("record: lock: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return Record{}, time.Time{}, err
	}
	return r, now, nil
}

func (s *PGStore) Get(ctx context.Context, pipelineID, key string) (Record, error) {
	return scanRecord(s.pool.QueryRow(ctx,
		`SELECT `+recordColumns+` FROM file_record WHERE pipeline_id = $1 AND key = $2`,
		pipelineID, key))
}

// Observe records a source version without claiming it. source_* only moves
// forward (isNewer). While a key is being copied its source_* fields belong
// to the lease holder (Complete matches on them), so Observe leaves them
// alone; the newer version's own copy job claims it afterwards.
func (s *PGStore) Observe(ctx context.Context, c Candidate) error {
	c = normalize(c)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		r, _, err := lockRow(ctx, tx, c.PipelineID, c.Key)
		if err != nil {
			return err
		}
		if r.Status == StatusCopying {
			return nil
		}
		if !isNewer(c.Sequencer, c.MTime, c.Version, r.SourceSequencer, r.SourceMTime, r.SourceVersion) {
			return nil
		}
		status := StatusPending
		if r.Status == StatusSynced && c.Version == r.SyncedVersion {
			status = StatusSynced
		}
		eventTime := c.EventTime
		if (r.Status == StatusPending || r.Status == StatusFailed) && !r.EventTime.IsZero() {
			// Keep the oldest unsynced change for lag measurement.
			eventTime = r.EventTime
		}
		_, err = tx.Exec(ctx, `UPDATE file_record SET
				source_version = $3, source_size = $4, source_mtime = $5, source_sequencer = $6,
				status = $7, event_time = $8, updated_at = clock_timestamp()
			WHERE pipeline_id = $1 AND key = $2`,
			c.PipelineID, c.Key, c.Version, c.Size, nullTime(c.MTime), c.Sequencer,
			string(status), nullTime(eventTime))
		return err
	})
}

// Claim applies Decide under a row lock (SELECT ... FOR UPDATE), so
// concurrent callers serialise on the key and at most one gets DecisionCopy.
func (s *PGStore) Claim(ctx context.Context, c Candidate, lease time.Duration) (Decision, Record, error) {
	c = normalize(c)
	var (
		dec Decision
		out Record
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		r, now, err := lockRow(ctx, tx, c.PipelineID, c.Key)
		if err != nil {
			return err
		}
		dec = Decide(&r, c, now)
		if dec != DecisionCopy {
			out = r
			return nil
		}
		attempts := 1
		if r.SourceVersion == c.Version {
			attempts = r.Attempts + 1
		}
		eventTime := c.EventTime
		if r.Status != StatusSynced && r.Status != StatusDeleted && !r.EventTime.IsZero() &&
			(eventTime.IsZero() || r.EventTime.Before(eventTime)) {
			eventTime = r.EventTime
		}
		out, err = scanRecord(tx.QueryRow(ctx, `UPDATE file_record SET
				status = 'copying', claimed_until = $3, attempts = $4,
				source_version = $5, source_size = $6, source_mtime = $7, source_sequencer = $8,
				event_time = $9, updated_at = $10
			WHERE pipeline_id = $1 AND key = $2
			RETURNING `+recordColumns,
			c.PipelineID, c.Key, now.Add(lease), attempts,
			c.Version, c.Size, nullTime(c.MTime), c.Sequencer,
			nullTime(eventTime), now))
		if err != nil {
			return fmt.Errorf("record: claim update: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", Record{}, err
	}
	return dec, out, nil
}

// leaseHeldSQL matches the row only while the caller holds the lease for
// version ($3).
const leaseHeldSQL = `pipeline_id = $1 AND key = $2 AND status = 'copying'
	AND source_version = $3 AND claimed_until > clock_timestamp()`

// execLeased runs an UPDATE that only applies while the caller holds the
// lease; ErrLeaseLost otherwise.
func (s *PGStore) execLeased(ctx context.Context, set string, args ...any) error {
	tag, err := s.pool.Exec(ctx, `UPDATE file_record SET `+set+`, updated_at = clock_timestamp() WHERE `+leaseHeldSQL, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *PGStore) ExtendLease(ctx context.Context, pipelineID, key, version string, lease time.Duration) error {
	return s.execLeased(ctx, `claimed_until = clock_timestamp() + $4::interval`,
		pipelineID, key, version, lease)
}

func (s *PGStore) SetUpload(ctx context.Context, pipelineID, key, version, uploadID string) error {
	return s.execLeased(ctx, `upload_id = $4, upload_version = $3`,
		pipelineID, key, version, uploadID)
}

func (s *PGStore) Complete(ctx context.Context, pipelineID, key, version string, res SyncResult) error {
	return s.execLeased(ctx, `status = 'synced',
			synced_version = source_version, synced_mtime = source_mtime, synced_sequencer = source_sequencer,
			sha256 = $4, dest_version = $5, synced_at = COALESCE($6, clock_timestamp()),
			claimed_until = NULL, upload_id = '', upload_version = '', last_error = ''`,
		pipelineID, key, version, res.SHA256, res.DestVersion, nullTime(res.SyncedAt))
}

func (s *PGStore) Fail(ctx context.Context, pipelineID, key, version string, cause error, retryable bool) error {
	status := StatusFailed
	if retryable {
		status = StatusPending
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	// The upload session is kept so the next attempt can resume it.
	return s.execLeased(ctx, `status = $4, last_error = $5, claimed_until = NULL`,
		pipelineID, key, version, string(status), msg)
}

// MarkDeleted records a source delete at time at. A delete older than the
// source version already known (source_mtime > at) is ignored: that version
// was written after the delete. Any lease is dropped, so a worker still
// copying the deleted version gets ErrLeaseLost.
func (s *PGStore) MarkDeleted(ctx context.Context, pipelineID, key string, at time.Time) error {
	at = at.Truncate(time.Microsecond)
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		r, _, err := lockRow(ctx, tx, pipelineID, key)
		if err != nil {
			return err
		}
		if !at.IsZero() && at.Before(r.SourceMTime) {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE file_record SET
				status = 'deleted', claimed_until = NULL, event_time = $3, updated_at = clock_timestamp()
			WHERE pipeline_id = $1 AND key = $2`,
			pipelineID, key, nullTime(at))
		return err
	})
}

// List returns records after afterKey in byte order (COLLATE "C"), matching
// how object stores order keys, so the reconciler can merge the two streams.
func (s *PGStore) List(ctx context.Context, pipelineID, afterKey string, limit int) ([]Record, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+recordColumns+` FROM file_record
		WHERE pipeline_id = $1 AND key COLLATE "C" > $2
		ORDER BY key COLLATE "C" LIMIT $3`, pipelineID, afterKey, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

func collect(rows pgx.Rows) ([]Record, error) {
	defer rows.Close()
	var out []Record
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) Stats(ctx context.Context, pipelineID string) (Stats, error) {
	var (
		st                   Stats
		lastSynced, oldestEv *time.Time
	)
	err := s.pool.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE status = 'synced'),
			count(*) FILTER (WHERE status = 'pending'),
			count(*) FILTER (WHERE status = 'copying'),
			count(*) FILTER (WHERE status = 'failed'),
			COALESCE(sum(source_size) FILTER (WHERE status = 'synced'), 0)::bigint,
			max(synced_at),
			min(event_time) FILTER (WHERE status IN ('pending', 'copying'))
		FROM file_record WHERE pipeline_id = $1`, pipelineID).
		Scan(&st.Synced, &st.Pending, &st.Copying, &st.Failed, &st.BytesSynced, &lastSynced, &oldestEv)
	if err != nil {
		return Stats{}, err
	}
	st.LastSyncedAt = deref(lastSynced)
	st.OldestPendingEvent = deref(oldestEv)

	rows, err := s.pool.Query(ctx, `SELECT `+recordColumns+` FROM file_record
		WHERE pipeline_id = $1 AND (status = 'failed' OR last_error <> '')
		ORDER BY updated_at DESC LIMIT $2`, pipelineID, recentErrorsLimit)
	if err != nil {
		return Stats{}, err
	}
	st.RecentErrors, err = collect(rows)
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}
