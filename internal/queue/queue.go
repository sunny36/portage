package queue

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"

	"github.com/sunny36/portage/internal/config"
)

// Client defaults.
const (
	// JobTimeout bounds one job attempt; generous for multi-GB copies (the
	// worker extends its file-record lease while it runs).
	JobTimeout = time.Hour
	// MaxAttempts before River discards a job.
	MaxAttempts = 25
	// MaxRetryBackoff caps the exponential retry delay.
	MaxRetryBackoff = 5 * time.Minute
	// defaultConcurrency mirrors config's documented default.
	defaultConcurrency = 16
)

// migrateLockKey serialises concurrent River migrations ("portage2").
const migrateLockKey int64 = 0x706f727461676532

// Migrate brings River's tables up to date, in the first schema of the
// connection's search_path. River's migrator takes no lock, and some of its
// migrations can't share one transaction (enum ALTERs), so a session-level
// advisory lock held on a dedicated connection serialises concurrent engine
// starts while River applies each migration in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("queue: migrate lock: %w", err)
	}
	defer func() {
		// Unlock even if ctx is cancelled; if this fails the lock dies with
		// the connection.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if _, err := conn.Exec(uctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey); err != nil {
			conn.Conn().Close(uctx) //nolint:errcheck // drop the session and its lock
		}
	}()
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("queue: river migrate: %w", err)
	}
	return nil
}

// NewClient builds a River client with Portage's defaults. workers and queues
// may both be nil for an insert-only client.
func NewClient(pool *pgxpool.Pool, workers *river.Workers, queues map[string]river.QueueConfig) (*river.Client[pgx.Tx], error) {
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:      queues,
		Workers:     workers,
		JobTimeout:  JobTimeout,
		MaxAttempts: MaxAttempts,
		RetryPolicy: &RetryPolicy{Max: MaxRetryBackoff},
	})
}

// QueuesFor gives each pipeline its own copy queue (CopyQueue(name), also
// used for delete jobs) sized by its Concurrency, plus the reconcile queue.
func QueuesFor(pipelines []config.Pipeline) map[string]river.QueueConfig {
	q := make(map[string]river.QueueConfig, len(pipelines)+1)
	for _, p := range pipelines {
		n := p.Concurrency
		if n < 1 {
			n = defaultConcurrency
		}
		q[CopyQueue(p.Name)] = river.QueueConfig{MaxWorkers: n}
	}
	q[QueueReconcile] = river.QueueConfig{MaxWorkers: max(1, len(pipelines))}
	return q
}

// RetryPolicy is exponential backoff (1s, 2s, 4s, …) with ±10% jitter,
// capped at Max. Snoozes (DecisionBusy) don't count as errors.
type RetryPolicy struct {
	Max time.Duration
}

func (p *RetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	return time.Now().UTC().Add(p.backoff(len(job.Errors) + 1))
}

func (p *RetryPolicy) backoff(attempt int) time.Duration {
	d := p.Max
	if attempt < 30 {
		if e := time.Second << (attempt - 1); e < d {
			d = e
		}
	}
	jitter := time.Duration(rand.Int64N(int64(d)/5+1)) - d/10
	return max(time.Second, d+jitter)
}

// copyUniqueStates: a copy job for the same (pipeline, key, version) is
// collapsed while another is waiting or running. Completed, cancelled and
// discarded jobs don't block, so a later event for that version (e.g. after a
// destination-side loss) can enqueue again; the file record decides whether
// it actually copies. River requires available/pending/running/scheduled.
var copyUniqueStates = []rivertype.JobState{
	rivertype.JobStateAvailable,
	rivertype.JobStatePending,
	rivertype.JobStateRetryable,
	rivertype.JobStateRunning,
	rivertype.JobStateScheduled,
}

// CopyInsertOpts routes a copy job to its pipeline's queue and dedupes it on
// the CopyArgs fields tagged river:"unique" (pipeline, key, version). Use it
// for every CopyArgs insert, including InsertMany.
func CopyInsertOpts(args CopyArgs) *river.InsertOpts {
	return &river.InsertOpts{
		Queue: CopyQueue(args.PipelineID),
		UniqueOpts: river.UniqueOpts{
			ByArgs:  true,
			ByState: copyUniqueStates,
		},
	}
}

// InsertCopy enqueues a copy job; a duplicate of a waiting or running job
// for the same version is silently skipped.
func InsertCopy(ctx context.Context, client *river.Client[pgx.Tx], args CopyArgs) error {
	_, err := client.Insert(ctx, args, CopyInsertOpts(args))
	return err
}
