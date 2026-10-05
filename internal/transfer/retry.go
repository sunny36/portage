package transfer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/verify"
)

// retryer retries one operation (a part read, a part upload, a commit) inside
// a copy. The job queue retries whole copies; this only smooths over
// transient errors so one throttled part doesn't fail a large copy.
type retryer struct {
	attempts int
	base     time.Duration
	max      time.Duration
	// Each attempt gets its own deadline, opBase + bytes/minRate, so a
	// stalled connection (no reset, no bytes) becomes a retryable error
	// instead of holding the key until the job timeout while the lease
	// heartbeat keeps it alive. Zero values disable the deadline.
	opBase  time.Duration
	minRate int64 // bytes per second
}

// ErrStalled reports an attempt that didn't finish within its deadline.
// It is retryable.
var ErrStalled = errors.New("transfer: operation stalled")

// opTimeout is the deadline for one attempt that moves n bytes.
func (r retryer) opTimeout(n int64) time.Duration {
	if r.opBase <= 0 {
		return 0
	}
	d := r.opBase
	if r.minRate > 0 && n > 0 {
		d += time.Duration(n / r.minRate * int64(time.Second))
	}
	return d
}

// throttleFactor stretches the backoff after ErrThrottled.
const throttleFactor = 4

func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	return connector.IsRetryable(err) && !errors.Is(err, verify.ErrMismatch)
}

// do runs op until it succeeds, fails with a non-retryable error, attempts
// run out, or ctx ends. Each attempt has the deadline for a metadata-sized
// operation; use doSized for operations that move data.
func (r retryer) do(ctx context.Context, op func(context.Context) error) error {
	return r.doSized(ctx, 0, op)
}

// doSized is do for an operation that moves n bytes.
func (r retryer) doSized(ctx context.Context, n int64, op func(context.Context) error) error {
	var err error
	for attempt := 1; ; attempt++ {
		if err = r.attempt(ctx, n, op); err == nil {
			return nil
		}
		if attempt >= r.attempts || !retryable(ctx, err) {
			return err
		}
		t := time.NewTimer(r.delay(attempt, errors.Is(err, connector.ErrThrottled)))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

// delay is exponential backoff with jitter in [d/2, d].
func (r retryer) delay(attempt int, throttled bool) time.Duration {
	d := r.base << (attempt - 1)
	if throttled {
		d *= throttleFactor
	}
	if d > r.max || d <= 0 {
		d = r.max
	}
	half := d / 2
	return half + rand.N(half+1)
}

// attempt runs op once under its own deadline.
func (r retryer) attempt(ctx context.Context, n int64, op func(context.Context) error) error {
	d := r.opTimeout(n)
	if d <= 0 {
		return op(ctx)
	}
	actx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	err := op(actx)
	if err != nil && ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: no completion within %s (%d bytes): %w", ErrStalled, d, n, err)
	}
	return err
}
