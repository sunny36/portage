package transfer

import (
	"context"
	"errors"
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
// run out, or ctx ends.
func (r retryer) do(ctx context.Context, op func(context.Context) error) error {
	var err error
	for attempt := 1; ; attempt++ {
		if err = op(ctx); err == nil {
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
