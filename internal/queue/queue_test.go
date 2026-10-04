package queue

import (
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"

	"github.com/sunny36/portage/internal/config"
)

func TestRetryPolicyBackoff(t *testing.T) {
	p := &RetryPolicy{Max: 5 * time.Minute}
	for attempt, want := range map[int]time.Duration{
		1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second,
		9: 256 * time.Second, 10: 5 * time.Minute, 25: 5 * time.Minute, 100: 5 * time.Minute,
	} {
		for i := 0; i < 50; i++ {
			got := p.backoff(attempt)
			lo, hi := want-want/10, want+want/10
			if lo < time.Second {
				lo = time.Second
			}
			if got < lo || got > hi {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v]", attempt, got, lo, hi)
			}
		}
	}
	next := p.NextRetry(&rivertype.JobRow{Errors: make([]rivertype.AttemptError, 30)})
	if d := time.Until(next); d > p.Max+p.Max/10 || d < p.Max-p.Max/10-time.Second {
		t.Fatalf("NextRetry after many errors is %v away, want ≈ %v", d, p.Max)
	}
}

func TestQueuesFor(t *testing.T) {
	q := QueuesFor([]config.Pipeline{{Name: "a", Concurrency: 4}, {Name: "b-2"}})
	if len(q) != 3 {
		t.Fatalf("queues = %v", q)
	}
	if q["copy_a"].MaxWorkers != 4 || q["copy_b-2"].MaxWorkers != defaultConcurrency || q[QueueReconcile].MaxWorkers != 2 {
		t.Fatalf("queues = %+v", q)
	}
}

func TestCopyInsertOpts(t *testing.T) {
	o := CopyInsertOpts(CopyArgs{PipelineID: "p", Key: "k", Version: "v"})
	if o.Queue != "copy_p" || !o.UniqueOpts.ByArgs {
		t.Fatalf("opts = %+v", o)
	}
}
