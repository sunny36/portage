package transfer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

// BufferPool bounds the bytes held by in-flight part buffers across every
// copy in the process. Acquire blocks (FIFO, so large requests are not
// starved) until enough capacity is free; that is the engine's memory
// backpressure. Released buffers are recycled per size to spare the GC.
type BufferPool struct {
	max   int64
	sem   *semaphore.Weighted
	inUse atomic.Int64
	peak  atomic.Int64
	free  sync.Map // int64 capacity -> *sync.Pool of *[]byte
}

// NewBufferPool bounds total bytes held by in-flight part buffers across all
// copies. maxBytes must be at least the largest part (or single-put object)
// any copy will use; see Copy.
func NewBufferPool(maxBytes int64) *BufferPool {
	if maxBytes < 1 {
		maxBytes = 1
	}
	return &BufferPool{max: maxBytes, sem: semaphore.NewWeighted(maxBytes)}
}

// Cap returns the pool's byte limit.
func (p *BufferPool) Cap() int64 { return p.max }

// InUse returns the bytes currently held by acquired buffers.
func (p *BufferPool) InUse() int64 { return p.inUse.Load() }

// recycleMin is the smallest buffer recycled. Larger requests are rounded up
// to a multiple of it, so the set of recycled sizes stays small (part sizes
// are normally MiB multiples, so they waste nothing); smaller ones are just
// allocated.
const recycleMin = 1 << 20

// class returns the bytes actually allocated (and charged) for a request.
func class(n int64) int64 {
	if n < recycleMin {
		return n
	}
	return (n + recycleMin - 1) / recycleMin * recycleMin
}

// acquire returns a buffer of exactly n bytes, waiting for capacity. It
// fails if n can never fit or ctx ends first.
func (p *BufferPool) acquire(ctx context.Context, n int64) ([]byte, error) {
	c := class(n)
	if c > p.max {
		return nil, fmt.Errorf("transfer: buffer of %d bytes exceeds pool capacity %d (raise the pool size or lower PartSize/SinglePutMax)", n, p.max)
	}
	if n <= 0 {
		return nil, nil
	}
	if err := p.sem.Acquire(ctx, c); err != nil {
		return nil, err
	}
	cur := p.inUse.Add(c)
	for {
		pk := p.peak.Load()
		if cur <= pk || p.peak.CompareAndSwap(pk, cur) {
			break
		}
	}
	if c >= recycleMin {
		if b, ok := p.sizePool(c).Get().(*[]byte); ok && b != nil {
			return (*b)[:n], nil
		}
	}
	return make([]byte, n, c), nil
}

// release returns a buffer obtained from acquire. b must not be used after.
func (p *BufferPool) release(b []byte) {
	c := int64(cap(b))
	if c == 0 {
		return
	}
	if c >= recycleMin {
		b = b[:c]
		p.sizePool(c).Put(&b)
	}
	p.inUse.Add(-c)
	p.sem.Release(c)
}

func (p *BufferPool) sizePool(c int64) *sync.Pool {
	if v, ok := p.free.Load(c); ok {
		return v.(*sync.Pool)
	}
	v, _ := p.free.LoadOrStore(c, &sync.Pool{})
	return v.(*sync.Pool)
}
