package transfer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

// BufferPool bounds the bytes held by part buffers across every copy in the
// process: buffers in use plus idle buffers kept for reuse never exceed Cap.
// Acquire blocks (FIFO, so large requests are not starved) until enough
// capacity is free; that is the engine's memory backpressure. Idle buffers
// are recycled per size class; when capacity is needed for a different size
// they are dropped (left to the GC) rather than kept beyond the cap. The
// previous design recycled through unbounded sync.Pools, which let idle
// buffers pile up outside the cap (the local soak saw a 4 GB heap with a
// 2 GiB cap).
type BufferPool struct {
	max   int64
	sem   *semaphore.Weighted
	inUse atomic.Int64
	peak  atomic.Int64

	mu        sync.Mutex
	idle      map[int64][][]byte // size class -> idle buffers
	idleBytes int64
}

// NewBufferPool bounds total bytes held by part buffers across all copies.
// maxBytes must be at least the largest part (or single-put object) any copy
// will use; see Copy.
func NewBufferPool(maxBytes int64) *BufferPool {
	if maxBytes < 1 {
		maxBytes = 1
	}
	return &BufferPool{max: maxBytes, sem: semaphore.NewWeighted(maxBytes), idle: map[int64][][]byte{}}
}

// Cap returns the pool's byte limit.
func (p *BufferPool) Cap() int64 { return p.max }

// InUse returns the bytes currently held by acquired buffers.
func (p *BufferPool) InUse() int64 { return p.inUse.Load() }

// Idle returns the bytes held by idle buffers kept for reuse.
func (p *BufferPool) Idle() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idleBytes
}

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

	p.mu.Lock()
	var buf []byte
	if c >= recycleMin {
		if list := p.idle[c]; len(list) > 0 {
			buf = list[len(list)-1]
			p.idle[c] = list[:len(list)-1]
			p.idleBytes -= c
		}
	}
	// Keep in-use + idle within the cap: drop idle buffers of other sizes.
	p.evictLocked(cur)
	p.mu.Unlock()

	if buf != nil {
		return buf[:n], nil
	}
	return make([]byte, n, c), nil
}

// release returns a buffer obtained from acquire. b must not be used after.
func (p *BufferPool) release(b []byte) {
	c := int64(cap(b))
	if c == 0 {
		return
	}
	cur := p.inUse.Add(-c)
	if c >= recycleMin {
		p.mu.Lock()
		if cur+p.idleBytes+c <= p.max {
			p.idle[c] = append(p.idle[c], b[:c])
			p.idleBytes += c
		}
		p.mu.Unlock()
	}
	p.sem.Release(c)
}

// evictLocked drops idle buffers until inUse + idle <= max. p.mu is held.
func (p *BufferPool) evictLocked(inUse int64) {
	for size, list := range p.idle {
		for len(list) > 0 && inUse+p.idleBytes > p.max {
			list[len(list)-1] = nil
			list = list[:len(list)-1]
			p.idleBytes -= size
		}
		if len(list) == 0 {
			delete(p.idle, size)
		} else {
			p.idle[size] = list
		}
		if inUse+p.idleBytes <= p.max {
			return
		}
	}
}
