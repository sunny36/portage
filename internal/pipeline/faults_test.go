//go:build integration

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// Fault injection for engine tests.
//
// A faults value wraps every connector the engine builds for one endpoint
// (an Azure container or an S3 bucket) and its multipart Uploads. Per
// operation the test can, at runtime:
//
//   - fail the next N calls, or every call, or each call with a
//     probability (ErrThrottled, ErrPermission, ...), and clear that again;
//   - hang calls after the first `after` succeed, until released (to
//     simulate a crash mid-multipart);
//   - read how many times each operation was called and how many of those
//     calls had a fault injected.
//
// Endpoints are registered by name, so parallel tests (each with its own
// container and bucket) can inject faults independently. connectorFactory
// is swapped while at least one registration is live and restored when the
// last one is cleaned up.

// op names one connector or upload operation.
type op string

const (
	opList         op = "List"
	opStat         op = "Stat"
	opOpenRange    op = "OpenRange"
	opPutObject    op = "PutObject"
	opBeginUpload  op = "BeginUpload"
	opResumeUpload op = "ResumeUpload"
	opDelete       op = "Delete"
	opUploadPart   op = "UploadPart"
	opListParts    op = "ListParts"
	opComplete     op = "Complete"
	opAbort        op = "Abort"
)

// destWriteOps are the operations that write to a destination.
var destWriteOps = []op{opPutObject, opBeginUpload, opResumeUpload, opUploadPart, opComplete, opDelete}

type failRule struct {
	err       error
	remaining int     // calls left to fail; < 0 = until cleared
	prob      float64 // > 0: fail each call with this probability instead
}

type hangRule struct {
	after     int // hang once this many calls have succeeded
	honorCtx  bool
	release   chan struct{}
	hung      chan struct{} // closed when the first call starts hanging
	hungOnce  sync.Once
	succeeded int
}

type faults struct {
	name string

	mu       sync.Mutex
	calls    map[op]int
	injected map[op]int
	ok       map[op]int // calls that reached the provider and succeeded
	fail     map[op]*failRule
	hang     map[op]*hangRule
	rng      *rand.Rand
	// partsOK lists the part numbers of successful UploadPart calls, in
	// completion order.
	partsOK []int
}

var (
	faultsMu       sync.Mutex
	faultsByName   = map[string]*faults{}
	faultsSaved    func(context.Context, config.Endpoint) (connector.Connector, error)
	faultsRefCount int
)

// injectFaults returns the fault controller for the endpoint named name (an
// Azure container or S3 bucket), installing the wrapping connectorFactory if
// needed. Must be called before the engine starts.
func injectFaults(t *testing.T, name string) *faults {
	t.Helper()
	f := &faults{
		name:     name,
		calls:    map[op]int{},
		injected: map[op]int{},
		ok:       map[op]int{},
		fail:     map[op]*failRule{},
		hang:     map[op]*hangRule{},
		rng:      rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
	faultsMu.Lock()
	if faultsRefCount == 0 {
		faultsSaved = connectorFactory
		connectorFactory = faultyConnectorFactory
	}
	faultsRefCount++
	faultsByName[name] = f
	faultsMu.Unlock()
	t.Cleanup(func() {
		faultsMu.Lock()
		defer faultsMu.Unlock()
		delete(faultsByName, name)
		faultsRefCount--
		if faultsRefCount == 0 {
			connectorFactory = faultsSaved
			faultsSaved = nil
		}
	})
	return f
}

func faultyConnectorFactory(ctx context.Context, ep config.Endpoint) (connector.Connector, error) {
	faultsMu.Lock()
	base := faultsSaved
	var name string
	switch {
	case ep.Azure != nil:
		name = ep.Azure.Container
	case ep.S3 != nil:
		name = ep.S3.Bucket
	}
	f := faultsByName[name]
	faultsMu.Unlock()
	if base == nil {
		base = newConnector
	}
	c, err := base(ctx, ep)
	if err != nil || f == nil {
		return c, err
	}
	return &faultConnector{inner: c, f: f}, nil
}

// --- control ----------------------------------------------------------------

// FailN makes the next n calls of each op fail with err.
func (f *faults) FailN(err error, n int, ops ...op) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range ops {
		f.fail[o] = &failRule{err: err, remaining: n}
	}
}

// FailAlways makes every call of each op fail with err until Clear.
func (f *faults) FailAlways(err error, ops ...op) { f.FailN(err, -1, ops...) }

// FailProb makes each call of each op fail with err with probability p.
func (f *faults) FailProb(err error, p float64, ops ...op) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range ops {
		f.fail[o] = &failRule{err: err, remaining: -1, prob: p}
	}
}

// Clear removes failure rules for ops (all ops when none given).
func (f *faults) Clear(ops ...op) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(ops) == 0 {
		clear(f.fail)
		return
	}
	for _, o := range ops {
		delete(f.fail, o)
	}
}

// HangAfter lets calls of o run until `after` of them have succeeded (calls
// that fail, e.g. a transient provider 500, don't count) and then blocks
// every later call until release is called. With honorCtx a blocked call returns
// when its context ends (a graceful shutdown); without, it ignores the
// context, like a process that died mid-request. hung is closed when the
// first call starts blocking.
func (f *faults) HangAfter(o op, after int, honorCtx bool) (hung <-chan struct{}, release func()) {
	h := &hangRule{after: after, honorCtx: honorCtx, release: make(chan struct{}), hung: make(chan struct{})}
	f.mu.Lock()
	f.hang[o] = h
	f.mu.Unlock()
	var once sync.Once
	return h.hung, func() {
		once.Do(func() {
			f.mu.Lock()
			if f.hang[o] == h {
				delete(f.hang, o)
			}
			f.mu.Unlock()
			close(h.release)
		})
	}
}

// StopHanging makes new calls of o run normally again. Calls already
// blocked stay blocked until their release (or context).
func (f *faults) StopHanging(o op) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.hang, o)
}

// Calls returns how many times o was called (including injected failures).
func (f *faults) Calls(o op) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[o]
}

// Succeeded returns how many calls of o reached the provider and succeeded.
func (f *faults) Succeeded(o op) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ok[o]
}

// Injected returns how many calls of o got an injected failure or hang.
func (f *faults) Injected(o op) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.injected[o]
}

// errInjectedCrash is what a hung call returns when released without its
// context ending: the "process" is gone, its request never completed.
var errInjectedCrash = errors.New("faults: injected crash")

// before is called at the start of every wrapped operation. It returns the
// error to inject, or nil to run the real operation.
func (f *faults) before(ctx context.Context, o op, key string) error {
	f.mu.Lock()
	f.calls[o]++
	if h := f.hang[o]; h != nil {
		if h.succeeded >= h.after {
			f.injected[o]++
			f.mu.Unlock()
			h.hungOnce.Do(func() { close(h.hung) })
			if h.honorCtx {
				select {
				case <-h.release:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				<-h.release
			}
			return fmt.Errorf("%s %q: %w", o, key, errInjectedCrash)
		}
	}
	r := f.fail[o]
	if r == nil {
		f.mu.Unlock()
		return nil
	}
	inject := false
	switch {
	case r.prob > 0:
		inject = f.rng.Float64() < r.prob
	case r.remaining != 0:
		inject = true
		if r.remaining > 0 {
			r.remaining--
		}
	}
	if !inject {
		f.mu.Unlock()
		return nil
	}
	f.injected[o]++
	err := r.err
	f.mu.Unlock()
	return injectedError(o, key, err)
}

// injectedError looks like what a real connector returns for sentinel: the
// provider's HTTP status and error code, wrapping the sentinel.
func injectedError(o op, key string, sentinel error) error {
	status, code := 500, "InternalError"
	switch {
	case errors.Is(sentinel, connector.ErrThrottled):
		status, code = 503, "SlowDown"
	case errors.Is(sentinel, connector.ErrPermission):
		status, code = 403, "AccessDenied"
	case errors.Is(sentinel, connector.ErrAuth):
		status, code = 401, "InvalidAccessKeyId"
	case errors.Is(sentinel, connector.ErrNotFound):
		status, code = 404, "NoSuchKey"
	case errors.Is(sentinel, connector.ErrVersionChanged):
		status, code = 412, "PreconditionFailed"
	}
	return &connector.ProviderError{
		Op: string(o), Key: key, Status: status, Code: code, Sentinel: sentinel,
		Err: fmt.Errorf("injected fault (%s)", code),
	}
}

// --- wrappers ---------------------------------------------------------------

// call runs one wrapped operation: injected fault or hang first, then the
// real call, whose outcome is recorded.
func call[T any](ctx context.Context, f *faults, o op, key string, fn func() (T, error)) (T, error) {
	if err := f.before(ctx, o, key); err != nil {
		var zero T
		return zero, err
	}
	v, err := fn()
	f.after(o, err)
	return v, err
}

// after records the outcome of a real (not injected) call.
func (f *faults) after(o op, err error) {
	if err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ok[o]++
	if h := f.hang[o]; h != nil {
		h.succeeded++
	}
}

type faultConnector struct {
	inner connector.Connector
	f     *faults
}

var _ connector.Connector = (*faultConnector)(nil)

func (c *faultConnector) Name() string             { return c.inner.Name() }
func (c *faultConnector) Limits() connector.Limits { return c.inner.Limits() }

func (c *faultConnector) List(ctx context.Context, prefix, cursor string, limit int) (connector.ListPage, error) {
	return call(ctx, c.f, opList, prefix, func() (connector.ListPage, error) {
		return c.inner.List(ctx, prefix, cursor, limit)
	})
}

func (c *faultConnector) Stat(ctx context.Context, key string) (connector.ObjectInfo, error) {
	return call(ctx, c.f, opStat, key, func() (connector.ObjectInfo, error) {
		return c.inner.Stat(ctx, key)
	})
}

func (c *faultConnector) OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error) {
	return call(ctx, c.f, opOpenRange, key, func() (io.ReadCloser, error) {
		return c.inner.OpenRange(ctx, key, version, offset, length)
	})
}

func (c *faultConnector) PutObject(ctx context.Context, key string, data io.Reader, size int64, opts connector.WriteOptions) (connector.WriteResult, error) {
	return call(ctx, c.f, opPutObject, key, func() (connector.WriteResult, error) {
		return c.inner.PutObject(ctx, key, data, size, opts)
	})
}

func (c *faultConnector) BeginUpload(ctx context.Context, key string, opts connector.WriteOptions) (connector.Upload, error) {
	return call(ctx, c.f, opBeginUpload, key, func() (connector.Upload, error) {
		up, err := c.inner.BeginUpload(ctx, key, opts)
		if err != nil {
			return nil, err
		}
		return &faultUpload{inner: up, key: key, f: c.f}, nil
	})
}

func (c *faultConnector) ResumeUpload(ctx context.Context, key, uploadID string, opts connector.WriteOptions) (connector.Upload, error) {
	return call(ctx, c.f, opResumeUpload, key, func() (connector.Upload, error) {
		up, err := c.inner.ResumeUpload(ctx, key, uploadID, opts)
		if err != nil {
			return nil, err
		}
		return &faultUpload{inner: up, key: key, f: c.f}, nil
	})
}

func (c *faultConnector) Delete(ctx context.Context, key string) error {
	_, err := call(ctx, c.f, opDelete, key, func() (struct{}, error) {
		return struct{}{}, c.inner.Delete(ctx, key)
	})
	return err
}

type faultUpload struct {
	inner connector.Upload
	key   string
	f     *faults
}

var _ connector.Upload = (*faultUpload)(nil)

func (u *faultUpload) ID() string { return u.inner.ID() }

func (u *faultUpload) UploadPart(ctx context.Context, number int, data []byte) (connector.Part, error) {
	return call(ctx, u.f, opUploadPart, u.key, func() (connector.Part, error) {
		p, err := u.inner.UploadPart(ctx, number, data)
		if err == nil {
			u.f.mu.Lock()
			u.f.partsOK = append(u.f.partsOK, number)
			u.f.mu.Unlock()
		}
		return p, err
	})
}

// PartsUploaded returns the part numbers of successful UploadPart calls so
// far, in completion order.
func (f *faults) PartsUploaded() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.partsOK...)
}

func (u *faultUpload) ListParts(ctx context.Context) ([]connector.Part, error) {
	return call(ctx, u.f, opListParts, u.key, func() ([]connector.Part, error) {
		return u.inner.ListParts(ctx)
	})
}

func (u *faultUpload) Complete(ctx context.Context, parts []connector.Part) (connector.WriteResult, error) {
	return call(ctx, u.f, opComplete, u.key, func() (connector.WriteResult, error) {
		return u.inner.Complete(ctx, parts)
	})
}

func (u *faultUpload) Abort(ctx context.Context) error {
	_, err := call(ctx, u.f, opAbort, u.key, func() (struct{}, error) {
		return struct{}{}, u.inner.Abort(ctx)
	})
	return err
}
