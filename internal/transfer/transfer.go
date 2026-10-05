// Package transfer copies one object version from a source connector to a
// destination connector by streaming through memory (never local disk).
//
// # How a copy runs
//
// Every source read is OpenRange(SrcKey, Info.Version, ...), i.e. conditional
// on the version that was Stat'ed, so bytes of two versions are never mixed;
// a change mid-copy surfaces as connector.ErrVersionChanged.
//
// Objects of at most SinglePutMax bytes are read into one pool buffer,
// hashed, and written with PutObject carrying the hex SHA-256 in
// connector.MetaSHA256 metadata.
//
// Larger objects use a multipart upload. The calling goroutine reads parts
// from the source in order into pool buffers, feeding one SHA-256 hasher and
// the verify.Sampler, then hands each buffer to up to PartConcurrency upload
// goroutines. Reading in-region is cheap; uploading is the slow part, so
// this keeps the full-content hash in order while uploads run in parallel.
//
// Known limitation (v0.1): a multipart destination object carries
// MetaSHA256 only if the source already advertised one (Info.Checksums.SHA256
// or Info.Metadata[MetaSHA256]). S3 fixes metadata when the upload starts,
// before the hash is known. Result.SHA256 is always computed; the file record
// holds it.
//
// # Memory
//
// All part buffers come from a BufferPool shared by every copy in the
// process. A copy holds at most PartConcurrency+1 buffers (one being read, the
// rest uploading). When the pool is full, readers block: that is the
// backpressure that bounds memory. A pool smaller than one part makes Copy
// fail rather than deadlock.
//
// # Resume
//
// Hooks.OnUploadStarted hands the caller the session ID before any part is
// uploaded. A later attempt for the same source version passes it back as
// Request.ResumeUploadID; parts already uploaded with the expected number and
// size are not uploaded again (their bytes are still read from the source, to
// keep the SHA-256 correct). A session whose parts don't fit the computed
// layout is aborted and a fresh one started. On failure the session is left
// in place for resume, except after ErrVersionChanged (it would hold bytes of
// a dead version), where it is aborted.
//
// # Verification
//
// After the write, the destination is Stat'ed (size, and SHA-256 if it reports
// one) and sampled ranges are read back and compared; see package verify. A
// mismatch returns an error wrapping verify.ErrMismatch.
package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/verify"
)

// Defaults for zero Options fields.
const (
	DefaultPartSize        = 8 << 20
	DefaultPartConcurrency = 4
	DefaultHeartbeatEvery  = 30 * time.Second
	DefaultOpTimeoutBase   = 60 * time.Second
	DefaultMinThroughput   = 256 << 10
	DefaultRetryAttempts   = 4
	DefaultRetryBaseDelay  = 200 * time.Millisecond
	DefaultRetryMaxDelay   = 15 * time.Second
	// abortTimeout bounds the best-effort Abort after a failed copy.
	abortTimeout = 30 * time.Second
	mib          = 1 << 20
)

// Request describes one copy.
type Request struct {
	Src    connector.Connector
	SrcKey string
	// Info is a fresh source Stat: Version, Size, ModTime, Checksums,
	// Metadata. Version must be non-empty.
	Info   connector.ObjectInfo
	Dst    connector.Connector
	DstKey string
	// ResumeUploadID is a multipart session from a previous attempt at this
	// same source version ("" for none). If it's gone (ErrNotFound), start fresh.
	ResumeUploadID string
	ContentType    string
}

// Hooks let the caller persist state and keep its lease during a copy.
type Hooks struct {
	// OnUploadStarted is called once a multipart session exists (new or
	// resumed), before any part is uploaded, so the caller can persist it for
	// resume. Error aborts the copy.
	OnUploadStarted func(ctx context.Context, uploadID string) error
	// Heartbeat is called at least every HeartbeatEvery during the copy (lease
	// renewal). An error cancels the copy (the upload is NOT aborted: another
	// worker may own it now) and is returned wrapped.
	Heartbeat func(ctx context.Context) error
	// OnBytes reports bytes uploaded to the destination as they go (metrics).
	// Optional. Called concurrently from upload goroutines.
	OnBytes func(n int64)
}

// Options tune a copy. Zero values mean the defaults above.
type Options struct {
	// PartSize is the desired part size; it is clamped to Dst.Limits() and
	// grown so that the object needs at most MaxParts parts.
	PartSize        int64
	PartConcurrency int
	// Pool is shared by all copies in the process and bounds memory. If nil,
	// the copy gets a private pool of (PartConcurrency+1) parts.
	Pool           *BufferPool
	HeartbeatEvery time.Duration
	// SinglePutMax: objects <= this use one PutObject (default = PartSize,
	// capped at Dst.Limits().SinglePutMax).
	SinglePutMax int64

	// RetryAttempts is the number of tries for one part read/upload or
	// commit before the copy fails (default 4).
	RetryAttempts  int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// OpTimeoutBase and MinThroughput set each attempt's deadline:
	// OpTimeoutBase + bytes/MinThroughput (defaults 60s and 256 KiB/s, so
	// a 64 MiB part gets ~5 minutes). A negative OpTimeoutBase disables it.
	OpTimeoutBase time.Duration
	MinThroughput int64
	// SampleSeed picks the verification sample ranges; 0 means random.
	SampleSeed uint64
}

// Result describes a finished copy.
type Result struct {
	SHA256      []byte // of the full source content, computed while streaming
	DestVersion string
	Bytes       int64
	PartsReused int // parts skipped thanks to resume
	// UploadID is the multipart session used ("" for a single put).
	UploadID string
	Verified verify.Report
}

// Copy copies req.Info.Version of req.SrcKey to req.DstKey.
func Copy(ctx context.Context, req Request, opts Options, hooks Hooks) (Result, error) {
	if req.Src == nil || req.Dst == nil {
		return Result{}, errors.New("transfer: Src and Dst are required")
	}
	if req.Info.Version == "" {
		return Result{}, fmt.Errorf("transfer: %q: Info.Version is required (reads are conditional on it)", req.SrcKey)
	}
	if req.Info.Size < 0 {
		return Result{}, fmt.Errorf("transfer: %q: negative size %d", req.SrcKey, req.Info.Size)
	}
	lim := req.Dst.Limits()
	partSize, nParts, err := layout(req.Info.Size, opts.PartSize, lim)
	if err != nil {
		return Result{}, fmt.Errorf("transfer: %q: %w", req.SrcKey, err)
	}
	opts = withDefaults(opts, partSize, lim)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopHeartbeat := startHeartbeat(ctx, cancel, hooks.Heartbeat, opts.HeartbeatEvery)

	c := &copier{
		req:   req,
		opts:  opts,
		hooks: hooks,
		rt: retryer{
			attempts: opts.RetryAttempts, base: opts.RetryBaseDelay, max: opts.RetryMaxDelay,
			opBase: opts.OpTimeoutBase, minRate: opts.MinThroughput,
		},
		hasher:   sha256.New(),
		sampler:  verify.NewSampler(req.Info.Size, opts.SampleSeed),
		partSize: partSize,
		nParts:   nParts,
	}
	var res Result
	if req.Info.Size <= opts.SinglePutMax {
		res, err = c.single(ctx)
	} else {
		res, err = c.multipart(ctx)
	}
	if hbErr := stopHeartbeat(); hbErr != nil && err != nil {
		return res, fmt.Errorf("transfer: %q: heartbeat failed, copy cancelled: %w", req.SrcKey, hbErr)
	}
	return res, err
}

func withDefaults(o Options, partSize int64, lim connector.Limits) Options {
	o.PartSize = partSize
	if o.PartConcurrency <= 0 {
		o.PartConcurrency = DefaultPartConcurrency
	}
	if o.HeartbeatEvery <= 0 {
		o.HeartbeatEvery = DefaultHeartbeatEvery
	}
	if o.OpTimeoutBase == 0 {
		o.OpTimeoutBase = DefaultOpTimeoutBase
	}
	if o.MinThroughput <= 0 {
		o.MinThroughput = DefaultMinThroughput
	}
	if o.SinglePutMax <= 0 {
		o.SinglePutMax = partSize
	}
	if lim.SinglePutMax > 0 && o.SinglePutMax > lim.SinglePutMax {
		o.SinglePutMax = lim.SinglePutMax
	}
	if o.RetryAttempts <= 0 {
		o.RetryAttempts = DefaultRetryAttempts
	}
	if o.RetryBaseDelay <= 0 {
		o.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if o.RetryMaxDelay <= 0 {
		o.RetryMaxDelay = DefaultRetryMaxDelay
	}
	if o.SampleSeed == 0 {
		var b [8]byte
		_, _ = rand.Read(b[:])
		o.SampleSeed = binary.LittleEndian.Uint64(b[:]) | 1
	}
	if o.Pool == nil {
		o.Pool = NewBufferPool(max(partSize, o.SinglePutMax) * int64(o.PartConcurrency+1))
	}
	return o
}

// layout picks the part size for an object of size bytes and returns it with
// the number of parts.
func layout(size, want int64, lim connector.Limits) (int64, int, error) {
	part := want
	if part <= 0 {
		part = DefaultPartSize
	}
	if lim.MaxPartSize > 0 && part > lim.MaxPartSize {
		part = lim.MaxPartSize
	}
	if part < lim.MinPartSize {
		part = lim.MinPartSize
	}
	if lim.MaxParts > 0 {
		need := (size + int64(lim.MaxParts) - 1) / int64(lim.MaxParts)
		if part < need {
			part = (need + mib - 1) / mib * mib
			if lim.MaxPartSize > 0 && part > lim.MaxPartSize {
				part = need
			}
		}
		if lim.MaxPartSize > 0 && part > lim.MaxPartSize {
			return 0, 0, fmt.Errorf("object of %d bytes needs parts of %d bytes, over the destination's max part size %d", size, need, lim.MaxPartSize)
		}
	}
	return part, int((size + part - 1) / part), nil
}

// startHeartbeat calls hb every interval until the returned stop function is
// called. A failing hb cancels the copy via cancel; stop waits for the
// goroutine and returns that failure, if any.
func startHeartbeat(ctx context.Context, cancel context.CancelFunc, hb func(context.Context) error, every time.Duration) (stop func() error) {
	if hb == nil {
		return func() error { return nil }
	}
	hbCtx, hbCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var hbErr error
	go func() {
		defer close(done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := hb(hbCtx); err != nil {
					if hbCtx.Err() == nil {
						hbErr = err
						cancel()
					}
					return
				}
			}
		}
	}()
	return func() error {
		hbCancel()
		<-done
		return hbErr
	}
}

type copier struct {
	req      Request
	opts     Options
	hooks    Hooks
	rt       retryer
	hasher   hash.Hash
	sampler  *verify.Sampler
	partSize int64
	nParts   int
}

func (c *copier) onBytes(n int64) {
	if c.hooks.OnBytes != nil && n > 0 {
		c.hooks.OnBytes(n)
	}
}

// readInto fills buf with source bytes [off, off+len(buf)) of Info.Version
// and feeds them to the hasher and sampler. Calls must be in offset order.
func (c *copier) readInto(ctx context.Context, off int64, buf []byte) error {
	err := c.rt.doSized(ctx, int64(len(buf)), func(ctx context.Context) error {
		rc, err := c.req.Src.OpenRange(ctx, c.req.SrcKey, c.req.Info.Version, off, int64(len(buf)))
		if err != nil {
			return err
		}
		defer rc.Close()
		if n, err := io.ReadFull(rc, buf); err != nil {
			return fmt.Errorf("read %d of %d bytes: %w", n, len(buf), err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("transfer: read source %q [%d,+%d): %w", c.req.SrcKey, off, len(buf), err)
	}
	c.hasher.Write(buf)
	c.sampler.Observe(off, buf)
	return nil
}

func (c *copier) single(ctx context.Context) (Result, error) {
	size := c.req.Info.Size
	buf, err := c.opts.Pool.acquire(ctx, size)
	if err != nil {
		return Result{}, err
	}
	release := func() {
		if buf != nil {
			c.opts.Pool.release(buf)
			buf = nil
		}
	}
	defer release()
	if buf == nil {
		buf = []byte{}
	}
	if err := c.readInto(ctx, 0, buf); err != nil {
		return Result{}, err
	}
	sum := c.hasher.Sum(nil)
	wo := connector.WriteOptions{
		ContentType: c.req.ContentType,
		Metadata:    map[string]string{connector.MetaSHA256: hex.EncodeToString(sum)},
	}
	var wr connector.WriteResult
	err = c.rt.doSized(ctx, size, func(ctx context.Context) error {
		var err error
		wr, err = c.req.Dst.PutObject(ctx, c.req.DstKey, bytes.NewReader(buf), size, wo)
		return err
	})
	if err != nil {
		return Result{SHA256: sum}, fmt.Errorf("transfer: put %q: %w", c.req.DstKey, err)
	}
	c.onBytes(size)
	release()
	return c.finish(ctx, Result{SHA256: sum, DestVersion: wr.Version, Bytes: size})
}

// sourceSHA returns the SHA-256 the source advertises for this version, if any.
func (c *copier) sourceSHA() []byte {
	if s := c.req.Info.Checksums.SHA256; len(s) == sha256.Size {
		return s
	}
	if b, err := hex.DecodeString(c.req.Info.Metadata[connector.MetaSHA256]); err == nil && len(b) == sha256.Size {
		return b
	}
	return nil
}

func (c *copier) partLen(n int) int64 {
	off := int64(n-1) * c.partSize
	return min(c.partSize, c.req.Info.Size-off)
}

func (c *copier) abort(ctx context.Context, up connector.Upload) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	_ = up.Abort(actx) // best effort: an orphaned session expires on its own
}

// openSession resumes ResumeUploadID if it is usable, else begins a new
// upload. It returns the parts that can be reused.
func (c *copier) openSession(ctx context.Context, wo connector.WriteOptions) (connector.Upload, map[int]connector.Part, bool, error) {
	if id := c.req.ResumeUploadID; id != "" {
		var up connector.Upload
		err := c.rt.do(ctx, func(ctx context.Context) error {
			var err error
			up, err = c.req.Dst.ResumeUpload(ctx, c.req.DstKey, id, wo)
			return err
		})
		var parts []connector.Part
		if err == nil {
			err = c.rt.do(ctx, func(ctx context.Context) error {
				var err error
				parts, err = up.ListParts(ctx)
				return err
			})
		}
		switch {
		case err == nil:
			if reuse, ok := c.matchLayout(parts); ok {
				return up, reuse, false, nil
			}
			c.abort(ctx, up)
		case errors.Is(err, connector.ErrNotFound):
			// Expired, completed or aborted: start over.
		default:
			return nil, nil, false, fmt.Errorf("transfer: resume upload %q of %q: %w", id, c.req.DstKey, err)
		}
	}
	var up connector.Upload
	err := c.rt.do(ctx, func(ctx context.Context) error {
		var err error
		up, err = c.req.Dst.BeginUpload(ctx, c.req.DstKey, wo)
		return err
	})
	if err != nil {
		return nil, nil, false, fmt.Errorf("transfer: begin upload %q: %w", c.req.DstKey, err)
	}
	return up, nil, true, nil
}

// matchLayout reports whether every uploaded part fits the current layout and
// returns them by number.
func (c *copier) matchLayout(parts []connector.Part) (map[int]connector.Part, bool) {
	reuse := make(map[int]connector.Part, len(parts))
	for _, p := range parts {
		if p.Number < 1 || p.Number > c.nParts || p.Size != c.partLen(p.Number) {
			return nil, false
		}
		reuse[p.Number] = p
	}
	return reuse, true
}

func (c *copier) multipart(ctx context.Context) (Result, error) {
	res := Result{Bytes: c.req.Info.Size}
	srcSHA := c.sourceSHA()
	wo := connector.WriteOptions{ContentType: c.req.ContentType}
	if srcSHA != nil {
		wo.Metadata = map[string]string{connector.MetaSHA256: hex.EncodeToString(srcSHA)}
	}
	up, reuse, fresh, err := c.openSession(ctx, wo)
	if err != nil {
		return res, err
	}
	res.UploadID = up.ID()
	if c.hooks.OnUploadStarted != nil {
		if err := c.hooks.OnUploadStarted(ctx, up.ID()); err != nil {
			if fresh {
				c.abort(ctx, up) // nobody knows about it
			}
			return res, fmt.Errorf("transfer: OnUploadStarted %q: %w", up.ID(), err)
		}
	}

	parts, reused, err := c.streamParts(ctx, up, reuse)
	res.PartsReused = reused
	if err != nil {
		if errors.Is(err, connector.ErrVersionChanged) && ctx.Err() == nil {
			c.abort(ctx, up) // its parts hold bytes of a dead version
		}
		return res, err
	}
	res.SHA256 = c.hasher.Sum(nil)
	if srcSHA != nil && !bytes.Equal(srcSHA, res.SHA256) {
		// The session carries the wrong hash in its metadata; discard it.
		c.abort(ctx, up)
		return res, fmt.Errorf("%w: source %q advertises sha256 %x but its content hashes to %x",
			verify.ErrMismatch, c.req.SrcKey, srcSHA, res.SHA256)
	}

	wr, err := c.complete(ctx, up, parts)
	if err != nil {
		return res, err
	}
	res.DestVersion = wr.Version
	return c.finish(ctx, res)
}

// streamParts reads every part from the source in order and uploads the ones
// not in reuse, PartConcurrency at a time. It returns all parts in order.
func (c *copier) streamParts(ctx context.Context, up connector.Upload, reuse map[int]connector.Part) ([]connector.Part, int, error) {
	parts := make([]connector.Part, c.nParts)
	reused := 0
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.opts.PartConcurrency)
	var readErr error
	for n := 1; n <= c.nParts; n++ {
		size := c.partLen(n)
		buf, err := c.opts.Pool.acquire(gctx, size)
		if err != nil {
			readErr = err
			break
		}
		if err := c.readInto(gctx, int64(n-1)*c.partSize, buf); err != nil {
			c.opts.Pool.release(buf)
			readErr = err
			break
		}
		if p, ok := reuse[n]; ok {
			c.opts.Pool.release(buf)
			parts[n-1] = p
			reused++
			continue
		}
		g.Go(func() error {
			defer c.opts.Pool.release(buf)
			var p connector.Part
			err := c.rt.doSized(gctx, int64(len(buf)), func(ctx context.Context) error {
				var err error
				p, err = up.UploadPart(ctx, n, buf)
				return err
			})
			if err != nil {
				return fmt.Errorf("transfer: upload part %d/%d of %q: %w", n, c.nParts, c.req.DstKey, err)
			}
			parts[n-1] = p
			c.onBytes(size)
			return nil
		})
	}
	upErr := g.Wait()
	switch {
	case readErr != nil && (upErr == nil || !errors.Is(readErr, context.Canceled)):
		return nil, reused, readErr
	case upErr != nil:
		return nil, reused, upErr
	}
	return parts, reused, nil
}

// complete commits the upload. If a retried Complete finds the session gone,
// an earlier attempt probably succeeded with its response lost; the
// destination's current version is then taken and verification decides.
func (c *copier) complete(ctx context.Context, up connector.Upload, parts []connector.Part) (connector.WriteResult, error) {
	var wr connector.WriteResult
	tries := 0
	err := c.rt.doSized(ctx, c.req.Info.Size/16, func(ctx context.Context) error {
		tries++
		var err error
		wr, err = up.Complete(ctx, parts)
		return err
	})
	if err != nil && tries > 1 && errors.Is(err, connector.ErrNotFound) {
		if info, serr := c.req.Dst.Stat(ctx, c.req.DstKey); serr == nil && info.Size == c.req.Info.Size {
			return connector.WriteResult{Version: info.Version, Checksums: info.Checksums}, nil
		}
	}
	if err != nil {
		return wr, fmt.Errorf("transfer: complete upload %q of %q: %w", up.ID(), c.req.DstKey, err)
	}
	return wr, nil
}

// finish verifies the destination and fills res.Verified.
func (c *copier) finish(ctx context.Context, res Result) (Result, error) {
	var st, sr verify.Report
	err := c.rt.do(ctx, func(ctx context.Context) error {
		var err error
		st, err = verify.CheckStat(ctx, c.req.Dst, c.req.DstKey, res.DestVersion, res.Bytes, res.SHA256)
		return err
	})
	if err == nil {
		err = c.rt.do(ctx, func(ctx context.Context) error {
			var err error
			sr, err = c.sampler.Check(ctx, c.req.Dst, c.req.DstKey, res.DestVersion)
			return err
		})
	}
	res.Verified = verify.Report{
		Method:       st.Method + "+" + sr.Method,
		SampledBytes: sr.SampledBytes,
		OK:           err == nil && st.OK && sr.OK,
		Detail:       joinDetail(st.Detail, sr.Detail),
	}
	if err != nil {
		return res, fmt.Errorf("transfer: verify %q: %w", c.req.DstKey, err)
	}
	return res, nil
}

func joinDetail(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + "; " + b
}
