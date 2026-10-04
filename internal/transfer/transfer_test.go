package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/transfer/internal/memconn"
	"github.com/sunny36/portage/internal/verify"
)

const kib = 1 << 10

// Small limits keep the unit tests fast while exercising multipart.
var testLimits = connector.Limits{
	MinPartSize:  64 * kib,
	MaxPartSize:  1 << 20,
	MaxParts:     10000,
	SinglePutMax: 1 << 20,
}

func randData(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

type fixture struct {
	src, dst *memconn.Conn
	data     []byte
	req      Request
}

func newFixture(t *testing.T, size int) *fixture {
	t.Helper()
	f := &fixture{src: memconn.New(testLimits), dst: memconn.New(testLimits), data: randData(uint64(size)+1, size)}
	f.src.Set("src/obj", f.data, nil)
	info, err := f.src.Stat(context.Background(), "src/obj")
	if err != nil {
		t.Fatal(err)
	}
	f.req = Request{Src: f.src, SrcKey: "src/obj", Info: info, Dst: f.dst, DstKey: "dst/obj", ContentType: "application/octet-stream"}
	return f
}

func testOpts() Options {
	return Options{
		PartSize:        64 * kib,
		PartConcurrency: 3,
		RetryBaseDelay:  time.Millisecond,
		RetryMaxDelay:   5 * time.Millisecond,
	}
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (f *fixture) assertCopied(t *testing.T, res Result) {
	t.Helper()
	got, _, ok := f.dst.Get("dst/obj")
	if !ok {
		t.Fatal("destination object missing")
	}
	if !bytes.Equal(got, f.data) {
		t.Fatalf("destination content differs (len %d vs %d)", len(got), len(f.data))
	}
	want := sha256.Sum256(f.data)
	if !bytes.Equal(res.SHA256, want[:]) {
		t.Fatalf("SHA256 = %x, want %x", res.SHA256, want)
	}
	if res.Bytes != int64(len(f.data)) {
		t.Fatalf("Bytes = %d, want %d", res.Bytes, len(f.data))
	}
	if !res.Verified.OK {
		t.Fatalf("Verified = %+v", res.Verified)
	}
	info, err := f.dst.Stat(context.Background(), "dst/obj")
	if err != nil || info.Version != res.DestVersion {
		t.Fatalf("dst Stat = %+v, %v; DestVersion %q", info, err, res.DestVersion)
	}
}

func TestCopySizes(t *testing.T) {
	part := 64 * kib
	for _, tc := range []struct {
		name      string
		size      int
		multipart bool
	}{
		{"zero", 0, false},
		{"one", 1, false},
		{"1KiB", kib, false},
		{"exactlySinglePutMax", part, false},
		{"singlePutMaxPlusOne", part + 1, true},
		{"threePartsPlusOneByte", 3*part + 1, true},
		{"exactMultiple", 5 * part, true},
		{"overSampleWindow", 3<<20 + 17, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, tc.size)
			var uploaded atomic.Int64
			var started []string
			res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{
				OnBytes:         func(n int64) { uploaded.Add(n) },
				OnUploadStarted: func(_ context.Context, id string) error { started = append(started, id); return nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			f.assertCopied(t, res)
			if uploaded.Load() != int64(tc.size) {
				t.Fatalf("OnBytes total %d, want %d", uploaded.Load(), tc.size)
			}
			_, meta, _ := f.dst.Get("dst/obj")
			if tc.multipart {
				if f.dst.Puts.Load() != 0 || len(started) != 1 || res.UploadID != started[0] {
					t.Fatalf("puts=%d started=%v uploadID=%q", f.dst.Puts.Load(), started, res.UploadID)
				}
				if _, ok := meta[connector.MetaSHA256]; ok {
					t.Fatalf("multipart without a source hash must not set %s", connector.MetaSHA256)
				}
				if f.dst.PeakConcurrentParts() > 3 {
					t.Fatalf("peak concurrent parts %d > 3", f.dst.PeakConcurrentParts())
				}
				if f.dst.OpenUploads() != 0 {
					t.Fatal("upload left open")
				}
			} else {
				want := sha256.Sum256(f.data)
				if f.dst.Puts.Load() != 1 || len(started) != 0 {
					t.Fatalf("puts=%d started=%v", f.dst.Puts.Load(), started)
				}
				if meta[connector.MetaSHA256] != hex.EncodeToString(want[:]) {
					t.Fatalf("metadata %v lacks sha256", meta)
				}
				if res.Verified.Method != "size+sha256+sample" {
					t.Fatalf("Method = %q", res.Verified.Method)
				}
			}
			wantSampled := min(int64(tc.size), verify.WholeObjectMax)
			if tc.size > verify.WholeObjectMax {
				wantSampled = 2*verify.EdgeBytes + verify.WindowBytes
			}
			if res.Verified.SampledBytes != wantSampled {
				t.Fatalf("SampledBytes = %d, want %d", res.Verified.SampledBytes, wantSampled)
			}
		})
	}
}

func TestLayout(t *testing.T) {
	lim := connector.Limits{MinPartSize: 5 << 20, MaxPartSize: 5 << 30, MaxParts: 10000}
	for _, tc := range []struct {
		size, want, part int64
		n                int
	}{
		{100 << 20, 8 << 20, 8 << 20, 13},
		{100 << 20, 1 << 20, 5 << 20, 20}, // clamped up to MinPartSize
		{100 << 20, 0, DefaultPartSize, 13},
		{100 << 30, 8 << 20, 11 << 20, 9310}, // grown to fit 10000 parts (rounded to MiB)
		{1 << 40, 8 << 20, 105 << 20, 9987},  // 1 TiB
		{100 << 20, 10 << 30, 5 << 30, 1},    // clamped down to MaxPartSize
		{5 << 20, 5 << 20, 5 << 20, 1},
	} {
		part, n, err := layout(tc.size, tc.want, lim)
		if err != nil || part != tc.part || n != tc.n {
			t.Errorf("layout(%d, %d) = %d, %d, %v; want %d, %d", tc.size, tc.want, part, n, err, tc.part, tc.n)
		}
		if int64(n) > int64(lim.MaxParts) {
			t.Errorf("layout(%d) = %d parts", tc.size, n)
		}
	}
	if _, _, err := layout(lim.MaxPartSize*int64(lim.MaxParts)+1, 0, lim); err == nil {
		t.Error("object over MaxPartSize*MaxParts must fail")
	}
}

func TestThrottledPartsRetried(t *testing.T) {
	f := newFixture(t, 5*64*kib)
	var throttles atomic.Int64
	f.dst.UploadPartHook = func(n int) error {
		if n == 2 && throttles.Add(1) <= 3 {
			return &connector.ProviderError{Op: "UploadPart", Status: 503, Code: "SlowDown", Sentinel: connector.ErrThrottled}
		}
		return nil
	}
	var reads atomic.Int64
	f.src.ReadHook = func(string, int64, int64) error {
		if reads.Add(1) == 3 {
			return &connector.ProviderError{Op: "OpenRange", Status: 500, Err: errors.New("internal error")}
		}
		return nil
	}
	res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if got := f.dst.UploadPartCalls.Load(); got != 5+3 {
		t.Fatalf("UploadPart calls = %d, want 8", got)
	}
}

func TestRetriesExhausted(t *testing.T) {
	f := newFixture(t, 3*64*kib)
	f.dst.UploadPartHook = func(int) error { return connector.ErrThrottled }
	_, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
	if !errors.Is(err, connector.ErrThrottled) {
		t.Fatalf("err = %v, want ErrThrottled", err)
	}
	if f.dst.OpenUploads() != 1 {
		t.Fatal("failed copy must leave its session for resume")
	}
}

func TestNonRetryablePartFailsFast(t *testing.T) {
	f := newFixture(t, 20*64*kib)
	f.dst.UploadPartHook = func(n int) error {
		if n == 3 {
			return &connector.ProviderError{Op: "UploadPart", Status: 403, Sentinel: connector.ErrPermission}
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	}
	_, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
	if !errors.Is(err, connector.ErrPermission) {
		t.Fatalf("err = %v, want ErrPermission", err)
	}
	if calls := f.dst.UploadPartCalls.Load(); calls >= 20 {
		t.Fatalf("copy did not stop early: %d UploadPart calls", calls)
	}
	if _, _, ok := f.dst.Get("dst/obj"); ok {
		t.Fatal("destination written despite failure")
	}
	if f.dst.OpenUploads() != 1 {
		t.Fatal("session must be left for resume")
	}
}

func TestVersionChangedMidCopy(t *testing.T) {
	for _, size := range []int{10 * kib, 10 * 64 * kib} {
		f := newFixture(t, size)
		var reads atomic.Int64
		f.src.ReadHook = func(key string, _, _ int64) error {
			if reads.Add(1) == 2 {
				f.src.Set(key, []byte("new version"), nil)
			}
			return nil
		}
		if size < 64*kib {
			// Single put reads once; change the object before that read.
			f.src.Set("src/obj", []byte("newer"), nil)
		}
		var upID string
		_, err := Copy(ctxT(t), f.req, testOpts(), Hooks{OnUploadStarted: func(_ context.Context, id string) error { upID = id; return nil }})
		if !errors.Is(err, connector.ErrVersionChanged) {
			t.Fatalf("size %d: err = %v, want ErrVersionChanged", size, err)
		}
		if _, _, ok := f.dst.Get("dst/obj"); ok {
			t.Fatalf("size %d: destination written", size)
		}
		if upID != "" && !f.dst.Aborted(upID) {
			t.Fatalf("size %d: upload not aborted after version change", size)
		}
	}
}

func TestCorruptionDetected(t *testing.T) {
	for _, size := range []int{10 * kib, 2 << 20, 3 << 20} {
		f := newFixture(t, size)
		f.dst.CorruptAt = 100
		res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
		if !errors.Is(err, verify.ErrMismatch) {
			t.Fatalf("size %d: err = %v, want ErrMismatch", size, err)
		}
		if res.Verified.OK || res.Verified.Detail == "" {
			t.Fatalf("size %d: Verified = %+v", size, res.Verified)
		}
	}
}

func TestResumeAfterCrash(t *testing.T) {
	const nParts = 12
	f := newFixture(t, nParts*64*kib-500)
	ctx, crash := context.WithCancel(ctxT(t))
	var stored atomic.Int64
	f.dst.UploadPartHook = func(int) error {
		if stored.Add(1) > 5 {
			crash()
			return context.Canceled
		}
		return nil
	}
	var uploadID string
	_, err := Copy(ctx, f.req, testOpts(), Hooks{OnUploadStarted: func(_ context.Context, id string) error {
		uploadID = id
		return nil
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("crashed copy err = %v", err)
	}
	if f.dst.OpenUploads() != 1 || uploadID == "" {
		t.Fatalf("session %q not left open", uploadID)
	}
	have := f.dst.PartsStored.Load()

	f.dst.UploadPartHook = nil
	f.dst.UploadPartCalls.Store(0)
	f.req.ResumeUploadID = uploadID
	var resumedID string
	res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{OnUploadStarted: func(_ context.Context, id string) error {
		resumedID = id
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if resumedID != uploadID {
		t.Fatalf("resumed session %q, want %q", resumedID, uploadID)
	}
	if res.PartsReused == 0 || int64(res.PartsReused) != have {
		t.Fatalf("PartsReused = %d, stored before crash %d", res.PartsReused, have)
	}
	if got := f.dst.UploadPartCalls.Load(); got != int64(nParts-res.PartsReused) {
		t.Fatalf("re-uploaded %d parts, want %d", got, nParts-res.PartsReused)
	}
}

func TestResumeLayoutMismatch(t *testing.T) {
	f := newFixture(t, 8*64*kib)
	ctx, crash := context.WithCancel(ctxT(t))
	f.dst.UploadPartHook = func(n int) error {
		if n > 2 {
			crash()
			return context.Canceled
		}
		return nil
	}
	opts := testOpts()
	opts.PartConcurrency = 1
	var uploadID string
	_, err := Copy(ctx, f.req, opts, Hooks{OnUploadStarted: func(_ context.Context, id string) error { uploadID = id; return nil }})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	f.dst.UploadPartHook = nil
	f.req.ResumeUploadID = uploadID
	opts.PartSize = 128 * kib
	res, err := Copy(ctxT(t), f.req, opts, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if res.PartsReused != 0 || res.UploadID == uploadID || !f.dst.Aborted(uploadID) {
		t.Fatalf("PartsReused=%d uploadID=%q aborted=%v", res.PartsReused, res.UploadID, f.dst.Aborted(uploadID))
	}
}

func TestResumeSessionGone(t *testing.T) {
	f := newFixture(t, 4*64*kib)
	f.req.ResumeUploadID = "expired"
	res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if res.PartsReused != 0 {
		t.Fatal("nothing to reuse")
	}
}

func TestOnUploadStartedError(t *testing.T) {
	f := newFixture(t, 4*64*kib)
	boom := errors.New("db down")
	_, err := Copy(ctxT(t), f.req, testOpts(), Hooks{OnUploadStarted: func(context.Context, string) error { return boom }})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if f.dst.UploadPartCalls.Load() != 0 || f.dst.OpenUploads() != 0 {
		t.Fatal("parts uploaded or session leaked after OnUploadStarted failed")
	}
}

func TestSourceSHAPropagated(t *testing.T) {
	f := newFixture(t, 4*64*kib)
	sum := sha256.Sum256(f.data)
	f.req.Info.Metadata = map[string]string{connector.MetaSHA256: hex.EncodeToString(sum[:])}
	res, err := Copy(ctxT(t), f.req, testOpts(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if _, meta, _ := f.dst.Get("dst/obj"); meta[connector.MetaSHA256] != hex.EncodeToString(sum[:]) {
		t.Fatalf("metadata = %v", meta)
	}
	if res.Verified.Method != "size+sha256+sample" {
		t.Fatalf("Method = %q", res.Verified.Method)
	}

	// A source advertising the wrong hash fails before Complete.
	f2 := newFixture(t, 4*64*kib)
	f2.req.Info.Checksums.SHA256 = make([]byte, 32)
	var id string
	_, err = Copy(ctxT(t), f2.req, testOpts(), Hooks{OnUploadStarted: func(_ context.Context, u string) error { id = u; return nil }})
	if !errors.Is(err, verify.ErrMismatch) || !f2.dst.Aborted(id) {
		t.Fatalf("err = %v, aborted = %v", err, f2.dst.Aborted(id))
	}
	if _, _, ok := f2.dst.Get("dst/obj"); ok {
		t.Fatal("destination written")
	}
}

func TestHeartbeat(t *testing.T) {
	f := newFixture(t, 10*64*kib)
	f.dst.UploadPartHook = func(int) error { time.Sleep(10 * time.Millisecond); return nil }
	var beats atomic.Int64
	opts := testOpts()
	opts.HeartbeatEvery = 5 * time.Millisecond
	res, err := Copy(ctxT(t), f.req, opts, Hooks{Heartbeat: func(context.Context) error { beats.Add(1); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	f.assertCopied(t, res)
	if beats.Load() < 2 {
		t.Fatalf("heartbeats = %d", beats.Load())
	}
	after := beats.Load()
	time.Sleep(30 * time.Millisecond)
	if beats.Load() != after {
		t.Fatal("heartbeat still running after Copy returned")
	}
}

func TestHeartbeatFailureCancels(t *testing.T) {
	f := newFixture(t, 50*64*kib)
	f.dst.UploadPartHook = func(int) error { time.Sleep(5 * time.Millisecond); return nil }
	lost := errors.New("lease lost")
	opts := testOpts()
	opts.HeartbeatEvery = 10 * time.Millisecond
	var id string
	_, err := Copy(ctxT(t), f.req, opts, Hooks{
		Heartbeat:       func(context.Context) error { return lost },
		OnUploadStarted: func(_ context.Context, u string) error { id = u; return nil },
	})
	if !errors.Is(err, lost) {
		t.Fatalf("err = %v, want lease lost", err)
	}
	if f.dst.Aborted(id) || f.dst.OpenUploads() != 1 {
		t.Fatal("upload must not be aborted after a heartbeat failure")
	}
	if f.dst.UploadPartCalls.Load() >= 50 {
		t.Fatal("copy was not cancelled")
	}
}

func TestPoolBoundsConcurrentCopies(t *testing.T) {
	const part = 64 * kib
	pool := NewBufferPool(5 * part)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := newFixture(t, (i%4+2)*part+i*100)
			f.dst.UploadPartHook = func(int) error { time.Sleep(time.Millisecond); return nil }
			opts := testOpts()
			opts.Pool = pool
			opts.PartConcurrency = 4
			res, err := Copy(ctxT(t), f.req, opts, Hooks{})
			if err == nil {
				got, _, _ := f.dst.Get("dst/obj")
				if !bytes.Equal(got, f.data) || !res.Verified.OK {
					err = errors.New("bad copy")
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if pk := pool.peak.Load(); pk > pool.Cap() || pk == 0 {
		t.Fatalf("peak in-flight %d, cap %d", pk, pool.Cap())
	}
	if pool.InUse() != 0 {
		t.Fatalf("InUse = %d after all copies", pool.InUse())
	}
}

func TestPoolStress(t *testing.T) {
	pool := NewBufferPool(10 << 20)
	var cur atomic.Int64
	var bad atomic.Bool
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 1))
			for range 200 {
				n := r.Int64N(3<<20) + 1
				b, err := pool.acquire(context.Background(), n)
				if err != nil || int64(len(b)) != n {
					bad.Store(true)
					return
				}
				if cur.Add(int64(cap(b))) > pool.Cap() {
					bad.Store(true)
				}
				b[0], b[n-1] = 1, 2
				cur.Add(-int64(cap(b)))
				pool.release(b)
			}
		}()
	}
	wg.Wait()
	if bad.Load() || pool.InUse() != 0 || pool.peak.Load() > pool.Cap() {
		t.Fatalf("bad=%v inUse=%d peak=%d", bad.Load(), pool.InUse(), pool.peak.Load())
	}
}

func TestPoolTooSmallAndCancel(t *testing.T) {
	f := newFixture(t, 4*64*kib)
	opts := testOpts()
	opts.Pool = NewBufferPool(32 * kib)
	if _, err := Copy(ctxT(t), f.req, opts, Hooks{}); err == nil {
		t.Fatal("pool smaller than a part must fail")
	}
	pool := NewBufferPool(64 * kib)
	held, err := pool.acquire(context.Background(), 64*kib)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.acquire(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked acquire err = %v", err)
	}
	pool.release(held)
}

func TestRequestValidation(t *testing.T) {
	f := newFixture(t, 10)
	req := f.req
	req.Info.Version = ""
	if _, err := Copy(ctxT(t), req, testOpts(), Hooks{}); err == nil {
		t.Fatal("empty version must fail")
	}
}
