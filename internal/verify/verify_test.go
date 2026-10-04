package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/sunny36/portage/internal/connector"
)

// fakeDst serves Stat and OpenRange over one in-memory object.
type fakeDst struct {
	connector.Connector // other methods unused (nil)
	data                []byte
	version             string
	sha                 []byte
}

func (f *fakeDst) Stat(context.Context, string) (connector.ObjectInfo, error) {
	return connector.ObjectInfo{Size: int64(len(f.data)), Version: f.version, Checksums: connector.Checksums{SHA256: f.sha}}, nil
}

func (f *fakeDst) OpenRange(_ context.Context, _, version string, off, n int64) (io.ReadCloser, error) {
	if version != f.version {
		return nil, connector.ErrVersionChanged
	}
	end := min(off+n, int64(len(f.data)))
	return io.NopCloser(bytes.NewReader(f.data[off:end])), nil
}

func data(n int) []byte {
	r := rand.New(rand.NewPCG(uint64(n), 3))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func TestSampleRanges(t *testing.T) {
	if r := SampleRanges(0, 1); len(r) != 0 {
		t.Fatalf("zero size: %v", r)
	}
	if r := SampleRanges(WholeObjectMax, 1); len(r) != 1 || r[0] != (Range{0, WholeObjectMax}) {
		t.Fatalf("small: %v", r)
	}
	size := int64(100 << 20)
	a, b := SampleRanges(size, 42), SampleRanges(size, 42)
	if len(a) != 3 || a[2] != b[2] {
		t.Fatalf("not deterministic: %v %v", a, b)
	}
	if a[0] != (Range{0, EdgeBytes}) || a[1] != (Range{size - EdgeBytes, EdgeBytes}) {
		t.Fatalf("edges: %v", a)
	}
	if w := a[2]; w.Length != WindowBytes || w.Offset < 0 || w.Offset+w.Length > size {
		t.Fatalf("window out of bounds: %v", w)
	}
	seen := map[int64]bool{}
	for s := range uint64(20) {
		seen[SampleRanges(size, s)[2].Offset] = true
	}
	if len(seen) < 10 {
		t.Fatalf("window offsets barely vary with seed: %v", seen)
	}
}

// feed observes src in chunks of n, re-observing some bytes like a retried read.
func feed(s *Sampler, src []byte, n int) {
	for off := 0; off < len(src); off += n {
		end := min(off+n, len(src))
		s.Observe(int64(off), src[off:end])
		s.Observe(int64(off), src[off:end]) // duplicates are ignored
	}
}

func TestSamplerCheck(t *testing.T) {
	ctx := context.Background()
	for _, size := range []int{1, 1000, WholeObjectMax, WholeObjectMax + 1, 5<<20 + 3} {
		src := data(size)
		for _, chunk := range []int{1 << 20, 7777, size} {
			s := NewSampler(int64(size), 9)
			feed(s, src, chunk)
			dst := &fakeDst{data: bytes.Clone(src), version: "v1"}
			rep, err := s.Check(ctx, dst, "k", "v1")
			if err != nil || !rep.OK {
				t.Fatalf("size %d chunk %d: %+v %v", size, chunk, rep, err)
			}
			// Corrupt a byte inside a sampled range.
			r := s.Ranges()[len(s.Ranges())-1]
			dst.data[r.Offset+r.Length/2] ^= 1
			if _, err := s.Check(ctx, dst, "k", "v1"); !errors.Is(err, ErrMismatch) {
				t.Fatalf("size %d: corruption not detected: %v", size, err)
			}
			dst.data = dst.data[:len(dst.data)-1]
			if _, err := s.Check(ctx, dst, "k", "v1"); !errors.Is(err, ErrMismatch) {
				t.Fatalf("size %d: truncation not detected: %v", size, err)
			}
			if _, err := s.Check(ctx, dst, "k", "v2"); !errors.Is(err, connector.ErrVersionChanged) {
				t.Fatalf("size %d: version not passed through: %v", size, err)
			}
		}
	}
}

func TestSamplerZeroAndGaps(t *testing.T) {
	s := NewSampler(0, 1)
	if rep, err := s.Check(context.Background(), &fakeDst{version: "v"}, "k", "v"); err != nil || !rep.OK || rep.SampledBytes != 0 {
		t.Fatalf("zero: %+v %v", rep, err)
	}
	src := data(1000)
	s = NewSampler(1000, 1)
	s.Observe(0, src[:100])
	s.Observe(200, src[200:])
	if _, err := s.Check(context.Background(), &fakeDst{data: src, version: "v"}, "k", "v"); err == nil || errors.Is(err, ErrMismatch) {
		t.Fatalf("gap must be an internal error, got %v", err)
	}
	s = NewSampler(1000, 1)
	s.Observe(0, src[:500])
	if _, err := s.Check(context.Background(), &fakeDst{data: src, version: "v"}, "k", "v"); err == nil {
		t.Fatal("incomplete observation must fail")
	}
}

func TestCheckStat(t *testing.T) {
	ctx := context.Background()
	src := data(100)
	sum := sha256.Sum256(src)
	dst := &fakeDst{data: src, version: "v1"}
	if rep, err := CheckStat(ctx, dst, "k", "v1", 100, sum[:]); err != nil || rep.Method != "size" {
		t.Fatalf("%+v %v", rep, err)
	}
	dst.sha = sum[:]
	if rep, err := CheckStat(ctx, dst, "k", "v1", 100, sum[:]); err != nil || rep.Method != "size+sha256" || !rep.OK {
		t.Fatalf("%+v %v", rep, err)
	}
	if _, err := CheckStat(ctx, dst, "k", "v1", 100, make([]byte, 32)); !errors.Is(err, ErrMismatch) {
		t.Fatalf("sha mismatch: %v", err)
	}
	if _, err := CheckStat(ctx, dst, "k", "v1", 99, sum[:]); !errors.Is(err, ErrMismatch) {
		t.Fatalf("size mismatch: %v", err)
	}
	if _, err := CheckStat(ctx, dst, "k", "v0", 100, sum[:]); !errors.Is(err, connector.ErrVersionChanged) {
		t.Fatalf("version: %v", err)
	}
}
