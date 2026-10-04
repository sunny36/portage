// Package verify checks that a destination object matches the source bytes
// that were copied into it.
//
// Two checks are combined by the transfer layer:
//
//   - CheckStat: the destination's size equals the source size and, when the
//     destination reports a SHA-256 (native or portagesha256 metadata), it
//     equals the hash computed while streaming.
//   - Sampler: before streaming, a few byte ranges are chosen (the first and
//     last 64 KiB plus one random 1 MiB window, or the whole object when it is
//     at most 2 MiB). The copier feeds every byte it streams to Observe, which
//     hashes just the sampled ranges. After the write, Check reads the same
//     ranges back from the destination and compares hashes.
//
// A mismatch is reported as an error wrapping ErrMismatch.
package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"

	"github.com/sunny36/portage/internal/connector"
)

// ErrMismatch means the destination content does not match the source. The
// copy attempt has failed; retrying it from scratch may succeed.
//
// Note that connector.IsRetryable(ErrMismatch) is true (it is not one of the
// connector's permanent errors), which suits a job-level retry. Code retrying
// a single operation inside one attempt should not retry it.
var ErrMismatch = errors.New("verify: destination does not match source")

// Sample sizes.
const (
	EdgeBytes      = 64 << 10 // first and last bytes sampled
	WindowBytes    = 1 << 20  // random window
	WholeObjectMax = 2 << 20  // objects up to this size are sampled whole
)

// Report summarises the verification of one copy.
type Report struct {
	// Method lists the checks performed, e.g. "size+sha256+sample".
	Method       string
	SampledBytes int64
	OK           bool
	Detail       string
}

// Range is a byte range of the object.
type Range struct {
	Offset, Length int64
}

// SampleRanges returns the ranges sampled for an object of size bytes. The
// choice is a pure function of (size, seed).
func SampleRanges(size int64, seed uint64) []Range {
	switch {
	case size <= 0:
		return nil
	case size <= WholeObjectMax:
		return []Range{{0, size}}
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return []Range{
		{0, EdgeBytes},
		{size - EdgeBytes, EdgeBytes},
		{rng.Int64N(size - WindowBytes + 1), WindowBytes},
	}
}

type sample struct {
	Range
	h    hash.Hash
	seen int64 // bytes of the range hashed so far
}

// Sampler hashes the sampled ranges of a stream and later compares them with
// the destination. Observe must be called from one goroutine at a time.
type Sampler struct {
	size    int64
	seed    uint64
	samples []*sample
	gap     error
}

// NewSampler picks the ranges for an object of size bytes from seed.
func NewSampler(size int64, seed uint64) *Sampler {
	s := &Sampler{size: size, seed: seed}
	for _, r := range SampleRanges(size, seed) {
		s.samples = append(s.samples, &sample{Range: r, h: sha256.New()})
	}
	return s
}

// Ranges returns the sampled ranges.
func (s *Sampler) Ranges() []Range {
	out := make([]Range, len(s.samples))
	for i, sm := range s.samples {
		out[i] = sm.Range
	}
	return out
}

// Seed returns the seed the ranges were chosen from.
func (s *Sampler) Seed() uint64 { return s.seed }

// Observe feeds the bytes at [offset, offset+len(b)) of the source. Bytes
// must arrive in ascending offset order within each sampled range;
// re-observing bytes already seen is ignored, and skipping bytes of a range
// makes Check fail.
func (s *Sampler) Observe(offset int64, b []byte) {
	end := offset + int64(len(b))
	for _, sm := range s.samples {
		next := sm.Offset + sm.seen // first byte of the range not yet hashed
		rEnd := sm.Offset + sm.Length
		if next >= rEnd || end <= next || offset >= rEnd {
			continue
		}
		if offset > next {
			if s.gap == nil {
				s.gap = fmt.Errorf("verify: sampler skipped bytes [%d,%d)", next, offset)
			}
			continue
		}
		stop := min(end, rEnd)
		sm.h.Write(b[next-offset : stop-offset])
		sm.seen += stop - next
	}
}

// Check reads the sampled ranges of key at version back from dst and
// compares them with what Observe saw. A difference returns an error
// wrapping ErrMismatch; read failures are returned as the connector reported
// them.
func (s *Sampler) Check(ctx context.Context, dst connector.Connector, key, version string) (Report, error) {
	rep := Report{Method: "sample", Detail: fmt.Sprintf("seed=%d ranges=%v", s.seed, s.Ranges())}
	if s.gap != nil {
		return rep, s.gap
	}
	for _, sm := range s.samples {
		if sm.seen != sm.Length {
			return rep, fmt.Errorf("verify: sampler saw %d of %d bytes of range at %d", sm.seen, sm.Length, sm.Offset)
		}
	}
	for _, sm := range s.samples {
		got, n, err := readHash(ctx, dst, key, version, sm.Range)
		if err != nil {
			return rep, err
		}
		rep.SampledBytes += n
		want := sm.h.Sum(nil)
		if n != sm.Length || !bytes.Equal(got, want) {
			rep.Detail = fmt.Sprintf("range [%d,%d): destination read %d bytes, sha256 %x, source sha256 %x (seed=%d)",
				sm.Offset, sm.Offset+sm.Length, n, got, want, s.seed)
			return rep, fmt.Errorf("%w: %s", ErrMismatch, rep.Detail)
		}
	}
	rep.OK = true
	return rep, nil
}

func readHash(ctx context.Context, dst connector.Connector, key, version string, r Range) ([]byte, int64, error) {
	rc, err := dst.OpenRange(ctx, key, version, r.Offset, r.Length)
	if err != nil {
		return nil, 0, fmt.Errorf("verify: read back %q [%d,+%d): %w", key, r.Offset, r.Length, err)
	}
	defer rc.Close()
	h := sha256.New()
	// Read one byte past the range to notice a provider returning too much.
	n, err := io.Copy(h, io.LimitReader(rc, r.Length+1))
	if err != nil {
		return nil, n, fmt.Errorf("verify: read back %q [%d,+%d): %w", key, r.Offset, r.Length, err)
	}
	return h.Sum(nil), n, nil
}

// CheckStat stats key on dst and checks that it is version (when non-empty),
// has size bytes and, if dst reports a SHA-256, that it equals sha.
//
// A different version means the destination was overwritten after this copy
// wrote it; that returns an error wrapping connector.ErrVersionChanged.
func CheckStat(ctx context.Context, dst connector.Connector, key, version string, size int64, sha []byte) (Report, error) {
	rep := Report{Method: "size"}
	info, err := dst.Stat(ctx, key)
	if err != nil {
		return rep, fmt.Errorf("verify: stat destination %q: %w", key, err)
	}
	if version != "" && info.Version != version {
		rep.Detail = fmt.Sprintf("destination version %q, wrote %q", info.Version, version)
		return rep, fmt.Errorf("verify: destination %q changed after write (%s): %w", key, rep.Detail, connector.ErrVersionChanged)
	}
	if info.Size != size {
		rep.Detail = fmt.Sprintf("destination size %d, source size %d", info.Size, size)
		return rep, fmt.Errorf("%w: %q: %s", ErrMismatch, key, rep.Detail)
	}
	if len(info.Checksums.SHA256) > 0 {
		rep.Method = "size+sha256"
		if !bytes.Equal(info.Checksums.SHA256, sha) {
			rep.Detail = fmt.Sprintf("destination sha256 %s, source sha256 %s",
				hex.EncodeToString(info.Checksums.SHA256), hex.EncodeToString(sha))
			return rep, fmt.Errorf("%w: %q: %s", ErrMismatch, key, rep.Detail)
		}
	}
	rep.OK = true
	return rep, nil
}
