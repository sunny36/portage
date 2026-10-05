package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/connector"
)

// destination is the part of connector.Connector the verifier reads.
type destination interface {
	Stat(ctx context.Context, key string) (connector.ObjectInfo, error)
	OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error)
}

// Problems a manifest entry can have at the destination.
const (
	problemMissing      = "missing"
	problemSizeMismatch = "size_mismatch"
	problemSHAMismatch  = "sha256_mismatch"
	problemError        = "error" // could not be checked (after retries)
)

// target is a manifest entry mapped onto the destination.
type target struct {
	Entry   manifestEntry
	DestKey string // relative to the destination connector's prefix
}

// mapping is how manifest keys were mapped through the pipeline.
type mapping struct {
	Targets    []target
	OutOfScope int64 // keys outside the pipeline's source prefix
	Excluded   int64 // keys the pipeline's filters exclude
}

// mapToDestination maps source-container keys to destination keys: strip the
// pipeline's source prefix (the destination connector adds its own), and
// drop keys the pipeline would never sync.
func mapToDestination(entries []manifestEntry, srcPrefix string, filter *change.Filter) mapping {
	var m mapping
	for _, e := range entries {
		if !strings.HasPrefix(e.Key, srcPrefix) || len(e.Key) == len(srcPrefix) {
			m.OutOfScope++
			continue
		}
		rel := e.Key[len(srcPrefix):]
		if filter != nil && !filter.Match(rel) {
			m.Excluded++
			continue
		}
		m.Targets = append(m.Targets, target{Entry: e, DestKey: rel})
	}
	return m
}

// failure is one entry that did not verify.
type failure struct {
	Key            string `json:"key"`
	DestKey        string `json:"dest_key"`
	Problem        string `json:"problem"`
	ExpectedSize   int64  `json:"expected_size"`
	ActualSize     int64  `json:"actual_size,omitempty"`
	ExpectedSHA256 string `json:"expected_sha256"`
	ActualSHA256   string `json:"actual_sha256,omitempty"`
	Error          string `json:"error,omitempty"`
}

// checkOptions configure check.
type checkOptions struct {
	Concurrency int
	// SizeOnly skips hashing content (existence and size only).
	SizeOnly bool
	// RetryMissing keeps re-checking missing / wrong-size entries for this
	// long after the first pass (the engine may still be catching up).
	RetryMissing time.Duration
	RetryEvery   time.Duration
	Progress     time.Duration // 0 = no progress output
	Out          io.Writer     // progress lines
	// DestPrefix is prepended to DestKey in failures for readability.
	DestPrefix string
	// Attempts per entry for transient errors (default 3).
	Attempts int
	// MinRate (bytes/s) sizes the per-attempt timeout: 2m + size/MinRate.
	// Default 1 MB/s.
	MinRate float64
}

// attemptTimeout bounds one check of an object of size bytes.
func attemptTimeout(size int64, minRate float64) time.Duration {
	if minRate <= 0 {
		minRate = 1e6
	}
	return 2*time.Minute + time.Duration(float64(size)/minRate*float64(time.Second))
}

// checkResult summarises a verification.
type checkResult struct {
	Files        int64     `json:"files"`
	OK           int64     `json:"ok"`
	Missing      int64     `json:"missing"`
	SizeMismatch int64     `json:"size_mismatch"`
	SHAMismatch  int64     `json:"sha256_mismatch"`
	Errors       int64     `json:"errors"`
	Bytes        int64     `json:"bytes"`
	BytesHashed  int64     `json:"bytes_hashed"`
	Hashed       bool      `json:"content_hashed"`
	Duration     float64   `json:"duration_seconds"`
	Failures     []failure `json:"failures"`
	// DestLag is destination LastModified minus manifest written_at over
	// verified files: exact percentiles, but S3 LastModified has 1s
	// resolution and, for multipart uploads, is when the upload started.
	DestLag *exactQuantiles `json:"dest_lag_seconds,omitempty"`
}

// Failed reports whether anything did not verify.
func (r *checkResult) Failed() bool { return r.OK != r.Files }

type exactQuantiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func exact(vals []float64) *exactQuantiles {
	if len(vals) == 0 {
		return nil
	}
	sort.Float64s(vals)
	at := func(q float64) float64 {
		// nearest rank
		i := int(math.Ceil(q*float64(len(vals)))) - 1
		return vals[max(0, min(i, len(vals)-1))]
	}
	return &exactQuantiles{Count: len(vals), P50: at(0.5), P95: at(0.95), P99: at(0.99), Max: vals[len(vals)-1]}
}

type outcome struct {
	fail    *failure
	hashed  int64
	destLag float64 // seconds; NaN when unknown
}

// check verifies every target against dst.
func check(ctx context.Context, dst destination, targets []target, opts checkOptions) *checkResult {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.Attempts < 1 {
		opts.Attempts = 3
	}
	if opts.RetryEvery <= 0 {
		opts.RetryEvery = 15 * time.Second
	}
	start := time.Now()
	res := &checkResult{Files: int64(len(targets)), Hashed: !opts.SizeOnly}
	for _, t := range targets {
		res.Bytes += t.Entry.Size
	}

	var done, hashed atomic.Int64
	var bad atomic.Int64
	stopProgress := progress(opts, res, &done, &hashed, &bad, start)
	outs := runChecks(ctx, dst, targets, opts, &done, &hashed, &bad)

	// Re-check what may simply not have arrived yet.
	if opts.RetryMissing > 0 {
		deadline := time.Now().Add(opts.RetryMissing)
		for {
			var again []int
			for i, o := range outs {
				if o.fail != nil && (o.fail.Problem == problemMissing || o.fail.Problem == problemSizeMismatch) {
					again = append(again, i)
				}
			}
			if len(again) == 0 || !time.Now().Before(deadline) || ctx.Err() != nil {
				break
			}
			if opts.Out != nil {
				fmt.Fprintf(opts.Out, "verify: %d not (fully) at the destination yet; re-checking in %s (until %s)\n",
					len(again), opts.RetryEvery, deadline.Format(time.TimeOnly))
			}
			select {
			case <-ctx.Done():
			case <-time.After(opts.RetryEvery):
			}
			sub := make([]target, len(again))
			for j, i := range again {
				sub[j] = targets[i]
			}
			var subDone atomic.Int64
			for j, o := range runChecks(ctx, dst, sub, opts, &subDone, &hashed, new(atomic.Int64)) {
				outs[again[j]] = o
			}
		}
	}
	stopProgress()

	var lags []float64
	for _, o := range outs {
		res.BytesHashed += o.hashed
		if o.fail == nil {
			res.OK++
			if !math.IsNaN(o.destLag) {
				lags = append(lags, o.destLag)
			}
			continue
		}
		switch o.fail.Problem {
		case problemMissing:
			res.Missing++
		case problemSizeMismatch:
			res.SizeMismatch++
		case problemSHAMismatch:
			res.SHAMismatch++
		default:
			res.Errors++
		}
		f := *o.fail
		f.DestKey = opts.DestPrefix + f.DestKey
		res.Failures = append(res.Failures, f)
	}
	res.DestLag = exact(lags)
	res.Duration = time.Since(start).Seconds()
	return res
}

func runChecks(ctx context.Context, dst destination, targets []target, opts checkOptions, done, hashed, bad *atomic.Int64) []outcome {
	outs := make([]outcome, len(targets))
	next := make(chan int)
	var wg sync.WaitGroup
	for range min(opts.Concurrency, max(1, len(targets))) {
		wg.Go(func() {
			for i := range next {
				o := checkWithRetry(ctx, dst, targets[i], opts, hashed)
				if o.fail != nil {
					bad.Add(1)
				}
				outs[i] = o
				done.Add(1)
			}
		})
	}
	for i := range targets {
		next <- i
	}
	close(next)
	wg.Wait()
	return outs
}

func checkWithRetry(ctx context.Context, dst destination, t target, opts checkOptions, hashed *atomic.Int64) outcome {
	var o outcome
	for attempt := 1; ; attempt++ {
		// Bound each attempt so a stalled connection (no error, no bytes)
		// becomes a retry instead of hanging the whole run.
		actx, cancel := context.WithTimeout(ctx, attemptTimeout(t.Entry.Size, opts.MinRate))
		o = checkOne(actx, dst, t, opts.SizeOnly, hashed)
		cancel()
		if o.fail == nil || o.fail.Problem != problemError || attempt >= opts.Attempts || ctx.Err() != nil {
			return o
		}
		select {
		case <-ctx.Done():
			return o
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
}

// checkOne stats the destination object, compares size, then streams it
// through SHA-256.
func checkOne(ctx context.Context, dst destination, t target, sizeOnly bool, hashed *atomic.Int64) outcome {
	e := t.Entry
	fail := func(problem string) *failure {
		return &failure{Key: e.Key, DestKey: t.DestKey, Problem: problem, ExpectedSize: e.Size, ExpectedSHA256: e.SHA256}
	}
	o := outcome{destLag: math.NaN()}
	info, err := dst.Stat(ctx, t.DestKey)
	if errors.Is(err, connector.ErrNotFound) {
		o.fail = fail(problemMissing)
		return o
	}
	if err != nil {
		o.fail = fail(problemError)
		o.fail.Error = err.Error()
		return o
	}
	if info.Size != e.Size {
		o.fail = fail(problemSizeMismatch)
		o.fail.ActualSize = info.Size
		return o
	}
	if !info.ModTime.IsZero() && !e.WrittenAt.IsZero() {
		o.destLag = info.ModTime.Sub(e.WrittenAt).Seconds()
	}
	if sizeOnly {
		// Still use the hash Portage stored, when the object carries one.
		if got := info.Metadata[connector.MetaSHA256]; got != "" && !strings.EqualFold(got, e.SHA256) {
			o.fail = fail(problemSHAMismatch)
			o.fail.ActualSize = info.Size
			o.fail.ActualSHA256 = got
			o.fail.Error = "portagesha256 metadata differs"
		}
		return o
	}
	// Read the exact version we just sized, so a concurrent overwrite shows
	// up as an error rather than a confusing mismatch.
	rc, err := dst.OpenRange(ctx, t.DestKey, info.Version, 0, -1)
	if err != nil {
		o.fail = fail(problemError)
		o.fail.Error = err.Error()
		if errors.Is(err, connector.ErrNotFound) {
			o.fail.Problem = problemMissing
		}
		return o
	}
	h := sha256.New()
	n, err := io.Copy(h, &countingReader{r: rc, n: hashed})
	_ = rc.Close()
	o.hashed = n
	if err != nil {
		o.fail = fail(problemError)
		o.fail.Error = fmt.Sprintf("read after %d bytes: %v", n, err)
		return o
	}
	sum := hex.EncodeToString(h.Sum(nil))
	switch {
	case n != e.Size:
		// Stat already matched the size, so a short body is a transport
		// problem (truncated response), not proof of a bad object: retry.
		o.fail = fail(problemError)
		o.fail.ActualSize = n
		o.fail.Error = fmt.Sprintf("short read: got %d of %d bytes", n, e.Size)
	case !strings.EqualFold(sum, e.SHA256):
		o.fail = fail(problemSHAMismatch)
		o.fail.ActualSize = n
		o.fail.ActualSHA256 = sum
	}
	return o
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func progress(opts checkOptions, res *checkResult, done, hashed, bad *atomic.Int64, start time.Time) (stop func()) {
	if opts.Progress <= 0 || opts.Out == nil {
		return func() {}
	}
	quit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(opts.Progress)
		defer t.Stop()
		for {
			select {
			case <-quit:
				return
			case <-t.C:
				d, h := done.Load(), hashed.Load()
				el := time.Since(start).Seconds()
				line := fmt.Sprintf("verify: %d/%d files (%.1f%%)", d, res.Files, pct(d, res.Files))
				if !opts.SizeOnly {
					line += fmt.Sprintf(", %s/%s hashed at %s/s", human(float64(h)), human(float64(res.Bytes)), human(float64(h)/el))
					if h > 0 && h < res.Bytes {
						eta := time.Duration(float64(res.Bytes-h) / (float64(h) / el) * float64(time.Second))
						line += ", ETA " + eta.Round(time.Second).String()
					}
				}
				line += fmt.Sprintf(", %d problem(s) so far", bad.Load())
				fmt.Fprintln(opts.Out, line)
			}
		}
	})
	return func() { close(quit); wg.Wait() }
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 100
	}
	return 100 * float64(a) / float64(b)
}

// human formats a byte count in decimal units.
func human(n float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	for n >= 1000 && i < len(units)-1 {
		n /= 1000
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f B", n)
	}
	return fmt.Sprintf("%.2f %s", n, units[i])
}
