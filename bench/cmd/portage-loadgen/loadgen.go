package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// genConfig controls one load-generation run.
type genConfig struct {
	Prefix      string        // key prefix inside the container
	Rate        float64       // bytes/second; 0 = unlimited
	TotalBytes  int64         // stop after scheduling this many bytes; 0 = no limit
	MaxBlobs    int64         // stop after this many blobs; 0 = no limit
	Duration    time.Duration // stop scheduling after this long; 0 = no limit
	Concurrency int           // blobs uploaded in parallel
	BlockSize   int64         // block size for staged uploads
	BlockConc   int           // concurrent block uploads per blob
	Seed        uint64        // makes sizes and content reproducible
	Mix         *sizeMix
	Manifest    io.Writer
	Progress    time.Duration // progress log interval; 0 = off
	Logger      *slog.Logger
	now         func() time.Time
}

// manifestEntry is one JSONL line of the manifest.
type manifestEntry struct {
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
	SHA256    string    `json:"sha256"`
	WrittenAt time.Time `json:"written_at"`
}

// genStats summarises a run.
type genStats struct {
	Blobs  int64
	Bytes  int64
	Errors int64
}

type blobJob struct {
	seq  int64
	key  string
	size int64
}

// contentReader returns the reproducible content of blob seq: a ChaCha8
// stream keyed by (seed, seq), so a verifier can regenerate it and two runs
// with the same seed write identical bytes.
func contentReader(seed uint64, seq int64, size int64) io.Reader {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[0:], seed)
	binary.LittleEndian.PutUint64(key[8:], uint64(seq))
	copy(key[16:], "portage-loadgen!")
	return io.LimitReader(rand.NewChaCha8(key), size)
}

// keyName builds a unique, time-ordered key: <prefix>YYYY/MM/DD/HH/<ts>-<run>-<seq>.bin
func keyName(prefix, runID string, t time.Time, seq int64) string {
	t = t.UTC()
	return fmt.Sprintf("%s%s/%s-%s-%09d.bin", prefix, t.Format("2006/01/02/15"),
		t.Format("20060102T150405.000Z"), runID, seq)
}

// generate writes blobs into c until a stop condition (or ctx) ends the run.
func generate(ctx context.Context, c *container.Client, cfg genConfig) (genStats, error) {
	if cfg.Mix == nil {
		return genStats{}, errors.New("generate: size mix is required")
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	runID := fmt.Sprintf("%016x", cfg.Seed)[8:]

	var (
		stats   genStats
		mu      sync.Mutex // guards manifest
		bw      = bufio.NewWriter(cfg.Manifest)
		enc     = json.NewEncoder(bw)
		jobs    = make(chan blobJob)
		wg      sync.WaitGroup
		firstMu sync.Mutex
		first   error
	)
	record := func(e manifestEntry) error {
		mu.Lock()
		defer mu.Unlock()
		if err := enc.Encode(e); err != nil {
			return err
		}
		return bw.Flush() // keep the manifest valid if the run is killed
	}

	for range cfg.Concurrency {
		wg.Go(func() {
			for j := range jobs {
				e, err := uploadOne(ctx, c, cfg, j)
				if err == nil {
					err = record(e)
				}
				if err != nil {
					atomic.AddInt64(&stats.Errors, 1)
					cfg.Logger.Error("upload failed", "key", j.key, "size", j.size, "err", err)
					firstMu.Lock()
					if first == nil {
						first = err
					}
					firstMu.Unlock()
					continue
				}
				atomic.AddInt64(&stats.Blobs, 1)
				atomic.AddInt64(&stats.Bytes, j.size)
			}
		})
	}

	start := cfg.now()
	stopProgress := startProgress(ctx, cfg, &stats, start)
	defer stopProgress()

	sizes := rand.New(rand.NewPCG(cfg.Seed, 0x9e3779b97f4a7c15))
	var scheduled int64
dispatch:
	for seq := int64(0); ; seq++ {
		if cfg.MaxBlobs > 0 && seq >= cfg.MaxBlobs {
			break
		}
		if cfg.TotalBytes > 0 && scheduled >= cfg.TotalBytes {
			break
		}
		if cfg.Duration > 0 && cfg.now().Sub(start) >= cfg.Duration {
			break
		}
		size := cfg.Mix.Sample(sizes)
		if cfg.TotalBytes > 0 {
			size = min(size, cfg.TotalBytes-scheduled)
		}
		// Pace by bytes: blob n starts no earlier than start + bytes(0..n-1)/rate,
		// so the long-run average matches the target regardless of size mix.
		if cfg.Rate > 0 {
			at := start.Add(time.Duration(float64(scheduled) / cfg.Rate * float64(time.Second)))
			if d := at.Sub(cfg.now()); d > 0 {
				if cfg.Duration > 0 && at.Sub(start) >= cfg.Duration {
					break
				}
				t := time.NewTimer(d)
				select {
				case <-ctx.Done():
					t.Stop()
					break dispatch
				case <-t.C:
				}
			}
		}
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- blobJob{seq: seq, key: keyName(cfg.Prefix, runID, cfg.now(), seq), size: size}:
			scheduled += size
		}
	}
	close(jobs)
	wg.Wait()

	if err := ctx.Err(); err != nil && first == nil {
		first = err
	}
	if first != nil {
		return stats, fmt.Errorf("%d upload(s) failed, first: %w", stats.Errors, first)
	}
	return stats, nil
}

// uploadOne streams one generated blob, hashing it on the way out. Memory use
// is bounded by BlockSize*BlockConc regardless of blob size.
func uploadOne(ctx context.Context, c *container.Client, cfg genConfig, j blobJob) (manifestEntry, error) {
	h := sha256.New()
	cr := &countingReader{r: io.TeeReader(contentReader(cfg.Seed, j.seq, j.size), h)}
	bb := c.NewBlockBlobClient(j.key)
	_, err := bb.UploadStream(ctx, cr, &blockblob.UploadStreamOptions{
		BlockSize:   cfg.BlockSize,
		Concurrency: cfg.BlockConc,
	})
	if err != nil {
		return manifestEntry{}, err
	}
	if cr.n != j.size {
		return manifestEntry{}, fmt.Errorf("short upload: wrote %d of %d bytes", cr.n, j.size)
	}
	return manifestEntry{
		Key:       j.key,
		Size:      j.size,
		SHA256:    hex.EncodeToString(h.Sum(nil)),
		WrittenAt: cfg.now().UTC(),
	}, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func startProgress(ctx context.Context, cfg genConfig, s *genStats, start time.Time) func() {
	if cfg.Progress <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	t := time.NewTicker(cfg.Progress)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				b := atomic.LoadInt64(&s.Bytes)
				el := cfg.now().Sub(start).Seconds()
				cfg.Logger.Info("progress",
					"blobs", atomic.LoadInt64(&s.Blobs),
					"bytes", formatBytes(float64(b)),
					"rate_per_day", formatBytes(float64(b)/el*86400),
					"errors", atomic.LoadInt64(&s.Errors))
			}
		}
	}()
	return func() { close(done) }
}

// openManifest opens path for appending (or stdout for "-").
func openManifest(path string) (io.WriteCloser, error) {
	if path == "-" {
		return nopCloser{os.Stdout}, nil
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }
