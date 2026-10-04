//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/sunny36/portage/internal/testenv"
)

func TestGenerateAzurite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := testenv.UniqueName(t, "loadgen")
	c, err := container.NewClientFromConnectionString(testenv.AzuriteConnectionString(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.Delete(context.Background(), nil) })

	mix, err := parseMix("1KB-64KB:0.8,1MiB-3MiB:0.2") // some blobs span multiple 1MiB blocks
	if err != nil {
		t.Fatal(err)
	}
	var manifest bytes.Buffer
	const n = 20
	stats, err := generate(ctx, c, genConfig{
		Prefix:      "it/",
		MaxBlobs:    n,
		Concurrency: 4,
		BlockSize:   1 << 20,
		BlockConc:   2,
		Seed:        12345,
		Mix:         mix,
		Manifest:    &manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blobs != n || stats.Errors != 0 {
		t.Fatalf("stats = %+v, want %d blobs and no errors", stats, n)
	}

	var entries []manifestEntry
	sc := bufio.NewScanner(&manifest)
	for sc.Scan() {
		var e manifestEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("manifest line %q: %v", sc.Text(), err)
		}
		entries = append(entries, e)
	}
	if len(entries) != n {
		t.Fatalf("manifest has %d entries, want %d", len(entries), n)
	}

	seen := map[string]bool{}
	var total int64
	for _, e := range entries {
		if seen[e.Key] {
			t.Errorf("duplicate key %s", e.Key)
		}
		seen[e.Key] = true
		total += e.Size
		if !strings.HasPrefix(e.Key, "it/") || e.WrittenAt.IsZero() {
			t.Errorf("bad entry %+v", e)
		}
		resp, err := c.NewBlobClient(e.Key).DownloadStream(ctx, nil)
		if err != nil {
			t.Fatalf("download %s: %v", e.Key, err)
		}
		h := sha256.New()
		got, err := io.Copy(h, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got != e.Size || hex.EncodeToString(h.Sum(nil)) != e.SHA256 {
			t.Errorf("%s: got %d bytes sha %x, manifest says %d bytes sha %s",
				e.Key, got, h.Sum(nil), e.Size, e.SHA256)
		}
	}
	if total != stats.Bytes {
		t.Errorf("manifest total %d != stats bytes %d", total, stats.Bytes)
	}

	// Listing the container must show exactly the manifest's keys.
	pager := c.NewListBlobsFlatPager(nil)
	listed := 0
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range page.Segment.BlobItems {
			listed++
			if !seen[*b.Name] {
				t.Errorf("unexpected blob %s", *b.Name)
			}
		}
	}
	if listed != n {
		t.Errorf("listed %d blobs, want %d", listed, n)
	}
}

func TestGenerateAzuriteBurstAndRate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	name := testenv.UniqueName(t, "loadgen-burst")
	c, err := container.NewClientFromConnectionString(testenv.AzuriteConnectionString(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.Delete(context.Background(), nil) })

	mix, _ := parseMix("10KB:1")
	// Burst: exactly 55KB total, last blob truncated to fit.
	stats, err := generate(ctx, c, genConfig{
		TotalBytes: 55_000, Concurrency: 2, Seed: 1, Mix: mix, Manifest: io.Discard,
	})
	if err != nil || stats.Bytes != 55_000 || stats.Blobs != 6 {
		t.Fatalf("burst: stats %+v, err %v", stats, err)
	}

	// Rate: 100KB/s with 10KB blobs for ~1s should write about 10 blobs, not hundreds.
	start := time.Now()
	stats, err = generate(ctx, c, genConfig{
		Rate: 100_000, Duration: time.Second, Concurrency: 4, Seed: 2, Mix: mix, Manifest: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blobs < 8 || stats.Blobs > 12 {
		t.Errorf("rate-limited run wrote %d blobs in %v, want ~10", stats.Blobs, time.Since(start))
	}
}
