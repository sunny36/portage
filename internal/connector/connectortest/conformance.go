// Package connectortest is the behavioural contract every connector must
// pass. Each connector's integration test calls Run with a factory that
// returns a Connector scoped to a fresh, empty container/bucket.
package connectortest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/connector"
)

// Factory returns a connector scoped to a new, empty location. It should
// register cleanup with t.Cleanup.
type Factory func(t *testing.T) connector.Connector

// Run executes the conformance suite.
func Run(t *testing.T, newConn Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, connector.Connector)
	}{
		{"StatMissing", testStatMissing},
		{"PutStatRead", testPutStatRead},
		{"ZeroByte", testZeroByte},
		{"OpenRangeStaleVersion", testStaleVersion},
		{"ListPaginationAndPrefix", testList},
		{"MultipartParallel", testMultipartParallel},
		{"MultipartResume", testMultipartResume},
		{"MultipartAbort", testMultipartAbort},
		{"Delete", testDelete},
		{"Limits", testLimits},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newConn(t))
		})
	}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 42))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func sumHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func meta(b []byte) connector.WriteOptions {
	return connector.WriteOptions{
		ContentType: "application/octet-stream",
		Metadata:    map[string]string{connector.MetaSHA256: sumHex(b)},
	}
}

func put(t *testing.T, c connector.Connector, key string, b []byte) connector.WriteResult {
	t.Helper()
	res, err := c.PutObject(ctx(t), key, bytes.NewReader(b), int64(len(b)), meta(b))
	if err != nil {
		t.Fatalf("PutObject %q: %v", key, err)
	}
	if res.Version == "" {
		t.Fatalf("PutObject %q: empty version", key)
	}
	return res
}

func readAll(t *testing.T, c connector.Connector, key, version string, off, n int64) []byte {
	t.Helper()
	rc, err := c.OpenRange(ctx(t), key, version, off, n)
	if err != nil {
		t.Fatalf("OpenRange %q [%d,+%d): %v", key, off, n, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return b
}

func testStatMissing(t *testing.T, c connector.Connector) {
	_, err := c.Stat(ctx(t), "does/not/exist")
	if !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("Stat missing: got %v, want ErrNotFound", err)
	}
	_, err = c.OpenRange(ctx(t), "does/not/exist", "", 0, -1)
	if !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("OpenRange missing: got %v, want ErrNotFound", err)
	}
}

func testPutStatRead(t *testing.T, c connector.Connector) {
	data := randBytes(300_000)
	key := "dir/sub dir/ファイル-ü.bin" // spaces + unicode
	res := put(t, c, key, data)

	info, err := c.Stat(ctx(t), key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Key != key || info.Size != int64(len(data)) || info.Version != res.Version {
		t.Fatalf("Stat = %+v; want key=%q size=%d version=%q", info, key, len(data), res.Version)
	}
	if info.ModTime.IsZero() {
		t.Errorf("Stat: zero ModTime")
	}
	if info.ContentType != "application/octet-stream" {
		t.Errorf("Stat: ContentType = %q, want the one written", info.ContentType)
	}
	if got := info.Metadata[connector.MetaSHA256]; got != sumHex(data) {
		t.Errorf("metadata %s = %q, want %q", connector.MetaSHA256, got, sumHex(data))
	}
	if got := readAll(t, c, key, info.Version, 0, -1); !bytes.Equal(got, data) {
		t.Fatalf("full read mismatch (len %d vs %d)", len(got), len(data))
	}
	if got := readAll(t, c, key, "", 1000, 5000); !bytes.Equal(got, data[1000:6000]) {
		t.Fatalf("range read mismatch")
	}
	if got := readAll(t, c, key, "", 299_990, -1); !bytes.Equal(got, data[299_990:]) {
		t.Fatalf("tail read mismatch")
	}
}

func testZeroByte(t *testing.T, c connector.Connector) {
	put(t, c, "empty", nil)
	info, err := c.Stat(ctx(t), "empty")
	if err != nil || info.Size != 0 {
		t.Fatalf("Stat empty = %+v, %v", info, err)
	}
	if got := readAll(t, c, "empty", "", 0, -1); len(got) != 0 {
		t.Fatalf("read empty: %d bytes", len(got))
	}
}

func testStaleVersion(t *testing.T, c connector.Connector) {
	v1 := put(t, c, "k", []byte("version one")).Version
	v2 := put(t, c, "k", []byte("version two!")).Version
	if v1 == v2 {
		t.Fatalf("overwrite kept version %q", v1)
	}
	_, err := c.OpenRange(ctx(t), "k", v1, 0, -1)
	if !errors.Is(err, connector.ErrVersionChanged) {
		t.Fatalf("OpenRange stale version: got %v, want ErrVersionChanged", err)
	}
	if got := readAll(t, c, "k", v2, 0, -1); string(got) != "version two!" {
		t.Fatalf("read v2 = %q", got)
	}
}

func testList(t *testing.T, c connector.Connector) {
	want := []string{"a/1", "a/2", "a/3", "a/sub/4", "a/ü5"}
	for _, k := range append([]string{"b/other", "a0"}, want...) {
		put(t, c, k, []byte(k))
	}
	var got []string
	cursor, pages := "", 0
	for {
		page, err := c.List(ctx(t), "a/", cursor, 2)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		pages++
		if len(page.Objects) > 2 {
			t.Fatalf("page has %d objects, limit 2", len(page.Objects))
		}
		for _, o := range page.Objects {
			if o.Version == "" || o.Size != int64(len(o.Key)) {
				t.Errorf("list entry %+v missing version/size", o)
			}
			got = append(got, o.Key)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if pages < 3 {
		t.Errorf("expected >=3 pages with limit 2, got %d", pages)
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("listing not sorted: %v", got)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("List(a/) = %v, want %v (recursive, no b/ or a0)", got, want)
	}
}

func partSize(c connector.Connector) int {
	ps := c.Limits().MinPartSize
	if ps < 5<<20 {
		ps = 5 << 20
	}
	return int(ps)
}

func testMultipartParallel(t *testing.T, c connector.Connector) {
	ps := partSize(c)
	data := randBytes(2*ps + 12345) // 3 parts, short last part
	up, err := c.BeginUpload(ctx(t), "big/file.bin", meta(data))
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if up.ID() == "" {
		t.Fatal("empty upload ID")
	}
	chunks := [][]byte{data[:ps], data[ps : 2*ps], data[2*ps:]}
	parts := make([]connector.Part, len(chunks))
	var wg sync.WaitGroup
	errs := make(chan error, len(chunks)+1)
	for i := len(chunks) - 1; i >= 0; i-- { // out of order, concurrently
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p, err := up.UploadPart(ctx(t), i+1, chunks[i])
			if err != nil {
				errs <- fmt.Errorf("part %d: %w", i+1, err)
				return
			}
			parts[i] = p
		}(i)
	}
	wg.Wait()
	// Retrying a part (same number, same data) must be harmless.
	p2, err := up.UploadPart(ctx(t), 2, chunks[1])
	if err != nil {
		errs <- fmt.Errorf("retry part 2: %w", err)
	}
	parts[1] = p2
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	res, err := up.Complete(ctx(t), parts)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	info, err := c.Stat(ctx(t), "big/file.bin")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(data)) || info.Version != res.Version {
		t.Fatalf("Stat = size %d version %q; want %d %q", info.Size, info.Version, len(data), res.Version)
	}
	if got := info.Metadata[connector.MetaSHA256]; got != sumHex(data) {
		t.Errorf("multipart metadata %s = %q, want %q", connector.MetaSHA256, got, sumHex(data))
	}
	if got := readAll(t, c, "big/file.bin", "", 0, -1); sumHex(got) != sumHex(data) {
		t.Fatal("multipart content mismatch")
	}
}

func testMultipartResume(t *testing.T, c connector.Connector) {
	ps := partSize(c)
	data := randBytes(ps + 777)
	opts := meta(data)
	up, err := c.BeginUpload(ctx(t), "resume.bin", opts)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if _, err := up.UploadPart(ctx(t), 1, data[:ps]); err != nil {
		t.Fatalf("part 1: %v", err)
	}
	id := up.ID()

	// Simulate a crash: a new process resumes from the persisted ID.
	up2, err := c.ResumeUpload(ctx(t), "resume.bin", id, opts)
	if err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}
	done, err := up2.ListParts(ctx(t))
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(done) != 1 || done[0].Number != 1 || done[0].Size != int64(ps) {
		t.Fatalf("ListParts after resume = %+v; want part 1 of size %d", done, ps)
	}
	p2, err := up2.UploadPart(ctx(t), 2, data[ps:])
	if err != nil {
		t.Fatalf("part 2: %v", err)
	}
	if _, err := up2.Complete(ctx(t), []connector.Part{done[0], p2}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if got := readAll(t, c, "resume.bin", "", 0, -1); sumHex(got) != sumHex(data) {
		t.Fatal("resumed content mismatch")
	}
}

func testMultipartAbort(t *testing.T, c connector.Connector) {
	up, err := c.BeginUpload(ctx(t), "aborted.bin", connector.WriteOptions{})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if _, err := up.UploadPart(ctx(t), 1, randBytes(partSize(c))); err != nil {
		t.Fatalf("part 1: %v", err)
	}
	if err := up.Abort(ctx(t)); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := c.Stat(ctx(t), "aborted.bin"); !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("aborted upload is visible: %v", err)
	}
}

func testDelete(t *testing.T, c connector.Connector) {
	put(t, c, "gone", []byte("x"))
	if err := c.Delete(ctx(t), "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Stat(ctx(t), "gone"); !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("Stat after delete: %v", err)
	}
	if err := c.Delete(ctx(t), "never-existed"); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
}

func testLimits(t *testing.T, c connector.Connector) {
	l := c.Limits()
	if l.MinPartSize <= 0 || l.MaxPartSize < l.MinPartSize || l.MaxParts <= 0 || l.SinglePutMax <= 0 {
		t.Fatalf("implausible limits %+v", l)
	}
	if c.Name() == "" {
		t.Fatal("empty Name")
	}
}
