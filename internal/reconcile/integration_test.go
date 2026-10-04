//go:build integration

package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/record/pgtest"
	"github.com/sunny36/portage/internal/testenv"
)

func azureConn(t *testing.T, containerName, prefix string) *azure.Connector {
	t.Helper()
	c, err := azure.New(context.Background(), config.AzureConfig{
		Container: containerName, Auth: "connection_string", ConnectionString: testenv.AzuriteConnectionString(),
	}, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newContainer(t *testing.T) string {
	t.Helper()
	name := testenv.UniqueName(t, "portage-rec")
	cc, err := container.NewClientFromConnectionString(testenv.AzuriteConnectionString(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Create(context.Background(), nil); err != nil {
		t.Fatalf("create container: %v", err)
	}
	t.Cleanup(func() { _, _ = cc.Delete(context.Background(), nil) })
	return name
}

func put(t *testing.T, c connector.Connector, key, body string) {
	t.Helper()
	if _, err := c.PutObject(context.Background(), key, bytes.NewReader([]byte(body)), int64(len(body)), connector.WriteOptions{}); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

// syncAll plays the copy worker: claim and complete every upsert, mark every
// delete.
func syncAll(t *testing.T, store record.Store, src connector.Connector, changes []change.ObjectChanged) {
	t.Helper()
	ctx := context.Background()
	for _, c := range changes {
		if c.Kind == change.KindDelete {
			if err := store.MarkDeleted(ctx, c.PipelineID, c.Key, c.EventTime); err != nil {
				t.Fatal(err)
			}
			continue
		}
		info, err := src.Stat(ctx, c.Key)
		if err != nil {
			t.Fatal(err)
		}
		cand := record.Candidate{PipelineID: c.PipelineID, Key: c.Key, Version: info.Version, Size: info.Size, MTime: info.ModTime, EventTime: c.EventTime}
		d, _, err := store.Claim(ctx, cand, time.Minute)
		if err != nil || d != record.DecisionCopy {
			t.Fatalf("Claim %q: %v %v", c.Key, d, err)
		}
		if err := store.Complete(ctx, c.PipelineID, c.Key, info.Version, record.SyncResult{DestVersion: "d"}); err != nil {
			t.Fatal(err)
		}
	}
}

func reconcileOnce(t *testing.T, store record.Store, src connector.Connector) (Stats, []change.ObjectChanged, error) {
	t.Helper()
	out := &sink{}
	st, err := Run(context.Background(), Options{
		PipelineID: "p1", Source: src, Store: store, Emit: out.emit, PageSize: 3,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return st, out.got, err
}

// Keys where byte order differs from locale collation, and where UTF-8 byte
// order differs from UTF-16 code-unit order ("～" vs the emoji).
var integrationKeys = []string{
	"B", "a", "Z/z", "a b", "a-b", "a/b", "a.b", "a_b", "aB", "ab", "Ab",
	"é", "é", "日本/語", "日本語", "～", "😀", "~", "0",
}

func TestIntegrationAzurePostgresOrdering(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.NewPool(t)
	if err := record.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := record.NewPGStore(pool)

	name := newContainer(t)
	root := azureConn(t, name, "")
	src := azureConn(t, name, "scope/")
	put(t, root, "outside", "x") // not in the pipeline's scope
	for _, k := range integrationKeys {
		put(t, src, k, "v1:"+k)
	}

	// Record how the emulator orders keys. Azurite uses UTF-16 code-unit
	// order, which puts "😀" before "\uff5e" (byte order is the reverse);
	// the join must cope either way.
	var listed []string
	for cursor := ""; ; {
		page, err := src.List(ctx, "", cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Objects {
			listed = append(listed, o.Key)
		}
		if cursor = page.NextCursor; cursor == "" {
			break
		}
	}
	if len(listed) != len(integrationKeys) {
		t.Fatalf("listed %q", listed)
	}
	if !sort.StringsAreSorted(listed) {
		t.Logf("source lists in non-byte order: %q", listed)
	}

	// Initial copy.
	st, changes, err := reconcileOnce(t, store, src)
	if err != nil {
		t.Fatal(err)
	}
	if st.Emitted != int64(len(integrationKeys)) || st.Deleted != 0 {
		t.Fatalf("initial: %+v", st)
	}
	syncAll(t, store, src, changes)

	// Everything synced: the join must line up exactly — no false deletes or
	// spurious upserts despite mixed case and multi-byte keys.
	st, changes, err = reconcileOnce(t, store, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || st.AlreadySynced != int64(len(integrationKeys)) {
		t.Fatalf("steady state: %+v, changes %+v", st, changes)
	}

	// One overwrite, one delete.
	put(t, src, "日本語", "v2")
	if err := src.Delete(ctx, "a b"); err != nil {
		t.Fatal(err)
	}
	st, changes, err = reconcileOnce(t, store, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || st.Emitted != 1 || st.Deleted != 1 {
		t.Fatalf("after changes: %+v, %+v", st, changes)
	}
	for _, c := range changes {
		switch {
		case c.Kind == change.KindUpsert && c.Key == "日本語":
		case c.Kind == change.KindDelete && c.Key == "a b":
		default:
			t.Errorf("unexpected change %+v", c)
		}
	}
	syncAll(t, store, src, changes)
	if _, changes, err = reconcileOnce(t, store, src); err != nil || len(changes) != 0 {
		t.Fatalf("after re-sync: %v %+v", err, changes)
	}

	// A wrong prefix lists nothing: refuse to delete everything.
	wrong := azureConn(t, name, "scope-typo/")
	if _, changes, err = reconcileOnce(t, store, wrong); !errors.Is(err, ErrEmptySource) || len(changes) != 0 {
		t.Fatalf("empty source: err=%v changes=%+v", err, changes)
	}
}
