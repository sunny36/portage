//go:build integration

package azure

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/connectortest"
	"github.com/sunny36/portage/internal/testenv"
)

// newContainer creates a fresh Azurite container, deleted on cleanup.
func newContainer(t *testing.T) string {
	t.Helper()
	name := testenv.UniqueName(t, "portage-az")
	cc, err := container.NewClientFromConnectionString(testenv.AzuriteConnectionString(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Create(context.Background(), nil); err != nil {
		t.Fatalf("create container %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = cc.Delete(context.Background(), nil) })
	return name
}

func newConn(t *testing.T, containerName, prefix, auth string) *Connector {
	t.Helper()
	cfg := config.AzureConfig{Container: containerName, Auth: auth}
	switch auth {
	case "connection_string":
		cfg.ConnectionString = testenv.AzuriteConnectionString()
	case "shared_key":
		cfg.AccountURL = testenv.AzuriteBlobURL()
		cfg.AccountName = testenv.AzuriteAccountName
		cfg.AccountKey = testenv.AzuriteAccountKey
	}
	c, err := New(context.Background(), cfg, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func factory(prefix, auth string) connectortest.Factory {
	return func(t *testing.T) connector.Connector {
		return newConn(t, newContainer(t), prefix, auth)
	}
}

func TestConformance(t *testing.T) {
	connectortest.Run(t, factory("", "connection_string"))
}

func TestConformancePrefix(t *testing.T) {
	connectortest.Run(t, factory("scope/", "shared_key"))
}

// TestPrefixIsolation checks that objects outside the scope are invisible and
// that scoped writes land under the prefix.
func TestPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	name := newContainer(t)
	root := newConn(t, name, "", "connection_string")
	scoped := newConn(t, name, "scope/", "connection_string")
	for _, k := range []string{"outside", "scopeX/y", "scope/inside"} {
		if _, err := root.PutObject(ctx, k, nil, 0, connector.WriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := scoped.List(ctx, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "inside" {
		t.Fatalf("scoped List = %+v", page.Objects)
	}
	if _, err := scoped.Stat(ctx, "inside"); err != nil {
		t.Fatal(err)
	}
	if _, err := scoped.PutObject(ctx, "new", nil, 0, connector.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Stat(ctx, "scope/new"); err != nil {
		t.Fatalf("scoped write not under prefix: %v", err)
	}
}

// TestResumeCompleted checks that a committed or malformed session cannot be
// resumed.
func TestResumeCompleted(t *testing.T) {
	ctx := context.Background()
	c := newConn(t, newContainer(t), "", "connection_string")
	up, err := c.BeginUpload(ctx, "k", connector.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResumeUpload(ctx, "k", up.ID(), connector.WriteOptions{}); err != nil {
		t.Fatalf("resume before any part: %v", err)
	}
	p, err := up.UploadPart(ctx, 1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResumeUpload(ctx, "k", up.ID(), connector.WriteOptions{}); err != nil {
		t.Fatalf("resume before complete: %v", err)
	}
	if _, err := up.Complete(ctx, []connector.Part{p}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ResumeUpload(ctx, "k", up.ID(), connector.WriteOptions{}); !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("resume after complete: %v, want ErrNotFound", err)
	}
	if _, err := c.ResumeUpload(ctx, "k", "garbage", connector.WriteOptions{}); !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("resume malformed id: %v, want ErrNotFound", err)
	}
}

// TestConditionalPut checks IfNoneMatch "*" and IfMatch map to
// ErrVersionChanged.
func TestConditionalPut(t *testing.T) {
	ctx := context.Background()
	c := newConn(t, newContainer(t), "", "connection_string")
	res, err := c.PutObject(ctx, "k", nil, 0, connector.WriteOptions{IfNoneMatch: "*"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutObject(ctx, "k", nil, 0, connector.WriteOptions{IfNoneMatch: "*"}); !errors.Is(err, connector.ErrVersionChanged) {
		t.Fatalf("IfNoneMatch on existing: %v", err)
	}
	if _, err := c.PutObject(ctx, "k", nil, 0, connector.WriteOptions{IfMatch: res.Version}); err != nil {
		t.Fatalf("IfMatch current: %v", err)
	}
	if _, err := c.PutObject(ctx, "k", nil, 0, connector.WriteOptions{IfMatch: res.Version}); !errors.Is(err, connector.ErrVersionChanged) {
		t.Fatalf("IfMatch stale: %v", err)
	}
}
