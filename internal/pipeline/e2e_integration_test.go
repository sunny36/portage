//go:build integration

package pipeline

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azq "github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/testenv"
)

// TestEndToEnd runs the real engine: Azurite (source + event queue) ->
// SeaweedFS (destination), with a throwaway Postgres database.
func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	dbURL := freshDatabase(t)
	container, bucket, queueName := newContainer(t), newBucket(t), newEventQueue(t)

	cfg := &config.File{
		Version:     1,
		DatabaseURL: dbURL,
		Pipelines: []config.Pipeline{{
			Name: "e2e",
			Source: config.Endpoint{Prefix: "in/", Azure: &config.AzureConfig{
				AccountURL: testenv.AzuriteBlobURL(), Container: container,
				Auth: "connection_string", ConnectionString: testenv.AzuriteConnectionString(),
			}},
			Destination: config.Endpoint{Prefix: "out/", S3: &config.S3Config{
				Bucket: bucket, Region: testenv.S3Region, Endpoint: testenv.S3Endpoint(),
				PathStyle: true, Flavor: "generic",
				AccessKeyID: testenv.S3AccessKeyID, SecretAccessKey: testenv.S3SecretAccessKey,
			}},
			Events:  config.Events{Type: "azure_queue", QueueName: queueName},
			Filters: config.Filters{Exclude: []string{"*.tmp"}},
			// Long interval: only the on-start reconcile runs, so changes
			// after that must arrive through events.
			ReconcileInterval: time.Hour,
			ExistingFiles:     "copy",
			Concurrency:       4,
			PartConcurrency:   3,
			PartSize:          5 << 20,
		}},
	}
	src, err := azure.New(ctx, *cfg.Pipelines[0].Source.Azure, "")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := s3.New(ctx, *cfg.Pipelines[0].Destination.S3, "out/")
	if err != nil {
		t.Fatal(err)
	}

	want := map[string][]byte{ // keys relative to the prefixes
		"small.txt":     randBytes(t, 1024),
		"empty":         nil,
		"日本 語/ü.bin":    randBytes(t, 300_000),
		"big/multi.bin": randBytes(t, 23<<20+12345), // multipart: 5 parts
	}
	for k, b := range want {
		putBlob(t, src, "in/"+k, b)
	}
	putBlob(t, src, "in/scratch.tmp", []byte("excluded by filter"))
	putBlob(t, src, "elsewhere/x", []byte("outside the source prefix"))

	// Phase 1: initial copy via the on-start reconcile.
	stop := startEngine(t, cfg)
	waitSynced(t, dst, want, 90*time.Second)
	assertAbsent(t, dst, "scratch.tmp", "elsewhere/x")

	// Phase 2: an overwrite and a new file, delivered as Event Grid events.
	want["small.txt"] = randBytes(t, 2048)
	want["new.csv"] = []byte("a,b\n1,2\n")
	began := time.Now()
	for _, k := range []string{"small.txt", "new.csv"} {
		v := putBlob(t, src, "in/"+k, want[k])
		sendBlobCreated(t, queueName, container, "in/"+k, v, int64(len(want[k])))
	}
	waitSynced(t, dst, want, 30*time.Second)
	if took := time.Since(began); took > 15*time.Second {
		t.Errorf("event-driven sync took %s; want seconds", took)
	}
	t.Logf("event-driven sync of 2 files took %s", time.Since(began).Round(time.Millisecond))

	stats := pipelineStats(t, dbURL, "e2e")
	if stats.Synced != int64(len(want)) || stats.Failed != 0 {
		t.Errorf("record stats = %+v; want %d synced, 0 failed", stats, len(want))
	}
	before := destVersions(t, dst)
	stop()

	// Phase 3: restart. The on-start reconcile must find nothing to copy.
	stop = startEngine(t, cfg)
	time.Sleep(5 * time.Second)
	stop()
	if after := destVersions(t, dst); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("restart re-copied objects:\nbefore %v\nafter  %v", before, after)
	}
}

// startEngine runs pipeline.Run in the background and returns a function
// that stops it and checks it shut down cleanly.
func startEngine(t *testing.T, cfg *config.File) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	log := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
	go func() { done <- Run(ctx, cfg, nil, log) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v on shutdown, want nil", err)
			}
		case <-time.After(shutdownTimeout + 5*time.Second):
			t.Error("engine did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitSynced(t *testing.T, dst connector.Connector, want map[string][]byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var missing []string
	for time.Now().Before(deadline) {
		missing = missing[:0]
		for k, b := range want {
			if !sameContent(t, dst, k, b) {
				missing = append(missing, k)
			}
		}
		if len(missing) == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("not synced after %s: %v", timeout, missing)
}

func sameContent(t *testing.T, dst connector.Connector, key string, want []byte) bool {
	rc, err := dst.OpenRange(context.Background(), key, "", 0, -1)
	if errors.Is(err, connector.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("read dest %q: %v", key, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(got) == sha256.Sum256(want)
}

func assertAbsent(t *testing.T, dst connector.Connector, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, err := dst.Stat(context.Background(), k); !errors.Is(err, connector.ErrNotFound) {
			t.Errorf("%q should not have been copied (Stat err %v)", k, err)
		}
	}
}

func destVersions(t *testing.T, dst connector.Connector) map[string]string {
	t.Helper()
	out := map[string]string{}
	page, err := dst.List(context.Background(), "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page.Objects {
		out[o.Key] = o.Version
	}
	return out
}

func pipelineStats(t *testing.T, dbURL, id string) record.Stats {
	t.Helper()
	pool := mustPool(t, dbURL)
	defer pool.Close()
	st, err := record.NewPGStore(pool).Stats(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// --- fixtures ---------------------------------------------------------------

// freshDatabase creates a throwaway database so the engine's migrations and
// River tables don't collide with other tests sharing the Postgres container.
func freshDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, testenv.PostgresURL())
	if err != nil {
		t.Fatalf("connect to Postgres (is compose up?): %v", err)
	}
	name := strings.ReplaceAll(testenv.UniqueName(t, "portage-e2e"), "-", "_")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(context.Background())
	})
	u, err := url.Parse(testenv.PostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func newContainer(t *testing.T) string {
	t.Helper()
	svc, err := azblob.NewClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	name := testenv.UniqueName(t, "e2e-src")
	if _, err := svc.CreateContainer(context.Background(), name, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = svc.DeleteContainer(context.Background(), name, nil) })
	return name
}

func newBucket(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ac, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(testenv.S3Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testenv.S3AccessKeyID, testenv.S3SecretAccessKey, "")))
	if err != nil {
		t.Fatal(err)
	}
	cl := s3sdk.NewFromConfig(ac, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String(testenv.S3Endpoint())
		o.UsePathStyle = true
	})
	name := testenv.UniqueName(t, "e2e-dst")
	if _, err := cl.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		out, _ := cl.ListObjectsV2(context.Background(), &s3sdk.ListObjectsV2Input{Bucket: aws.String(name)})
		if out != nil {
			for _, o := range out.Contents {
				_, _ = cl.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key})
			}
		}
		_, _ = cl.DeleteBucket(context.Background(), &s3sdk.DeleteBucketInput{Bucket: aws.String(name)})
	})
	return name
}

func newEventQueue(t *testing.T) string {
	t.Helper()
	svc := queueService(t)
	name := testenv.UniqueName(t, "e2e-events")
	if _, err := svc.CreateQueue(context.Background(), name, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = svc.DeleteQueue(context.Background(), name, nil)
		_, _ = svc.DeleteQueue(context.Background(), name+"-poison", nil)
	})
	return name
}

func queueService(t *testing.T) *azq.ServiceClient {
	t.Helper()
	svc, err := azq.NewServiceClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// putBlob writes through the Azure connector (container-scoped) and returns
// the new version.
func putBlob(t *testing.T, src connector.Connector, key string, b []byte) string {
	t.Helper()
	res, err := src.PutObject(context.Background(), key, bytes.NewReader(b), int64(len(b)),
		connector.WriteOptions{ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
	return res.Version
}

var sequencer int64

// sendBlobCreated enqueues what Event Grid would deliver to a Storage Queue
// (one base64-encoded Event Grid schema event per message).
func sendBlobCreated(t *testing.T, queueName, container, name, etag string, size int64) {
	t.Helper()
	sequencer++
	blobURL := testenv.AzuriteBlobURL() + "/" + container + "/" + (&url.URL{Path: name}).EscapedPath()
	ev := map[string]any{
		"id":        fmt.Sprintf("e2e-%d", sequencer),
		"topic":     "/subscriptions/x/resourceGroups/x/providers/Microsoft.Storage/storageAccounts/devstoreaccount1",
		"subject":   "/blobServices/default/containers/" + container + "/blobs/" + name,
		"eventType": "Microsoft.Storage.BlobCreated",
		"eventTime": time.Now().UTC().Format(time.RFC3339Nano),
		"data": map[string]any{
			"api": "PutBlob", "blobType": "BlockBlob", "url": blobURL,
			"eTag": etag, "contentLength": size,
			"sequencer": fmt.Sprintf("%032x", time.Now().UnixNano()),
		},
		"dataVersion": "", "metadataVersion": "1",
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	msg := base64.StdEncoding.EncodeToString(raw)
	if _, err := queueService(t).NewQueueClient(queueName).EnqueueMessage(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func mustPool(t *testing.T, dbURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}
