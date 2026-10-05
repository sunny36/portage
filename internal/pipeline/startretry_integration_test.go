//go:build integration

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/testenv"
)

// A file-mode pipeline whose destination isn't ready at start-up must not
// stop the engine: it retries in the background and syncs once the bucket
// exists (soak finding: `portage run` used to exit and needed a supervisor).
func TestFileModeRetriesFailedStart(t *testing.T) {
	oldBase, oldMax := startRetryBase, startRetryMax
	startRetryBase, startRetryMax = 200*time.Millisecond, time.Second
	t.Cleanup(func() { startRetryBase, startRetryMax = oldBase, oldMax })

	ctx := context.Background()
	container := newContainer(t)
	bucket := testenv.UniqueName(t, "later")
	cfg := &config.File{
		Version:     1,
		DatabaseURL: freshDatabase(t),
		Pipelines: []config.Pipeline{{
			Name: "later",
			Source: config.Endpoint{Prefix: "in/", Azure: &config.AzureConfig{
				AccountURL: testenv.AzuriteBlobURL(), Container: container,
				Auth: "connection_string", ConnectionString: testenv.AzuriteConnectionString(),
			}},
			Destination: config.Endpoint{Prefix: "out/", S3: &config.S3Config{
				Bucket: bucket, Region: testenv.S3Region, Endpoint: testenv.S3Endpoint(),
				PathStyle: true, Flavor: "generic",
				AccessKeyID: testenv.S3AccessKeyID, SecretAccessKey: testenv.S3SecretAccessKey,
			}},
			Events:            config.Events{Type: "none"},
			ReconcileInterval: time.Hour, // only the on-start reconcile
			ExistingFiles:     "copy",
			Concurrency:       2,
			PartConcurrency:   2,
			PartSize:          5 << 20,
		}},
	}
	src, err := azure.New(ctx, *cfg.Pipelines[0].Source.Azure, "")
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(t, 4096)
	putBlob(t, src, "in/early.bin", data)

	stop := startEngine(t, cfg) // must not exit although the bucket is missing
	time.Sleep(1500 * time.Millisecond)

	newBucketNamed(t, bucket)
	dst, err := s3.New(ctx, *cfg.Pipelines[0].Destination.S3, "out/")
	if err != nil {
		t.Fatal(err)
	}
	waitSynced(t, dst, map[string][]byte{"early.bin": data}, 60*time.Second)
	stop()
}
