//go:build integration

package check

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/testenv"
)

type emulatorEnv struct {
	container, queue, bucket string
	q                        *azqueue.QueueClient
}

func setupEmulators(t *testing.T) emulatorEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := emulatorEnv{
		container: testenv.UniqueName(t, "check-src"),
		queue:     testenv.UniqueName(t, "check-q"),
		bucket:    testenv.UniqueName(t, "check-dst"),
	}

	az, err := azblob.NewClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := az.CreateContainer(ctx, e.container, nil); err != nil {
		t.Fatalf("create container (are the emulators up?): %v", err)
	}
	if _, err := az.UploadBuffer(ctx, e.container, "incoming/hello.txt", []byte("hello\n"), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = az.DeleteContainer(context.Background(), e.container, nil) })

	qs, err := azqueue.NewServiceClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{e.queue, e.queue + poisonSuffix} {
		if _, err := qs.CreateQueue(ctx, name, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = qs.DeleteQueue(context.Background(), name, nil) })
	}
	e.q = qs.NewQueueClient(e.queue)
	msg := fmt.Sprintf(`{"topic":"/subscriptions/s/resourceGroups/r/providers/Microsoft.Storage/storageAccounts/devstoreaccount1",
"subject":"/blobServices/default/containers/%[1]s/blobs/incoming/hello.txt","eventType":"Microsoft.Storage.BlobCreated",
"eventTime":"2026-10-05T00:00:00Z","id":"1","data":{"api":"PutBlob","eTag":"0x1","contentLength":6,
"url":"http://127.0.0.1/devstoreaccount1/%[1]s/incoming/hello.txt","sequencer":"01"},"dataVersion":"","metadataVersion":"1"}`, e.container)
	if _, err := e.q.EnqueueMessage(ctx, base64.StdEncoding.EncodeToString([]byte(msg)), nil); err != nil {
		t.Fatal(err)
	}

	ac, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(testenv.S3Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testenv.S3AccessKeyID, testenv.S3SecretAccessKey, "")))
	if err != nil {
		t.Fatal(err)
	}
	s3c := s3sdk.NewFromConfig(ac, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String(testenv.S3Endpoint())
		o.UsePathStyle = true
	})
	if _, err := s3c.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(e.bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s3c.DeleteBucket(context.Background(), &s3sdk.DeleteBucketInput{Bucket: aws.String(e.bucket)})
	})
	// The shared SeaweedFS allocates volumes for a new bucket lazily and
	// answers 500 while it has none free (other suites churn buckets). Wait
	// until the bucket takes writes so the checks see a settled emulator.
	for i := 0; ; i++ {
		_, err := s3c.PutObject(ctx, &s3sdk.PutObjectInput{Bucket: aws.String(e.bucket), Key: aws.String("warmup"),
			Body: strings.NewReader("x")})
		if err == nil {
			_, _ = s3c.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String(e.bucket), Key: aws.String("warmup")})
			break
		}
		if i == 40 {
			t.Fatalf("SeaweedFS bucket %s never became writable: %v", e.bucket, err)
		}
		time.Sleep(time.Second)
	}
	return e
}

func (e emulatorEnv) config(t *testing.T, bucket, secret string) *config.File {
	t.Helper()
	cfg, err := config.Parse(fmt.Appendf(nil, `version: 1
database_url: %q
pipelines:
  - name: check-it
    source:
      prefix: incoming/
      azure:
        container: %s
        auth: connection_string
        connection_string: %q
    destination:
      prefix: from-azure/
      s3:
        bucket: %s
        region: %s
        endpoint: %s
        path_style: true
        flavor: generic
        access_key_id: %s
        secret_access_key: %s
    events:
      type: azure_queue
      queue_account_url: %s
      queue_name: %s
`, testenv.PostgresURL(), e.container, testenv.AzuriteConnectionString(), bucket, testenv.S3Region,
		testenv.S3Endpoint(), testenv.S3AccessKeyID, secret, testenv.AzuriteQueueURL(), e.queue))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestCheckEmulators(t *testing.T) {
	e := setupEmulators(t)
	ctx := context.Background()

	t.Run("all ok", func(t *testing.T) {
		rep, err := Run(ctx, e.config(t, e.bucket, testenv.S3SecretAccessKey), Options{Timeout: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rep.Results {
			if r.Status != StatusOK {
				t.Errorf("%s: %s %q (fix %q)", r.Check, r.Status, r.Detail, r.Fix)
			}
		}
		if !strings.Contains(find(t, rep, CheckEvents).Detail, "~1 message(s)") {
			t.Errorf("events detail %q", find(t, rep, CheckEvents).Detail)
		}
		// Peek must not consume: the message is still there and visible.
		props, err := e.q.GetProperties(ctx, nil)
		if err != nil || props.ApproximateMessagesCount == nil || *props.ApproximateMessagesCount != 1 {
			t.Errorf("queue count after check: %v %v", props.ApproximateMessagesCount, err)
		}
		peek, err := e.q.PeekMessages(ctx, nil)
		if err != nil || len(peek.Messages) != 1 {
			t.Errorf("message no longer visible after check: %v", err)
		}
	})

	t.Run("wrong bucket", func(t *testing.T) {
		rep, err := Run(ctx, e.config(t, e.bucket+"-missing", testenv.S3SecretAccessKey), Options{Timeout: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		expect(t, rep, CheckDestList, StatusFail, "was not found", "s3.bucket")
		expect(t, rep, CheckDestWrite, StatusSkipped)
		expect(t, rep, CheckSourceList, StatusOK)
		if !rep.Failed() {
			t.Error("report should fail")
		}
	})

	t.Run("wrong secret", func(t *testing.T) {
		rep, err := Run(ctx, e.config(t, e.bucket, "not-the-secret"), Options{Timeout: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		expect(t, rep, CheckDestList, StatusFail, "credentials were rejected", "secret_access_key")
		expect(t, rep, CheckDestMultipart, StatusSkipped)
	})
}
