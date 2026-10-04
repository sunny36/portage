//go:build integration

package s3

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/connectortest"
	"github.com/sunny36/portage/internal/testenv"
)

func seaweedConfig(bucket string) config.S3Config {
	return config.S3Config{
		Bucket:          bucket,
		Region:          testenv.S3Region,
		Endpoint:        testenv.S3Endpoint(),
		PathStyle:       true,
		AccessKeyID:     testenv.S3AccessKeyID,
		SecretAccessKey: testenv.S3SecretAccessKey,
		Flavor:          FlavorGeneric,
	}
}

// seaweedFactory creates a fresh bucket per subtest and a connector scoped to
// prefix inside it.
func seaweedFactory(prefix string) connectortest.Factory {
	return func(t *testing.T) connector.Connector {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		bucket := testenv.UniqueName(t, "portage-s3")
		c, err := New(ctx, seaweedConfig(bucket), prefix)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, err := c.client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("CreateBucket %s at %s: %v (is SeaweedFS up? docker compose -f deploy/docker-compose.yml up -d --wait seaweedfs)",
				bucket, testenv.S3Endpoint(), err)
		}
		t.Cleanup(func() {
			emptyScope(t, c.client, bucket, "")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := c.client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
				t.Logf("cleanup: DeleteBucket %s: %v", bucket, err)
			}
		})
		return c
	}
}

// emptyScope deletes every object and aborts every multipart upload under
// prefix in bucket. Errors are logged, not fatal.
func emptyScope(t *testing.T, client *s3sdk.Client, bucket, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	objs := s3sdk.NewListObjectsV2Paginator(client, &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	for objs.HasMorePages() {
		page, err := objs.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup: list %s/%s: %v", bucket, prefix, err)
			break
		}
		for _, o := range page.Contents {
			if _, err := client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String(bucket), Key: o.Key}); err != nil {
				t.Logf("cleanup: delete %s: %v", aws.ToString(o.Key), err)
			}
		}
	}
	var keyMarker, idMarker *string
	for {
		out, err := client.ListMultipartUploads(ctx, &s3sdk.ListMultipartUploadsInput{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix),
			KeyMarker: keyMarker, UploadIdMarker: idMarker,
		})
		if err != nil {
			t.Logf("cleanup: list uploads %s/%s: %v", bucket, prefix, err)
			return
		}
		for _, u := range out.Uploads {
			abortUpload(ctx, t, client, bucket, u)
		}
		if !aws.ToBool(out.IsTruncated) || len(out.Uploads) == 0 {
			return
		}
		keyMarker, idMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
}

func abortUpload(ctx context.Context, t *testing.T, client *s3sdk.Client, bucket string, u types.MultipartUpload) {
	if _, err := client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: u.Key, UploadId: u.UploadId,
	}); err != nil {
		t.Logf("cleanup: abort %s: %v", aws.ToString(u.Key), err)
	}
}

func TestConformanceSeaweedFS(t *testing.T) {
	connectortest.Run(t, seaweedFactory(""))
}

func TestConformanceSeaweedFSPrefix(t *testing.T) {
	connectortest.Run(t, seaweedFactory("scoped/pipeline-a"))
}

// TestPrefixIsolation checks that a prefixed connector neither sees nor
// returns keys outside its prefix.
func TestPrefixIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	root := seaweedFactory("")(t).(*Connector)
	scoped, err := New(ctx, seaweedConfig(root.bucket), "in")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"in/a", "inside/b", "out/c"} {
		if _, err := root.PutObject(ctx, k, nil, 0, connector.WriteOptions{}); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	page, err := scoped.List(ctx, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "a" {
		t.Fatalf("scoped List = %+v, want only key a", page.Objects)
	}
	if _, err := scoped.Stat(ctx, "a"); err != nil {
		t.Fatalf("scoped Stat a: %v", err)
	}
}

// TestPutUnseekable: PutObject must accept a plain io.Reader (no Seek), which
// the SDK cannot hash up front for SigV4.
func TestPutUnseekable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := seaweedFactory("")(t)
	data := strings.Repeat("portage", 1000)
	if _, err := c.PutObject(ctx, "stream", io.MultiReader(strings.NewReader(data)), int64(len(data)), connector.WriteOptions{}); err != nil {
		t.Fatalf("PutObject unseekable: %v", err)
	}
	rc, err := c.OpenRange(ctx, "stream", "", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != data {
		t.Fatalf("read back %d bytes, want %d", len(got), len(data))
	}
}

// TestResumeUnknownUpload:ResumeUpload of a session that does not exist
// must report ErrNotFound so the engine restarts the upload.
func TestResumeUnknownUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c := seaweedFactory("")(t)
	up, err := c.BeginUpload(ctx, "x.bin", connector.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := up.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if err := up.Abort(ctx); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if _, err := c.ResumeUpload(ctx, "x.bin", up.ID(), connector.WriteOptions{}); !errorsIsNotFound(err) {
		t.Fatalf("ResumeUpload aborted session: %v, want ErrNotFound", err)
	}
}
