//go:build integration

package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/testenv"
)

const mibT = 1 << 20

func itCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// newAzurite returns a connector on a fresh Azurite container.
func newAzurite(t *testing.T) connector.Connector {
	t.Helper()
	name := testenv.UniqueName(t, "portage-xfer")
	cc, err := container.NewClientFromConnectionString(testenv.AzuriteConnectionString(), name, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Create(itCtx(t), nil); err != nil {
		t.Fatalf("create container %s: %v (is Azurite up?)", name, err)
	}
	t.Cleanup(func() { _, _ = cc.Delete(context.Background(), nil) })
	c, err := azure.New(itCtx(t), config.AzureConfig{
		Container: name, Auth: "connection_string", ConnectionString: testenv.AzuriteConnectionString(),
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// newSeaweed returns a connector on a fresh SeaweedFS bucket.
func newSeaweed(t *testing.T) connector.Connector {
	t.Helper()
	ctx := itCtx(t)
	bucket := testenv.UniqueName(t, "portage-xfer")
	client := s3sdk.New(s3sdk.Options{
		Region:       testenv.S3Region,
		BaseEndpoint: aws.String(testenv.S3Endpoint()),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(testenv.S3AccessKeyID, testenv.S3SecretAccessKey, ""),
	})
	if _, err := client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket %s: %v (is SeaweedFS up?)", bucket, err)
	}
	c, err := s3.New(ctx, config.S3Config{
		Bucket: bucket, Region: testenv.S3Region, Endpoint: testenv.S3Endpoint(), PathStyle: true,
		AccessKeyID: testenv.S3AccessKeyID, SecretAccessKey: testenv.S3SecretAccessKey, Flavor: s3.FlavorGeneric,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		ups, err := client.ListMultipartUploads(ctx, &s3sdk.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
		if err == nil {
			for _, u := range ups.Uploads {
				_, _ = client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: u.Key, UploadId: u.UploadId})
			}
		}
		page, err := c.List(ctx, "", "", 0)
		if err == nil {
			for _, o := range page.Objects {
				_ = c.Delete(ctx, o.Key)
			}
		}
		if _, err := client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Logf("cleanup: delete bucket %s: %v", bucket, err)
		}
	})
	return c
}

func readAll(t *testing.T, c connector.Connector, key string) []byte {
	t.Helper()
	rc, err := c.OpenRange(itCtx(t), key, "", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func putSource(t *testing.T, src connector.Connector, key string, data []byte) connector.ObjectInfo {
	t.Helper()
	ctx := itCtx(t)
	if _, err := src.PutObject(ctx, key, bytes.NewReader(data), int64(len(data)), connector.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	info, err := src.Stat(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

type pair struct {
	name     string
	src, dst func(*testing.T) connector.Connector
}

var pairs = []pair{
	{"AzuriteToSeaweedFS", newAzurite, newSeaweed},
	{"SeaweedFSToAzurite", newSeaweed, newAzurite},
}

func TestIntegrationCopy(t *testing.T) {
	pool := NewBufferPool(64 * mibT)
	for _, p := range pairs {
		for _, size := range []int{0, 1 << 10, 7 * mibT, 23 * mibT} {
			t.Run(fmt.Sprintf("%s/%d", p.name, size), func(t *testing.T) {
				t.Parallel()
				src, dst := p.src(t), p.dst(t)
				data := randData(uint64(size)+11, size)
				info := putSource(t, src, "in/obj.bin", data)
				res, err := Copy(itCtx(t), Request{
					Src: src, SrcKey: "in/obj.bin", Info: info,
					Dst: dst, DstKey: "out/obj.bin", ContentType: "application/octet-stream",
				}, Options{PartSize: 5 * mibT, PartConcurrency: 3, Pool: pool}, Hooks{})
				if err != nil {
					t.Fatal(err)
				}
				want := sha256.Sum256(data)
				if !bytes.Equal(res.SHA256, want[:]) || !res.Verified.OK {
					t.Fatalf("result %+v", res)
				}
				got := readAll(t, dst, "out/obj.bin")
				if gotSum := sha256.Sum256(got); gotSum != want {
					t.Fatalf("destination sha256 %x, want %x (len %d vs %d)", gotSum, want, len(got), size)
				}
				t.Logf("verified: %+v", res.Verified)
			})
		}
	}
}

// crashingConn wraps a connector and cancels the copy after `after`
// successful UploadPart calls, simulating a worker dying mid-upload.
type crashingConn struct {
	connector.Connector
	after int64
	done  atomic.Int64
	crash context.CancelFunc
}

func (c *crashingConn) BeginUpload(ctx context.Context, key string, opts connector.WriteOptions) (connector.Upload, error) {
	u, err := c.Connector.BeginUpload(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	return &crashingUpload{Upload: u, c: c}, nil
}

type crashingUpload struct {
	connector.Upload
	c *crashingConn
}

func (u *crashingUpload) UploadPart(ctx context.Context, n int, data []byte) (connector.Part, error) {
	if u.c.done.Load() >= u.c.after {
		u.c.crash()
		return connector.Part{}, context.Canceled
	}
	p, err := u.Upload.UploadPart(ctx, n, data)
	if err == nil {
		u.c.done.Add(1)
	}
	return p, err
}

func TestIntegrationResume(t *testing.T) {
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			src, dst := p.src(t), p.dst(t)
			size := 23 * mibT
			data := randData(99, size)
			info := putSource(t, src, "big.bin", data)
			opts := Options{PartSize: 5 * mibT, PartConcurrency: 1}

			ctx, crash := context.WithCancel(itCtx(t))
			cc := &crashingConn{Connector: dst, after: 2, crash: crash}
			var uploadID string
			_, err := Copy(ctx, Request{Src: src, SrcKey: "big.bin", Info: info, Dst: cc, DstKey: "big.bin"}, opts,
				Hooks{OnUploadStarted: func(_ context.Context, id string) error { uploadID = id; return nil }})
			if !errors.Is(err, context.Canceled) || uploadID == "" {
				t.Fatalf("interrupted copy: err=%v id=%q", err, uploadID)
			}

			var uploaded atomic.Int64
			res, err := Copy(itCtx(t), Request{Src: src, SrcKey: "big.bin", Info: info, Dst: dst, DstKey: "big.bin", ResumeUploadID: uploadID},
				opts, Hooks{OnBytes: func(n int64) { uploaded.Add(n) }})
			if err != nil {
				t.Fatal(err)
			}
			if res.PartsReused != 2 || res.UploadID != uploadID {
				t.Fatalf("PartsReused=%d UploadID=%q (want 2, %q)", res.PartsReused, res.UploadID, uploadID)
			}
			if uploaded.Load() != int64(size-2*5*mibT) {
				t.Fatalf("uploaded %d bytes on resume, want %d", uploaded.Load(), size-2*5*mibT)
			}
			want := sha256.Sum256(data)
			got := readAll(t, dst, "big.bin")
			if gotSum := sha256.Sum256(got); gotSum != want || !bytes.Equal(res.SHA256, want[:]) || !res.Verified.OK {
				t.Fatalf("content sha %x, result %+v, want %x", gotSum, res, want)
			}
		})
	}
}
