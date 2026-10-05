//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/s3"
	"github.com/sunny36/portage/internal/testenv"
)

// TestVerifyAgainstSeaweedFS writes objects the way a synced pipeline would
// leave them (destination prefix, keys relative to the source prefix), then
// runs the command end to end: config -> destination connector -> report.
func TestVerifyAgainstSeaweedFS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bucket := newBucket(t)

	s3cfg := config.S3Config{
		Bucket: bucket, Region: testenv.S3Region, Endpoint: testenv.S3Endpoint(), PathStyle: true,
		Flavor: "generic", AccessKeyID: testenv.S3AccessKeyID, SecretAccessKey: testenv.S3SecretAccessKey,
	}
	dst, err := s3.New(ctx, s3cfg, "out/")
	if err != nil {
		t.Fatal(err)
	}

	// Source keys live under in/; the pipeline maps in/<k> -> out/<k>.
	contents := map[string][]byte{
		"a.bin":         randBytes(t, 1000),
		"dir/b.bin":     randBytes(t, 6<<20+7), // a few MiB, streamed
		"empty":         {},
		"日本/ü.bin":      randBytes(t, 1234),
		"to-corrupt":    randBytes(t, 500),
		"wrong-size":    randBytes(t, 500),
		"never-written": randBytes(t, 10), // in the manifest only
	}
	var manifest strings.Builder
	now := time.Now().UTC()
	for k, b := range contents {
		manifest.WriteString(line("in/"+k, int64(len(b)), shaHex(b), now))
		switch k {
		case "never-written":
			continue
		case "to-corrupt":
			b = append([]byte("X"), b[1:]...) // same size, different content
		case "wrong-size":
			b = b[:499]
		}
		if _, err := dst.PutObject(ctx, k, bytes.NewReader(b), int64(len(b)), connector.WriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// An earlier write of a.bin with other content: superseded, ignored.
	manifest.WriteString(line("in/a.bin", 3, shaHex([]byte("old")), now.Add(-time.Minute)))
	// Outside the source prefix and excluded by the filter: not checked.
	manifest.WriteString(line("elsewhere/x", 1, shaHex([]byte("x")), now))
	manifest.WriteString(line("in/skip.tmp", 1, shaHex([]byte("x")), now))

	dir := t.TempDir()
	mpath := filepath.Join(dir, "m.jsonl")
	if err := os.WriteFile(mpath, []byte(manifest.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := writeConfig(t, dir, bucket)

	metricsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, sampleMetrics)
	}))
	defer metricsSrv.Close()

	jsonPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	err = run(ctx, []string{"-c", cfgPath, "-pipeline", "soak", "-concurrency", "3", "-json", jsonPath,
		"-metrics", metricsSrv.URL, mpath}, &stdout, &stderr)
	if !errors.Is(err, errFailed) {
		t.Fatalf("run = %v, want errFailed\nstdout:\n%s\nstderr:\n%s", err, &stdout, &stderr)
	}
	t.Logf("summary:\n%s", &stdout)

	var rep report
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	v := rep.Verify
	if rep.Passed || v.Files != 7 || v.OK != 4 || v.Missing != 1 || v.SHAMismatch != 1 || v.SizeMismatch != 1 || v.Errors != 0 {
		t.Errorf("verify = %+v (passed %v)", v, rep.Passed)
	}
	if rep.Superseded != 1 || rep.OutOfScope != 1 || rep.Excluded != 1 || rep.UniqueKeys != 9 {
		t.Errorf("manifest stats: superseded %d out-of-scope %d excluded %d unique %d",
			rep.Superseded, rep.OutOfScope, rep.Excluded, rep.UniqueKeys)
	}
	problems := map[string]string{}
	for _, f := range v.Failures {
		problems[f.DestKey] = f.Problem
	}
	want := map[string]string{
		"out/never-written": problemMissing, "out/to-corrupt": problemSHAMismatch, "out/wrong-size": problemSizeMismatch,
	}
	if fmt.Sprint(problems) != fmt.Sprint(want) {
		t.Errorf("failures = %v, want %v", problems, want)
	}
	if rep.Metrics == nil || rep.Metrics.SyncLagSeconds.P50 == nil || *rep.Metrics.SyncLagSeconds.P50 != 2 {
		t.Errorf("metrics = %+v (error %q)", rep.Metrics, rep.MetricsError)
	}

	// Repair the destination: now it passes, and exits zero.
	for _, k := range []string{"never-written", "to-corrupt", "wrong-size"} {
		b := contents[k]
		if _, err := dst.PutObject(ctx, k, bytes.NewReader(b), int64(len(b)), connector.WriteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	stdout.Reset()
	if err := run(ctx, []string{"-c", cfgPath, "-pipeline", "soak", mpath}, &stdout, &stderr); err != nil {
		t.Fatalf("after repair: %v\n%s", err, &stdout)
	}
	if !strings.Contains(stdout.String(), "PASS") {
		t.Errorf("summary:\n%s", &stdout)
	}
}

func writeConfig(t *testing.T, dir, bucket string) string {
	t.Helper()
	cfg := fmt.Sprintf(`version: 1
database_url: postgres://unused
pipelines:
  - name: other
    source: {prefix: x/, azure: {container: c, auth: connection_string, connection_string: "%[1]s"}}
    destination: {s3: {bucket: nope, region: us-east-1}}
    events: {type: none}
  - name: soak
    source:
      prefix: in
      azure: {container: c, auth: connection_string, connection_string: "%[1]s"}
    destination:
      prefix: out
      s3:
        bucket: %[2]s
        region: %[3]s
        endpoint: %[4]s
        path_style: true
        flavor: generic
        access_key_id: %[5]s
        secret_access_key: %[6]s
    events: {type: none}
    filters: {exclude: ["*.tmp"]}
`, testenv.AzuriteConnectionString(), bucket, testenv.S3Region, testenv.S3Endpoint(),
		testenv.S3AccessKeyID, testenv.S3SecretAccessKey)
	p := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(p, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
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
	name := testenv.UniqueName(t, "verify-it")
	if _, err := cl.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p := s3sdk.NewListObjectsV2Paginator(cl, &s3sdk.ListObjectsV2Input{Bucket: aws.String(name)})
		for p.HasMorePages() {
			out, err := p.NextPage(context.Background())
			if err != nil {
				break
			}
			for _, o := range out.Contents {
				_, _ = cl.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key})
			}
		}
		_, _ = cl.DeleteBucket(context.Background(), &s3sdk.DeleteBucketInput{Bucket: aws.String(name)})
	})
	return name
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}
