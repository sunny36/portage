package check

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// fakeConn is an in-memory connector with per-operation error injection.
type fakeConn struct {
	mu       sync.Mutex
	objects  map[string][]byte
	meta     map[string]map[string]string
	errs     map[string]error // by op: List, Stat, OpenRange, PutObject, BeginUpload, UploadPart, Complete, Delete
	readOnly bool             // fail the test on any write
	t        *testing.T
	writes   int
}

func newFake(t *testing.T, objs map[string]string) *fakeConn {
	f := &fakeConn{objects: map[string][]byte{}, meta: map[string]map[string]string{}, errs: map[string]error{}, t: t}
	for k, v := range objs {
		f.objects[k] = []byte(v)
	}
	return f
}

func (f *fakeConn) err(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[op]
}

func (f *fakeConn) write(op string) error {
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	if f.readOnly {
		f.t.Errorf("%s on a read-only (source) connector", op)
	}
	return f.err(op)
}

func (f *fakeConn) Name() string { return "fake" }
func (f *fakeConn) Limits() connector.Limits {
	return connector.Limits{MinPartSize: 5 << 20, MaxPartSize: 5 << 30, MaxParts: 10000, SinglePutMax: 5 << 30}
}

func (f *fakeConn) List(_ context.Context, _, _ string, limit int) (connector.ListPage, error) {
	if err := f.err("List"); err != nil {
		return connector.ListPage{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var page connector.ListPage
	for _, k := range keys {
		if limit > 0 && len(page.Objects) == limit {
			break
		}
		page.Objects = append(page.Objects, connector.ObjectInfo{Key: k, Size: int64(len(f.objects[k])), Version: "v-" + k})
	}
	return page, nil
}

func (f *fakeConn) Stat(_ context.Context, key string) (connector.ObjectInfo, error) {
	if err := f.err("Stat"); err != nil {
		return connector.ObjectInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok {
		return connector.ObjectInfo{}, connector.ErrNotFound
	}
	return connector.ObjectInfo{Key: key, Size: int64(len(b)), Version: "v-" + key, Metadata: f.meta[key]}, nil
}

func (f *fakeConn) OpenRange(_ context.Context, key, _ string, offset, length int64) (io.ReadCloser, error) {
	if err := f.err("OpenRange"); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	if !ok {
		return nil, connector.ErrNotFound
	}
	end := int64(len(b))
	if length >= 0 && offset+length < end {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(b[offset:end])), nil
}

func (f *fakeConn) PutObject(_ context.Context, key string, data io.Reader, _ int64, opts connector.WriteOptions) (connector.WriteResult, error) {
	if err := f.write("PutObject"); err != nil {
		return connector.WriteResult{}, err
	}
	b, _ := io.ReadAll(data)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = b
	f.meta[key] = opts.Metadata
	return connector.WriteResult{Version: "v-" + key}, nil
}

func (f *fakeConn) BeginUpload(_ context.Context, key string, _ connector.WriteOptions) (connector.Upload, error) {
	if err := f.write("BeginUpload"); err != nil {
		return nil, err
	}
	return &fakeUpload{f: f, key: key}, nil
}

func (f *fakeConn) ResumeUpload(context.Context, string, string, connector.WriteOptions) (connector.Upload, error) {
	return nil, connector.ErrNotFound
}

func (f *fakeConn) Delete(_ context.Context, key string) error {
	if err := f.write("Delete"); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func (f *fakeConn) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

type fakeUpload struct {
	f       *fakeConn
	key     string
	parts   map[int][]byte
	aborted bool
}

func (u *fakeUpload) ID() string { return "up-1" }
func (u *fakeUpload) UploadPart(_ context.Context, n int, data []byte) (connector.Part, error) {
	if err := u.f.err("UploadPart"); err != nil {
		return connector.Part{}, err
	}
	if u.parts == nil {
		u.parts = map[int][]byte{}
	}
	u.parts[n] = append([]byte(nil), data...)
	return connector.Part{Number: n, Size: int64(len(data)), Token: fmt.Sprint(n)}, nil
}
func (u *fakeUpload) ListParts(context.Context) ([]connector.Part, error) { return nil, nil }
func (u *fakeUpload) Complete(_ context.Context, parts []connector.Part) (connector.WriteResult, error) {
	if err := u.f.err("Complete"); err != nil {
		return connector.WriteResult{}, err
	}
	var b []byte
	for _, p := range parts {
		b = append(b, u.parts[p.Number]...)
	}
	u.f.mu.Lock()
	u.f.objects[u.key] = b
	u.f.mu.Unlock()
	return connector.WriteResult{Version: "v-" + u.key}, nil
}
func (u *fakeUpload) Abort(context.Context) error { u.aborted = true; return nil }

type fakeQueue struct {
	msgs     []string
	peekErr  error
	countErr error
	poison   bool
	poisErr  error
}

func (q *fakeQueue) Peek(context.Context) ([]string, error) { return q.msgs, q.peekErr }
func (q *fakeQueue) ApproximateCount(context.Context) (int64, error) {
	return int64(len(q.msgs)), q.countErr
}
func (q *fakeQueue) PoisonExists(context.Context) (bool, error) { return q.poison, q.poisErr }

const egCreated = `{"topic":"/subscriptions/s/resourceGroups/r/providers/Microsoft.Storage/storageAccounts/a",
"subject":"/blobServices/default/containers/exports/blobs/x.txt","eventType":"Microsoft.Storage.BlobCreated",
"eventTime":"2026-10-05T00:00:00Z","id":"1","data":{"api":"PutBlob","eTag":"0x1","contentLength":3,
"url":"https://a.blob.core.windows.net/exports/x.txt","sequencer":"01"},"dataVersion":"","metadataVersion":"1"}`

func azureToOCI() config.Pipeline {
	return config.Pipeline{
		Name: "azure-to-oci",
		Source: config.Endpoint{Azure: &config.AzureConfig{
			AccountURL: "https://acct.blob.core.windows.net", Container: "exports", Auth: "default"}},
		Destination: config.Endpoint{Prefix: "in/", S3: &config.S3Config{
			Bucket: "imports", Region: "ap-singapore-1", Flavor: "oci",
			Endpoint: "https://ns.compat.objectstorage.ap-singapore-1.oraclecloud.com"}},
		Events: config.Events{Type: "azure_queue", QueueAccountURL: "https://acct.queue.core.windows.net",
			QueueName: "portage-events"},
		ReconcileInterval: 15 * time.Minute,
	}
}

type harness struct {
	cfg      *config.File
	src, dst *fakeConn
	q        *fakeQueue
	db       DBInfo
	dbErr    error
	skew     time.Duration
	clockErr error
}

func newHarness(t *testing.T) *harness {
	src := newFake(t, map[string]string{"a.txt": "hello"})
	src.readOnly = true
	return &harness{
		cfg: &config.File{DatabaseURL: "postgres://x", Pipelines: []config.Pipeline{azureToOCI()}},
		src: src, dst: newFake(t, nil),
		q:  &fakeQueue{msgs: []string{base64.StdEncoding.EncodeToString([]byte(egCreated))}, poison: true},
		db: DBInfo{ServerVersion: "17.2", AppliedMigrations: 3, QueueSchema: true},
	}
}

func (h *harness) run(t *testing.T) Report {
	t.Helper()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	rep, err := Run(context.Background(), h.cfg, Options{
		Timeout: 5 * time.Second,
		NewConnector: func(_ context.Context, ep config.Endpoint) (connector.Connector, error) {
			if ep.Azure != nil {
				return h.src, nil
			}
			return h.dst, nil
		},
		NewQueue:   func(config.Pipeline) (QueueProber, error) { return h.q, nil },
		ProbeDB:    func(context.Context, string) (DBInfo, error) { return h.db, h.dbErr },
		ServerTime: func(context.Context, string) (time.Time, error) { return now.Add(-h.skew), h.clockErr },
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func find(t *testing.T, rep Report, name string) Result {
	t.Helper()
	for _, r := range rep.Results {
		if r.Check == name {
			return r
		}
	}
	t.Fatalf("no result for %q in %+v", name, rep.Results)
	return Result{}
}

func expect(t *testing.T, rep Report, name string, status Status, fixHas ...string) Result {
	t.Helper()
	r := find(t, rep, name)
	if r.Status != status {
		t.Errorf("%s: status %s, want %s (detail %q, fix %q)", name, r.Status, status, r.Detail, r.Fix)
	}
	for _, s := range fixHas {
		if !strings.Contains(r.Fix, s) {
			t.Errorf("%s: fix %q does not mention %q", name, r.Fix, s)
		}
	}
	return r
}

func TestAllOK(t *testing.T) {
	h := newHarness(t)
	rep := h.run(t)
	for _, r := range rep.Results {
		if r.Status != StatusOK {
			t.Errorf("%s: %s %q (fix %q)", r.Check, r.Status, r.Detail, r.Fix)
		}
	}
	if !rep.OK || rep.Failed() {
		t.Error("report should be OK")
	}
	if h.src.writes != 0 {
		t.Errorf("source saw %d writes", h.src.writes)
	}
	if n := h.dst.count(); n != 0 {
		t.Errorf("destination has %d leftover probe objects", n)
	}
	want := []string{CheckDatabase, CheckSourceList, CheckSourceRead, CheckDestList, CheckDestWrite,
		CheckDestMultipart, CheckEvents, CheckPoisonQueue, CheckClock}
	var got []string
	for _, r := range rep.Results {
		got = append(got, r.Check)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("order %v, want %v", got, want)
	}
}

func provErr(op string, status int, code string, sentinel error) error {
	return &connector.ProviderError{Op: op, Status: status, Code: code, Sentinel: sentinel, Err: errors.New("boom")}
}

func TestSourceFailures(t *testing.T) {
	t.Run("permission", func(t *testing.T) {
		h := newHarness(t)
		h.src.errs["List"] = provErr("List", 403, "AuthorizationPermissionMismatch", connector.ErrPermission)
		rep := h.run(t)
		r := expect(t, rep, CheckSourceList, StatusFail, "Storage Blob Data Reader", "blobServices/default/containers/")
		if !strings.Contains(r.Detail, "HTTP 403 AuthorizationPermissionMismatch") {
			t.Errorf("detail %q", r.Detail)
		}
		expect(t, rep, CheckSourceRead, StatusSkipped)
		if !rep.Failed() {
			t.Error("report should fail")
		}
	})
	t.Run("default credential unavailable", func(t *testing.T) {
		h := newHarness(t)
		h.src.errs["List"] = &connector.ProviderError{Op: "List",
			Err: errors.New("DefaultAzureCredential: failed to acquire a token; attempted credentials: none")}
		rep := h.run(t)
		expect(t, rep, CheckSourceList, StatusFail, "az login", "managed identity")
	})
	t.Run("container not found", func(t *testing.T) {
		h := newHarness(t)
		h.src.errs["List"] = provErr("List", 404, "ContainerNotFound", connector.ErrNotFound)
		expect(t, h.run(t), CheckSourceList, StatusFail, `Container "exports" was not found`)
	})
	t.Run("empty source", func(t *testing.T) {
		h := newHarness(t)
		h.src.objects = map[string][]byte{}
		rep := h.run(t)
		expect(t, rep, CheckSourceList, StatusWarn)
		expect(t, rep, CheckSourceRead, StatusSkipped)
		if rep.Failed() {
			t.Error("an empty source is not a failure")
		}
	})
	t.Run("read denied", func(t *testing.T) {
		h := newHarness(t)
		h.src.errs["OpenRange"] = provErr("OpenRange", 403, "AuthorizationPermissionMismatch", connector.ErrPermission)
		expect(t, h.run(t), CheckSourceRead, StatusFail, "Storage Blob Data Reader")
	})
}

func TestDestinationFailures(t *testing.T) {
	t.Run("bucket not found skips writes", func(t *testing.T) {
		h := newHarness(t)
		h.dst.errs["List"] = provErr("List", 404, "NoSuchBucket", nil)
		rep := h.run(t)
		expect(t, rep, CheckDestList, StatusFail, `Bucket "imports" was not found`, "oci os ns get", "ap-singapore-1")
		expect(t, rep, CheckDestWrite, StatusSkipped)
		expect(t, rep, CheckDestMultipart, StatusSkipped)
		if h.dst.writes != 0 {
			t.Errorf("wrote %d times to a missing bucket", h.dst.writes)
		}
	})
	t.Run("bad customer secret key", func(t *testing.T) {
		h := newHarness(t)
		h.dst.errs["List"] = provErr("List", 403, "SignatureDoesNotMatch", connector.ErrAuth)
		rep := h.run(t)
		expect(t, rep, CheckDestList, StatusFail, "Customer Secret Key", "ap-singapore-1")
		expect(t, rep, CheckDestWrite, StatusSkipped)
	})
	t.Run("write denied by OCI policy", func(t *testing.T) {
		h := newHarness(t)
		h.dst.errs["PutObject"] = provErr("PutObject", 403, "AccessDenied", connector.ErrPermission)
		h.dst.errs["BeginUpload"] = provErr("BeginUpload", 403, "AccessDenied", connector.ErrPermission)
		rep := h.run(t)
		expect(t, rep, CheckDestList, StatusOK)
		expect(t, rep, CheckDestWrite, StatusFail, "manage objects in compartment", "target.bucket.name")
		expect(t, rep, CheckDestMultipart, StatusFail, "manage objects in compartment")
	})
	t.Run("multipart part rejected", func(t *testing.T) {
		h := newHarness(t)
		h.dst.errs["UploadPart"] = provErr("UploadPart", 400, "InvalidDigest", nil)
		r := expect(t, h.run(t), CheckDestMultipart, StatusFail)
		if !strings.Contains(r.Detail, "upload part") {
			t.Errorf("detail %q", r.Detail)
		}
	})
	t.Run("delete denied without deletes is a warning", func(t *testing.T) {
		h := newHarness(t)
		h.dst.errs["Delete"] = provErr("Delete", 403, "AccessDenied", connector.ErrPermission)
		rep := h.run(t)
		r := expect(t, rep, CheckDestWrite, StatusWarn, "deletes: true")
		if !strings.Contains(r.Detail, ".portage-check/") {
			t.Errorf("detail should name the leftover key: %q", r.Detail)
		}
		expect(t, rep, CheckDestMultipart, StatusWarn)
		if rep.Failed() {
			t.Error("should not fail without deletes")
		}
	})
	t.Run("delete denied with deletes fails", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Pipelines[0].Deletes = true
		h.dst.errs["Delete"] = provErr("Delete", 403, "AccessDenied", connector.ErrPermission)
		expect(t, h.run(t), CheckDestWrite, StatusFail, "manage objects")
	})
	t.Run("connector construction error", func(t *testing.T) {
		h := newHarness(t)
		rep, err := Run(context.Background(), h.cfg, Options{
			NewConnector: func(context.Context, config.Endpoint) (connector.Connector, error) {
				return nil, errors.New("s3: region is required")
			},
			NewQueue:   func(config.Pipeline) (QueueProber, error) { return h.q, nil },
			ProbeDB:    func(context.Context, string) (DBInfo, error) { return h.db, nil },
			ServerTime: func(context.Context, string) (time.Time, error) { return time.Now(), nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		expect(t, rep, CheckSourceList, StatusFail)
		expect(t, rep, CheckDestList, StatusFail)
		expect(t, rep, CheckDestWrite, StatusSkipped)
	})
}

func TestEvents(t *testing.T) {
	t.Run("none warns", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Pipelines[0].Events = config.Events{Type: "none"}
		rep := h.run(t)
		r := expect(t, rep, CheckEvents, StatusWarn, "azure_queue")
		if !strings.Contains(r.Detail, "only the reconciler") || !strings.Contains(r.Detail, "15m") {
			t.Errorf("detail %q", r.Detail)
		}
		if rep.Failed() {
			t.Error("events none is not a failure")
		}
	})
	t.Run("webhook", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Pipelines[0].Events = config.Events{Type: "webhook", WebhookPath: "/events/x", WebhookSecret: strings.Repeat("s", 24)}
		r := expect(t, h.run(t), CheckEvents, StatusOK)
		if !strings.Contains(r.Detail, "secret 24 chars") || strings.Contains(r.Detail, "ssss") {
			t.Errorf("detail %q (must not leak the secret)", r.Detail)
		}
	})
	t.Run("queue not found", func(t *testing.T) {
		h := newHarness(t)
		h.q.peekErr = provErr("PeekMessages", 404, "QueueNotFound", connector.ErrNotFound)
		rep := h.run(t)
		expect(t, rep, CheckEvents, StatusFail, `Queue "portage-events" was not found`, "setup.sh")
		expect(t, rep, CheckPoisonQueue, StatusSkipped)
	})
	t.Run("queue permission", func(t *testing.T) {
		h := newHarness(t)
		h.q.peekErr = provErr("PeekMessages", 403, "AuthorizationPermissionMismatch", connector.ErrPermission)
		expect(t, h.run(t), CheckEvents, StatusFail, "Storage Queue Data Message Processor", "queueServices/default/queues/")
	})
	t.Run("count unavailable is still ok", func(t *testing.T) {
		h := newHarness(t)
		h.q.countErr = provErr("GetProperties", 403, "AuthorizationPermissionMismatch", connector.ErrPermission)
		r := expect(t, h.run(t), CheckEvents, StatusOK)
		if !strings.Contains(r.Detail, "count unavailable") {
			t.Errorf("detail %q", r.Detail)
		}
	})
	t.Run("empty queue warns", func(t *testing.T) {
		h := newHarness(t)
		h.q.msgs = nil
		expect(t, h.run(t), CheckEvents, StatusWarn, "az storage message peek")
	})
	t.Run("foreign messages warn", func(t *testing.T) {
		h := newHarness(t)
		h.q.msgs = append(h.q.msgs, "hello world")
		r := expect(t, h.run(t), CheckEvents, StatusWarn, "eventgridschema")
		if !strings.Contains(r.Detail, "1 of 2") {
			t.Errorf("detail %q", r.Detail)
		}
	})
	t.Run("poison queue missing", func(t *testing.T) {
		h := newHarness(t)
		h.q.poison = false
		expect(t, h.run(t), CheckPoisonQueue, StatusWarn, "Storage Queue Data Contributor")
	})
	t.Run("queue client error", func(t *testing.T) {
		h := newHarness(t)
		rep, _ := Run(context.Background(), h.cfg, Options{
			NewConnector: func(_ context.Context, ep config.Endpoint) (connector.Connector, error) {
				if ep.Azure != nil {
					return h.src, nil
				}
				return h.dst, nil
			},
			NewQueue:   func(config.Pipeline) (QueueProber, error) { return nil, errors.New("bad url") },
			ProbeDB:    func(context.Context, string) (DBInfo, error) { return h.db, nil },
			ServerTime: func(context.Context, string) (time.Time, error) { return time.Now(), nil },
		})
		expect(t, rep, CheckEvents, StatusFail)
		expect(t, rep, CheckPoisonQueue, StatusSkipped)
	})
}

func TestDatabase(t *testing.T) {
	h := newHarness(t)
	h.db = DBInfo{ServerVersion: "16.4"}
	r := expect(t, h.run(t), CheckDatabase, StatusOK)
	if !strings.Contains(r.Detail, "16.4") || !strings.Contains(r.Detail, "no Portage tables yet") {
		t.Errorf("detail %q", r.Detail)
	}

	h.dbErr = errors.New("failed to connect to `host=db user=portage`: dial error (dial tcp 10.0.0.4:5432: connect: connection refused)")
	expect(t, h.run(t), CheckDatabase, StatusFail, "Nothing is listening")
}

func TestClock(t *testing.T) {
	for _, tc := range []struct {
		skew time.Duration
		want Status
	}{
		{0, StatusOK},
		{30 * time.Second, StatusOK},
		{5 * time.Minute, StatusWarn},
		{-20 * time.Minute, StatusFail},
	} {
		h := newHarness(t)
		h.skew = tc.skew
		r := expect(t, h.run(t), CheckClock, tc.want)
		if !strings.Contains(r.Detail, "destination") {
			t.Errorf("skew %v: detail %q", tc.skew, r.Detail)
		}
	}
	h := newHarness(t)
	h.clockErr = errors.New("unreachable")
	expect(t, h.run(t), CheckClock, StatusWarn, "NTP")
}

func TestUnknownPipeline(t *testing.T) {
	h := newHarness(t)
	_, err := Run(context.Background(), h.cfg, Options{Pipeline: "nope"})
	if err == nil || !strings.Contains(err.Error(), "azure-to-oci") {
		t.Fatalf("err %v", err)
	}
}

func TestHint(t *testing.T) {
	oci := azureToOCI().Destination
	aws := config.Endpoint{S3: &config.S3Config{Bucket: "b", Region: "us-east-1", Flavor: "aws"}}
	az := azureToOCI().Source
	azKey := config.Endpoint{Azure: &config.AzureConfig{Container: "c", Auth: "shared_key"}}
	queue := Target{Role: RoleQueue, Endpoint: az, Events: azureToOCI().Events}
	dns := &net.DNSError{Name: "acct.blob.core.windows.net", Err: "no such host", IsNotFound: true}
	for name, tc := range map[string]struct {
		err  error
		t    Target
		want []string
	}{
		"oci auth":          {connector.ErrAuth, Target{Role: RoleDestination, Endpoint: oci}, []string{"Customer Secret Key", "region"}},
		"aws auth":          {connector.ErrAuth, Target{Role: RoleDestination, Endpoint: aws}, []string{"SignatureDoesNotMatch"}},
		"azure default":     {connector.ErrAuth, Target{Role: RoleSource, Endpoint: az}, []string{"az login", "managed identity"}},
		"azure shared key":  {connector.ErrAuth, Target{Role: RoleSource, Endpoint: azKey}, []string{"az storage account keys list"}},
		"azure perm source": {connector.ErrPermission, Target{Role: RoleSource, Endpoint: az}, []string{"Storage Blob Data Reader"}},
		"azure firewall": {provErr("List", 403, "AuthorizationFailure", connector.ErrPermission),
			Target{Role: RoleSource, Endpoint: az}, []string{"network rule"}},
		"queue perm": {connector.ErrPermission, queue, []string{"Storage Queue Data Message Processor", "-poison"}},
		"oci perm": {connector.ErrPermission, Target{Role: RoleDestination, Endpoint: oci},
			[]string{"Allow group <group> to read buckets in compartment", "manage objects", "'<domain>'/'<group>'"}},
		"aws perm":         {connector.ErrPermission, Target{Role: RoleDestination, Endpoint: aws, Deletes: true}, []string{"s3:PutObject", "s3:DeleteObject"}},
		"oci not found":    {connector.ErrNotFound, Target{Role: RoleDestination, Endpoint: oci}, []string{"namespace", "regional"}},
		"no such bucket":   {provErr("List", 404, "NoSuchBucket", nil), Target{Role: RoleDestination, Endpoint: aws}, []string{`Bucket "b" was not found`}},
		"azure not found":  {connector.ErrNotFound, Target{Role: RoleSource, Endpoint: az}, []string{`Container "exports"`}},
		"queue not found":  {provErr("Peek", 404, "QueueNotFound", nil), queue, []string{"queue_name", "setup.sh"}},
		"dns":              {fmt.Errorf("list: %w", dns), Target{Role: RoleSource, Endpoint: az}, []string{"does not resolve", "source.azure.account_url"}},
		"timeout":          {fmt.Errorf("x: %w", context.DeadlineExceeded), Target{Role: RoleDestination, Endpoint: oci}, []string{"Timed out"}},
		"throttled":        {connector.ErrThrottled, Target{Role: RoleDestination, Endpoint: oci}, []string{"transient"}},
		"checksum headers": {errors.New("The value of x-amz-content-sha256 header is invalid"), Target{Role: RoleDestination, Endpoint: aws}, []string{"flavor: oci"}},
	} {
		got := Hint(tc.err, tc.t)
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: hint %q does not mention %q", name, got, w)
			}
		}
	}
	if Hint(errors.New("something odd"), Target{Role: RoleSource, Endpoint: az}) != "" {
		t.Error("unknown errors get no hint")
	}
}

func TestQueueErr(t *testing.T) {
	if err := queueErr("Peek", errors.New("plain")); errors.Is(err, connector.ErrNotFound) {
		t.Error("plain error must not map to a sentinel")
	}
}

func TestEndpointURL(t *testing.T) {
	for _, tc := range []struct {
		ep   config.Endpoint
		want string
	}{
		{azureToOCI().Source, "https://acct.blob.core.windows.net"},
		{azureToOCI().Destination, "https://ns.compat.objectstorage.ap-singapore-1.oraclecloud.com"},
		{config.Endpoint{S3: &config.S3Config{Region: "eu-west-1"}}, "https://s3.eu-west-1.amazonaws.com"},
		{config.Endpoint{Azure: &config.AzureConfig{ConnectionString: "AccountName=a;BlobEndpoint=http://127.0.0.1:20000/a;"}}, "http://127.0.0.1:20000/a"},
	} {
		if got := endpointURL(tc.ep); got != tc.want {
			t.Errorf("endpointURL = %q, want %q", got, tc.want)
		}
	}
}

func TestWriteTable(t *testing.T) {
	rep := Report{Results: []Result{
		{Check: CheckDatabase, Status: StatusOK, Detail: "PostgreSQL 17"},
		{Pipeline: "p", Check: CheckDestList, Status: StatusFail, Detail: "List: HTTP 404 NoSuchBucket", Fix: "line1\nline2"},
		{Pipeline: "p", Check: CheckEvents, Status: StatusWarn, Detail: "none", Fix: "use a queue"},
	}}
	var b bytes.Buffer
	if err := WriteTable(&b, rep); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"CHECK", "RESULT", "DETAIL", "Pipeline p", "FIX [FAIL] destination list: line1\n      line2",
		"FIX [warn] events: use a queue", "FAIL: 1 check(s) failed, 1 warning(s)"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}
