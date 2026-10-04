package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

func TestNormalizePrefix(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "/": "", "a": "a/", "a/": "a/", "/a/b": "a/b/", "a/b/": "a/b/",
	} {
		if got := normalizePrefix(in); got != want {
			t.Errorf("normalizePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrefixMapping(t *testing.T) {
	c := &Connector{prefix: normalizePrefix("root/x")}
	if got := c.full("a/b"); got != "root/x/a/b" {
		t.Fatalf("full = %q", got)
	}
	if k, ok := relativeKey(c.prefix, "root/x/a/b"); !ok || k != "a/b" {
		t.Fatalf("relativeKey = %q %v", k, ok)
	}
	if _, ok := relativeKey(c.prefix, "root/y/a"); ok {
		t.Fatal("relativeKey accepted a key outside the prefix")
	}
	if k, ok := relativeKey("", "a"); !ok || k != "a" {
		t.Fatalf("relativeKey empty prefix = %q %v", k, ok)
	}
}

func TestETagAndVersion(t *testing.T) {
	cases := map[string]string{
		`"abc"`: "abc", `abc`: "abc", `W/"abc"`: "abc", ` "x-2" `: "x-2", `""`: "", "": "",
	}
	for in, want := range cases {
		if got := normalizeETag(in); got != want {
			t.Errorf("normalizeETag(%q) = %q, want %q", in, got, want)
		}
	}
	if got := quoteETag("abc"); got != `"abc"` {
		t.Errorf("quoteETag = %s", got)
	}
	if got := quoteETag(`"abc"`); got != `"abc"` {
		t.Errorf("quoteETag already quoted = %s", got)
	}
	if got := makeVersion("", "e1"); got != "e1" {
		t.Errorf("makeVersion no vid = %q", got)
	}
	if got := makeVersion("null", "e1"); got != "e1" {
		t.Errorf("makeVersion null vid = %q", got)
	}
	v := makeVersion("3HL4kqtJ", "e1")
	if v != "vid:3HL4kqtJ" {
		t.Errorf("makeVersion vid = %q", v)
	}
	if vid, etag := splitVersion(v); vid != "3HL4kqtJ" || etag != "" {
		t.Errorf("splitVersion(%q) = %q %q", v, vid, etag)
	}
	if vid, etag := splitVersion(`"e1"`); vid != "" || etag != "e1" {
		t.Errorf("splitVersion etag = %q %q", vid, etag)
	}
}

func TestMD5FromETag(t *testing.T) {
	if b := md5FromETag("9e107d9d372bb6826bd81d3542a419d6"); len(b) != 16 {
		t.Errorf("single-part etag: %x", b)
	}
	for _, e := range []string{"9e107d9d372bb6826bd81d3542a419d6-3", "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "zz", ""} {
		if b := md5FromETag(e); b != nil {
			t.Errorf("md5FromETag(%q) = %x, want nil", e, b)
		}
	}
}

func TestRangeHeader(t *testing.T) {
	cases := []struct {
		off, n int64
		want   string
	}{
		{0, -1, ""}, {10, -1, "bytes=10-"}, {0, 1, "bytes=0-0"}, {1000, 5000, "bytes=1000-5999"},
	}
	for _, c := range cases {
		if got := rangeHeader(c.off, c.n); got != c.want {
			t.Errorf("rangeHeader(%d,%d) = %q, want %q", c.off, c.n, got, c.want)
		}
	}
}

// sdkError builds an error shaped like what the SDK returns for an HTTP error
// response: an OperationError wrapping a ResponseError wrapping an APIError.
func sdkError(status int, code string) error {
	var inner error = &smithy.GenericAPIError{Code: code, Message: "test"}
	if code == "" {
		inner = errors.New("http error without body")
	}
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "HeadObject",
		Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
				Err:      inner,
			},
			RequestID: "req",
		},
	}
}

func TestMapError(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{404, "NoSuchKey", connector.ErrNotFound},
		{404, "NotFound", connector.ErrNotFound},
		{404, "", connector.ErrNotFound},
		{404, "NoSuchUpload", connector.ErrNotFound},
		{404, "NoSuchBucket", nil},
		{412, "PreconditionFailed", connector.ErrVersionChanged},
		{412, "", connector.ErrVersionChanged},
		{503, "SlowDown", connector.ErrThrottled},
		{503, "", connector.ErrThrottled},
		{429, "", connector.ErrThrottled},
		{400, "RequestLimitExceeded", connector.ErrThrottled},
		{400, "Throttling", connector.ErrThrottled},
		{403, "AccessDenied", connector.ErrPermission},
		{403, "", connector.ErrPermission},
		{403, "InvalidAccessKeyId", connector.ErrAuth},
		{403, "SignatureDoesNotMatch", connector.ErrAuth},
		{400, "ExpiredToken", connector.ErrAuth},
		{401, "", connector.ErrAuth},
		{500, "InternalError", nil},
	}
	all := []error{connector.ErrNotFound, connector.ErrVersionChanged, connector.ErrThrottled, connector.ErrPermission, connector.ErrAuth}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%d_%s", c.status, c.code), func(t *testing.T) {
			raw := sdkError(c.status, c.code)
			err := mapError("Stat", "k", raw)
			var pe *connector.ProviderError
			if !errors.As(err, &pe) {
				t.Fatalf("not a ProviderError: %T", err)
			}
			if pe.Status != c.status || pe.Code != c.code || pe.Op != "Stat" || pe.Key != "k" {
				t.Errorf("ProviderError = %+v", pe)
			}
			if !errors.Is(err, raw) {
				t.Error("underlying SDK error not unwrappable")
			}
			for _, s := range all {
				if got := errors.Is(err, s); got != (s == c.want) {
					t.Errorf("errors.Is(%v) = %v", s, got)
				}
			}
			if c.want == nil && !connector.IsRetryable(err) {
				t.Error("unmapped error should be retryable")
			}
		})
	}
}

func TestMapErrorContext(t *testing.T) {
	err := mapError("List", "", fmt.Errorf("op: %w", context.Canceled))
	if !errors.Is(err, context.Canceled) || connector.IsRetryable(err) {
		t.Fatalf("canceled: %v retryable=%v", err, connector.IsRetryable(err))
	}
	if mapError("x", "", nil) != nil {
		t.Fatal("nil error mapped to non-nil")
	}
}

func TestMapReadErrorPinned(t *testing.T) {
	err := mapReadError("OpenRange", "k", true, sdkError(404, "NoSuchVersion"))
	if !errors.Is(err, connector.ErrVersionChanged) || errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("pinned missing version: %v", err)
	}
	err = mapReadError("OpenRange", "k", true, sdkError(400, "InvalidArgument"))
	if !errors.Is(err, connector.ErrVersionChanged) {
		t.Fatalf("pinned invalid version id: %v", err)
	}
	err = mapReadError("OpenRange", "k", false, sdkError(404, "NoSuchKey"))
	if !errors.Is(err, connector.ErrNotFound) {
		t.Fatalf("unpinned missing: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	ctx := context.Background()
	base := config.S3Config{Bucket: "b", Region: "us-east-1", AccessKeyID: "a", SecretAccessKey: "s"}
	if _, err := New(ctx, config.S3Config{Region: "r"}, ""); err == nil {
		t.Error("missing bucket accepted")
	}
	if _, err := New(ctx, config.S3Config{Bucket: "b"}, ""); err == nil {
		t.Error("missing region accepted")
	}
	bad := base
	bad.Flavor = "azure"
	if _, err := New(ctx, bad, ""); err == nil {
		t.Error("unknown flavor accepted")
	}
	for _, f := range []string{"", FlavorAWS, FlavorOCI, FlavorGeneric} {
		cfg := base
		cfg.Flavor = f
		cfg.Endpoint = "https://ns.compat.objectstorage.eu-frankfurt-1.oraclecloud.com"
		c, err := New(ctx, cfg, "p")
		if err != nil {
			t.Fatalf("flavor %q: %v", f, err)
		}
		o := c.client.Options()
		wantPath := f == FlavorOCI
		if o.UsePathStyle != wantPath {
			t.Errorf("flavor %q: UsePathStyle = %v", f, o.UsePathStyle)
		}
		if f == FlavorOCI || f == FlavorGeneric {
			if o.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired || o.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
				t.Errorf("flavor %q: checksums not when_required: %v %v", f, o.RequestChecksumCalculation, o.ResponseChecksumValidation)
			}
		} else if o.RequestChecksumCalculation == aws.RequestChecksumCalculationWhenRequired {
			t.Errorf("flavor %q: checksum default changed", f)
		}
		if got := o.Retryer.MaxAttempts(); got != maxAttempts {
			t.Errorf("flavor %q: MaxAttempts = %d", f, got)
		}
		if c.prefix != "p/" || c.Name() != "s3" {
			t.Errorf("prefix %q name %q", c.prefix, c.Name())
		}
	}
}
