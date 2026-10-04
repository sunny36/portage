package azure

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

func TestBlockIDRoundTrip(t *testing.T) {
	s, err := newSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if !validSessionID(s) {
		t.Fatalf("invalid session %q", s)
	}
	var wantLen int
	for _, n := range []int{1, 2, 99, 12345, maxParts} {
		id := blockID(s, n)
		if wantLen == 0 {
			wantLen = len(id)
		}
		if len(id) != wantLen {
			t.Fatalf("block IDs differ in length: %d vs %d", len(id), wantLen)
		}
		if strings.Contains(id, "=") {
			t.Errorf("block ID %q has padding", id)
		}
		gs, gn, ok := parseBlockID(id)
		if !ok || gs != s || gn != n {
			t.Fatalf("parseBlockID(%q) = %q,%d,%v; want %q,%d", id, gs, gn, ok, s, n)
		}
	}
	if blockID("0123456789abcdef", 1) != base64.StdEncoding.EncodeToString([]byte("0123456789abcdef-0000001")) {
		t.Fatal("block ID format changed; existing sessions would not resume")
	}
}

func TestParseBlockIDRejectsForeign(t *testing.T) {
	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	for _, id := range []string{
		"", "not base64!",
		enc("short"),
		enc("ZZZZZZZZZZZZZZZZ-0000001"), // non-hex session
		enc("0123456789abcdef_0000001"), // wrong separator
		enc("0123456789abcdef-0000000"), // part 0
		enc("0123456789abcdef-00000x1"),
	} {
		if _, _, ok := parseBlockID(id); ok {
			t.Errorf("parseBlockID(%q) accepted", id)
		}
	}
}

func TestValidSessionID(t *testing.T) {
	for s, want := range map[string]bool{
		"0123456789abcdef": true, "0123456789ABCDEF": false, "0123": false, "": false, "0123456789abcdeg": false,
	} {
		if got := validSessionID(s); got != want {
			t.Errorf("validSessionID(%q) = %v", s, got)
		}
	}
}

func TestCommitList(t *testing.T) {
	u := &upload{session: "0123456789abcdef", key: "k"}
	ids, err := u.commitList([]connector.Part{{Number: 3}, {Number: 1, Token: blockID(u.session, 1)}, {Number: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids = %v", ids)
	}
	for i, id := range ids {
		if id != blockID(u.session, i+1) {
			t.Errorf("ids[%d] = %q", i, id)
		}
	}
	if _, err := u.commitList([]connector.Part{{Number: 1}, {Number: 1}}); err == nil {
		t.Error("duplicate part accepted")
	}
	if _, err := u.commitList([]connector.Part{{Number: 1, Token: blockID("fedcba9876543210", 1)}}); err == nil {
		t.Error("foreign token accepted")
	}
	if _, err := u.commitList([]connector.Part{{Number: 0}}); err == nil {
		t.Error("part 0 accepted")
	}
}

func TestKeyPrefixMapping(t *testing.T) {
	for _, prefix := range []string{"", "scope/"} {
		c := &Connector{prefix: prefix}
		key := "dir/sub dir/ファイル.bin"
		if got := c.blobName(key); got != prefix+key {
			t.Errorf("blobName = %q", got)
		}
		if got := c.relKey(c.blobName(key)); got != key {
			t.Errorf("relKey round trip = %q", got)
		}
	}
	if got := containerURL("http://h:1/acct/", "c"); got != "http://h:1/acct/c" {
		t.Errorf("containerURL = %q", got)
	}
}

func TestNormMetaAndChecksums(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	m := normMeta(map[string]*string{"Portagesha256": to.Ptr(sha), "Other": to.Ptr("x"), "nil": nil})
	if m[connector.MetaSHA256] != sha || m["other"] != "x" || len(m) != 2 {
		t.Fatalf("normMeta = %v", m)
	}
	cs := checksums(make([]byte, 16), m)
	if len(cs.MD5) != 16 || len(cs.SHA256) != 32 || cs.SHA256[0] != 0xab {
		t.Fatalf("checksums = %+v", cs)
	}
	cs = checksums([]byte{1}, map[string]string{connector.MetaSHA256: "zz"})
	if cs.MD5 != nil || cs.SHA256 != nil {
		t.Fatalf("bad digests not ignored: %+v", cs)
	}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{404, "BlobNotFound", connector.ErrNotFound},
		{404, "ContainerNotFound", connector.ErrNotFound},
		{404, "", connector.ErrNotFound},
		{412, "ConditionNotMet", connector.ErrVersionChanged},
		{412, "", connector.ErrVersionChanged},
		{409, "BlobAlreadyExists", connector.ErrVersionChanged},
		{503, "ServerBusy", connector.ErrThrottled},
		{500, "OperationTimedOut", connector.ErrThrottled},
		{429, "", connector.ErrThrottled},
		{503, "", connector.ErrThrottled},
		{403, "AuthorizationPermissionMismatch", connector.ErrPermission},
		{403, "", connector.ErrPermission},
		{403, "AuthenticationFailed", connector.ErrAuth},
		{401, "", connector.ErrAuth},
		{500, "InternalError", nil},
		{400, "InvalidBlockList", nil},
	}
	sentinels := []error{connector.ErrNotFound, connector.ErrVersionChanged, connector.ErrThrottled, connector.ErrPermission, connector.ErrAuth}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d_%s", tc.status, tc.code), func(t *testing.T) {
			re := &azcore.ResponseError{StatusCode: tc.status, ErrorCode: tc.code, RawResponse: &http.Response{StatusCode: tc.status}}
			err := wrap("Op", "key", re)
			var pe *connector.ProviderError
			if !errors.As(err, &pe) || pe.Status != tc.status || pe.Code != tc.code || pe.Op != "Op" || pe.Key != "key" {
				t.Fatalf("wrap = %#v", err)
			}
			for _, s := range sentinels {
				if got := errors.Is(err, s); got != errors.Is(s, tc.want) {
					t.Errorf("errors.Is(%v) = %v", s, got)
				}
			}
			var re2 *azcore.ResponseError
			if !errors.As(err, &re2) {
				t.Error("underlying ResponseError lost")
			}
		})
	}
	if wrap("Op", "k", nil) != nil {
		t.Error("wrap(nil) != nil")
	}
	if err := wrap("Op", "k", context.Canceled); !errors.Is(err, context.Canceled) || connector.IsRetryable(err) {
		t.Errorf("context.Canceled mapping: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	ctx := context.Background()
	bad := []config.AzureConfig{
		{},
		{Container: "c", Auth: "nope", AccountURL: "http://x"},
		{Container: "c", Auth: "shared_key", AccountURL: "http://x"},
		{Container: "c", Auth: "connection_string"},
		{Container: "c", Auth: "default"},
	}
	for _, cfg := range bad {
		if _, err := New(ctx, cfg, ""); err == nil {
			t.Errorf("New(%+v) accepted", cfg)
		}
	}
	c, err := New(ctx, config.AzureConfig{Container: "c", Auth: "shared_key", AccountURL: "http://127.0.0.1:1/devstoreaccount1",
		AccountName: "devstoreaccount1", AccountKey: base64.StdEncoding.EncodeToString([]byte("key"))}, "p/")
	if err != nil {
		t.Fatal(err)
	}
	if c.Name() != "azure" || c.Limits().MaxParts != 50_000 {
		t.Errorf("Name/Limits = %q %+v", c.Name(), c.Limits())
	}
}
