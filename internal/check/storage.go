package check

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
)

// multipartProbeSize is one part of the multipart probe: the S3 minimum
// non-final part size, so the probe exercises a realistic part.
const multipartProbeSize = 5 << 20

func newConnector(ctx context.Context, ep config.Endpoint) (connector.Connector, error) {
	switch ep.Provider() {
	case "azure":
		return azure.New(ctx, *ep.Azure, ep.Prefix)
	case "s3":
		return s3.New(ctx, *ep.S3, ep.Prefix)
	}
	return nil, errors.New("no provider configured")
}

func prefixLabel(p string) string {
	if p == "" {
		return "the whole container"
	}
	return "prefix " + p
}

// sourceList lists one object. Proves the container exists and the
// identity may list it.
func sourceList(ctx context.Context, src connector.Connector, p config.Pipeline, t Target) (Result, *connector.ObjectInfo) {
	page, err := src.List(ctx, "", "", 1)
	if err != nil {
		return failRes(err, t), nil
	}
	if len(page.Objects) == 0 {
		return warn("listed OK, but "+prefixLabel(p.Source.Prefix)+" is empty",
			"Nothing to copy yet. If files should be there, check source.prefix."), nil
	}
	first := page.Objects[0]
	return ok(fmt.Sprintf("listed OK under %s (first: %s)", prefixLabel(p.Source.Prefix), first.Key)), &first
}

// sourceRead stats obj and reads its first byte. Never writes.
func sourceRead(ctx context.Context, src connector.Connector, obj connector.ObjectInfo, t Target) Result {
	info, err := src.Stat(ctx, obj.Key)
	if err != nil {
		return failRes(err, t)
	}
	if info.Size == 0 {
		return ok(fmt.Sprintf("stat %s OK (empty object, nothing to read)", obj.Key))
	}
	rc, err := src.OpenRange(ctx, obj.Key, info.Version, 0, 1)
	if err != nil {
		return failRes(err, t)
	}
	defer rc.Close()
	n, err := io.Copy(io.Discard, rc)
	if err != nil {
		return failRes(err, t)
	}
	if n != 1 {
		return Result{Status: StatusFail, Detail: fmt.Sprintf("range read of %s returned %d bytes, want 1", obj.Key, n),
			Fix: "The provider ignored the Range header; report this with the provider name."}
	}
	return ok(fmt.Sprintf("stat + 1-byte read of %s OK (%d bytes, version %s)", obj.Key, info.Size, info.Version))
}

func destList(ctx context.Context, dst connector.Connector) error {
	_, err := dst.List(ctx, "", "", 1)
	return err
}

func probeKey() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return ProbePrefix + hex.EncodeToString(b)
}

// cleanup deletes a probe object. A failure is fatal only when the pipeline
// propagates deletes (it will need delete permission); otherwise the
// leftover object is reported as a warning.
func cleanup(ctx context.Context, dst connector.Connector, key string, t Target, okDetail string) Result {
	err := dst.Delete(ctx, key)
	if err == nil {
		return ok(okDetail)
	}
	detail := fmt.Sprintf("%s, but deleting %s failed: %s (remove it by hand)", okDetail, key, describeErr(err))
	if t.Deletes {
		return Result{Status: StatusFail, Detail: detail, Fix: Hint(err, t)}
	}
	return warn(detail, "Delete permission is only needed with `deletes: true`. "+Hint(err, t))
}

// destWrite writes a probe object the way the engine does (single PUT with
// the SHA-256 metadata), reads it back, compares, and deletes it.
func destWrite(ctx context.Context, dst connector.Connector, t Target) Result {
	key := probeKey()
	body := []byte("portage check probe " + key + "\n")
	sum := sha256.Sum256(body)
	wr, err := dst.PutObject(ctx, key, bytes.NewReader(body), int64(len(body)), connector.WriteOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{connector.MetaSHA256: hex.EncodeToString(sum[:])},
	})
	if err != nil {
		return failRes(err, t)
	}
	var notes []string
	info, err := dst.Stat(ctx, key)
	if err != nil {
		_ = dst.Delete(ctx, key)
		return failRes(fmt.Errorf("stat after write: %w", err), t)
	}
	if info.Version != wr.Version {
		notes = append(notes, fmt.Sprintf("version from write %q != stat %q", wr.Version, info.Version))
	}
	if info.Metadata[connector.MetaSHA256] != hex.EncodeToString(sum[:]) {
		notes = append(notes, "user metadata "+connector.MetaSHA256+" was not preserved")
	}
	rc, err := dst.OpenRange(ctx, key, info.Version, 0, -1)
	if err != nil {
		_ = dst.Delete(ctx, key)
		return failRes(fmt.Errorf("read back: %w", err), t)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		_ = dst.Delete(ctx, key)
		return failRes(fmt.Errorf("read back: %w", err), t)
	}
	if !bytes.Equal(got, body) {
		_ = dst.Delete(ctx, key)
		return Result{Status: StatusFail, Detail: fmt.Sprintf("read back %d bytes that differ from the %d written", len(got), len(body)),
			Fix: "The destination returned different content than was written. Check for a proxy or gateway rewriting objects."}
	}
	res := cleanup(ctx, dst, key, t, "put + stat + read back + delete of "+key+" OK")
	if len(notes) > 0 && res.Status == StatusOK {
		res = warn(res.Detail+"; but "+strings.Join(notes, "; "),
			"Verification relies on stable versions and metadata; check the S3 flavor (oci/generic) matches the provider.")
	}
	return res
}

// destMultipart runs BeginUpload, one 5 MiB part (sent with Content-MD5),
// Complete, Stat and Delete: the path every large file takes.
func destMultipart(ctx context.Context, dst connector.Connector, t Target) Result {
	key := probeKey() + "-multipart"
	up, err := dst.BeginUpload(ctx, key, connector.WriteOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return failRes(fmt.Errorf("begin upload: %w", err), t)
	}
	abort := func() {
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = up.Abort(actx)
	}
	data := make([]byte, multipartProbeSize)
	_, _ = rand.Read(data)
	part, err := up.UploadPart(ctx, 1, data)
	if err != nil {
		abort()
		return failRes(fmt.Errorf("upload part: %w", err), t)
	}
	if _, err := up.Complete(ctx, []connector.Part{part}); err != nil {
		abort()
		return failRes(fmt.Errorf("complete: %w", err), t)
	}
	info, err := dst.Stat(ctx, key)
	if err != nil {
		_ = dst.Delete(ctx, key)
		return failRes(fmt.Errorf("stat after complete: %w", err), t)
	}
	if info.Size != multipartProbeSize {
		_ = dst.Delete(ctx, key)
		return Result{Status: StatusFail, Detail: fmt.Sprintf("completed object is %d bytes, want %d", info.Size, multipartProbeSize),
			Fix: "The provider assembled the multipart upload incorrectly; check the S3 flavor matches the provider."}
	}
	return cleanup(ctx, dst, key, t, "begin + 5 MiB part + complete + delete OK")
}
