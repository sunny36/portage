// Package connector defines the storage interface every endpoint implements.
//
// One Connector reads and writes one container/bucket (optionally scoped to a
// prefix). Keys passed to and returned from a Connector are relative to that
// scope. The engine never touches provider SDKs directly; it only talks to
// this interface. See docs/adr/0002-connector-and-file-record.md.
package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// Sentinel errors. Implementations wrap provider errors so that callers can
// use errors.Is. Anything not matching one of these is treated as retryable
// by default up to the job's attempt limit.
var (
	// ErrNotFound: the key (or upload session) does not exist.
	ErrNotFound = errors.New("connector: not found")
	// ErrThrottled: the provider asked us to slow down (HTTP 429/503, SlowDown,
	// ServerBusy). Always retryable, with backoff.
	ErrThrottled = errors.New("connector: throttled")
	// ErrVersionChanged: a conditional read/write failed because the object's
	// version is no longer the one requested (HTTP 412). The copy must restart
	// from a fresh Stat.
	ErrVersionChanged = errors.New("connector: version changed")
	// ErrPermission: credentials are valid but lack access (HTTP 403). Not
	// retryable; must surface as an alert, never a silent stop.
	ErrPermission = errors.New("connector: permission denied")
	// ErrAuth: credentials are missing, expired or invalid (HTTP 401).
	ErrAuth = errors.New("connector: authentication failed")
)

// IsRetryable reports whether an operation that returned err may succeed if
// repeated unchanged.
func IsRetryable(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrPermission), errors.Is(err, ErrAuth),
		errors.Is(err, ErrNotFound), errors.Is(err, ErrVersionChanged):
		return false
	case errors.Is(err, context.Canceled):
		return false
	default:
		return true
	}
}

// ObjectInfo describes one object at one version.
type ObjectInfo struct {
	Key  string
	Size int64
	// Version is the object's ETag (quotes stripped) on every provider, even
	// on versioned buckets, so List, Stat and writes all report the same value
	// and OpenRange can use it with If-Match. Equal Version means identical
	// content. It is NOT orderable; use ModTime/Sequencer.
	Version string
	ModTime time.Time
	// Sequencer orders writes to the same key when the provider exposes it
	// (from change events). Empty when unknown, e.g. from List/Stat.
	Sequencer string
	// Checksums the provider already knows for this version, if any.
	Checksums Checksums
	// Metadata is user metadata. Portage writes MetaSHA256 on every object it
	// creates.
	Metadata map[string]string
}

// MetaSHA256 is the user-metadata key Portage stores the hex SHA-256 under on
// every destination object it writes. No hyphen or underscore: Azure requires
// C#-identifier names and some proxies drop underscored S3 headers.
const MetaSHA256 = "portagesha256"

// Checksums holds whatever digests are available. Nil slices mean unknown.
type Checksums struct {
	MD5    []byte // Azure Content-MD5, S3 single-part ETag
	SHA256 []byte // native SHA-256 or MetaSHA256 metadata
	// CRC64 / CRC32C etc. can be added when a connector needs them.
}

// ListPage is one page of a listing, ordered by Key ascending.
type ListPage struct {
	Objects []ObjectInfo
	// NextCursor is empty when the listing is complete.
	NextCursor string
}

// Limits describes the provider's multipart constraints. The transfer layer
// picks a part size within them.
type Limits struct {
	MinPartSize int64 // smallest allowed non-final part
	MaxPartSize int64
	MaxParts    int
	// SinglePutMax is the largest object that can be written with PutObject.
	SinglePutMax int64
}

// WriteOptions apply to PutObject and BeginUpload.
type WriteOptions struct {
	ContentType string
	Metadata    map[string]string // must include MetaSHA256 when known up front
	// IfNoneMatch "*" / IfMatch <version> for conditional writes, when the
	// provider supports them. Empty means unconditional.
	IfMatch     string
	IfNoneMatch string
}

// WriteResult is returned after an object is fully written.
type WriteResult struct {
	Version   string // destination version (ETag / version ID)
	Checksums Checksums
}

// Part is one uploaded part of a multipart upload.
type Part struct {
	Number int // 1-based
	Size   int64
	// ETag/block ID or whatever the provider needs to complete the upload.
	Token string
}

// Upload is an in-progress multipart upload. UploadPart may be called
// concurrently for different part numbers. Re-uploading the same part number
// replaces it (this is what makes retries safe).
type Upload interface {
	// ID identifies the session so it can be resumed after a crash. It is
	// persisted in the file record.
	ID() string
	UploadPart(ctx context.Context, number int, data []byte) (Part, error)
	// ListParts returns parts already uploaded in this session (for resume).
	ListParts(ctx context.Context) ([]Part, error)
	// Complete assembles the parts in Number order. metadata is applied to the
	// final object where the provider allows it at completion time.
	Complete(ctx context.Context, parts []Part) (WriteResult, error)
	Abort(ctx context.Context) error
}

// Connector is implemented once per provider (azure, s3, later gcs, sftp).
// All methods must be safe for concurrent use.
type Connector interface {
	// Name is a short identifier for logs/metrics, e.g. "azure", "s3".
	Name() string
	Limits() Limits

	// List returns objects under prefix (relative to the connector's scope),
	// one page at a time. Pass the previous NextCursor to continue; "" starts.
	// limit <= 0 means provider default.
	List(ctx context.Context, prefix, cursor string, limit int) (ListPage, error)
	// Stat returns the current version of key, or ErrNotFound.
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	// OpenRange reads [offset, offset+length) of key. If version is non-empty
	// the read is conditional on it (If-Match) and fails with
	// ErrVersionChanged if the object changed. length < 0 means to the end.
	OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error)

	// PutObject writes a whole object in one request (size <= SinglePutMax).
	PutObject(ctx context.Context, key string, data io.Reader, size int64, opts WriteOptions) (WriteResult, error)
	// BeginUpload starts a multipart upload.
	BeginUpload(ctx context.Context, key string, opts WriteOptions) (Upload, error)
	// ResumeUpload reopens a session started by BeginUpload, or returns
	// ErrNotFound if it expired or was completed/aborted.
	ResumeUpload(ctx context.Context, key, uploadID string, opts WriteOptions) (Upload, error)

	// Delete removes key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// ProviderError carries the provider's raw status for logs while still
// matching a sentinel via errors.Is.
type ProviderError struct {
	Op       string // e.g. "Stat", "UploadPart"
	Key      string
	Status   int    // HTTP status if any
	Code     string // provider error code, e.g. "SlowDown", "BlobNotFound"
	Sentinel error  // one of the Err* values, or nil
	Err      error  // underlying SDK error
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("%s %q: status=%d code=%s: %v", e.Op, e.Key, e.Status, e.Code, e.Err)
}

func (e *ProviderError) Unwrap() []error {
	if e.Sentinel != nil {
		return []error{e.Sentinel, e.Err}
	}
	return []error{e.Err}
}
