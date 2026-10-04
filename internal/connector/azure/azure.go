// Package azure implements connector.Connector for Azure Blob Storage (block
// blobs) using the official azblob SDK.
//
// Multipart uploads map to Put Block / Put Block List. An upload ID is a
// random session token; each part's block ID is the base64 encoding of the
// fixed-length string "<session>-<part number, zero padded>", so a session can
// be resumed from (key, uploadID) alone by any process. See
// docs/adr/0002-connector-and-file-record.md.
package azure

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// Azure block blob limits (service version 2019-12-12 and later).
const (
	minPartSize  = 1 << 20    // Azure accepts 1 byte; 1 MiB is a sensible floor.
	maxPartSize  = 4000 << 20 // 4000 MiB per block
	maxParts     = 50_000     // committed blocks per blob
	singlePutMax = 5000 << 20 // Put Blob
	listMax      = 5000       // service maximum page size
)

// Connector is an Azure Blob container scoped to a key prefix.
type Connector struct {
	client *container.Client
	prefix string
}

var _ connector.Connector = (*Connector)(nil)

// New builds a connector for cfg.Container. Keys are relative to prefix,
// which is prepended verbatim (pass "dir/" to scope to a directory).
func New(ctx context.Context, cfg config.AzureConfig, prefix string) (*Connector, error) {
	_ = ctx // client construction does no I/O
	if cfg.Container == "" {
		return nil, errors.New("azure: container is required")
	}
	var (
		cl  *container.Client
		err error
	)
	switch cfg.Auth {
	case "", "default":
		if cfg.AccountURL == "" {
			return nil, errors.New("azure: account_url is required for auth=default")
		}
		cred, cerr := azidentity.NewDefaultAzureCredential(nil)
		if cerr != nil {
			return nil, fmt.Errorf("azure: default credential: %w", cerr)
		}
		cl, err = container.NewClient(containerURL(cfg.AccountURL, cfg.Container), cred, nil)
	case "shared_key":
		if cfg.AccountURL == "" || cfg.AccountName == "" || cfg.AccountKey == "" {
			return nil, errors.New("azure: account_url, account_name and account_key are required for auth=shared_key")
		}
		cred, cerr := container.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
		if cerr != nil {
			return nil, fmt.Errorf("azure: shared key: %w", cerr)
		}
		cl, err = container.NewClientWithSharedKeyCredential(containerURL(cfg.AccountURL, cfg.Container), cred, nil)
	case "connection_string":
		if cfg.ConnectionString == "" {
			return nil, errors.New("azure: connection_string is required for auth=connection_string")
		}
		cl, err = container.NewClientFromConnectionString(cfg.ConnectionString, cfg.Container, nil)
	default:
		return nil, fmt.Errorf("azure: unknown auth %q (want default, shared_key or connection_string)", cfg.Auth)
	}
	if err != nil {
		return nil, fmt.Errorf("azure: client: %w", err)
	}
	return &Connector{client: cl, prefix: prefix}, nil
}

func containerURL(accountURL, name string) string {
	return strings.TrimRight(accountURL, "/") + "/" + name
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return "azure" }

// Limits implements connector.Connector.
func (c *Connector) Limits() connector.Limits {
	return connector.Limits{
		MinPartSize:  minPartSize,
		MaxPartSize:  maxPartSize,
		MaxParts:     maxParts,
		SinglePutMax: singlePutMax,
	}
}

func (c *Connector) blobName(key string) string { return c.prefix + key }

func (c *Connector) relKey(name string) string { return strings.TrimPrefix(name, c.prefix) }

func (c *Connector) blockBlob(key string) *blockblob.Client {
	return c.client.NewBlockBlobClient(c.blobName(key))
}

// List implements connector.Connector.
func (c *Connector) List(ctx context.Context, prefix, cursor string, limit int) (connector.ListPage, error) {
	opts := &container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Metadata: true},
		Prefix:  to.Ptr(c.blobName(prefix)),
	}
	if cursor != "" {
		opts.Marker = to.Ptr(cursor)
	}
	if limit > 0 {
		if limit > listMax {
			limit = listMax
		}
		opts.MaxResults = to.Ptr(int32(limit))
	}
	resp, err := c.client.NewListBlobsFlatPager(opts).NextPage(ctx)
	if err != nil {
		return connector.ListPage{}, wrap("List", prefix, err)
	}
	var page connector.ListPage
	if resp.Segment != nil {
		for _, it := range resp.Segment.BlobItems {
			if it == nil || it.Name == nil || it.Properties == nil {
				continue
			}
			p := it.Properties
			info := connector.ObjectInfo{
				Key:      c.relKey(*it.Name),
				Size:     deref(p.ContentLength),
				Version:  etag(p.ETag),
				ModTime:  derefTime(p.LastModified),
				Metadata: normMeta(it.Metadata),
			}
			info.Checksums = checksums(p.ContentMD5, info.Metadata)
			page.Objects = append(page.Objects, info)
		}
	}
	if resp.NextMarker != nil {
		page.NextCursor = *resp.NextMarker
	}
	return page, nil
}

// Stat implements connector.Connector.
func (c *Connector) Stat(ctx context.Context, key string) (connector.ObjectInfo, error) {
	resp, err := c.blockBlob(key).GetProperties(ctx, nil)
	if err != nil {
		return connector.ObjectInfo{}, wrap("Stat", key, err)
	}
	info := connector.ObjectInfo{
		Key:      key,
		Size:     deref(resp.ContentLength),
		Version:  etag(resp.ETag),
		ModTime:  derefTime(resp.LastModified),
		Metadata: normMeta(resp.Metadata),
	}
	if resp.ContentType != nil {
		info.ContentType = *resp.ContentType
	}
	info.Checksums = checksums(resp.ContentMD5, info.Metadata)
	return info, nil
}

// OpenRange implements connector.Connector.
func (c *Connector) OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error) {
	if length == 0 {
		// Azure treats Count 0 as "to the end"; a zero-length read still
		// verifies existence and version.
		if _, err := c.statVersion(ctx, key, version); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	opts := &blob.DownloadStreamOptions{}
	if offset > 0 || length > 0 {
		opts.Range = blob.HTTPRange{Offset: offset}
		if length > 0 {
			opts.Range.Count = length
		}
	}
	if version != "" {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: to.Ptr(azcore.ETag(version))},
		}
	}
	resp, err := c.blockBlob(key).DownloadStream(ctx, opts)
	if err != nil {
		return nil, wrap("OpenRange", key, err)
	}
	return resp.Body, nil
}

func (c *Connector) statVersion(ctx context.Context, key, version string) (blob.GetPropertiesResponse, error) {
	opts := &blob.GetPropertiesOptions{}
	if version != "" {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: to.Ptr(azcore.ETag(version))},
		}
	}
	resp, err := c.blockBlob(key).GetProperties(ctx, opts)
	if err != nil {
		return resp, wrap("OpenRange", key, err)
	}
	return resp, nil
}

// PutObject implements connector.Connector. If data is not an io.ReadSeeker
// it is buffered in memory (the SDK must be able to rewind the body to retry).
func (c *Connector) PutObject(ctx context.Context, key string, data io.Reader, size int64, opts connector.WriteOptions) (connector.WriteResult, error) {
	if size > singlePutMax {
		return connector.WriteResult{}, fmt.Errorf("azure: PutObject %q: size %d exceeds single put max %d", key, size, int64(singlePutMax))
	}
	body, err := sizedBody(data, size)
	if err != nil {
		return connector.WriteResult{}, fmt.Errorf("azure: PutObject %q: %w", key, err)
	}
	resp, err := c.blockBlob(key).Upload(ctx, body, &blockblob.UploadOptions{
		Metadata:         toAzMeta(opts.Metadata),
		HTTPHeaders:      httpHeaders(opts),
		AccessConditions: accessConditions(opts),
	})
	if err != nil {
		return connector.WriteResult{}, wrap("PutObject", key, err)
	}
	return connector.WriteResult{
		Version:   etag(resp.ETag),
		Checksums: checksums(resp.ContentMD5, opts.Metadata),
	}, nil
}

// sizedBody returns a rewindable body of exactly size bytes from data.
func sizedBody(data io.Reader, size int64) (io.ReadSeekCloser, error) {
	if data == nil {
		data = bytes.NewReader(nil)
	}
	if rs, ok := data.(io.ReadSeeker); ok {
		start, err := rs.Seek(0, io.SeekCurrent)
		if err == nil {
			if ra, ok := data.(io.ReaderAt); ok {
				return streaming.NopCloser(io.NewSectionReader(ra, start, size)), nil
			}
			if end, err := rs.Seek(0, io.SeekEnd); err == nil {
				if _, err := rs.Seek(start, io.SeekStart); err != nil {
					return nil, err
				}
				if end-start == size {
					return streaming.NopCloser(rs), nil
				}
			}
		}
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(data, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return streaming.NopCloser(bytes.NewReader(buf)), nil
}

// Delete implements connector.Connector.
func (c *Connector) Delete(ctx context.Context, key string) error {
	_, err := c.blockBlob(key).Delete(ctx, &blob.DeleteOptions{
		DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude),
	})
	if err != nil {
		werr := wrap("Delete", key, err)
		if errors.Is(werr, connector.ErrNotFound) {
			return nil
		}
		return werr
	}
	return nil
}

// --- helpers ---

func deref(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.UTC()
}

func etag(e *azcore.ETag) string {
	if e == nil {
		return ""
	}
	return string(*e)
}

// normMeta lowercases metadata keys: Azure metadata names are case-insensitive
// and HTTP header canonicalisation changes their case on the way back.
func normMeta(m map[string]*string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if v != nil {
			out[strings.ToLower(k)] = *v
		}
	}
	return out
}

func toAzMeta(m map[string]string) map[string]*string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = to.Ptr(v)
	}
	return out
}

func checksums(contentMD5 []byte, meta map[string]string) connector.Checksums {
	var cs connector.Checksums
	if len(contentMD5) == md5.Size {
		cs.MD5 = contentMD5
	}
	if h, ok := meta[connector.MetaSHA256]; ok {
		if b, err := hex.DecodeString(h); err == nil && len(b) == 32 {
			cs.SHA256 = b
		}
	}
	return cs
}

func httpHeaders(opts connector.WriteOptions) *blob.HTTPHeaders {
	if opts.ContentType == "" {
		return nil
	}
	return &blob.HTTPHeaders{BlobContentType: to.Ptr(opts.ContentType)}
}

func accessConditions(opts connector.WriteOptions) *blob.AccessConditions {
	if opts.IfMatch == "" && opts.IfNoneMatch == "" {
		return nil
	}
	mac := &blob.ModifiedAccessConditions{}
	if opts.IfMatch != "" {
		mac.IfMatch = to.Ptr(azcore.ETag(opts.IfMatch))
	}
	if opts.IfNoneMatch != "" {
		mac.IfNoneMatch = to.Ptr(azcore.ETag(opts.IfNoneMatch))
	}
	return &blob.AccessConditions{ModifiedAccessConditions: mac}
}
