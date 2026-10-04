// Package s3 implements connector.Connector for Amazon S3 and S3-compatible
// object stores (OCI Object Storage's S3 compatibility API, SeaweedFS for
// local development, and other "generic" providers) using aws-sdk-go-v2.
//
// # Versions
//
// ObjectInfo.Version / WriteResult.Version is always the object's ETag
// (quotes stripped), from List, Stat, PutObject and Complete alike, even on
// versioned buckets. S3 version IDs are ignored: ListObjectsV2 does not
// return them, and the reconciler compares List versions with Stat-derived
// synced versions, so both must be the same kind of value. ETags change on
// every overwrite, which is all the engine needs. OpenRange enforces the
// version with If-Match (412 → ErrVersionChanged).
//
// # Flavors and provider quirks
//
// Flavor "aws" (default) uses SDK defaults. Flavors "oci" and "generic"
// disable features that S3-compatible providers reject:
//
//   - Default integrity protection. Since aws-sdk-go-v2 service/s3 v1.73
//     (Jan 2025) the SDK sends CRC32 request checksums on every upload and,
//     for unseekable bodies, aws-chunked "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
//     encoding with a trailing checksum, and validates response checksums.
//     OCI (and many other S3-compatibles) reject these requests or return
//     errors such as "The value of x-amz-content-sha256 header is invalid".
//     RequestChecksumCalculation and ResponseChecksumValidation are therefore
//     set to "when_required" for non-aws flavors.
//   - ETags are not treated as MD5 digests. OCI ETags are UUID-like opaque
//     strings and multipart ETags are never MD5s anywhere; Checksums.MD5 is
//     only filled from single-part, non-KMS, non-SSE-C ETags on flavor aws.
//
// OCI Object Storage, S3 compatibility API specifics:
//
//   - Endpoint: https://<namespace>.compat.objectstorage.<region>.oraclecloud.com
//     (namespace = the tenancy's Object Storage namespace).
//   - Region: the OCI region identifier (e.g. eu-frankfurt-1), used for SigV4
//     signing. It must match the region in the endpoint.
//   - Addressing: path-style is forced for flavor oci (virtual-hosted style
//     needs a different host layout and is not available everywhere).
//   - Credentials: "Customer Secret Keys" (access key + secret) of an OCI
//     user; there is no default-chain equivalent on OCI, so static
//     credentials are expected.
//   - Multipart: at most 10,000 parts, minimum part 5 MiB except the last
//     (rclone's native oracleobjectstorage backend uses the same values:
//     minChunkSize 5 MiB, maxUploadParts 10000). Limits stay at the S3
//     values (5 MiB / 5 GiB / 10,000 / 5 GiB single PUT), which OCI accepts.
//   - User metadata is stored as opc-meta-* and exposed via x-amz-meta-*;
//     keys come back lowercased, which is why Stat lowercases all keys.
//   - Not used by this connector and unsupported or partial on OCI: ACLs,
//     SSE-KMS headers (SSE-C only), object tagging, aws-chunked uploads.
//
// rclone has no OCI entry among its S3 providers (it uses a native OCI
// backend); for "Other"/SeaweedFS it sets list_version=1, force_path_style,
// list_url_encode=false, use_multipart_etag=false. This connector follows
// the latter three (path style is configured explicitly, no encoding-type=url
// is requested, multipart ETags are never used as digests) and uses
// ListObjectsV2, which OCI and SeaweedFS both support. If a provider without
// ListObjectsV2 must be supported, that is the first quirk to add.
//
// # Retries
//
// The SDK's standard retryer is capped at 3 attempts and its client-side
// retry-quota rate limiter is disabled, so throttling surfaces quickly as
// connector.ErrThrottled and the job-level backoff stays in control.
package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// Flavors accepted in config.S3Config.Flavor.
const (
	FlavorAWS     = "aws"
	FlavorOCI     = "oci"
	FlavorGeneric = "generic"
)

const (
	// maxAttempts caps the SDK retryer (first try included).
	maxAttempts = 3
	// maxListKeys is the S3 ListObjectsV2 page size ceiling.
	maxListKeys = 1000
)

var limits = connector.Limits{
	MinPartSize:  5 << 20,
	MaxPartSize:  5 << 30,
	MaxParts:     10000,
	SinglePutMax: 5 << 30,
}

// Connector is a connector.Connector for one S3 bucket, scoped to a prefix.
type Connector struct {
	client *s3sdk.Client
	bucket string
	prefix string // normalised: "" or ends with "/"
	flavor string
}

var _ connector.Connector = (*Connector)(nil)

// New returns a connector for cfg.Bucket with keys relative to prefix.
// Static credentials are used when cfg.AccessKeyID is set, otherwise the AWS
// default credential chain. No request is made.
func New(ctx context.Context, cfg config.S3Config, prefix string) (*Connector, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3: region is required")
	}
	flavor := cfg.Flavor
	if flavor == "" {
		flavor = FlavorAWS
	}
	switch flavor {
	case FlavorAWS, FlavorOCI, FlavorGeneric:
	default:
		return nil, fmt.Errorf("s3: unknown flavor %q (want aws, oci or generic)", cfg.Flavor)
	}
	client, err := newClient(ctx, cfg, flavor)
	if err != nil {
		return nil, err
	}
	return &Connector{client: client, bucket: cfg.Bucket, prefix: normalizePrefix(prefix), flavor: flavor}, nil
}

func newClient(ctx context.Context, cfg config.S3Config, flavor string) (*s3sdk.Client, error) {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		// Many parts and files are transferred in parallel to one host.
		tr.MaxIdleConnsPerHost = 128
	})
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
				o.RateLimiter = ratelimit.None
			})
		}),
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	if flavor != FlavorAWS {
		opts = append(opts,
			awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
			awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		)
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load aws config: %w", err)
	}
	return s3sdk.NewFromConfig(awsCfg, func(o *s3sdk.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle || flavor == FlavorOCI
		o.DisableLogOutputChecksumValidationSkipped = true
		if flavor != FlavorAWS {
			// Belt and braces: LoadDefaultConfig env/shared-config values
			// must not re-enable these for S3-compatible providers.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	}), nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return "s3" }

// Limits implements connector.Connector.
func (c *Connector) Limits() connector.Limits { return limits }

func (c *Connector) full(key string) string { return c.prefix + key }

// List implements connector.Connector.
func (c *Connector) List(ctx context.Context, prefix, cursor string, limit int) (connector.ListPage, error) {
	if limit <= 0 || limit > maxListKeys {
		limit = maxListKeys
	}
	in := &s3sdk.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(c.full(prefix)),
		MaxKeys: aws.Int32(int32(limit)),
	}
	if cursor != "" {
		in.ContinuationToken = aws.String(cursor)
	}
	out, err := c.client.ListObjectsV2(ctx, in)
	if err != nil {
		return connector.ListPage{}, mapError("List", prefix, err)
	}
	page := connector.ListPage{Objects: make([]connector.ObjectInfo, 0, len(out.Contents))}
	for _, o := range out.Contents {
		full := aws.ToString(o.Key)
		key, ok := relativeKey(c.prefix, full)
		if !ok || strings.HasSuffix(key, "/") {
			// Outside our scope (should not happen) or a folder marker.
			continue
		}
		page.Objects = append(page.Objects, connector.ObjectInfo{
			Key:     key,
			Size:    aws.ToInt64(o.Size),
			Version: normalizeETag(aws.ToString(o.ETag)),
			ModTime: aws.ToTime(o.LastModified),
		})
	}
	sort.Slice(page.Objects, func(i, j int) bool { return page.Objects[i].Key < page.Objects[j].Key })
	if aws.ToBool(out.IsTruncated) {
		page.NextCursor = aws.ToString(out.NextContinuationToken)
	}
	return page, nil
}

// Stat implements connector.Connector.
func (c *Connector) Stat(ctx context.Context, key string) (connector.ObjectInfo, error) {
	out, err := c.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.full(key)),
	})
	if err != nil {
		return connector.ObjectInfo{}, mapError("Stat", key, err)
	}
	md := lowerKeys(out.Metadata)
	etag := normalizeETag(aws.ToString(out.ETag))
	info := connector.ObjectInfo{
		Key:      key,
		Size:     aws.ToInt64(out.ContentLength),
		Version:  etag,
		ModTime:  aws.ToTime(out.LastModified),
		Metadata: md,
	}
	info.ContentType = aws.ToString(out.ContentType)
	if sum, err := hex.DecodeString(md[connector.MetaSHA256]); err == nil && len(sum) == 32 {
		info.Checksums.SHA256 = sum
	}
	if c.flavor == FlavorAWS && out.SSECustomerAlgorithm == nil &&
		out.ServerSideEncryption != types.ServerSideEncryptionAwsKms &&
		out.ServerSideEncryption != types.ServerSideEncryptionAwsKmsDsse {
		info.Checksums.MD5 = md5FromETag(etag)
	}
	return info, nil
}

// OpenRange implements connector.Connector.
func (c *Connector) OpenRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, fmt.Errorf("s3: OpenRange %q: negative offset %d", key, offset)
	}
	var ifMatch *string
	if etag := normalizeETag(version); etag != "" {
		ifMatch = aws.String(quoteETag(etag))
	}
	if length == 0 {
		// HTTP ranges cannot express zero bytes; check existence/version only.
		if _, err := c.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
			Bucket: aws.String(c.bucket), Key: aws.String(c.full(key)), IfMatch: ifMatch,
		}); err != nil {
			return nil, mapError("OpenRange", key, err)
		}
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	in := &s3sdk.GetObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(c.full(key)), IfMatch: ifMatch}
	rng := rangeHeader(offset, length)
	if rng != "" {
		in.Range = aws.String(rng)
	}
	out, err := c.client.GetObject(ctx, in)
	if err != nil {
		return nil, mapError("OpenRange", key, err)
	}
	if rng != "" && out.ContentRange == nil {
		// The server ignored Range and is sending the whole object.
		out.Body.Close()
		return nil, &connector.ProviderError{Op: "OpenRange", Key: key, Status: http.StatusOK,
			Err: fmt.Errorf("provider ignored Range %q", rng)}
	}
	return out.Body, nil
}

// PutObject implements connector.Connector.
func (c *Connector) PutObject(ctx context.Context, key string, data io.Reader, size int64, opts connector.WriteOptions) (connector.WriteResult, error) {
	if size > limits.SinglePutMax {
		return connector.WriteResult{}, fmt.Errorf("s3: PutObject %q: size %d exceeds single PUT max %d", key, size, limits.SinglePutMax)
	}
	if data == nil {
		data = bytes.NewReader(nil)
	}
	in := &s3sdk.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(c.full(key)),
		Body:          data,
		ContentLength: aws.Int64(size),
		Metadata:      opts.Metadata,
	}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	in.IfMatch, in.IfNoneMatch = conditions(opts)
	var optFns []func(*s3sdk.Options)
	if _, seekable := data.(io.Seeker); !seekable {
		// SigV4 needs the payload hash up front, which needs a seekable
		// body. Sign the payload as UNSIGNED instead of buffering it.
		optFns = append(optFns, s3sdk.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware))
	}
	out, err := c.client.PutObject(ctx, in, optFns...)
	if err != nil {
		return connector.WriteResult{}, mapError("PutObject", key, err)
	}
	etag := normalizeETag(aws.ToString(out.ETag))
	res := connector.WriteResult{Version: etag}
	if c.flavor == FlavorAWS && out.SSECustomerAlgorithm == nil &&
		out.ServerSideEncryption != types.ServerSideEncryptionAwsKms &&
		out.ServerSideEncryption != types.ServerSideEncryptionAwsKmsDsse {
		res.Checksums.MD5 = md5FromETag(etag)
	}
	return res, nil
}

// Delete implements connector.Connector.
func (c *Connector) Delete(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.full(key)),
	})
	if err != nil {
		err = mapError("Delete", key, err)
		if errors.Is(err, connector.ErrNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// conditions converts WriteOptions preconditions into S3 header values.
func conditions(opts connector.WriteOptions) (ifMatch, ifNoneMatch *string) {
	if opts.IfMatch != "" {
		ifMatch = aws.String(quoteETag(opts.IfMatch))
	}
	if opts.IfNoneMatch != "" {
		ifNoneMatch = aws.String(opts.IfNoneMatch)
	}
	return ifMatch, ifNoneMatch
}
