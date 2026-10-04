package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"

	"github.com/sunny36/portage/internal/connector"
)

// normalizePrefix turns a configured prefix into "" or "dir/sub/".
func normalizePrefix(p string) string {
	p = strings.TrimLeft(p, "/")
	if p != "" && !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

// relativeKey strips the connector prefix from a full object key.
func relativeKey(prefix, full string) (string, bool) {
	if !strings.HasPrefix(full, prefix) {
		return "", false
	}
	return full[len(prefix):], true
}

// normalizeETag strips surrounding quotes (and a weak validator prefix).
func normalizeETag(etag string) string {
	etag = strings.TrimSpace(etag)
	etag = strings.TrimPrefix(etag, "W/")
	if len(etag) >= 2 && strings.HasPrefix(etag, `"`) && strings.HasSuffix(etag, `"`) {
		etag = etag[1 : len(etag)-1]
	}
	return etag
}

// quoteETag returns the quoted form used in If-Match and CompletedPart.
func quoteETag(etag string) string {
	if etag == "" || etag == "*" {
		return etag
	}
	return `"` + normalizeETag(etag) + `"`
}

// md5FromETag returns the MD5 digest encoded in a single-part S3 ETag, or nil
// for multipart ("<hex>-<n>") and any non-hex ETag.
func md5FromETag(etag string) []byte {
	if len(etag) != 32 {
		return nil
	}
	b, err := hex.DecodeString(etag)
	if err != nil {
		return nil
	}
	return b
}

// rangeHeader builds an HTTP Range value; "" means the whole object.
func rangeHeader(offset, length int64) string {
	switch {
	case length < 0 && offset == 0:
		return ""
	case length < 0:
		return fmt.Sprintf("bytes=%d-", offset)
	default:
		return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}
}

func lowerKeys(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}

var (
	notFoundCodes = map[string]bool{
		"NoSuchKey": true, "NotFound": true, "NoSuchUpload": true, "NoSuchVersion": true,
	}
	versionChangedCodes = map[string]bool{"PreconditionFailed": true}
	throttledCodes      = map[string]bool{
		"SlowDown": true, "Throttling": true, "ThrottlingException": true,
		"ThrottledException": true, "RequestThrottled": true, "RequestThrottledException": true,
		"RequestLimitExceeded": true, "TooManyRequests": true, "TooManyRequestsException": true,
		"ServiceUnavailable": true, "ProvisionedThroughputExceededException": true,
		"BandwidthLimitExceeded": true,
	}
	permissionCodes = map[string]bool{
		"AccessDenied": true, "AllAccessDisabled": true, "AccountProblem": true, "Forbidden": true,
	}
	authCodes = map[string]bool{
		"InvalidAccessKeyId": true, "SignatureDoesNotMatch": true, "ExpiredToken": true,
		"InvalidToken": true, "TokenRefreshRequired": true, "MissingSecurityHeader": true,
		"InvalidSecurity": true, "AuthorizationHeaderMalformed": true, "Unauthorized": true,
	}
)

// classify maps an S3 error code and HTTP status to a connector sentinel.
// The code wins over the status because AWS answers bad credentials with 403
// (InvalidAccessKeyId, SignatureDoesNotMatch) and ExpiredToken with 400.
func classify(code string, status int) error {
	switch {
	case code == "NoSuchBucket":
		// Unmapped on purpose: ErrNotFound would read as "object deleted" and could propagate deletes.
		return nil
	case notFoundCodes[code]:
		return connector.ErrNotFound
	case versionChangedCodes[code]:
		return connector.ErrVersionChanged
	case throttledCodes[code]:
		return connector.ErrThrottled
	case authCodes[code]:
		return connector.ErrAuth
	case permissionCodes[code]:
		return connector.ErrPermission
	}
	switch status {
	case http.StatusNotFound:
		return connector.ErrNotFound
	case http.StatusPreconditionFailed:
		return connector.ErrVersionChanged
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return connector.ErrThrottled
	case http.StatusUnauthorized:
		return connector.ErrAuth
	case http.StatusForbidden:
		return connector.ErrPermission
	}
	return nil
}

// mapError wraps an SDK error in a connector.ProviderError.
func mapError(op, key string, err error) error {
	if err == nil {
		return nil
	}
	pe := &connector.ProviderError{Op: op, Key: key, Err: err}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return pe
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		pe.Code = apiErr.ErrorCode()
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		pe.Status = respErr.HTTPStatusCode()
	}
	pe.Sentinel = classify(pe.Code, pe.Status)
	return pe
}
