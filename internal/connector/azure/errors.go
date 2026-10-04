package azure

import (
	"errors"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/sunny36/portage/internal/connector"
)

// wrap converts an SDK error into a *connector.ProviderError carrying the
// matching sentinel. A nil err returns nil.
func wrap(op, key string, err error) error {
	if err == nil {
		return nil
	}
	pe := &connector.ProviderError{Op: op, Key: key, Err: err}
	var re *azcore.ResponseError
	var authErr *azidentity.AuthenticationFailedError
	var authReq *azidentity.AuthenticationRequiredError
	switch {
	case errors.As(err, &re):
		pe.Status = re.StatusCode
		pe.Code = re.ErrorCode
		pe.Sentinel = sentinel(re.StatusCode, re.ErrorCode)
	case errors.As(err, &authErr), errors.As(err, &authReq):
		// Token acquisition failed before any storage request was sent.
		pe.Sentinel = connector.ErrAuth
	}
	return pe
}

// sentinel maps an HTTP status and Azure Storage error code to a connector
// sentinel, or nil when none applies. Codes take precedence where Azure's
// status is ambiguous (e.g. a bad signature is 403 AuthenticationFailed).
func sentinel(status int, code string) error {
	switch code {
	case "BlobNotFound", "ContainerNotFound", "ResourceNotFound":
		return connector.ErrNotFound
	case "ConditionNotMet", "TargetConditionNotMet", "SourceConditionNotMet",
		"BlobAlreadyExists": // If-None-Match: * on an existing blob (409)
		return connector.ErrVersionChanged
	case "ServerBusy", "OperationTimedOut", "TooManyRequests":
		return connector.ErrThrottled
	case "AuthenticationFailed", "InvalidAuthenticationInfo", "NoAuthenticationInformation":
		return connector.ErrAuth
	case "AuthorizationFailure", "AuthorizationPermissionMismatch",
		"AuthorizationSourceIPMismatch", "AuthorizationProtocolMismatch",
		"AuthorizationResourceTypeMismatch", "AuthorizationServiceMismatch",
		"InsufficientAccountPermissions":
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
