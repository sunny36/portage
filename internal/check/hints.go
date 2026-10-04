package check

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// Role is what an endpoint is used for in a pipeline. It decides which
// grants a permission hint asks for.
type Role string

// Roles a hint can be given for.
const (
	RoleSource      Role = "source"
	RoleDestination Role = "destination"
	RoleQueue       Role = "queue"
)

// Target describes what a failing check talked to, for Hint.
type Target struct {
	Role     Role
	Endpoint config.Endpoint // the pipeline's source for RoleQueue
	Events   config.Events   // only for RoleQueue
	Deletes  bool            // pipeline propagates deletes
}

// Hint returns a concrete next step for err, or "" if it has none. It looks
// at the connector sentinels first, then provider error codes the connectors
// leave unmapped (NoSuchBucket), then network-level failures.
func Hint(err error, t Target) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "Timed out. Check the endpoint host is reachable from this machine (firewall, NSG, proxy, " +
			"storage account network rules / private endpoints), then re-run with a larger --timeout."
	case isAzureCredentialError(err) || errors.Is(err, connector.ErrAuth):
		return authHint(t)
	case errors.Is(err, connector.ErrPermission):
		return permissionHint(err, t)
	case errors.Is(err, connector.ErrNotFound) || providerCode(err) == "NoSuchBucket" ||
		providerCode(err) == "QueueNotFound" || providerCode(err) == "BucketNotFound":
		return notFoundHint(t)
	case errors.Is(err, connector.ErrThrottled):
		return "The provider is throttling requests. This is usually transient: re-run in a minute."
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return fmt.Sprintf("Host %q does not resolve. Check the spelling of the account name / namespace / region in %s.",
			dnsErr.Name, urlField(t))
	}
	var certErr *x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &hostErr) {
		return "TLS certificate check failed. A proxy may be intercepting HTTPS, or the endpoint host is wrong " +
			"(for OCI use https://<namespace>.compat.objectstorage.<region>.oraclecloud.com)."
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return fmt.Sprintf("Network error reaching the endpoint (%s). Check %s and that outbound HTTPS (443) is allowed.",
			opErr.Op, urlField(t))
	}
	msg := err.Error()
	if t.Endpoint.S3 != nil && (strings.Contains(msg, "x-amz-content-sha256") ||
		strings.Contains(strings.ToLower(msg), "checksum")) && t.Endpoint.S3.Flavor == "aws" {
		return "The provider rejected the AWS SDK's default checksum headers. For OCI set `flavor: oci`, " +
			"for other S3-compatible stores `flavor: generic`."
	}
	return ""
}

func providerCode(err error) string {
	var pe *connector.ProviderError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// isAzureCredentialError reports a token that could not be obtained, before
// any storage request was made (DefaultAzureCredential found nothing usable,
// or the identity was rejected by Entra ID).
func isAzureCredentialError(err error) bool {
	var failed *azidentity.AuthenticationFailedError
	var required *azidentity.AuthenticationRequiredError
	if errors.As(err, &failed) || errors.As(err, &required) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "DefaultAzureCredential") || strings.Contains(msg, "ManagedIdentityCredential")
}

func urlField(t Target) string {
	switch {
	case t.Role == RoleQueue:
		return "events.queue_account_url"
	case t.Endpoint.Azure != nil:
		return string(t.Role) + ".azure.account_url"
	case t.Endpoint.S3 != nil:
		return string(t.Role) + ".s3.endpoint"
	}
	return "the endpoint URL"
}

func authHint(t Target) string {
	switch {
	case t.Endpoint.S3 != nil && t.Endpoint.S3.Flavor == "oci":
		return "OCI rejected the credentials. access_key_id / secret_access_key must be a Customer Secret Key pair " +
			"(Profile → Customer secret keys; the secret is shown only once, so create a new key if it was lost). " +
			"The key belongs to one user: that user must be in the group your bucket policy names. " +
			"Also check s3.region equals the region in the endpoint (ap-singapore-1) and the clock is within 15 min. " +
			"See docs/setup/oci.md."
	case t.Endpoint.S3 != nil:
		return "The S3 credentials were rejected (InvalidAccessKeyId / SignatureDoesNotMatch). Check access_key_id / " +
			"secret_access_key (or the AWS default chain when they are empty), that s3.region is the bucket's region, " +
			"and that the system clock is within 15 minutes."
	case t.Endpoint.Azure != nil:
		switch t.Endpoint.Azure.Auth {
		case "shared_key":
			return "Azure rejected the shared key. Copy account_name and key1 from " +
				"`az storage account keys list -g <rg> -n <account>`; also check the account allows shared key access."
		case "connection_string":
			return "Azure rejected the connection string. Copy it again from " +
				"`az storage account show-connection-string -g <rg> -n <account>`; " +
				"also check the account allows shared key access."
		default:
			return "auth: default could not get a token (DefaultAzureCredential). On an Azure VM assign a managed identity " +
				"(`az vm identity assign -g <rg> -n <vm>`; for a user-assigned identity also set AZURE_CLIENT_ID). " +
				"On a workstation run `az login` (and `az account set -s <subscription>`). A service principal uses " +
				"AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET. See docs/setup/azure.md."
		}
	}
	return "The credentials were rejected."
}

func permissionHint(err error, t Target) string {
	code := providerCode(err)
	if t.Endpoint.Azure != nil && (code == "AuthorizationFailure" || code == "AuthorizationSourceIPMismatch") {
		return "Azure refused the request by network rule (" + code + "). If the storage account has a firewall " +
			"or private endpoint, allow this machine's IP / VNet: `az storage account network-rule add`."
	}
	switch {
	case t.Role == RoleQueue:
		return "The identity cannot read the event queue. Grant \"Storage Queue Data Message Processor\" on the queue " +
			"(peek, receive, delete) and \"Storage Queue Data Contributor\" on the <queue>-poison queue: " +
			"`az role assignment create --assignee <principal-id> --role \"Storage Queue Data Message Processor\" " +
			"--scope <storage-account-id>/queueServices/default/queues/<queue>`. " +
			"Assignments can take up to 10 minutes to apply. deploy/azure/setup.sh does this."
	case t.Endpoint.Azure != nil && t.Role == RoleSource:
		return "The identity cannot read the source container. Grant \"Storage Blob Data Reader\" (data plane; the " +
			"Owner/Contributor roles are not enough with auth: default): `az role assignment create --assignee " +
			"<principal-id> --role \"Storage Blob Data Reader\" --scope <storage-account-id>/blobServices/default/" +
			"containers/<container>`. Assignments can take up to 10 minutes to apply."
	case t.Endpoint.Azure != nil:
		return "The identity cannot write the destination container. Grant \"Storage Blob Data Contributor\" on it."
	case t.Endpoint.S3 != nil && t.Endpoint.S3.Flavor == "oci":
		return "OCI IAM denied the request. The key's user must be in a group with policies in the bucket's " +
			"compartment, e.g.\n" +
			"    Allow group <group> to read buckets in compartment <compartment> where target.bucket.name='<bucket>'\n" +
			"    Allow group <group> to manage objects in compartment <compartment> where target.bucket.name='<bucket>'\n" +
			"(identity-domain tenancies: group '<domain>'/'<group>'). Policy changes can take a few minutes. " +
			"See docs/setup/oci.md."
	case t.Endpoint.S3 != nil:
		perms := "s3:ListBucket, s3:GetObject, s3:PutObject, s3:AbortMultipartUpload and s3:ListMultipartUploadParts"
		if t.Deletes {
			perms += ", s3:DeleteObject"
		}
		if t.Role == RoleSource {
			perms = "s3:ListBucket and s3:GetObject"
		}
		return "Access denied. The bucket policy / IAM policy must allow " + perms + " on the bucket."
	}
	return "Permission denied."
}

func notFoundHint(t Target) string {
	switch {
	case t.Role == RoleQueue:
		return fmt.Sprintf("Queue %q was not found at %s. Check events.queue_name and events.queue_account_url "+
			"(https://<account>.queue.core.windows.net), or create it with deploy/azure/setup.sh.",
			t.Events.QueueName, t.Events.QueueAccountURL)
	case t.Endpoint.Azure != nil:
		return fmt.Sprintf("Container %q was not found. Check %s.azure.container and that account_url "+
			"(https://<account>.blob.core.windows.net) names the right storage account.", t.Endpoint.Azure.Container, t.Role)
	case t.Endpoint.S3 != nil && t.Endpoint.S3.Flavor == "oci":
		return fmt.Sprintf("Bucket %q was not found. Check %s.s3.bucket, the namespace in the endpoint "+
			"(`oci os ns get`), and that the bucket is in the endpoint's region (buckets are regional: "+
			"https://<namespace>.compat.objectstorage.ap-singapore-1.oraclecloud.com with region ap-singapore-1). "+
			"A missing read-buckets policy can also look like this.", t.Endpoint.S3.Bucket, t.Role)
	case t.Endpoint.S3 != nil:
		return fmt.Sprintf("Bucket %q was not found. Check %s.s3.bucket, s3.region and s3.endpoint.",
			t.Endpoint.S3.Bucket, t.Role)
	}
	return "Not found."
}
