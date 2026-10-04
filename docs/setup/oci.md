# OCI Object Storage as a destination (S3 Compatibility API)

Portage writes to OCI Object Storage through OCI's
[Amazon S3 Compatibility API](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm),
using the `s3` connector with `flavor: oci`. This guide sets up a bucket in
**Singapore (`ap-singapore-1`)**, a dedicated user with the minimum access,
and the credentials for `pipeline.yaml`.

You can do it in the Console (below) or run [`deploy/oci/setup.sh`](../../deploy/oci/setup.sh),
which does the same steps with the `oci` CLI.

## 1. Find your Object Storage namespace

Every tenancy has one namespace, a short random-looking string, and it is
part of the endpoint.

- Console: **Profile menu → Tenancy: <name>**, field **Object storage namespace**.
- CLI: `oci os ns get` → `{"data": "axabc123defg"}`.

## 2. Create the bucket

Buckets are regional. Create it in **ap-singapore-1** (Singapore). The other
Singapore region, `ap-singapore-2` (Singapore West), is a different region with
a different endpoint.

- Console: switch the region selector to **Singapore**, then **Storage → Object Storage &
  Archive Storage → Buckets → Create bucket**. Pick the compartment, keep
  **Standard** tier (it can't be changed later) and leave versioning off unless you want it.
- CLI:
  ```sh
  oci os bucket create --region ap-singapore-1 \
    --compartment-id <compartment-ocid> --name imports \
    --storage-tier Standard --public-access-type NoPublicAccess
  ```

> Buckets created *through the S3 API* land in the root compartment by
> default. Portage never creates buckets, so this doesn't matter here, but if you
> create buckets with S3 tools, see
> [designating compartments](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/designatingcompartments.htm).

## 3. A dedicated user, group and policy (recommended)

S3-compatible credentials ("Customer Secret Keys") belong to **one user** and
carry **all of that user's permissions**. Don't use your own admin user's key.
Create a user that can only touch this bucket.

1. **Group:** **Identity & Security → Domains → Default → Groups → Create group**:
   `portage-sync`.
2. **User:** **… → Users → Create user**: `portage-sync`. An email is required in
   identity-domain tenancies. Add it to the `portage-sync` group. It needs no
   Console password.
3. **Policy:** **Identity & Security → Policies**. Pick the bucket's compartment,
   then **Create policy** and switch to the manual editor:

   ```
   Allow group portage-sync to read buckets in compartment <compartment-name> where target.bucket.name='imports'
   Allow group portage-sync to manage objects in compartment <compartment-name> where target.bucket.name='imports'
   ```

   - `read buckets` lets the API find the bucket (HeadBucket / GetBucket). The
     `where` clause keeps the user from even listing other buckets.
   - `manage objects` covers what Portage does: list, read (verification), put,
     multipart create/upload/commit/abort, and delete (only used with
     `deletes: true`). The per-operation permissions are in the
     [Object Storage policy reference](https://docs.oracle.com/en-us/iaas/Content/Identity/policyreference/objectstoragepolicyreference.htm).
   - For a group in a domain other than **Default**, write the subject as
     `group '<domain-name>'/'portage-sync'`
     ([policy syntax](https://docs.oracle.com/en-us/iaas/Content/Identity/policysyntax/subject.htm)).
   - For a nested compartment use its path (`Parent:Child`), or write
     `in compartment id <compartment-ocid>`.
   - Policies usually take effect within seconds, sometimes a few minutes.

## 4. Create a Customer Secret Key

Do this **as the portage-sync user** (or as an admin, on that user's page):
**User → Customer secret keys → Generate secret key**, name it `portage`.

- Copy the **secret** right away. It is shown **only once**.
- The **Access key** is shown in the list afterwards. That pair is
  `access_key_id` / `secret_access_key`.
- Each user can have at most **two** keys, and keys don't expire. Rotate by creating the
  second key, switching Portage over, then deleting the old one.

CLI: `oci iam customer-secret-key create --user-id <user-ocid> --display-name portage`
(the output's `key` field is the secret and `id` is the access key ID).

Reference: [Working with Customer Secret Keys](https://docs.oracle.com/en-us/iaas/Content/Identity/access/working-with-customer-secret-keys.htm).

## 5. pipeline.yaml destination block

```yaml
    destination:
      prefix: from-azure/          # optional "folder" inside the bucket
      s3:
        bucket: imports
        region: ap-singapore-1     # must match the region in the endpoint (SigV4 signing)
        endpoint: https://<namespace>.compat.objectstorage.ap-singapore-1.oraclecloud.com
        path_style: true           # forced anyway for flavor oci
        flavor: oci                # disables AWS SDK checksum headers OCI rejects
        access_key_id: ${OCI_S3_ACCESS_KEY_ID}
        secret_access_key: ${OCI_S3_SECRET_ACCESS_KEY}
```

- **Endpoint:** `https://<namespace>.compat.objectstorage.ap-singapore-1.oraclecloud.com`.
  Oracle's current docs write the same endpoint as
  `https://<namespace>.compat.objectstorage.ap-singapore-1.oci.customer-oci.com`.
  Both names resolve to the same service, so either works. Use the namespace
  from step 1 in lowercase, exactly as `oci os ns get` prints it.
- **Path style:** the bucket goes in the URL path (`/imports/key`). OCI also
  offers virtual-hosted style on a different `vhcompat` host, which Portage doesn't use.
- **Region:** the OCI region identifier `ap-singapore-1`, not an AWS name.
- **Secrets:** keep them out of the file. `${VAR}` is expanded from the
  environment when the config loads (e.g. from a systemd `EnvironmentFile`).

## 6. Check it

```sh
export OCI_S3_ACCESS_KEY_ID=... OCI_S3_SECRET_ACCESS_KEY=...
portage check -c pipeline.yaml
```

`destination list / write / multipart` must all be `ok`. Common failures:

| `portage check` says | Usually means |
|---|---|
| `HTTP 403 SignatureDoesNotMatch` / `InvalidAccessKeyId` | Wrong key pair, a secret copied with a trailing space, or `region` doesn't match the endpoint |
| `HTTP 404 NoSuchBucket` | Bucket name, namespace or region wrong, or the bucket is in another region |
| `HTTP 403 AccessDenied` on write | The policy is missing, in the wrong compartment, or names another group, or the user isn't in the group |
| `x-amz-content-sha256 … invalid` | `flavor: oci` missing |
| clock skew FAIL | Fix NTP. Signatures are rejected beyond 15 minutes of skew |

Then run the opt-in conformance suite against the real bucket. See
[first-real-run.md](first-real-run.md#6a-real-oci-conformance-test).
