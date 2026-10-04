#!/usr/bin/env bash
# Portage: OCI Object Storage destination setup (S3 Compatibility API).
#
# Creates (or reuses) the destination bucket and a least-privilege identity
# for Portage, then prints the pipeline.yaml destination block:
#
#   bucket (Standard tier, private) in the region
#   group  portage-sync
#   policy in the bucket's compartment:
#     Allow group <group> to read buckets   in compartment id <c> where target.bucket.name='<bucket>'
#     Allow group <group> to manage objects in compartment id <c> where target.bucket.name='<bucket>'
#   optional (--create-user): user portage-sync in the group + a Customer
#     Secret Key (the S3 access key / secret pair)
#
# Idempotent except for the secret key: each run with --create-user makes a
# new key (a user may have at most two). Needs the `oci` CLI configured
# (`oci setup config`) as a user who may manage groups, users and policies
# (e.g. a tenancy administrator). Group/user commands act on the Default
# identity domain; for another domain, create the group and user in the
# Console and pass --group with the domain-qualified name for the policy.
#
# Usage:
#   deploy/oci/setup.sh --compartment-id <ocid> [options]
#
# Options (or the environment variable in brackets):
#   --compartment-id OCID   [COMPARTMENT_ID]  required: compartment for the bucket and policy
#   --bucket NAME           [BUCKET]          default: imports
#   --region NAME           [REGION]          default: ap-singapore-1
#   --group NAME            [GROUP]           default: portage-sync
#   --policy NAME           [POLICY]          default: portage-sync-<bucket>
#   --create-user           [CREATE_USER=1]   also create user <group> with a Customer Secret Key
#   --user-email EMAIL      [USER_EMAIL]      email for the user (required by identity-domain tenancies)
#   --tenancy-id OCID       [TENANCY_ID]      default: "tenancy" from ~/.oci/config ($OCI_CLI_PROFILE or DEFAULT)
#   -h, --help
set -euo pipefail

COMPARTMENT_ID=${COMPARTMENT_ID:-}
BUCKET=${BUCKET:-imports}
REGION=${REGION:-ap-singapore-1}
GROUP=${GROUP:-portage-sync}
POLICY=${POLICY:-}
CREATE_USER=${CREATE_USER:-0}
USER_EMAIL=${USER_EMAIL:-}
TENANCY_ID=${TENANCY_ID:-}

usage() { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; }
die() { echo "error: $*" >&2; exit 1; }
log() { printf '\n==> %s\n' "$*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compartment-id) COMPARTMENT_ID=$2; shift 2 ;;
    --bucket) BUCKET=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --group) GROUP=$2; shift 2 ;;
    --policy) POLICY=$2; shift 2 ;;
    --create-user) CREATE_USER=1; shift ;;
    --user-email) USER_EMAIL=$2; shift 2 ;;
    --tenancy-id) TENANCY_ID=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

[[ -n $COMPARTMENT_ID ]] || die "--compartment-id is required (see --help)"
[[ $COMPARTMENT_ID == ocid1.* ]] || die "--compartment-id must be an OCID (ocid1.compartment... or ocid1.tenancy... for root)"
[[ $BUCKET =~ ^[A-Za-z0-9._-]{1,256}$ ]] || die "bucket names use letters, digits, '.', '_' and '-'"
POLICY=${POLICY:-portage-sync-${BUCKET}}

command -v oci >/dev/null || die "the OCI CLI is not installed: https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm"

if [[ -z $TENANCY_ID ]]; then
  cfg=${OCI_CLI_CONFIG_FILE:-$HOME/.oci/config}
  profile=${OCI_CLI_PROFILE:-DEFAULT}
  [[ -r $cfg ]] || die "no OCI CLI config at $cfg: run 'oci setup config' or pass --tenancy-id"
  TENANCY_ID=$(awk -v p="[$profile]" '
    $0 == p { in_p = 1; next }
    /^\[/ { in_p = 0 }
    in_p && $1 ~ /^tenancy[ \t]*=?/ { sub(/^[^=]*=[ \t]*/, ""); print; exit }' "$cfg")
  [[ -n $TENANCY_ID ]] || die "no tenancy= in profile [$profile] of $cfg; pass --tenancy-id"
fi

# Every command targets the bucket's region; IAM calls are global and are
# routed to the home region by the service.
OCI=(oci --region "$REGION")

log "Object Storage namespace"
NAMESPACE=$("${OCI[@]}" os ns get --query data --raw-output) || die "oci os ns get failed: check 'oci setup config' and that you are subscribed to $REGION"
echo "$NAMESPACE"

log "Bucket $BUCKET ($REGION)"
if "${OCI[@]}" os bucket get --namespace "$NAMESPACE" --bucket-name "$BUCKET" >/dev/null 2>&1; then
  echo "exists"
else
  "${OCI[@]}" os bucket create --namespace "$NAMESPACE" --compartment-id "$COMPARTMENT_ID" --name "$BUCKET" \
    --storage-tier Standard --public-access-type NoPublicAccess >/dev/null
  echo "created (Standard tier, private, versioning disabled)"
fi
BUCKET_COMPARTMENT=$("${OCI[@]}" os bucket get --namespace "$NAMESPACE" --bucket-name "$BUCKET" --query 'data."compartment-id"' --raw-output)
if [[ $BUCKET_COMPARTMENT != "$COMPARTMENT_ID" ]]; then
  echo "note: bucket $BUCKET is in compartment $BUCKET_COMPARTMENT; the policy will target that compartment"
  COMPARTMENT_ID=$BUCKET_COMPARTMENT
fi

log "Group $GROUP"
GROUP_ID=$("${OCI[@]}" iam group list --compartment-id "$TENANCY_ID" --name "$GROUP" --all --query 'data[0].id' --raw-output 2>/dev/null || true)
if [[ -n $GROUP_ID && $GROUP_ID != null ]]; then
  echo "exists ($GROUP_ID)"
else
  GROUP_ID=$("${OCI[@]}" iam group create --compartment-id "$TENANCY_ID" --name "$GROUP" \
    --description "Portage sync: write access to bucket $BUCKET" --query data.id --raw-output)
  echo "created ($GROUP_ID)"
fi

log "Policy $POLICY"
STATEMENTS=$(printf '["Allow group %s to read buckets in compartment id %s where target.bucket.name=\x27%s\x27", "Allow group %s to manage objects in compartment id %s where target.bucket.name=\x27%s\x27"]' \
  "$GROUP" "$COMPARTMENT_ID" "$BUCKET" "$GROUP" "$COMPARTMENT_ID" "$BUCKET")
POLICY_ID=$("${OCI[@]}" iam policy list --compartment-id "$COMPARTMENT_ID" --name "$POLICY" --all --query 'data[0].id' --raw-output 2>/dev/null || true)
if [[ -n $POLICY_ID && $POLICY_ID != null ]]; then
  echo "exists ($POLICY_ID); its statements are left unchanged:"
  "${OCI[@]}" iam policy get --policy-id "$POLICY_ID" --query 'data.statements' --raw-output
else
  "${OCI[@]}" iam policy create --compartment-id "$COMPARTMENT_ID" --name "$POLICY" \
    --description "Portage sync: read bucket $BUCKET, manage its objects" --statements "$STATEMENTS" >/dev/null
  echo "created:"
  echo "$STATEMENTS"
fi

KEY_ID="<customer-secret-key-access-key>"
if [[ $CREATE_USER == 1 ]]; then
  log "User $GROUP"
  USER_ID=$("${OCI[@]}" iam user list --compartment-id "$TENANCY_ID" --name "$GROUP" --all --query 'data[0].id' --raw-output 2>/dev/null || true)
  if [[ -n $USER_ID && $USER_ID != null ]]; then
    echo "exists ($USER_ID)"
  else
    email_args=()
    [[ -n $USER_EMAIL ]] && email_args=(--email "$USER_EMAIL")
    USER_ID=$("${OCI[@]}" iam user create --compartment-id "$TENANCY_ID" --name "$GROUP" \
      --description "Portage sync (S3 Compatibility API only)" ${email_args[@]+"${email_args[@]}"} --query data.id --raw-output)
    echo "created ($USER_ID)"
  fi
  in_group=$("${OCI[@]}" iam group list-users --group-id "$GROUP_ID" --all --query "length(data[?id=='$USER_ID'])" --raw-output 2>/dev/null || echo 0)
  if [[ $in_group == 0 ]]; then
    "${OCI[@]}" iam group add-user --group-id "$GROUP_ID" --user-id "$USER_ID" >/dev/null
    echo "added to $GROUP"
  fi

  log "Customer Secret Key for $GROUP"
  key_json=$("${OCI[@]}" iam customer-secret-key create --user-id "$USER_ID" --display-name "portage-$(date +%Y%m%d)")
  KEY_ID=$(printf '%s' "$key_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')
  KEY_SECRET=$(printf '%s' "$key_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["key"])')
  cat <<EOF
Access key: $KEY_ID
Secret:     $KEY_SECRET
Store the secret now: OCI never shows it again. (Max two keys per user;
delete old ones under the user's Customer secret keys.)
EOF
fi

ENDPOINT="https://${NAMESPACE}.compat.objectstorage.${REGION}.oraclecloud.com"
log "Done. Paste into pipeline.yaml (under a pipeline):"
cat <<EOF

    destination:
      s3:
        bucket: ${BUCKET}
        region: ${REGION}
        endpoint: ${ENDPOINT}
        path_style: true
        flavor: oci
        access_key_id: \${OCI_S3_ACCESS_KEY_ID}           # ${KEY_ID}
        secret_access_key: \${OCI_S3_SECRET_ACCESS_KEY}

Policies usually apply within seconds. Then: portage check -c pipeline.yaml
EOF
