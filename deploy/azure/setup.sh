#!/usr/bin/env bash
# Portage: Azure source setup.
#
# Creates (or reuses) everything an Azure Blob source with Event Grid change
# events needs, then prints the pipeline.yaml blocks to paste:
#
#   resource group -> StorageV2 account -> source container
#                  -> Storage Queue (+ <queue>-poison)
#                  -> Event Grid system topic for the account
#                  -> event subscription: BlobCreated + BlobDeleted for the
#                     container, delivered to the queue
#   optional: data-plane role assignments for the identity Portage runs as
#
# Idempotent: every step checks for the resource first, so re-running is safe.
# Needs the Azure CLI (`az login` first) and Contributor (or Owner) on the
# resource group; role assignments additionally need Owner or
# "Role Based Access Control Administrator".
#
# Usage:
#   deploy/azure/setup.sh -g <resource-group> -a <storage-account> [options]
#
# Options (or the environment variable in brackets):
#   -g, --resource-group NAME     [RESOURCE_GROUP]   required
#   -a, --account NAME            [STORAGE_ACCOUNT]  required; 3-24 lowercase letters/digits, globally unique
#   -l, --location NAME           [LOCATION]         default: southeastasia
#   -c, --container NAME          [CONTAINER]        default: exports
#   -q, --queue NAME              [QUEUE]            default: portage-events
#       --topic NAME              [SYSTEM_TOPIC]     default: <account>-events (an existing one is reused)
#       --subscription-name NAME  [EVENT_SUB]        default: portage-<container>
#       --assignee-object-id ID   [ASSIGNEE_OBJECT_ID]  grant this principal the roles Portage needs
#       --assignee-type TYPE      [ASSIGNEE_TYPE]    ServicePrincipal (managed identity, default) or User
#   -h, --help
#
# Who to grant roles to (for `auth: default`):
#   VM system-assigned identity:  az vm identity show -g <rg> -n <vm> --query principalId -o tsv
#   yourself (testing locally):   az ad signed-in-user show --query id -o tsv   (with --assignee-type User)
set -euo pipefail

RESOURCE_GROUP=${RESOURCE_GROUP:-}
STORAGE_ACCOUNT=${STORAGE_ACCOUNT:-}
LOCATION=${LOCATION:-southeastasia}
CONTAINER=${CONTAINER:-exports}
QUEUE=${QUEUE:-portage-events}
SYSTEM_TOPIC=${SYSTEM_TOPIC:-}
EVENT_SUB=${EVENT_SUB:-}
ASSIGNEE_OBJECT_ID=${ASSIGNEE_OBJECT_ID:-}
ASSIGNEE_TYPE=${ASSIGNEE_TYPE:-ServicePrincipal}

# Storage resource provider API version for the queue PUT below.
STORAGE_API_VERSION=2023-05-01

usage() { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; }
die() { echo "error: $*" >&2; exit 1; }
log() { printf '\n==> %s\n' "$*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -g|--resource-group) RESOURCE_GROUP=$2; shift 2 ;;
    -a|--account) STORAGE_ACCOUNT=$2; shift 2 ;;
    -l|--location) LOCATION=$2; shift 2 ;;
    -c|--container) CONTAINER=$2; shift 2 ;;
    -q|--queue) QUEUE=$2; shift 2 ;;
    --topic) SYSTEM_TOPIC=$2; shift 2 ;;
    --subscription-name) EVENT_SUB=$2; shift 2 ;;
    --assignee-object-id) ASSIGNEE_OBJECT_ID=$2; shift 2 ;;
    --assignee-type) ASSIGNEE_TYPE=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

[[ -n $RESOURCE_GROUP ]] || die "--resource-group is required (see --help)"
[[ -n $STORAGE_ACCOUNT ]] || die "--account is required (see --help)"
[[ $STORAGE_ACCOUNT =~ ^[a-z0-9]{3,24}$ ]] || die "storage account name must be 3-24 lowercase letters and digits"
[[ $CONTAINER =~ ^[a-z0-9]([a-z0-9]|-[a-z0-9]){2,62}$ ]] || die "container name must be 3-63 lowercase letters, digits and single hyphens"
# The engine also uses "<queue>-poison", which must be a valid queue name too.
[[ $QUEUE =~ ^[a-z0-9]([a-z0-9]|-[a-z0-9]){2,55}$ ]] || die "queue name must be 3-56 lowercase letters, digits and single hyphens"
case "$ASSIGNEE_TYPE" in ServicePrincipal|User|Group) ;; *) die "--assignee-type must be ServicePrincipal, User or Group" ;; esac
SYSTEM_TOPIC=${SYSTEM_TOPIC:-${STORAGE_ACCOUNT}-events}
EVENT_SUB=${EVENT_SUB:-portage-${CONTAINER}}
POISON_QUEUE="${QUEUE}-poison"

command -v az >/dev/null || die "the Azure CLI (az) is not installed: https://learn.microsoft.com/cli/azure/install-azure-cli"
SUBSCRIPTION_ID=$(az account show --query id -o tsv 2>/dev/null) || die "not logged in: run 'az login' (and 'az account set -s <subscription>')"
echo "Subscription: $(az account show --query name -o tsv) ($SUBSCRIPTION_ID)"

register_provider() {
  local ns=$1 state
  state=$(az provider show --namespace "$ns" --query registrationState -o tsv 2>/dev/null || echo NotRegistered)
  if [[ $state != Registered ]]; then
    log "Registering resource provider $ns (one-time per subscription)"
    az provider register --namespace "$ns" >/dev/null
    for _ in $(seq 1 60); do
      state=$(az provider show --namespace "$ns" --query registrationState -o tsv)
      [[ $state == Registered ]] && break
      sleep 5
    done
    [[ $state == Registered ]] || die "$ns is still $state; re-run in a few minutes"
  fi
}
register_provider Microsoft.Storage
register_provider Microsoft.EventGrid

log "Resource group $RESOURCE_GROUP ($LOCATION)"
if [[ $(az group exists --name "$RESOURCE_GROUP") == true ]]; then
  echo "exists"
else
  az group create --name "$RESOURCE_GROUP" --location "$LOCATION" -o none
  echo "created"
fi

log "Storage account $STORAGE_ACCOUNT"
if az storage account show -n "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" -o none 2>/dev/null; then
  echo "exists"
else
  avail=$(az storage account check-name --name "$STORAGE_ACCOUNT" --query nameAvailable -o tsv)
  [[ $avail == true ]] || die "storage account name $STORAGE_ACCOUNT is taken (names are global); pick another"
  az storage account create -n "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" -l "$LOCATION" \
    --kind StorageV2 --sku Standard_LRS --min-tls-version TLS1_2 --allow-blob-public-access false -o none
  echo "created (StorageV2, Standard_LRS)"
fi
ACCOUNT_ID=$(az storage account show -n "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" --query id -o tsv)
ACCOUNT_LOCATION=$(az storage account show -n "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" --query primaryLocation -o tsv)

log "Container $CONTAINER"
# Management-plane command: needs only Contributor, no data-plane role.
if [[ $(az storage container-rm exists --storage-account "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" --name "$CONTAINER" --query exists -o tsv) == true ]]; then
  echo "exists"
else
  az storage container-rm create --storage-account "$STORAGE_ACCOUNT" -g "$RESOURCE_GROUP" --name "$CONTAINER" --public-access off -o none
  echo "created"
fi

# The CLI has no management-plane queue command, so PUT the queue resource
# through ARM (Queue - Create; idempotent, needs only Contributor).
create_queue() {
  local name=$1
  az rest --method put -o none \
    --url "https://management.azure.com${ACCOUNT_ID}/queueServices/default/queues/${name}?api-version=${STORAGE_API_VERSION}" \
    --body '{}'
  echo "queue $name ready"
}
log "Queues $QUEUE and $POISON_QUEUE"
create_queue "$QUEUE"
# Portage moves undecodable messages here; pre-creating it lets a role be
# scoped to it instead of granting queue-create on the whole account.
create_queue "$POISON_QUEUE"

log "Event Grid system topic"
# Only one system topic may exist per storage account: reuse it if present.
# Resource IDs compare case-insensitively.
lower() { tr '[:upper:]' '[:lower:]' <<<"$1"; }
existing_topic="" existing_topic_rg=""
while IFS=$'\t' read -r t_source t_name t_rg; do
  if [[ -n $t_source && $(lower "$t_source") == $(lower "$ACCOUNT_ID") ]]; then
    existing_topic=$t_name existing_topic_rg=$t_rg
    break
  fi
done < <(az eventgrid system-topic list --query '[].[source, name, resourceGroup]' -o tsv)
if [[ -n $existing_topic ]]; then
  SYSTEM_TOPIC=$existing_topic
  TOPIC_RG=$existing_topic_rg
  echo "reusing $SYSTEM_TOPIC (resource group $TOPIC_RG)"
else
  TOPIC_RG=$RESOURCE_GROUP
  az eventgrid system-topic create -g "$TOPIC_RG" --name "$SYSTEM_TOPIC" --location "$ACCOUNT_LOCATION" \
    --topic-type microsoft.storage.storageaccounts --source "$ACCOUNT_ID" -o none
  echo "created $SYSTEM_TOPIC"
fi

log "Event subscription $EVENT_SUB -> queue $QUEUE"
if az eventgrid system-topic event-subscription show -g "$TOPIC_RG" --system-topic-name "$SYSTEM_TOPIC" -n "$EVENT_SUB" -o none 2>/dev/null; then
  echo "exists (delete it with 'az eventgrid system-topic event-subscription delete' to recreate with new settings)"
else
  # Event Grid schema (what Portage parses); the trailing slash in the
  # subject filter keeps container "exports" from matching "exports2".
  # Queue message TTL -1 = never expire, so events survive an engine outage
  # longer than the 7-day default.
  az eventgrid system-topic event-subscription create -g "$TOPIC_RG" --system-topic-name "$SYSTEM_TOPIC" -n "$EVENT_SUB" \
    --endpoint-type storagequeue \
    --endpoint "${ACCOUNT_ID}/queueservices/default/queues/${QUEUE}" \
    --included-event-types Microsoft.Storage.BlobCreated Microsoft.Storage.BlobDeleted \
    --subject-begins-with "/blobServices/default/containers/${CONTAINER}/" \
    --event-delivery-schema eventgridschema \
    --storage-queue-msg-ttl -1 \
    --max-delivery-attempts 30 --event-ttl 1440 \
    -o none
  echo "created"
fi

CONTAINER_SCOPE="${ACCOUNT_ID}/blobServices/default/containers/${CONTAINER}"
QUEUE_SCOPE="${ACCOUNT_ID}/queueServices/default/queues/${QUEUE}"
POISON_SCOPE="${ACCOUNT_ID}/queueServices/default/queues/${POISON_QUEUE}"

assign_role() {
  local role=$1 scope=$2 n
  n=$(az role assignment list --assignee-object-id "$ASSIGNEE_OBJECT_ID" --role "$role" --scope "$scope" --query 'length(@)' -o tsv)
  if [[ $n != 0 ]]; then
    echo "\"$role\" already assigned on ${scope##*/providers/}"
    return
  fi
  az role assignment create --assignee-object-id "$ASSIGNEE_OBJECT_ID" --assignee-principal-type "$ASSIGNEE_TYPE" \
    --role "$role" --scope "$scope" -o none
  echo "assigned \"$role\" on ${scope##*/providers/}"
}
if [[ -n $ASSIGNEE_OBJECT_ID ]]; then
  log "Role assignments for $ASSIGNEE_TYPE $ASSIGNEE_OBJECT_ID (auth: default)"
  assign_role "Storage Blob Data Reader" "$CONTAINER_SCOPE"             # list + read blobs
  assign_role "Storage Queue Data Message Processor" "$QUEUE_SCOPE"     # peek, receive, delete events
  assign_role "Storage Queue Data Contributor" "$POISON_SCOPE"          # create + add poison messages
  echo "Role assignments can take up to 10 minutes to take effect."
else
  log "Skipping role assignments (pass --assignee-object-id to grant them)"
  cat <<EOF
For auth: default, the identity Portage runs as needs:
  az role assignment create --assignee-object-id <principal-id> --assignee-principal-type ServicePrincipal \\
    --role "Storage Blob Data Reader" --scope $CONTAINER_SCOPE
  az role assignment create --assignee-object-id <principal-id> --assignee-principal-type ServicePrincipal \\
    --role "Storage Queue Data Message Processor" --scope $QUEUE_SCOPE
  az role assignment create --assignee-object-id <principal-id> --assignee-principal-type ServicePrincipal \\
    --role "Storage Queue Data Contributor" --scope $POISON_SCOPE
EOF
fi

BLOB_URL="https://${STORAGE_ACCOUNT}.blob.core.windows.net"
QUEUE_URL="https://${STORAGE_ACCOUNT}.queue.core.windows.net"
log "Done. Paste into pipeline.yaml (under a pipeline):"
cat <<EOF

    source:
      azure:
        account_url: ${BLOB_URL}
        container: ${CONTAINER}
        auth: default            # managed identity on the VM, or az login locally
    events:
      type: azure_queue
      queue_account_url: ${QUEUE_URL}
      queue_name: ${QUEUE}

Confirm events flow (--auth-mode key uses the account key, which Contributor
can read; with --auth-mode login you need Storage Blob Data Contributor and
Storage Queue Data Reader yourself):
  az storage blob upload --account-name ${STORAGE_ACCOUNT} --auth-mode key -c ${CONTAINER} -n portage-test.txt -f <some-file>
  az storage message peek --account-name ${STORAGE_ACCOUNT} --auth-mode key -q ${QUEUE} --num-messages 5
Then: portage check -c pipeline.yaml
EOF
