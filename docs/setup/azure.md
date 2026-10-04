# Azure Blob Storage as a source (with Event Grid events)

Portage reads the source container and learns about changes from **Event
Grid**. Event Grid delivers `BlobCreated` / `BlobDeleted` events to an
**Azure Storage Queue**, and Portage polls that queue (`events.type: azure_queue`).
Nothing inbound is needed. A periodic reconcile catches anything the events miss.

## What `deploy/azure/setup.sh` does

```sh
az login
deploy/azure/setup.sh -g portage-rg -a <storageaccount> -c exports -q portage-events \
  [--assignee-object-id <principal-id>]
```

Each step checks first, so re-running is safe. The script:

1. Registers the `Microsoft.Storage` and `Microsoft.EventGrid` resource providers
   if needed. This is a one-time step per subscription.
2. Creates the **resource group** in `southeastasia` (change it with `-l`).
3. Creates a **StorageV2** account (`Standard_LRS`, TLS 1.2 minimum, no anonymous blob access).
4. Creates the **source container**. This uses the management plane, so Contributor on
   the resource group is enough.
5. Creates the **queue** and **`<queue>-poison`** through ARM. Portage moves
   messages it can't decode to the poison queue. The CLI has no management-plane queue
   command, so the script calls the Queue - Create REST API with `az rest`.
6. Creates (or reuses) the account's **Event Grid system topic**. Only one
   system topic can exist per storage account, so an existing one is reused.
7. Creates an **event subscription** on that topic with these settings:
   - endpoint type `storagequeue` → your queue
   - event types `Microsoft.Storage.BlobCreated` and `Microsoft.Storage.BlobDeleted`
   - subject filter `/blobServices/default/containers/<container>/`. The
     trailing slash stops `exports` from also matching `exports2`.
   - **Event Grid schema** (`--event-delivery-schema eventgridschema`), which is what
     Portage parses
   - queue message TTL `-1`, so messages never expire. Events survive an engine outage
     longer than the default 7 days.
8. With `--assignee-object-id`, grants the identity Portage runs as these roles
   (for `auth: default`):

   | Role | Scope | Why |
   |---|---|---|
   | Storage Blob Data Reader | the container | list + read blobs |
   | Storage Queue Data Message Processor | the queue | peek, receive, delete event messages |
   | Storage Queue Data Contributor | `<queue>-poison` | create it if missing, add undecodable messages |

   Message Processor can't add messages or create queues, which is why the poison
   queue gets its own role. Role assignments can take **up to 10 minutes** to
   apply. Until then `portage check` reports `AuthorizationPermissionMismatch`.
9. Prints the `source:` and `events:` blocks for `pipeline.yaml`.

Who to grant roles to:

- **VM managed identity** (recommended for the engine VM):
  `az vm identity assign -g <rg> -n <vm>`, then
  `az vm identity show -g <rg> -n <vm> --query principalId -o tsv`
- **Yourself**, for a local test with `az login`:
  `az ad signed-in-user show --query id -o tsv` with `--assignee-type User`

Note that Owner/Contributor do **not** grant data access with `auth: default`.
Azure RBAC keeps management-plane roles and data-plane roles separate.

## Doing it in the portal instead

1. **Storage accounts → Create**: region *Southeast Asia*, performance Standard, redundancy LRS.
2. In the account: **Data storage → Containers → + Container** (`exports`), and
   **Queues → + Queue** (`portage-events` and `portage-events-poison`).
3. **Events → + Event Subscription**:
   - Event schema: *Event Grid Schema*
   - System topic name: e.g. `<account>-events`
   - Filter to event types: *Blob Created*, *Blob Deleted*
   - Endpoint type: *Storage Queues* → pick the account and `portage-events`
   - **Filters** tab: enable subject filtering, *Subject begins with*
     `/blobServices/default/containers/exports/`
4. **Access control (IAM)** on the container and queues: add the role assignments from the table above.

## pipeline.yaml blocks

```yaml
    source:
      azure:
        account_url: https://<account>.blob.core.windows.net
        container: exports
        auth: default            # managed identity on the VM, az login locally
    events:
      type: azure_queue
      queue_account_url: https://<account>.queue.core.windows.net
      queue_name: portage-events
```

`auth: default` uses `DefaultAzureCredential`, which tries environment
variables (service principal), then workload identity, then managed identity, then `az login`.
With a *user-assigned* managed identity, set `AZURE_CLIENT_ID` to its client ID.

## Confirm events flow

Upload a blob, then look at the queue without consuming it:

```sh
az storage blob upload --account-name <account> --auth-mode key \
  -c exports -n portage-test.txt -f ./some-file.txt
az storage message peek --account-name <account> --auth-mode key \
  -q portage-events --num-messages 5
```

You should see a message within seconds. Its `content` is base64. Decode
it with `base64 -d` to see the Event Grid JSON (`"eventType": "Microsoft.Storage.BlobCreated"`,
`"subject": "/blobServices/default/containers/exports/blobs/portage-test.txt"`).
`--auth-mode key` uses the account key, which Contributor can read. With
`--auth-mode login` you need *Storage Blob Data Contributor* and *Storage Queue Data Reader* yourself.

Then:

```sh
portage check -c pipeline.yaml
```

The `events` row reports the approximate queue depth and confirms that peeked
messages parse as Event Grid events. Peeking consumes nothing.

### If no message appears

- `az eventgrid system-topic event-subscription show -g <rg> --system-topic-name <topic> -n portage-exports`
  should show `provisioningState: Succeeded`.
- A subject filter typo silently matches nothing. It must be
  `/blobServices/default/containers/<container>/`.
- `BlobCreated` fires when a blob is **committed** (Put Blob / Put Block List /
  Copy Blob), not for each uploaded block
  ([blob events](https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage)).
- If the storage account has a firewall, Event Grid can only deliver to the queue with a
  system-assigned identity on the system topic and *trusted services* allowed.

## References

- System topics: https://learn.microsoft.com/en-us/azure/event-grid/system-topics
- `az eventgrid system-topic event-subscription`: https://learn.microsoft.com/en-us/cli/azure/eventgrid/system-topic/event-subscription
- Storage Queue handler: https://learn.microsoft.com/en-us/azure/event-grid/handler-storage-queues
- Storage built-in roles: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage
