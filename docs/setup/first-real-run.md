# First real run: Azure Southeast Asia → OCI Singapore

An end-to-end checklist for the first real pipeline: an Azure Blob container in
**Southeast Asia** (Singapore) synced to an OCI Object Storage bucket in
**ap-singapore-1**. Expect about two hours, most of it waiting on sign-ups and
role propagation. Go top to bottom. Each step says how to confirm it worked.

## 0. Accounts, budgets, cost awareness

- [ ] **Azure subscription.** Set a budget alert *before* creating anything: **Cost
  Management → Budgets → Add**, scoped to the subscription or the resource
  group, with email alerts at 50/80/100%.
- [ ] **OCI tenancy.** Set **Billing & Cost Management → Budgets → Create budget**
  with an alert rule. Your home region doesn't need to be Singapore, but you must be
  subscribed to `ap-singapore-1` (**Governance → Region management**).
- [ ] **Know what costs money:**
  - **Azure egress is the big one.** Every byte Portage copies leaves Azure
    Southeast Asia over the internet to OCI and is billed as Internet egress
    from that region. Look up the current rate for your monthly volume on the
    [Azure bandwidth pricing page](https://azure.microsoft.com/en-us/pricing/details/bandwidth/)
    and multiply by what you plan to sync (initial copy + daily volume). The
    soak in step 7 alone moves terabytes.
  - Azure: storage, transactions (List / Get / Event Grid operations / queue polls),
    and the VM.
  - OCI: storage and requests. Check inbound transfer on the
    [Oracle price list](https://www.oracle.com/cloud/price-list/) (Networking
    and Storage sections).
- [ ] Run the engine **in Azure Southeast Asia**, next to the source. Reading
  from Blob within the region doesn't add transfer charges, and each file leaves
  Azure exactly once, straight to OCI.

## 1. OCI destination

Follow [oci.md](oci.md). Either use the Console, or run:

```sh
deploy/oci/setup.sh --compartment-id <compartment-ocid> --bucket imports --create-user
```

When you're done you should have:

- [ ] the namespace (`oci os ns get`)
- [ ] the bucket `imports` in ap-singapore-1
- [ ] the group `portage-sync` with a bucket-scoped policy
- [ ] the user `portage-sync`, in the group, with a **Customer Secret Key**. Store the
  secret now. It isn't shown again.

## 2. Azure source

Follow [azure.md](azure.md):

```sh
az login
az account set -s <subscription>
deploy/azure/setup.sh -g portage-rg -a <storageaccount> -c exports -q portage-events
```

- [ ] Note the `source:` / `events:` blocks it prints.
- [ ] Upload a test blob and `az storage message peek` the queue. You should see one
  message within seconds (see azure.md, *Confirm events flow*).

## 3. The engine VM (Azure Southeast Asia)

The workload is network-bound with modest CPU. Memory needs are bounded:
in-flight part buffers are capped at 2 GiB, plus Postgres.

- **Start with `Standard_D2s_v5`** (2 vCPU, 8 GiB). Move to `Standard_D4s_v5`
  if `portage_queue_depth` keeps growing under load (bigger VMs also get more
  network bandwidth). Check that the size is available in the region and within your
  quota (`az vm list-skus -l southeastasia --size Standard_D2s_v5 -o table`).
- **Ubuntu 24.04 LTS** with a system-assigned managed identity. No public IP is needed,
  since Portage only makes outbound calls. Use Bastion or a jump host to get in.

```sh
az vm create -g portage-rg -n portage-vm -l southeastasia \
  --image Canonical:ubuntu-24_04-lts:server:latest --size Standard_D2s_v5 \
  --assign-identity '[system]' --public-ip-address "" --generate-ssh-keys
PRINCIPAL_ID=$(az vm identity show -g portage-rg -n portage-vm --query principalId -o tsv)
deploy/azure/setup.sh -g portage-rg -a <storageaccount> -c exports -q portage-events \
  --assignee-object-id "$PRINCIPAL_ID"          # grants the data-plane roles
```

On the VM:

```sh
sudo apt-get update && sudo apt-get install -y postgresql   # Postgres 16 on 24.04
sudo -u postgres createuser portage --pwprompt
sudo -u postgres createdb -O portage portage
timedatectl status                                          # "System clock synchronized: yes"
```

Build the binary on your machine and copy it over (the VM has no public IP, so use
`az network bastion ssh`/`tunnel` or a jump host):

```sh
GOOS=linux GOARCH=amd64 make build        # -> bin/portage (static, CGO off)
scp bin/portage <vm>:/tmp/ && ssh <vm> 'sudo install -m 0755 /tmp/portage /usr/local/bin/portage'
```

## 4. pipeline.yaml

Put it at `/etc/portage/pipeline.yaml` and put the secrets in `/etc/portage/env` (mode 0600):

```yaml
version: 1
database_url: postgres://portage:${PORTAGE_DB_PASSWORD}@127.0.0.1:5432/portage?sslmode=disable
metrics_addr: "127.0.0.1:9090"

pipelines:
  - name: azure-to-oci
    source:
      azure:
        account_url: https://<storageaccount>.blob.core.windows.net
        container: exports
        auth: default                     # the VM's managed identity
    destination:
      prefix: from-azure/
      s3:
        bucket: imports
        region: ap-singapore-1
        endpoint: https://<namespace>.compat.objectstorage.ap-singapore-1.oraclecloud.com
        path_style: true
        flavor: oci
        access_key_id: ${OCI_S3_ACCESS_KEY_ID}
        secret_access_key: ${OCI_S3_SECRET_ACCESS_KEY}
    events:
      type: azure_queue
      queue_account_url: https://<storageaccount>.queue.core.windows.net
      queue_name: portage-events
    existing_files: copy                  # also copy what's already there
    deletes: false                        # leave on false for the first run
```

```sh
# /etc/portage/env
PORTAGE_DB_PASSWORD=...
OCI_S3_ACCESS_KEY_ID=...
OCI_S3_SECRET_ACCESS_KEY=...
```

- [ ] `portage validate -c /etc/portage/pipeline.yaml` prints the plan with secrets redacted.

## 5. `portage check`, then `portage run`

```sh
set -a; . /etc/portage/env; set +a
portage check -c /etc/portage/pipeline.yaml
```

Every row should be `ok`. Expected warnings:

- `source list` warns if the container is still empty.
- `events` warns if the queue is empty. Upload a blob and re-run.
- `events` may say `count unavailable`. That's fine: Message Processor can't read queue
  properties.

Each `FAIL` prints a `FIX` line (missing role, wrong namespace, bad key, and
so on). Fix it and re-run. A role you just assigned can take up to 10 minutes to
apply. The destination probes write and delete objects under
`from-azure/.portage-check/`.

When the check is clean, install the service:

```ini
# /etc/systemd/system/portage.service
[Unit]
Description=Portage object-storage sync
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=portage
EnvironmentFile=/etc/portage/env
ExecStartPre=/usr/local/bin/portage check -c /etc/portage/pipeline.yaml
ExecStart=/usr/local/bin/portage run -c /etc/portage/pipeline.yaml --log-format json
Restart=on-failure
RestartSec=10
# Lets running copies finish (they resume after a restart anyway).
TimeoutStopSec=60
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`ExecStartPre` refuses to start on a failed check and leaves the reason in the
journal. Drop that line if you'd rather start anyway.

```sh
sudo useradd --system --no-create-home portage
sudo chown root:portage /etc/portage/env && sudo chmod 0640 /etc/portage/env
sudo systemctl daemon-reload && sudo systemctl enable --now portage
journalctl -u portage -f
```

## 6. Watch it

```sh
portage status -c /etc/portage/pipeline.yaml --watch 5s
curl -s 127.0.0.1:9090/metrics | grep -E '^portage_(sync_lag|oldest_pending|queue_depth|files_total|bytes_copied)'
```

- **SYNCED** climbs during the initial copy. **OLDEST PENDING** (the lag building up right now)
  should drop to seconds once the backlog is gone.
- New uploads to `exports` should show up in OCI within seconds through events. If
  they only show up every `reconcile_interval`, events aren't flowing.
- `FAILED` with recent errors → `portage status` shows the last error per key.
- A Grafana dashboard for these metrics is in `deploy/grafana/`. Tunnel
  `127.0.0.1:9090` over SSH or Bastion to scrape it.

### 6a. Real-OCI conformance test

This is the connector contract test suite against the real bucket. Run it from your machine or
the VM. It works under a unique `portage-it/…` prefix and deletes everything it
wrote. It creates no buckets.

```sh
export PORTAGE_TEST_OCI_ENDPOINT=https://<namespace>.compat.objectstorage.ap-singapore-1.oraclecloud.com
export PORTAGE_TEST_OCI_REGION=ap-singapore-1
export PORTAGE_TEST_OCI_BUCKET=imports
export PORTAGE_TEST_OCI_ACCESS_KEY_ID=...
export PORTAGE_TEST_OCI_SECRET_ACCESS_KEY=...
go test -tags integration -run TestConformanceOCI -v ./internal/connector/s3/
```

Without these variables the test is skipped. If it fails, the failure points at a
specific OCI S3-compatibility quirk. Keep the output.

## 7. Soak

Only after steps 5 and 6 look clean. See [bench/README.md](../../bench/README.md#azure--oci-soak).

- [ ] **Do the cost maths first.** At `-rate 500GB/day` for 72h, that's about 1.5 TB of
  Azure egress before any burst. Pick a rate and duration your budget covers. A
  shorter soak (`-duration 12h` at a lower rate) still exercises every path.
- [ ] The load generator writes to the source, so it needs **Storage Blob Data
  Contributor** on the container. Grant that to whoever runs it, not to Portage's
  identity. Portage only reads.
- [ ] Run the generator on the same VM or another VM in Southeast Asia.
- [ ] Afterwards: verify the manifest against the bucket (size and `portagesha256`
  metadata), and check `portage_sync_lag_seconds` over the run.
- [ ] Clean up: delete `soak/` and `burst/` from both sides, and stop or deallocate the VM
  if you're pausing.

## Rollback / stopping

- `sudo systemctl stop portage` stops cleanly. Running copies get up to 30s to finish,
  and interrupted multipart uploads resume on the next start.
- Nothing is deleted at the destination unless `deletes: true`.
- Event messages wait in the queue (TTL infinite) until the engine is back.
