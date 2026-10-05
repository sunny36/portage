# Benchmarks and soak tests

## portage-loadgen

Writes synthetic blobs into an Azure Blob container at a target daily volume,
so a Portage pipeline (Azure → S3/OCI) can be soak-tested under realistic
load. Every blob written is appended to a JSONL manifest that a verifier can
check against the destination.

```sh
make loadgen            # -> bin/portage-loadgen
```

### What it writes

- **Keys**: `<prefix>YYYY/MM/DD/HH/<UTC timestamp>-<run id>-<seq>.bin`, e.g.
  `loadgen/2026/10/05/02/20261005T023507.123Z-0000002a-000000042.bin`. Unique
  and time-ordered; the run id is derived from the seed.
- **Sizes**: drawn from a weighted mix (`-mix`). Within each range, sizes are
  log-uniform. The default, `10KB-10MB:0.95,100MB-1GB:0.045,5GB:0.005`, is
  heavy-tailed (mean ≈ 44 MB): mostly small files, some large ones, and an
  occasional 5 GB blob that exercises multipart/resume.
- **Content**: a ChaCha8 stream keyed by `(seed, seq)`. Re-running with the same
  `-seed` reproduces the same sizes and bytes; content is incompressible.
- **Manifest** (`-manifest`, appended, flushed per line):
  `{"key":"…","size":123,"sha256":"<hex>","written_at":"<RFC3339>"}`.
  Only successful uploads are recorded.

Data is streamed via Put Block / Put Block List; memory is roughly
`concurrency × block-concurrency × block-size` (default 16 × 4 × 8 MiB = 512 MiB).
Lower `-concurrency` or `-block-size` on small machines.

### Pacing

- `-rate 500GB/day` (default): blob *n* starts no earlier than
  `start + bytes(0..n-1) / rate`, so the long-run average matches the target
  whatever the size mix. Units: `KB MB GB TB` (decimal) or `KiB MiB GiB TiB`,
  per `s`, `min`, `h` or `day`. `-rate 0` is unlimited.
- `-burst 2TB`: ignore `-rate`, write 2 TB as fast as `-concurrency` allows,
  then stop.
- `-duration 24h`, `-count N`: other stop conditions. Ctrl-C stops cleanly;
  in-flight uploads are abandoned and not recorded.

If the generator can't keep up (uploads slower than the target), the progress
log's `rate_per_day` falls below target: raise `-concurrency`.

### Auth

- `-connection-string` or `$AZURE_STORAGE_CONNECTION_STRING`, or
- `-account-url https://<account>.blob.core.windows.net` with
  `DefaultAzureCredential` (env vars, managed identity, `az login`). Needs the
  *Storage Blob Data Contributor* role on the container.

`-create-container` creates the container if missing (and the `-event-queue`
queue, when given).

### Synthetic events (emulators)

Azurite has no Event Grid. `-event-queue NAME` makes the generator also
enqueue, per blob written, the Storage Queue message Event Grid would deliver
(`Microsoft.Storage.BlobCreated`, base64 JSON, `eventTime` = commit time), so
a pipeline with `events.type: azure_queue` exercises its event path and
reports event-driven sync lag locally. The queue comes from the connection
string's `QueueEndpoint`, or `-queue-account-url` with
`DefaultAzureCredential`. Against real Azure leave it off: Event Grid sends
the real events.

## Azure → OCI soak

1. Create a source container and an OCI bucket, and a Portage pipeline between
   them with `events: azure_queue` (see `examples/pipeline.yaml`). Run the
   generator close to the storage account (same region VM) so your uplink
   isn't the bottleneck.
2. Start `portage run`, then the load:

   ```sh
   bin/portage-loadgen -account-url https://<acct>.blob.core.windows.net \
     -container exports -prefix soak/ -rate 500GB/day -duration 72h \
     -seed 42 -manifest bench/results/raw/soak-42.jsonl
   ```

3. Mid-soak, add a burst from a second generator with a different seed and
   prefix (`-burst 2TB -seed 43 -prefix burst/ -concurrency 64`) to check the
   backlog drains and sync lag recovers.
4. Sample the engine for the whole run (RSS, goroutines, queue depth, file
   record / River table growth) with `bench/soak/sample.sh`:

   ```sh
   METRICS_URL=http://127.0.0.1:9090/metrics PIDFILE=/run/portage.pid \
   PSQL='psql postgres://…/portage' bench/soak/sample.sh bench/results/raw/samples.csv
   ```

5. After the load stops and Portage has caught up, verify every manifest line
   with `portage-verify` (below) and commit its JSON report under
   `bench/results/`.

Raw results go under `bench/results/raw/` (git-ignored).

Running locally against Azurite:

```sh
make up
export AZURE_STORAGE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:20000/devstoreaccount1;'
bin/portage-loadgen -container source -create-container -prefix incoming/ \
  -rate 50MB/s -duration 1m -mix 10KB-10MB:1 -manifest /tmp/loadgen.jsonl
```

This feeds the `azure-to-s3-local` pipeline in `examples/pipeline.yaml`.

## portage-verify

Proves "zero missing or mismatched files": for every manifest entry, the
object exists at the pipeline's destination, its size matches, and the
SHA-256 of its full content (streamed, not trusted from metadata) equals the
manifest's.

```sh
go build -o bin/portage-verify ./bench/cmd/portage-verify
bin/portage-verify -c pipeline.yaml -pipeline azure-to-oci \
  -concurrency 32 -retry-missing 10m \
  -metrics http://127.0.0.1:9090/metrics -metrics-window 60s \
  -json bench/results/soak-2026-10-07.json \
  bench/results/raw/soak-42.jsonl bench/results/raw/burst-43.jsonl
```

- The destination connector is built from the config exactly as the engine
  builds it. Manifest keys are source-container keys: the pipeline's source
  prefix is stripped and the destination prefix applied. Keys outside the
  source prefix or excluded by the pipeline's filters are counted, not
  checked.
- A key written more than once counts only at its last write (latest
  `written_at`; ties go to the entry read last).
- `-retry-missing` keeps re-checking missing objects while the engine
  catches up. `-size-only` skips reading content (existence + size, plus the
  `portagesha256` metadata where present) for a fast first look.
- `-metrics` adds the engine's view: sync-lag and copy-time p50/p95/p99,
  files by outcome, events by source, bytes copied and throughput, queue
  depth and oldest pending. Quantiles come from the histogram buckets,
  interpolated like PromQL `histogram_quantile`, so they are only as precise
  as the bucket boundaries; counters restart with the engine process. The
  report also has exact percentiles of *destination LastModified −
  written_at* per file (1 s resolution; on S3/OCI a multipart object's
  LastModified is when its upload started).
- Exit status: 0 all verified, 1 something missing/mismatched/unreadable,
  2 usage or setup error.
