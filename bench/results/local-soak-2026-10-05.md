# Local soak, 2026-10-05 (Azurite → SeaweedFS)

A laptop run before the real 24 h Azure → OCI soak. It used the real `bin/portage`
at 3d1cd4f with engine defaults (concurrency 16, part_concurrency 4, 64 MiB parts),
`events: azure_queue` fed by `portage-loadgen -event-queue`, and `reconcile_interval: 1m`.
The database was a fresh one on the shared compose Postgres. Azurite and SeaweedFS
ran as private containers on :46000/:46001/:46333 because the shared SeaweedFS
allows only 50 × 256 MB volumes (about 12.8 GB, and it is already full), and a load
this size would have starved other users. Raw data is in `bench/results/raw/soak-d7dc15/`,
which git ignores.

## Load
| phase | files | bytes | rate |
|---|---|---|---|
| steady (`-rate 10MB/s`, mix `10KB-10MB:0.95,100MB-512MiB:0.05`) | 2539 | 34.97 GB | ~9.6 MB/s while the host was awake |
| burst (`-burst 10GB -concurrency 16`, at T+19 m) | 772 | 10.00 GB | ~52 MB/s (3 m 13 s) |
| **total** | **3311** | **44.97 GB** | |

Five steady uploads ended with `context canceled` when the generator stopped.
Loadgen records only successful uploads, so none of the five is in the manifest.

## Events during the run
- **T+4 m: engine crash.** SIGQUIT was sent on purpose to get a goroutine dump. The
  engine restarted cleanly, and the interrupted multipart upload resumed (attempt 2).
- **T+22 m and T+31 m: destination outages.** The private SeaweedFS was OOM-killed
  by the Docker VM (7.7 GB VM, about 30 containers). The outages lasted 77 s and 14 min.
- **T+44 m: graceful restart.** SIGTERM stopped the engine in 14 s. Its first restart
  (engine-3) exited at once with `destination check` failed, because SeaweedFS was
  still down. That is the engine's fail-fast start check, an environmental cause.
  Engine-4 started at T+47 m.
- **After T+60 m:** the laptop slept several times. That froze timers and caused
  repeated 10 s Postgres timeouts in River.

## Results
- **Restart and crash: no file was copied twice.** 2112 synced records were compared
  across the SIGTERM restart and 292 across the crash. `synced_at` and `dest_version`
  were unchanged for all of them. After engine-4 started, it drained a backlog of 550
  files (about 10 GB) in about 50 s.
- **Sync lag** (exact, from `file_record.synced_at − event_time`, 3310 files):

  | window | p50 | p95 | p99 | max |
  |---|---|---|---|---|
  | steady, before the burst (includes the crash) | 3.5 s | 9.0 s | 28.9 s | 164 s |
  | burst | 2.2 s | 30.1 s | 101.7 s | 148 s |
  | outage 2 + restart | 547 s | 909 s | 938 s | 975 s |
  | steady, after recovery | 0.8 s | 4.5 s | 5.3 s | 6.2 s |
  | whole run | 3.4 s | 840 s | 937 s | 999 s |
- **Copy errors** (engine-2.log): 930 `put`, 48 `begin upload`, 12 `resume upload` and
  7 `upload part` failed with `connection refused`. Another 12 failed with
  `connection reset by peer` and 6 with send errors. All of them fall inside the two
  SeaweedFS OOM windows. They were emulator outages, not engine faults, and every
  affected file was retried and synced.
- **Memory and goroutines:** the engine was sampled every 30 s from T+0 to T+60 m.
  Go heap in use was 0.5–1.3 GB during steady load. During the burst and the outage
  drains it peaked at 3.3–4.0 GB, which is above the 2 GiB buffer-pool cap (see
  concern 3). Goroutines stayed between 55 and 146 and fell back to 55–75 at idle, so
  there is no leak trend. `ps` RSS on macOS swings with memory compression and is not
  usable for this.
- **Database growth:** `file_record` held 3311 rows in 2.1 MB. `river_job` held 4327
  rows in 9.5 MB, about 1.3 jobs per file (event and reconcile duplicates collapse to
  `already_synced`). The whole database was 20 MB. River pruned nothing: all completed
  jobs from T+0 are still there, because River keeps completed jobs for 24 h by default.
  Growth is about 4.3k jobs and 9 MB per hour at about 3.3k files per hour.
- **Verify** (`portage-verify` reading back every object and hashing the full
  content): **3310 of 3311 OK, 1 missing.** Three objects first failed with
  connection resets during a SeaweedFS OOM and passed on recheck. The missing file is
  `steady/…T203105.900Z-…-000000011.bin`. It is the engine bug below: it stayed
  `pending` for 5.6 h. After its orphaned job was released by hand, it synced and
  verified, giving **3311 / 3311**. The engine's file record agrees: 3310 records are
  synced with the same size and SHA-256 as the manifest, and that one key was pending.

## Engine bug: an orphaned River job blocks a key (or the reconciler) for 7 h

> **Fixed in 2aada3e.** Copy and reconcile jobs carry a time window in their
> dedupe key, so an orphaned job blocks its key for at most 5 minutes (copies)
> or two reconcile intervals.
**What happens.** River's `JobGetAvailable` can time out on the client after Postgres
has already committed the fetch (this run logged `timed out after 10s` during
database stalls). The job is then left in state `running` with no worker. Job 13
(`attempted_at` 20:31:16.336, exactly when the timed-out fetch began) never reached
`Claim`, so the file record stayed `pending` with `attempts=0`. Every reconcile then
emitted the key with `Generation=0`, the same unique key as the orphan. Because
`copyUniqueStates` includes `running` (internal/queue/queue.go:130), each insert was
deduplicated away. The orphan is only rescued after `RescueStuckJobsAfter = 7h`
(internal/queue/queue.go:27). The same thing happened to the **periodic reconcile
job**: job 8101 was orphaned at 23:53Z, and no reconcile ran for more than 2 h
(internal/pipeline/reconcile.go: `UniqueOpts.ByState` includes `Running`). A crash
between River's fetch and `Claim` orphans jobs the same way.

**Confirmed.** Setting jobs 13 and 8101 back to `available` by hand made the file sync
and verify, and the reconcile ran within seconds.

**Proposed fix.** In `Engine.emit` (internal/pipeline/run.go:293), when the record is
not leased and has been `pending` or `failed` for longer than about 2 × `leaseDuration`,
add a coarse time epoch to the dedupe key: a new `river:"unique"` field
`Epoch = now / 5m`, zero otherwise. A stuck key then gets a fresh job within minutes.
Duplicate jobs are harmless because `Claim` returns Busy or AlreadySynced. For
reconcile, drop `Running` from `ByState` and rely on the per-pipeline queue having
`MaxWorkers 1`, or give it a separate short rescue. Also add an alert on
`portage_oldest_pending_seconds`: it showed this problem the whole time, rising to
14 482 s.

## Other findings
All fixed: 1–2 in 2aada3e, 3–5 in the commit that adds this note.

1. Config validation requires `events.queue_account_url` even with
   `auth: connection_string` (internal/config/config.go:244). The azqueue source
   ignores that field. **Fixed:** optional with `connection_string` auth.
2. A single failed heartbeat (`ExtendLease`) cancels a running copy
   (internal/transfer/transfer.go:273), even though the 2 min lease leaves room for
   3 more tries. A 10 s database stall restarts a multi-GB copy. The copy resumes, but
   the work is wasted. **Fixed:** renewal errors are tolerated while the lease
   is safely valid; a lost lease still stops the copy.
3. The 2 GiB buffer cap is not a memory cap. Idle buffers are kept in per-size
   `sync.Pool`s outside the semaphore (internal/transfer/pool.go), and size classes
   are rounded to 1 MiB, giving up to 64 of them. The heap reached 4 GB. Set
   `GOMEMLIMIT` and size the VM for at least 4–5 GB. **Fixed:** idle buffers count
   against the cap (in-use + idle ≤ cap), and the engine sets a soft Go memory
   limit of pool + 1 GiB unless `GOMEMLIMIT` is set.
4. Requests have no per-request timeout: no Azure `TryTimeout`, and no S3
   `ResponseHeaderTimeout`. The heartbeat keeps the lease alive even when no bytes are
   moving, so a stalled connection that is never reset holds a key until the 6 h
   `copyTimeout`. **Fixed:** every read, upload and commit attempt has a deadline
   of 60 s + bytes / 256 KiB/s; a stall becomes a retryable `ErrStalled`.
5. `portage run` exits if the destination is unreachable at start. Run it under a
   supervisor that restarts it. **Fixed:** a pipeline that fails its start checks
   retries in the background (15 s, doubling to 5 min) while the engine and other
   pipelines keep running. A missing database is still fatal.
