# ADR 0002: Connector interface and file record

Status: accepted (2026-10-05)

## Connector
`internal/connector/connector.go` is the only way the engine touches storage.
One Connector = one container/bucket scoped to a prefix; keys are relative.

- `Version` is the ETag on every provider (never an S3 version ID), so List
  and Stat agree and the reconciler can compare listings to the file record.
- Reads: `List(prefix, cursor, limit)` (recursive, key-ordered),
  `Stat`, `OpenRange(key, version, offset, length)` with If-Match on version.
- Writes: `PutObject` for small objects, `BeginUpload/ResumeUpload` →
  `UploadPart` (concurrent, idempotent per part number) → `Complete`/`Abort`.
  Azure maps this to Put Block / Put Block List with deterministic block IDs;
  S3 to multipart upload.
- Errors wrap sentinels (`ErrNotFound`, `ErrThrottled`, `ErrVersionChanged`,
  `ErrPermission`, `ErrAuth`) so callers use `errors.Is`.
- Every object Portage writes carries user metadata `portagesha256` (hex).
  No hyphen/underscore: Azure needs C#-identifier names.
- Behaviour is pinned by `internal/connector/connectortest`, which every
  connector's integration test must pass.

Local emulators: Azurite (Azure), SeaweedFS (S3 API). MinIO stopped publishing
public images in 2025, so SeaweedFS replaces it. OCI has no emulator; its S3
API is tested against a real free-tier bucket.

## File record
`internal/record/migrations/0001_file_record.sql`, one row per
(pipeline, key). Copy decisions (`record.Decide`, applied atomically by
`Store.Claim`):

1. Worker always `Stat`s the source first and copies the *current* version,
   so an event for an old version never causes an old copy.
2. If `synced_version == candidate.version` → `already_synced` (skip).
3. Ordering, newest wins: if both candidate and record have a sequencer,
   compare sequencers (left-pad to equal length, then lexical). Otherwise
   compare mtime. Candidate older than what is synced or being copied →
   `stale` (skip). Equal mtime with a different version → copy.
4. A key with an unexpired lease (`status=copying`, `claimed_until > now`)
   held for another version → `busy` (job snoozes and retries).
5. Lease expiry lets another worker take over after a crash; it resumes the
   multipart session if `upload_version == candidate.version`.
6. `Complete` succeeds only while the caller still holds the lease for that
   version (`ErrLeaseLost` otherwise), so a slow worker can't overwrite a
   newer completion.

Repeating any step is safe: same key + same version is a no-op or a resume.

### Known limitation (v0.1)
Leases are identified by (key, version), not a per-claim token. If worker A's
lease expires and worker B takes over the same version, a late `Complete`
from A still succeeds (and B then gets `ErrLeaseLost`). Both copied identical
bytes of the same version, so the result is correct; they may briefly share a
multipart session. A claim token can be added to `ExtendLease`/`SetUpload`/
`Complete`/`Fail` later if this shows up in failure tests.
