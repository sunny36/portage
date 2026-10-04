# Portage

Continuous one-way sync between object stores, across clouds.

Portage keeps a destination bucket in step with a source bucket: every new or
changed file is copied, checksum-verified and recorded, driven by the source's
change events and backed by a reconciler that catches anything missed. It's
built for pipelines that run indefinitely at hundreds of GB/day.

> **Status: pre-release.** v0.1 targets Azure Blob Storage → Amazon S3 /
> OCI Object Storage. GCS and SFTP follow.

## Guarantees
- **Newest version wins.** An older version never overwrites a newer one.
- **Checksum-verified.** Every part is checked by the destination on upload
  (Content-MD5). The SHA-256 of the full content is computed while streaming
  and kept in the file record, and sampled ranges are read back from the
  destination and compared. Objects small enough for a single upload also
  carry the hash as `portagesha256` metadata.
- **Safe to retry.** A crash mid-copy resumes the multipart upload; nothing is
  copied twice or skipped.
- **Deletes off by default.**

## Development
```sh
docker compose -f deploy/docker-compose.yml up -d --wait   # Postgres, Azurite, SeaweedFS
go test ./...                                              # unit tests
go test -tags integration ./...                            # against the emulators
```
Design decisions live in [docs/adr](docs/adr).

## License
Apache-2.0
