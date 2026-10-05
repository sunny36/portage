# Handoff: Portage engine

State at `7060c61` (2026-10-05). CI on `main` is green (lint, unit, integration).
Read this first, then the documents it points to. It records context that isn't
already in the ADRs, commits or setup guides.

## Where things stand
- **Built and tested:** Azure Blob source → S3 / OCI / S3-compatible destination;
  Event Grid events (Storage Queue or webhook) plus a periodic reconciler;
  resumable parallel multipart; per-part MD5, full SHA-256 in the file record,
  sampled read-back; newest-version-wins; deletes off by default; pipelines from
  a YAML file or from the database (`pipeline_spec`, ADR 0004); CLI `run`,
  `status`, `validate`, `check`, `pipelines`; Prometheus metrics + Grafana
  dashboard (`deploy/grafana/`).
- **Verified locally:** end-to-end, failure-mode and soak tests against the
  emulators. Soak results and the bugs it found (all fixed) are in
  `bench/results/local-soak-2026-10-05.md`.
- **Not yet done:** nothing has run against a real cloud. The setup scripts
  (`deploy/azure/setup.sh`, `deploy/oci/setup.sh`) were syntax-checked and
  checked against the CLIs' `--help`, but never run. No release has been tagged.

## Next steps, in order
1. **Real-cloud run**: follow `docs/setup/first-real-run.md` (Azure Southeast
   Asia → OCI `ap-singapore-1`). Run `portage check`, then the opt-in real-OCI
   connector conformance test (`PORTAGE_TEST_OCI_*`, see
   `internal/connector/s3/oci_integration_test.go`), then the 24 h soak at
   500 GB/day with `bench/cmd/portage-loadgen` + `bench/cmd/portage-verify`.
   Pass criteria: zero missing or mismatched files; p95 lag < 60 s for files
   < 1 GB. Commit the report to `bench/results/`.
2. **Tag `v0.1.0`** once the soak passes (GoReleaser config and
   `.github/workflows/release.yml` are ready; never run on a real tag). Put the
   soak numbers in the README.
3. **Grant-based auth modes** for connectors: AWS assume-role with external ID,
   Azure Entra app (workload identity), GCS service-account impersonation, OCI
   cross-tenancy. A control plane can only offer least-privilege grants once
   these exist; today specs fall back to keys or default credentials.
4. Roadmap items in the README: S3 / GCS / OCI as sources, SFTP both ways, Azure
   as destination, inventory-report reconciliation.

## Known limitations (deliberate, documented, not bugs)
- Lease identity is (key, version), not a claim token: see the note at the end
  of ADR 0002.
- Multipart uploads to S3 can't carry `portagesha256` metadata; the file record
  holds the hash (ADR 0002).
- User metadata from the source isn't copied to the destination.
- River keeps completed jobs for 24 h, so `river_job` holds about one day of
  history (~9 MB per 3.3k files per hour in the soak).
- In database mode the reconcile queue is fixed at 16 workers.
- The Azure queue source needs to create or write `<queue>-poison`;
  `deploy/azure/setup.sh` pre-creates it and grants Storage Queue Data
  Contributor on it.

## Working in this repo
- **Contracts first.** `internal/connector/connector.go` plus the conformance
  suite `internal/connector/connectortest` define every connector; a new
  connector must pass `connectortest.Run`. Record decisions as ADRs in
  `docs/adr/`.
- **Local stack.** Quickstart is in `README.md`. Emulators use non-default host ports (Postgres 25432, Azurite 20000/20001,
  SeaweedFS 28333) and `network_mode: bridge`, because the original dev
  machine had port clashes and no free Docker address pools. Override them with
  the `PORTAGE_*` variables in `deploy/docker-compose.yml` and
  `internal/testenv`.
- **SeaweedFS, not MinIO.** MinIO stopped publishing public images in 2025.
  SeaweedFS runs with `-volume.max=0`: a fixed volume cap filled up and caused
  HTTP 500s in parallel tests.
- **Tests.** `make test` (unit), `make itest` (compose up + integration), and
  `make lint` must report 0 issues. `internal/pipeline` integration tests take
  about 2.5 min and are load-sensitive: under heavy machine load the
  failure-mode tests can time out. Rerun the package on its own before
  suspecting the code.
- **Commits.** The repo's git config sets the author to the maintainer's GitHub
  noreply address. Keep it that way, and don't add AI co-author trailers.

## Suggested skills
- `tdd`: for new connectors and auth modes; the conformance suite is the spec.
- `diagnose`: for anything the real-cloud soak turns up.
- `code-review` / `simplify`: before tagging `v0.1.0`.
- `security-review`: before the release (credential handling, `secret://`
  resolution, webhook auth).
