# ADR 0003: Change sources for a self-hosted engine

Status: accepted (2026-10-05)

## Context
Hosted Portage will receive Azure Event Grid events on an HTTPS webhook.
A self-hosted `portage run` usually has no public HTTPS endpoint.

## Decision
All sources implement `change.Source` and produce `change.ObjectChanged`.

- `azure_queue` (default for Azure sources): Event Grid subscription delivers
  `Microsoft.Storage.BlobCreated` / `BlobDeleted` to an Azure Storage Queue;
  the engine polls it. Nothing inbound is needed. Messages are deleted only
  after `Emit` returns nil (at-least-once).
- `webhook`: Event Grid → HTTPS, including the subscription validation
  handshake. Kept for hosted mode.
- `none`: reconciler only.
- The reconciler runs for every pipeline regardless, diffing the source
  listing against the file record. It catches lost events and performs the
  initial copy (`existing_files: copy`).

Events carry the blob URL, eTag, size, sequencer and event time. Sync lag is
measured from event time to verified-at-destination.
