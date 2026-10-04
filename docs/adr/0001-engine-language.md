# ADR 0001: Engine language is Go

Status: accepted (2026-10-05)

## Context
The engine streams objects between Azure Blob, S3, OCI Object Storage, GCS and
SFTP at hundreds of GB/day. Go and Rust were both considered.

As of Oct 2026, official Rust SDKs exist for AWS (GA 2023), Google Cloud
(1.0, Sep 2025) and Azure (1.0, May 2026). OCI has only community Rust crates.

## Decision
Go.

- Azure's Go SDK (`azblob`, `azqueue`) has years of production use; the Rust
  one is months old.
- OCI's S3 compatibility API has known gaps. The fallback, a native OCI
  connector, has an official SDK only in Go (`oci-go-sdk`).
- River (Postgres-backed job queue) and `pkg/sftp` are mature Go libraries with
  no Rust equivalent of the same maturity.
- rclone (Go) is a reference for multipart, throttling and provider quirks.
- The workload is I/O-bound (network, API rate limits). Rust's CPU advantage
  barely matters; GC pressure is handled with a bounded part-buffer pool.

## Consequences
Single static binary, cheap concurrency. Revisit only if benchmarks show GC
or memory to be the bottleneck at 2 TB bursts; the connector interface would
let a Rust transfer core replace the Go one.
