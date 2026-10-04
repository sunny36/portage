# Contributing to Portage

## Dev setup

- Go (version in `go.mod`), Docker with Compose v2, `make`.
- Optional: `golangci-lint` v2 (`make lint` falls back to `go run` if it isn't
  installed).

```sh
make up      # Postgres, Azurite, SeaweedFS via deploy/docker-compose.yml
make down    # stop them and drop their data
```

Emulator ports (25432, 20000/20001, 28333) can be overridden with the env vars
in `deploy/docker-compose.yml`; tests read the overrides listed in
`internal/testenv`.

## Tests

```sh
make test    # go test -race ./...           unit tests, no services needed
make itest   # go test -race -tags integration ./...   against the emulators
make lint    # golangci-lint (config in .golangci.yml)
make tidy    # go mod tidy
```

- Integration tests live in files with `//go:build integration` and get
  endpoints from `internal/testenv`. Use `testenv.UniqueName` for containers
  and buckets so tests can run in parallel and repeatedly.
- Every connector must pass `internal/connector/connectortest`.
- CI (`.github/workflows/ci.yml`) runs lint, unit and integration tests on
  every PR; all three must be green to merge.

## Design decisions (ADRs)

Significant decisions (interfaces, storage formats, dependencies, guarantees)
are recorded in `docs/adr/NNNN-short-title.md`: status, context, decision,
consequences. Add a new ADR in the same PR as the change, numbered after the
last one. To reverse a decision, write a new ADR that supersedes the old one
and update the old one's status; don't rewrite history.

## Commits and PRs

- Imperative, present-tense subject line, ≤72 chars, optionally scoped:
  `connector/azure: resume uploads from the committed block list`.
- Body explains *why*, not what. Reference issues (`Fixes #12`).
- Keep PRs focused; one logical change each. Squash-merged.
- New behaviour needs tests; bug fixes need a regression test.

## Releases

Push a `vX.Y.Z` tag. `.github/workflows/release.yml` runs GoReleaser, which
publishes binaries (linux/darwin, amd64/arm64), checksums and the
`ghcr.io/sunny36/portage` image. Dry run: `goreleaser release --snapshot --clean`.
