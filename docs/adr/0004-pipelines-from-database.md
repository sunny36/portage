# ADR 0004: Pipelines from the database (control-plane contract)

Status: accepted (2026-10-05)

## Context
`portage run -c pipeline.yaml` reads pipelines from a file at start-up. A
hosted service (and larger self-hosted setups) needs to add, change and
remove pipelines while workers keep running, from something other than a
file. The engine must not know about tenants, users, billing or any portal:
those live outside this repository.

## Decision
The engine can take its pipelines from a Postgres table instead of (not in
addition to) the YAML file's `pipelines:` list:

```yaml
version: 1
database_url: postgres://…
pipelines_from: database      # default: file
```

### Table (owned and migrated by the engine)
```sql
CREATE TABLE pipeline_spec (
    name        text        PRIMARY KEY,   -- same rules as pipeline.yaml names
    spec        jsonb       NOT NULL,      -- one pipeline, same shape as a
                                           -- pipelines[] entry in pipeline.yaml
    enabled     boolean     NOT NULL DEFAULT true,
    revision    bigint      NOT NULL DEFAULT 1,  -- bump on every change
    updated_at  timestamptz NOT NULL DEFAULT now()
);
```
- `spec` uses the YAML field names (`source`, `destination`, `events`,
  `filters`, `concurrency`, `part_size`, `reconcile_interval` as a Go
  duration string such as `"15m"`, …). The engine validates it with the same
  rules as the file; an invalid spec is skipped and reported (log, metric
  `portage_pipeline_config_errors`), never applied, and never stops other
  pipelines.
- `name` in the row is authoritative; a `name` inside `spec` is ignored.
- Writers bump `revision` on every update. A trigger sends
  `NOTIFY portage_pipelines, '<name>'` on insert, update and delete, and the
  engine also re-reads the table every 30 s, so a missed notification only
  delays a change.

### Secrets
A spec must not hold credentials in plain text. Any string value of the form
`secret://<name>` is resolved by the engine at load time, from (in order):
1. a file `$PORTAGE_SECRETS_DIR/<name>` (mounted secrets: Kubernetes, ECS,
   Container Apps, systemd credentials), then
2. the environment variable `PORTAGE_SECRET_<NAME>` (upper-cased, `-`/`.`
   become `_`).

An unresolvable reference makes that pipeline invalid (reported as above).
Grant-based auth modes (AWS role + external ID, Entra app consent, GCS
service account, OCI cross-tenancy) will be added as connector auth options
later and need no secret in the spec.

### Runtime behaviour
- Added or enabled pipeline: build connectors, run `portage check`-style
  start checks, add its River queue and periodic reconcile, start its
  change source.
- Changed (revision bump): stop the old instance of that pipeline gracefully
  (running copies finish or release their lease), start the new one. The
  file record is keyed by pipeline name, so history carries over.
- Removed or disabled: stop it gracefully; its file records stay.
- One bad pipeline never stops the others or the engine.

### What stays outside the engine
Tenants, users, auth, connection setup flows, grant generation, billing and
any UI. A control plane (for example a hosted portal) owns its own tables, in
its own Postgres schema, and talks to the engine only through
`pipeline_spec` (write) and the read-only status surface: `file_record`,
`portage status --json`, and the Prometheus metrics.

## Consequences
The engine stays single-purpose and fully usable on its own; a closed-source
control plane can drive a fleet of open-source workers without importing
engine code. The spec format is the YAML format, so there is one schema to
document and validate.
