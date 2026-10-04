-- File record: one row per (pipeline, source key). Every copy decision is made
-- against this table. See docs/adr/0002-connector-and-file-record.md.

CREATE TABLE IF NOT EXISTS file_record (
    pipeline_id       text        NOT NULL,
    key               text        NOT NULL,

    -- Latest source version we know about (from an event or a Stat).
    source_version    text        NOT NULL DEFAULT '',
    source_size       bigint      NOT NULL DEFAULT 0,
    source_mtime      timestamptz,
    source_sequencer  text        NOT NULL DEFAULT '',

    -- Last version successfully copied and verified.
    synced_version    text        NOT NULL DEFAULT '',
    synced_mtime      timestamptz,
    synced_sequencer  text        NOT NULL DEFAULT '',
    sha256            bytea,
    dest_version      text        NOT NULL DEFAULT '',

    status            text        NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'copying', 'synced', 'failed', 'deleted')),
    attempts          integer     NOT NULL DEFAULT 0,
    last_error        text        NOT NULL DEFAULT '',

    -- Resumable multipart session for the version being copied.
    upload_id         text        NOT NULL DEFAULT '',
    upload_version    text        NOT NULL DEFAULT '',
    -- Lease so a crashed worker's claim expires and another can take over.
    claimed_until     timestamptz,

    -- Lag measurement: provider event time of the change being copied, and
    -- when it was verified at the destination.
    event_time        timestamptz,
    synced_at         timestamptz,

    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (pipeline_id, key)
);

CREATE INDEX IF NOT EXISTS file_record_status_idx
    ON file_record (pipeline_id, status)
    WHERE status <> 'synced';
