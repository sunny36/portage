-- Per-pipeline state that outlives a single engine process.
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_id      text        PRIMARY KEY,
    -- First time any engine ran this pipeline. With existing_files: skip,
    -- objects older than this (and never seen by an event) are not copied.
    first_started_at timestamptz NOT NULL DEFAULT now()
);
