-- Pipelines from the database (pipelines_from: database). The contract with
-- control planes: docs/adr/0004-pipelines-from-database.md.
CREATE TABLE IF NOT EXISTS pipeline_spec (
    name        text        PRIMARY KEY,   -- same rules as pipeline.yaml names
    spec        jsonb       NOT NULL,      -- one pipeline, same shape as a
                                           -- pipelines[] entry in pipeline.yaml
    enabled     boolean     NOT NULL DEFAULT true,
    revision    bigint      NOT NULL DEFAULT 1,  -- bump on every change
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Engines LISTEN on portage_pipelines; the payload is the pipeline name.
-- They also re-read the table periodically, so a missed notification only
-- delays a change.
CREATE OR REPLACE FUNCTION portage_pipeline_spec_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('portage_pipelines', OLD.name);
        RETURN OLD;
    END IF;
    PERFORM pg_notify('portage_pipelines', NEW.name);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS pipeline_spec_notify ON pipeline_spec;
CREATE TRIGGER pipeline_spec_notify
    AFTER INSERT OR UPDATE OR DELETE ON pipeline_spec
    FOR EACH ROW EXECUTE FUNCTION portage_pipeline_spec_notify();
