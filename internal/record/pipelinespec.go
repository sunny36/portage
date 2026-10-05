package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PipelinesChannel is the NOTIFY channel the pipeline_spec trigger signals
// on insert, update and delete; the payload is the pipeline name.
const PipelinesChannel = "portage_pipelines"

// PipelineSpec is one row of pipeline_spec (pipelines_from: database). Spec
// is the pipeline as JSON in the pipeline.yaml format; the engine parses it
// with config.ParsePipelineSpec.
type PipelineSpec struct {
	Name      string          `json:"name"`
	Spec      json.RawMessage `json:"spec"`
	Enabled   bool            `json:"enabled"`
	Revision  int64           `json:"revision"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Querier is what the pipeline_spec helpers need: a *pgxpool.Pool, a
// *pgx.Conn or a pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ListPipelineSpecs returns every row, ordered by name.
func ListPipelineSpecs(ctx context.Context, q Querier) ([]PipelineSpec, error) {
	rows, err := q.Query(ctx, `
		SELECT name, spec, enabled, revision, updated_at FROM pipeline_spec ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("read pipeline_spec: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (PipelineSpec, error) {
		var s PipelineSpec
		var spec []byte
		err := r.Scan(&s.Name, &spec, &s.Enabled, &s.Revision, &s.UpdatedAt)
		s.Spec = spec
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("read pipeline_spec: %w", err)
	}
	return out, nil
}

// UpsertPipelineSpec inserts a pipeline or replaces its spec, bumping the
// revision. An identical spec is left alone (no revision bump, so engines
// don't restart the pipeline); changed reports whether anything was
// written. enabled is not touched on update.
func UpsertPipelineSpec(ctx context.Context, q Querier, name string, spec []byte) (revision int64, changed bool, err error) {
	err = q.QueryRow(ctx, `
		INSERT INTO pipeline_spec (name, spec) VALUES ($1, $2::jsonb)
		ON CONFLICT (name) DO UPDATE
			SET spec = EXCLUDED.spec, revision = pipeline_spec.revision + 1, updated_at = now()
			WHERE pipeline_spec.spec IS DISTINCT FROM EXCLUDED.spec
		RETURNING revision`, name, string(spec)).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		// Unchanged: the WHERE skipped the update.
		err = q.QueryRow(ctx, `SELECT revision FROM pipeline_spec WHERE name = $1`, name).Scan(&revision)
		return revision, false, err
	}
	if err != nil {
		return 0, false, fmt.Errorf("upsert pipeline_spec %s: %w", name, err)
	}
	return revision, true, nil
}

// SetPipelineEnabled enables or disables a pipeline, bumping its revision
// when the flag changes. found is false if there is no such row.
func SetPipelineEnabled(ctx context.Context, q Querier, name string, enabled bool) (found, changed bool, err error) {
	tag, err := q.Exec(ctx, `
		UPDATE pipeline_spec SET enabled = $2, revision = revision + 1, updated_at = now()
		WHERE name = $1 AND enabled <> $2`, name, enabled)
	if err != nil {
		return false, false, fmt.Errorf("update pipeline_spec %s: %w", name, err)
	}
	if tag.RowsAffected() > 0 {
		return true, true, nil
	}
	err = q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pipeline_spec WHERE name = $1)`, name).Scan(&found)
	return found, false, err
}

// DeletePipelineSpec removes a pipeline's row; its file records stay.
// found is false if there was no such row.
func DeletePipelineSpec(ctx context.Context, q Querier, name string) (found bool, err error) {
	tag, err := q.Exec(ctx, `DELETE FROM pipeline_spec WHERE name = $1`, name)
	if err != nil {
		return false, fmt.Errorf("delete pipeline_spec %s: %w", name, err)
	}
	return tag.RowsAffected() > 0, nil
}
