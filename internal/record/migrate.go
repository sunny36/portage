package record

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateLockKey is the pg_advisory_xact_lock key that serialises concurrent
// Migrate calls (several engine processes starting at once). Arbitrary
// constant: "portage1" in ASCII.
const migrateLockKey int64 = 0x706f727461676531

// Migrate applies the embedded migrations (migrations/NNNN_*.sql) in order.
// Applied versions are tracked in portage_schema_migrations, so repeated
// calls are no-ops. Everything runs in one transaction under an advisory
// lock, so concurrent engine starts are safe. Tables are created in the
// first schema of the connection's search_path.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	names, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockKey); err != nil {
			return fmt.Errorf("record: migrate lock: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS portage_schema_migrations (
			version    text        PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
			return fmt.Errorf("record: create migrations table: %w", err)
		}
		for _, name := range names {
			version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
			var applied bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM portage_schema_migrations WHERE version = $1)`,
				version).Scan(&applied); err != nil {
				return fmt.Errorf("record: check migration %s: %w", version, err)
			}
			if applied {
				continue
			}
			sql, err := migrationsFS.ReadFile(name)
			if err != nil {
				return err
			}
			// No arguments: pgx uses the simple protocol, which allows
			// several statements per file.
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("record: apply migration %s: %w", version, err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO portage_schema_migrations (version) VALUES ($1)`, version); err != nil {
				return fmt.Errorf("record: record migration %s: %w", version, err)
			}
		}
		return nil
	})
}
