package check

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBInfo is what the database check learns.
type DBInfo struct {
	ServerVersion string
	// AppliedMigrations counts Portage's own schema migrations applied
	// (0 when the migrations table does not exist yet).
	AppliedMigrations int
	// QueueSchema reports whether River's tables exist.
	QueueSchema bool
}

// probeDB connects, reads the server version and looks for Portage's and
// River's tables. It changes nothing (`portage run` applies migrations).
func probeDB(ctx context.Context, url string) (DBInfo, error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return DBInfo{}, err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	var info DBInfo
	if err := conn.QueryRow(ctx, `SHOW server_version`).Scan(&info.ServerVersion); err != nil {
		return info, err
	}
	var haveMigrations bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('portage_schema_migrations') IS NOT NULL,
		to_regclass('river_job') IS NOT NULL`).Scan(&haveMigrations, &info.QueueSchema); err != nil {
		return info, err
	}
	if haveMigrations {
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM portage_schema_migrations`).Scan(&info.AppliedMigrations); err != nil {
			return info, err
		}
	}
	return info, nil
}

func (r *runner) database(ctx context.Context, url string) Result {
	return r.timed(ctx, "", CheckDatabase, func(ctx context.Context) Result {
		info, err := r.opts.ProbeDB(ctx, url)
		if err != nil {
			return Result{Status: StatusFail, Detail: describeErr(err), Fix: dbHint(err)}
		}
		detail := "PostgreSQL " + info.ServerVersion
		if info.AppliedMigrations == 0 && !info.QueueSchema {
			return ok(detail + "; no Portage tables yet (`portage run` creates them on start)")
		}
		queue := "queue tables present"
		if !info.QueueSchema {
			queue = "queue tables missing (created on start)"
		}
		return ok(fmt.Sprintf("%s; %d Portage migration(s) applied, %s", detail, info.AppliedMigrations, queue))
	})
}

func dbHint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01", "28000":
			return "Postgres rejected the user/password in database_url (or pg_hba.conf has no matching entry)."
		case "3D000":
			return "The database in database_url does not exist: create it (`createdb portage`) or fix the name."
		case "42501":
			return "The database user lacks privileges; Portage needs to create tables in its schema on first run."
		}
	}
	msg := err.Error()
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr):
		return "The database host in database_url does not resolve."
	case strings.Contains(msg, "connection refused"):
		return "Nothing is listening at the database_url host:port. Is Postgres running and reachable from here?"
	case strings.Contains(msg, "SSL") || strings.Contains(msg, "TLS") || strings.Contains(msg, "no encryption"):
		return "TLS mismatch: managed Postgres (e.g. Azure Database for PostgreSQL) needs `?sslmode=require` in database_url."
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "timeout"):
		return "Timed out connecting to Postgres: check the host, port and firewall rules between this machine and the database."
	}
	return "Check database_url (postgres://user:pass@host:5432/db?sslmode=...)."
}
