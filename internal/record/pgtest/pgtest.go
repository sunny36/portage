// Package pgtest gives integration tests a pgx pool isolated in a fresh
// Postgres schema, because the test Postgres is shared by parallel runs.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sunny36/portage/internal/testenv"
)

// NewPool creates schema portage_test_<random>, returns a pool whose
// connections use it as search_path, and drops it (CASCADE) on cleanup.
func NewPool(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	schema := "portage_test_" + hex.EncodeToString(b)

	admin, err := pgxpool.New(ctx, testenv.PostgresURL())
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatalf("pgtest: create schema (is Postgres up at %s?): %v", testenv.PostgresURL(), err)
	}

	cfg, err := pgxpool.ParseConfig(testenv.PostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 30
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("pgtest: drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	return pool, schema
}
