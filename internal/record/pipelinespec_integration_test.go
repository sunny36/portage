//go:build integration

package record_test

import (
	"context"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/record/pgtest"
)

func TestPipelineSpecTableAndNotify(t *testing.T) {
	ctx := context.Background()
	pool, _ := pgtest.NewPool(t)
	if err := record.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := record.Migrate(ctx, pool); err != nil { // idempotent
		t.Fatal(err)
	}

	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+record.PipelinesChannel); err != nil {
		t.Fatal(err)
	}
	// The channel is database-wide and other tests share the database, so
	// wait for our own payload.
	expect := func(name string) {
		t.Helper()
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		for {
			n, err := listener.Conn().WaitForNotification(wctx)
			if err != nil {
				t.Fatalf("no notification for %s: %v", name, err)
			}
			if n.Payload == name {
				return
			}
		}
	}
	const name = "spec-notify-test"

	rev, changed, err := record.UpsertPipelineSpec(ctx, pool, name, []byte(`{"events": {"type": "none"}}`))
	if err != nil || rev != 1 || !changed {
		t.Fatalf("insert: rev %d changed %v err %v", rev, changed, err)
	}
	expect(name)
	// Same spec (different JSON formatting): no revision bump.
	if rev, changed, err = record.UpsertPipelineSpec(ctx, pool, name, []byte(`{"events":{"type":"none"}}`)); err != nil || rev != 1 || changed {
		t.Fatalf("unchanged upsert: rev %d changed %v err %v", rev, changed, err)
	}
	if rev, changed, err = record.UpsertPipelineSpec(ctx, pool, name, []byte(`{"events":{"type":"webhook"}}`)); err != nil || rev != 2 || !changed {
		t.Fatalf("update: rev %d changed %v err %v", rev, changed, err)
	}
	expect(name)
	if found, changed, err := record.SetPipelineEnabled(ctx, pool, name, false); err != nil || !found || !changed {
		t.Fatalf("disable: %v %v %v", found, changed, err)
	}
	expect(name)
	if found, changed, err := record.SetPipelineEnabled(ctx, pool, name, false); err != nil || !found || changed {
		t.Fatalf("disable again: %v %v %v", found, changed, err)
	}
	rows, err := record.ListPipelineSpecs(ctx, pool)
	if err != nil || len(rows) != 1 || rows[0].Enabled || rows[0].Revision != 3 {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
	if found, err := record.DeletePipelineSpec(ctx, pool, name); err != nil || !found {
		t.Fatalf("delete: %v %v", found, err)
	}
	expect(name)
	if found, _, err := record.SetPipelineEnabled(ctx, pool, name, true); err != nil || found {
		t.Fatalf("enable deleted: %v %v", found, err)
	}
}
