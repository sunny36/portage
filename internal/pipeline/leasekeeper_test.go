package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/record"
)

func TestLeaseKeeper(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := time.Unix(0, 0)
	now := func() time.Time { return clock }
	var next error
	hb := leaseKeeper(func(context.Context) error { return next }, log, now)
	ctx := context.Background()

	// A transient failure right after a renewal is tolerated.
	clock = clock.Add(heartbeatEvery)
	next = errors.New("db blip")
	if err := hb(ctx); err != nil {
		t.Fatalf("first blip: %v, want tolerated", err)
	}
	// Recovery resets the clock.
	clock = clock.Add(heartbeatEvery)
	next = nil
	if err := hb(ctx); err != nil {
		t.Fatal(err)
	}
	// Failures until less than one heartbeat of lease remains: then stop.
	next = errors.New("db down")
	clock = clock.Add(leaseDuration - heartbeatEvery - time.Second)
	if err := hb(ctx); err != nil {
		t.Fatalf("lease still valid: %v, want tolerated", err)
	}
	clock = clock.Add(2 * time.Second)
	if err := hb(ctx); err == nil {
		t.Fatal("lease nearly expired: want error")
	}
	// A lost lease always stops at once.
	hb = leaseKeeper(func(context.Context) error { return record.ErrLeaseLost }, log, now)
	if err := hb(ctx); !errors.Is(err, record.ErrLeaseLost) {
		t.Fatalf("lease lost: %v", err)
	}
}
