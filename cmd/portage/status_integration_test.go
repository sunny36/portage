//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/record/pgtest"
	"github.com/sunny36/portage/internal/testenv"
)

// statusConfig points database_url at the test schema via search_path.
func statusConfig(t *testing.T, schema string) string {
	t.Helper()
	sep := "?"
	if strings.Contains(testenv.PostgresURL(), "?") {
		sep = "&"
	}
	return writeConfig(t, fmt.Sprintf(`version: 1
database_url: "%s%ssearch_path=%s"
pipelines:
  - name: alpha
    source:
      azure: {account_url: "https://a.blob.core.windows.net", container: c}
    destination:
      s3: {bucket: b, region: r}
    events: {type: none}
  - name: beta
    source:
      azure: {account_url: "https://a.blob.core.windows.net", container: c}
    destination:
      s3: {bucket: b, region: r}
    events: {type: none}
`, testenv.PostgresURL(), sep, schema))
}

func TestStatusIntegration(t *testing.T) {
	ctx := context.Background()
	pool, schema := pgtest.NewPool(t)
	if err := record.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := record.NewPGStore(pool)
	now := time.Now().UTC()
	cand := func(key string, size int64, age time.Duration) record.Candidate {
		return record.Candidate{PipelineID: "alpha", Key: key, Version: "v1", Size: size,
			MTime: now.Add(-age), EventTime: now.Add(-age)}
	}

	// synced
	c := cand("done.bin", 3<<20, time.Minute)
	if d, _, err := s.Claim(ctx, c, time.Minute); err != nil || d != record.DecisionCopy {
		t.Fatalf("claim: %v %v", d, err)
	}
	if err := s.Complete(ctx, "alpha", c.Key, c.Version, record.SyncResult{SHA256: make([]byte, 32), SyncedAt: now}); err != nil {
		t.Fatal(err)
	}
	// pending for ~2 minutes
	if err := s.Observe(ctx, cand("waiting.bin", 10, 2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// failed permanently
	c = cand("broken.bin", 10, time.Minute)
	if _, _, err := s.Claim(ctx, c, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, "alpha", c.Key, c.Version, errors.New("403 AccessDenied"), false); err != nil {
		t.Fatal(err)
	}

	path := statusConfig(t, schema)
	code, out, errOut := runCLI(t, "status", "-c", path)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, want := range []string{"alpha", "beta", "3.0 MiB", "never", "Recent errors — alpha:", "broken.bin", "403 AccessDenied"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q:\n%s", want, out)
		}
	}
	t.Logf("\n%s", out)

	code, out, errOut = runCLI(t, "status", "-c", path, "--json")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	var rep statusReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("%v:\n%s", err, out)
	}
	a := rep.Pipelines[0]
	if a.Name != "alpha" || a.Synced != 1 || a.Pending != 1 || a.Failed != 1 || a.BytesSynced != 3<<20 {
		t.Errorf("alpha = %+v", a)
	}
	if a.CurrentLagSeconds < 100 || a.CurrentLagSeconds > 600 {
		t.Errorf("alpha current lag = %vs, want ~120s", a.CurrentLagSeconds)
	}
	if len(a.RecentErrors) != 1 || a.RecentErrors[0].Key != "broken.bin" {
		t.Errorf("recent errors = %+v", a.RecentErrors)
	}
	if b := rep.Pipelines[1]; b.Synced != 0 || b.LastSyncedAt != nil {
		t.Errorf("beta = %+v", b)
	}
}

func TestStatusIntegrationNoTables(t *testing.T) {
	_, schema := pgtest.NewPool(t) // fresh schema, never migrated
	code, out, errOut := runCLI(t, "status", "-c", statusConfig(t, schema))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, noDataMsg) {
		t.Errorf("out = %q", out)
	}
}
