//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sunny36/portage/internal/record"
	"github.com/sunny36/portage/internal/record/pgtest"
	"github.com/sunny36/portage/internal/testenv"
)

// dbModeConfig is a pipelines_from: database config on the test schema.
func dbModeConfig(t *testing.T, schema string) string {
	t.Helper()
	sep := "?"
	if strings.Contains(testenv.PostgresURL(), "?") {
		sep = "&"
	}
	return writeConfig(t, fmt.Sprintf("version: 1\ndatabase_url: \"%s%ssearch_path=%s\"\npipelines_from: database\n",
		testenv.PostgresURL(), sep, schema))
}

func TestPipelinesCommandsIntegration(t *testing.T) {
	pool, schema := pgtest.NewPool(t)
	cfg := dbModeConfig(t, schema)
	t.Setenv("PORTAGE_SECRETS_DIR", "")
	t.Setenv("PORTAGE_SECRET_S3_ID", "id")
	t.Setenv("PORTAGE_SECRET_S3_KEY", "key")

	// Before anything created the table.
	if code, _, errOut := runCLI(t, "pipelines", "list", "-c", cfg); code != 1 || !strings.Contains(errOut, "no pipeline_spec table") {
		t.Errorf("list without table: exit %d: %s", code, errOut)
	}
	if code, out, _ := runCLI(t, "status", "-c", cfg); code != 0 || !strings.Contains(out, noDataMsg) {
		t.Errorf("status without table: exit %d: %s", code, out)
	}

	file := writeConfig(t, applyFile)
	code, out, errOut := runCLI(t, "pipelines", "apply", "-c", cfg, "-f", file)
	if code != 0 || !strings.Contains(out, "alpha: created (revision 1)") {
		t.Fatalf("apply: exit %d: %s%s", code, out, errOut)
	}
	if _, out, _ = runCLI(t, "pipelines", "apply", "-c", cfg, "-f", file); !strings.Contains(out, "alpha: unchanged (revision 1)") {
		t.Errorf("re-apply: %s", out)
	}
	file2 := writeConfig(t, strings.Replace(applyFile, "5m", "10m", 1))
	if _, out, _ = runCLI(t, "pipelines", "apply", "-c", cfg, "-f", file2); !strings.Contains(out, "alpha: updated (revision 2)") {
		t.Errorf("apply change: %s", out)
	}
	// An invalid file writes nothing.
	bad := writeConfig(t, applyFile+strings.Replace(strings.TrimPrefix(applyFile, "pipelines:\n"), "alpha", "beta", 1)+"    bogus: 1\n")
	if code, _, errOut := runCLI(t, "pipelines", "apply", "-c", cfg, "-f", bad); code != 1 || !strings.Contains(errOut, "bogus") {
		t.Errorf("bad apply: exit %d: %s", code, errOut)
	}
	rows, err := record.ListPipelineSpecs(context.Background(), pool)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %+v, %v", rows, err)
	}

	// A row written directly (as a control plane would), with a problem.
	if _, _, err := record.UpsertPipelineSpec(context.Background(), pool, "broken", []byte(`{"source":{}}`)); err != nil {
		t.Fatal(err)
	}

	code, out, _ = runCLI(t, "pipelines", "list", "-c", cfg)
	if code != 0 || !strings.Contains(out, "alpha") || !strings.Contains(out, "azure:c/in/  →  s3:b/") || !strings.Contains(out, "INVALID") {
		t.Errorf("list: exit %d:\n%s", code, out)
	}
	code, out, _ = runCLI(t, "validate", "-c", cfg)
	if code != 1 || !strings.Contains(out, "Pipeline alpha (revision 2)") || !strings.Contains(out, "Pipeline broken (revision 1)\n  INVALID") ||
		!strings.Contains(out, "1 of 2 pipeline(s) invalid") {
		t.Errorf("validate: exit %d:\n%s", code, out)
	}
	if strings.Contains(out, "key") && strings.Contains(out, "secret_access_key: key") {
		t.Errorf("validate leaks a secret:\n%s", out)
	}

	if code, out, _ = runCLI(t, "pipelines", "disable", "-c", cfg, "alpha"); code != 0 || !strings.Contains(out, "alpha: disabled") {
		t.Errorf("disable: exit %d: %s", code, out)
	}
	if _, out, _ = runCLI(t, "pipelines", "disable", "-c", cfg, "alpha"); !strings.Contains(out, "already disabled") {
		t.Errorf("disable again: %s", out)
	}
	if code, _, errOut := runCLI(t, "pipelines", "enable", "-c", cfg, "nope"); code != 1 || !strings.Contains(errOut, "no such pipeline: nope") {
		t.Errorf("enable missing: exit %d: %s", code, errOut)
	}

	code, out, _ = runCLI(t, "status", "-c", cfg, "--json")
	if code != 0 {
		t.Fatalf("status: exit %d", code)
	}
	var rep statusReport
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Pipelines) != 2 || rep.Pipelines[0].Name != "alpha" || rep.Pipelines[0].Enabled == nil ||
		*rep.Pipelines[0].Enabled || rep.Pipelines[0].Revision != 3 {
		t.Errorf("status pipelines = %+v", rep.Pipelines)
	}
	if _, out, _ = runCLI(t, "status", "-c", cfg); !strings.Contains(out, "alpha (disabled)") {
		t.Errorf("status table:\n%s", out)
	}

	// check fails the invalid enabled row without contacting anything for it.
	code, out, _ = runCLI(t, "check", "-c", cfg, "--pipeline", "broken")
	if code != 1 || !strings.Contains(out, "Pipeline broken") || !strings.Contains(out, "spec") {
		t.Errorf("check broken: exit %d:\n%s", code, out)
	}

	if code, out, _ = runCLI(t, "pipelines", "delete", "-c", cfg, "alpha", "broken"); code != 0 || !strings.Contains(out, "broken: deleted") {
		t.Errorf("delete: exit %d: %s", code, out)
	}
	if code, out, _ = runCLI(t, "validate", "-c", cfg); code != 0 || !strings.Contains(out, "No pipelines in pipeline_spec") {
		t.Errorf("validate empty: exit %d:\n%s", code, out)
	}
}
