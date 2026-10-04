package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/record"
)

const examplePath = "../../examples/pipeline.yaml"

func runCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = execute(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestVersion(t *testing.T) {
	old := [3]string{version, commit, date}
	version, commit, date = "v1.2.3", "abc1234", "2026-10-05T00:00:00Z"
	t.Cleanup(func() { version, commit, date = old[0], old[1], old[2] })

	code, out, _ := runCLI(t, "version")
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	for _, want := range []string{"portage v1.2.3", "abc1234", "2026-10-05T00:00:00Z", runtime.Version()} {
		if !strings.Contains(out, want) {
			t.Errorf("version output missing %q:\n%s", want, out)
		}
	}
}

func TestValidateExample(t *testing.T) {
	code, out, errOut := runCLI(t, "validate", "-c", examplePath)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, want := range []string{
		"Pipeline azure-to-s3-local",
		"azure:source/incoming/  →  s3:dest/from-azure/",
		"events:       none",
		"include everything",
		"part size 8.0 MiB",
		"every 1m0s",
		"OK: 1 pipeline(s) valid.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("validate output missing %q:\n%s", want, out)
		}
	}
	// Secrets from the example must never be printed.
	for _, secret := range []string{"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1", "portage-secret", "portage:portage@"} {
		if strings.Contains(out, secret) {
			t.Errorf("validate output leaks %q:\n%s", secret, out)
		}
	}
}

func TestValidateRedactsWebhookSecretAndSAS(t *testing.T) {
	path := writeConfig(t, `version: 1
database_url: "host=db user=p password=hunter2 dbname=portage"
pipelines:
  - name: hook
    source:
      azure:
        account_url: https://acct.blob.core.windows.net/?sv=2024&sig=SASSIGNATURE
        container: c
    destination:
      s3: {bucket: b, region: r}
    events:
      type: webhook
      webhook_secret: supersecretwebhookvalue
    filters:
      include: ["*.parquet"]
`)
	code, out, errOut := runCLI(t, "validate", "-c", path)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, secret := range []string{"hunter2", "SASSIGNATURE", "supersecretwebhookvalue"} {
		if strings.Contains(out, secret) {
			t.Errorf("leaks %q:\n%s", secret, out)
		}
	}
	for _, want := range []string{"Webhooks:  :8080", "webhook path=/events/hook", "include *.parquet"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
}

func TestValidateInvalid(t *testing.T) {
	path := writeConfig(t, "version: 1\npipelines: []\n")
	code, _, errOut := runCLI(t, "validate", "-c", path)
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(errOut, "database_url: required") {
		t.Errorf("stderr = %q", errOut)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("PORTAGE_CONFIG", examplePath)
	code, out, errOut := runCLI(t, "validate")
	if code != 0 || !strings.Contains(out, "Config:    "+examplePath) {
		t.Fatalf("exit %d: %s%s", code, out, errOut)
	}
}

func TestRunReportsNotImplemented(t *testing.T) {
	path := writeConfig(t, strings.Replace(mustRead(t, examplePath), `metrics_addr: ":9090"`, `metrics_addr: "127.0.0.1:0"`, 1))
	code, _, errOut := runCLI(t, "run", "-c", path, "--log-format", "json")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(errOut, "not implemented") || !strings.Contains(errOut, `"msg":"starting portage"`) {
		t.Errorf("stderr = %s", errOut)
	}
}

func TestRunBadLogFlags(t *testing.T) {
	if code, _, errOut := runCLI(t, "run", "-c", examplePath, "--log-format", "xml"); code != 1 || !strings.Contains(errOut, "--log-format") {
		t.Errorf("exit %d: %s", code, errOut)
	}
	if code, _, errOut := runCLI(t, "run", "-c", examplePath, "--log-level", "loud"); code != 1 || !strings.Contains(errOut, "--log-level") {
		t.Errorf("exit %d: %s", code, errOut)
	}
}

type fakeStats map[string]record.Stats

func (f fakeStats) Stats(_ context.Context, id string) (record.Stats, error) { return f[id], nil }

type errStats struct{ err error }

func (e errStats) Stats(context.Context, string) (record.Stats, error) { return record.Stats{}, e.err }

func twoPipelines() *config.File {
	return &config.File{Pipelines: []config.Pipeline{{Name: "alpha"}, {Name: "beta"}}}
}

func TestStatusTable(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	store := fakeStats{
		"alpha": {
			Synced: 1200, Pending: 3, Copying: 2, Failed: 1, BytesSynced: 5 << 30,
			LastSyncedAt:       now.Add(-42 * time.Second),
			OldestPendingEvent: now.Add(-(3*time.Minute + 5*time.Second)),
			RecentErrors: []record.Record{{
				Key: "a/b.bin", Status: record.StatusFailed, Attempts: 5,
				LastError: "permission\ndenied", UpdatedAt: now.Add(-10 * time.Second),
			}},
		},
	}
	var out bytes.Buffer
	if err := runStatus(context.Background(), &out, twoPipelines(), store, &statusOpts{}, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	for _, want := range []string{
		"PIPELINE", "OLDEST PENDING",
		"alpha", "1200", "5.0 GiB", "42s ago", "3m05s ago",
		"beta", "never",
		"Recent errors — alpha:", "a/b.bin", "failed", "5 attempt(s)", "permission denied",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("status missing %q:\n%s", want, s)
		}
	}
}

func TestStatusJSON(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	store := fakeStats{"alpha": {Pending: 1, OldestPendingEvent: now.Add(-90 * time.Second)}}
	var out bytes.Buffer
	if err := runStatus(context.Background(), &out, twoPipelines(), store, &statusOpts{json: true}, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("%v:\n%s", err, out.String())
	}
	if len(rep.Pipelines) != 2 || rep.Pipelines[0].CurrentLagSeconds != 90 || rep.Pipelines[1].LastSyncedAt != nil {
		t.Errorf("report = %+v", rep)
	}
}

func TestStatusNoTables(t *testing.T) {
	var out bytes.Buffer
	store := errStats{err: &pgconn.PgError{Code: "42P01", Message: `relation "file_record" does not exist`}}
	if err := runStatus(context.Background(), &out, twoPipelines(), store, &statusOpts{}, time.Now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), noDataMsg) {
		t.Errorf("out = %q", out.String())
	}
}

func TestStatusError(t *testing.T) {
	var out bytes.Buffer
	err := runStatus(context.Background(), &out, twoPipelines(), errStats{err: errors.New("connection refused")}, &statusOpts{}, time.Now)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v", err)
	}
}

func TestStatusWatchStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runStatus(ctx, &out, twoPipelines(), fakeStats{}, &statusOpts{watch: 10 * time.Millisecond}, time.Now)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop")
	}
}

func TestFormatting(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1536: "1.5 KiB", 64 << 20: "64.0 MiB", 3 << 40: "3.0 TiB"} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
	for d, want := range map[time.Duration]string{
		850 * time.Millisecond: "850ms", 42 * time.Second: "42s", 192 * time.Second: "3m12s",
		5*time.Hour + 4*time.Minute: "5h04m", 74 * time.Hour: "3d2h",
	} {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%v) = %q, want %q", d, got, want)
		}
	}
	for in, want := range map[string]string{
		"postgres://u:pw@h:5432/db?sslmode=disable&password=x": "postgres://u:xxxxx@h:5432/db?password=xxxxx&sslmode=disable",
		"host=h password=pw dbname=d":                          "host=h password=xxxxx dbname=d",
		"https://a.blob.core.windows.net/?sig=abc":             "https://a.blob.core.windows.net/?sig=xxxxx",
	} {
		if got := redactURL(in); got != want {
			t.Errorf("redactURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustRead(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
