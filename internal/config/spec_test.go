package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const specJSON = `{
  "name": "ignored-name",
  "source": {"prefix": "/in", "azure": {"account_url": "https://a.blob.core.windows.net", "container": "c",
             "auth": "shared_key", "account_name": "a", "account_key": "secret://azure-key"}},
  "destination": {"prefix": "out", "s3": {"bucket": "b", "region": "r",
                  "access_key_id": "secret://s3.id", "secret_access_key": "secret://s3-secret"}},
  "events": {"type": "none"},
  "filters": {"exclude": ["*.tmp"]},
  "part_size": "8MiB",
  "reconcile_interval": "90s"
}`

func TestParsePipelineSpec(t *testing.T) {
	t.Setenv(SecretsDirEnv, "")
	t.Setenv("PORTAGE_SECRET_AZURE_KEY", "azkey")
	t.Setenv("PORTAGE_SECRET_S3_ID", "id")
	t.Setenv("PORTAGE_SECRET_S3_SECRET", "s3secret")
	p, err := ParsePipelineSpec("from-row", []byte(specJSON))
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "from-row" {
		t.Errorf("name = %q, want the row's", p.Name)
	}
	if p.Source.Prefix != "in/" || p.Destination.Prefix != "out/" {
		t.Errorf("prefixes not normalised: %q %q", p.Source.Prefix, p.Destination.Prefix)
	}
	if p.Source.Azure.AccountKey != "azkey" || p.Destination.S3.AccessKeyID != "id" || p.Destination.S3.SecretAccessKey != "s3secret" {
		t.Errorf("secrets not resolved: %+v %+v", *p.Source.Azure, *p.Destination.S3)
	}
	if p.PartSize != 8<<20 || p.ReconcileInterval != 90*time.Second {
		t.Errorf("part_size=%d reconcile=%s", p.PartSize, p.ReconcileInterval)
	}
	if p.Concurrency != 16 || p.PartConcurrency != 4 || p.ExistingFiles != "copy" || p.Destination.S3.Flavor != "aws" {
		t.Errorf("defaults not applied: %+v", p)
	}
	if len(p.Filters.Exclude) != 1 || p.Filters.Exclude[0] != "*.tmp" {
		t.Errorf("filters = %+v", p.Filters)
	}

	// A numeric part_size works too, and CheckPipelineSpec accepts
	// references it cannot resolve.
	q, err := CheckPipelineSpec("ab", []byte(`{"source":{"s3":{"bucket":"a","region":"r"}},
		"destination":{"s3":{"bucket":"b","region":"r","access_key_id":"secret://not-here","secret_access_key":"secret://nor-here"}},
		"part_size":6291456}`))
	if err != nil || q.PartSize != 6<<20 || q.ReconcileInterval != 15*time.Minute {
		t.Errorf("CheckPipelineSpec: %+v, %v", q, err)
	}
}

func TestParsePipelineSpecErrors(t *testing.T) {
	t.Setenv(SecretsDirEnv, "")
	const ends = `"source":{"s3":{"bucket":"a","region":"r"}},"destination":{"s3":{"bucket":"b","region":"r"}}`
	for name, tc := range map[string]struct {
		pipeline, spec string
		want           []string
	}{
		"unknown field": {"ab", `{` + ends + `,"concurency":4}`, []string{"concurency"}},
		"invalid json":  {"ab", `{"source":`, []string{"parse pipeline spec"}},
		"empty":         {"ab", ``, []string{"empty"}},
		"bad name":      {"Bad_Name", `{` + ends + `}`, []string{"name:"}},
		"validation": {"ab", `{"source":{},"destination":{"s3":{"bucket":"b"}},"reconcile_interval":"10s"}`,
			[]string{"source: set exactly one", "destination.s3.region: required", "reconcile_interval"}},
		"bad duration": {"ab", `{"reconcile_interval":"soon"}`, []string{"parse pipeline spec"}},
		"bad secret name": {"ab", `{"source":{"s3":{"bucket":"a","region":"r"}},"destination":{"s3":{"bucket":"b","region":"r","access_key_id":"secret://../etc/passwd","secret_access_key":"x"}}}`,
			[]string{"destination.s3.access_key_id", "secret name"}},
		"missing secret": {"ab", `{"source":{"s3":{"bucket":"a","region":"r"}},"destination":{"s3":{"bucket":"b","region":"r","access_key_id":"x","secret_access_key":"secret://nope-missing"}}}`,
			[]string{"destination.s3.secret_access_key: secret://nope-missing", "PORTAGE_SECRET_NOPE_MISSING"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParsePipelineSpec(tc.pipeline, []byte(tc.spec))
			if err == nil {
				t.Fatal("want error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error missing %q: %v", w, err)
				}
			}
		})
	}
}

func TestResolveSecret(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(SecretsDirEnv, dir)
	for name, content := range map[string]string{"both": "from-file\n", "two-newlines": "v\n\n", "crlf": "w\r\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PORTAGE_SECRET_BOTH", "from-env")
	t.Setenv("PORTAGE_SECRET_ENV_ONLY_X", "env-value")

	for name, want := range map[string]string{
		"both":         "from-file", // file wins over env
		"two-newlines": "v\n",       // only one newline trimmed
		"crlf":         "w",
		"env-only.x":   "env-value",
	} {
		got, err := ResolveSecret(name)
		if err != nil || got != want {
			t.Errorf("ResolveSecret(%q) = %q, %v; want %q", name, got, err, want)
		}
	}
	if _, err := ResolveSecret("absent"); err == nil || !strings.Contains(err.Error(), "PORTAGE_SECRET_ABSENT") {
		t.Errorf("missing secret error = %v", err)
	}
	if _, err := ResolveSecret("../x"); err == nil {
		t.Error("path traversal accepted")
	}
}

func TestSecretErrorsDoNotLeakValues(t *testing.T) {
	t.Setenv(SecretsDirEnv, "")
	t.Setenv("PORTAGE_SECRET_GOOD", "TOPSECRETVALUE")
	// A resolvable and a missing secret, plus a validation error on the
	// endpoint holding the resolved one.
	_, err := ParsePipelineSpec("ab", []byte(`{
		"source":{"azure":{"container":"c","auth":"shared_key","account_key":"secret://good"}},
		"destination":{"s3":{"bucket":"b","region":"r","access_key_id":"secret://good","secret_access_key":"secret://missing"}}}`))
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "TOPSECRETVALUE") {
		t.Errorf("error leaks a secret value: %v", err)
	}
	for _, want := range []string{"secret://missing", "source.azure.account_url: required"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestSecretsInFile(t *testing.T) {
	t.Setenv(SecretsDirEnv, "")
	t.Setenv("PORTAGE_SECRET_S3_KEY", "resolved")
	const cfg = `
database_url: postgres://x
pipelines:
  - name: p1
    source: {s3: {bucket: a, region: r}}
    destination: {s3: {bucket: b, region: r, access_key_id: id, secret_access_key: secret://%s}}
`
	f, err := Parse([]byte(strings.Replace(cfg, "%s", "s3-key", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Pipelines[0].Destination.S3.SecretAccessKey; got != "resolved" {
		t.Errorf("secret_access_key = %q", got)
	}
	_, err = Parse([]byte(strings.Replace(cfg, "%s", "gone", 1)))
	if err == nil || !strings.Contains(err.Error(), "pipelines[0].destination.s3.secret_access_key: secret://gone") {
		t.Errorf("want error naming the field and reference, got %v", err)
	}
}

func TestPipelinesFrom(t *testing.T) {
	f, err := Parse([]byte("database_url: postgres://x\npipelines_from: database\nmetrics_addr: :9191\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !f.FromDatabase() || len(f.Pipelines) != 0 || f.MetricsAddr != ":9191" {
		t.Errorf("database mode: %+v", f)
	}
	f, err = Parse([]byte(`
database_url: postgres://x
pipelines:
  - name: p1
    source: {s3: {bucket: a, region: r}}
    destination: {s3: {bucket: b, region: r}}
`))
	if err != nil || f.FromDatabase() || f.PipelinesFrom != PipelinesFromFile {
		t.Errorf("default mode: %+v, %v", f, err)
	}
	for in, want := range map[string]string{
		"database_url: x\npipelines_from: database\npipelines:\n  - name: p1\n": "must be empty",
		"database_url: x\npipelines_from: table\n":                              "pipelines_from",
		"database_url: x\npipelines_from: file\n":                               "at least one required",
		"pipelines_from: database\n":                                            "database_url: required",
	} {
		if _, err := Parse([]byte(in)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) = %v; want error containing %q", in, err, want)
		}
	}
}
