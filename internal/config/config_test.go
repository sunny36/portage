package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestExampleParses(t *testing.T) {
	raw, err := os.ReadFile("../../examples/pipeline.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("example config invalid: %v", err)
	}
	p := f.Pipelines[0]
	if p.Source.Provider() != "azure" || p.Destination.Provider() != "s3" {
		t.Fatalf("providers = %s -> %s", p.Source.Provider(), p.Destination.Provider())
	}
	if p.PartSize != 8<<20 || p.ReconcileInterval != time.Minute {
		t.Fatalf("part_size=%d reconcile=%s", p.PartSize, p.ReconcileInterval)
	}
}

func TestDefaultsAndEnv(t *testing.T) {
	t.Setenv("KEY", "secret")
	f, err := Parse([]byte(`
database_url: postgres://x
pipelines:
  - name: p1
    source: {azure: {account_url: https://a, container: c}}
    destination: {s3: {bucket: b, region: r, access_key_id: id, secret_access_key: ${KEY}}}
    events: {type: none}
`))
	if err != nil {
		t.Fatal(err)
	}
	p := f.Pipelines[0]
	if p.Destination.S3.SecretAccessKey != "secret" {
		t.Errorf("env not expanded")
	}
	if p.Concurrency != 16 || p.PartSize != 64<<20 || p.ExistingFiles != "copy" || p.Source.Azure.Auth != "default" {
		t.Errorf("defaults not applied: %+v", p)
	}
}

func TestValidationErrors(t *testing.T) {
	_, err := Parse([]byte(`
pipelines:
  - name: Bad_Name
    source: {}
    destination: {s3: {bucket: b}}
`))
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"database_url", "name", "set exactly one", "region"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{"64MiB": 64 << 20, "8MB": 8e6, "1024": 1024, "1 GiB": 1 << 30} {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("%q = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ParseByteSize("lots"); err == nil {
		t.Error("want error")
	}
}
