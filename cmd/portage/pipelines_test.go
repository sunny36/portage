package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const applyFile = `pipelines:
  - name: alpha
    source:
      prefix: in/
      azure: {account_url: "https://a.blob.core.windows.net", container: c}
    destination:
      s3: {bucket: b, region: r, access_key_id: "secret://s3-id", secret_access_key: "secret://s3-key"}
    events: {type: none}
    part_size: 8MiB
    reconcile_interval: 5m
`

func TestSpecsFromFile(t *testing.T) {
	specs, err := specsFromFile([]byte(applyFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 || specs[0].name != "alpha" {
		t.Fatalf("specs = %+v", specs)
	}
	var got map[string]any
	if err := json.Unmarshal(specs[0].spec, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["name"]; ok {
		t.Error("name kept inside the spec")
	}
	if got["part_size"] != "8MiB" || got["reconcile_interval"] != "5m" {
		t.Errorf("spec = %s", specs[0].spec)
	}
	// References are stored, never resolved.
	if !strings.Contains(string(specs[0].spec), `"secret://s3-key"`) {
		t.Errorf("secret reference not kept: %s", specs[0].spec)
	}
}

func TestDatabaseExampleApplies(t *testing.T) {
	specs, err := specsFromFile([]byte(mustRead(t, "../../examples/pipelines-db.yaml")))
	if err != nil || len(specs) != 1 || specs[0].name != "azure-to-s3-local" {
		t.Fatalf("specs = %+v, %v", specs, err)
	}
}

func TestSpecsFromFileErrors(t *testing.T) {
	for name, tc := range map[string]struct{ file, want string }{
		"empty":         {"version: 1\n", "no pipelines"},
		"unknown field": {strings.Replace(applyFile, "part_size", "partsize", 1), "partsize"},
		"env ref":       {strings.Replace(applyFile, `"secret://s3-key"`, `"${KEY}"`, 1), "destination.s3.secret_access_key: ${VAR} is not expanded"},
		"bad name":      {strings.Replace(applyFile, "alpha", "Alpha", 1), "name:"},
		"duplicate":     {applyFile + strings.TrimPrefix(applyFile, "pipelines:\n"), "duplicate name"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := specsFromFile([]byte(tc.file))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPipelinesNeedsDatabaseMode(t *testing.T) {
	code, _, errOut := runCLI(t, "pipelines", "list", "-c", examplePath)
	if code != 1 || !strings.Contains(errOut, "pipelines_from: database") {
		t.Errorf("exit %d: %s", code, errOut)
	}
}
