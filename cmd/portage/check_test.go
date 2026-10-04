package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/check"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

func TestCheckUnknownPipeline(t *testing.T) {
	code, _, errOut := runCLI(t, "check", "-c", examplePath, "--pipeline", "nope")
	if code == 0 || !strings.Contains(errOut, `no pipeline named "nope"`) || !strings.Contains(errOut, "azure-to-s3-local") {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
}

func TestCheckHelp(t *testing.T) {
	code, out, _ := runCLI(t, "check", "--help")
	if code != 0 || !strings.Contains(out, "never writes") || !strings.Contains(out, "--json") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func failingOptions() check.Options {
	return check.Options{
		NewConnector: func(context.Context, config.Endpoint) (connector.Connector, error) {
			return nil, errors.New("no credentials")
		},
		ProbeDB: func(context.Context, string) (check.DBInfo, error) {
			return check.DBInfo{ServerVersion: "17"}, nil
		},
		ServerTime: func(context.Context, string) (time.Time, error) { return time.Now(), nil },
	}
}

func TestRunCheckExitAndJSON(t *testing.T) {
	cfg, err := config.Load(examplePath)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	err = runCheck(context.Background(), &b, cfg, &checkOpts{json: true, timeout: time.Second}, failingOptions())
	if !errors.Is(err, errSilent) {
		t.Fatalf("want errSilent (non-zero exit), got %v", err)
	}
	var rep check.Report
	if err := json.Unmarshal(b.Bytes(), &rep); err != nil {
		t.Fatalf("json: %v\n%s", err, b.String())
	}
	if rep.OK || len(rep.Results) == 0 {
		t.Fatalf("report %+v", rep)
	}
	if rep.Results[0].Check != check.CheckDatabase || rep.Results[0].Status != check.StatusOK {
		t.Errorf("first result %+v", rep.Results[0])
	}

	b.Reset()
	err = runCheck(context.Background(), &b, cfg, &checkOpts{timeout: time.Second}, failingOptions())
	if !errors.Is(err, errSilent) || !strings.Contains(b.String(), "FAIL") || !strings.Contains(b.String(), "Pipeline azure-to-s3-local") {
		t.Fatalf("err %v:\n%s", err, b.String())
	}
}
