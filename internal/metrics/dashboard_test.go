package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

const dashboardPath = "../../deploy/grafana/portage-dashboard.json"

// exposedSeriesNames scrapes a fully exercised Metrics and returns every
// series name in the text exposition (so histogram _bucket/_sum/_count and
// any exporter-added suffix are taken from the real output, not assumed).
func exposedSeriesNames(t *testing.T) map[string]bool {
	t.Helper()
	m, h, err := New()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m.CopyFinished(ctx, "p", OutcomeSynced, 1, time.Second, time.Second)
	m.BytesUploaded(ctx, "p", 1)
	m.EventReceived(ctx, "p", "webhook", "created")
	m.Retry(ctx, "p", "op", "throttled")
	m.ReconcileFinished(ctx, "p", 1, 1, 1, time.Second, errors.New("x"))
	_ = m.RegisterQueueDepth(func(context.Context) (map[string]int64, error) { return map[string]int64{"p": 1}, nil })
	_ = m.RegisterOldestPending(func(context.Context) (map[string]time.Duration, error) {
		return map[string]time.Duration{"p": time.Second}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	names := map[string]bool{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		names[name] = true
	}
	return names
}

func TestDashboardReferencesExposedMetrics(t *testing.T) {
	raw, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Templating struct {
			List []struct {
				Name string `json:"name"`
			} `json:"list"`
		} `json:"templating"`
		Panels []struct {
			Type    string `json:"type"`
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	vars := map[string]bool{}
	for _, v := range dash.Templating.List {
		vars[v.Name] = true
	}
	if !vars["datasource"] || !vars["pipeline"] {
		t.Errorf("dashboard variables %v: want datasource and pipeline", vars)
	}

	exposed := exposedSeriesNames(t)
	nameRE := regexp.MustCompile(`portage_[a-z_]+`)
	referenced := map[string]bool{}
	for _, m := range nameRE.FindAllString(string(raw), -1) {
		referenced[m] = true
		if !exposed[m] {
			t.Errorf("dashboard references %s, which the exporter does not expose", m)
		}
	}
	// Every emitted metric should appear on the dashboard somewhere.
	for _, n := range Names {
		found := referenced[n]
		for _, sfx := range []string{"_bucket", "_sum", "_count"} {
			found = found || referenced[n+sfx]
		}
		if !found {
			t.Errorf("metric %s is not on the dashboard", n)
		}
	}

	for _, p := range dash.Panels {
		if p.Type == "row" {
			continue
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q has no queries", p.Title)
		}
		for _, tg := range p.Targets {
			if !strings.Contains(tg.Expr, `pipeline=~"$pipeline"`) {
				t.Errorf("panel %q query %q does not filter by $pipeline", p.Title, tg.Expr)
			}
		}
	}
}
