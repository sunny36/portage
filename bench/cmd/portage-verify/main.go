// Command portage-verify proves a soak run lost nothing: every blob that
// portage-loadgen recorded in its manifest(s) must exist at the pipeline's
// destination with the same size and SHA-256. Optionally it also scrapes the
// engine's /metrics for sync lag, throughput and file outcomes. It exits
// non-zero if anything is missing or mismatched. See bench/README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector/azure"
	"github.com/sunny36/portage/internal/connector/s3"
)

// errFailed signals verification failures (exit status 1) as opposed to
// usage or setup errors (exit status 2).
var errFailed = errors.New("verification failed")

func main() {
	err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)
	switch {
	case err == nil:
	case errors.Is(err, errFailed):
		os.Exit(1)
	default:
		fmt.Fprintln(os.Stderr, "portage-verify:", err)
		os.Exit(2)
	}
}

// report is the -json output; committed to bench/results/ after a soak.
type report struct {
	GeneratedAt time.Time `json:"generated_at"`
	Pipeline    string    `json:"pipeline"`
	Manifests   []string  `json:"manifests"`
	Source      endpoint  `json:"source"`
	Destination endpoint  `json:"destination"`

	ManifestLines int64       `json:"manifest_lines"`
	UniqueKeys    int64       `json:"unique_keys"`
	Superseded    int64       `json:"superseded_entries"`
	OutOfScope    int64       `json:"out_of_scope"`
	Excluded      int64       `json:"excluded_by_filters"`
	Load          loadSummary `json:"load"`

	Verify  *checkResult   `json:"verify"`
	Metrics *metricsReport `json:"metrics,omitempty"`
	// MetricsError is set when -metrics was given but the scrape failed.
	MetricsError string `json:"metrics_error,omitempty"`
	Passed       bool   `json:"passed"`
}

type endpoint struct {
	Provider  string `json:"provider"`
	Container string `json:"container"`
	Prefix    string `json:"prefix"`
}

func describe(e config.Endpoint) endpoint {
	out := endpoint{Provider: e.Provider(), Prefix: e.Prefix}
	switch {
	case e.Azure != nil:
		out.Container = e.Azure.Container
	case e.S3 != nil:
		out.Container = e.S3.Bucket
	}
	return out
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("portage-verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: portage-verify -c pipeline.yaml [-pipeline NAME] [flags] manifest.jsonl...")
		fs.PrintDefaults()
	}
	var (
		cfgPath      = fs.String("c", "pipeline.yaml", "Portage config; the destination is built from it exactly as the engine does")
		pipelineName = fs.String("pipeline", "", "pipeline to verify (default: the only one in the config)")
		concurrency  = fs.Int("concurrency", 16, "objects verified in parallel")
		sizeOnly     = fs.Bool("size-only", false, "check existence and size (and portagesha256 metadata when present) without reading content")
		retryMissing = fs.Duration("retry-missing", 0, "keep re-checking missing objects for this long (engine still catching up)")
		metricsURL   = fs.String("metrics", "", "engine /metrics URL, e.g. http://127.0.0.1:9090/metrics; adds lag, throughput and outcomes")
		window       = fs.Duration("metrics-window", 0, "scrape twice this far apart to measure current throughput (0 = once)")
		jsonPath     = fs.String("json", "", "write the full report as JSON to this file")
		progressIvl  = fs.Duration("progress", 10*time.Second, "progress interval on stderr (0 = off)")
		maxListed    = fs.Int("max-failures", 50, "failures listed in the summary (the JSON report has all)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	manifests := fs.Args()
	if len(manifests) == 0 {
		fs.Usage()
		return errors.New("at least one manifest is required")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	pc, err := pickPipeline(cfg, *pipelineName)
	if err != nil {
		return err
	}
	filter, err := change.NewFilter(pc.Filters)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	dst, err := newDestination(ctx, pc.Destination)
	if err != nil {
		return fmt.Errorf("destination: %w", err)
	}

	set, err := readManifests(manifests)
	if err != nil {
		return err
	}
	m := mapToDestination(set.Entries, pc.Source.Prefix, filter)
	fmt.Fprintf(stderr, "verify: %d manifest line(s), %d unique key(s), %d to check at %s://%s/%s\n",
		set.Lines, len(set.Entries), len(m.Targets), pc.Destination.Provider(), describe(pc.Destination).Container, pc.Destination.Prefix)

	res := check(ctx, dst, m.Targets, checkOptions{
		Concurrency:  *concurrency,
		SizeOnly:     *sizeOnly,
		RetryMissing: *retryMissing,
		Progress:     *progressIvl,
		Out:          stderr,
		DestPrefix:   pc.Destination.Prefix,
	})
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("interrupted: %w", err)
	}

	rep := &report{
		GeneratedAt:   time.Now().UTC(),
		Pipeline:      pc.Name,
		Manifests:     manifests,
		Source:        describe(pc.Source),
		Destination:   describe(pc.Destination),
		ManifestLines: set.Lines,
		UniqueKeys:    int64(len(set.Entries)),
		Superseded:    set.Superseded,
		OutOfScope:    m.OutOfScope,
		Excluded:      m.Excluded,
		Load:          summarizeLoad(set.Entries),
		Verify:        res,
		Passed:        !res.Failed() && len(m.Targets) > 0,
	}
	if *metricsURL != "" {
		mr, err := collectMetrics(ctx, *metricsURL, pc.Name, *window, time.Now)
		if err != nil {
			rep.MetricsError = err.Error()
		}
		rep.Metrics = mr
	}

	writeSummary(stdout, rep, *maxListed)
	if *jsonPath != "" {
		if err := writeJSON(*jsonPath, rep); err != nil {
			return err
		}
	}
	if !rep.Passed {
		return errFailed
	}
	return nil
}

func pickPipeline(cfg *config.File, name string) (config.Pipeline, error) {
	if name == "" {
		if len(cfg.Pipelines) != 1 {
			names := make([]string, len(cfg.Pipelines))
			for i, p := range cfg.Pipelines {
				names[i] = p.Name
			}
			return config.Pipeline{}, fmt.Errorf("config has %d pipelines (%s); pick one with -pipeline", len(names), strings.Join(names, ", "))
		}
		return cfg.Pipelines[0], nil
	}
	for _, p := range cfg.Pipelines {
		if p.Name == name {
			return p, nil
		}
	}
	return config.Pipeline{}, fmt.Errorf("no pipeline %q in config", name)
}

// newDestination builds the destination connector the way the engine does
// (internal/pipeline newConnector), scoped to the destination prefix.
func newDestination(ctx context.Context, ep config.Endpoint) (destination, error) {
	switch ep.Provider() {
	case "azure":
		return azure.New(ctx, *ep.Azure, ep.Prefix)
	case "s3":
		return s3.New(ctx, *ep.S3, ep.Prefix)
	}
	return nil, errors.New("no provider configured")
}

func writeJSON(path string, rep *report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func writeSummary(w io.Writer, r *report, maxListed int) {
	v := r.Verify
	p := func(format string, a ...any) { fmt.Fprintf(w, format+"\n", a...) }
	verdict := "PASS"
	if !r.Passed {
		verdict = "FAIL"
	}
	p("portage-verify %s — pipeline %s", verdict, r.Pipeline)
	p("  manifests:   %d line(s) in %d file(s); %d unique key(s), %d superseded, %d out of scope, %d excluded by filters",
		r.ManifestLines, len(r.Manifests), r.UniqueKeys, r.Superseded, r.OutOfScope, r.Excluded)
	if r.Load.Files > 0 {
		p("  load:        %d files, %s, %s → %s (%s/s avg, largest %s)", r.Load.Files, human(float64(r.Load.Bytes)),
			r.Load.FirstWrite.Format(time.RFC3339), r.Load.LastWrite.Format(time.RFC3339),
			human(r.Load.BytesPerSecond), human(float64(r.Load.MaxSize)))
	}
	mode := "size + full SHA-256"
	if !v.Hashed {
		mode = "size only"
	}
	p("  checked:     %d files, %s (%s) in %s", v.Files, human(float64(v.Bytes)), mode,
		(time.Duration(v.Duration * float64(time.Second))).Round(time.Second))
	p("  result:      %d ok, %d missing, %d size mismatch, %d sha256 mismatch, %d error(s)",
		v.OK, v.Missing, v.SizeMismatch, v.SHAMismatch, v.Errors)
	if v.DestLag != nil {
		p("  dest lag:    p50 %.1fs  p95 %.1fs  p99 %.1fs  max %.1fs  (dest LastModified − written_at; 1s resolution)",
			v.DestLag.P50, v.DestLag.P95, v.DestLag.P99, v.DestLag.Max)
	}
	for i, f := range v.Failures {
		if i >= maxListed {
			p("  ... and %d more (see -json)", len(v.Failures)-maxListed)
			break
		}
		line := fmt.Sprintf("  %-16s %s", f.Problem, f.DestKey)
		if f.Problem == problemSizeMismatch {
			line += fmt.Sprintf(" (want %d bytes, got %d)", f.ExpectedSize, f.ActualSize)
		}
		if f.Error != "" {
			line += ": " + f.Error
		}
		p("%s", line)
	}
	if r.MetricsError != "" {
		p("  metrics:     scrape failed: %s", r.MetricsError)
	}
	if m := r.Metrics; m != nil {
		q := func(x *float64) string {
			if x == nil {
				return "n/a"
			}
			return fmt.Sprintf("%.1fs", *x)
		}
		p("  sync lag:    p50 %s  p95 %s  p99 %s  over %.0f files (engine histogram; bucket resolution)",
			q(m.SyncLagSeconds.P50), q(m.SyncLagSeconds.P95), q(m.SyncLagSeconds.P99), m.SyncLagSeconds.Count)
		p("  copy time:   p50 %s  p95 %s  p99 %s", q(m.CopyDurationSeconds.P50), q(m.CopyDurationSeconds.P95), q(m.CopyDurationSeconds.P99))
		p("  outcomes:    %s", formatCounts(m.FilesByOutcome))
		p("  events:      %s", formatCounts(m.EventsBySource))
		line := fmt.Sprintf("  throughput:  %s copied", human(m.BytesCopied))
		if m.AvgBytesPerSecond > 0 {
			line += fmt.Sprintf(", %s/s avg over %s engine uptime", human(m.AvgBytesPerSecond),
				(time.Duration(m.EngineUptimeSecond * float64(time.Second))).Round(time.Second))
		}
		if m.WindowSeconds > 0 {
			line += fmt.Sprintf(", %s/s over the last %.0fs", human(m.WindowBytesPerSecond), m.WindowSeconds)
		}
		p("%s", line)
		p("  engine now:  queue depth %.0f, oldest pending %.0fs, RSS %s, %.0f goroutines",
			m.QueueDepth, m.OldestPendingSeconds, human(m.RSSBytes), m.Goroutines)
	}
}

func formatCounts(m map[string]float64) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%.0f", k, m[k])
	}
	return strings.Join(parts, " ")
}
