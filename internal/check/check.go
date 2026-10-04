// Package check implements `portage check`: live preflight checks that prove
// a pipeline's credentials, permissions, buckets, event queue and database
// work before `portage run`, and say how to fix what doesn't.
//
// Checks never write to a source and never consume queue messages: the
// source is listed and read, the queue is peeked. On the destination a small
// probe object and a one-part multipart upload are written under
// ".portage-check/" (inside the destination prefix) and deleted again.
package check

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// Status is the outcome of one check.
type Status string

// Check outcomes. Only StatusFail makes `portage check` exit non-zero.
const (
	StatusOK      Status = "ok"
	StatusFail    Status = "FAIL"
	StatusWarn    Status = "warn"
	StatusSkipped Status = "skipped"
)

// Result is one row of the report.
type Result struct {
	// Pipeline is empty for checks shared by all pipelines (database).
	Pipeline string `json:"pipeline,omitempty"`
	Check    string `json:"check"`
	Status   Status `json:"result"`
	Detail   string `json:"detail"`
	// Fix is a concrete next step; set for failures and most warnings.
	Fix       string `json:"fix,omitempty"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

// Report is the outcome of Run.
type Report struct {
	OK      bool     `json:"ok"`
	Results []Result `json:"results"`
}

// Failed reports whether any check failed.
func (r Report) Failed() bool {
	for _, res := range r.Results {
		if res.Status == StatusFail {
			return true
		}
	}
	return false
}

// Check names, in report order.
const (
	CheckDatabase      = "database"
	CheckSourceList    = "source list"
	CheckSourceRead    = "source read"
	CheckDestList      = "destination list"
	CheckDestWrite     = "destination write"
	CheckDestMultipart = "destination multipart"
	CheckEvents        = "events"
	CheckPoisonQueue   = "events poison queue"
	CheckClock         = "clock skew"
)

// ProbePrefix is where destination probe objects are written, relative to
// the destination prefix.
const ProbePrefix = ".portage-check/"

// Options configures Run. Zero values use the real implementations.
type Options struct {
	// Pipeline restricts the run to one pipeline by name.
	Pipeline string
	// Timeout bounds each check (default 60s).
	Timeout time.Duration

	NewConnector func(ctx context.Context, ep config.Endpoint) (connector.Connector, error)
	NewQueue     func(p config.Pipeline) (QueueProber, error)
	ProbeDB      func(ctx context.Context, url string) (DBInfo, error)
	// ServerTime returns the Date header of a response from url.
	ServerTime func(ctx context.Context, url string) (time.Time, error)
	Now        func() time.Time
}

func (o *Options) defaults() {
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	if o.NewConnector == nil {
		o.NewConnector = newConnector
	}
	if o.NewQueue == nil {
		o.NewQueue = newAzureQueue
	}
	if o.ProbeDB == nil {
		o.ProbeDB = probeDB
	}
	if o.ServerTime == nil {
		o.ServerTime = serverTime
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Run checks every pipeline in cfg (or only opts.Pipeline). Checks run
// concurrently; results come back in a fixed order.
func Run(ctx context.Context, cfg *config.File, opts Options) (Report, error) {
	opts.defaults()
	var pipelines []config.Pipeline
	for _, p := range cfg.Pipelines {
		if opts.Pipeline == "" || p.Name == opts.Pipeline {
			pipelines = append(pipelines, p)
		}
	}
	if len(pipelines) == 0 {
		names := make([]string, 0, len(cfg.Pipelines))
		for _, p := range cfg.Pipelines {
			names = append(names, p.Name)
		}
		return Report{}, fmt.Errorf("no pipeline named %q (have: %s)", opts.Pipeline, strings.Join(names, ", "))
	}

	r := &runner{opts: opts}
	groups := make([][]Result, 1+len(pipelines))
	var wg sync.WaitGroup
	wg.Go(func() { groups[0] = []Result{r.database(ctx, cfg.DatabaseURL)} })
	for i, p := range pipelines {
		wg.Go(func() { groups[i+1] = r.pipeline(ctx, p) })
	}
	wg.Wait()

	var rep Report
	for _, g := range groups {
		rep.Results = append(rep.Results, g...)
	}
	rep.OK = !rep.Failed()
	return rep, nil
}

type runner struct {
	opts Options
}

// timed runs fn with the per-check timeout and fills in timing.
func (r *runner) timed(ctx context.Context, pipeline, name string, fn func(ctx context.Context) Result) Result {
	ctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	start := time.Now()
	res := fn(ctx)
	res.Pipeline, res.Check = pipeline, name
	res.ElapsedMS = time.Since(start).Milliseconds()
	if res.Status == StatusFail && res.Fix == "" && ctx.Err() != nil {
		res.Fix = Hint(context.DeadlineExceeded, Target{})
	}
	return res
}

func (r *runner) pipeline(ctx context.Context, p config.Pipeline) []Result {
	srcT := Target{Role: RoleSource, Endpoint: p.Source, Deletes: p.Deletes}
	dstT := Target{Role: RoleDestination, Endpoint: p.Destination, Deletes: p.Deletes}
	src, srcErr := r.opts.NewConnector(ctx, p.Source)
	dst, dstErr := r.opts.NewConnector(ctx, p.Destination)

	var (
		srcRes [2]Result
		dstRes [3]Result
		events []Result
		clock  Result
		wg     sync.WaitGroup
	)
	wg.Go(func() {
		if srcErr != nil {
			srcRes[0] = fail(p.Name, CheckSourceList, fmt.Errorf("create connector: %w", srcErr), srcT)
			srcRes[1] = skipped(p.Name, CheckSourceRead, "source connector could not be created")
			return
		}
		var first *connector.ObjectInfo
		srcRes[0] = r.timed(ctx, p.Name, CheckSourceList, func(ctx context.Context) Result {
			res, obj := sourceList(ctx, src, p, srcT)
			first = obj
			return res
		})
		switch {
		case srcRes[0].Status == StatusFail:
			srcRes[1] = skipped(p.Name, CheckSourceRead, "source list failed")
		case first == nil:
			srcRes[1] = skipped(p.Name, CheckSourceRead, "no objects under the source prefix to read")
		default:
			srcRes[1] = r.timed(ctx, p.Name, CheckSourceRead, func(ctx context.Context) Result {
				return sourceRead(ctx, src, *first, srcT)
			})
		}
	})
	wg.Go(func() {
		if dstErr != nil {
			dstRes[0] = fail(p.Name, CheckDestList, fmt.Errorf("create connector: %w", dstErr), dstT)
			dstRes[1] = skipped(p.Name, CheckDestWrite, "destination connector could not be created")
			dstRes[2] = skipped(p.Name, CheckDestMultipart, "destination connector could not be created")
			return
		}
		var listErr error
		dstRes[0] = r.timed(ctx, p.Name, CheckDestList, func(ctx context.Context) Result {
			listErr = destList(ctx, dst)
			if listErr != nil {
				return failRes(listErr, dstT)
			}
			return ok("listed OK (verification and the reconciler need this)")
		})
		// A missing bucket or rejected credentials fail every write the
		// same way, so don't repeat them. (Some S3-compatible stores also
		// create a missing bucket on the first write.)
		if listErr != nil && (errors.Is(listErr, connector.ErrNotFound) || providerCode(listErr) == "NoSuchBucket" ||
			errors.Is(listErr, connector.ErrAuth) || isAzureCredentialError(listErr)) {
			dstRes[1] = skipped(p.Name, CheckDestWrite, "destination list failed")
			dstRes[2] = skipped(p.Name, CheckDestMultipart, "destination list failed")
			return
		}
		var dwg sync.WaitGroup
		dwg.Go(func() {
			dstRes[1] = r.timed(ctx, p.Name, CheckDestWrite, func(ctx context.Context) Result { return destWrite(ctx, dst, dstT) })
		})
		dwg.Go(func() {
			dstRes[2] = r.timed(ctx, p.Name, CheckDestMultipart, func(ctx context.Context) Result { return destMultipart(ctx, dst, dstT) })
		})
		dwg.Wait()
	})
	wg.Go(func() { events = r.events(ctx, p) })
	wg.Go(func() {
		clock = r.timed(ctx, p.Name, CheckClock, func(ctx context.Context) Result { return r.clock(ctx, p) })
	})
	wg.Wait()

	out := append([]Result{}, srcRes[:]...)
	out = append(out, dstRes[:]...)
	out = append(out, events...)
	return append(out, clock)
}

func ok(detail string) Result { return Result{Status: StatusOK, Detail: detail} }

func warn(detail, fix string) Result { return Result{Status: StatusWarn, Detail: detail, Fix: fix} }

func failRes(err error, t Target) Result {
	return Result{Status: StatusFail, Detail: describeErr(err), Fix: Hint(err, t)}
}

func fail(pipeline, name string, err error, t Target) Result {
	res := failRes(err, t)
	res.Pipeline, res.Check = pipeline, name
	return res
}

func skipped(pipeline, name, why string) Result {
	return Result{Pipeline: pipeline, Check: name, Status: StatusSkipped, Detail: why}
}

// describeErr renders err in one line. Provider errors are summarised from
// their status and code: Azure SDK errors embed the whole HTTP response.
func describeErr(err error) string {
	var pe *connector.ProviderError
	if errors.As(err, &pe) && (pe.Status != 0 || pe.Code != "") {
		// Keep context added around the provider error ("upload part: ").
		s := strings.TrimSuffix(err.Error(), pe.Error())
		if s == err.Error() {
			s = ""
		}
		s += pe.Op
		if pe.Status != 0 {
			s += fmt.Sprintf(": HTTP %d", pe.Status)
		}
		if pe.Code != "" {
			s += " " + pe.Code
		}
		if pe.Sentinel != nil {
			s += " (" + strings.TrimPrefix(pe.Sentinel.Error(), "connector: ") + ")"
		}
		return s
	}
	msg := err.Error()
	if i := strings.Index(msg, "\n"); i >= 0 {
		msg = msg[:i]
	}
	msg = strings.Join(strings.Fields(msg), " ")
	if r := []rune(msg); len(r) > 240 {
		msg = string(r[:239]) + "…"
	}
	return msg
}
