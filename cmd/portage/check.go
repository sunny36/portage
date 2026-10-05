package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/check"
	"github.com/sunny36/portage/internal/config"
)

type checkOpts struct {
	pipeline string
	json     bool
	timeout  time.Duration
	// specErrors are failed results for pipeline_spec rows that don't parse
	// (pipelines_from: database).
	specErrors []check.Result
}

func newCheckCmd(o *rootOpts) *cobra.Command {
	co := &checkOpts{}
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Test credentials, permissions, buckets, events and the database against the real endpoints",
		Long: "Check runs live preflight checks for each pipeline and prints how to fix what fails:\n" +
			"  source       list, stat and read 1 byte (never writes)\n" +
			"  destination  list; write, read back and delete a probe object; a 5 MiB multipart upload\n" +
			"               (both under <destination prefix>/" + check.ProbePrefix + ", deleted afterwards)\n" +
			"  events       peek the queue (nothing is consumed) and look for its poison queue\n" +
			"  database     connect, server version, whether tables exist (nothing is migrated)\n" +
			"  clock skew   local clock vs the providers' Date headers\n" +
			"With pipelines_from: database it checks every enabled pipeline in pipeline_spec, resolving\n" +
			"secret:// references here as the engine would, and fails rows whose spec is invalid.\n" +
			"It exits non-zero if any check fails.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := o.loadConfig()
			if err != nil {
				return err
			}
			if cfg.FromDatabase() {
				if cfg, co.specErrors, err = enabledSpecs(cmd.Context(), cfg); err != nil {
					return err
				}
			}
			return runCheck(cmd.Context(), cmd.OutOrStdout(), cfg, co, check.Options{})
		},
	}
	f := cmd.Flags()
	f.StringVar(&co.pipeline, "pipeline", "", "check only this pipeline")
	f.BoolVar(&co.json, "json", false, "print JSON instead of a table")
	f.DurationVar(&co.timeout, "timeout", 60*time.Second, "timeout for each check")
	return cmd
}

// runCheck runs the checks with opts (tests inject fakes) and renders them.
func runCheck(ctx context.Context, w io.Writer, cfg *config.File, co *checkOpts, opts check.Options) error {
	opts.Pipeline = co.pipeline
	opts.Timeout = co.timeout
	var specErrs []check.Result
	for _, r := range co.specErrors {
		if co.pipeline == "" || r.Pipeline == co.pipeline {
			specErrs = append(specErrs, r)
		}
	}
	selected := slices.ContainsFunc(cfg.Pipelines, func(p config.Pipeline) bool {
		return co.pipeline == "" || p.Name == co.pipeline
	})
	var rep check.Report
	switch {
	case selected || len(specErrs) == 0:
		if len(cfg.Pipelines) == 0 && len(co.specErrors) == 0 {
			return errors.New("no enabled pipelines in pipeline_spec")
		}
		var err error
		if rep, err = check.Run(ctx, cfg, opts); err != nil {
			return err
		}
	}
	rep.Results = append(specErrs, rep.Results...)
	rep.OK = !rep.Failed()
	var err error
	if co.json {
		err = check.WriteJSON(w, rep)
	} else {
		err = check.WriteTable(w, rep)
	}
	if err != nil {
		return err
	}
	if rep.Failed() {
		return errSilent
	}
	return nil
}

// enabledSpecs returns cfg with the enabled, valid pipelines of the
// pipeline_spec table (secrets resolved here, as the engine would), and a
// failed result for each enabled row that is invalid.
func enabledSpecs(ctx context.Context, cfg *config.File) (*config.File, []check.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	specs, err := readSpecs(ctx, pool, config.ParsePipelineSpec)
	if err != nil {
		return nil, nil, err
	}
	out := *cfg
	out.Pipelines = nil
	var invalid []check.Result
	for _, s := range specs {
		switch {
		case !s.Enabled:
		case s.Err != nil:
			invalid = append(invalid, check.Result{
				Pipeline: s.Name, Check: "spec", Status: check.StatusFail,
				Detail: fmt.Sprintf("revision %d: %s", s.Revision, strings.ReplaceAll(s.Err.Error(), "\n", "; ")),
				Fix:    "Fix the row in pipeline_spec (e.g. `portage pipelines apply`); engines skip it until then.",
			})
		default:
			out.Pipelines = append(out.Pipelines, s.Pipeline)
		}
	}
	return &out, invalid, nil
}
