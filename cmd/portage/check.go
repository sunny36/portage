package main

import (
	"context"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/check"
	"github.com/sunny36/portage/internal/config"
)

type checkOpts struct {
	pipeline string
	json     bool
	timeout  time.Duration
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
			"It exits non-zero if any check fails.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := o.loadConfig()
			if err != nil {
				return err
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
	rep, err := check.Run(ctx, cfg, opts)
	if err != nil {
		return err
	}
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
