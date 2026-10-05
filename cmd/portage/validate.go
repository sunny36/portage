package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/config"
)

func newValidateCmd(o *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Check the config and print what each pipeline will do (secrets redacted)",
		Long: "Validate checks the config file and prints what each pipeline will do. With\n" +
			"pipelines_from: database it reads every row of pipeline_spec and validates it as the\n" +
			"engine would (secret:// references resolved here), exiting non-zero if a row is invalid.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := o.loadConfig()
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			if !cfg.FromDatabase() {
				printConfigHeader(w, o.configPath, cfg, cfg.Pipelines)
				for _, p := range cfg.Pipelines {
					printPipeline(w, p, "")
				}
				fmt.Fprintf(w, "\nOK: %d pipeline(s) valid.\n", len(cfg.Pipelines))
				return nil
			}
			return validateDatabase(cmd.Context(), w, o.configPath, cfg)
		},
	}
}

// validateDatabase validates every pipeline_spec row.
func validateDatabase(ctx context.Context, w io.Writer, path string, cfg *config.File) error {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	specs, err := readSpecs(ctx, pool, config.ParsePipelineSpec)
	if err != nil {
		return err
	}
	var valid []config.Pipeline
	for _, s := range specs {
		if s.Err == nil {
			valid = append(valid, s.Pipeline)
		}
	}
	printConfigHeader(w, path, cfg, valid)
	invalid := 0
	for _, s := range specs {
		label := fmt.Sprintf(" (revision %d", s.Revision)
		if !s.Enabled {
			label += ", disabled"
		}
		label += ")"
		if s.Err != nil {
			invalid++
			fmt.Fprintf(w, "\nPipeline %s%s\n  INVALID: %s\n", s.Name, label,
				strings.ReplaceAll(s.Err.Error(), "\n", "\n           "))
			continue
		}
		printPipeline(w, s.Pipeline, label)
	}
	if invalid > 0 {
		fmt.Fprintf(w, "\n%d of %d pipeline(s) invalid; engines skip them until they are fixed.\n", invalid, len(specs))
		return errSilent
	}
	if len(specs) == 0 {
		fmt.Fprintln(w, "\nNo pipelines in pipeline_spec yet (add some with `portage pipelines apply`).")
		return nil
	}
	fmt.Fprintf(w, "\nOK: %d pipeline(s) valid.\n", len(specs))
	return nil
}

var errNoSpecTable = errors.New("no pipeline_spec table yet: `portage run` or `portage pipelines apply` creates it")

func printConfigHeader(w io.Writer, path string, cfg *config.File, pipelines []config.Pipeline) {
	fmt.Fprintf(w, "Config:    %s\n", path)
	fmt.Fprintf(w, "Database:  %s\n", redactURL(cfg.DatabaseURL))
	fmt.Fprintf(w, "Metrics:   %s (/metrics, /healthz, /readyz)\n", cfg.MetricsAddr)
	if cfg.FromDatabase() {
		fmt.Fprintf(w, "Pipelines: from the database (pipeline_spec)\n")
	}
	for _, p := range pipelines {
		if p.Events.Type == "webhook" {
			fmt.Fprintf(w, "Webhooks:  %s\n", cfg.WebhookAddr)
			break
		}
	}
}

func printPipeline(w io.Writer, p config.Pipeline, label string) {
	fmt.Fprintf(w, "\nPipeline %s%s\n", p.Name, label)
	fmt.Fprintf(w, "  %s  →  %s\n", endpointShort(p.Source), endpointShort(p.Destination))
	fmt.Fprintf(w, "  source:       %s\n", describeEndpoint(p.Source))
	fmt.Fprintf(w, "  destination:  %s\n", describeEndpoint(p.Destination))
	fmt.Fprintf(w, "  events:       %s\n", describeEvents(p.Events))
	fmt.Fprintf(w, "  filters:      include %s; exclude %s\n", patterns(p.Filters.Include, "everything"), patterns(p.Filters.Exclude, "nothing"))
	deletes := "not propagated"
	if p.Deletes {
		deletes = "propagated"
	}
	fmt.Fprintf(w, "  behaviour:    existing files: %s; deletes: %s\n", p.ExistingFiles, deletes)
	fmt.Fprintf(w, "  transfer:     %d files in parallel, %d parts per file, part size %s\n",
		p.Concurrency, p.PartConcurrency, humanBytes(int64(p.PartSize)))
	fmt.Fprintf(w, "  reconcile:    every %s\n", p.ReconcileInterval)
}

func endpointShort(e config.Endpoint) string {
	switch e.Provider() {
	case "azure":
		return "azure:" + e.Azure.Container + "/" + e.Prefix
	case "s3":
		return "s3:" + e.S3.Bucket + "/" + e.Prefix
	}
	return "?"
}

func describeEndpoint(e config.Endpoint) string {
	prefix := e.Prefix
	if prefix == "" {
		prefix = "(whole container)"
	}
	switch e.Provider() {
	case "azure":
		a := e.Azure
		acct := redactURL(a.AccountURL)
		if acct == "" {
			acct = "(from connection string)"
		}
		return fmt.Sprintf("azure blob %s container=%s prefix=%s auth=%s", acct, a.Container, prefix, a.Auth)
	case "s3":
		s := e.S3
		endpoint := redactURL(s.Endpoint)
		if endpoint == "" {
			endpoint = "(AWS default)"
		}
		creds := "default credential chain"
		if s.AccessKeyID != "" {
			creds = "static access key"
		}
		return fmt.Sprintf("s3 %s bucket=%s region=%s prefix=%s flavor=%s path_style=%t credentials=%s",
			endpoint, s.Bucket, s.Region, prefix, s.Flavor, s.PathStyle, creds)
	}
	return "invalid endpoint"
}

func describeEvents(e config.Events) string {
	switch e.Type {
	case "none":
		return "none (changes found by the periodic reconcile only)"
	case "azure_queue":
		return fmt.Sprintf("azure_queue %s queue=%s", redactURL(e.QueueAccountURL), e.QueueName)
	case "webhook":
		return fmt.Sprintf("webhook path=%s secret=<redacted>", e.WebhookPath)
	}
	return e.Type
}

func patterns(p []string, empty string) string {
	if len(p) == 0 {
		return empty
	}
	return strings.Join(p, ", ")
}
