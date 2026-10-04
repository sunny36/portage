package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/config"
)

func newValidateCmd(o *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Check the config and print what each pipeline will do (secrets redacted)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := o.loadConfig()
			if err != nil {
				return err
			}
			printConfigSummary(cmd.OutOrStdout(), o.configPath, cfg)
			return nil
		},
	}
}

func printConfigSummary(w io.Writer, path string, cfg *config.File) {
	fmt.Fprintf(w, "Config:    %s\n", path)
	fmt.Fprintf(w, "Database:  %s\n", redactURL(cfg.DatabaseURL))
	fmt.Fprintf(w, "Metrics:   %s (/metrics, /healthz, /readyz)\n", cfg.MetricsAddr)
	for _, p := range cfg.Pipelines {
		if p.Events.Type == "webhook" {
			fmt.Fprintf(w, "Webhooks:  %s\n", cfg.WebhookAddr)
			break
		}
	}
	for _, p := range cfg.Pipelines {
		fmt.Fprintf(w, "\nPipeline %s\n", p.Name)
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
	fmt.Fprintf(w, "\nOK: %d pipeline(s) valid.\n", len(cfg.Pipelines))
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
