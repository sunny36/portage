package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/record"
)

// Pipelines from the database (pipelines_from: database, ADR 0004): the
// `pipelines` command edits the pipeline_spec table; status, check and
// validate read it.

const dbTimeout = 30 * time.Second

func newPipelinesCmd(o *rootOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipelines",
		Short: "List and edit pipelines stored in the database (pipelines_from: database)",
		Long: "With pipelines_from: database the engine takes its pipelines from the pipeline_spec\n" +
			"table and applies changes while it runs (docs/adr/0004-pipelines-from-database.md).\n" +
			"These commands read and write that table; database_url comes from the config.",
	}
	cmd.AddCommand(newPipelinesListCmd(o), newPipelinesApplyCmd(o), newPipelinesDeleteCmd(o),
		newPipelinesEnableCmd(o, true), newPipelinesEnableCmd(o, false))
	return cmd
}

// specDB loads the config, insists on database mode and connects.
func (o *rootOpts) specDB(ctx context.Context) (*config.File, *pgxpool.Pool, error) {
	cfg, err := o.loadConfig()
	if err != nil {
		return nil, nil, err
	}
	if !cfg.FromDatabase() {
		return nil, nil, fmt.Errorf("config %s reads pipelines from the file (set pipelines_from: database to keep them in the database)", o.configPath)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("database: %w", err)
	}
	return cfg, pool, nil
}

// specEntry is one pipeline_spec row and the outcome of parsing it.
type specEntry struct {
	record.PipelineSpec
	Pipeline config.Pipeline
	Err      error
}

// readSpecs reads every row and parses it with parse (config.ParsePipelineSpec
// to resolve secrets as the engine would, config.CheckPipelineSpec not to).
func readSpecs(ctx context.Context, q record.Querier, parse func(string, []byte) (config.Pipeline, error)) ([]specEntry, error) {
	rows, err := record.ListPipelineSpecs(ctx, q)
	if err != nil {
		if isUndefinedTable(err) {
			return nil, errNoSpecTable
		}
		return nil, err
	}
	out := make([]specEntry, 0, len(rows))
	for _, r := range rows {
		p, err := parse(r.Name, r.Spec)
		out = append(out, specEntry{PipelineSpec: r, Pipeline: p, Err: err})
	}
	return out, nil
}

func newPipelinesListCmd(o *rootOpts) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the pipelines in the database, with whether each spec is valid",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), dbTimeout)
			defer cancel()
			_, pool, err := o.specDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()
			// Structure only: secrets are resolved by the engine, which may
			// run elsewhere.
			specs, err := readSpecs(ctx, pool, config.CheckPipelineSpec)
			if err != nil {
				return err
			}
			return writeSpecList(cmd.OutOrStdout(), specs, asJSON)
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON instead of a table")
	return cmd
}

func writeSpecList(w io.Writer, specs []specEntry, asJSON bool) error {
	if asJSON {
		type item struct {
			record.PipelineSpec
			Error string `json:"error,omitempty"`
		}
		items := make([]item, 0, len(specs))
		for _, s := range specs {
			it := item{PipelineSpec: s.PipelineSpec}
			if s.Err != nil {
				it.Error = s.Err.Error()
			}
			items = append(items, it)
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(items)
	}
	if len(specs) == 0 {
		fmt.Fprintln(w, "No pipelines. Add some with `portage pipelines apply -f pipelines.yaml`.")
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tENABLED\tREVISION\tUPDATED\tSYNC")
	for _, s := range specs {
		sync := "INVALID: " + oneLine(fmt.Sprint(s.Err))
		if s.Err == nil {
			sync = endpointShort(s.Pipeline.Source) + "  →  " + endpointShort(s.Pipeline.Destination)
		}
		fmt.Fprintf(tw, "%s\t%t\t%d\t%s\t%s\n", s.Name, s.Enabled, s.Revision, s.UpdatedAt.Local().Format(time.DateTime), sync)
	}
	return tw.Flush()
}

func newPipelinesApplyCmd(o *rootOpts) *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "apply -f pipelines.yaml",
		Short: "Create or update pipelines from a file with a pipelines: list",
		Long: "Apply reads a file with a `pipelines:` list in the pipeline.yaml format, validates every\n" +
			"entry, then creates or updates one pipeline_spec row per entry (bumping its revision\n" +
			"when the spec changed). Nothing is written if any entry is invalid. Running engines\n" +
			"pick the changes up within seconds. Pipelines not in the file are left alone.\n\n" +
			"Credentials belong in secret://<name> references, resolved by each engine from\n" +
			"$PORTAGE_SECRETS_DIR/<name> or PORTAGE_SECRET_<NAME>; ${VAR} is not expanded.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if file == "" {
				return errors.New("-f/--file is required")
			}
			raw, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			specs, err := specsFromFile(raw)
			if err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), dbTimeout)
			defer cancel()
			_, pool, err := o.specDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := record.Migrate(ctx, pool); err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				for _, s := range specs {
					rev, changed, err := record.UpsertPipelineSpec(ctx, tx, s.name, s.spec)
					if err != nil {
						return err
					}
					switch {
					case !changed:
						fmt.Fprintf(w, "%s: unchanged (revision %d)\n", s.name, rev)
					case rev == 1:
						fmt.Fprintf(w, "%s: created (revision 1)\n", s.name)
					default:
						fmt.Fprintf(w, "%s: updated (revision %d)\n", s.name, rev)
					}
				}
				return nil
			})
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "file with a pipelines: list (pipeline.yaml format)")
	return cmd
}

type namedSpec struct {
	name string
	spec []byte // JSON, as stored in pipeline_spec.spec
}

// specsFromFile turns the pipelines: list of a pipeline.yaml-format file
// into pipeline_spec rows, validating each entry as the engine will (except
// that secret:// references are not resolved here).
func specsFromFile(raw []byte) ([]namedSpec, error) {
	var doc struct {
		Pipelines []map[string]any `yaml:"pipelines"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if len(doc.Pipelines) == 0 {
		return nil, errors.New("no pipelines: list")
	}
	var (
		out  []namedSpec
		errs []error
		seen = map[string]bool{}
	)
	for i, entry := range doc.Pipelines {
		name, _ := entry["name"].(string)
		at := fmt.Sprintf("pipelines[%d] (%s)", i, name)
		if name == "" {
			at = fmt.Sprintf("pipelines[%d]", i)
		}
		delete(entry, "name")
		if seen[name] {
			errs = append(errs, fmt.Errorf("%s: duplicate name", at))
			continue
		}
		seen[name] = true
		if ref := findEnvRef(entry, ""); ref != "" {
			errs = append(errs, fmt.Errorf("%s: %s: ${VAR} is not expanded in stored pipelines; use secret://<name>", at, ref))
			continue
		}
		spec, err := json.Marshal(entry)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", at, err))
			continue
		}
		if _, err := config.CheckPipelineSpec(name, spec); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", at, err))
			continue
		}
		out = append(out, namedSpec{name: name, spec: spec})
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return out, nil
}

// findEnvRef returns the path of the first string value containing "${".
func findEnvRef(v any, path string) string {
	switch t := v.(type) {
	case string:
		if strings.Contains(t, "${") {
			return path
		}
	case map[string]any:
		for k, x := range t {
			p := k
			if path != "" {
				p = path + "." + k
			}
			if found := findEnvRef(x, p); found != "" {
				return found
			}
		}
	case []any:
		for i, x := range t {
			if found := findEnvRef(x, fmt.Sprintf("%s[%d]", path, i)); found != "" {
				return found
			}
		}
	}
	return ""
}

// isUndefinedTable reports a Postgres undefined_table error.
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func newPipelinesDeleteCmd(o *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME...",
		Short: "Delete pipelines (running engines stop them; their file records stay)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, names []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), dbTimeout)
			defer cancel()
			_, pool, err := o.specDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()
			var missing []string
			for _, name := range names {
				found, err := record.DeletePipelineSpec(ctx, pool, name)
				if err != nil {
					return err
				}
				if !found {
					missing = append(missing, name)
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s: deleted\n", name)
			}
			if len(missing) > 0 {
				return fmt.Errorf("no such pipeline: %s", strings.Join(missing, ", "))
			}
			return nil
		},
	}
}

func newPipelinesEnableCmd(o *rootOpts, enable bool) *cobra.Command {
	use, short, done := "enable NAME...", "Enable pipelines (running engines start them)", "enabled"
	if !enable {
		use, short, done = "disable NAME...", "Disable pipelines (running engines stop them; nothing is deleted)", "disabled"
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, names []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), dbTimeout)
			defer cancel()
			_, pool, err := o.specDB(ctx)
			if err != nil {
				return err
			}
			defer pool.Close()
			var missing []string
			for _, name := range names {
				found, changed, err := record.SetPipelineEnabled(ctx, pool, name, enable)
				if err != nil {
					return err
				}
				switch {
				case !found:
					missing = append(missing, name)
				case changed:
					fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", name, done)
				default:
					fmt.Fprintf(cmd.OutOrStdout(), "%s: already %s\n", name, done)
				}
			}
			if len(missing) > 0 {
				return fmt.Errorf("no such pipeline: %s", strings.Join(missing, ", "))
			}
			return nil
		},
	}
}
