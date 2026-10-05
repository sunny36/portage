// Command portage is the Portage sync engine CLI.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/config"
)

// Set by -ldflags at release (see .goreleaser.yaml and the Makefile).
var version, commit, date = "dev", "none", "unknown"

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// errSilent is returned by commands that already printed their failure; it
// only sets the exit code.
var errSilent = errors.New("")

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	root := newRootCmd()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		if !errors.Is(err, errSilent) {
			fmt.Fprintf(stderr, "portage: %v\n", err)
		}
		return 1
	}
	return 0
}

type rootOpts struct {
	configPath string
	logFormat  string
	logLevel   string
}

func defaultConfigPath() string {
	if p := os.Getenv("PORTAGE_CONFIG"); p != "" {
		return p
	}
	return "pipeline.yaml"
}

func newRootCmd() *cobra.Command {
	o := &rootOpts{}
	root := &cobra.Command{
		Use:   "portage",
		Short: "Continuous one-way object-storage sync",
		Long: "Portage keeps a destination bucket in sync with a source container, one way,\n" +
			"continuously: change events drive copies and a periodic reconcile catches anything missed.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringVarP(&o.configPath, "config", "c", defaultConfigPath(), "pipeline config file (env PORTAGE_CONFIG)")
	pf.StringVar(&o.logFormat, "log-format", "text", "log format: text or json")
	pf.StringVar(&o.logLevel, "log-level", "info", "log level: debug, info, warn or error")

	root.AddCommand(newRunCmd(o), newCheckCmd(o), newStatusCmd(o), newValidateCmd(o), newPipelinesCmd(o), newVersionCmd())
	return root
}

func (o *rootOpts) loadConfig() (*config.File, error) {
	cfg, err := config.Load(o.configPath)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", o.configPath, err)
	}
	return cfg, nil
}

func (o *rootOpts) logger(w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(o.logLevel)); err != nil {
		return nil, fmt.Errorf("--log-level %q: want debug, info, warn or error", o.logLevel)
	}
	hopts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(o.logFormat) {
	case "text":
		return slog.New(slog.NewTextHandler(w, hopts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, hopts)), nil
	default:
		return nil, fmt.Errorf("--log-format %q: want text or json", o.logFormat)
	}
}
