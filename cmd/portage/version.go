package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "portage %s\n", version)
			fmt.Fprintf(w, "  commit: %s\n", commit)
			fmt.Fprintf(w, "  built:  %s\n", date)
			fmt.Fprintf(w, "  go:     %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
			return nil
		},
	}
}
