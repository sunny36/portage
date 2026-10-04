package check

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// WriteJSON writes the report as indented JSON.
func WriteJSON(w io.Writer, rep Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

// WriteTable writes a CHECK / RESULT / DETAIL table per pipeline, followed
// by a FIX line for every failure and warning that has one, and a summary.
func WriteTable(w io.Writer, rep Report) error {
	var (
		order  []string
		groups = map[string][]Result{}
	)
	for _, r := range rep.Results {
		if _, seen := groups[r.Pipeline]; !seen {
			order = append(order, r.Pipeline)
		}
		groups[r.Pipeline] = append(groups[r.Pipeline], r)
	}
	for i, name := range order {
		if i > 0 {
			fmt.Fprintln(w)
		}
		if name == "" {
			fmt.Fprintln(w, "All pipelines")
		} else {
			fmt.Fprintf(w, "Pipeline %s\n", name)
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  CHECK\tRESULT\tDETAIL")
		for _, r := range groups[name] {
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", r.Check, r.Status, r.Detail)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
		for _, r := range groups[name] {
			if r.Fix == "" || (r.Status != StatusFail && r.Status != StatusWarn) {
				continue
			}
			fix := strings.ReplaceAll(r.Fix, "\n", "\n      ")
			fmt.Fprintf(w, "  FIX [%s] %s: %s\n", r.Status, r.Check, fix)
		}
	}
	fails, warns := 0, 0
	for _, r := range rep.Results {
		switch r.Status {
		case StatusFail:
			fails++
		case StatusWarn:
			warns++
		}
	}
	fmt.Fprintln(w)
	if fails > 0 {
		_, err := fmt.Fprintf(w, "FAIL: %d check(s) failed, %d warning(s). Fix the failures above and re-run `portage check`.\n", fails, warns)
		return err
	}
	_, err := fmt.Fprintf(w, "OK: all checks passed (%d warning(s)).\n", warns)
	return err
}
