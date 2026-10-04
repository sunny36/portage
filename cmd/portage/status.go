package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/record"
)

const (
	noDataMsg     = "no data yet — has `portage run` started?"
	statusTimeout = 15 * time.Second
	errorMaxLen   = 160
)

type statusOpts struct {
	json  bool
	watch time.Duration
}

func newStatusCmd(o *rootOpts) *cobra.Command {
	so := &statusOpts{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show sync progress, current lag and recent errors per pipeline",
		Long: "Status reads the file record in database_url (read-only; it never migrates) and\n" +
			"prints counts, bytes synced, time since the last sync and the age of the oldest\n" +
			"pending change (the lag building up right now), plus recent errors.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if so.watch < 0 {
				return errors.New("--watch must be positive")
			}
			cfg, err := o.loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if so.watch > 0 {
				var stop context.CancelFunc
				ctx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
				defer stop()
			}
			pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
			if err != nil {
				return fmt.Errorf("database: %w", err)
			}
			defer pool.Close()
			return runStatus(ctx, cmd.OutOrStdout(), cfg, record.NewPGStore(pool), so, time.Now)
		},
	}
	cmd.Flags().BoolVar(&so.json, "json", false, "print JSON instead of a table")
	cmd.Flags().DurationVar(&so.watch, "watch", 0, "refresh every interval (e.g. 5s) until interrupted")
	return cmd
}

type statsSource interface {
	Stats(ctx context.Context, pipelineID string) (record.Stats, error)
}

type pipelineStatus struct {
	Name               string        `json:"name"`
	Synced             int64         `json:"synced"`
	Pending            int64         `json:"pending"`
	Copying            int64         `json:"copying"`
	Failed             int64         `json:"failed"`
	BytesSynced        int64         `json:"bytes_synced"`
	LastSyncedAt       *time.Time    `json:"last_synced_at"`
	OldestPendingEvent *time.Time    `json:"oldest_pending_event"`
	CurrentLagSeconds  float64       `json:"current_lag_seconds"`
	RecentErrors       []recentError `json:"recent_errors"`
}

type recentError struct {
	Key       string    `json:"key"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	UpdatedAt time.Time `json:"updated_at"`
}

type statusReport struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Note        string           `json:"note,omitempty"`
	Pipelines   []pipelineStatus `json:"pipelines"`
}

func runStatus(ctx context.Context, w io.Writer, cfg *config.File, store statsSource, so *statusOpts, now func() time.Time) error {
	for {
		rep, err := collectStatus(ctx, cfg, store, now())
		if err != nil {
			if so.watch > 0 && ctx.Err() != nil {
				return nil
			}
			return err
		}
		if so.watch > 0 && !so.json {
			fmt.Fprint(w, "\033[H\033[2J") // clear screen
		}
		if err := renderStatus(w, rep, so); err != nil {
			return err
		}
		if so.watch <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(so.watch):
		}
	}
}

func collectStatus(ctx context.Context, cfg *config.File, store statsSource, now time.Time) (statusReport, error) {
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	rep := statusReport{GeneratedAt: now.UTC(), Pipelines: []pipelineStatus{}}
	for _, p := range cfg.Pipelines {
		st, err := store.Stats(ctx, p.Name)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
				rep.Note = noDataMsg
				rep.Pipelines = []pipelineStatus{}
				return rep, nil
			}
			return rep, fmt.Errorf("status of %s: %w", p.Name, err)
		}
		ps := pipelineStatus{
			Name: p.Name, Synced: st.Synced, Pending: st.Pending, Copying: st.Copying, Failed: st.Failed,
			BytesSynced: st.BytesSynced, RecentErrors: []recentError{},
		}
		if !st.LastSyncedAt.IsZero() {
			t := st.LastSyncedAt.UTC()
			ps.LastSyncedAt = &t
		}
		if !st.OldestPendingEvent.IsZero() {
			t := st.OldestPendingEvent.UTC()
			ps.OldestPendingEvent = &t
			ps.CurrentLagSeconds = max(now.Sub(t), 0).Seconds()
		}
		for _, r := range st.RecentErrors {
			ps.RecentErrors = append(ps.RecentErrors, recentError{
				Key: r.Key, Status: string(r.Status), Attempts: r.Attempts, LastError: r.LastError, UpdatedAt: r.UpdatedAt.UTC(),
			})
		}
		rep.Pipelines = append(rep.Pipelines, ps)
	}
	return rep, nil
}

func renderStatus(w io.Writer, rep statusReport, so *statusOpts) error {
	if so.json {
		enc := json.NewEncoder(w)
		if so.watch <= 0 {
			enc.SetIndent("", "  ")
		}
		return enc.Encode(rep)
	}
	if so.watch > 0 {
		fmt.Fprintf(w, "portage status — %s (every %s, Ctrl-C to stop)\n\n", rep.GeneratedAt.Local().Format(time.TimeOnly), so.watch)
	}
	if rep.Note != "" {
		fmt.Fprintln(w, rep.Note)
		return nil
	}
	now := rep.GeneratedAt
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tSYNCED\tPENDING\tCOPYING\tFAILED\tBYTES\tLAST SYNC\tOLDEST PENDING")
	for _, p := range rep.Pipelines {
		last := "never"
		if p.LastSyncedAt != nil {
			last = ago(now, *p.LastSyncedAt)
		}
		oldest := "-"
		if p.OldestPendingEvent != nil {
			oldest = ago(now, *p.OldestPendingEvent)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
			p.Name, p.Synced, p.Pending, p.Copying, p.Failed, humanBytes(p.BytesSynced), last, oldest)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, p := range rep.Pipelines {
		if len(p.RecentErrors) == 0 {
			continue
		}
		fmt.Fprintf(w, "\nRecent errors — %s:\n", p.Name)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, e := range p.RecentErrors {
			fmt.Fprintf(tw, "  %s\t%s\t%d attempt(s)\t%s\t%s\n",
				e.Key, e.Status, e.Attempts, ago(now, e.UpdatedAt), oneLine(e.LastError))
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > errorMaxLen {
		s = string(r[:errorMaxLen-1]) + "…"
	}
	return s
}
