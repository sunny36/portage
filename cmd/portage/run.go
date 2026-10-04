package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/metrics"
	"github.com/sunny36/portage/internal/pipeline"
)

func newRunCmd(o *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run every pipeline in the config until interrupted",
		Long: "Run loads the config, serves /metrics, /healthz and /readyz on metrics_addr, and\n" +
			"syncs every pipeline until SIGINT/SIGTERM (graceful). A second signal forces exit.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			log, err := o.logger(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			cfg, err := o.loadConfig()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			stopSignals := handleSignals(cancel, log)
			defer stopSignals()
			return runEngine(ctx, cancel, cfg, log)
		},
	}
}

// handleSignals cancels on the first SIGINT/SIGTERM and exits the process
// on the second.
func handleSignals(cancel context.CancelFunc, log *slog.Logger) (stop func()) {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case s := <-sigs:
			log.Info("shutting down; send the signal again to force exit", "signal", s.String())
			cancel()
		case <-done:
			return
		}
		select {
		case s := <-sigs:
			log.Error("forced exit", "signal", s.String())
			os.Exit(130)
		case <-done:
		}
	}()
	return func() {
		signal.Stop(sigs)
		close(done)
	}
}

func runEngine(ctx context.Context, cancel context.CancelFunc, cfg *config.File, log *slog.Logger) error {
	m, metricsHandler, err := metrics.New()
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer scancel()
		_ = m.Shutdown(sctx)
	}()

	// Readiness = the file record database is reachable. The pool is lazy
	// and tiny: it only serves /readyz.
	readyPool, err := newReadyPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer readyPool.Close()

	serveErr := make(chan error, 1)
	go func() {
		err := metrics.Serve(ctx, cfg.MetricsAddr, metricsHandler, readyPool.Ping, log)
		if err != nil {
			log.Error("metrics server failed; stopping", "err", err)
			cancel()
		}
		serveErr <- err
	}()

	log.Info("starting portage", "version", version, "pipelines", len(cfg.Pipelines), "metrics_addr", cfg.MetricsAddr)
	runErr := pipeline.Run(ctx, cfg, m, log)
	cancel()
	sErr := <-serveErr

	switch {
	case errors.Is(runErr, pipeline.ErrNotImplemented):
		return fmt.Errorf("%w: this build of portage cannot sync yet (the pipeline runtime is not implemented)", runErr)
	case runErr != nil:
		return runErr
	case sErr != nil:
		return sErr
	}
	log.Info("portage stopped")
	return nil
}

func newReadyPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("database_url: %w", err)
	}
	pc.MaxConns = 1
	pc.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	return pool, nil
}
