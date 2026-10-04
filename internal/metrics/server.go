package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const (
	readyTimeout    = 3 * time.Second
	shutdownTimeout = 5 * time.Second
)

// Serve serves /metrics (metricsHandler), /healthz (200 while the process is
// up) and /readyz (200 when ready returns nil, 503 otherwise; ready may be
// nil) on addr until ctx is cancelled, then shuts down gracefully. It
// returns nil after a clean shutdown and an error if the listener fails
// (e.g. the port is taken).
func Serve(ctx context.Context, addr string, metricsHandler http.Handler, ready func(context.Context) error, log *slog.Logger) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	return serve(ctx, ln, metricsHandler, ready, log)
}

func serve(ctx context.Context, ln net.Listener, metricsHandler http.Handler, ready func(context.Context) error, log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	srv := &http.Server{
		Handler:           newMux(metricsHandler, ready, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("metrics server listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		return fmt.Errorf("metrics: serve: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("metrics: shutdown: %w", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics: serve: %w", err)
	}
	return nil
}

func newMux(metricsHandler http.Handler, ready func(context.Context) error, log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	if metricsHandler == nil {
		metricsHandler = http.NotFoundHandler()
	}
	mux.Handle("GET /metrics", metricsHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
			defer cancel()
			if err := ready(ctx); err != nil {
				log.Debug("readiness check failed", "err", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, "not ready: %v\n", err)
				return
			}
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}
