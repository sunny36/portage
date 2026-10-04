// Package pipeline wires change sources, the job queue, transfer, verify and
// the file record into a running sync. Owned by the lead integrator.
package pipeline

import (
	"context"
	"errors"
	"log/slog"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/metrics"
)

// ErrNotImplemented is returned until the runtime is wired (Wave 2).
var ErrNotImplemented = errors.New("pipeline: runtime not wired yet")

// Run starts every pipeline in cfg and blocks until ctx is cancelled (clean
// shutdown, returns nil) or a fatal error occurs. m may be nil.
func Run(ctx context.Context, cfg *config.File, m *metrics.Metrics, log *slog.Logger) error {
	return ErrNotImplemented
}
