// Package webhook receives Azure Event Grid blob events over HTTPS
// (hosted mode). See docs/adr/0003-change-sources.md.
//
// Requests must carry the pipeline's secret as the `key` query parameter of
// the subscription URL, e.g. https://host/events/p1?key=SECRET. The handler
// implements both endpoint-validation flows:
//
//   - Event Grid schema: a POST whose body holds a
//     Microsoft.EventGrid.SubscriptionValidationEvent is answered with
//     {"validationResponse": "<validationCode>"}.
//     https://learn.microsoft.com/en-us/azure/event-grid/webhook-event-delivery
//   - CloudEvents 1.0: an OPTIONS request with WebHook-Request-Origin is
//     answered with WebHook-Allowed-Origin (echoing the origin) and
//     WebHook-Allowed-Rate: *.
//     https://learn.microsoft.com/en-us/azure/event-grid/end-point-validation-cloud-events-schema
//
// Event batches are emitted one by one. If any emit fails the response is 503
// so Event Grid redelivers the whole batch (already-emitted changes are then
// emitted again, which is safe: delivery is at-least-once).
package webhook

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/change/eventgrid"
)

// DefaultMaxBodyBytes caps a delivery. Event Grid limits an event, and a
// delivery batch, to 1 MB, so a larger body is not from Event Grid.
const DefaultMaxBodyBytes = 1 << 20

// HandlerOptions configures NewHandler.
type HandlerOptions struct {
	PipelineID string
	// Secret is compared (in constant time) with the `key` query parameter.
	// An empty Secret rejects every request.
	Secret string
	// Container and Prefix are the pipeline's source container and prefix
	// ("" or ending in "/"); events outside them are acknowledged and dropped.
	Container string
	Prefix    string
	Filter    *change.Filter
	Emit      change.Emit
	Log       *slog.Logger
	// MaxBodyBytes defaults to DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// Now defaults to time.Now (for tests).
	Now func() time.Time
}

type handler struct {
	opts   HandlerOptions
	target eventgrid.Target
	secret [32]byte
	emit   func() change.Emit
}

// NewHandler returns the Event Grid webhook endpoint for one pipeline.
func NewHandler(opts HandlerOptions) http.Handler {
	emit := opts.Emit
	return newHandler(opts, func() change.Emit { return emit })
}

func newHandler(opts HandlerOptions, emit func() change.Emit) *handler {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	opts.Log = opts.Log.With("pipeline", opts.PipelineID, "source", "webhook")
	return &handler{
		opts: opts,
		target: eventgrid.Target{
			PipelineID: opts.PipelineID,
			Container:  opts.Container,
			Prefix:     opts.Prefix,
			Filter:     opts.Filter,
		},
		secret: sha256.Sum256([]byte(opts.Secret)),
		emit:   emit,
	}
}

func (h *handler) authorized(r *http.Request) bool {
	if h.opts.Secret == "" {
		return false
	}
	// Hash both sides so the comparison takes the same time whatever the
	// presented key's length.
	got := sha256.Sum256([]byte(r.URL.Query().Get("key")))
	return subtle.ConstantTimeCompare(got[:], h.secret[:]) == 1
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		h.opts.Log.Warn("webhook: rejected request with missing or wrong key", "remote", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.Method {
	case http.MethodOptions:
		h.abuseProtection(w, r)
	case http.MethodPost:
		h.post(w, r)
	default:
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// abuseProtection answers the CloudEvents 1.0 webhook validation handshake.
func (h *handler) abuseProtection(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "POST, OPTIONS")
	if origin := r.Header.Get("WebHook-Request-Origin"); origin != "" {
		w.Header().Set("WebHook-Allowed-Origin", origin)
		w.Header().Set("WebHook-Allowed-Rate", "*")
		h.opts.Log.Info("webhook: CloudEvents validation handshake accepted", "origin", origin)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *handler) post(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.opts.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	events, err := eventgrid.ParseJSON(body)
	if err != nil {
		h.opts.Log.Warn("webhook: unparseable delivery", "err", err)
		http.Error(w, "malformed event", http.StatusBadRequest)
		return
	}
	for _, ev := range events {
		if code := eventgrid.ValidationCode(ev); code != "" {
			h.opts.Log.Info("webhook: Event Grid subscription validation handshake", "event_id", ev.ID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"validationResponse": code})
			return
		}
	}
	emit := h.emit()
	if emit == nil {
		http.Error(w, "source not running", http.StatusServiceUnavailable)
		return
	}
	now := h.opts.Now()
	for _, ev := range events {
		c, ok, skip, err := h.target.Map(ev, now)
		if err != nil {
			// One bad event must not block (or endlessly retry) the batch.
			h.opts.Log.Warn("webhook: dropping malformed event", "event_id", ev.ID, "err", err)
			continue
		}
		if !ok {
			h.opts.Log.Debug("webhook: skipping event", "event_id", ev.ID, "type", ev.Type, "reason", skip)
			continue
		}
		if err := emit(r.Context(), c); err != nil {
			h.opts.Log.Warn("webhook: emit failed; asking Event Grid to retry", "key", c.Key, "err", err)
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// Source adapts the webhook to change.Source: mount Handler on the HTTP
// server, and run Source.Run with the pipeline's Emit like any other source.
// Until Run is called (and after it returns) deliveries get 503, so Event
// Grid retries them later.
type Source struct {
	h    *handler
	emit atomic.Pointer[change.Emit]
}

var _ change.Source = (*Source)(nil)

// NewSource builds a webhook Source. opts.Emit is ignored; the Emit passed
// to Run is used.
func NewSource(opts HandlerOptions) *Source {
	s := &Source{}
	s.h = newHandler(opts, func() change.Emit {
		if p := s.emit.Load(); p != nil {
			return *p
		}
		return nil
	})
	return s
}

// Name implements change.Source.
func (s *Source) Name() string { return "webhook" }

// Handler is the http.Handler to mount at the pipeline's webhook path.
func (s *Source) Handler() http.Handler { return s.h }

// Run implements change.Source: it enables delivery until ctx is done.
func (s *Source) Run(ctx context.Context, emit change.Emit) error {
	s.emit.Store(&emit)
	defer s.emit.Store(nil)
	<-ctx.Done()
	return ctx.Err()
}
