// Package azqueue is the default Azure change source: Event Grid delivers
// blob events to an Azure Storage Queue and Portage polls it, so a
// self-hosted engine needs nothing inbound. See
// docs/adr/0003-change-sources.md.
//
// Delivery is at-least-once: a message is deleted only after every change in
// it was emitted. If Emit fails the message is left alone and reappears after
// the visibility timeout. Messages that cannot be parsed, or that have been
// dequeued more than MaxDequeue times, are copied to "<queue>-poison" and
// then deleted.
//
// Message encoding: Event Grid's Storage Queue handler writes one event per
// message, base64-encoded (Microsoft's samples read such queues with
// TextBase64DecodePolicy:
// https://learn.microsoft.com/en-us/azure/cyclecloud/how-to/event-grid).
// Raw JSON bodies are accepted too.
package azqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/queueerror"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/change/eventgrid"
	"github.com/sunny36/portage/internal/config"
)

// Defaults for Options.
const (
	DefaultBatchSize         = 32 // service maximum per Get Messages
	DefaultVisibilityTimeout = 5 * time.Minute
	DefaultMaxEmptyBackoff   = 5 * time.Second
	DefaultMaxErrorBackoff   = 30 * time.Second
	DefaultMaxDequeue        = 5

	minBackoff     = 100 * time.Millisecond
	poisonSuffix   = "-poison"
	maxQueueName   = 63
	poisonMaxBytes = 64 << 10 // queue message size limit
)

// Options configures New.
type Options struct {
	PipelineID string
	// QueueAccountURL is the queue service URL, e.g.
	// https://acct.queue.core.windows.net or, for Azurite,
	// http://127.0.0.1:10001/devstoreaccount1. Ignored for
	// auth=connection_string (the connection string's QueueEndpoint is used).
	QueueAccountURL string
	QueueName       string
	// Auth reuses the source's credentials: Auth, AccountName, AccountKey and
	// ConnectionString are read; AccountURL and Container are not.
	Auth config.AzureConfig

	// Container and Prefix are the pipeline's source container and prefix
	// ("" or ending in "/").
	Container string
	Prefix    string
	Filter    *change.Filter

	BatchSize         int           // default 32 (max 32)
	VisibilityTimeout time.Duration // default 5m
	MaxEmptyBackoff   time.Duration // idle polling backs off up to this; default 5s
	MaxErrorBackoff   time.Duration // transient errors back off up to this; default 30s
	MaxDequeue        int           // default 5
	Log               *slog.Logger

	// Now defaults to time.Now (for tests).
	Now func() time.Time
}

// queue is the subset of *azqueue.QueueClient the source uses.
type queue interface {
	Create(ctx context.Context, o *azqueue.CreateOptions) (azqueue.CreateResponse, error)
	DequeueMessages(ctx context.Context, o *azqueue.DequeueMessagesOptions) (azqueue.DequeueMessagesResponse, error)
	DeleteMessage(ctx context.Context, messageID, popReceipt string, o *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error)
	EnqueueMessage(ctx context.Context, content string, o *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error)
}

// Source polls one Storage Queue for one pipeline.
type Source struct {
	opts         Options
	target       eventgrid.Target
	q, poison    queue
	poisonExists bool // only touched by Run's goroutine
	log          *slog.Logger
}

var _ change.Source = (*Source)(nil)

// New validates opts and builds the queue clients. It does no I/O.
func New(opts Options) (*Source, error) {
	if opts.QueueName == "" {
		return nil, errors.New("azqueue: queue name is required")
	}
	if len(opts.QueueName)+len(poisonSuffix) > maxQueueName {
		return nil, fmt.Errorf("azqueue: queue name %q is too long: %q must also be a valid queue name (max %d chars)",
			opts.QueueName, opts.QueueName+poisonSuffix, maxQueueName)
	}
	if opts.Container == "" {
		return nil, errors.New("azqueue: source container is required")
	}
	svc, err := serviceClient(opts)
	if err != nil {
		return nil, err
	}
	return newSource(opts, svc.NewQueueClient(opts.QueueName), svc.NewQueueClient(opts.QueueName+poisonSuffix)), nil
}

func newSource(opts Options, q, poison queue) *Source {
	if opts.BatchSize <= 0 || opts.BatchSize > DefaultBatchSize {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.VisibilityTimeout < time.Second {
		opts.VisibilityTimeout = DefaultVisibilityTimeout
	}
	if opts.MaxEmptyBackoff <= 0 {
		opts.MaxEmptyBackoff = DefaultMaxEmptyBackoff
	}
	if opts.MaxErrorBackoff <= 0 {
		opts.MaxErrorBackoff = DefaultMaxErrorBackoff
	}
	if opts.MaxDequeue <= 0 {
		opts.MaxDequeue = DefaultMaxDequeue
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Source{
		opts: opts,
		target: eventgrid.Target{
			PipelineID: opts.PipelineID,
			Container:  opts.Container,
			Prefix:     opts.Prefix,
			Filter:     opts.Filter,
		},
		q:      q,
		poison: poison,
		log:    opts.Log.With("pipeline", opts.PipelineID, "source", "azure_queue", "queue", opts.QueueName),
	}
}

func serviceClient(opts Options) (*azqueue.ServiceClient, error) {
	a := opts.Auth
	var (
		svc *azqueue.ServiceClient
		err error
	)
	switch a.Auth {
	case "", "default":
		if opts.QueueAccountURL == "" {
			return nil, errors.New("azqueue: queue_account_url is required for auth=default")
		}
		cred, cerr := azidentity.NewDefaultAzureCredential(nil)
		if cerr != nil {
			return nil, fmt.Errorf("azqueue: default credential: %w", cerr)
		}
		svc, err = azqueue.NewServiceClient(opts.QueueAccountURL, cred, nil)
	case "shared_key":
		if opts.QueueAccountURL == "" || a.AccountName == "" || a.AccountKey == "" {
			return nil, errors.New("azqueue: queue_account_url, account_name and account_key are required for auth=shared_key")
		}
		cred, cerr := azqueue.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if cerr != nil {
			return nil, fmt.Errorf("azqueue: shared key: %w", cerr)
		}
		svc, err = azqueue.NewServiceClientWithSharedKeyCredential(opts.QueueAccountURL, cred, nil)
	case "connection_string":
		if a.ConnectionString == "" {
			return nil, errors.New("azqueue: connection_string is required for auth=connection_string")
		}
		svc, err = azqueue.NewServiceClientFromConnectionString(a.ConnectionString, nil)
	default:
		return nil, fmt.Errorf("azqueue: unknown auth %q (want default, shared_key or connection_string)", a.Auth)
	}
	if err != nil {
		return nil, fmt.Errorf("azqueue: client: %w", err)
	}
	return svc, nil
}

// Name implements change.Source.
func (s *Source) Name() string { return "azure_queue" }

// Run implements change.Source. It returns ctx.Err() on shutdown, or an
// error when the queue is unusable (bad credentials, missing permission,
// queue not found). Other errors are logged and retried with backoff.
func (s *Source) Run(ctx context.Context, emit change.Emit) error {
	idle, errWait := time.Duration(0), time.Duration(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := s.q.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
			NumberOfMessages:  to.Ptr(int32(s.opts.BatchSize)),
			VisibilityTimeout: to.Ptr(int32(s.opts.VisibilityTimeout / time.Second)),
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if fatal(err) {
				return fmt.Errorf("azqueue: receive from %q: %w", s.opts.QueueName, err)
			}
			errWait = grow(errWait, s.opts.MaxErrorBackoff)
			s.log.Warn("azqueue: receive failed; backing off", "err", err, "backoff", errWait)
			if err := sleep(ctx, errWait); err != nil {
				return err
			}
			continue
		}
		errWait = 0
		if len(resp.Messages) == 0 {
			idle = grow(idle, s.opts.MaxEmptyBackoff)
			if err := sleep(ctx, idle); err != nil {
				return err
			}
			continue
		}
		idle = 0
		for _, m := range resp.Messages {
			if err := s.handle(ctx, m, emit); err != nil {
				return err
			}
		}
	}
}

func grow(d, maxD time.Duration) time.Duration {
	if d < minBackoff {
		return minBackoff
	}
	d *= 2
	if d > maxD {
		d = maxD
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// handle processes one message. It returns an error only when Run must stop.
func (s *Source) handle(ctx context.Context, m *azqueue.DequeuedMessage, emit change.Emit) error {
	if m == nil || m.MessageID == nil || m.PopReceipt == nil {
		return nil
	}
	id := *m.MessageID
	text := ""
	if m.MessageText != nil {
		text = *m.MessageText
	}
	var dequeues int64
	if m.DequeueCount != nil {
		dequeues = *m.DequeueCount
	}
	if dequeues > int64(s.opts.MaxDequeue) {
		return s.toPoison(ctx, m, text, fmt.Sprintf("dequeued %d times (max %d)", dequeues, s.opts.MaxDequeue))
	}
	events, err := eventgrid.ParseMessage([]byte(text))
	if err != nil {
		return s.toPoison(ctx, m, text, err.Error())
	}
	now := s.opts.Now()
	changes := make([]change.ObjectChanged, 0, len(events))
	for _, ev := range events {
		c, ok, skip, err := s.target.Map(ev, now)
		if err != nil {
			return s.toPoison(ctx, m, text, err.Error())
		}
		if !ok {
			s.log.Debug("azqueue: skipping event", "message_id", id, "event_id", ev.ID, "type", ev.Type, "reason", skip)
			continue
		}
		changes = append(changes, c)
	}
	for _, c := range changes {
		if err := emit(ctx, c); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Leave the message: it becomes visible again after the
			// visibility timeout and is retried (or poisoned after
			// MaxDequeue attempts).
			s.log.Warn("azqueue: emit failed; message will be redelivered", "message_id", id, "key", c.Key, "err", err)
			return nil
		}
	}
	return s.deleteMessage(ctx, m)
}

func (s *Source) deleteMessage(ctx context.Context, m *azqueue.DequeuedMessage) error {
	_, err := s.q.DeleteMessage(ctx, *m.MessageID, *m.PopReceipt, nil)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return ctx.Err()
	case fatal(err):
		return fmt.Errorf("azqueue: delete message: %w", err)
	default:
		// Most likely the visibility timeout expired and another consumer
		// took the message (pop receipt mismatch / MessageNotFound). It will
		// be processed again; emits are idempotent downstream.
		s.log.Warn("azqueue: delete message failed; it may be redelivered", "message_id", *m.MessageID, "err", err)
		return nil
	}
}

// toPoison copies m to the poison queue (creating it if needed), then
// deletes it from the main queue. If the copy fails the original is kept so
// nothing is lost; it will be retried after the visibility timeout.
func (s *Source) toPoison(ctx context.Context, m *azqueue.DequeuedMessage, text, reason string) error {
	poisonName := s.opts.QueueName + poisonSuffix
	s.log.Error("azqueue: moving message to poison queue", "message_id", *m.MessageID, "poison_queue", poisonName, "reason", reason)
	if len(text) > poisonMaxBytes {
		text = text[:poisonMaxBytes]
	}
	if err := s.enqueuePoison(ctx, text); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fatal(err) {
			return fmt.Errorf("azqueue: poison queue %q: %w", poisonName, err)
		}
		s.log.Warn("azqueue: poison enqueue failed; leaving message in place", "message_id", *m.MessageID, "err", err)
		return nil
	}
	return s.deleteMessage(ctx, m)
}

func (s *Source) enqueuePoison(ctx context.Context, text string) error {
	if !s.poisonExists {
		if _, err := s.poison.Create(ctx, nil); err != nil && !queueerror.HasCode(err, queueerror.QueueAlreadyExists) {
			return err
		}
		s.poisonExists = true
	}
	// TimeToLive -1: keep poison messages until an operator looks at them.
	_, err := s.poison.EnqueueMessage(ctx, text, &azqueue.EnqueueMessageOptions{TimeToLive: to.Ptr(int32(-1))})
	if err != nil && queueerror.HasCode(err, queueerror.QueueNotFound) {
		s.poisonExists = false // deleted behind our back; recreate next time
	}
	return err
}

// fatal reports whether err means the queue cannot be used at all, so Run
// should stop and surface it rather than retry forever.
func fatal(err error) bool {
	var authFailed *azidentity.AuthenticationFailedError
	var authRequired *azidentity.AuthenticationRequiredError
	if errors.As(err, &authFailed) || errors.As(err, &authRequired) {
		return true
	}
	// azidentity marks "no credential available" (e.g. DefaultAzureCredential
	// found nothing) as non-retriable.
	var nonRetriable interface{ NonRetriable() }
	if errors.As(err, &nonRetriable) {
		return true
	}
	var re *azcore.ResponseError
	if !errors.As(err, &re) {
		return false
	}
	switch re.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return true
	}
	return re.ErrorCode == string(queueerror.QueueNotFound) ||
		strings.HasPrefix(re.ErrorCode, "Authentication") ||
		strings.HasPrefix(re.ErrorCode, "Authorization")
}
