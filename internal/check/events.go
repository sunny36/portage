package check

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/sunny36/portage/internal/change/eventgrid"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
)

// poisonSuffix matches internal/change/azqueue: undecodable messages are
// moved to "<queue>-poison".
const poisonSuffix = "-poison"

// peekMax is the service maximum for Peek Messages.
const peekMax = 32

// QueueProber is the read-only view of an event queue the checks need.
// Implementations must not dequeue or delete messages.
type QueueProber interface {
	// Peek returns the text of up to 32 messages without changing their
	// visibility.
	Peek(ctx context.Context) ([]string, error)
	// ApproximateCount returns the queue's approximate message count.
	ApproximateCount(ctx context.Context) (int64, error)
	// PoisonExists reports whether "<queue>-poison" exists.
	PoisonExists(ctx context.Context) (bool, error)
}

func (r *runner) events(ctx context.Context, p config.Pipeline) []Result {
	switch p.Events.Type {
	case "none":
		return []Result{{Pipeline: p.Name, Check: CheckEvents, Status: StatusWarn,
			Detail: fmt.Sprintf("events.type none: only the reconciler will detect changes (lag up to reconcile_interval, %s)",
				p.ReconcileInterval),
			Fix: "For an Azure source, route Event Grid blob events to a Storage Queue (deploy/azure/setup.sh) " +
				"and set events.type: azure_queue."}}
	case "webhook":
		return []Result{{Pipeline: p.Name, Check: CheckEvents, Status: StatusOK,
			Detail: fmt.Sprintf("webhook path %s, secret %d chars (inbound reachability is not tested; Event Grid must reach "+
				"https://<public host>%s?key=<secret>)", p.Events.WebhookPath, len(p.Events.WebhookSecret), p.Events.WebhookPath)}}
	case "azure_queue":
	default:
		return []Result{skipped(p.Name, CheckEvents, "unknown events.type "+p.Events.Type)}
	}

	t := Target{Role: RoleQueue, Endpoint: p.Source, Events: p.Events}
	q, err := r.opts.NewQueue(p)
	if err != nil {
		return []Result{
			fail(p.Name, CheckEvents, fmt.Errorf("create queue client: %w", err), t),
			skipped(p.Name, CheckPoisonQueue, "queue client could not be created"),
		}
	}
	main := r.timed(ctx, p.Name, CheckEvents, func(ctx context.Context) Result {
		msgs, err := q.Peek(ctx)
		if err != nil {
			return failRes(err, t)
		}
		count := "count unavailable"
		if n, err := q.ApproximateCount(ctx); err == nil {
			count = fmt.Sprintf("~%d message(s) waiting", n)
		} else {
			count += " (" + describeErr(err) + ")"
		}
		detail := fmt.Sprintf("queue %s reachable via peek (nothing consumed); %s", p.Events.QueueName, count)
		if len(msgs) == 0 {
			return warn(detail+"; none to inspect",
				"To confirm Event Grid delivers, upload a test blob to the source container, wait a few seconds and "+
					"re-run (or `az storage message peek --queue-name "+p.Events.QueueName+" --account-name <account> --auth-mode login`).")
		}
		bad := 0
		for _, m := range msgs {
			if _, err := eventgrid.ParseMessage([]byte(m)); err != nil {
				bad++
			}
		}
		if bad > 0 {
			return warn(fmt.Sprintf("%s; %d of %d peeked message(s) are not Event Grid events", detail, bad, len(msgs)),
				"The event subscription must use the Event Grid event schema (`--event-delivery-schema eventgridschema`), "+
					"and nothing else should write to this queue. Undecodable messages go to "+p.Events.QueueName+poisonSuffix+".")
		}
		return ok(fmt.Sprintf("%s; %d peeked message(s) parse as Event Grid events", detail, len(msgs)))
	})
	if main.Status == StatusFail {
		return []Result{main, skipped(p.Name, CheckPoisonQueue, "queue check failed")}
	}
	poisonName := p.Events.QueueName + poisonSuffix
	poison := r.timed(ctx, p.Name, CheckPoisonQueue, func(ctx context.Context) Result {
		exists, err := q.PoisonExists(ctx)
		switch {
		case err != nil:
			return warn("could not check "+poisonName+": "+describeErr(err),
				"The engine creates and writes "+poisonName+" when a message cannot be decoded. Grant "+
					"\"Storage Queue Data Contributor\" on it (pre-create it so the grant can be scoped to it).")
		case !exists:
			return warn(poisonName+" does not exist yet",
				"The engine creates it on the first undecodable message, which needs queue-create permission. "+
					"Pre-create it and grant \"Storage Queue Data Contributor\" on it (deploy/azure/setup.sh does both).")
		}
		return ok(poisonName + " exists")
	})
	return []Result{main, poison}
}

// azureQueue implements QueueProber with the Azure SDK.
type azureQueue struct {
	q, poison *azqueue.QueueClient
}

func newAzureQueue(p config.Pipeline) (QueueProber, error) {
	if p.Source.Azure == nil {
		return nil, errors.New("azure_queue needs an azure source")
	}
	svc, err := queueService(p.Events.QueueAccountURL, *p.Source.Azure)
	if err != nil {
		return nil, err
	}
	return &azureQueue{
		q:      svc.NewQueueClient(p.Events.QueueName),
		poison: svc.NewQueueClient(p.Events.QueueName + poisonSuffix),
	}, nil
}

// queueService builds a queue service client with the source's credentials,
// the same way internal/change/azqueue does.
func queueService(accountURL string, a config.AzureConfig) (*azqueue.ServiceClient, error) {
	switch a.Auth {
	case "", "default":
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("default credential: %w", err)
		}
		return azqueue.NewServiceClient(accountURL, cred, nil)
	case "shared_key":
		cred, err := azqueue.NewSharedKeyCredential(a.AccountName, a.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("shared key: %w", err)
		}
		return azqueue.NewServiceClientWithSharedKeyCredential(accountURL, cred, nil)
	case "connection_string":
		return azqueue.NewServiceClientFromConnectionString(a.ConnectionString, nil)
	}
	return nil, fmt.Errorf("unknown auth %q", a.Auth)
}

func (a *azureQueue) Peek(ctx context.Context) ([]string, error) {
	resp, err := a.q.PeekMessages(ctx, &azqueue.PeekMessagesOptions{NumberOfMessages: to.Ptr(int32(peekMax))})
	if err != nil {
		return nil, queueErr("PeekMessages", err)
	}
	var out []string
	for _, m := range resp.Messages {
		if m != nil && m.MessageText != nil {
			out = append(out, *m.MessageText)
		}
	}
	return out, nil
}

func (a *azureQueue) ApproximateCount(ctx context.Context) (int64, error) {
	resp, err := a.q.GetProperties(ctx, nil)
	if err != nil {
		return 0, queueErr("GetProperties", err)
	}
	if resp.ApproximateMessagesCount == nil {
		return 0, errors.New("no count in response")
	}
	return int64(*resp.ApproximateMessagesCount), nil
}

func (a *azureQueue) PoisonExists(ctx context.Context) (bool, error) {
	_, err := a.poison.GetProperties(ctx, nil)
	if err == nil {
		return true, nil
	}
	err = queueErr("GetProperties", err)
	if errors.Is(err, connector.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// queueErr maps a queue SDK error onto the connector sentinels so Hint can
// treat queues like containers.
func queueErr(op string, err error) error {
	pe := &connector.ProviderError{Op: op, Err: err}
	var re *azcore.ResponseError
	var authFailed *azidentity.AuthenticationFailedError
	switch {
	case errors.As(err, &re):
		pe.Status, pe.Code = re.StatusCode, re.ErrorCode
		switch {
		case re.ErrorCode == "QueueNotFound" || re.StatusCode == http.StatusNotFound:
			pe.Sentinel = connector.ErrNotFound
		case re.ErrorCode == "AuthenticationFailed" || re.StatusCode == http.StatusUnauthorized:
			pe.Sentinel = connector.ErrAuth
		case re.StatusCode == http.StatusForbidden:
			pe.Sentinel = connector.ErrPermission
		}
	case errors.As(err, &authFailed):
		pe.Sentinel = connector.ErrAuth
	}
	return pe
}
