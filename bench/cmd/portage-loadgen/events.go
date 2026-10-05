package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue/queueerror"
)

// eventSink receives one synthetic Event Grid BlobCreated event per blob
// written. Locally (Azurite has no Event Grid) this exercises Portage's
// azure_queue change source and event-driven sync lag; against real Azure,
// leave it off and let Event Grid deliver the real events.
type eventSink interface {
	BlobCreated(ctx context.Context, ev blobCreated) error
}

// blobCreated is what the sink needs to describe one committed blob.
type blobCreated struct {
	BlobURL   string // full blob URL, as Event Grid reports it
	Container string
	Name      string
	ETag      string
	Size      int64
	At        time.Time // commit time; becomes the event's eventTime
}

var eventSeq atomic.Int64

// eventGridMessage builds the Storage Queue message Event Grid would deliver
// for ev: one Event Grid schema event, JSON, base64-encoded. Same shape as
// sendBlobCreated in internal/pipeline/e2e_integration_test.go.
func eventGridMessage(ev blobCreated) (string, error) {
	n := eventSeq.Add(1)
	body := map[string]any{
		"id":        fmt.Sprintf("loadgen-%d-%d", ev.At.UnixNano(), n),
		"topic":     "/subscriptions/x/resourceGroups/x/providers/Microsoft.Storage/storageAccounts/loadgen",
		"subject":   "/blobServices/default/containers/" + ev.Container + "/blobs/" + ev.Name,
		"eventType": "Microsoft.Storage.BlobCreated",
		"eventTime": ev.At.UTC().Format(time.RFC3339Nano),
		"data": map[string]any{
			"api": "PutBlockList", "blobType": "BlockBlob", "url": ev.BlobURL,
			"eTag": ev.ETag, "contentLength": ev.Size,
			// Event Grid sequencers are fixed-width hex that sort in write order.
			"sequencer": fmt.Sprintf("%032x", ev.At.UnixNano()),
		},
		"dataVersion": "", "metadataVersion": "1",
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// blobURL joins a container URL and a blob name the way the service reports
// it (path-escaped name).
func blobURL(containerURL, name string) string {
	return strings.TrimRight(containerURL, "/") + "/" + (&url.URL{Path: name}).EscapedPath()
}

// queueSink enqueues to an Azure Storage Queue.
type queueSink struct {
	q *azqueue.QueueClient
}

func (s *queueSink) BlobCreated(ctx context.Context, ev blobCreated) error {
	msg, err := eventGridMessage(ev)
	if err != nil {
		return err
	}
	if _, err := s.q.EnqueueMessage(ctx, msg, nil); err != nil {
		return fmt.Errorf("enqueue event: %w", err)
	}
	return nil
}

// newQueueSink builds a sink for queueName from the connection string (its
// QueueEndpoint) or, failing that, queueAccountURL with
// DefaultAzureCredential. create makes the queue if missing.
func newQueueSink(ctx context.Context, connStr, queueAccountURL, queueName string, create bool) (*queueSink, error) {
	var (
		svc *azqueue.ServiceClient
		err error
	)
	switch {
	case connStr != "":
		svc, err = azqueue.NewServiceClientFromConnectionString(connStr, nil)
	case queueAccountURL != "":
		cred, cerr := azidentity.NewDefaultAzureCredential(nil)
		if cerr != nil {
			return nil, fmt.Errorf("default azure credential: %w", cerr)
		}
		svc, err = azqueue.NewServiceClient(queueAccountURL, cred, nil)
	default:
		return nil, errors.New("-event-queue needs -connection-string (with a QueueEndpoint) or -queue-account-url")
	}
	if err != nil {
		return nil, fmt.Errorf("queue client: %w", err)
	}
	q := svc.NewQueueClient(queueName)
	if create {
		if _, err := q.Create(ctx, nil); err != nil && !queueerror.HasCode(err, queueerror.QueueAlreadyExists) {
			return nil, fmt.Errorf("create queue: %w", err)
		}
	}
	return &queueSink{q: q}, nil
}
