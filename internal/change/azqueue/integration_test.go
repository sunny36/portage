//go:build integration

package azqueue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/testenv"
)

// newQueue creates a fresh Azurite queue (and cleans up it and its poison
// queue).
func newQueue(t *testing.T) (string, *azqueue.ServiceClient) {
	t.Helper()
	svc, err := azqueue.NewServiceClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	name := testenv.UniqueName(t, "portage-q")
	if _, err := svc.CreateQueue(context.Background(), name, nil); err != nil {
		t.Fatalf("create queue (is Azurite up at %s?): %v", testenv.AzuriteQueueURL(), err)
	}
	t.Cleanup(func() {
		_, _ = svc.DeleteQueue(context.Background(), name, nil)
		_, _ = svc.DeleteQueue(context.Background(), name+poisonSuffix, nil)
	})
	return name, svc
}

func newIntegrationSource(t *testing.T, queueName, auth string, visibility time.Duration) *Source {
	t.Helper()
	o := Options{
		PipelineID:        "p1",
		QueueName:         queueName,
		Container:         "src",
		Prefix:            "in/",
		VisibilityTimeout: visibility,
		MaxEmptyBackoff:   50 * time.Millisecond,
		MaxDequeue:        2,
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	switch auth {
	case "connection_string":
		o.Auth = config.AzureConfig{Auth: auth, ConnectionString: testenv.AzuriteConnectionString()}
	case "shared_key":
		o.QueueAccountURL = testenv.AzuriteQueueURL()
		o.Auth = config.AzureConfig{Auth: auth, AccountName: testenv.AzuriteAccountName, AccountKey: testenv.AzuriteAccountKey}
	}
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func enqueue(t *testing.T, svc *azqueue.ServiceClient, queue, text string) {
	t.Helper()
	if _, err := svc.NewQueueClient(queue).EnqueueMessage(context.Background(), text, nil); err != nil {
		t.Fatal(err)
	}
}

// visible counts messages currently visible (Peek doesn't change visibility).
func visible(t *testing.T, svc *azqueue.ServiceClient, queue string) []string {
	t.Helper()
	resp, err := svc.NewQueueClient(queue).PeekMessages(context.Background(), &azqueue.PeekMessagesOptions{NumberOfMessages: new(int32(32))})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range resp.Messages {
		out = append(out, *m.MessageText)
	}
	return out
}

// approxCount is the queue's message count including invisible ones.
func approxCount(t *testing.T, svc *azqueue.ServiceClient, queue string) int32 {
	t.Helper()
	p, err := svc.NewQueueClient(queue).GetProperties(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return *p.ApproximateMessagesCount
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestIntegrationEmitThenDelete(t *testing.T) {
	for _, auth := range []string{"connection_string", "shared_key"} {
		t.Run(auth, func(t *testing.T) {
			q, svc := newQueue(t)
			// Path-style Azurite URLs, as the blob URL would look in a
			// synthetic event from the emulator.
			blob := testenv.AzuriteBlobURL() + "/src/in/"
			enqueue(t, svc, q, b64(event("Microsoft.Storage.BlobCreated", blob+"a%20b.txt")))
			enqueue(t, svc, q, event("Microsoft.Storage.BlobDeleted", blob+"dir/%E6%97%A5.bin"))
			enqueue(t, svc, q, b64(event("Microsoft.Storage.BlobTierChanged", blob+"c")))

			s := newIntegrationSource(t, q, auth, time.Minute)
			out := &sink{}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx, out.emit) }()
			waitFor(t, "queue drained", func() bool { return approxCount(t, svc, q) == 0 })
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run: %v", err)
			}
			got := out.changes()
			if len(got) != 2 {
				t.Fatalf("got %+v", got)
			}
			if got[0].Kind != change.KindUpsert || got[0].Key != "a b.txt" || got[0].Version != "0x8DE" || got[0].Size != 42 {
				t.Errorf("change 0: %+v", got[0])
			}
			if got[1].Kind != change.KindDelete || got[1].Key != "dir/日.bin" {
				t.Errorf("change 1: %+v", got[1])
			}
		})
	}
}

func TestIntegrationEmitFailureLeavesMessage(t *testing.T) {
	q, svc := newQueue(t)
	body := b64(event("Microsoft.Storage.BlobCreated", testenv.AzuriteBlobURL()+"/src/in/a"))
	enqueue(t, svc, q, body)

	// Short visibility timeout so the message comes back quickly.
	s := newIntegrationSource(t, q, "connection_string", time.Second)
	out := &sink{fail: true}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.Run(ctx, out.emit)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	if n := approxCount(t, svc, q); n != 1 {
		t.Fatalf("message count %d, want 1 (not deleted after failed emit)", n)
	}
	waitFor(t, "message visible again", func() bool { return len(visible(t, svc, q)) == 1 })

	// Now emit succeeds: the redelivered message is processed and deleted.
	out.setFail(false)
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, out.emit) }()
	waitFor(t, "queue drained", func() bool { return approxCount(t, svc, q) == 0 })
	cancel()
	<-done
	if len(out.changes()) != 1 {
		t.Fatalf("got %+v", out.changes())
	}
}

func TestIntegrationPoison(t *testing.T) {
	q, svc := newQueue(t)
	enqueue(t, svc, q, "garbage that is not an event")
	stuck := b64(event("Microsoft.Storage.BlobCreated", testenv.AzuriteBlobURL()+"/src/in/stuck"))
	enqueue(t, svc, q, stuck)

	// Emit always fails, so "stuck" is retried until it exceeds MaxDequeue (2).
	s := newIntegrationSource(t, q, "connection_string", time.Second)
	out := &sink{fail: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, out.emit) }()
	waitFor(t, "queue drained", func() bool { return approxCount(t, svc, q) == 0 })
	cancel()
	<-done

	got := visible(t, svc, q+poisonSuffix)
	if len(got) != 2 {
		t.Fatalf("poison queue has %q", got)
	}
	want := map[string]bool{"garbage that is not an event": true, stuck: true}
	for _, m := range got {
		if !want[m] {
			t.Errorf("unexpected poison message %q", m)
		}
	}
}

func TestIntegrationMissingQueueIsFatal(t *testing.T) {
	s := newIntegrationSource(t, testenv.UniqueName(t, "portage-missing"), "connection_string", time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.Run(ctx, (&sink{}).emit)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want a fatal QueueNotFound error", err)
	}
}

func TestIntegrationBadKeyIsFatal(t *testing.T) {
	q, _ := newQueue(t)
	s, err := New(Options{
		PipelineID: "p1", QueueName: q, Container: "src",
		QueueAccountURL: testenv.AzuriteQueueURL(),
		Auth:            config.AzureConfig{Auth: "shared_key", AccountName: testenv.AzuriteAccountName, AccountKey: "d3Jvbmc="},
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Run(ctx, (&sink{}).emit); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want a fatal auth error", err)
	}
}
