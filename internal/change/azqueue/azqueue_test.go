package azqueue

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
)

// fakeQueue is an in-memory Storage Queue: dequeued messages are invisible
// until release() (standing in for the visibility timeout).
type fakeQueue struct {
	mu         sync.Mutex
	msgs       []*fakeMsg
	nextID     int
	created    bool
	dequeueErr []error // returned (and popped) before serving messages
	enqueued   []string
}

type fakeMsg struct {
	id, text, pop string
	count         int64
	hidden        bool
}

func (q *fakeQueue) add(text string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	q.msgs = append(q.msgs, &fakeMsg{id: strconv.Itoa(q.nextID), text: text})
}

func (q *fakeQueue) release() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, m := range q.msgs {
		m.hidden = false
	}
}

func (q *fakeQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.msgs)
}

func (q *fakeQueue) Create(context.Context, *azqueue.CreateOptions) (azqueue.CreateResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.created = true
	return azqueue.CreateResponse{}, nil
}

func (q *fakeQueue) DequeueMessages(_ context.Context, o *azqueue.DequeueMessagesOptions) (azqueue.DequeueMessagesResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.dequeueErr) > 0 {
		err := q.dequeueErr[0]
		q.dequeueErr = q.dequeueErr[1:]
		return azqueue.DequeueMessagesResponse{}, err
	}
	var resp azqueue.DequeueMessagesResponse
	for _, m := range q.msgs {
		if m.hidden || len(resp.Messages) >= int(*o.NumberOfMessages) {
			continue
		}
		m.hidden = true
		m.count++
		m.pop = m.id + "-" + strconv.FormatInt(m.count, 10)
		resp.Messages = append(resp.Messages, &azqueue.DequeuedMessage{
			MessageID: to.Ptr(m.id), PopReceipt: to.Ptr(m.pop),
			MessageText: to.Ptr(m.text), DequeueCount: to.Ptr(m.count),
		})
	}
	return resp, nil
}

func (q *fakeQueue) DeleteMessage(_ context.Context, id, pop string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, m := range q.msgs {
		if m.id == id && m.pop == pop {
			q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
			return azqueue.DeleteMessageResponse{}, nil
		}
	}
	return azqueue.DeleteMessageResponse{}, &azcore.ResponseError{StatusCode: 404, ErrorCode: "MessageNotFound"}
}

func (q *fakeQueue) EnqueueMessage(_ context.Context, text string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.created {
		return azqueue.EnqueueMessagesResponse{}, &azcore.ResponseError{StatusCode: 404, ErrorCode: "QueueNotFound"}
	}
	q.enqueued = append(q.enqueued, text)
	return azqueue.EnqueueMessagesResponse{}, nil
}

type sink struct {
	mu   sync.Mutex
	got  []change.ObjectChanged
	fail bool
}

func (s *sink) emit(_ context.Context, c change.ObjectChanged) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("enqueue failed")
	}
	s.got = append(s.got, c)
	return nil
}

func (s *sink) setFail(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = v
}

func (s *sink) changes() []change.ObjectChanged {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]change.ObjectChanged(nil), s.got...)
}

func event(typ, url string) string {
	return `{"topic":"t","subject":"s","eventType":"` + typ + `","eventTime":"2026-10-05T10:00:00Z","id":"e","data":{"url":"` + url +
		`","eTag":"0x8DE","contentLength":42,"sequencer":"00000001"},"dataVersion":"","metadataVersion":"1"}`
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func newTestSource(t *testing.T, filters config.Filters) (*Source, *fakeQueue, *fakeQueue) {
	t.Helper()
	f, err := change.NewFilter(filters)
	if err != nil {
		t.Fatal(err)
	}
	q, poison := &fakeQueue{}, &fakeQueue{}
	s := newSource(Options{
		PipelineID:      "p1",
		QueueName:       "events",
		Container:       "src",
		Prefix:          "in/",
		Filter:          f,
		MaxEmptyBackoff: 5 * time.Millisecond,
		MaxErrorBackoff: 5 * time.Millisecond,
		MaxDequeue:      2,
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, q, poison)
	return s, q, poison
}

// runUntil runs s until cond holds (or fails after 5s), then cancels.
func runUntil(t *testing.T, s *Source, emit change.Emit, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, emit) }()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("Run returned early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("timed out")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestEmitsAndDeletes(t *testing.T) {
	s, q, _ := newTestSource(t, config.Filters{Exclude: []string{"*.tmp"}})
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/in/a.txt")))
	q.add(event("Microsoft.Storage.BlobDeleted", "https://a.blob.core.windows.net/src/in/dir/b%20c.txt"))
	q.add(b64(event("Microsoft.Storage.BlobTierChanged", "https://a.blob.core.windows.net/src/in/c")))
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/other/in/d")))
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/out/e")))
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/in/f.tmp")))
	out := &sink{}
	runUntil(t, s, out.emit, func() bool { return q.len() == 0 })

	got := out.changes()
	if len(got) != 2 {
		t.Fatalf("got %d changes: %+v", len(got), got)
	}
	if got[0].Kind != change.KindUpsert || got[0].Key != "a.txt" || got[0].Version != "0x8DE" ||
		got[0].Size != 42 || got[0].Sequencer != "00000001" || got[0].Origin != change.OriginEvent || got[0].PipelineID != "p1" {
		t.Errorf("change 0: %+v", got[0])
	}
	if got[1].Kind != change.KindDelete || got[1].Key != "dir/b c.txt" {
		t.Errorf("change 1: %+v", got[1])
	}
}

func TestEmitFailureKeepsMessage(t *testing.T) {
	s, q, poison := newTestSource(t, config.Filters{})
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/in/a")))
	out := &sink{fail: true}
	runUntil(t, s, out.emit, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.msgs[0].count == 1
	})
	if q.len() != 1 || len(poison.enqueued) != 0 {
		t.Fatalf("message must stay in the queue: len=%d poison=%d", q.len(), len(poison.enqueued))
	}
	// After the visibility timeout it is redelivered and succeeds.
	q.release()
	out.setFail(false)
	runUntil(t, s, out.emit, func() bool { return q.len() == 0 })
	if len(out.changes()) != 1 {
		t.Fatalf("got %+v", out.changes())
	}
}

func TestPoisonUnparseable(t *testing.T) {
	s, q, poison := newTestSource(t, config.Filters{})
	q.add("this is not an event")
	q.add(b64(`{"eventType":"Microsoft.Storage.BlobCreated","id":"x","data":{"eTag":"e"}}`)) // no url
	runUntil(t, s, (&sink{}).emit, func() bool { return q.len() == 0 })
	if !poison.created || len(poison.enqueued) != 2 || poison.enqueued[0] != "this is not an event" {
		t.Fatalf("poison: created=%v enqueued=%q", poison.created, poison.enqueued)
	}
}

func TestPoisonAfterMaxDequeue(t *testing.T) {
	s, q, poison := newTestSource(t, config.Filters{})
	body := b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/in/a"))
	q.add(body)
	out := &sink{fail: true}
	// MaxDequeue is 2: attempts 1 and 2 fail, the 3rd dequeue poisons it.
	for i := 0; i < 3 && q.len() > 0; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = s.Run(ctx, out.emit)
		cancel()
		q.release()
	}
	if q.len() != 0 || len(poison.enqueued) != 1 || poison.enqueued[0] != body {
		t.Fatalf("queue len %d, poison %q", q.len(), poison.enqueued)
	}
}

func TestTransientErrorBacksOffThenContinues(t *testing.T) {
	s, q, _ := newTestSource(t, config.Filters{})
	q.dequeueErr = []error{
		&azcore.ResponseError{StatusCode: 503, ErrorCode: "ServerBusy"},
		errors.New("connection reset"),
	}
	q.add(b64(event("Microsoft.Storage.BlobCreated", "https://a.blob.core.windows.net/src/in/a")))
	out := &sink{}
	runUntil(t, s, out.emit, func() bool { return q.len() == 0 })
	if len(out.changes()) != 1 {
		t.Fatal("expected one change")
	}
}

func TestFatalErrors(t *testing.T) {
	for _, err := range []error{
		&azcore.ResponseError{StatusCode: 403, ErrorCode: "AuthorizationPermissionMismatch"},
		&azcore.ResponseError{StatusCode: 403, ErrorCode: "AuthenticationFailed"},
		&azcore.ResponseError{StatusCode: 404, ErrorCode: "QueueNotFound"},
	} {
		s, q, _ := newTestSource(t, config.Filters{})
		q.dequeueErr = []error{err}
		got := s.Run(context.Background(), (&sink{}).emit)
		if got == nil || !errors.Is(got, err) {
			t.Errorf("Run = %v, want wrapping %v", got, err)
		}
	}
}

func TestNewValidation(t *testing.T) {
	base := Options{QueueName: "q", Container: "c", Auth: config.AzureConfig{Auth: "shared_key", AccountName: "a", AccountKey: "a2V5"}, QueueAccountURL: "http://127.0.0.1:1/a"}
	if _, err := New(base); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	bad := []Options{
		{Container: "c"},
		{QueueName: "q"},
		{QueueName: "q", Container: "c", Auth: config.AzureConfig{Auth: "nope"}},
		{QueueName: "q", Container: "c", Auth: config.AzureConfig{Auth: "shared_key"}},
		{QueueName: "q", Container: "c", Auth: config.AzureConfig{Auth: "connection_string"}},
		{QueueName: "q", Container: "c"}, // default auth needs a URL
		{QueueName: "q123456789012345678901234567890123456789012345678901234567", Container: "c",
			Auth: config.AzureConfig{Auth: "connection_string", ConnectionString: "x"}},
	}
	for i, o := range bad {
		if _, err := New(o); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
}
