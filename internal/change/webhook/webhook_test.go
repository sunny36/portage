package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/change"
)

const secret = "0123456789abcdef-secret"

type recorder struct {
	mu      sync.Mutex
	got     []change.ObjectChanged
	failKey string
}

func (r *recorder) emit(_ context.Context, c change.ObjectChanged) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.Key == r.failKey {
		return errors.New("queue down")
	}
	r.got = append(r.got, c)
	return nil
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newServer(t *testing.T, rec *recorder) *httptest.Server {
	t.Helper()
	h := NewHandler(HandlerOptions{
		PipelineID: "p1",
		Secret:     secret,
		Container:  "src",
		Prefix:     "in/",
		Emit:       rec.emit,
		Log:        quietLog(),
		Now:        func() time.Time { return time.Unix(1000, 0) },
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, body string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func egEvent(typ, name string) string {
	return `{"id":"` + name + `","subject":"s","eventType":"` + typ + `","eventTime":"2026-10-05T10:00:00.1234567Z","data":{"url":"https://acct.blob.core.windows.net/src/` + name + `","eTag":"0xE","contentLength":7,"sequencer":"0001"},"dataVersion":"","metadataVersion":"1"}`
}

func ceEvent(typ, name string) string {
	return `{"specversion":"1.0","id":"` + name + `","source":"/x","subject":"s","type":"` + typ + `","time":"2026-10-05T10:00:00Z","data":{"url":"https://acct.blob.core.windows.net/src/` + name + `","eTag":"0xE","contentLength":7,"sequencer":"0001"}}`
}

func TestAuth(t *testing.T) {
	rec := &recorder{}
	srv := newServer(t, rec)
	body := "[" + egEvent("Microsoft.Storage.BlobCreated", "in/a") + "]"
	for _, u := range []string{srv.URL, srv.URL + "?key=wrong", srv.URL + "?key=" + secret + "x"} {
		if resp := do(t, http.MethodPost, u, body, nil); resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", u, resp.StatusCode)
		}
		if resp := do(t, http.MethodOptions, u, "", map[string]string{"WebHook-Request-Origin": "eventgrid.azure.net"}); resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("OPTIONS %s: status %d, want 401", u, resp.StatusCode)
		}
	}
	if len(rec.got) != 0 {
		t.Fatalf("emitted %d changes for unauthorised requests", len(rec.got))
	}
}

func TestEmptySecretRejectsAll(t *testing.T) {
	srv := httptest.NewServer(NewHandler(HandlerOptions{Log: quietLog(), Emit: (&recorder{}).emit}))
	defer srv.Close()
	if resp := do(t, http.MethodPost, srv.URL+"?key=", "[]", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestSubscriptionValidation(t *testing.T) {
	srv := newServer(t, &recorder{})
	body := `[{"id":"2d1781af-3a4c-4d7c-bd0c-e34b19da4e66","topic":"/subscriptions/x","subject":"","data":{"validationCode":"512d38b6-c7b8-40c8-89fe-f46f9e9622b6","validationUrl":"https://rp-eastus2.eventgrid.azure.net:553/eventsubscriptions/x/validate?id=y"},"eventType":"Microsoft.EventGrid.SubscriptionValidationEvent","eventTime":"2018-01-25T22:12:19.4556811Z","metadataVersion":"1","dataVersion":"1"}]`
	resp := do(t, http.MethodPost, srv.URL+"/events/p1?key="+secret, body, map[string]string{"aeg-event-type": "SubscriptionValidation"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["validationResponse"] != "512d38b6-c7b8-40c8-89fe-f46f9e9622b6" {
		t.Fatalf("got %v", out)
	}
}

func TestCloudEventsAbuseProtection(t *testing.T) {
	srv := newServer(t, &recorder{})
	resp := do(t, http.MethodOptions, srv.URL+"?key="+secret, "", map[string]string{
		"WebHook-Request-Origin": "eventgrid.azure.net",
		"WebHook-Request-Rate":   "120",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WebHook-Allowed-Origin"); got != "eventgrid.azure.net" {
		t.Fatalf("WebHook-Allowed-Origin %q", got)
	}
	if resp.Header.Get("WebHook-Allowed-Rate") == "" {
		t.Fatal("missing WebHook-Allowed-Rate")
	}
}

func TestBatchBothSchemas(t *testing.T) {
	rec := &recorder{}
	srv := newServer(t, rec)
	u := srv.URL + "?key=" + secret

	eg := "[" + strings.Join([]string{
		egEvent("Microsoft.Storage.BlobCreated", "in/a"),
		egEvent("Microsoft.Storage.BlobDeleted", "in/b"),
		egEvent("Microsoft.Storage.BlobTierChanged", "in/c"),                 // ignored
		egEvent("Microsoft.Storage.BlobCreated", "out/d"),                    // outside prefix
		`{"id":"bad","eventType":"Microsoft.Storage.BlobCreated","data":{}}`, // malformed, dropped
	}, ",") + "]"
	if resp := do(t, http.MethodPost, u, eg, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("eg status %d", resp.StatusCode)
	}
	ceBatch := "[" + ceEvent("Microsoft.Storage.BlobCreated", "in/e") + "]"
	if resp := do(t, http.MethodPost, u, ceBatch, map[string]string{"Content-Type": "application/cloudevents-batch+json"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("ce batch status %d", resp.StatusCode)
	}
	ceSingle := ceEvent("Microsoft.Storage.BlobDeleted", "in/f")
	if resp := do(t, http.MethodPost, u, ceSingle, map[string]string{"Content-Type": "application/cloudevents+json"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("ce single status %d", resp.StatusCode)
	}

	want := []struct {
		key  string
		kind change.Kind
	}{{"a", change.KindUpsert}, {"b", change.KindDelete}, {"e", change.KindUpsert}, {"f", change.KindDelete}}
	if len(rec.got) != len(want) {
		t.Fatalf("got %d changes: %+v", len(rec.got), rec.got)
	}
	for i, w := range want {
		g := rec.got[i]
		if g.Key != w.key || g.Kind != w.kind || g.PipelineID != "p1" || g.Origin != change.OriginEvent {
			t.Errorf("change %d = %+v, want %s %s", i, g, w.kind, w.key)
		}
	}
	if rec.got[0].Version != "0xE" || rec.got[0].Size != 7 || rec.got[0].Sequencer != "0001" ||
		!rec.got[0].DetectedAt.Equal(time.Unix(1000, 0)) {
		t.Errorf("fields: %+v", rec.got[0])
	}
}

func TestEmitFailureIs503(t *testing.T) {
	rec := &recorder{failKey: "b"}
	srv := newServer(t, rec)
	body := "[" + egEvent("Microsoft.Storage.BlobCreated", "in/a") + "," + egEvent("Microsoft.Storage.BlobCreated", "in/b") + "]"
	if resp := do(t, http.MethodPost, srv.URL+"?key="+secret, body, nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
}

func TestBadRequests(t *testing.T) {
	srv := newServer(t, &recorder{})
	u := srv.URL + "?key=" + secret
	if resp := do(t, http.MethodPost, u, "not json", nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("garbage: status %d", resp.StatusCode)
	}
	big := "[" + strings.Repeat(" ", DefaultMaxBodyBytes) + "]"
	if resp := do(t, http.MethodPost, u, big, nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("big: status %d", resp.StatusCode)
	}
	if resp := do(t, http.MethodGet, u, "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET: status %d", resp.StatusCode)
	}
}

func TestSourceWrapper(t *testing.T) {
	s := NewSource(HandlerOptions{PipelineID: "p1", Secret: secret, Container: "src", Log: quietLog()})
	if s.Name() != "webhook" {
		t.Fatal(s.Name())
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	u := srv.URL + "?key=" + secret
	body := "[" + egEvent("Microsoft.Storage.BlobCreated", "k") + "]"

	if resp := do(t, http.MethodPost, u, body, nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("before Run: status %d, want 503", resp.StatusCode)
	}

	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, rec.emit) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.emit.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("Run did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if resp := do(t, http.MethodPost, u, body, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("during Run: status %d", resp.StatusCode)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if len(rec.got) != 1 || rec.got[0].Key != "k" {
		t.Fatalf("got %+v", rec.got)
	}
	if resp := do(t, http.MethodPost, u, body, nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("after Run: status %d, want 503", resp.StatusCode)
	}
}
