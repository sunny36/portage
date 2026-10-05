package main

import (
	"testing"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/change/eventgrid"
)

// The synthetic event must be exactly what the engine's azure_queue source
// accepts, so parse it with the engine's own Event Grid code.
func TestEventGridMessageIsAcceptedByEngine(t *testing.T) {
	at := time.Date(2026, 10, 5, 1, 2, 3, 456_000_000, time.UTC)
	name := "soak/2026/10/05/01/x y+ü.bin"
	msg, err := eventGridMessage(blobCreated{
		BlobURL:   blobURL("http://127.0.0.1:10000/devstoreaccount1/src/", name),
		Container: "src",
		Name:      name,
		ETag:      `"0x8DCABC"`,
		Size:      1234,
		At:        at,
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := eventgrid.ParseMessage([]byte(msg))
	if err != nil || len(events) != 1 {
		t.Fatalf("ParseMessage = %v, %v", events, err)
	}
	c, ok, skip, err := eventgrid.Target{PipelineID: "p", Container: "src", Prefix: "soak/"}.Map(events[0], at.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("Map: ok=%v skip=%q err=%v", ok, skip, err)
	}
	if c.Kind != change.KindUpsert || c.Key != "2026/10/05/01/x y+ü.bin" || c.Version != "0x8DCABC" ||
		c.Size != 1234 || !c.EventTime.Equal(at) || c.Origin != change.OriginEvent || c.Sequencer == "" {
		t.Errorf("change = %+v", c)
	}

	// Two events get distinct IDs and ordered sequencers.
	m2, _ := eventGridMessage(blobCreated{BlobURL: "http://h/a/src/k", Container: "src", Name: "k", At: at.Add(time.Millisecond)})
	e2, err := eventgrid.ParseMessage([]byte(m2))
	if err != nil {
		t.Fatal(err)
	}
	if e2[0].ID == events[0].ID {
		t.Error("event IDs repeat")
	}
}

func TestContainerName(t *testing.T) {
	for in, want := range map[string]string{
		"http://127.0.0.1:10000/devstoreaccount1/src":     "src",
		"https://acct.blob.core.windows.net/exports/":     "exports",
		"https://acct.blob.core.windows.net/exports?sv=1": "exports",
	} {
		if got := containerName(in); got != want {
			t.Errorf("containerName(%q) = %q, want %q", in, got, want)
		}
	}
}
