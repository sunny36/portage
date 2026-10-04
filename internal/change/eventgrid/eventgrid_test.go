package eventgrid

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
)

// Fixtures follow the samples in
// https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage
const egCreated = `{
  "topic": "/subscriptions/s/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
  "subject": "/blobServices/default/containers/testcontainer/blobs/data/new-file.txt",
  "eventType": "Microsoft.Storage.BlobCreated",
  "eventTime": "2017-06-26T18:41:00.9584103Z",
  "id": "831e1650-001e-001b-66ab-eeb76e069631",
  "data": {
    "api": "PutBlockList",
    "clientRequestId": "6d79dbfb-0e37-4fc4-981f-442c9ca65760",
    "requestId": "831e1650-001e-001b-66ab-eeb76e000000",
    "eTag": "0x8D4BCC2E4835CD0",
    "contentType": "text/plain",
    "contentLength": 524288,
    "blobType": "BlockBlob",
    "url": "https://my-storage-account.blob.core.windows.net/testcontainer/data/new-file.txt",
    "sequencer": "00000000000004420000000000028963",
    "storageDiagnostics": {"batchId": "b68529f3-68cd-4744-baa4-3c0498ec19f0"}
  },
  "dataVersion": "",
  "metadataVersion": "1"
}`

const ceDeleted = `{
  "source": "/subscriptions/s/resourceGroups/Storage/providers/Microsoft.Storage/storageAccounts/my-storage-account",
  "subject": "/blobServices/default/containers/testcontainer/blobs/data/file-to-delete.txt",
  "type": "Microsoft.Storage.BlobDeleted",
  "time": "2017-11-07T20:09:22.5674003Z",
  "id": "6b1e0e0a-a9b3-4b5c-8f57-1b4b0d1f0f00",
  "data": {
    "api": "DeleteBlob",
    "requestId": "4c2359fe-001e-00ba-0e04-58586806d298",
    "contentType": "text/plain",
    "blobType": "BlockBlob",
    "url": "https://my-storage-account.blob.core.windows.net/testcontainer/data/file-to-delete.txt",
    "sequencer": "0000000000000281000000000002F5CA",
    "storageDiagnostics": {"batchId": "b68529f3-68cd-4744-baa4-3c0498ec19f0"}
  },
  "specversion": "1.0"
}`

const egTierChanged = `{"topic":"t","subject":"/blobServices/default/containers/testcontainer/blobs/data/Auto.jpg","eventType":"Microsoft.Storage.BlobTierChanged","eventTime":"2021-05-04T15:00:00.8350154Z","id":"x","data":{"api":"SetBlobTier","url":"https://my-storage-account.blob.core.windows.net/testcontainer/data/Auto.jpg"},"dataVersion":"","metadataVersion":"1"}`

func target() Target {
	return Target{PipelineID: "p1", Container: "testcontainer", Prefix: "data/"}
}

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func mustOne(t *testing.T, body string) Event {
	t.Helper()
	evs, err := ParseMessage([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("got %d events", len(evs))
	}
	return evs[0]
}

func TestParseCreatedEventGridSchemaRawAndBase64(t *testing.T) {
	for name, body := range map[string]string{
		"raw":    egCreated,
		"base64": base64.StdEncoding.EncodeToString([]byte(egCreated)),
		"array":  "[" + egCreated + "]",
	} {
		t.Run(name, func(t *testing.T) {
			ev := mustOne(t, body)
			if ev.CloudEvents {
				t.Error("want Event Grid schema")
			}
			c, ok, skip, err := target().Map(ev, now)
			if err != nil || !ok {
				t.Fatalf("Map: ok=%v skip=%q err=%v", ok, skip, err)
			}
			want := change.ObjectChanged{
				PipelineID: "p1",
				Kind:       change.KindUpsert,
				Key:        "new-file.txt",
				Version:    "0x8D4BCC2E4835CD0",
				Size:       524288,
				Sequencer:  "00000000000004420000000000028963",
				EventTime:  time.Date(2017, 6, 26, 18, 41, 0, 958410300, time.UTC),
				DetectedAt: now,
				Origin:     change.OriginEvent,
			}
			if c != want {
				t.Fatalf("got  %+v\nwant %+v", c, want)
			}
		})
	}
}

func TestParseDeletedCloudEventsBase64(t *testing.T) {
	ev := mustOne(t, base64.StdEncoding.EncodeToString([]byte(ceDeleted)))
	if !ev.CloudEvents {
		t.Error("want CloudEvents schema")
	}
	c, ok, _, err := target().Map(ev, now)
	if err != nil || !ok {
		t.Fatalf("Map: ok=%v err=%v", ok, err)
	}
	if c.Kind != change.KindDelete || c.Key != "file-to-delete.txt" || c.Version != "" ||
		c.Sequencer != "0000000000000281000000000002F5CA" ||
		!c.EventTime.Equal(time.Date(2017, 11, 7, 20, 9, 22, 567400300, time.UTC)) {
		t.Fatalf("got %+v", c)
	}
}

func TestMapSkips(t *testing.T) {
	tests := []struct {
		name string
		tgt  Target
		body string
		skip string
	}{
		{"tier changed", target(), egTierChanged, SkipType},
		{"directory created", target(), `{"eventType":"Microsoft.Storage.DirectoryCreated","id":"d","data":{"url":"https://a.dfs.core.windows.net/testcontainer/data/dir"}}`, SkipType},
		{"other container", Target{Container: "other", Prefix: "data/"}, egCreated, SkipContainer},
		{"outside prefix", Target{Container: "testcontainer", Prefix: "elsewhere/"}, egCreated, SkipPrefix},
		{"prefix is not a folder match", Target{Container: "testcontainer", Prefix: "dat/"}, egCreated, SkipPrefix},
		{"filtered", func() Target {
			tg := target()
			f, err := change.NewFilter(config.Filters{Exclude: []string{"*.txt"}})
			if err != nil {
				t.Fatal(err)
			}
			tg.Filter = f
			return tg
		}(), egCreated, SkipFilter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok, skip, err := tt.tgt.Map(mustOne(t, tt.body), now)
			if err != nil || ok || skip != tt.skip {
				t.Fatalf("ok=%v skip=%q err=%v, want skip %q", ok, skip, err, tt.skip)
			}
		})
	}
}

func TestMapNoPrefix(t *testing.T) {
	c, ok, _, err := Target{Container: "testcontainer"}.Map(mustOne(t, egCreated), now)
	if err != nil || !ok || c.Key != "data/new-file.txt" {
		t.Fatalf("c=%+v ok=%v err=%v", c, ok, err)
	}
}

func TestMapURLDecoding(t *testing.T) {
	tests := []struct{ url, container, key string }{
		{"https://a.blob.core.windows.net/c/dir/my%20file.txt", "c", "dir/my file.txt"},
		{"https://a.blob.core.windows.net/c/%E6%97%A5%E6%9C%AC/%C3%A9t%C3%A9.csv", "c", "日本/été.csv"},
		{"https://a.blob.core.windows.net/c/a+b/%2Bplus%3F%23.txt", "c", "a+b/+plus?#.txt"},
		{"https://a.blob.core.windows.net/c/raw space.txt", "c", "raw space.txt"},
		{"https://a.dfs.core.windows.net/fs/x/y.parquet", "fs", "x/y.parquet"},
		// Path-style (Azurite, IP endpoints): first segment is the account.
		{"http://127.0.0.1:10000/devstoreaccount1/c/k%20ey", "c", "k ey"},
		{"http://localhost:10000/devstoreaccount1/c/k", "c", "k"},
		{"http://azurite:10000/devstoreaccount1/c/k", "c", "k"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			body := `{"eventType":"Microsoft.Storage.BlobCreated","eventTime":"2026-01-01T00:00:00Z","id":"i","data":{"url":` +
				jsonString(tt.url) + `,"eTag":"\"0x1\"","contentLength":3}}`
			c, ok, skip, err := Target{Container: tt.container}.Map(mustOne(t, body), now)
			if err != nil || !ok {
				t.Fatalf("ok=%v skip=%q err=%v", ok, skip, err)
			}
			if c.Key != tt.key {
				t.Fatalf("key %q, want %q", c.Key, tt.key)
			}
			if c.Version != "0x1" {
				t.Fatalf("version %q: quotes not stripped", c.Version)
			}
		})
	}
}

func jsonString(s string) string {
	return `"` + s + `"`
}

func TestMalformed(t *testing.T) {
	for name, body := range map[string]string{
		"empty":         "",
		"not base64":    "!!!not-base64!!!",
		"base64 junk":   base64.StdEncoding.EncodeToString([]byte("hello")),
		"bad json":      `{"eventType":`,
		"no type":       `{"id":"x","data":{}}`,
		"bad time":      `{"eventType":"Microsoft.Storage.BlobCreated","eventTime":"yesterday"}`,
		"array garbage": `[1,2]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseMessage([]byte(body)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
	// Parseable envelope, but a blob event without a usable url.
	for name, body := range map[string]string{
		"no data":      `{"eventType":"Microsoft.Storage.BlobCreated","id":"x"}`,
		"no url":       `{"eventType":"Microsoft.Storage.BlobCreated","id":"x","data":{"eTag":"e"}}`,
		"no blob name": `{"eventType":"Microsoft.Storage.BlobDeleted","id":"x","data":{"url":"https://a.blob.core.windows.net/c"}}`,
		"data string":  `{"eventType":"Microsoft.Storage.BlobDeleted","id":"x","data":"nope"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := target().Map(mustOne(t, body), now)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestValidationCode(t *testing.T) {
	body := `[{"id":"2d1781af","topic":"/subscriptions/x","subject":"","data":{"validationCode":"512d38b6-c7b8-40c8-89fe-f46f9e9622b6","validationUrl":"https://rp-eastus2.eventgrid.azure.net/x"},"eventType":"Microsoft.EventGrid.SubscriptionValidationEvent","eventTime":"2018-01-25T22:12:19.4556811Z","metadataVersion":"1","dataVersion":"1"}]`
	ev := mustOne(t, body)
	if got := ValidationCode(ev); got != "512d38b6-c7b8-40c8-89fe-f46f9e9622b6" {
		t.Fatalf("code %q", got)
	}
	if ValidationCode(mustOne(t, egCreated)) != "" {
		t.Fatal("blob event is not a validation event")
	}
}
