// Package eventgrid parses Azure Event Grid blob-storage events and maps them
// to change.ObjectChanged. It is shared by the azqueue (Storage Queue) and
// webhook (HTTPS) change sources.
//
// Both delivery schemas are accepted:
//
//   - Event Grid schema: {"id", "topic", "subject", "eventType", "eventTime",
//     "data", "dataVersion", "metadataVersion"}.
//   - CloudEvents 1.0: {"specversion": "1.0", "id", "source", "subject",
//     "type", "time", "data"}.
//
// The blob fields used from "data" are the same in both schemas: url, eTag,
// contentLength, sequencer. Reference:
// https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage
package eventgrid

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/sunny36/portage/internal/change"
)

// Event types Portage acts on.
const (
	TypeBlobCreated          = "Microsoft.Storage.BlobCreated"
	TypeBlobDeleted          = "Microsoft.Storage.BlobDeleted"
	TypeSubscriptionValidate = "Microsoft.EventGrid.SubscriptionValidationEvent"
)

// Event is one Event Grid event in either schema, normalised.
type Event struct {
	ID      string
	Type    string // eventType (Event Grid) or type (CloudEvents)
	Subject string
	Time    time.Time // eventTime or time; zero if absent
	// CloudEvents is true when the event used the CloudEvents 1.0 schema.
	CloudEvents bool
	Data        json.RawMessage
}

// wire covers the fields of both schemas.
type wire struct {
	ID          string          `json:"id"`
	Subject     string          `json:"subject"`
	EventType   string          `json:"eventType"`
	EventTime   string          `json:"eventTime"`
	Type        string          `json:"type"`
	Time        string          `json:"time"`
	SpecVersion string          `json:"specversion"`
	Data        json.RawMessage `json:"data"`
}

// BlobData is the subset of a storage event's data Portage uses.
type BlobData struct {
	API           string `json:"api"`
	URL           string `json:"url"`
	ETag          string `json:"eTag"`
	ContentLength int64  `json:"contentLength"`
	Sequencer     string `json:"sequencer"`
}

// ErrMalformed wraps every parse failure, so callers can route the message to
// a poison queue / 400 instead of retrying it.
var ErrMalformed = errors.New("eventgrid: malformed event")

func malformed(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, a...))
}

// ParseMessage parses a Storage Queue message body. Event Grid's Storage
// Queue handler writes each event base64-encoded (Microsoft's own samples
// read these queues with TextBase64DecodePolicy), but raw JSON is also
// accepted so queues fed by other tools or Azurite tests work. The decoded
// body may be a single event object or an array of events.
func ParseMessage(body []byte) ([]Event, error) {
	b := bytes.TrimSpace(body)
	if len(b) == 0 {
		return nil, malformed("empty message")
	}
	if b[0] != '{' && b[0] != '[' {
		dec, err := decodeBase64(string(b))
		if err != nil {
			return nil, malformed("body is neither JSON nor base64: %v", err)
		}
		b = bytes.TrimSpace(dec)
	}
	return ParseJSON(b)
}

func decodeBase64(s string) ([]byte, error) {
	var firstErr error
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		out, err := enc.DecodeString(s)
		if err == nil {
			return out, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return nil, firstErr
}

// ParseJSON parses a JSON body holding one event object or an array of
// events (an Event Grid webhook delivery is always an array; CloudEvents
// may be a single event or a batch).
func ParseJSON(b []byte) ([]Event, error) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return nil, malformed("empty body")
	}
	var ws []wire
	switch b[0] {
	case '[':
		if err := json.Unmarshal(b, &ws); err != nil {
			return nil, malformed("%v", err)
		}
	case '{':
		var w wire
		if err := json.Unmarshal(b, &w); err != nil {
			return nil, malformed("%v", err)
		}
		ws = []wire{w}
	default:
		return nil, malformed("not a JSON object or array")
	}
	out := make([]Event, 0, len(ws))
	for i, w := range ws {
		ev, err := w.event()
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
		out = append(out, ev)
	}
	return out, nil
}

func (w wire) event() (Event, error) {
	ev := Event{ID: w.ID, Subject: w.Subject, Data: w.Data}
	ts := w.EventTime
	switch {
	case w.SpecVersion != "":
		ev.CloudEvents = true
		ev.Type, ts = w.Type, w.Time
	case w.EventType != "":
		ev.Type = w.EventType
	default:
		ev.Type = w.Type
		if ts == "" {
			ts = w.Time
		}
	}
	if ev.Type == "" {
		return Event{}, malformed("no event type")
	}
	if ts != "" {
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return Event{}, malformed("event time %q: %v", ts, err)
		}
		ev.Time = t
	}
	return ev, nil
}

// ValidationCode returns the code of a SubscriptionValidationEvent, or "" if
// ev is not one.
func ValidationCode(ev Event) string {
	if ev.Type != TypeSubscriptionValidate {
		return ""
	}
	var d struct {
		ValidationCode string `json:"validationCode"`
	}
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return ""
	}
	return d.ValidationCode
}

// Target scopes events to one pipeline's source.
type Target struct {
	PipelineID string
	// Container is the source container; events for other containers are
	// skipped.
	Container string
	// Prefix is the pipeline's source prefix ("" or ending in "/"). Blobs
	// outside it are skipped; Key is the blob name with Prefix stripped.
	Prefix string
	Filter *change.Filter
}

// Skip reasons returned by Map when an event is not for this pipeline.
const (
	SkipType      = "ignored event type"
	SkipContainer = "other container"
	SkipPrefix    = "outside source prefix"
	SkipFilter    = "excluded by filter"
)

// Map converts a blob event into a change. When the event does not apply to
// t it returns ok=false and a skip reason (one of the Skip* constants). An
// error (wrapping ErrMalformed) means a blob event Portage should act on
// lacks the fields to do so.
func (t Target) Map(ev Event, now time.Time) (c change.ObjectChanged, ok bool, skip string, err error) {
	var kind change.Kind
	switch ev.Type {
	case TypeBlobCreated:
		kind = change.KindUpsert
	case TypeBlobDeleted:
		kind = change.KindDelete
	default:
		return change.ObjectChanged{}, false, SkipType, nil
	}
	var d BlobData
	if len(ev.Data) == 0 || bytes.Equal(ev.Data, []byte("null")) {
		return change.ObjectChanged{}, false, "", malformed("%s %s: no data", ev.Type, ev.ID)
	}
	if err := json.Unmarshal(ev.Data, &d); err != nil {
		return change.ObjectChanged{}, false, "", malformed("%s %s: data: %v", ev.Type, ev.ID, err)
	}
	container, name, err := blobFromURL(d.URL)
	if err != nil {
		return change.ObjectChanged{}, false, "", fmt.Errorf("%s %s: %w", ev.Type, ev.ID, err)
	}
	if container != t.Container {
		return change.ObjectChanged{}, false, SkipContainer, nil
	}
	if !strings.HasPrefix(name, t.Prefix) || len(name) == len(t.Prefix) {
		return change.ObjectChanged{}, false, SkipPrefix, nil
	}
	key := name[len(t.Prefix):]
	if !t.Filter.Match(key) {
		return change.ObjectChanged{}, false, SkipFilter, nil
	}
	c = change.ObjectChanged{
		PipelineID: t.PipelineID,
		Kind:       kind,
		Key:        key,
		Sequencer:  d.Sequencer,
		EventTime:  ev.Time,
		DetectedAt: now,
		Origin:     change.OriginEvent,
	}
	if kind == change.KindUpsert {
		c.Version = strings.Trim(d.ETag, `"`)
		c.Size = d.ContentLength
	}
	return c, true, "", nil
}

// blobFromURL splits a blob (or ADLS Gen2 dfs) URL into container and
// URL-decoded blob name. Azure URLs are virtual-host style
// (https://acct.blob.core.windows.net/<container>/<name>). Emulator and
// IP-addressed endpoints are path style (http://127.0.0.1:10000/<account>/
// <container>/<name>); those are detected by a host that is an IP address,
// "localhost" or has no dot.
func blobFromURL(raw string) (container, name string, err error) {
	if raw == "" {
		return "", "", malformed("no url")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", malformed("url %q: %v", raw, err)
	}
	p := strings.TrimPrefix(u.Path, "/")
	host := u.Hostname()
	if net.ParseIP(host) != nil || host == "localhost" || !strings.Contains(host, ".") {
		_, rest, found := strings.Cut(p, "/")
		if !found {
			return "", "", malformed("url %q: no container", raw)
		}
		p = rest
	}
	container, name, found := strings.Cut(p, "/")
	if !found || container == "" || name == "" {
		return "", "", malformed("url %q: no blob name", raw)
	}
	return container, name, nil
}
