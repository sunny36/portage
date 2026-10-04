package reconcile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/sunny36/portage/internal/change"
	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/record"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// fakeSource lists objects in byte order (as S3/Azure do), limit per page.
type fakeSource struct {
	connector.Connector // only List is used
	objs                []connector.ObjectInfo
	less                func(a, b string) bool // listing order; default byte order
	lists               int
	stats               int
}

func (f *fakeSource) Stat(_ context.Context, key string) (connector.ObjectInfo, error) {
	f.stats++
	for _, o := range f.objs {
		if o.Key == key {
			return o, nil
		}
	}
	return connector.ObjectInfo{}, connector.ErrNotFound
}

// utf16Less is UTF-16 code-unit order, which is how Azurite lists blobs.
func utf16Less(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

func (f *fakeSource) List(_ context.Context, prefix, cursor string, limit int) (connector.ListPage, error) {
	f.lists++
	if prefix != "" {
		panic("reconcile lists the whole scope")
	}
	objs := append([]connector.ObjectInfo(nil), f.objs...)
	less := f.less
	if less == nil {
		less = func(a, b string) bool { return a < b }
	}
	sort.Slice(objs, func(i, j int) bool { return less(objs[i].Key, objs[j].Key) })
	start := 0
	if cursor != "" {
		start, _ = strconv.Atoi(cursor)
	}
	end := min(start+limit, len(objs))
	page := connector.ListPage{Objects: objs[start:end]}
	if end < len(objs) {
		page.NextCursor = strconv.Itoa(end)
	}
	return page, nil
}

// fakeStore mimics PGStore.List: byte order (COLLATE "C"), after afterKey.
type fakeStore struct {
	record.Store // only List is used
	recs         []record.Record
	maxPage      int
	unsorted     bool
}

func (f *fakeStore) Get(_ context.Context, pipelineID, key string) (record.Record, error) {
	for _, r := range f.recs {
		if r.PipelineID == pipelineID && r.Key == key {
			return r, nil
		}
	}
	return record.Record{}, record.ErrNotFound
}

func (f *fakeStore) List(_ context.Context, pipelineID, afterKey string, limit int) ([]record.Record, error) {
	recs := append([]record.Record(nil), f.recs...)
	if !f.unsorted {
		sort.Slice(recs, func(i, j int) bool { return recs[i].Key < recs[j].Key })
	}
	var out []record.Record
	for _, r := range recs {
		if r.PipelineID == pipelineID && r.Key > afterKey && len(out) < limit {
			out = append(out, r)
		}
	}
	if limit > f.maxPage {
		f.maxPage = limit
	}
	return out, nil
}

type sink struct {
	got  []change.ObjectChanged
	fail error
}

func (s *sink) emit(_ context.Context, c change.ObjectChanged) error {
	if s.fail != nil {
		return s.fail
	}
	s.got = append(s.got, c)
	return nil
}

func (s *sink) summary() map[string]change.Kind {
	m := map[string]change.Kind{}
	for _, c := range s.got {
		m[c.Key] = c.Kind
	}
	return m
}

func obj(key, version string) connector.ObjectInfo {
	return connector.ObjectInfo{Key: key, Version: version, Size: int64(len(key)), ModTime: now.Add(-time.Hour)}
}

func synced(key, version string) record.Record {
	return record.Record{PipelineID: "p1", Key: key, SourceVersion: version, SyncedVersion: version, Status: record.StatusSynced}
}

func runRec(t *testing.T, src connector.Connector, store *fakeStore, mod func(*Options)) (Stats, *sink, error) {
	t.Helper()
	out := &sink{}
	opts := Options{
		PipelineID: "p1",
		Source:     src,
		Store:      store,
		Emit:       out.emit,
		PageSize:   2, // force paging on both sides
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:        func() time.Time { return now },
	}
	if mod != nil {
		mod(&opts)
	}
	st, err := Run(context.Background(), opts)
	return st, out, err
}

func TestInitialCopy(t *testing.T) {
	src := &fakeSource{objs: []connector.ObjectInfo{obj("c", "v3"), obj("a", "v1"), obj("b/x", "v2"), obj("d", "v4"), obj("e", "v5")}}
	st, out, err := runRec(t, src, &fakeStore{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Listed != 5 || st.Emitted != 5 || st.Deleted != 0 {
		t.Fatalf("stats %+v", st)
	}
	c := out.got[0]
	if c.Key != "a" || c.Kind != change.KindUpsert || c.Version != "v1" || c.Size != 1 ||
		c.Origin != change.OriginReconcile || !c.EventTime.Equal(now.Add(-time.Hour)) || !c.DetectedAt.Equal(now) || c.PipelineID != "p1" {
		t.Fatalf("change %+v", c)
	}
	if src.lists != 3 {
		t.Fatalf("listed %d pages, want 3", src.lists)
	}
}

func TestAllSynced(t *testing.T) {
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a", "v1"), obj("b", "v2"), obj("c", "v3")}}
	store := &fakeStore{recs: []record.Record{synced("a", "v1"), synced("b", "v2"), synced("c", "v3")}}
	st, out, err := runRec(t, src, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.AlreadySynced != 3 || len(out.got) != 0 {
		t.Fatalf("stats %+v, emitted %+v", st, out.got)
	}
}

func TestMixed(t *testing.T) {
	future, past := now.Add(time.Minute), now.Add(-time.Minute)
	src := &fakeSource{objs: []connector.ObjectInfo{
		obj("changed", "v2"),
		obj("copying-live", "v2"),
		obj("copying-expired", "v2"),
		obj("new", "v1"),
		obj("pending", "v1"),
		obj("recreated", "v1"),
		obj("same", "v1"),
	}}
	store := &fakeStore{recs: []record.Record{
		synced("changed", "v1"),
		{PipelineID: "p1", Key: "copying-live", SyncedVersion: "v1", SourceVersion: "v2", Status: record.StatusCopying, ClaimedUntil: future},
		{PipelineID: "p1", Key: "copying-expired", SyncedVersion: "v1", SourceVersion: "v2", Status: record.StatusCopying, ClaimedUntil: past},
		synced("gone", "v1"),
		{PipelineID: "p1", Key: "gone-already", SyncedVersion: "v1", Status: record.StatusDeleted},
		{PipelineID: "p1", Key: "pending", SourceVersion: "v1", Status: record.StatusPending},
		{PipelineID: "p1", Key: "recreated", SyncedVersion: "v1", Status: record.StatusDeleted},
		synced("same", "v1"),
		synced("zz-gone", "v9"),
		synced("other-pipeline", "v1"),
	}}
	store.recs[len(store.recs)-1].PipelineID = "p2"

	st, out, err := runRec(t, src, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]change.Kind{
		"changed":         change.KindUpsert,
		"copying-expired": change.KindUpsert,
		"new":             change.KindUpsert,
		"pending":         change.KindUpsert,
		"recreated":       change.KindUpsert,
		"gone":            change.KindDelete,
		"zz-gone":         change.KindDelete,
	}
	got := out.summary()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %q, want %q", k, got[k], v)
		}
	}
	if st.Listed != 7 || st.Emitted != 5 || st.Deleted != 2 || st.AlreadySynced != 1 || st.InFlight != 1 {
		t.Fatalf("stats %+v", st)
	}
	for _, c := range out.got {
		if c.Kind == change.KindDelete && (c.Version != "" || c.Origin != change.OriginReconcile || !c.DetectedAt.Equal(now)) {
			t.Errorf("delete %+v", c)
		}
	}
}

func TestIgnoreBefore(t *testing.T) {
	old := obj("old", "v1")
	old.ModTime = now.Add(-48 * time.Hour)
	fresh := obj("fresh", "v1")
	fresh.ModTime = now.Add(-time.Minute)
	oldChanged := obj("old-changed", "v2")
	oldChanged.ModTime = now.Add(-48 * time.Hour)
	src := &fakeSource{objs: []connector.ObjectInfo{old, fresh, oldChanged}}
	// A key with a record is still reconciled, however old.
	store := &fakeStore{recs: []record.Record{synced("old-changed", "v1")}}
	st, out, err := runRec(t, src, store, func(o *Options) { o.IgnoreBefore = now.Add(-24 * time.Hour) })
	if err != nil {
		t.Fatal(err)
	}
	got := out.summary()
	if len(got) != 2 || got["fresh"] != change.KindUpsert || got["old-changed"] != change.KindUpsert {
		t.Fatalf("got %v", got)
	}
	if st.Ignored != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestFilterBothSides(t *testing.T) {
	f, err := change.NewFilter(config.Filters{Exclude: []string{"**/*.tmp"}})
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a.tmp", "v1"), obj("b", "v1"), obj("d/e.tmp", "v1")}}
	// "c.tmp" has a record but is now excluded: no delete for it.
	store := &fakeStore{recs: []record.Record{synced("b", "v1"), synced("c.tmp", "v1")}}
	st, out, err := runRec(t, src, store, func(o *Options) { o.Filter = f })
	if err != nil {
		t.Fatal(err)
	}
	if len(out.got) != 0 || st.Listed != 1 || st.AlreadySynced != 1 {
		t.Fatalf("stats %+v, emitted %+v", st, out.got)
	}
}

func TestEmptySourceGuard(t *testing.T) {
	store := &fakeStore{recs: []record.Record{synced("a", "v1"), synced("b", "v1"), synced("c", "v1"),
		{PipelineID: "p1", Key: "d", Status: record.StatusDeleted}}}
	_, out, err := runRec(t, &fakeSource{}, store, nil)
	if !errors.Is(err, ErrEmptySource) {
		t.Fatalf("err = %v, want ErrEmptySource", err)
	}
	if len(out.got) != 0 {
		t.Fatalf("emitted %+v", out.got)
	}
	if want := "refusing to emit 3 deletes"; !contains(err.Error(), want) {
		t.Fatalf("error %q lacks %q", err, want)
	}

	// Source with only filtered-out objects counts as empty too.
	f, _ := change.NewFilter(config.Filters{Include: []string{"*.csv"}})
	store = &fakeStore{recs: []record.Record{synced("x.csv", "v1")}}
	if _, _, err := runRec(t, &fakeSource{objs: []connector.ObjectInfo{obj("y.txt", "v1")}}, store,
		func(o *Options) { o.Filter = f }); !errors.Is(err, ErrEmptySource) {
		t.Fatalf("err = %v", err)
	}

	// Nothing live to delete: an empty source is fine.
	store = &fakeStore{recs: []record.Record{{PipelineID: "p1", Key: "d", Status: record.StatusDeleted}}}
	if _, _, err := runRec(t, &fakeSource{}, store, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runRec(t, &fakeSource{}, &fakeStore{}, nil); err != nil {
		t.Fatal(err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// unicodeKeys mixes case, punctuation that sorts around '/', and multi-byte
// UTF-8, where byte order differs from most locale collations (e.g. en_US
// sorts "a" < "B" and ignores punctuation; byte order puts "B" < "a").
var unicodeKeys = []string{
	"B", "a", "Z/z", "a b", "a-b", "a/b", "a.b", "a_b", "aB", "ab", "Ab",
	"é", "é", "f", "日本/語", "日本語", "😀", "ÿ", "Ā", "~", "0", "9/0",
}

func TestUnicodeOrderingNoFalseDeletes(t *testing.T) {
	src := &fakeSource{}
	store := &fakeStore{}
	for i, k := range unicodeKeys {
		v := "v" + strconv.Itoa(i)
		src.objs = append(src.objs, obj(k, v))
		store.recs = append(store.recs, synced(k, v))
	}
	st, out, err := runRec(t, src, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.got) != 0 || st.AlreadySynced != int64(len(unicodeKeys)) {
		t.Fatalf("stats %+v, emitted %+v", st, out.got)
	}
}

// Azurite lists in UTF-16 order, which disagrees with byte order for these
// keys ("\uff5e" < "😀" in UTF-8, the reverse in UTF-16). The join must still
// find every key synced and emit nothing.
func TestUTF16SourceOrderNoFalseDeletes(t *testing.T) {
	keys := []string{"a", "\uff5e", "😀", "😀/x", "\uffff", "z", "\ue000"}
	if utf16Less(keys[1], keys[2]) || keys[1] >= keys[2] {
		t.Fatal("fixture does not exercise the UTF-8/UTF-16 difference")
	}
	src := &fakeSource{less: utf16Less}
	store := &fakeStore{}
	for _, k := range keys {
		src.objs = append(src.objs, obj(k, "v1"))
		store.recs = append(store.recs, synced(k, "v1"))
	}
	store.recs = append(store.recs, synced("\ufffd-gone", "v1"))
	st, out, err := runRec(t, src, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.got) != 1 || out.got[0].Kind != change.KindDelete || out.got[0].Key != "\ufffd-gone" {
		t.Fatalf("got %+v", out.got)
	}
	if st.AlreadySynced != int64(len(keys)) || st.Emitted != 0 || st.Deleted != 1 {
		t.Fatalf("stats %+v", st)
	}

	// A genuinely new key that sorts into the misordered region is still
	// copied.
	src.objs = append(src.objs, obj("\uff00-new", "v1"))
	if _, out, err = runRec(t, src, store, nil); err != nil {
		t.Fatal(err)
	}
	if got := out.summary(); got["\uff00-new"] != change.KindUpsert {
		t.Fatalf("got %v", got)
	}
}

func TestDeleteCandidatesAreConfirmed(t *testing.T) {
	// The listing missed "b" (e.g. a listing glitch) but Stat finds it.
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a", "v1"), obj("c", "v1")}}
	store := &fakeStore{recs: []record.Record{synced("a", "v1"), synced("b", "v1"), synced("c", "v1"), synced("d", "v1")}}
	hidden := obj("b", "v1")
	st, out, err := runRec(t, &statOnly{fakeSource: src, extra: hidden}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.got) != 1 || out.got[0].Key != "d" || st.Deleted != 1 {
		t.Fatalf("stats %+v, got %+v", st, out.got)
	}
}

// statOnly is a source whose listing omits extra but whose Stat finds it.
type statOnly struct {
	*fakeSource
	extra connector.ObjectInfo
}

func (s *statOnly) Stat(ctx context.Context, key string) (connector.ObjectInfo, error) {
	if key == s.extra.Key {
		return s.extra, nil
	}
	return s.fakeSource.Stat(ctx, key)
}

func TestOutOfOrderRecordsAbort(t *testing.T) {
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a", "v1"), obj("b", "v1")}}
	store := &fakeStore{unsorted: true, recs: []record.Record{synced("b", "v1"), synced("a", "v1"), synced("c", "v1")}}
	_, out, err := runRec(t, src, store, func(o *Options) { o.PageSize = 10 })
	if err == nil {
		t.Fatal("want an ordering error")
	}
	for _, c := range out.got {
		if c.Kind == change.KindDelete {
			t.Fatalf("emitted a delete: %+v", c)
		}
	}
}

func TestEmitErrorStops(t *testing.T) {
	boom := errors.New("queue down")
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a", "v1"), obj("b", "v1")}}
	opts := Options{PipelineID: "p1", Source: src, Store: &fakeStore{}, Emit: (&sink{fail: boom}).emit,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, err := Run(context.Background(), opts); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultPageSize(t *testing.T) {
	store := &fakeStore{recs: []record.Record{synced("a", "v1")}}
	_, _, err := runRec(t, &fakeSource{objs: []connector.ObjectInfo{obj("a", "v1")}}, store, func(o *Options) { o.PageSize = 0 })
	if err != nil {
		t.Fatal(err)
	}
	if store.maxPage != DefaultPageSize {
		t.Fatalf("page size %d", store.maxPage)
	}
}

func TestQuotedVersionsMatch(t *testing.T) {
	src := &fakeSource{objs: []connector.ObjectInfo{obj("a", "0x8D1")}}
	store := &fakeStore{recs: []record.Record{synced("a", `"0x8D1"`)}}
	st, out, err := runRec(t, src, store, nil)
	if err != nil || len(out.got) != 0 || st.AlreadySynced != 1 {
		t.Fatalf("err=%v stats=%+v got=%+v", err, st, out.got)
	}
}
