package record

import (
	"testing"
	"time"
)

func TestCompareSequencers(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"0A", "0A", 0},
		{"0a", "0A", 0},
		{"A", "0A", 0},    // left-pad
		{"FF", "100", -1}, // 0FF < 100
		{"100", "FF", 1},  // 100 > 0FF
		{"0000000000000000000000000000BF4F0000000000123456", "0000000000000000000000000000BF4F0000000000123457", -1},
		{"0062E99A88DC407460", "0062E99A88DC407461", -1},
		{"9", "10", -1},
	}
	for _, tt := range tests {
		if got := CompareSequencers(tt.a, tt.b); got != tt.want {
			t.Errorf("CompareSequencers(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	tests := []struct {
		name string
		aSeq string
		aMT  time.Time
		bSeq string
		bMT  time.Time
		want int
	}{
		{"seq wins over mtime", "02", t0, "01", t1, 1},
		{"seq older", "01", t1, "02", t0, -1},
		{"one side no seq uses mtime", "02", t0, "", t1, -1},
		{"no seq mtime newer", "", t1, "", t0, 1},
		{"no seq mtime equal", "", t0, "", t0, 0},
	}
	for _, tt := range tests {
		if got := CompareVersions(tt.aSeq, tt.aMT, tt.bSeq, tt.bMT); got != tt.want {
			t.Errorf("%s: got %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestDecide(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	t0 := now.Add(-time.Hour)
	t1 := t0.Add(time.Minute)
	t2 := t1.Add(time.Minute)
	live := now.Add(time.Minute)
	expired := now.Add(-time.Second)

	cand := func(version, seq string, mt time.Time) Candidate {
		return Candidate{PipelineID: "p", Key: "k", Version: version, Sequencer: seq, MTime: mt}
	}
	synced := func(version, seq string, mt time.Time) *Record {
		return &Record{
			PipelineID: "p", Key: "k", Status: StatusSynced,
			SourceVersion: version, SourceSequencer: seq, SourceMTime: mt,
			SyncedVersion: version, SyncedSequencer: seq, SyncedMTime: mt,
		}
	}
	copying := func(base *Record, version, seq string, mt, until time.Time) *Record {
		r := &Record{PipelineID: "p", Key: "k"}
		if base != nil {
			*r = *base
		}
		r.Status = StatusCopying
		r.SourceVersion, r.SourceSequencer, r.SourceMTime = version, seq, mt
		r.ClaimedUntil = until
		return r
	}
	withStatus := func(r *Record, s Status) *Record { r.Status = s; return r }

	tests := []struct {
		name     string
		existing *Record
		c        Candidate
		want     Decision
	}{
		{"no row", nil, cand("v1", "", t0), DecisionCopy},
		{"empty pending row", &Record{Status: StatusPending}, cand("v1", "", t0), DecisionCopy},
		{"observed pending row", &Record{Status: StatusPending, SourceVersion: "v1", SourceMTime: t0}, cand("v1", "", t0), DecisionCopy},

		{"same version synced", synced("v1", "01", t0), cand("v1", "01", t0), DecisionAlreadySynced},
		{"same version synced, no seq", synced("v1", "", t0), cand("v1", "", t0), DecisionAlreadySynced},

		{"newer by sequencer", synced("v1", "0A", t1), cand("v2", "0B", t0), DecisionCopy},
		{"newer by sequencer, padded", synced("v1", "FF", t0), cand("v2", "100", t0), DecisionCopy},
		{"older by sequencer", synced("v2", "0B", t0), cand("v1", "0A", t1), DecisionStale},
		{"older by sequencer, padded", synced("v2", "100", t0), cand("v1", "FF", t1), DecisionStale},

		{"no seq: mtime newer", synced("v1", "", t0), cand("v2", "", t1), DecisionCopy},
		{"no seq: mtime older", synced("v2", "", t1), cand("v1", "", t0), DecisionStale},
		{"no seq: mtime equal, different version", synced("v1", "", t0), cand("v2", "", t0), DecisionCopy},
		{"seq only on candidate: mtime decides", synced("v1", "", t1), cand("v2", "99", t0), DecisionStale},

		{"busy: lease held for another (newer) version", copying(synced("v1", "", t0), "v3", "", t2, live), cand("v2", "", t2), DecisionBusy},
		{"busy: candidate newer than in-flight copy", copying(synced("v1", "", t0), "v2", "", t1, live), cand("v3", "", t2), DecisionBusy},
		{"stale: candidate older than in-flight copy", copying(synced("v1", "", t0), "v3", "", t2, live), cand("v2", "", t1), DecisionStale},
		{"busy: lease held for the SAME version", copying(nil, "v1", "", t0, live), cand("v1", "", t0), DecisionBusy},
		{"busy: same version, first copy ever", copying(nil, "v1", "01", t0, live), cand("v1", "01", t0), DecisionBusy},
		{"expired lease: takeover same version", copying(nil, "v1", "", t0, expired), cand("v1", "", t0), DecisionCopy},
		{"expired lease: takeover newer version", copying(synced("v1", "", t0), "v2", "", t1, expired), cand("v3", "", t2), DecisionCopy},
		{"lease exactly at now counts as expired", copying(nil, "v1", "", t0, now), cand("v1", "", t0), DecisionCopy},
		{"expired lease, candidate older than synced", copying(synced("v2", "", t1), "v3", "", t2, expired), cand("v1", "", t0), DecisionStale},

		{"failed status retries", withStatus(&Record{SourceVersion: "v1", SourceMTime: t0, Attempts: 3, LastError: "boom"}, StatusFailed), cand("v1", "", t0), DecisionCopy},
		{"failed after a sync, newer version", withStatus(synced("v1", "", t0), StatusFailed), cand("v2", "", t1), DecisionCopy},
		{"failed after a sync, same version still synced", withStatus(synced("v1", "", t0), StatusFailed), cand("v1", "", t0), DecisionAlreadySynced},

		{"deleted then re-created (new version)", withStatus(synced("v1", "", t0), StatusDeleted), cand("v2", "", t1), DecisionCopy},
		{"deleted then re-created (same version/etag)", withStatus(synced("v1", "", t0), StatusDeleted), cand("v1", "", t0), DecisionCopy},
		{"deleted, candidate older than synced", withStatus(synced("v2", "", t1), StatusDeleted), cand("v1", "", t0), DecisionStale},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var before Record
			if tt.existing != nil {
				before = *tt.existing
			}
			if got := Decide(tt.existing, tt.c, now); got != tt.want {
				t.Fatalf("Decide = %q, want %q", got, tt.want)
			}
			if tt.existing != nil && !equalRecord(before, *tt.existing) {
				t.Fatal("Decide mutated its input")
			}
		})
	}
}

func equalRecord(a, b Record) bool {
	return a.Status == b.Status && a.SourceVersion == b.SourceVersion &&
		a.SyncedVersion == b.SyncedVersion && a.ClaimedUntil.Equal(b.ClaimedUntil) &&
		a.Attempts == b.Attempts
}

func TestIsNewer(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	if !isNewer("", t0, "v1", "", time.Time{}, "") {
		t.Error("anything is newer than nothing")
	}
	if isNewer("", t1, "v1", "", t0, "v1") {
		t.Error("same version is not newer")
	}
	if isNewer("", t0, "v1", "", t1, "v2") {
		t.Error("older mtime is not newer")
	}
	if !isNewer("", t0, "v2", "", t0, "v1") {
		t.Error("equal mtime, different version is newer")
	}
	if isNewer("01", t1, "v2", "02", t0, "v1") {
		t.Error("older sequencer is not newer")
	}
}
