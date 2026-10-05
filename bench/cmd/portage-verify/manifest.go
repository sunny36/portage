package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

// manifestEntry is one JSONL line written by portage-loadgen.
type manifestEntry struct {
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
	SHA256    string    `json:"sha256"`
	WrittenAt time.Time `json:"written_at"`

	// order is the entry's position across all manifests read, in input
	// order; it breaks written_at ties.
	order int64
}

// manifestSet is the deduplicated content of one or more manifests.
type manifestSet struct {
	Entries    []manifestEntry // one per key, sorted by key
	Lines      int64           // entries read, including superseded ones
	Superseded int64           // entries replaced by a later write of the same key
}

// readManifests reads every manifest and keeps only the last write of each
// key: the entry with the latest written_at, ties going to the one read
// last (later file on the command line, later line in a file). Several
// generators may share a key space, so written_at, not file order, decides.
func readManifests(paths []string) (*manifestSet, error) {
	latest := map[string]manifestEntry{}
	set := &manifestSet{}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		err = readManifest(f, func(e manifestEntry) {
			e.order = set.Lines
			set.Lines++
			if prev, ok := latest[e.Key]; ok {
				set.Superseded++
				if e.WrittenAt.Before(prev.WrittenAt) {
					return
				}
			}
			latest[e.Key] = e
		})
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}
	set.Entries = make([]manifestEntry, 0, len(latest))
	for _, e := range latest {
		set.Entries = append(set.Entries, e)
	}
	sort.Slice(set.Entries, func(i, j int) bool { return set.Entries[i].Key < set.Entries[j].Key })
	return set, nil
}

// readManifest decodes JSONL from r. A truncated final line (the generator
// was killed mid-write) is an error: the run's manifest is not trustworthy.
func readManifest(r io.Reader, fn func(manifestEntry)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var e manifestEntry
		if err := json.Unmarshal(b, &e); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if e.Key == "" || e.Size < 0 || len(e.SHA256) != 64 {
			return fmt.Errorf("line %d: incomplete entry (key %q, size %d, sha256 %q)", line, e.Key, e.Size, e.SHA256)
		}
		fn(e)
	}
	return sc.Err()
}

// loadSummary describes the load the manifests record.
type loadSummary struct {
	Files      int64     `json:"files"`
	Bytes      int64     `json:"bytes"`
	FirstWrite time.Time `json:"first_write"`
	LastWrite  time.Time `json:"last_write"`
	// BytesPerSecond is Bytes over the first-to-last write span.
	BytesPerSecond float64 `json:"bytes_per_second"`
	MaxSize        int64   `json:"max_size"`
}

func summarizeLoad(entries []manifestEntry) loadSummary {
	var s loadSummary
	for i, e := range entries {
		s.Files++
		s.Bytes += e.Size
		s.MaxSize = max(s.MaxSize, e.Size)
		if i == 0 || e.WrittenAt.Before(s.FirstWrite) {
			s.FirstWrite = e.WrittenAt
		}
		if e.WrittenAt.After(s.LastWrite) {
			s.LastWrite = e.WrittenAt
		}
	}
	if span := s.LastWrite.Sub(s.FirstWrite).Seconds(); span > 0 {
		s.BytesPerSecond = float64(s.Bytes) / span
	}
	return s
}
