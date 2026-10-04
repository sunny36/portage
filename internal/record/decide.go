package record

import (
	"strings"
	"time"
)

// Decide applies the copy-decision rules of ADR 0002 (rules 2–4) to the
// current record of a key and a freshly Stat'ed candidate version. existing
// == nil means there is no row yet. It is pure: Store.Claim calls it under a
// row lock and persists the outcome.
//
// Order of evaluation:
//
//  1. No row → copy.
//  2. synced_version == candidate.version → already_synced (unless the key
//     was deleted since: the destination copy may be gone, so a re-created
//     object with the same version is copied again).
//  3. Candidate strictly older than the synced version, or than a version
//     being copied under a live lease → stale. Equal ordering with a
//     different version counts as newer (copy).
//  4. Live lease (status=copying, claimed_until > now) for any version,
//     including the candidate's own → busy. A duplicate job must never run a
//     parallel copy of the same version.
//
// Anything else (pending, failed, synced at an older version, deleted,
// expired lease) → copy.
func Decide(existing *Record, c Candidate, now time.Time) Decision {
	if existing == nil {
		return DecisionCopy
	}
	r := existing

	// Rule 2.
	if r.Status != StatusDeleted && r.SyncedVersion != "" && r.SyncedVersion == c.Version {
		return DecisionAlreadySynced
	}

	// Rule 3 against what is synced.
	if r.SyncedVersion != "" && r.SyncedVersion != c.Version &&
		CompareVersions(c.Sequencer, c.MTime, r.SyncedSequencer, r.SyncedMTime) < 0 {
		return DecisionStale
	}

	if leaseHeld(r, now) {
		// Rule 3 against what is being copied.
		if r.SourceVersion != c.Version &&
			CompareVersions(c.Sequencer, c.MTime, r.SourceSequencer, r.SourceMTime) < 0 {
			return DecisionStale
		}
		// Rule 4.
		return DecisionBusy
	}

	return DecisionCopy
}

// leaseHeld reports whether some worker holds an unexpired claim on r.
func leaseHeld(r *Record, now time.Time) bool {
	return r.Status == StatusCopying && r.ClaimedUntil.After(now)
}

// CompareVersions orders two source versions, newest wins (ADR 0002 rule 3).
// When both have a sequencer the sequencers are compared; otherwise the
// mtimes are. It returns -1 if a is older than b, 0 if they order equally and
// +1 if a is newer. Callers treat 0 with a different version id as newer.
func CompareVersions(aSeq string, aMTime time.Time, bSeq string, bMTime time.Time) int {
	if aSeq != "" && bSeq != "" {
		return CompareSequencers(aSeq, bSeq)
	}
	switch {
	case aMTime.Before(bMTime):
		return -1
	case aMTime.After(bMTime):
		return 1
	}
	return 0
}

// CompareSequencers compares two provider event sequencers (Azure and S3 use
// hex strings of possibly different lengths): the shorter one is left-padded
// with '0' to equal length, then they are compared lexically
// (case-insensitively). It returns -1, 0 or +1.
func CompareSequencers(a, b string) int {
	a, b = strings.ToLower(a), strings.ToLower(b)
	if d := len(a) - len(b); d > 0 {
		b = strings.Repeat("0", d) + b
	} else if d < 0 {
		a = strings.Repeat("0", -d) + a
	}
	return strings.Compare(a, b)
}

// isNewer reports whether candidate (seq, mtime, version) should replace the
// known version r* : strictly newer, or equal ordering with a different
// version.
func isNewer(cSeq string, cMTime time.Time, cVersion, rSeq string, rMTime time.Time, rVersion string) bool {
	if rVersion == "" {
		return cVersion != ""
	}
	if cVersion == rVersion {
		return false
	}
	return CompareVersions(cSeq, cMTime, rSeq, rMTime) >= 0
}
