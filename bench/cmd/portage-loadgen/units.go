package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Byte units. Decimal (KB, MB, ...) follows how cloud providers bill volume;
// binary (KiB, MiB, ...) is accepted too.
var byteUnits = map[string]float64{
	"":    1,
	"b":   1,
	"kb":  1e3,
	"mb":  1e6,
	"gb":  1e9,
	"tb":  1e12,
	"pb":  1e15,
	"kib": 1 << 10,
	"mib": 1 << 20,
	"gib": 1 << 30,
	"tib": 1 << 40,
	"pib": 1 << 50,
}

// parseSize parses a byte quantity such as "10KB", "1.5GiB" or "4096".
func parseSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	i := 0
	for i < len(t) && (t[i] >= '0' && t[i] <= '9' || t[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("size %q: missing number", s)
	}
	n, err := strconv.ParseFloat(t[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: %w", s, err)
	}
	mult, ok := byteUnits[strings.ToLower(strings.TrimSpace(t[i:]))]
	if !ok {
		return 0, fmt.Errorf("size %q: unknown unit %q", s, t[i:])
	}
	v := n * mult
	if v < 0 || v > math.MaxInt64 {
		return 0, fmt.Errorf("size %q: out of range", s)
	}
	return int64(v), nil
}

var rateUnits = map[string]time.Duration{
	"s": time.Second, "sec": time.Second, "second": time.Second,
	"m": time.Minute, "min": time.Minute, "minute": time.Minute,
	"h": time.Hour, "hr": time.Hour, "hour": time.Hour,
	"d": 24 * time.Hour, "day": 24 * time.Hour,
}

// parseRate parses a throughput such as "500GB/day" or "20MiB/s" and returns
// bytes per second. "0" or "" means unlimited and returns 0.
func parseRate(s string) (float64, error) {
	t := strings.TrimSpace(s)
	if t == "" || t == "0" {
		return 0, nil
	}
	amount, per, ok := strings.Cut(t, "/")
	if !ok {
		return 0, fmt.Errorf("rate %q: want <size>/<period>, e.g. 500GB/day", s)
	}
	n, err := parseSize(amount)
	if err != nil {
		return 0, fmt.Errorf("rate %q: %w", s, err)
	}
	d, ok := rateUnits[strings.ToLower(strings.TrimSpace(per))]
	if !ok {
		return 0, fmt.Errorf("rate %q: unknown period %q (use s, min, h or day)", s, per)
	}
	if n == 0 {
		return 0, fmt.Errorf("rate %q: must be positive", s)
	}
	return float64(n) / d.Seconds(), nil
}

// sizeBucket is one weighted range of a size mix. Sizes are drawn
// log-uniformly from [Min, Max] so small sizes within a range are as likely
// per decade as large ones.
type sizeBucket struct {
	Min, Max int64
	Weight   float64
}

// sizeMix is a weighted set of size buckets.
type sizeMix struct {
	buckets []sizeBucket
	cum     []float64 // cumulative normalised weights
}

// defaultMix is heavy-tailed: mostly small files, some large, rare huge ones.
const defaultMix = "10KB-10MB:0.95,100MB-1GB:0.045,5GB:0.005"

// parseMix parses "MIN-MAX:WEIGHT,..." (a single size "5GB:W" is a fixed
// size). Weights are relative and need not sum to 1.
func parseMix(s string) (*sizeMix, error) {
	var buckets []sizeBucket
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		rng, w, ok := strings.Cut(part, ":")
		if !ok {
			return nil, fmt.Errorf("mix entry %q: want MIN-MAX:WEIGHT", part)
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(w), 64)
		if err != nil || weight < 0 || math.IsInf(weight, 0) || math.IsNaN(weight) {
			return nil, fmt.Errorf("mix entry %q: bad weight", part)
		}
		lo, hi, isRange := strings.Cut(rng, "-")
		minSize, err := parseSize(lo)
		if err != nil {
			return nil, fmt.Errorf("mix entry %q: %w", part, err)
		}
		maxSize := minSize
		if isRange {
			if maxSize, err = parseSize(hi); err != nil {
				return nil, fmt.Errorf("mix entry %q: %w", part, err)
			}
		}
		if minSize < 1 || maxSize < minSize {
			return nil, fmt.Errorf("mix entry %q: need 1 <= MIN <= MAX", part)
		}
		buckets = append(buckets, sizeBucket{Min: minSize, Max: maxSize, Weight: weight})
	}
	return newSizeMix(buckets)
}

func newSizeMix(buckets []sizeBucket) (*sizeMix, error) {
	var total float64
	for _, b := range buckets {
		total += b.Weight
	}
	if len(buckets) == 0 || total <= 0 {
		return nil, errors.New("size mix: need at least one bucket with positive weight")
	}
	m := &sizeMix{buckets: buckets, cum: make([]float64, len(buckets))}
	var acc float64
	for i, b := range buckets {
		acc += b.Weight / total
		m.cum[i] = acc
	}
	m.cum[len(m.cum)-1] = 1
	return m, nil
}

// Sample draws one size from the mix.
func (m *sizeMix) Sample(r *rand.Rand) int64 {
	u := r.Float64()
	i := sort.SearchFloat64s(m.cum, u)
	if i >= len(m.buckets) {
		i = len(m.buckets) - 1
	}
	// SearchFloat64s returns the first index with cum >= u; skip zero-weight
	// buckets that share a cumulative value with their predecessor.
	for i < len(m.buckets)-1 && m.buckets[i].Weight == 0 {
		i++
	}
	b := m.buckets[i]
	if b.Min == b.Max {
		return b.Min
	}
	lo, hi := math.Log(float64(b.Min)), math.Log(float64(b.Max))
	v := int64(math.Exp(lo + r.Float64()*(hi-lo)))
	return min(max(v, b.Min), b.Max)
}

// Mean is the expected size of one sample, in bytes.
func (m *sizeMix) Mean() float64 {
	var total, mean float64
	for _, b := range m.buckets {
		total += b.Weight
	}
	for _, b := range m.buckets {
		bm := float64(b.Min)
		if b.Max > b.Min {
			// Mean of a log-uniform distribution on [a, b]: (b-a)/ln(b/a).
			bm = float64(b.Max-b.Min) / math.Log(float64(b.Max)/float64(b.Min))
		}
		mean += b.Weight / total * bm
	}
	return mean
}

// formatBytes renders n with a decimal unit, e.g. "1.50 GB".
func formatBytes(n float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	for n >= 1000 && i < len(units)-1 {
		n /= 1000
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f B", n)
	}
	return fmt.Sprintf("%.2f %s", n, units[i])
}
