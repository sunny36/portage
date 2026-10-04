package main

import (
	"crypto/sha256"
	"io"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"4096":   4096,
		"10KB":   10_000,
		"10kb":   10_000,
		"1.5GB":  1_500_000_000,
		"8MiB":   8 << 20,
		"2 TB":   2_000_000_000_000,
		"5GiB":   5 << 30,
		"100 mb": 100_000_000,
	}
	for in, want := range cases {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "GB", "10XB", "-5MB", "1..2KB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q): want error", bad)
		}
	}
}

func TestParseRate(t *testing.T) {
	cases := map[string]float64{
		"500GB/day": 500e9 / 86400,
		"500GB/d":   500e9 / 86400,
		"20MB/s":    20e6,
		"36GB/h":    36e9 / 3600,
		"6MB/min":   6e6 / 60,
		"1MiB/sec":  1 << 20,
		"0":         0,
		"":          0,
	}
	for in, want := range cases {
		got, err := parseRate(in)
		if err != nil || math.Abs(got-want) > 1e-6 {
			t.Errorf("parseRate(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"500GB", "500GB/week", "abc/s", "0GB/s"} {
		if _, err := parseRate(bad); err == nil {
			t.Errorf("parseRate(%q): want error", bad)
		}
	}
}

func TestParseMixErrors(t *testing.T) {
	for _, bad := range []string{"", "10KB", "10KB-1KB:1", "0-1KB:1", "1KB-2KB:x", "1KB-2KB:-1", "1KB:0"} {
		if _, err := parseMix(bad); err == nil {
			t.Errorf("parseMix(%q): want error", bad)
		}
	}
}

func TestSizeMixDefaultDistribution(t *testing.T) {
	m, err := parseMix(defaultMix)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	const n = 200_000
	var small, large, huge int
	var sum float64
	for range n {
		s := m.Sample(r)
		sum += float64(s)
		switch {
		case s >= 10e3 && s <= 10e6:
			small++
		case s >= 100e6 && s <= 1e9:
			large++
		case s == 5e9:
			huge++
		default:
			t.Fatalf("sample %d outside every bucket", s)
		}
	}
	within := func(name string, got int, p float64) {
		want := p * n
		if math.Abs(float64(got)-want) > 5*math.Sqrt(want) {
			t.Errorf("%s: got %d samples, want ~%.0f", name, got, want)
		}
	}
	within("small", small, 0.95)
	within("large", large, 0.045)
	within("huge", huge, 0.005)

	// Empirical mean should be close to the analytic mean (huge blobs dominate
	// variance, so allow 10%).
	if got, want := sum/n, m.Mean(); math.Abs(got-want)/want > 0.10 {
		t.Errorf("mean: got %s, want ~%s", formatBytes(got), formatBytes(want))
	}
}

func TestSizeMixLogUniform(t *testing.T) {
	m, err := parseMix("1KB-1GB:1")
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(7, 7))
	// Log-uniform over 6 decades: each decade gets ~1/6 of samples.
	var decades [6]int
	const n = 60_000
	for range n {
		s := m.Sample(r)
		d := int(math.Log10(float64(s)) - 3)
		if d == 6 {
			d = 5
		}
		decades[d]++
	}
	for i, c := range decades {
		if math.Abs(float64(c)-n/6) > 0.05*n {
			t.Errorf("decade %d: %d samples, want ~%d", i, c, n/6)
		}
	}
}

func TestSizeMixZeroWeightBucketNeverSampled(t *testing.T) {
	m, err := parseMix("1KB:0,2KB:1,3KB:0")
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(3, 4))
	for range 10_000 {
		if s := m.Sample(r); s != 2000 {
			t.Fatalf("sampled %d from a zero-weight bucket", s)
		}
	}
}

func TestSizeMixReproducible(t *testing.T) {
	m, _ := parseMix(defaultMix)
	a, b := rand.New(rand.NewPCG(42, 1)), rand.New(rand.NewPCG(42, 1))
	for range 1000 {
		if x, y := m.Sample(a), m.Sample(b); x != y {
			t.Fatalf("same seed gave %d and %d", x, y)
		}
	}
}

func TestContentReaderReproducible(t *testing.T) {
	sum := func(seed uint64, seq int64) [32]byte {
		h := sha256.New()
		n, err := io.Copy(h, contentReader(seed, seq, 100_000))
		if err != nil || n != 100_000 {
			t.Fatalf("copy: %d, %v", n, err)
		}
		return [32]byte(h.Sum(nil))
	}
	if a, b := sum(1, 1), sum(1, 1); a != b {
		t.Error("same (seed, seq) gave different content")
	}
	if sum(1, 1) == sum(1, 2) || sum(1, 1) == sum(2, 1) {
		t.Error("different (seed, seq) gave identical content")
	}
}

func TestKeyName(t *testing.T) {
	ts := time.Date(2026, 10, 5, 2, 35, 7, 123e6, time.UTC)
	got := keyName("loadgen/", "abcd1234", ts, 42)
	want := "loadgen/2026/10/05/02/20261005T023507.123Z-abcd1234-000000042.bin"
	if got != want {
		t.Errorf("keyName = %q, want %q", got, want)
	}
	if !strings.HasPrefix(keyName("", "x", ts, 1), "2026/") {
		t.Error("empty prefix should start with the date")
	}
}
