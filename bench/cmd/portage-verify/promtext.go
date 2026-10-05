package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sample is one line of the Prometheus text exposition format.
type sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// parsePromText parses the Prometheus text format (version 0.0.4). It keeps
// only what portage-verify needs: names, labels and values; comments,
// timestamps and exemplars are ignored.
func parsePromText(r io.Reader) ([]sample, error) {
	var out []sample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	line := 0
	for sc.Scan() {
		line++
		s := strings.TrimSpace(sc.Text())
		if s == "" || s[0] == '#' {
			continue
		}
		smp, err := parsePromLine(s)
		if err != nil {
			return nil, fmt.Errorf("metrics line %d: %w", line, err)
		}
		out = append(out, smp)
	}
	return out, sc.Err()
}

func parsePromLine(s string) (sample, error) {
	smp := sample{Labels: map[string]string{}}
	i := strings.IndexAny(s, "{ \t")
	if i <= 0 {
		return smp, fmt.Errorf("no value in %q", s)
	}
	smp.Name = s[:i]
	rest := s[i:]
	if rest[0] == '{' {
		n, err := parseLabels(rest, smp.Labels)
		if err != nil {
			return smp, err
		}
		rest = rest[n:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return smp, fmt.Errorf("no value in %q", s)
	}
	v, err := parseFloat(fields[0])
	if err != nil {
		return smp, err
	}
	smp.Value = v
	return smp, nil
}

// parseLabels parses `{a="x",b="y"}` at the start of s into into and returns
// the number of bytes consumed.
func parseLabels(s string, into map[string]string) (int, error) {
	i := 1 // past '{'
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return 0, fmt.Errorf("unterminated labels in %q", s)
		}
		if s[i] == '}' {
			return i + 1, nil
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return 0, fmt.Errorf("bad label in %q", s)
		}
		name := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			return 0, fmt.Errorf("label %s: value not quoted in %q", name, s)
		}
		i++
		var b strings.Builder
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		if i >= len(s) {
			return 0, fmt.Errorf("label %s: unterminated value in %q", name, s)
		}
		i++ // closing quote
		into[name] = b.String()
	}
}

func parseFloat(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

// scrape fetches and parses url.
func scrape(ctx context.Context, url string) ([]sample, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return parsePromText(resp.Body)
}

// bucket is one cumulative histogram bucket.
type bucket struct {
	UpperBound float64
	Count      float64
}

// histogramQuantile estimates the q-quantile from cumulative buckets the way
// Prometheus' histogram_quantile does: find the bucket the rank falls in and
// interpolate linearly inside it, assuming observations are spread evenly.
// The answer can therefore be no more precise than the bucket boundaries.
// The highest finite bound is returned when the rank lands in +Inf, and the
// first bucket is taken to start at 0. Returns NaN with no observations or
// no +Inf bucket.
func histogramQuantile(q float64, buckets []bucket) float64 {
	if math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(1)
	}
	bs := append([]bucket(nil), buckets...)
	sort.Slice(bs, func(i, j int) bool { return bs[i].UpperBound < bs[j].UpperBound })
	if len(bs) < 2 || !math.IsInf(bs[len(bs)-1].UpperBound, 1) {
		return math.NaN()
	}
	// Scrapes are not atomic; force counts to be monotonic like Prometheus.
	for i := 1; i < len(bs); i++ {
		if bs[i].Count < bs[i-1].Count {
			bs[i].Count = bs[i-1].Count
		}
	}
	total := bs[len(bs)-1].Count
	if total == 0 {
		return math.NaN()
	}
	rank := q * total
	b := sort.Search(len(bs)-1, func(i int) bool { return bs[i].Count >= rank })
	if b == len(bs)-1 {
		return bs[len(bs)-2].UpperBound
	}
	if b == 0 && bs[0].UpperBound <= 0 {
		return bs[0].UpperBound
	}
	start, end, count := 0.0, bs[b].UpperBound, bs[b].Count
	if b > 0 {
		start = bs[b-1].UpperBound
		count -= bs[b-1].Count
		rank -= bs[b-1].Count
	}
	return start + (end-start)*(rank/count)
}

// matches reports whether every want label is present with that value.
func matches(s sample, want map[string]string) bool {
	for k, v := range want {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}

// histogramBuckets sums name_bucket series matching want by le.
func histogramBuckets(samples []sample, name string, want map[string]string) []bucket {
	byLE := map[float64]float64{}
	for _, s := range samples {
		if s.Name != name+"_bucket" || !matches(s, want) {
			continue
		}
		le, err := parseFloat(s.Labels["le"])
		if err != nil {
			continue
		}
		byLE[le] += s.Value
	}
	out := make([]bucket, 0, len(byLE))
	for le, c := range byLE {
		out = append(out, bucket{UpperBound: le, Count: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpperBound < out[j].UpperBound })
	return out
}

// sum adds every series of name matching want.
func sum(samples []sample, name string, want map[string]string) float64 {
	var t float64
	for _, s := range samples {
		if s.Name == name && matches(s, want) {
			t += s.Value
		}
	}
	return t
}

// sumBy adds series of name matching want, grouped by label.
func sumBy(samples []sample, name, label string, want map[string]string) map[string]float64 {
	out := map[string]float64{}
	for _, s := range samples {
		if s.Name == name && matches(s, want) {
			out[s.Labels[label]] += s.Value
		}
	}
	return out
}
