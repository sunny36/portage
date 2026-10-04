package check

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sunny36/portage/internal/config"
)

const (
	// skewWarn: well inside what providers accept, but worth fixing.
	skewWarn = time.Minute
	// skewFail: SigV4 (S3/OCI) and Azure Shared Key reject requests whose
	// timestamp is more than 15 minutes off.
	skewFail = 15 * time.Minute
)

// endpointURL returns a URL on the endpoint's host that answers an
// unauthenticated request with a Date header, or "".
func endpointURL(ep config.Endpoint) string {
	switch {
	case ep.Azure != nil:
		if ep.Azure.AccountURL != "" {
			return ep.Azure.AccountURL
		}
		for part := range strings.SplitSeq(ep.Azure.ConnectionString, ";") {
			if k, v, ok := strings.Cut(part, "="); ok && strings.EqualFold(strings.TrimSpace(k), "BlobEndpoint") {
				return strings.TrimSpace(v)
			}
		}
	case ep.S3 != nil:
		if ep.S3.Endpoint != "" {
			return ep.S3.Endpoint
		}
		return "https://s3." + ep.S3.Region + ".amazonaws.com"
	}
	return ""
}

// serverTime sends an unauthenticated HEAD and returns the Date header. Any
// HTTP status will do (an anonymous request is usually refused).
func serverTime(ctx context.Context, url string) (time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return time.Time{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	resp.Body.Close()
	d := resp.Header.Get("Date")
	if d == "" {
		return time.Time{}, errors.New("response has no Date header")
	}
	return http.ParseTime(d)
}

func (r *runner) clock(ctx context.Context, p config.Pipeline) Result {
	var (
		parts   []string
		worst   time.Duration
		measErr []string
	)
	for _, side := range []struct {
		name string
		ep   config.Endpoint
	}{{"source", p.Source}, {"destination", p.Destination}} {
		url := endpointURL(side.ep)
		if url == "" {
			continue
		}
		before := r.opts.Now()
		st, err := r.opts.ServerTime(ctx, url)
		if err != nil {
			measErr = append(measErr, side.name+": "+describeErr(err))
			continue
		}
		after := r.opts.Now()
		local := before.Add(after.Sub(before) / 2)
		// Date has 1s resolution: anything under 2s is noise.
		skew := local.Sub(st).Round(time.Second)
		sign := "+"
		if skew < 0 {
			sign = ""
		}
		parts = append(parts, fmt.Sprintf("%s %s%v", side.name, sign, skew))
		if skew.Abs() > worst.Abs() {
			worst = skew
		}
	}
	if len(parts) == 0 {
		return warn("could not measure ("+strings.Join(measErr, "; ")+")",
			"Make sure the host clock is NTP-synced (`timedatectl status`); request signing fails beyond 15 minutes of skew.")
	}
	detail := "local clock vs " + strings.Join(parts, ", ")
	if len(measErr) > 0 {
		detail += " (not measured: " + strings.Join(measErr, "; ") + ")"
	}
	fix := "Sync the clock with NTP (`timedatectl set-ntp true` on Linux; Azure VMs sync from the host by default)."
	switch {
	case worst.Abs() >= skewFail:
		return Result{Status: StatusFail, Detail: detail + ": over 15 minutes, requests will be rejected", Fix: fix}
	case worst.Abs() > skewWarn:
		return warn(detail+": over 1 minute", fix)
	}
	return ok(detail)
}
