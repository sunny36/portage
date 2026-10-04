package main

import (
	"fmt"
	"net/url"
	"regexp"
	"time"
)

// humanBytes formats n with IEC units: 0 B, 1.5 KiB, 64.0 MiB, 2.3 TiB.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 5; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// shortDuration rounds d for humans: 850ms, 42s, 3m12s, 5h4m, 3d2h.
func shortDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return d.Round(time.Second).String()
	case d < time.Hour:
		d = d.Round(time.Second)
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		d = d.Round(time.Minute)
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		d = d.Round(time.Hour)
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// ago renders t relative to now ("3m12s ago"), or "never" for the zero time.
func ago(now, t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return shortDuration(now.Sub(t)) + " ago"
}

var dsnPasswordRE = regexp.MustCompile(`(?i)(password\s*=\s*)('[^']*'|\S+)`)

// redactURL hides credentials in a URL or libpq key=value DSN: the userinfo
// password and every query value (SAS tokens, passwords as parameters).
func redactURL(s string) string {
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" {
		return dsnPasswordRE.ReplaceAllString(s, "${1}xxxxx")
	}
	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			if !safeQueryParam[k] {
				q.Set(k, "xxxxx")
			}
		}
		u.RawQuery = q.Encode()
	}
	return u.Redacted()
}

// Query parameters that never carry secrets and help when shown.
var safeQueryParam = map[string]bool{
	"sslmode": true, "search_path": true, "application_name": true, "connect_timeout": true,
	"pool_max_conns": true, "target_session_attrs": true,
}
