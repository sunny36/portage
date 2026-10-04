// Package metrics exposes Portage's OpenTelemetry instruments via a
// Prometheus /metrics endpoint. Implemented in Wave 2 (agent G); this file
// pins the type the rest of the engine refers to.
package metrics

// Metrics holds every instrument. A nil *Metrics must be safe to call
// methods on (no-op), so packages and tests can run without metrics.
type Metrics struct{}
