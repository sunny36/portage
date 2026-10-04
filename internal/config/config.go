// Package config loads and validates pipeline.yaml.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// File is the top-level pipeline.yaml document.
type File struct {
	Version     int    `yaml:"version"`
	DatabaseURL string `yaml:"database_url"`
	// MetricsAddr serves Prometheus /metrics and /healthz, e.g. ":9090".
	MetricsAddr string `yaml:"metrics_addr"`
	// WebhookAddr serves Event Grid webhooks when any source uses them.
	WebhookAddr string     `yaml:"webhook_addr"`
	Pipelines   []Pipeline `yaml:"pipelines"`
}

// Pipeline is one continuous one-way sync.
type Pipeline struct {
	// Name is the pipeline ID: lowercase letters, digits and hyphens.
	Name        string   `yaml:"name"`
	Source      Endpoint `yaml:"source"`
	Destination Endpoint `yaml:"destination"`
	Events      Events   `yaml:"events"`
	Filters     Filters  `yaml:"filters"`
	// ExistingFiles: "copy" (default) copies what's already at the source on
	// first run; "skip" only syncs changes from now on.
	ExistingFiles string `yaml:"existing_files"`
	// Deletes propagates source deletes. Off by default.
	Deletes bool `yaml:"deletes"`
	// Concurrency is the number of files copied in parallel (default 16).
	Concurrency int `yaml:"concurrency"`
	// PartConcurrency is the number of parts per file uploaded in parallel
	// (default 4).
	PartConcurrency int `yaml:"part_concurrency"`
	// PartSize for multipart uploads (default 64MiB; clamped to provider limits).
	PartSize ByteSize `yaml:"part_size"`
	// ReconcileInterval between listing diffs (default 15m).
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`
}

// Endpoint is a source or destination. Exactly one provider block is set.
type Endpoint struct {
	// Prefix scopes the pipeline to a "folder" in the container/bucket.
	Prefix string       `yaml:"prefix"`
	Azure  *AzureConfig `yaml:"azure,omitempty"`
	S3     *S3Config    `yaml:"s3,omitempty"`
}

// Provider returns "azure" or "s3", or "" if none/multiple are set.
func (e Endpoint) Provider() string {
	switch {
	case e.Azure != nil && e.S3 == nil:
		return "azure"
	case e.S3 != nil && e.Azure == nil:
		return "s3"
	}
	return ""
}

// AzureConfig addresses one Blob container.
type AzureConfig struct {
	// AccountURL, e.g. https://acct.blob.core.windows.net or the Azurite URL
	// http://127.0.0.1:10000/devstoreaccount1.
	AccountURL string `yaml:"account_url"`
	Container  string `yaml:"container"`
	// Auth: "default" (DefaultAzureCredential: managed identity, az login,
	// env), "shared_key", or "connection_string".
	Auth             string `yaml:"auth"`
	AccountName      string `yaml:"account_name"`
	AccountKey       string `yaml:"account_key"`
	ConnectionString string `yaml:"connection_string"`
}

// S3Config addresses one bucket on S3 or an S3-compatible API (OCI, GCS
// interop, SeaweedFS for local dev).
type S3Config struct {
	Bucket string `yaml:"bucket"`
	Region string `yaml:"region"`
	// Endpoint overrides the AWS endpoint, e.g.
	// https://<namespace>.compat.objectstorage.eu-frankfurt-1.oraclecloud.com
	Endpoint  string `yaml:"endpoint"`
	PathStyle bool   `yaml:"path_style"`
	// Static credentials. When empty, the AWS default chain is used.
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	// Flavor tweaks behaviour for S3-compatible providers: "aws" (default),
	// "oci", "generic".
	Flavor string `yaml:"flavor"`
}

// Events configures how changes are delivered from the source.
type Events struct {
	// Type: "azure_queue" (Event Grid → Storage Queue, polled; default for
	// azure sources), "webhook" (Event Grid → HTTPS), or "none" (reconciler
	// only).
	Type string `yaml:"type"`
	// AzureQueue: Event Grid delivers to this Storage Queue.
	QueueAccountURL string `yaml:"queue_account_url"`
	QueueName       string `yaml:"queue_name"`
	// Webhook: path under File.WebhookAddr, e.g. /events/azure-to-oci.
	WebhookPath string `yaml:"webhook_path"`
}

// Filters select which keys a pipeline syncs. Patterns use path.Match syntax
// against the key relative to the source prefix.
type Filters struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,40}$`)

// Load reads path, expands ${ENV} references, applies defaults and validates.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse is Load without the file read.
func Parse(raw []byte) (*File, error) {
	expanded := os.ExpandEnv(string(raw))
	var f File
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse pipeline config: %w", err)
	}
	f.applyDefaults()
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *File) applyDefaults() {
	if f.Version == 0 {
		f.Version = 1
	}
	if f.MetricsAddr == "" {
		f.MetricsAddr = ":9090"
	}
	if f.WebhookAddr == "" {
		f.WebhookAddr = ":8080"
	}
	for i := range f.Pipelines {
		p := &f.Pipelines[i]
		if p.ExistingFiles == "" {
			p.ExistingFiles = "copy"
		}
		if p.Concurrency == 0 {
			p.Concurrency = 16
		}
		if p.PartConcurrency == 0 {
			p.PartConcurrency = 4
		}
		if p.PartSize == 0 {
			p.PartSize = 64 << 20
		}
		if p.ReconcileInterval == 0 {
			p.ReconcileInterval = 15 * time.Minute
		}
		if p.Events.Type == "" {
			if p.Source.Provider() == "azure" {
				p.Events.Type = "azure_queue"
			} else {
				p.Events.Type = "none"
			}
		}
		if p.Events.Type == "webhook" && p.Events.WebhookPath == "" {
			p.Events.WebhookPath = "/events/" + p.Name
		}
		for _, e := range []*Endpoint{&p.Source, &p.Destination} {
			// A prefix is a folder: "exports" must not also match "exportsX/".
			e.Prefix = strings.TrimLeft(e.Prefix, "/")
			if e.Prefix != "" && !strings.HasSuffix(e.Prefix, "/") {
				e.Prefix += "/"
			}
			if e.Azure != nil && e.Azure.Auth == "" {
				e.Azure.Auth = "default"
			}
			if e.S3 != nil && e.S3.Flavor == "" {
				e.S3.Flavor = "aws"
			}
		}
	}
}

// Validate reports every problem found, joined.
func (f *File) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if f.Version != 1 {
		add("version: unsupported %d (want 1)", f.Version)
	}
	if f.DatabaseURL == "" {
		add("database_url: required")
	}
	if len(f.Pipelines) == 0 {
		add("pipelines: at least one required")
	}
	seen := map[string]bool{}
	for i, p := range f.Pipelines {
		at := fmt.Sprintf("pipelines[%d]", i)
		if !nameRE.MatchString(p.Name) {
			add("%s.name: %q must match %s", at, p.Name, nameRE)
		}
		if seen[p.Name] {
			add("%s.name: duplicate %q", at, p.Name)
		}
		seen[p.Name] = true
		errs = append(errs, validateEndpoint(at+".source", p.Source)...)
		errs = append(errs, validateEndpoint(at+".destination", p.Destination)...)
		switch p.ExistingFiles {
		case "copy", "skip":
		default:
			add("%s.existing_files: %q must be copy or skip", at, p.ExistingFiles)
		}
		switch p.Events.Type {
		case "none":
		case "azure_queue":
			if p.Source.Provider() != "azure" {
				add("%s.events.type: azure_queue requires an azure source", at)
			}
			if p.Events.QueueAccountURL == "" || p.Events.QueueName == "" {
				add("%s.events: azure_queue needs queue_account_url and queue_name", at)
			}
		case "webhook":
			if !strings.HasPrefix(p.Events.WebhookPath, "/") {
				add("%s.events.webhook_path: must start with /", at)
			}
		default:
			add("%s.events.type: %q must be azure_queue, webhook or none", at, p.Events.Type)
		}
		if p.Concurrency < 1 || p.PartConcurrency < 1 {
			add("%s: concurrency and part_concurrency must be >= 1", at)
		}
		if p.PartSize < 5<<20 {
			add("%s.part_size: must be at least 5MiB", at)
		}
		if p.ReconcileInterval < time.Minute {
			add("%s.reconcile_interval: must be at least 1m", at)
		}
	}
	return errors.Join(errs...)
}

func validateEndpoint(at string, e Endpoint) []error {
	var errs []error
	switch e.Provider() {
	case "azure":
		a := e.Azure
		if a.Container == "" {
			errs = append(errs, fmt.Errorf("%s.azure.container: required", at))
		}
		switch a.Auth {
		case "default", "shared_key":
			if a.AccountURL == "" {
				errs = append(errs, fmt.Errorf("%s.azure.account_url: required", at))
			}
			if a.Auth == "shared_key" && (a.AccountName == "" || a.AccountKey == "") {
				errs = append(errs, fmt.Errorf("%s.azure: shared_key needs account_name and account_key", at))
			}
		case "connection_string":
			if a.ConnectionString == "" {
				errs = append(errs, fmt.Errorf("%s.azure.connection_string: required", at))
			}
		default:
			errs = append(errs, fmt.Errorf("%s.azure.auth: %q must be default, shared_key or connection_string", at, a.Auth))
		}
	case "s3":
		s := e.S3
		if s.Bucket == "" {
			errs = append(errs, fmt.Errorf("%s.s3.bucket: required", at))
		}
		if s.Region == "" {
			errs = append(errs, fmt.Errorf("%s.s3.region: required", at))
		}
		switch s.Flavor {
		case "aws", "oci", "generic":
		default:
			errs = append(errs, fmt.Errorf("%s.s3.flavor: %q must be aws, oci or generic", at, s.Flavor))
		}
		if (s.AccessKeyID == "") != (s.SecretAccessKey == "") {
			errs = append(errs, fmt.Errorf("%s.s3: access_key_id and secret_access_key go together", at))
		}
	default:
		errs = append(errs, fmt.Errorf("%s: set exactly one of azure or s3", at))
	}
	return errs
}

// ByteSize parses "64MiB", "8MB", "1048576".
type ByteSize int64

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1},
}

// ParseByteSize parses an integer with an optional unit suffix.
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return ByteSize(n * mult), nil
}
