// Package config loads and validates pipeline.yaml, and pipeline specs
// stored in the database (docs/adr/0004-pipelines-from-database.md).
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	WebhookAddr string `yaml:"webhook_addr"`
	// PipelinesFrom is where pipelines come from: "file" (default: the
	// pipelines list below) or "database" (the pipeline_spec table).
	PipelinesFrom string     `yaml:"pipelines_from"`
	Pipelines     []Pipeline `yaml:"pipelines"`
}

// PipelinesFrom values.
const (
	PipelinesFromFile     = "file"
	PipelinesFromDatabase = "database"
)

// FromDatabase reports whether pipelines are read from the pipeline_spec
// table instead of this file.
func (f *File) FromDatabase() bool { return f.PipelinesFrom == PipelinesFromDatabase }

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
	// WebhookSecret must be presented by Event Grid as the `key` query
	// parameter of the subscription URL. Required for webhook.
	WebhookSecret string `yaml:"webhook_secret"`
}

// Filters select which keys a pipeline syncs. Patterns use path.Match syntax
// against the key relative to the source prefix.
type Filters struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}

// nameRE matches what River accepts in a queue name (no leading, trailing or
// doubled hyphens). Length is checked separately.
var nameRE = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ValidName reports whether name is a valid pipeline name: 2-40 chars of
// lowercase letters, digits and single hyphens.
func ValidName(name string) bool {
	return nameRE.MatchString(name) && len(name) >= 2 && len(name) <= 40
}

// Load reads path, expands ${ENV} references, resolves secret://
// references, applies defaults and validates.
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
	var errs []error
	for i := range f.Pipelines {
		errs = append(errs, resolveSecrets(&f.Pipelines[i], fmt.Sprintf("pipelines[%d]", i), ResolveSecret)...)
	}
	f.applyDefaults()
	errs = append(errs, f.Validate())
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &f, nil
}

// ParsePipelineSpec parses the spec of one pipeline_spec row: an object
// with the fields of a pipelines[] entry in pipeline.yaml, as JSON (JSON is
// YAML, so the same decoder and field names apply). Unknown fields are
// rejected; the name comes from the row (a name inside spec is ignored);
// secret:// references are resolved; the file's defaults and validation
// apply. ${ENV} references are not expanded. Errors name fields, never
// secret values.
func ParsePipelineSpec(name string, spec []byte) (Pipeline, error) {
	return parsePipelineSpec(name, spec, ResolveSecret)
}

// CheckPipelineSpec is ParsePipelineSpec without resolving secrets: each
// secret:// reference must be well formed and is replaced by a placeholder.
// For tools that write specs on a machine that does not hold the engine's
// secrets (`portage pipelines apply`).
func CheckPipelineSpec(name string, spec []byte) (Pipeline, error) {
	return parsePipelineSpec(name, spec, placeholderSecret)
}

func parsePipelineSpec(name string, spec []byte, resolve func(string) (string, error)) (Pipeline, error) {
	var p Pipeline
	dec := yaml.NewDecoder(bytes.NewReader(spec))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		if errors.Is(err, io.EOF) {
			return Pipeline{}, errors.New("parse pipeline spec: empty")
		}
		return Pipeline{}, fmt.Errorf("parse pipeline spec: %w", err)
	}
	p.Name = name
	errs := resolveSecrets(&p, "", resolve)
	p.applyDefaults()
	errs = append(errs, p.validate("")...)
	if err := errors.Join(errs...); err != nil {
		return Pipeline{}, err
	}
	return p, nil
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
	if f.PipelinesFrom == "" {
		f.PipelinesFrom = PipelinesFromFile
	}
	for i := range f.Pipelines {
		f.Pipelines[i].applyDefaults()
	}
}

// applyDefaults fills unset pipeline fields; shared by the file and
// pipeline_spec rows.
func (p *Pipeline) applyDefaults() {
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
	switch f.PipelinesFrom {
	case "", PipelinesFromFile:
		if len(f.Pipelines) == 0 {
			add("pipelines: at least one required")
		}
	case PipelinesFromDatabase:
		if len(f.Pipelines) > 0 {
			add("pipelines: must be empty with pipelines_from: database (pipelines live in the pipeline_spec table)")
		}
	default:
		add("pipelines_from: %q must be file or database", f.PipelinesFrom)
	}
	seen := map[string]bool{}
	for i, p := range f.Pipelines {
		at := fmt.Sprintf("pipelines[%d]", i)
		if seen[p.Name] {
			add("%s.name: duplicate %q", at, p.Name)
		}
		seen[p.Name] = true
		errs = append(errs, p.validate(at)...)
	}
	return errors.Join(errs...)
}

// validate checks one pipeline with defaults applied. at prefixes field
// paths ("pipelines[0]"); it is empty for a pipeline_spec row.
func (p *Pipeline) validate(at string) []error {
	var errs []error
	add := func(field, format string, a ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", joinPath(at, field), fmt.Sprintf(format, a...)))
	}
	if !ValidName(p.Name) {
		add("name", "%q must be 2-40 chars of lowercase letters, digits and single hyphens", p.Name)
	}
	errs = append(errs, validateEndpoint(joinPath(at, "source"), p.Source)...)
	errs = append(errs, validateEndpoint(joinPath(at, "destination"), p.Destination)...)
	switch p.ExistingFiles {
	case "copy", "skip":
	default:
		add("existing_files", "%q must be copy or skip", p.ExistingFiles)
	}
	switch p.Events.Type {
	case "none":
	case "azure_queue":
		if p.Source.Provider() != "azure" {
			add("events.type", "azure_queue requires an azure source")
		}
		if p.Events.QueueAccountURL == "" || p.Events.QueueName == "" {
			add("events", "azure_queue needs queue_account_url and queue_name")
		}
	case "webhook":
		if !strings.HasPrefix(p.Events.WebhookPath, "/") {
			add("events.webhook_path", "must start with /")
		}
		if len(p.Events.WebhookSecret) < 16 {
			add("events.webhook_secret", "required, at least 16 characters")
		}
	default:
		add("events.type", "%q must be azure_queue, webhook or none", p.Events.Type)
	}
	if p.Concurrency < 1 || p.PartConcurrency < 1 {
		add("concurrency", "concurrency and part_concurrency must be >= 1")
	}
	if p.PartSize < 5<<20 {
		add("part_size", "must be at least 5MiB")
	}
	if p.ReconcileInterval < time.Minute {
		add("reconcile_interval", "must be at least 1m")
	}
	return errs
}

// joinPath joins field path segments, skipping empty ones.
func joinPath(at, field string) string {
	switch {
	case at == "":
		return field
	case field == "":
		return at
	}
	return at + "." + field
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
