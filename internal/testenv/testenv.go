// Package testenv gives integration tests the endpoints of the emulators in
// deploy/docker-compose.yml. Integration test files use the build tag
// `integration` and are run with:
//
//	docker compose -f deploy/docker-compose.yml up -d --wait
//	go test -tags integration ./...
//
// Every value can be overridden with the environment variable named in its
// doc comment, e.g. to point at CI service containers.
package testenv

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// PostgresURL: PORTAGE_TEST_DATABASE_URL.
func PostgresURL() string {
	return env("PORTAGE_TEST_DATABASE_URL", "postgres://portage:portage@127.0.0.1:25432/portage?sslmode=disable")
}

// Azurite's fixed development account.
const (
	AzuriteAccountName = "devstoreaccount1"
	AzuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// AzuriteBlobURL: PORTAGE_TEST_AZURITE_BLOB_URL.
func AzuriteBlobURL() string {
	return env("PORTAGE_TEST_AZURITE_BLOB_URL", "http://127.0.0.1:20000/"+AzuriteAccountName)
}

// AzuriteQueueURL: PORTAGE_TEST_AZURITE_QUEUE_URL.
func AzuriteQueueURL() string {
	return env("PORTAGE_TEST_AZURITE_QUEUE_URL", "http://127.0.0.1:20001/"+AzuriteAccountName)
}

// AzuriteConnectionString covers both blob and queue endpoints.
func AzuriteConnectionString() string {
	return "DefaultEndpointsProtocol=http;AccountName=" + AzuriteAccountName +
		";AccountKey=" + AzuriteAccountKey +
		";BlobEndpoint=" + AzuriteBlobURL() +
		";QueueEndpoint=" + AzuriteQueueURL() + ";"
}

// S3 (SeaweedFS) endpoint and credentials from deploy/seaweedfs-s3.json.
// PORTAGE_TEST_S3_ENDPOINT overrides the endpoint.
func S3Endpoint() string { return env("PORTAGE_TEST_S3_ENDPOINT", "http://127.0.0.1:28333") }

const (
	S3Region          = "us-east-1"
	S3AccessKeyID     = "portage"
	S3SecretAccessKey = "portage-secret"
)

// UniqueName returns a lowercase name valid as an Azure container or S3
// bucket (3–63 chars, [a-z0-9-]) that is unique per call, so tests can run in
// parallel and repeatedly against the same emulator.
func UniqueName(t testing.TB, prefix string) string {
	t.Helper()
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	p := strings.ToLower(prefix)
	if len(p) > 40 {
		p = p[:40]
	}
	return strings.Trim(p, "-") + "-" + hex.EncodeToString(b)
}
