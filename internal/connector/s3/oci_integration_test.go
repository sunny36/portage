//go:build integration

package s3

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sunny36/portage/internal/config"
	"github.com/sunny36/portage/internal/connector"
	"github.com/sunny36/portage/internal/connector/connectortest"
	"github.com/sunny36/portage/internal/testenv"
)

func errorsIsNotFound(err error) bool { return errors.Is(err, connector.ErrNotFound) }

// TestConformanceOCI runs the conformance suite against a real OCI Object
// Storage bucket through its S3 compatibility API. It is opt-in: set
//
//	PORTAGE_TEST_OCI_ENDPOINT          https://<namespace>.compat.objectstorage.<region>.oraclecloud.com
//	PORTAGE_TEST_OCI_REGION            e.g. eu-frankfurt-1
//	PORTAGE_TEST_OCI_BUCKET            an existing bucket
//	PORTAGE_TEST_OCI_ACCESS_KEY_ID     Customer Secret Key ID
//	PORTAGE_TEST_OCI_SECRET_ACCESS_KEY Customer Secret Key
//
// Each subtest works under a unique prefix inside the bucket and deletes
// everything under it afterwards; no buckets are created.
func TestConformanceOCI(t *testing.T) {
	vars := map[string]string{}
	for _, k := range []string{
		"PORTAGE_TEST_OCI_ENDPOINT", "PORTAGE_TEST_OCI_REGION", "PORTAGE_TEST_OCI_BUCKET",
		"PORTAGE_TEST_OCI_ACCESS_KEY_ID", "PORTAGE_TEST_OCI_SECRET_ACCESS_KEY",
	} {
		v := os.Getenv(k)
		if v == "" {
			t.Skipf("%s not set; skipping real OCI conformance run", k)
		}
		vars[k] = v
	}
	cfg := config.S3Config{
		Bucket:          vars["PORTAGE_TEST_OCI_BUCKET"],
		Region:          vars["PORTAGE_TEST_OCI_REGION"],
		Endpoint:        vars["PORTAGE_TEST_OCI_ENDPOINT"],
		PathStyle:       true,
		AccessKeyID:     vars["PORTAGE_TEST_OCI_ACCESS_KEY_ID"],
		SecretAccessKey: vars["PORTAGE_TEST_OCI_SECRET_ACCESS_KEY"],
		Flavor:          FlavorOCI,
	}
	connectortest.Run(t, func(t *testing.T) connector.Connector {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		prefix := "portage-it/" + testenv.UniqueName(t, "run") + "/"
		c, err := New(ctx, cfg, prefix)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { emptyScope(t, c.client, cfg.Bucket, prefix) })
		return c
	})
}
