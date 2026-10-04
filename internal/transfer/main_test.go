//go:build !integration

package transfer

import (
	"testing"

	"go.uber.org/goleak"
)

// Unit tests must leave no goroutines behind (heartbeat, upload workers).
// Integration builds skip this: SDK HTTP clients keep idle connections.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
