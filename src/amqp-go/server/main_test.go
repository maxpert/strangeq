package server

import (
	"os"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// TestMain arms the PutPendingMessage lease-leak panic for the whole server
// test binary (WS-I Phase B): a PendingMessage returned to the pool while
// still carrying a publish lease is a bug and must fail the test that leaked
// it. (In production the same violation releases the lease and increments
// amqp_publish_lease_leaked_total instead.)
func TestMain(m *testing.M) {
	protocol.SetLeaseLeakPanicForTest(true)
	os.Exit(m.Run())
}
