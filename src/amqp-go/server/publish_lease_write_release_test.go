package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// THE PUBLISH LEASE IS RELEASED AT THE WAL WRITE, NOT AT THE CONFIRM.
//
// The lease bounds the publish body bytes the broker HOLDS. Once the record's
// bytes are in the WAL file, the broker no longer holds the body: it lives in
// the file (and in the hot cache's bounded copy). The client is only told
// "durable" after the fdatasync, so a crash before that loses the message —
// and the client, which was never confirmed, re-publishes. That is
// at-least-once, which is exactly what the broker already promises.
//
// So the lease's budget must bound the BODIES IN THE WRITE PIPELINE, not the
// outstanding publish window: the bytes come back when the write returns.

// TestLease_ReleasedAtTheWriteNotAtTheFsync (TDD): with the WAL's fdatasync
// gated open, the publish's lease must already be released (the write
// returned), while the publisher confirm is still outstanding.
func TestLease_ReleasedAtTheWriteNotAtTheFsync(t *testing.T) {
	release := gateWALFsync(t)
	// Released on every exit path, BEFORE the cleanups: the server's
	// shutdown waits for the WAL's committer, which is parked in the gated
	// fsync.
	defer release()
	srv, addr, _, _, probe := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	defer r.c.Close()
	r.handshake("guest", "guest", 16, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl-write-release-q")
	leaseConfirmSelect(t, r, 1)

	leaseSendPublish(t, r, 1, "", "sl-write-release-q", leaseBody(4096), true)

	// The lease WAS taken (the grant is recorded by the metrics probe).
	require.Eventually(t, func() bool { return probe.maxUsed.Load() > 0 }, 30*time.Second, time.Millisecond,
		"the publish must have been leased")

	// The lease is released even though the fdatasync is still gated: the
	// write already returned.
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond,
		"the publish lease must be released at the WAL write, not at the confirm")

	// The confirm is still outstanding (the durability barrier is intact):
	// nothing may reach the wire before the fdatasync.
	// A negative assertion: nothing may reach the wire before the fdatasync.
	r.expectSilence(200 * time.Millisecond)

	// Release durability: the confirm arrives.
	release()
	leaseWaitAck(t, r, 1, 1)
}
