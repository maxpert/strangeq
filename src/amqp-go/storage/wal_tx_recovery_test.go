package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALRecovery_IgnoresUncommittedTransaction is the SQ-8 recovery-atomicity
// test. It crafts a WAL file that contains one fully-committed transaction
// (Begin, a message, Commit) followed by a second transaction that was
// interrupted mid-flight — a Begin and a message record but NO Commit marker,
// exactly the on-disk state left by a crash between a transaction's operations
// being written and its commit becoming durable (or a torn tail that lost the
// commit marker).
//
// Recovery must apply the committed transaction and DISCARD the uncommitted one.
//
// Pre-fix there were no transaction-boundary markers and no per-record type tag:
// every durable message record in the WAL was recovered unconditionally (the WAL
// ack bitmap is not persisted, so on restart all still-present messages are
// replayed). An uncommitted transaction's message would therefore have been
// recovered and applied — the advertised all-or-nothing semantics were violated
// across a crash. With v2 markers the scanner buffers records after a Begin and
// only emits them on a matching Commit, so a Begin-without-Commit is dropped.
func TestWALRecovery_IgnoresUncommittedTransaction(t *testing.T) {
	dir := t.TempDir()

	wm, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	committed := &protocol.Message{
		Exchange: "e", RoutingKey: "committed-q", Body: []byte("committed"), DeliveryMode: 2,
	}
	uncommitted := &protocol.Message{
		Exchange: "e", RoutingKey: "uncommitted-q", Body: []byte("uncommitted"), DeliveryMode: 2,
	}

	const epochCommitted = uint64(1)
	const epochUncommitted = uint64(2)

	// Craft: [header] [Begin][msg][Commit] [Begin][msg]  (second tx never commits)
	buf := append([]byte(WALMagic), WALFormatVersion)
	buf = append(buf, serializeTxMarker(WALTxMarkerBegin, epochCommitted)...)
	buf = append(buf, qw.serializeMessageVersioned(WALFormatVersion, "committed-q", committed, 100)...)
	buf = append(buf, serializeTxMarker(WALTxMarkerCommit, epochCommitted)...)
	buf = append(buf, serializeTxMarker(WALTxMarkerBegin, epochUncommitted)...)
	buf = append(buf, qw.serializeMessageVersioned(WALFormatVersion, "uncommitted-q", uncommitted, 200)...)

	// Use a high file number so it does not collide with the empty file the WAL
	// manager auto-creates; RecoverFromWAL scans every .wal file in the dir.
	craftedPath := filepath.Join(sharedWALDir(dir), fmt.Sprintf("%020d%s", uint64(900000), WALFileExtension))
	require.NoError(t, os.WriteFile(craftedPath, buf, 0644))

	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)

	counts := map[string]int{}
	for _, rm := range recovered {
		counts[rm.QueueName]++
	}

	assert.Equal(t, 1, counts["committed-q"],
		"the committed transaction's message must be recovered")
	assert.Equal(t, 0, counts["uncommitted-q"],
		"an uncommitted transaction (Begin without Commit) must NOT be recovered")
}

// TestWALRecovery_TornCommitMarkerDiscardsTransaction hardens the above: if the
// commit marker itself is torn (only its leading bytes made it to disk), the
// transaction is still treated as uncommitted and discarded.
func TestWALRecovery_TornCommitMarkerDiscardsTransaction(t *testing.T) {
	dir := t.TempDir()

	wm, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm.Close()
	qw := wm.sharedWAL

	msg := &protocol.Message{Exchange: "e", RoutingKey: "torn-q", Body: []byte("torn"), DeliveryMode: 2}

	buf := append([]byte(WALMagic), WALFormatVersion)
	buf = append(buf, serializeTxMarker(WALTxMarkerBegin, 7)...)
	buf = append(buf, qw.serializeMessageVersioned(WALFormatVersion, "torn-q", msg, 300)...)
	commit := serializeTxMarker(WALTxMarkerCommit, 7)
	// Truncate the commit marker to simulate a crash mid-write of the tail.
	buf = append(buf, commit[:len(commit)-3]...)

	craftedPath := filepath.Join(sharedWALDir(dir), fmt.Sprintf("%020d%s", uint64(900001), WALFileExtension))
	require.NoError(t, os.WriteFile(craftedPath, buf, 0644))

	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)
	for _, rm := range recovered {
		assert.NotEqual(t, "torn-q", rm.QueueName,
			"a transaction whose commit marker is torn must be discarded")
	}
}

// TestWALRecovery_CommittedTransactionSurvivesRestart drives the real slow-path
// writer end-to-end: WriteTxAtomic persists a committed transaction unit, the
// WAL is closed (simulating shutdown/crash after the commit is durable), and a
// fresh WAL manager over the same directory recovers every record.
func TestWALRecovery_CommittedTransactionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	wm, err := NewWALManager(dir)
	require.NoError(t, err)

	records := []*RecoveryMessage{
		{QueueName: "durable-q", Offset: 1, Message: &protocol.Message{Body: []byte("m1"), DeliveryMode: 2}},
		{QueueName: "durable-q", Offset: 2, Message: &protocol.Message{Body: []byte("m2"), DeliveryMode: 2}},
		{QueueName: "durable-q", Offset: 3, Message: &protocol.Message{Body: []byte("m3"), DeliveryMode: 2}},
	}
	require.NoError(t, wm.WriteTxAtomic(records))
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)

	n := 0
	for _, rm := range recovered {
		if rm.QueueName == "durable-q" {
			n++
		}
	}
	assert.Equal(t, len(records), n, "a committed transaction unit must fully survive restart")
}
