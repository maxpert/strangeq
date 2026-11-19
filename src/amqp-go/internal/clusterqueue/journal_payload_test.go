package clusterqueue

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type countedCommandSource struct {
	*bytes.Reader
	reads int
}

func (s *countedCommandSource) ReadAt(data []byte, offset int64) (int, error) {
	s.reads++
	return s.Reader.ReadAt(data, offset)
}

func TestJournaledBatchKeepsBodiesOffHeapAndReadsOnlyForReply(t *testing.T) {
	cold, session := claimBatchFixture(t, 2)
	hot, _ := claimBatchFixture(t, 2)
	first, second := bytes.Repeat([]byte{0x03}, 64<<10), bytes.Repeat([]byte{0xfe}, 64<<10)
	wire, err := EncodePublishBatch(queueName, session.Incarnation, [][]byte{first, second})
	require.NoError(t, err)
	hot.Apply(wire)
	stored := bytes.Clone(wire)
	source := &countedCommandSource{Reader: bytes.NewReader(stored)}
	if m, ok := any(cold).(interface {
		ApplyJournaled([]byte, bool, io.ReaderAt) ([]byte, error)
	}); ok {
		reply, err := m.ApplyJournaled(wire, false, source)
		require.NoError(t, err)
		require.Empty(t, reply)
	} else {
		cold.Apply(wire)
	}

	require.EqualValues(t, len(first)+len(second), cold.state.bodies, "logical capacity includes cold bytes")
	ready := cold.state.Queues[queueName].Ready
	require.Len(t, ready, 2)
	require.Empty(t, ready[0].Body, "a durable body must not retain a heap buffer")
	require.Empty(t, ready[1].Body, "a durable batch must not retain its input frame")
	require.Zero(t, source.reads, "applying a durable publish does not read its own command from disk")
	clear(wire)

	session.Sequence = 1
	claim := claimBatchRequest(t, session, 2, 1<<20)
	want := hot.Apply(claim)
	_, err = cold.ApplyCommitted(claim, false)
	require.NoError(t, err)
	require.Zero(t, source.reads, "a follower applies the claim without reading its response bodies")
	reply, err := cold.ApplyCommitted(claim, true)
	require.NoError(t, err)
	require.Equal(t, 2, source.reads)
	require.Equal(t, want, reply, "retry returns the identical attempts and payloads")
	portable := mustSnapshot(t, cold)
	require.Equal(t, mustSnapshot(t, hot), portable, "snapshots include portable message bytes")
	restored := &Machine{}
	require.NoError(t, restored.Restore(portable))
	require.Equal(t, want, restored.Apply(claim))
	flights := cold.state.Queues[queueName].Sessions[session.Session].Flights
	require.Empty(t, flights[1].Body, "checkpoint construction must not retain a temporary copy")

	// Damage the second range in the fixture's private durable image. Both
	// reads must fail locally even though length and attempts still match.
	offset := bytes.Index(stored, second)
	require.Greater(t, offset, 0)
	stored[offset] ^= 1
	reply, err = cold.ApplyCommitted(claim, true)
	require.Error(t, err)
	require.Empty(t, reply)
	_, err = cold.Snapshot()
	require.Error(t, err)
	require.Empty(t, flights[1].Body, "a later snapshot read failure must release the first temporary body")
}

func TestCapturedJournalClaimOwnsTheCommittedReplyUntilEncoding(t *testing.T) {
	machine, session := claimBatchFixture(t, 2)
	hot, _ := claimBatchFixture(t, 2)
	first, second := bytes.Repeat([]byte{0x21}, 64<<10), bytes.Repeat([]byte{0x43}, 64<<10)
	publish, err := EncodePublishBatch(queueName, session.Incarnation, [][]byte{first, second})
	require.NoError(t, err)
	hot.Apply(publish)
	source := &countedCommandSource{Reader: bytes.NewReader(bytes.Clone(publish))}
	_, err = machine.ApplyJournaled(publish, false, source)
	require.NoError(t, err)
	session.Sequence = 1
	claim := claimBatchRequest(t, session, 2, 1<<20)
	want := hot.Apply(claim)

	var encode func() ([]byte, error)
	var inline []byte
	if capturer, ok := any(machine).(interface {
		ApplyJournaledReply([]byte, bool, io.ReaderAt) ([]byte, func() ([]byte, error), error)
	}); ok {
		inline, encode, err = capturer.ApplyJournaledReply(claim, true, nil)
	} else {
		inline, err = machine.ApplyCommitted(claim, true)
	}
	require.NoError(t, err)
	require.Zero(t, source.reads, "capturing the committed claim must leave cold body reads for the encoder")
	require.Empty(t, inline)
	require.NotNil(t, encode)

	ack := session
	ack.Kind, ack.Sequence, ack.Attempt = AckMessage, 2, 1
	require.NoError(t, apply(t, machine, ack).Err())
	purge := QueueCommand{Kind: PurgeQueue, Queue: queueName, Incarnation: session.Incarnation}
	require.NoError(t, apply(t, machine, purge).Err())
	reply, err := encode()
	require.NoError(t, err)
	require.Equal(t, 2, source.reads)
	require.Equal(t, want, reply, "the encoder must use captured attempts and bytes after later session state")
}
