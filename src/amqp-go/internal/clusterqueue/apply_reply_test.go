package clusterqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyCommittedDoesNotEncodeClaimBodiesWithoutWaiter(t *testing.T) {
	first, second := []byte("first body"), []byte{0x00, 0xff, 0x20}
	origin, session := claimBatchFixture(t, 2, first, second)
	other := &Machine{}
	require.NoError(t, other.Restore(mustSnapshot(t, origin)))
	session.Sequence = 1
	request := claimBatchRequest(t, session, 2, 1<<20)

	encoded := 0
	encoder := func(response QueueResponse) ([]byte, error) {
		encoded++
		return encodeQueueReply(response)
	}
	suppressed, err := other.applyCommitted(request, false, nil, encoder)
	require.NoError(t, err)
	require.Zero(t, encoded, "no local caller means no delivery body encoding")
	require.Empty(t, suppressed)
	wire, err := origin.applyCommitted(request, true, nil, encoder)
	require.NoError(t, err)
	require.Equal(t, 1, encoded)
	decoded, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.NoError(t, decoded.Err())
	require.Len(t, decoded.Deliveries, 2)
	require.Equal(t, first, decoded.Deliveries[0].Body)
	require.Equal(t, second, decoded.Deliveries[1].Body)

	state := mustSnapshot(t, origin)
	require.Equal(t, state, mustSnapshot(t, other), "replicas keep the same claims and session history")
	replayed, err := other.applyCommitted(request, true, nil, encoder)
	require.NoError(t, err)
	require.Equal(t, wire, replayed, "replaying the session command recovers exact body and attempt bytes")
	require.Equal(t, 2, encoded)
	require.Equal(t, state, mustSnapshot(t, other), "replay does not claim the messages again")
}
