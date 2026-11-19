package clusterqueue

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

const claimBatchWireKind QueueCommandKind = 17

type claimBatchFixtureResult struct {
	Queue         QueueInfo
	Count         uint64
	Code          string
	DeliveryBatch bool
	deliveries    []Delivery
}

func claimBatchFixture(t *testing.T, prefetch uint32, bodies ...[]byte) (*Machine, QueueCommand) {
	t.Helper()
	m := &Machine{}
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, declared.Err())
	session := QueueCommand{Queue: queueName, Incarnation: declared.Queue.Incarnation, Session: 1, Owner: "batch-reader"}
	open := session
	open.Kind, open.Prefetch, open.Consumer, open.Deadline = OpenQueueSession, prefetch, true, 10
	require.NoError(t, apply(t, m, open).Err())
	for _, body := range bodies {
		require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: body}).Err())
	}
	return m, session
}

// Construct the proposal independently of the production command encoder so
// the baseline test reaches Machine.Apply with a complete ordered request.
func claimBatchRequest(t *testing.T, session QueueCommand, limit, maxBytes uint32) []byte {
	t.Helper()
	session.Kind = claimBatchWireKind
	wire, err := encodeFrame(struct {
		QueueCommand
		ClaimLimit uint32
		ClaimBytes uint32
	}{session, limit, maxBytes}, nil)
	require.NoError(t, err)
	return wire
}

func decodeClaimBatchFixture(t *testing.T, wire []byte) claimBatchFixtureResult {
	t.Helper()
	var result claimBatchFixtureResult
	body, err := decodeFrame(wire, &result)
	require.NoError(t, err)
	if result.Code != "" {
		require.Zero(t, result.Count)
		require.Empty(t, body)
		require.False(t, result.DeliveryBatch)
		return result
	}
	require.True(t, result.DeliveryBatch)
	require.Positive(t, result.Count)
	require.LessOrEqual(t, result.Count, uint64(32))
	for range result.Count {
		const headerSize = 21
		require.GreaterOrEqual(t, len(body), headerSize)
		id := MessageID(binary.BigEndian.Uint64(body[:8]))
		attempt := AttemptID(binary.BigEndian.Uint64(body[8:16]))
		flags := body[16]
		size := binary.BigEndian.Uint32(body[17:headerSize])
		require.LessOrEqual(t, flags, byte(1))
		require.LessOrEqual(t, uint64(size), uint64(len(body)-headerSize))
		next := headerSize + int(size)
		var deliveryBody []byte
		if size != 0 {
			deliveryBody = body[headerSize:next]
		}
		result.deliveries = append(result.deliveries, Delivery{
			MessageID: id, Attempt: attempt, Redelivered: flags == 1, Body: deliveryBody,
		})
		body = body[next:]
	}
	require.Empty(t, body)
	return result
}

func (r claimBatchFixtureResult) operationError() error {
	return (QueueResponse{Code: r.Code}).Err()
}

func TestClaimBatchRespectsRemainingCreditAndReplaysAfterRestore(t *testing.T) {
	first, raw, tail := []byte("first"), []byte{0xff, 0, 0x80}, []byte("tail")
	m, session := claimBatchFixture(t, 3, first, raw, nil, tail)
	single := session
	single.Kind, single.Sequence = ClaimMessage, 1
	original := apply(t, m, single)
	require.NoError(t, original.Err())
	require.Equal(t, first, original.Result.Delivery.Body)

	session.Sequence = 2
	request := claimBatchRequest(t, session, 32, 1<<20)
	wire := m.Apply(request)
	batch := decodeClaimBatchFixture(t, wire)
	require.NoError(t, batch.operationError())
	require.EqualValues(t, 2, batch.Count)
	require.EqualValues(t, 1, batch.Queue.Ready)
	require.Equal(t, []Delivery{
		{MessageID: 2, Attempt: original.Result.Delivery.Attempt + 1, Body: raw},
		{MessageID: 3, Attempt: original.Result.Delivery.Attempt + 2},
	}, batch.deliveries)

	// Apply consumed the request before its reply was lost.
	require.Equal(t, wire, m.Apply(request))
	restored := &Machine{}
	require.NoError(t, restored.Restore(mustSnapshot(t, m)))
	require.Equal(t, wire, restored.Apply(request))
	conflict := decodeClaimBatchFixture(t, restored.Apply(claimBatchRequest(t, session, 31, 1<<20)))
	require.ErrorIs(t, conflict.operationError(), ErrSequenceConflict)
	conflict = decodeClaimBatchFixture(t, restored.Apply(claimBatchRequest(t, session, 32, (1<<20)-1)))
	require.ErrorIs(t, conflict.operationError(), ErrSequenceConflict)
	session.Sequence++
	full := decodeClaimBatchFixture(t, restored.Apply(claimBatchRequest(t, session, 32, 1<<20)))
	require.ErrorIs(t, full.operationError(), ErrNoCredit)
	info := apply(t, restored, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.EqualValues(t, 3, info.Queue.Unacknowledged)
	require.EqualValues(t, 1, info.Queue.Ready)
}

func TestClaimBatchHonorsByteBudgetAndAdmitsOversizedFirst(t *testing.T) {
	m, session := claimBatchFixture(t, 9, []byte("abc"), []byte("def"), []byte("large!"), []byte("z"))
	for i, want := range []string{"abc", "def", "large!", "z"} {
		session.Sequence++
		batch := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, 3, 5)))
		require.NoError(t, batch.operationError())
		require.EqualValues(t, 1, batch.Count, "after the first oversized message, the batch stops")
		require.Equal(t, want, string(batch.deliveries[0].Body))
		require.EqualValues(t, i+1, batch.deliveries[0].MessageID)
		require.EqualValues(t, 3-i, batch.Queue.Ready)
	}
}

func TestClaimBatchExpiresUndeliveredFlightsInOrder(t *testing.T) {
	m, session := claimBatchFixture(t, 8, []byte("a"), []byte("b"), []byte("c"), []byte("d"))
	session.Sequence = 1
	batch := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, 2, 100)))
	require.NoError(t, batch.operationError())
	require.EqualValues(t, 2, batch.Count, "request count limits a batch below available credit")
	require.Equal(t, "a", string(batch.deliveries[0].Body))
	require.Equal(t, "b", string(batch.deliveries[1].Body))
	require.Zero(t, apply(t, m, QueueCommand{Kind: ExpireSessions, Deadline: 9}).Count)
	expired := apply(t, m, QueueCommand{Kind: ExpireSessions, Deadline: 10})
	require.NoError(t, expired.Err())
	require.EqualValues(t, 1, expired.Count)
	closed := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, 2, 100)))
	require.ErrorIs(t, closed.operationError(), ErrSessionClosed)

	open := session
	open.Kind, open.Session, open.Owner, open.Sequence, open.Prefetch, open.Deadline = OpenQueueSession, 2, "replacement", 0, 4, 20
	require.NoError(t, apply(t, m, open).Err())
	single := QueueCommand{Kind: ClaimMessage, Queue: queueName, Incarnation: session.Incarnation, Session: 2, Owner: open.Owner}
	for i, want := range []string{"c", "d", "a", "b"} {
		single.Sequence++
		result := apply(t, m, single)
		require.NoError(t, result.Err())
		require.Equal(t, want, string(result.Result.Delivery.Body))
		require.Equal(t, i >= 2, result.Result.Delivery.Redelivered)
		require.Greater(t, result.Result.Delivery.Attempt, batch.deliveries[1].Attempt)
	}
}

func TestClaimBatchRejectsInvalidRequestBoundsBeforeMutation(t *testing.T) {
	bodies := make([][]byte, 33)
	for i := range bodies {
		bodies[i] = []byte{byte(i)}
	}
	m, session := claimBatchFixture(t, uint32(len(bodies)), bodies...)
	session.Sequence = 1
	for _, limits := range [][2]uint32{{0, 1}, {33, 100}, {1, 0}, {1, (1 << 20) + 1}} {
		invalid := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, limits[0], limits[1])))
		require.ErrorIs(t, invalid.operationError(), ErrInvalidCommand)
	}
	batch := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, 32, 1<<20)))
	require.NoError(t, batch.operationError())
	require.EqualValues(t, 32, batch.Count)
	require.EqualValues(t, 1, batch.Queue.Ready)
	for i, delivery := range batch.deliveries {
		require.Equal(t, bodies[i], delivery.Body)
	}
}

func TestClaimBatchCommandAndResponseCodec(t *testing.T) {
	body := []byte{0xff, 0, 0x80}
	m, command := claimBatchFixture(t, 2, body, nil)
	command.Kind, command.Sequence, command.ClaimLimit, command.ClaimBytes = ClaimBatch, 1, 2, 32
	request, err := EncodeCommand(command)
	require.NoError(t, err)
	decoded, err := DecodeCommand(request)
	require.NoError(t, err)
	require.Equal(t, command, decoded)

	wire := m.Apply(request)
	response, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.NoError(t, response.Err())
	require.True(t, response.DeliveryBatch)
	require.EqualValues(t, 2, response.Count)
	require.Equal(t, []Delivery{{MessageID: 1, Attempt: 1, Body: body}, {MessageID: 2, Attempt: 2}}, response.Deliveries)

	var metadata QueueResponse
	records, err := decodeFrame(wire, &metadata)
	require.NoError(t, err)
	truncated, err := encodeFrame(metadata, records[:len(records)-1])
	require.NoError(t, err)
	invalid, err := DecodeResponse(truncated)
	require.ErrorIs(t, err, ErrInvalidCommand)
	require.Equal(t, QueueResponse{}, invalid)
}
