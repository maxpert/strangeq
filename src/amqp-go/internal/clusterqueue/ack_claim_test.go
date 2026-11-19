package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// The next queue opcode is encoded independently so the baseline reaches
// Machine.Apply and fails on behavior, without requiring production symbols.
const ackAndClaimWireKind QueueCommandKind = ClaimBatch + 1

type ackClaimFixtureResult struct {
	Queue         QueueInfo
	Count         uint64 // Claimed deliveries, independently of Settled.
	Settled       uint64
	Code          string // Settlement or session failure.
	ClaimCode     string // Refill failure after successful settlement.
	DeliveryBatch bool
	deliveries    []Delivery
}

func ackClaimFixture(t *testing.T, prefetch, initial uint32, bodies ...[]byte) (*Machine, QueueCommand, []Delivery) {
	t.Helper()
	m, session := claimBatchFixture(t, prefetch, bodies...)
	session.Sequence = 1
	claimed := decodeClaimBatchFixture(t, m.Apply(claimBatchRequest(t, session, initial, MaxClaimBatchBytes)))
	require.NoError(t, claimed.operationError())
	require.EqualValues(t, initial, claimed.Count)
	session.Sequence++
	return m, session, claimed.deliveries
}

func ackClaimRequest(t *testing.T, session QueueCommand, attempts []AttemptID, limit, maxBytes uint32) []byte {
	t.Helper()
	session.Kind = ackAndClaimWireKind
	session.Attempts = attempts
	session.ClaimLimit, session.ClaimBytes = limit, maxBytes
	wire, err := encodeFrame(session, nil)
	require.NoError(t, err)
	return wire
}

func decodeAckClaimFixture(t *testing.T, wire []byte) ackClaimFixtureResult {
	t.Helper()
	var result ackClaimFixtureResult
	body, err := decodeFrame(wire, &result)
	require.NoError(t, err)
	require.LessOrEqual(t, result.Count, uint64(MaxClaimBatchMessages))
	require.LessOrEqual(t, result.Settled, uint64(MaxSettlementBatch))
	if result.Count == 0 {
		require.False(t, result.DeliveryBatch)
		require.Empty(t, body)
		return result
	}
	require.True(t, result.DeliveryBatch)
	for range result.Count {
		const recordBytes = 21
		require.GreaterOrEqual(t, len(body), recordBytes)
		id := MessageID(binary.BigEndian.Uint64(body[:8]))
		attempt := AttemptID(binary.BigEndian.Uint64(body[8:16]))
		flags := body[16]
		size := binary.BigEndian.Uint32(body[17:recordBytes])
		require.NotZero(t, id)
		require.NotZero(t, attempt)
		require.LessOrEqual(t, flags, byte(1))
		require.LessOrEqual(t, uint64(size), uint64(len(body)-recordBytes))
		end := recordBytes + int(size)
		var payload []byte
		if size != 0 {
			payload = body[recordBytes:end]
		}
		result.deliveries = append(result.deliveries, Delivery{
			MessageID: id, Attempt: attempt, Body: payload, Redelivered: flags == 1,
		})
		body = body[end:]
	}
	require.Empty(t, body)
	return result
}

func TestAckAndClaimRefillsOnlyFreedCreditInQueueOrder(t *testing.T) {
	m, session, flights := ackClaimFixture(t, 3, 3,
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"))
	request := ackClaimRequest(t, session, []AttemptID{flights[2].Attempt, flights[0].Attempt}, MaxClaimBatchMessages, MaxClaimBatchBytes)
	result := decodeAckClaimFixture(t, m.Apply(request))
	require.NoError(t, (QueueResponse{Code: result.Code}).Err(), "one command must settle the requested ACKs and refill")
	require.Empty(t, result.ClaimCode)
	require.EqualValues(t, 2, result.Settled)
	require.EqualValues(t, 2, result.Count, "the remaining old flight still consumes one credit")
	require.EqualValues(t, 1, result.Queue.Ready)
	require.Equal(t, []Delivery{
		{MessageID: 4, Attempt: flights[2].Attempt + 1, Body: []byte("d")},
		{MessageID: 5, Attempt: flights[2].Attempt + 2, Body: []byte("e")},
	}, result.deliveries)
	info := apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.EqualValues(t, 3, info.Queue.Unacknowledged)
	require.EqualValues(t, 1, info.Queue.Ready)
}

func TestAckAndClaimUnknownAttemptKeepsOnlySuccessfulPrefixWithoutRefill(t *testing.T) {
	for _, unknownFirst := range []bool{false, true} {
		name := "unknown_after_success"
		if unknownFirst {
			name = "unknown_first"
		}
		t.Run(name, func(t *testing.T) {
			m, session, flights := ackClaimFixture(t, 3, 3,
				[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
			unknown := flights[2].Attempt + 100
			attempts := []AttemptID{flights[2].Attempt, unknown, flights[0].Attempt}
			wantSettled := uint64(1)
			if unknownFirst {
				attempts[0], attempts[1] = attempts[1], attempts[0]
				wantSettled = 0
			}
			request := ackClaimRequest(t, session, attempts, 3, MaxClaimBatchBytes)
			wire := m.Apply(request)
			result := decodeAckClaimFixture(t, wire)
			require.ErrorIs(t, (QueueResponse{Code: result.Code}).Err(), ErrUnknownAttempt)
			require.Equal(t, wantSettled, result.Settled, "settlement follows request order and stops at the failure")
			require.Zero(t, result.Count, "a partial ACK prefix must not trigger refill")
			require.Empty(t, result.ClaimCode, "refill was not attempted")
			require.Empty(t, result.deliveries)
			info := apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName})
			require.EqualValues(t, 3-wantSettled, info.Queue.Unacknowledged)
			require.EqualValues(t, 2, info.Queue.Ready)

			state := mustSnapshot(t, m)
			restored := &Machine{}
			require.NoError(t, restored.Restore(state))
			require.Equal(t, wire, restored.Apply(request), "retry must retain the original successful prefix")
			require.Equal(t, state, mustSnapshot(t, restored))

			// These legacy ACKs establish which identities survived, rather
			// than checking only aggregate counters after the prefix failure.
			ack := session
			ack.Kind, ack.Attempt = AckMessage, flights[2].Attempt
			ack.Sequence++
			third := apply(t, restored, ack)
			if unknownFirst {
				require.NoError(t, third.Err())
			} else {
				require.ErrorIs(t, third.Err(), ErrUnknownAttempt)
			}
			ack.Sequence++
			ack.Attempt = flights[0].Attempt
			require.NoError(t, apply(t, restored, ack).Err(), "the attempt after the failed entry remains outstanding")
		})
	}
}

func TestAckAndClaimEmptyRefillDoesNotTurnSuccessfulAckIntoFailure(t *testing.T) {
	m, session, flights := ackClaimFixture(t, 1, 1, []byte("only message"))
	request := ackClaimRequest(t, session, []AttemptID{flights[0].Attempt}, 1, MaxClaimBatchBytes)
	wire := m.Apply(request)
	result := decodeAckClaimFixture(t, wire)
	require.NoError(t, (QueueResponse{Code: result.Code}).Err(), "an empty refill cannot fail the completed ACK")
	require.EqualValues(t, 1, result.Settled)
	require.Zero(t, result.Count)
	require.ErrorIs(t, (QueueResponse{Code: result.ClaimCode}).Err(), ErrEmpty)
	require.Zero(t, result.Queue.Ready)
	info := apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.Zero(t, info.Queue.Unacknowledged)
	require.Zero(t, info.Queue.Ready)
	restored := &Machine{}
	require.NoError(t, restored.Restore(mustSnapshot(t, m)))
	require.Equal(t, wire, restored.Apply(request), "empty refill and successful settlement replay together")
}

func TestAckAndClaimReplayIncludesOrderedAttemptsAndClaimLimits(t *testing.T) {
	m, session, flights := ackClaimFixture(t, 3, 3,
		[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"))
	attempts := []AttemptID{flights[2].Attempt, flights[0].Attempt}
	request := ackClaimRequest(t, session, attempts, 2, 9)
	wire := m.Apply(request)
	result := decodeAckClaimFixture(t, wire)
	require.NoError(t, (QueueResponse{Code: result.Code}).Err())
	require.EqualValues(t, 2, result.Settled)
	require.EqualValues(t, 2, result.Count)
	require.Equal(t, []byte("d"), result.deliveries[0].Body)
	require.Equal(t, []byte("e"), result.deliveries[1].Body)

	// The operation committed but its original response was lost. Recovery
	// must reconstruct the exact settled count, attempts, and raw reply bytes.
	state := mustSnapshot(t, m)
	restored := &Machine{}
	require.NoError(t, restored.Restore(state))
	require.Equal(t, wire, restored.Apply(request))
	require.Equal(t, state, mustSnapshot(t, restored))

	for _, tc := range []struct {
		name     string
		attempts []AttemptID
		limit    uint32
		maxBytes uint32
	}{
		{"attempt_order", []AttemptID{attempts[1], attempts[0]}, 2, 9},
		{"attempt_identity", []AttemptID{attempts[0], flights[1].Attempt}, 2, 9},
		{"attempt_count", attempts[:1], 2, 9},
		{"claim_limit", attempts, 1, 9},
		{"claim_bytes", attempts, 2, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conflict := decodeAckClaimFixture(t, restored.Apply(ackClaimRequest(t, session, tc.attempts, tc.limit, tc.maxBytes)))
			require.ErrorIs(t, (QueueResponse{Code: conflict.Code}).Err(), ErrSequenceConflict)
			require.Zero(t, conflict.Settled)
			require.Zero(t, conflict.Count)
			require.Equal(t, state, mustSnapshot(t, restored), "a conflicting retry must not mutate either half")
			require.Equal(t, wire, restored.Apply(request))
		})
	}
}

func TestAckAndClaimReplyElisionKeepsPayloadOwnershipAndReplay(t *testing.T) {
	body := bytes.Repeat([]byte{0xff, 0, 0x80}, 1<<15)
	origin, session, flights := ackClaimFixture(t, 2, 1, []byte("acknowledged"), body, nil)
	request := ackClaimRequest(t, session, []AttemptID{flights[0].Attempt}, 2, MaxClaimBatchBytes)
	before := mustSnapshot(t, origin)
	originalPayload := origin.state.Queues[queueName].Ready[0].Body
	wire := origin.Apply(request)
	result := decodeAckClaimFixture(t, wire)
	require.NoError(t, (QueueResponse{Code: result.Code}).Err())
	require.EqualValues(t, 1, result.Settled)
	require.EqualValues(t, 2, result.Count)
	require.Equal(t, []Delivery{
		{MessageID: 2, Attempt: flights[0].Attempt + 1, Body: body},
		{MessageID: 3, Attempt: flights[0].Attempt + 2},
	}, result.deliveries)
	state := mustSnapshot(t, origin)
	claimed := origin.state.Queues[queueName].Sessions[session.Session].Flights[result.deliveries[0].Attempt]
	require.Same(t, &originalPayload[0], &claimed.Body[0], "claim transfers the existing payload into flight without cloning")

	for _, publicEntry := range []bool{false, true} {
		name := "encoder_gate"
		if publicEntry {
			name = "public_committed_entry"
		}
		t.Run(name, func(t *testing.T) {
			other := &Machine{}
			require.NoError(t, other.Restore(before))
			payload := other.state.Queues[queueName].Ready[0].Body
			if publicEntry {
				suppressed, err := other.ApplyCommitted(request, false)
				require.NoError(t, err)
				require.Nil(t, suppressed)
			} else {
				encodes := 0
				suppressed, err := other.applyCommitted(request, false, nil, func(response QueueResponse) ([]byte, error) {
					encodes++
					return encodeQueueReply(response)
				})
				require.NoError(t, err)
				require.Nil(t, suppressed)
				require.Zero(t, encodes, "an absent waiter must not serialize the refill payload")
			}
			require.Equal(t, state, mustSnapshot(t, other), "reply elision must preserve settlement, claims, and session history")
			claimed := other.state.Queues[queueName].Sessions[session.Session].Flights[result.deliveries[0].Attempt]
			require.Same(t, &payload[0], &claimed.Body[0], "reply elision must not clone the ready payload into flight")
			require.Equal(t, wire, other.Apply(request), "the lost response is reconstructed exactly on replay")
			require.Equal(t, state, mustSnapshot(t, other))
		})
	}

	// A borrowed decoder result must not force another payload clone. The
	// production decoder can expose the same raw record payload as this parser.
	decoded, err := DecodeResponse(wire)
	require.NoError(t, err)
	require.Equal(t, result.deliveries, decoded.Deliveries)
	require.Same(t, &result.deliveries[0].Body[0], &decoded.Deliveries[0].Body[0])
}
