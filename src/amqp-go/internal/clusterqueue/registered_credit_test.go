package clusterqueue

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyQueueCommandIgnoresUnknownAssignmentMetadata(t *testing.T) {
	machine := &Machine{}
	command, err := EncodeCommand(QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, err)
	declared, err := DecodeResponse(machine.Apply(command))
	require.NoError(t, err)
	require.NoError(t, declared.Err())

	legacy, err := encodeFrame(struct {
		QueueCommand
		AutoAssign json.RawMessage
	}{
		QueueCommand: QueueCommand{
			Kind: OpenQueueSession, Queue: queueName, Incarnation: declared.Queue.Incarnation,
			Session: 1, Owner: "legacy", Prefetch: 1, Consumer: true, Deadline: 1,
		},
		AutoAssign: json.RawMessage(`{"unknown": true}`),
	}, nil)
	require.NoError(t, err)
	opened, err := DecodeResponse(machine.Apply(legacy))
	require.NoError(t, err)
	require.NoError(t, opened.Err(), "legacy commands must ignore unknown JSON fields")
	require.False(t, machine.state.Queues[queueName].Sessions[1].AutoAssign)
}

// This test-only interface keeps the baseline compilable without implementing
// the proposed API in a fixture. Reads expose committed outstanding assignments
// after an exclusive attempt cursor, using ClaimLimit/ClaimBytes only as reply
// bounds. They neither reserve messages nor advance the settlement sequence.
type registeredAssignmentReader interface {
	ReadAssignments(QueueCommand, AttemptID) ([]Delivery, AttemptID, error)
}

func readRegisteredAssignments(t *testing.T, m *Machine, session QueueCommand, after AttemptID) ([]Delivery, AttemptID) {
	t.Helper()
	reader, ok := any(m).(registeredAssignmentReader)
	require.True(t, ok, "the machine must expose a non-mutating cursor read of committed assignments")
	request := QueueCommand{Queue: session.Queue, Incarnation: session.Incarnation,
		Session: session.Session, Owner: session.Owner, ClaimLimit: 1, ClaimBytes: MaxClaimBatchBytes}
	before := mustSnapshot(t, m)
	deliveries, next, err := reader.ReadAssignments(request, after)
	require.NoError(t, err)
	require.Equal(t, before, mustSnapshot(t, m), "reading assignments must not change ownership, credit, history, or cursor state")
	return deliveries, next
}

func TestSettlementAssignmentPageDoesNotChangeReplay(t *testing.T) {
	m := new(NewAssignmentMachine())
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, declared.Err())
	open := QueueCommand{
		Kind: OpenQueueSession, Queue: queueName, Incarnation: declared.Queue.Incarnation,
		Session: 1, Owner: "registered", Prefetch: 2, Consumer: true, AutoAssign: true, Deadline: 100,
	}
	require.NoError(t, apply(t, m, open).Err())
	for _, body := range []string{"first", "already assigned", "replacement", "still queued"} {
		require.NoError(t, apply(t, m, QueueCommand{
			Kind: PublishMessage, Queue: queueName, Incarnation: open.Incarnation, Body: []byte(body),
		}).Err())
	}
	before := mustSnapshot(t, m)
	first, after := readRegisteredAssignments(t, m, open, 0)
	require.Len(t, first, 1)
	ack := QueueCommand{
		Kind: AckMessage, Queue: queueName, Incarnation: open.Incarnation, Session: open.Session,
		Owner: open.Owner, Sequence: 1, Attempt: first[0].Attempt,
	}
	control := new(NewAssignmentMachine())
	require.NoError(t, control.Restore(before))
	controlReply := apply(t, control, ack)
	require.NoError(t, controlReply.Err())
	require.Nil(t, controlReply.AssignmentAfter)

	ack.AssignmentAfter = &after
	req := AssignmentRequest{
		Queue: ack.Queue, Incarnation: ack.Incarnation, Session: ack.Session, Owner: ack.Owner,
		After: after + 1, Limit: 1, Bytes: MaxClaimBatchBytes,
	}
	check := func(reply QueueResponse) {
		t.Helper()
		require.NoError(t, reply.Err())
		require.Equal(t, &req.After, reply.AssignmentAfter)
		deliveries, through, err := DecodeAssignmentPage(reply.AssignmentPage, req)
		require.NoError(t, err)
		require.Len(t, deliveries, 1)
		require.Equal(t, []byte("replacement"), deliveries[0].Body)
		require.Greater(t, through, after)
		reply.AssignmentAfter, reply.AssignmentPage = nil, nil
		require.Equal(t, controlReply, reply)
	}
	check(apply(t, m, ack))
	afterAck := mustSnapshot(t, m)
	require.Equal(t, mustSnapshot(t, control), afterAck, "the reply page must have no Raft state")
	reopened := new(NewAssignmentMachine())
	require.NoError(t, reopened.Restore(afterAck))
	require.Equal(t, controlReply, apply(t, reopened, ack), "a retry can fetch the page separately")
	require.Equal(t, afterAck, mustSnapshot(t, reopened), "retry must not settle or assign again")
}

func TestRegisteredConsumerCreditAssignsOnPublishAndAckWithoutClaims(t *testing.T) {
	m := new(NewAssignmentMachine())
	var committed []QueueCommand
	commit := func(wire []byte, wantReply bool) []byte {
		t.Helper()
		command, err := DecodeCommand(wire)
		require.NoError(t, err)
		committed = append(committed, command)
		reply, err := m.ApplyCommitted(wire, wantReply)
		require.NoError(t, err)
		if wantReply {
			result, err := DecodeResponse(reply)
			require.NoError(t, err)
			require.NoError(t, result.Err())
		} else {
			require.Nil(t, reply)
		}
		return reply
	}
	encode := func(command QueueCommand) []byte {
		t.Helper()
		wire, err := EncodeCommand(command)
		require.NoError(t, err)
		return wire
	}
	commit(encode(QueueCommand{Kind: DeclareQueue, Queue: queueName}), true)
	incarnation := m.state.Queues[queueName].ID
	registered := QueueCommand{Queue: queueName, Incarnation: incarnation, Session: 1, Owner: "registered"}
	pull := QueueCommand{Queue: queueName, Incarnation: incarnation, Session: 2, Owner: "legacy-pull"}
	for _, tc := range []struct {
		session QueueCommand
		auto    bool
	}{
		{registered, true},
		{pull, false},
	} {
		open := tc.session
		open.Kind, open.Consumer, open.Prefetch, open.Deadline = OpenQueueSession, true, 2, 100
		open.AutoAssign = tc.auto
		commit(encode(open), true)
	}
	commit(encode(QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: incarnation, Body: []byte("a")}), true)
	batch, err := EncodePublishBatch(queueName, incarnation, [][]byte{[]byte("b"), []byte("c"), []byte("d")})
	require.NoError(t, err)
	commit(batch, false) // Follower/no-waiter apply must perform the same assignments.

	q := m.state.Queues[queueName]
	require.EqualValues(t, 4, q.LastMessage, "both publish commits applied before inspecting assignment")
	require.Len(t, q.Sessions[registered.Session].Flights, 2,
		"publish commits must fill registered credit without a claim command or cursor read")
	require.EqualValues(t, 2, q.unacked)
	require.Equal(t, []messageState{{ID: 3, Body: []byte("c")}, {ID: 4, Body: []byte("d")}}, q.Ready)
	require.Equal(t, messageState{ID: 1, Body: []byte("a")}, q.Sessions[registered.Session].Flights[1])
	require.Equal(t, messageState{ID: 2, Body: []byte("b")}, q.Sessions[registered.Session].Flights[2])
	require.Empty(t, q.Sessions[pull.Session].Flights, "Consumer=true alone must preserve the old pull session")
	require.Zero(t, q.Sessions[registered.Session].Sequence, "automatic assignments do not consume the ACK lane")

	first, cursor := readRegisteredAssignments(t, m, registered, 0)
	require.Equal(t, []Delivery{{MessageID: 1, Attempt: 1, Body: []byte("a")}}, first)
	require.EqualValues(t, 1, cursor, "a bounded reply advances only through its returned prefix")
	second, cursor := readRegisteredAssignments(t, m, registered, cursor)
	require.Equal(t, []Delivery{{MessageID: 2, Attempt: 2, Body: []byte("b")}}, second)
	require.EqualValues(t, 2, cursor)
	empty, unchanged := readRegisteredAssignments(t, m, registered, cursor)
	require.Empty(t, empty)
	require.Equal(t, cursor, unchanged)

	ack := registered
	ack.Kind, ack.Sequence, ack.Attempt = AckMessage, 1, first[0].Attempt
	ackWire := encode(ack)
	ackReply := commit(ackWire, true)
	q = m.state.Queues[queueName]
	require.EqualValues(t, 1, q.Sessions[registered.Session].Sequence)
	require.EqualValues(t, 3, q.LastAttempt, "successful ACK commit must reuse the released credit immediately")
	require.Equal(t, messageState{ID: 3, Body: []byte("c")}, q.Sessions[registered.Session].Flights[3])
	require.NotContains(t, q.Sessions[registered.Session].Flights, AttemptID(1))
	require.Len(t, q.Sessions[registered.Session].Flights, 2)
	require.Equal(t, []messageState{{ID: 4, Body: []byte("d")}}, q.Ready)

	// The ACK committed and assigned c, but the serving process lost its
	// reply. Recovery must retain the ACK result and the same assignment IDs.
	state := mustSnapshot(t, m)
	m = new(NewAssignmentMachine())
	require.NoError(t, m.Restore(state))
	require.Equal(t, ackReply, commit(ackWire, true))
	require.Equal(t, state, mustSnapshot(t, m), "ACK replay must not free or refill credit twice")
	third, cursor := readRegisteredAssignments(t, m, registered, cursor)
	require.Equal(t, []Delivery{{MessageID: 3, Attempt: 3, Body: []byte("c")}}, third)
	require.EqualValues(t, 3, cursor)

	// A legacy pull reserves d explicitly. Its queue-wide attempt creates a
	// gap in the registered stream; it must not move that stream's watermark.
	legacyClaim := pull
	legacyClaim.Kind, legacyClaim.Sequence = ClaimMessage, 1
	pulled, err := DecodeResponse(commit(encode(legacyClaim), true))
	require.NoError(t, err)
	require.Equal(t, Delivery{MessageID: 4, Attempt: 4, Body: []byte("d")}, pulled.Result.Delivery)
	ack.Sequence, ack.Attempt = 2, second[0].Attempt
	commit(encode(ack), true)
	ack.Sequence, ack.Attempt = 3, third[0].Attempt
	latestAckWire := encode(ack)
	latestAckReply := commit(latestAckWire, true)
	q = m.state.Queues[queueName]
	require.Empty(t, q.Sessions[registered.Session].Flights)
	require.EqualValues(t, 4, q.LastAttempt)
	require.Len(t, q.Sessions[pull.Session].Flights, 1)

	state = mustSnapshot(t, m)
	m = new(NewAssignmentMachine())
	require.NoError(t, m.Restore(state))
	empty, recoveredCursor := readRegisteredAssignments(t, m, registered, 0)
	require.Empty(t, empty)
	require.Equal(t, cursor, recoveredCursor,
		"an empty registered stream must restore its own durable watermark, not zero or the queue-wide attempt")
	commit(encode(QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: incarnation, Body: []byte("e")}), false)
	q = m.state.Queues[queueName]
	require.EqualValues(t, 3, q.Sessions[registered.Session].Sequence, "publish-side assignment must leave the latest ACK replayable")
	require.Equal(t, messageState{ID: 5, Body: []byte("e")}, q.Sessions[registered.Session].Flights[5])
	require.EqualValues(t, 5, q.LastAttempt)
	state = mustSnapshot(t, m)
	require.Equal(t, latestAckReply, commit(latestAckWire, true))
	require.Equal(t, state, mustSnapshot(t, m), "an old ACK reply remains exact after independent publish assignments")
	fifth, next := readRegisteredAssignments(t, m, registered, recoveredCursor)
	require.Equal(t, []Delivery{{MessageID: 5, Attempt: 5, Body: []byte("e")}}, fifth)
	require.EqualValues(t, 5, next, "cursor traversal skips another session's attempt without inventing assignments")
	require.Zero(t, len(q.Ready))
	require.EqualValues(t, 2, q.unacked)
	require.EqualValues(t, 2, m.state.messages, "only the legacy flight and registered flight retain message ownership")

	var kinds []QueueCommandKind
	for _, command := range committed {
		kinds = append(kinds, command.Kind)
		if command.Session == registered.Session {
			require.NotContains(t, []QueueCommandKind{ClaimMessage, ClaimBatch, AckAndClaim}, command.Kind)
		}
	}
	require.Equal(t, []QueueCommandKind{
		DeclareQueue, OpenQueueSession, OpenQueueSession, PublishMessage, PublishBatch,
		AckMessage, AckMessage, ClaimMessage, AckMessage, AckMessage, PublishMessage, AckMessage,
	}, kinds, "the only added transitions are ordinary publish and ordered ACK commits, including their exact retries")
}
