package clusterqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const queueName = "/integration"

func TestQueueMachineFencesAttemptsAcrossReplayAndRestore(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	id := q.Queue.Incarnation
	require.NotZero(t, id)
	a := QueueCommand{Queue: queueName, Incarnation: id, Session: 1, Owner: "owner-a", Prefetch: 1, Consumer: true, Deadline: 10}
	a.Kind = OpenQueueSession
	require.NoError(t, apply(t, m, a).Err())
	b := a
	b.Session, b.Owner = 2, "owner-b"
	require.NoError(t, apply(t, m, b).Err())
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("first")}).Err())
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: id, Body: []byte("second")}).Err())
	a.Kind, a.Sequence = ClaimMessage, 1
	first := apply(t, m, a)
	require.NoError(t, first.Err())
	require.Equal(t, "first", string(first.Result.Delivery.Body))
	require.EqualValues(t, 1, first.Queue.Ready)
	require.Equal(t, first, apply(t, m, a))

	before, err := m.Snapshot()
	require.NoError(t, err)
	restored := &Machine{}
	require.NoError(t, restored.Restore(before))
	after, err := restored.Snapshot()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, first, apply(t, restored, a))

	b.Kind, b.Sequence, b.Attempt = AckMessage, 1, first.Result.Delivery.Attempt
	require.ErrorIs(t, apply(t, restored, b).Err(), ErrUnknownAttempt)
	a.Kind, a.Sequence, a.Attempt = RequeueMessage, 2, first.Result.Delivery.Attempt
	require.NoError(t, apply(t, restored, a).Err())
	b.Kind, b.Sequence = ClaimMessage, 2
	second := apply(t, restored, b)
	require.NoError(t, second.Err())
	require.Equal(t, "second", string(second.Result.Delivery.Body))
	b.Kind, b.Sequence, b.Attempt = AckMessage, 3, second.Result.Delivery.Attempt
	require.NoError(t, apply(t, restored, b).Err())
	b.Kind, b.Sequence = ClaimMessage, 4
	redelivery := apply(t, restored, b)
	require.NoError(t, redelivery.Err())
	require.Equal(t, first.Result.Delivery.MessageID, redelivery.Result.Delivery.MessageID)
	require.NotEqual(t, first.Result.Delivery.Attempt, redelivery.Result.Delivery.Attempt)
	require.True(t, redelivery.Result.Delivery.Redelivered)
}

func TestQueueMachineExpiryAndRecreation(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	id := q.Queue.Incarnation
	session := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: id, Session: 1, Owner: "owner", Prefetch: 1, Consumer: true, Deadline: 10}
	require.NoError(t, apply(t, m, session).Err())
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("lease")}).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	claimed := apply(t, m, session)
	require.NoError(t, claimed.Err())
	session.Kind, session.Deadline = RenewQueueSession, 20
	require.NoError(t, apply(t, m, session).Err())
	session.Deadline = 11
	require.NoError(t, apply(t, m, session).Err())

	expire := QueueCommand{Kind: ExpireSessions, Deadline: 19}
	require.Zero(t, apply(t, m, expire).Count)
	expire.Deadline = 20
	require.EqualValues(t, 1, apply(t, m, expire).Count)
	require.Zero(t, apply(t, m, expire).Count)

	session.Kind, session.Sequence, session.Attempt = AckMessage, 2, claimed.Result.Delivery.Attempt
	require.ErrorIs(t, apply(t, m, session).Err(), ErrSessionClosed)
	deleteQueue := QueueCommand{Kind: DeleteQueue, Queue: queueName, Incarnation: id, IfEmpty: true}
	require.ErrorIs(t, apply(t, m, deleteQueue).Err(), ErrQueueNotEmpty)
	deleteQueue.IfEmpty = false
	require.EqualValues(t, 1, apply(t, m, deleteQueue).Count)
	recreated := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, recreated.Err())
	require.Greater(t, recreated.Queue.Incarnation, id)
	require.ErrorIs(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: id, Body: []byte("stale")}).Err(), ErrQueueIncarnation)
}

func TestQueueMachineOwnerAndSequencingAreDurable(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	c := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: q.Queue.Incarnation, Session: 1, Owner: "owner", Prefetch: 1, Deadline: 10}
	require.NoError(t, apply(t, m, c).Err())
	wrong := c
	wrong.Owner = "another-owner"
	require.ErrorIs(t, apply(t, m, wrong).Err(), ErrSessionConflict)
	c.Kind, c.Sequence = ClaimMessage, 1
	require.ErrorIs(t, apply(t, m, c).Err(), ErrEmpty)
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName}).Err())
	require.ErrorIs(t, apply(t, m, c).Err(), ErrEmpty)
	c.Sequence = 3
	require.ErrorIs(t, apply(t, m, c).Err(), ErrSequenceGap)
	c.Sequence = 2
	claim := apply(t, m, c)
	require.NoError(t, claim.Err())
	c.Kind = AckMessage
	require.ErrorIs(t, apply(t, m, c).Err(), ErrSequenceConflict)
	c.Sequence = 3
	c.Attempt = claim.Result.Delivery.Attempt
	require.NoError(t, apply(t, m, c).Err())
	require.NoError(t, apply(t, m, c).Err())

	snap, err := m.Snapshot()
	require.NoError(t, err)
	require.Error(t, m.Restore([]byte(`{"Version":1}`)))
	require.Equal(t, snap, mustSnapshot(t, m))
}

func mustSnapshot(t *testing.T, m *Machine) []byte {
	t.Helper()
	snap, err := m.Snapshot()
	require.NoError(t, err)
	return snap
}

func apply(t *testing.T, m *Machine, command QueueCommand) QueueResponse {
	t.Helper()
	data, err := EncodeCommand(command)
	require.NoError(t, err)
	resp, err := DecodeResponse(m.Apply(data))
	require.NoError(t, err)
	return resp
}

func TestQueueMachineCodecCarriesRawPayloadAndRejectsTruncation(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	body := []byte{0, 0xff, 0x80, 0, 0x01, 0xff}
	wire, err := EncodeCommand(QueueCommand{Kind: PublishMessage, Queue: queueName, Body: body})
	require.NoError(t, err)
	require.Equal(t, body, wire[len(wire)-len(body):])
	broken, err := DecodeResponse(m.Apply(wire[:len(wire)-1]))
	require.NoError(t, err)
	require.ErrorIs(t, broken.Err(), ErrInvalidCommand)
	q = apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.Zero(t, q.Queue.Ready)
	accepted, err := DecodeResponse(m.Apply(wire))
	require.NoError(t, err)
	require.NoError(t, accepted.Err())
	wire[len(wire)-1] = 0 // The machine owns its copy after Apply.

	c := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: q.Queue.Incarnation, Session: 1, Owner: "codec", Prefetch: 1, Deadline: 10}
	require.NoError(t, apply(t, m, c).Err())
	c.Kind, c.Sequence = ClaimMessage, 1
	wire, err = EncodeCommand(c)
	require.NoError(t, err)
	delivered := m.Apply(wire)
	require.Equal(t, body, delivered[len(delivered)-len(body):])
	msg, err := DecodeResponse(delivered)
	require.NoError(t, err)
	require.Equal(t, body, msg.Result.Delivery.Body)
	_, err = DecodeResponse(delivered[:len(delivered)-1])
	require.Error(t, err)
}

func TestQueueMachineRevocationRequeuesOnlyLiveAttemptsInOrder(t *testing.T) {
	m := &Machine{}
	q := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, q.Err())
	open := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: q.Queue.Incarnation, Session: 1, Owner: "reader", Prefetch: 8, Consumer: true, Deadline: 10}
	require.NoError(t, apply(t, m, open).Err())
	for _, body := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte(body)}).Err())
	}
	c := open
	c.Kind = ClaimMessage
	var settled AttemptID
	for i := 1; i <= 5; i++ {
		c.Sequence = uint64(i)
		delivery := apply(t, m, c)
		require.NoError(t, delivery.Err())
		if i == 2 {
			settled = delivery.Result.Delivery.Attempt
		}
	}
	settle := c
	settle.Kind, settle.Sequence, settle.Attempt = AckMessage, 6, settled
	require.NoError(t, apply(t, m, settle).Err())
	release := open
	release.Kind = ReleaseQueueSession
	require.NoError(t, apply(t, m, release).Err())
	require.NoError(t, apply(t, m, release).Err())

	c = open
	c.Session, c.Owner = 2, "new-reader"
	require.NoError(t, apply(t, m, c).Err())
	c.Kind = ClaimMessage
	for i, want := range []string{"f", "a", "c", "d", "e"} {
		c.Sequence = uint64(i + 1)
		delivery := apply(t, m, c)
		require.NoError(t, delivery.Err())
		require.Equal(t, want, string(delivery.Result.Delivery.Body))
		require.Equal(t, i != 0, delivery.Result.Delivery.Redelivered)
	}
	q = apply(t, m, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.EqualValues(t, 1, q.Queue.Consumers)
	require.EqualValues(t, 5, q.Queue.Unacknowledged)
	require.Zero(t, q.Queue.Ready)
}
