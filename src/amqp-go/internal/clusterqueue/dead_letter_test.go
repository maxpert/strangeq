package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
	"github.com/stretchr/testify/require"
)

const deadLetterTestTime = 1700000000000000000

func TestDeadLetterPageBudgetUsesLogicalPayloadBytes(t *testing.T) {
	for _, tc := range []struct {
		name         string
		logicalBytes int
		packed       bool
		pageCount    int
		totalCount   int
	}{
		{name: "raw boundary", logicalBytes: DeadLetterPageBytes / 2, pageCount: 2, totalCount: 3},
		{name: "packed boundary", logicalBytes: 128 << 10, packed: true, pageCount: 16, totalCount: 17},
		{name: "packed overflow", logicalBytes: (128 << 10) + 21, packed: true, pageCount: 15, totalCount: 17},
		{name: "oversized singleton", logicalBytes: DeadLetterPageBytes + 1, pageCount: 1, totalCount: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Machine{}
			session := deadLetterSource(t, m)
			header := make([]byte, 21)
			header[0] = clusterpayload.RawEnvelope
			body := make([]byte, tc.logicalBytes-len(header))
			binary.BigEndian.PutUint32(header[3:7], 14)
			binary.BigEndian.PutUint64(header[11:19], uint64(len(body)))
			if tc.packed {
				header[0] = clusterpayload.PackedEnvelope
				body = clusterpayload.Pack(body)
				require.NotEmpty(t, body)
			}
			command, err := EncodePublishCommand(queueName, session.Incarnation, PublishRecord{
				Prefix: header, Body: body, Packed: tc.packed,
			})
			require.NoError(t, err)
			for range tc.totalCount {
				require.NoError(t, m.applyCommand(command, true, nil).Err())
				session.Kind, session.Sequence = ClaimMessage, session.Sequence+1
				session.Attempt, session.RejectedAt = 0, 0
				delivery := apply(t, m, session).Result.Delivery
				require.NotNil(t, delivery)
				session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, session.Sequence+1, delivery.Attempt, deadLetterTestTime
				require.NoError(t, apply(t, m, session).Err())
			}
			encode, wireBytes, logicalBytes := m.CaptureDeadLetterPage(0, 0)
			require.Equal(t, tc.pageCount*tc.logicalBytes, logicalBytes)
			require.Equal(t, tc.pageCount*(len(header)+len(body)), wireBytes)
			wire, err := encode()
			require.NoError(t, err)
			page, err := DecodeDeadLetterPage(wire)
			require.NoError(t, err)
			require.Len(t, page.Entries, tc.pageCount)
			require.EqualValues(t, tc.totalCount, page.Through)
			for i, entry := range page.Entries {
				require.EqualValues(t, i+1, entry.ID, "page capture must preserve source order")
			}
		})
	}
}

func deadLetterSource(t *testing.T, m *Machine) QueueCommand {
	t.Helper()
	q := apply(t, m, QueueCommand{
		Kind: DeclareQueue, Queue: queueName, QueueSpec: QueueSpec{Durable: true, DeadLetter: &DeadLetterPolicy{Exchange: "dlx"}},
	})
	require.NoError(t, q.Err())
	session := QueueCommand{
		Kind: OpenQueueSession, Queue: queueName, Incarnation: q.Queue.Incarnation, Session: 1, Owner: "consumer", Prefetch: 1, Deadline: deadLetterTestTime,
	}
	require.NoError(t, apply(t, m, session).Err())
	return session
}

func deadLetterView(t *testing.T, m *Machine) DeadLetterPageEntry {
	t.Helper()
	encode, _, _ := m.CaptureDeadLetterPage(0, 0)
	wire, err := encode()
	require.NoError(t, err)
	page, err := DecodeDeadLetterPage(wire)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	return page.Entries[0]
}

func deadLetterUpdate(t *testing.T, m *Machine, update DeadLetterUpdate) {
	t.Helper()
	require.NoError(t, apply(t, m, QueueCommand{Kind: UpdateDeadLetter, DeadLetter: &update}).Err())
}

func TestDeadLetterKeepsJournalBodyAndFanoutProgressThroughSnapshot(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	body := bytes.Repeat([]byte{0xab}, 64<<10)
	publish, err := EncodePublishCommand(queueName, session.Incarnation, PublishRecord{Body: body})
	require.NoError(t, err)
	source := &countedCommandSource{Reader: bytes.NewReader(publish)}
	_, err = m.ApplyJournaled(publish, false, source)
	require.NoError(t, err)
	session.Kind, session.Sequence = ClaimMessage, 1
	claim, err := EncodeCommand(session)
	require.NoError(t, err)
	_, err = m.ApplyCommitted(claim, false)
	require.NoError(t, err)

	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, 1, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())
	require.NoError(t, apply(t, m, session).Err(), "replayed rejection must not stage another entry")
	require.Zero(t, source.reads, "rejection must reuse the journal body without reading it")
	require.EqualValues(t, 1, m.state.messages)
	require.EqualValues(t, len(body), m.state.coldBytes)
	require.EqualValues(t, 0, m.state.Queues[queueName].unacked)
	require.Len(t, m.state.DeadLetters, 1)
	require.Empty(t, m.state.DeadLetters[1].Message.Body)
	require.NoError(t, apply(t, m, QueueCommand{Kind: DeleteQueue, Queue: queueName, Incarnation: session.Incarnation}).Err())

	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{"first", "second"}})
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Completed: "first"})
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{"first", "third"}})
	checkpoint := m.CloneForSnapshot()
	require.NoError(t, m.Restore(mustSnapshot(t, &checkpoint)))
	reply := deadLetterView(t, m)
	require.Equal(t, body, reply.Body)
	require.Equal(t, []string{"second"}, reply.Remaining)
	require.Equal(t, queueName, reply.Source)
	require.EqualValues(t, deadLetterTestTime, reply.Rejected)
	require.EqualValues(t, 1, m.state.messages)
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Completed: "first"})
	require.True(t, m.HasDeadLetters())
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Completed: "second"})
	require.False(t, m.HasDeadLetters())
	require.Zero(t, m.state.messages)
	require.Zero(t, m.state.bodies)
	require.Zero(t, m.state.deadLetterPlanBytes)
}

func TestDeadLetterSameShardMoveUsesFinalSourceCreditAndFencesDestination(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	body := []byte("a source body")
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: body}).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	delivery := apply(t, m, session).Result.Delivery
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())

	const target = "target"
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{target}})
	old := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: target}).Queue
	publish := func(incarnation QueueID) QueueResponse {
		t.Helper()
		wire, err := EncodeDeadLetterPublish(target, incarnation, 1, PublishRecord{Body: body})
		require.NoError(t, err)
		reply, err := DecodeResponse(m.Apply(wire))
		require.NoError(t, err)
		return reply
	}
	require.ErrorIs(t, publish(old.Incarnation).Err(), ErrInvalidCommand, "a transient queue cannot take ownership")
	require.True(t, m.HasDeadLetters())

	require.NoError(t, apply(t, m, QueueCommand{Kind: DeleteQueue, Queue: target, Incarnation: old.Incarnation}).Err())
	newQueue := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: target, QueueSpec: QueueSpec{Durable: true}}).Queue
	require.ErrorIs(t, publish(old.Incarnation).Err(), ErrQueueIncarnation)

	// Treat the unused budget as occupied by unrelated queues.
	m.state.bodies = maxBodyBytes - m.state.deadLetterCopyBytes
	m.state.messages = maxMessages - m.state.deadLetterCopySlots
	require.ErrorIs(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: target, Body: body}).Err(), ErrCapacity)
	require.NoError(t, publish(newQueue.Incarnation).Err())
	require.NoError(t, publish(newQueue.Incarnation).Err(), "replayed move is a no-op")
	require.False(t, m.HasDeadLetters())
	require.EqualValues(t, maxMessages-1, m.state.messages)
	require.Len(t, m.state.Queues[target].Ready, 1)
	require.Equal(t, body, m.state.Queues[target].Ready[0].Body)
}

func TestDeadLetterReservesAnIntermediateLocalCopyAtCapacity(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	const first, second = "first", "second"
	body := []byte("fanout body")
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: body}).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	delivery := apply(t, m, session).Result.Delivery
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())

	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{first, second}})
	for _, queue := range []string{first, second} {
		require.NoError(t, apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}}).Err())
	}
	m.state.bodies = maxBodyBytes - m.state.deadLetterCopyBytes
	m.state.messages = maxMessages - m.state.deadLetterCopySlots
	for _, queue := range []string{first, second} {
		wire, err := EncodeDeadLetterPublish(queue, 0, 1, PublishRecord{Body: body})
		require.NoError(t, err)
		reply, err := DecodeResponse(m.Apply(wire))
		require.NoError(t, err)
		require.NoError(t, reply.Err())
		require.Zero(t, m.state.deadLetterCopySlots)
		require.Zero(t, m.state.deadLetterCopyBytes)
		require.Len(t, m.state.Queues[queue].Ready, 1)
	}
	require.False(t, m.HasDeadLetters())
}

func TestDeadLetterAdmissionReservesEnvelopeHeadroom(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	policy := m.state.Queues[queueName].Spec.DeadLetter
	accepted := maxMessageBytes - int(policy.reserve(queueName))
	q := m.state.Queues[queueName]
	require.ErrorIs(t, m.publish(q, make([]byte, accepted+1), journalBodyOrigin{}, 0).Err(), ErrCapacity)
	require.Zero(t, m.state.messages)
	require.NoError(t, m.publish(q, make([]byte, accepted), journalBodyOrigin{}, 0).Err())
	require.Equal(t, uint64(maxMessageBytes), m.state.deadLetterCopyBytes)

	session.Kind, session.Sequence = ClaimMessage, 1
	delivery := apply(t, m, session).Result.Delivery
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())
	require.True(t, m.HasDeadLetters())
}

func TestDeadLetterFanoutCanReachDeadLetterSourcesAtCapacity(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	body := bytes.Repeat([]byte{0xcd}, 64<<10)
	q := m.state.Queues[queueName]
	require.NoError(t, m.publish(q, body, journalBodyOrigin{}, 0).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	delivery := apply(t, m, session).Result.Delivery
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())

	const first, second = "first", "second"
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{first, second}})
	for _, target := range []string{first, second} {
		require.NoError(t, apply(t, m, QueueCommand{
			Kind: DeclareQueue, Queue: target, QueueSpec: QueueSpec{Durable: true, DeadLetter: &DeadLetterPolicy{Exchange: "another-dlx"}},
		}).Err())
	}

	// Fill the unreserved capacity with real messages to verify that a
	// checkpoint can recover a funded local move before its target is ACKed.
	const unrelated = "unrelated"
	require.NoError(t, apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: unrelated}).Err())
	filler := m.state.Queues[unrelated]
	for remaining := maxBodyBytes - m.state.bodies - m.state.deadLetterCopyBytes; remaining > 0; {
		size := min(remaining, maxMessageBytes)
		require.NoError(t, m.publish(filler, make([]byte, size), journalBodyOrigin{}, 0).Err())
		remaining -= size
	}

	move := func(target string) {
		t.Helper()
		wire, err := EncodeDeadLetterPublish(target, 0, 1, PublishRecord{Body: body})
		require.NoError(t, err)
		reply, err := DecodeResponse(m.Apply(wire))
		require.NoError(t, err)
		require.NoError(t, reply.Err())
		require.Len(t, m.state.Queues[target].Ready, 1)
	}
	move(first)
	require.Greater(t, m.state.bodies+m.state.deadLetterCopyBytes, uint64(maxBodyBytes))
	require.ErrorIs(t, m.publish(filler, []byte{1}, journalBodyOrigin{}, 0).Err(), ErrCapacity)

	restored := &Machine{}
	require.NoError(t, restored.Restore(mustSnapshot(t, m)))
	m = restored
	target := QueueCommand{
		Kind: OpenQueueSession, Queue: first, Incarnation: m.state.Queues[first].ID,
		Session: 1, Owner: "target-consumer", Deadline: deadLetterTestTime, Prefetch: 1,
	}
	require.NoError(t, apply(t, m, target).Err())
	target.Kind, target.Sequence = ClaimMessage, 1
	delivery = apply(t, m, target).Result.Delivery
	target.Kind, target.Sequence, target.Attempt = AckMessage, 2, delivery.Attempt
	require.NoError(t, apply(t, m, target).Err())
	move(second)
	require.False(t, m.HasDeadLetters())
}
