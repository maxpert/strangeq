package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterpayload"

	"github.com/stretchr/testify/require"
)

// Build this replicated command without the production encoder to establish
// the destination's per-record acceptance independently of encoding.
func TestDeadLetterPublishBatchReportsOnlyAcceptedRecords(t *testing.T) {
	const queue = "dead"
	const wireKind QueueCommandKind = 22
	m := &Machine{}
	info := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}}).Queue
	// The large middle record cannot fit. The later small one can.
	m.state.bodies = maxBodyBytes - 6
	var records []byte
	for _, body := range []string{"one", "large", "two"} {
		records = binary.BigEndian.AppendUint64(records, 0)
		records = binary.BigEndian.AppendUint32(records, uint32(len(body)))
		records = append(records, body...)
	}
	command, err := encodeFrame(QueueCommand{Kind: wireKind, Queue: queue, Incarnation: info.Incarnation}, records)
	require.NoError(t, err)
	reply, err := DecodeResponse(m.Apply(command))
	require.NoError(t, err)
	require.EqualValues(t, 2, reply.Count)
	require.EqualValues(t, 0b101, reply.DeadLetterApplied)
	require.ErrorIs(t, reply.Err(), ErrCapacity)
	require.Len(t, m.state.Queues[queue].Ready, 2)
	require.Equal(t, "one", string(m.state.Queues[queue].Ready[0].Body))
	require.Equal(t, "two", string(m.state.Queues[queue].Ready[1].Body))
}

func TestDeadLetterPublishBatchReadsColdBodiesAfterFencedMove(t *testing.T) {
	const queue = "durable-target"
	m := &Machine{}
	session := deadLetterSource(t, m)
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("handoff")}).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	claimed := apply(t, m, session)
	require.NoError(t, claimed.Err())
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, claimed.Result.Delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())
	transfer := m.state.LastDeadLetter
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: transfer, Plan: []string{queue}})

	incarnation := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue}).Queue.Incarnation
	first := bytes.Repeat([]byte("A"), 64<<10)
	second := bytes.Repeat([]byte("B"), 64<<10)
	records := []DeadLetterPublishRecord{
		{Move: transfer, Record: PublishRecord{Body: first}},
		{Record: PublishRecord{Body: second}},
	}
	applyBatch := func(id QueueID) QueueResponse {
		t.Helper()
		wire, err := EncodeDeadLetterPublishBatch(queue, id, records)
		require.NoError(t, err)
		reply, err := m.ApplyJournaled(wire, true, bytes.NewReader(wire))
		require.NoError(t, err)
		decoded, err := DecodeResponse(reply)
		require.NoError(t, err)
		return decoded
	}
	reply := applyBatch(incarnation)
	require.ErrorIs(t, reply.Err(), ErrInvalidCommand)
	require.Zero(t, reply.DeadLetterApplied)
	require.NotEmpty(t, m.state.DeadLetters)
	require.NoError(t, apply(t, m, QueueCommand{Kind: DeleteQueue, Queue: queue, Incarnation: incarnation}).Err())
	durable := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}}).Queue.Incarnation
	reply = applyBatch(incarnation)
	require.ErrorIs(t, reply.Err(), ErrQueueIncarnation)
	require.Zero(t, reply.DeadLetterApplied)
	require.NotEmpty(t, m.state.DeadLetters)
	reply = applyBatch(durable)
	require.NoError(t, reply.Err())
	require.EqualValues(t, 0b11, reply.DeadLetterApplied)
	require.Empty(t, m.state.DeadLetters)
	ready := m.state.Queues[queue].Ready
	require.Len(t, ready, 2)
	for i, expected := range [][]byte{first, second} {
		require.NotNil(t, ready[i].cold)
		got := make([]byte, len(expected))
		require.NoError(t, ready[i].cold.read(got))
		require.Equal(t, expected, got)
	}

	records = records[:1]
	reply = applyBatch(durable)
	require.NoError(t, reply.Err())
	require.EqualValues(t, 0b1, reply.DeadLetterApplied)
	require.Len(t, m.state.Queues[queue].Ready, 2, "a repeated local move cannot create a second body")
}

func TestDeadLetterUpdateBatchAcknowledgesOnlyExactPlans(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	for i := range 3 {
		require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte{byte(i)}}).Err())
		session.Kind, session.Sequence, session.RejectedAt = ClaimMessage, session.Sequence+1, 0
		claimed := apply(t, m, session)
		require.NoError(t, claimed.Err())
		session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, session.Sequence+1, claimed.Result.Delivery.Attempt, deadLetterTestTime
		require.NoError(t, apply(t, m, session).Err())
	}
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{"one", "two"}})
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Completed: "one"})
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 2, Plan: []string{"other"}})
	// With no plan bytes free, a plan fails while a completion reduces
	// the budget and lets the following plan succeed.
	m.state.deadLetterPlanBytes = maxDeadLetterPlanBytes
	updates := []DeadLetterUpdate{
		{ID: 3, Plan: []string{"last"}},
		{ID: 1, Plan: []string{"one", "two"}},
		{ID: 1, Plan: []string{"two"}},
		{ID: 9, Plan: []string{"missing"}},
		{ID: 2, Completed: "other"},
		{ID: 3, Plan: []string{"last"}},
		{ID: 9, Completed: "missing"},
		{ID: 1, Authorize: "one"},
		{ID: 1, Authorize: "two"},
		{ID: 2, Authorize: "other"},
		{ID: 3, Authorize: "last"},
	}
	wire, err := EncodeDeadLetterUpdates(updates)
	require.NoError(t, err)
	reply, err := DecodeResponse(m.Apply(wire))
	require.NoError(t, err)
	require.NoError(t, reply.Err())
	require.EqualValues(t, 0b10101110100, reply.DeadLetterApplied)
	require.Equal(t, []string{"two"}, m.state.DeadLetters[1].Remaining)
	require.NotContains(t, m.state.DeadLetters, uint64(2))
	require.Equal(t, []string{"last"}, m.state.DeadLetters[3].Remaining)
}

type deadLetterRetainFailure struct {
	*bytes.Reader
	count int
	err   error
}

func (r *deadLetterRetainFailure) Retain() (io.ReaderAt, func(), error) {
	r.count++
	if r.count == 2 {
		return nil, nil, r.err
	}
	return r, func() {}, nil
}

func TestDeadLetterPublishBatchStorageFailureSuppressesReply(t *testing.T) {
	m := &Machine{}
	const queue = "dead"
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}}).Queue
	record := DeadLetterPublishRecord{Record: PublishRecord{Body: make([]byte, 64<<10)}}
	wire, err := EncodeDeadLetterPublishBatch(queue, declared.Incarnation, []DeadLetterPublishRecord{record, record, record})
	require.NoError(t, err)
	diskErr := errors.New("retain failed")
	r := &deadLetterRetainFailure{Reader: bytes.NewReader(wire), err: diskErr}
	reply, err := m.ApplyJournaled(wire, true, r)
	require.ErrorIs(t, err, diskErr)
	require.Empty(t, reply)
	require.Len(t, m.state.Queues[queue].Ready, 1, "storage failure stops the group before attempting the suffix")
}

func TestDeadLetterPublishBatchRejectsMalformedSuffixAndOversizedLogicalBody(t *testing.T) {
	m := &Machine{}
	const queue = "dead"
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queue, QueueSpec: QueueSpec{Durable: true}}).Queue
	first := DeadLetterPublishRecord{Record: PublishRecord{Body: []byte("one")}}
	wire, err := EncodeDeadLetterPublishBatch(queue, declared.Incarnation, []DeadLetterPublishRecord{first, first})
	require.NoError(t, err)
	second := len(wire) - first.WireBytes()
	binary.BigEndian.PutUint32(wire[second+8:second+12], 100)
	reply, err := DecodeResponse(m.Apply(wire))
	require.NoError(t, err)
	require.ErrorIs(t, reply.Err(), ErrInvalidCommand)
	require.Empty(t, m.state.Queues[queue].Ready)

	body := bytes.Repeat([]byte("C"), 128<<10)
	header := make([]byte, 21)
	header[0] = clusterpayload.PackedEnvelope
	binary.BigEndian.PutUint32(header[3:7], 14)
	binary.BigEndian.PutUint64(header[11:19], uint64(len(body)))
	packed := DeadLetterPublishRecord{Record: PublishRecord{Prefix: header, Body: clusterpayload.Pack(body), Packed: true}}
	require.NotEmpty(t, packed.Record.Body)
	raw := packed
	raw.Record.Prefix = bytes.Clone(header)
	raw.Record.Prefix[0] = clusterpayload.RawEnvelope
	raw.Record.Body, raw.Record.Packed = body, false
	wire, err = EncodeDeadLetterPublishBatch(queue, declared.Incarnation, []DeadLetterPublishRecord{packed, raw})
	require.NoError(t, err)
	reply, err = DecodeResponse(m.Apply(wire))
	require.NoError(t, err)
	require.NoError(t, reply.Err())
	require.EqualValues(t, 0b11, reply.DeadLetterApplied)
	require.EqualValues(t, 2*(len(header)+len(body)), m.state.bodies)
	largeBatch := make([]DeadLetterPublishRecord, MaxDeadLetterPublishBatchMessages)
	for i := range largeBatch {
		largeBatch[i] = packed
	}
	_, err = EncodeDeadLetterPublishBatch(queue, declared.Incarnation, largeBatch)
	require.ErrorIs(t, err, ErrCapacity)
}

func TestDeadLetterUpdateBatchValidatesTheWholeFrameBeforeApplying(t *testing.T) {
	m := &Machine{}
	session := deadLetterSource(t, m)
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("retained")}).Err())
	session.Kind, session.Sequence = ClaimMessage, 1
	claimed := apply(t, m, session)
	require.NoError(t, claimed.Err())
	session.Kind, session.Sequence, session.Attempt, session.RejectedAt = DropMessage, 2, claimed.Result.Delivery.Attempt, deadLetterTestTime
	require.NoError(t, apply(t, m, session).Err())
	deadLetterUpdate(t, m, DeadLetterUpdate{ID: 1, Plan: []string{"destination"}})

	for _, bad := range []DeadLetterUpdate{
		{ID: 0, Completed: "destination"},
		{ID: 2, Plan: []string{"z", "a"}},
		{ID: 2, Completed: "both", Plan: []string{"invalid"}},
	} {
		updates := []DeadLetterUpdate{{ID: 1, Completed: "destination"}, bad}
		_, err := EncodeDeadLetterUpdates(updates)
		require.ErrorIs(t, err, ErrInvalidCommand)
		wire, err := encodeFrame(QueueCommand{Kind: UpdateDeadLetterBatch, DeadLetterUpdates: &updates}, nil)
		require.NoError(t, err)
		reply, err := DecodeResponse(m.Apply(wire))
		require.NoError(t, err)
		require.ErrorIs(t, reply.Err(), ErrInvalidCommand)
		require.Equal(t, []string{"destination"}, m.state.DeadLetters[1].Remaining)
	}
}
