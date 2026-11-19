package clusterqueue

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeResponseClearsFieldsOnDecodeError(t *testing.T) {
	wire, err := encodeFrame(struct {
		Count uint64
		Queue string
	}{Count: 1, Queue: "invalid queue information"}, nil)
	require.NoError(t, err)

	response, err := DecodeResponse(wire)
	var fieldType *json.UnmarshalTypeError
	require.ErrorAs(t, err, &fieldType)
	require.Equal(t, QueueResponse{}, response)
}

func TestSettlementBatchReplaysOnlySuccessfulPrefixAfterRestore(t *testing.T) {
	m := &Machine{}
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, declared.Err())
	session := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: declared.Queue.Incarnation,
		Session: 1, Owner: "owner", Prefetch: 3, Deadline: 10}
	require.NoError(t, apply(t, m, session).Err())

	claim := QueueCommand{Kind: ClaimMessage, Queue: session.Queue, Incarnation: session.Incarnation, Session: session.Session, Owner: session.Owner}
	var attempts []AttemptID
	for _, body := range []string{"first", "second", "third"} {
		published := apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte(body)})
		require.NoError(t, published.Err())
		claim.Sequence++
		claimed := apply(t, m, claim)
		require.NoError(t, claimed.Err())
		require.Equal(t, body, string(claimed.Result.Delivery.Body))
		attempts = append(attempts, claimed.Result.Delivery.Attempt)
	}

	batch := QueueCommand{Kind: SettleBatch, Queue: session.Queue, Incarnation: session.Incarnation, Session: session.Session,
		Owner: session.Owner, Sequence: claim.Sequence + 1, Settlement: RequeueMessage,
		Attempts: []AttemptID{attempts[0], attempts[2] + 1, attempts[1]}}
	response := apply(t, m, batch)
	require.ErrorIs(t, response.Err(), ErrUnknownAttempt)
	require.EqualValues(t, 1, response.Count)
	restored := &Machine{}
	require.NoError(t, restored.Restore(mustSnapshot(t, m)))
	require.Equal(t, response, apply(t, restored, batch))

	changed := batch
	changed.Attempts = slices.Clone(batch.Attempts)
	changed.Attempts[1], changed.Attempts[2] = changed.Attempts[2], changed.Attempts[1]
	require.ErrorIs(t, apply(t, restored, changed).Err(), ErrSequenceConflict)
	changed = batch
	changed.Settlement = AckMessage
	require.ErrorIs(t, apply(t, restored, changed).Err(), ErrSequenceConflict)

	info := apply(t, restored, QueueCommand{Kind: DescribeQueue, Queue: queueName})
	require.EqualValues(t, 2, info.Queue.Unacknowledged)
	require.EqualValues(t, 1, info.Queue.Ready)
	batch.Sequence++
	batch.Settlement, batch.Attempts = AckMessage, attempts[1:]
	completed := apply(t, restored, batch)
	require.NoError(t, completed.Err())
	require.EqualValues(t, 2, completed.Count)
	claim.Sequence = batch.Sequence + 1
	redelivered := apply(t, restored, claim)
	require.NoError(t, redelivered.Err())
	require.True(t, redelivered.Result.Delivery.Redelivered)
	require.Equal(t, "first", string(redelivered.Result.Delivery.Body))
	require.Zero(t, redelivered.Queue.Ready)
}

func TestSettlementBatchCodecBoundsAndLegacyAck(t *testing.T) {
	legacyAck := []byte("\x01\x00\x00\x00\x65\x00\x00\x00\x00" +
		`{"Kind":11,"Queue":"/integration","Incarnation":1,"Session":1,"Owner":"old","Sequence":2,"Attempt":1}`)
	decoded, err := DecodeCommand(legacyAck)
	require.NoError(t, err)
	require.Equal(t, QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: 1, Session: 1, Owner: "old", Sequence: 2, Attempt: 1}, decoded)
	encoded, err := EncodeCommand(decoded)
	require.NoError(t, err)
	require.Equal(t, legacyAck, encoded)
	require.True(t, IsSettlementCommand(encoded))

	attempts := make([]AttemptID, MaxSettlementBatch)
	for i := range attempts {
		attempts[i] = AttemptID(i + 1)
	}
	batch := QueueCommand{Kind: SettleBatch, Queue: queueName, Incarnation: 1, Session: 1, Owner: "owner",
		Sequence: 2, Settlement: AckMessage, Attempts: attempts}
	encoded, err = EncodeCommand(batch)
	require.NoError(t, err)
	require.True(t, IsSettlementCommand(encoded))
	decoded, err = DecodeCommand(encoded)
	require.NoError(t, err)
	require.Equal(t, batch, decoded)
	publish, err := EncodeCommand(QueueCommand{Kind: PublishMessage, Queue: queueName, Body: []byte("body")})
	require.NoError(t, err)
	require.False(t, IsSettlementCommand(publish))

	for _, tc := range []struct {
		name string
		edit func(*QueueCommand)
	}{
		{"one_attempt", func(c *QueueCommand) { c.Attempts = attempts[:1] }},
		{"too_many_attempts", func(c *QueueCommand) { c.Attempts = append(slices.Clone(attempts), MaxSettlementBatch+1) }},
		{"zero_attempt", func(c *QueueCommand) { c.Attempts = []AttemptID{1, 0} }},
		{"invalid_settlement_kind", func(c *QueueCommand) { c.Settlement = ClaimMessage }},
		{"unknown_command_kind", func(c *QueueCommand) { c.Kind = 255 }},
		{"legacy_command_with_batch_fields", func(c *QueueCommand) { c.Kind = AckMessage }},
		{"batch_with_raw_body", func(c *QueueCommand) { c.Body = []byte("invalid") }},
		{"missing_session", func(c *QueueCommand) { c.Session = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := batch
			tc.edit(&command)
			_, err := EncodeCommand(command)
			require.ErrorIs(t, err, ErrInvalidCommand)
			malformed, err := encodeFrame(command, command.Body)
			require.NoError(t, err)
			_, err = DecodeCommand(malformed)
			require.ErrorIs(t, err, ErrInvalidCommand)
		})
	}
}
