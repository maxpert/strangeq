package server

import (
	"encoding/binary"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Protocol-level tests: ConfirmSelectMethod / ConfirmSelectOKMethod
// ============================================================================

func TestConfirmSelectMethodDeserializeNoWaitFalse(t *testing.T) {
	method := &protocol.ConfirmSelectMethod{}
	err := method.Deserialize([]byte{0x00})
	require.NoError(t, err)
	assert.False(t, method.NoWait)
}

func TestConfirmSelectMethodDeserializeNoWaitTrue(t *testing.T) {
	method := &protocol.ConfirmSelectMethod{}
	err := method.Deserialize([]byte{0x01})
	require.NoError(t, err)
	assert.True(t, method.NoWait)
}

func TestConfirmSelectMethodDeserializeEmptyDefaultsFalse(t *testing.T) {
	method := &protocol.ConfirmSelectMethod{}
	err := method.Deserialize([]byte{})
	require.NoError(t, err)
	assert.False(t, method.NoWait)
}

func TestConfirmSelectMethodSerialize(t *testing.T) {
	t.Run("no-wait=false", func(t *testing.T) {
		method := &protocol.ConfirmSelectMethod{NoWait: false}
		data, err := method.Serialize()
		require.NoError(t, err)
		assert.Equal(t, []byte{0x00}, data)
	})

	t.Run("no-wait=true", func(t *testing.T) {
		method := &protocol.ConfirmSelectMethod{NoWait: true}
		data, err := method.Serialize()
		require.NoError(t, err)
		assert.Equal(t, []byte{0x01}, data)
	})
}

func TestConfirmSelectMethodRoundTrip(t *testing.T) {
	original := &protocol.ConfirmSelectMethod{NoWait: true}
	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.ConfirmSelectMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, original.NoWait, decoded.NoWait)
}

func TestConfirmSelectOKMethodSerialize(t *testing.T) {
	method := &protocol.ConfirmSelectOKMethod{}
	data, err := method.Serialize()
	require.NoError(t, err)
	assert.Empty(t, data)
}

// ============================================================================
// Server handler tests: confirm.select
// ============================================================================

func TestConfirmSelectSetsConfirmModeAndRespondsOK(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.ConfirmSelectMethod{NoWait: false}
	payload, err := method.Serialize()
	require.NoError(t, err)

	err = srv.handleConfirmSelect(conn, 1, payload)
	require.NoError(t, err)

	assert.True(t, ch.ConfirmMode.Load())

	classID, methodID, respPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(85), classID)
	assert.Equal(t, uint16(11), methodID)
	assert.Empty(t, respPayload)
}

func TestConfirmSelectNoWaitSkipsResponse(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.ConfirmSelectMethod{NoWait: true}
	payload, err := method.Serialize()
	require.NoError(t, err)

	err = srv.handleConfirmSelect(conn, 1, payload)
	require.NoError(t, err)

	assert.True(t, ch.ConfirmMode.Load())

	select {
	case frame := <-frameCh:
		t.Fatalf("unexpected frame when no-wait=true: %v", frame)
	default:
	}
}

func TestConfirmSelectOnTransactionalChannelReturnsError(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	txSelect(t, srv, conn, frameCh, 1)

	method := &protocol.ConfirmSelectMethod{NoWait: false}
	payload, err := method.Serialize()
	require.NoError(t, err)

	err = srv.handleConfirmSelect(conn, 1, payload)
	require.Error(t, err)

	assert.False(t, ch.ConfirmMode.Load())

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID)
	assert.Equal(t, uint16(40), methodID)
}

func TestTxSelectOnConfirmModeChannelReturnsError(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	err = srv.handleTxSelect(conn, 1, []byte{})
	require.Error(t, err)

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID)
	assert.Equal(t, uint16(40), methodID)
}

func TestConfirmSelectOnNonExistentChannelReturnsError(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	payload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)

	err = srv.handleConfirmSelect(conn, 999, payload)
	require.Error(t, err)
}

// ============================================================================
// Publish confirm tests: basic.ack sent after publish
// ============================================================================

func TestPublishInConfirmModeSendsBasicAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-pub-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-pub-q", "confirmed"))
	require.NoError(t, err)

	classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(80), methodID)

	ackMethod := &protocol.BasicAckMethod{}
	err = ackMethod.Deserialize(ackPayload)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ackMethod.DeliveryTag)
	assert.False(t, ackMethod.Multiple)
}

func TestPublishNotInConfirmModeDoesNotSendBasicAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "no-confirm-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "no-confirm-q", "no-confirm"))
	require.NoError(t, err)

	select {
	case frame := <-frameCh:
		t.Fatalf("unexpected frame when not in confirm mode: %v", frame)
	default:
	}
}

func TestPublishInConfirmModeMultipleAcksIncrementDeliveryTag(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-multi-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	for i := 0; i < 3; i++ {
		err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-multi-q", "msg"))
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
		assert.Equal(t, uint16(60), classID)
		assert.Equal(t, uint16(80), methodID)

		ackMethod := &protocol.BasicAckMethod{}
		err = ackMethod.Deserialize(ackPayload)
		require.NoError(t, err)
		assert.Equal(t, uint64(i+1), ackMethod.DeliveryTag)
	}
}

// ============================================================================
// basic.return + confirm tests
// ============================================================================

func TestPublishMandatoryReturnInConfirmModeSendsReturnAndAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	pendingMsg := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: "nonexistent-mandatory",
			Mandatory:  true,
		},
		Header: &protocol.ContentHeader{
			ClassID:  60,
			BodySize: 4,
		},
		Body:     []byte("body"),
		Received: 4,
	}

	err = srv.processCompleteMessage(conn, 1, pendingMsg)
	require.NoError(t, err)

	frame := nextFrame(t, frameCh)
	require.Equal(t, byte(protocol.FrameMethod), frame.Type)
	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(50), methodID)

	frame = nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameHeader), frame.Type)

	frame = nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameBody), frame.Type)

	classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(80), methodID)

	ackMethod := &protocol.BasicAckMethod{}
	err = ackMethod.Deserialize(ackPayload)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ackMethod.DeliveryTag)
	assert.False(t, ackMethod.Multiple)
}

func TestPublishNonMandatoryUnroutableInConfirmModeSendsAckOnly(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	pendingMsg := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: "nonexistent-non-mandatory",
			Mandatory:  false,
		},
		Header: &protocol.ContentHeader{
			ClassID:  60,
			BodySize: 4,
		},
		Body:     []byte("body"),
		Received: 4,
	}

	err = srv.processCompleteMessage(conn, 1, pendingMsg)
	require.NoError(t, err)

	classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(80), methodID)

	ackMethod := &protocol.BasicAckMethod{}
	err = ackMethod.Deserialize(ackPayload)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ackMethod.DeliveryTag)
}

// ============================================================================
// Transactional publish + confirm tests
// ============================================================================

func TestTxPublishCommitInConfirmModeSendsAckAfterCommit(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-tx-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	require.NoError(t, srv.TransactionManager.Select(1))

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-tx-q", "tx-confirm"))
	require.NoError(t, err)

	select {
	case frame := <-frameCh:
		t.Fatalf("unexpected frame before commit: %v", frame)
	default:
	}

	err = srv.handleTxCommit(conn, 1, []byte{})
	require.NoError(t, err)

	classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(80), methodID)

	ackMethod := &protocol.BasicAckMethod{}
	err = ackMethod.Deserialize(ackPayload)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ackMethod.DeliveryTag)
	assert.False(t, ackMethod.Multiple)

	classID, methodID, _ = nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(90), classID)
	assert.Equal(t, uint16(21), methodID)
}

func TestTxPublishCommitMultipleInConfirmModeSendsAcksInOrder(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-tx-multi-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	require.NoError(t, srv.TransactionManager.Select(1))

	for i := 0; i < 3; i++ {
		err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-tx-multi-q", "msg"))
		require.NoError(t, err)
	}

	err = srv.handleTxCommit(conn, 1, []byte{})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
		assert.Equal(t, uint16(60), classID)
		assert.Equal(t, uint16(80), methodID)

		ackMethod := &protocol.BasicAckMethod{}
		err = ackMethod.Deserialize(ackPayload)
		require.NoError(t, err)
		assert.Equal(t, uint64(i+1), ackMethod.DeliveryTag)
	}

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(90), classID)
	assert.Equal(t, uint16(21), methodID)
}

func TestTxPublishRollbackInConfirmModeDoesNotSendAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-tx-rb-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	require.NoError(t, srv.TransactionManager.Select(1))

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-tx-rb-q", "rolled-back"))
	require.NoError(t, err)

	err = srv.handleTxRollback(conn, 1, []byte{})
	require.NoError(t, err)

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(90), classID)
	assert.Equal(t, uint16(31), methodID)

	select {
	case frame := <-frameCh:
		t.Fatalf("unexpected frame after rollback: %v", frame)
	default:
	}
}

// ============================================================================
// Dispatch test: class 85 routing
// ============================================================================

func TestProcessChannelMethodDispatchesConfirmClass(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)

	frame := &protocol.Frame{
		Type:    protocol.FrameMethod,
		Channel: 1,
		Payload: append([]byte{0, 85, 0, 10}, confirmPayload...),
	}

	err = srv.processMethodFrame(conn, frame)
	require.NoError(t, err)

	assert.True(t, ch.ConfirmMode.Load())

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(85), classID)
	assert.Equal(t, uint16(11), methodID)
}

// ============================================================================
// Non-transactional publish without confirm: no interference
// ============================================================================

func TestNonConfirmNonTxPublishHasNoExtraFrames(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "plain-pub-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "plain-pub-q", "plain"))
	require.NoError(t, err)

	select {
	case frame := <-frameCh:
		t.Fatalf("unexpected frame for plain publish: %v", frame)
	default:
	}
}

// ============================================================================
// Confirm sequence does not reset across transactions
// ============================================================================

func TestConfirmSequenceContinuesAcrossTransactions(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-seq-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	require.NoError(t, srv.TransactionManager.Select(1))

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-seq-q", "first"))
	require.NoError(t, err)

	err = srv.handleTxCommit(conn, 1, []byte{})
	require.NoError(t, err)

	_, _, ackPayload := nextMethodFrame(t, frameCh)
	ackMethod := &protocol.BasicAckMethod{}
	require.NoError(t, ackMethod.Deserialize(ackPayload))
	assert.Equal(t, uint64(1), ackMethod.DeliveryTag)
	drainFrame(t, frameCh) // tx.commit-ok

	// Channel remains transactional after commit — no need to Select again
	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-seq-q", "second"))
	require.NoError(t, err)

	err = srv.handleTxCommit(conn, 1, []byte{})
	require.NoError(t, err)

	_, _, ackPayload = nextMethodFrame(t, frameCh)
	ackMethod = &protocol.BasicAckMethod{}
	require.NoError(t, ackMethod.Deserialize(ackPayload))
	assert.Equal(t, uint64(2), ackMethod.DeliveryTag)
}

// ============================================================================
// Helper: verify pending operations contain publish with confirm tag
// ============================================================================

func TestConfirmTagStoredInTxOperation(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: "confirm-op-q"}
	declPayload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, declPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	err = srv.handleConfirmSelect(conn, 1, confirmPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh)

	require.NoError(t, srv.TransactionManager.Select(1))

	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "confirm-op-q", "op-test"))
	require.NoError(t, err)

	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, interfaces.OpPublish, ops[0].Type)
	assert.Equal(t, uint64(1), ops[0].DeliveryTag)
}
