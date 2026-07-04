package server

import (
	"encoding/binary"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendConnectionClose_PopulatesClassMethod(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	err := srv.sendConnectionClose(conn, 503, "COMMAND_INVALID", 10, 11)
	require.NoError(t, err)

	classID, methodID, payload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(10), classID, "expected connection class (10)")
	assert.Equal(t, uint16(50), methodID, "expected connection.close (50)")

	closeMethod := &protocol.ConnectionCloseMethod{}
	err = closeMethod.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, uint16(503), closeMethod.ReplyCode)
	assert.Equal(t, "COMMAND_INVALID", closeMethod.ReplyText)
	assert.Equal(t, uint16(10), closeMethod.ClassID, "ClassID must be populated from args")
	assert.Equal(t, uint16(11), closeMethod.MethodID, "MethodID must be populated from args")
}

func TestConnectionClose_SendsCloseOK(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	closePayload := make([]byte, 4)
	binary.BigEndian.PutUint16(closePayload[0:2], 10)
	binary.BigEndian.PutUint16(closePayload[2:4], 50)

	err := srv.processConnectionMethod(conn, protocol.ConnectionClose, closePayload)
	assert.ErrorIs(t, err, errClientRequestedClose)

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(10), classID, "expected connection.close-ok class")
	assert.Equal(t, uint16(51), methodID, "expected connection.close-ok method (51)")
}

func TestConnectionClose_SetsClosedFlag(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	closePayload := make([]byte, 4)
	binary.BigEndian.PutUint16(closePayload[0:2], 10)
	binary.BigEndian.PutUint16(closePayload[2:4], 50)

	assert.False(t, conn.Closed.Load())
	_ = srv.processConnectionMethod(conn, protocol.ConnectionClose, closePayload)
	assert.True(t, conn.Closed.Load(), "conn.Closed must be set after client connection.close")
}

func TestConnectionClose_TriggersTeardown(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	require.NotNil(t, srv.TransactionManager)
	require.NoError(t, srv.TransactionManager.Select(1))

	closePayload := make([]byte, 4)
	binary.BigEndian.PutUint16(closePayload[0:2], 10)
	binary.BigEndian.PutUint16(closePayload[2:4], 50)

	err := srv.processConnectionMethod(conn, protocol.ConnectionClose, closePayload)
	assert.ErrorIs(t, err, errClientRequestedClose)
	assert.True(t, conn.Closed.Load())

	srv.cleanupConnection(conn)
	assert.False(t, srv.TransactionManager.IsTransactional(1),
		"teardown after client close must clean up transaction state")
}

func TestErrClientRequestedCloseSentinel(t *testing.T) {
	assert.NotNil(t, errClientRequestedClose)
	assert.Contains(t, errClientRequestedClose.Error(), "connection closed by client")
}
