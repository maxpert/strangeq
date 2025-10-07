package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionBasicFlow(t *testing.T) {
	// Create server with transaction support
	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "memory"
	cfg.Network.Port = 15672
	cfg.Network.Address = "localhost:15672"

	amqpServer, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("info").
		Build()
	require.NoError(t, err)

	// Start server
	go func() {
		amqpServer.ListenAndServe("localhost:15672")
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)
	defer amqpServer.Shutdown()

	// Create client connection
	conn, err := createTestConnection("localhost:15672")
	require.NoError(t, err)
	defer conn.Close()

	// Test basic transaction flow
	t.Run("tx.select followed by tx.commit", func(t *testing.T) {
		channelID := uint16(1)

		// Open channel
		err := sendChannelOpen(conn, channelID)
		require.NoError(t, err)

		// Receive channel.open-ok
		frame, err := protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)
		assert.Equal(t, channelID, frame.Channel)

		// Send tx.select
		txSelectFrame := protocol.NewTxSelectFrame(channelID)
		err = protocol.WriteFrame(conn, txSelectFrame)
		require.NoError(t, err)

		// Receive tx.select-ok
		frame, err = protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)
		assert.Equal(t, channelID, frame.Channel)

		// Verify it's tx.select-ok (class 90, method 11)
		assert.Len(t, frame.Payload, 4) // class ID + method ID
		classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
		methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
		assert.Equal(t, uint16(90), classID)
		assert.Equal(t, uint16(11), methodID) // TxSelectOK

		// Send tx.commit
		txCommitFrame := protocol.NewTxCommitFrame(channelID)
		err = protocol.WriteFrame(conn, txCommitFrame)
		require.NoError(t, err)

		// Receive tx.commit-ok
		frame, err = protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)
		assert.Equal(t, channelID, frame.Channel)

		// Verify it's tx.commit-ok (class 90, method 21)
		assert.Len(t, frame.Payload, 4)
		classID = (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
		methodID = (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
		assert.Equal(t, uint16(90), classID)
		assert.Equal(t, uint16(21), methodID) // TxCommitOK
	})

	t.Run("tx.select followed by tx.rollback", func(t *testing.T) {
		channelID := uint16(2)

		// Open channel
		err := sendChannelOpen(conn, channelID)
		require.NoError(t, err)

		// Receive channel.open-ok
		frame, err := protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)
		assert.Equal(t, channelID, frame.Channel)

		// Send tx.select
		txSelectFrame := protocol.NewTxSelectFrame(channelID)
		err = protocol.WriteFrame(conn, txSelectFrame)
		require.NoError(t, err)

		// Receive tx.select-ok
		frame, err = protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)

		// Send tx.rollback
		txRollbackFrame := protocol.NewTxRollbackFrame(channelID)
		err = protocol.WriteFrame(conn, txRollbackFrame)
		require.NoError(t, err)

		// Receive tx.rollback-ok
		frame, err = protocol.ReadFrame(conn)
		require.NoError(t, err)
		assert.Equal(t, protocol.MethodFrame, frame.Type)
		assert.Equal(t, channelID, frame.Channel)

		// Verify it's tx.rollback-ok (class 90, method 31)
		assert.Len(t, frame.Payload, 4)
		classID := (uint16(frame.Payload[0]) << 8) | uint16(frame.Payload[1])
		methodID := (uint16(frame.Payload[2]) << 8) | uint16(frame.Payload[3])
		assert.Equal(t, uint16(90), classID)
		assert.Equal(t, uint16(31), methodID) // TxRollbackOK
	})
}

func TestTransactionWithOperations(t *testing.T) {
	// Create server
	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "memory"
	cfg.Network.Port = 15673
	cfg.Network.Address = "localhost:15673"

	amqpServer, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("info").
		Build()
	require.NoError(t, err)

	// Start server
	go func() {
		amqpServer.ListenAndServe("localhost:15673")
	}()

	time.Sleep(100 * time.Millisecond)
	defer amqpServer.Shutdown()

	// Create client connection
	conn, err := createTestConnection("localhost:15673")
	require.NoError(t, err)
	defer conn.Close()

	channelID := uint16(1)

	// Open channel
	err = sendChannelOpen(conn, channelID)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read channel.open-ok
	require.NoError(t, err)

	// Start transaction
	txSelectFrame := protocol.NewTxSelectFrame(channelID)
	err = protocol.WriteFrame(conn, txSelectFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read tx.select-ok
	require.NoError(t, err)

	// Declare exchange
	err = sendExchangeDeclare(conn, channelID, "test.exchange", "direct", false, false, false, nil)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read exchange.declare-ok
	require.NoError(t, err)

	// Declare queue
	err = sendQueueDeclare(conn, channelID, "test.queue", false, false, false, nil)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read queue.declare-ok
	require.NoError(t, err)

	// Bind queue to exchange
	err = sendQueueBind(conn, channelID, "test.queue", "test.exchange", "test.key", nil)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read queue.bind-ok
	require.NoError(t, err)

	// TODO: In a complete implementation, publish operations would be queued
	// during transactional mode and only executed on commit

	// Commit transaction
	txCommitFrame := protocol.NewTxCommitFrame(channelID)
	err = protocol.WriteFrame(conn, txCommitFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn) // Read tx.commit-ok
	require.NoError(t, err)
}

func TestTransactionMultipleCommits(t *testing.T) {
	// Create server
	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "memory"
	cfg.Network.Port = 15674
	cfg.Network.Address = "localhost:15674"

	amqpServer, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("info").
		Build()
	require.NoError(t, err)

	go func() {
		amqpServer.ListenAndServe("localhost:15674")
	}()

	time.Sleep(100 * time.Millisecond)
	defer amqpServer.Shutdown()

	conn, err := createTestConnection("localhost:15674")
	require.NoError(t, err)
	defer conn.Close()

	channelID := uint16(1)

	// Open channel
	err = sendChannelOpen(conn, channelID)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)

	// Start transaction
	txSelectFrame := protocol.NewTxSelectFrame(channelID)
	err = protocol.WriteFrame(conn, txSelectFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)

	// First commit
	txCommitFrame := protocol.NewTxCommitFrame(channelID)
	err = protocol.WriteFrame(conn, txCommitFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)

	// Second commit (should work since channel is still in transactional mode)
	txCommitFrame = protocol.NewTxCommitFrame(channelID)
	err = protocol.WriteFrame(conn, txCommitFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)

	// Third rollback
	txRollbackFrame := protocol.NewTxRollbackFrame(channelID)
	err = protocol.WriteFrame(conn, txRollbackFrame)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)
}

func TestTransactionErrors(t *testing.T) {
	// Create server
	cfg := config.DefaultConfig()
	cfg.Storage.Backend = "memory"
	cfg.Network.Port = 15675
	cfg.Network.Address = "localhost:15675"

	amqpServer, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("info").
		Build()
	require.NoError(t, err)

	go func() {
		amqpServer.ListenAndServe("localhost:15675")
	}()

	time.Sleep(100 * time.Millisecond)
	defer amqpServer.Shutdown()

	conn, err := createTestConnection("localhost:15675")
	require.NoError(t, err)
	defer conn.Close()

	channelID := uint16(1)

	// Open channel
	err = sendChannelOpen(conn, channelID)
	require.NoError(t, err)
	_, err = protocol.ReadFrame(conn)
	require.NoError(t, err)

	// Try to commit without tx.select (should fail gracefully or be ignored)
	txCommitFrame := protocol.NewTxCommitFrame(channelID)
	err = protocol.WriteFrame(conn, txCommitFrame)
	require.NoError(t, err)

	// The server should respond with an error or handle gracefully
	// In our implementation, it will return an error via the transaction manager
	// The exact error handling depends on how we implement error responses
}

// Helper functions

func sendChannelOpen(conn *protocol.Connection, channelID uint16) error {
	openMethod := &protocol.ChannelOpenMethod{
		Reserved1: "",
	}
	methodData, err := openMethod.Serialize()
	if err != nil {
		return err
	}
	frame := protocol.EncodeMethodFrameForChannel(channelID, 20, protocol.ChannelOpen, methodData)
	return protocol.WriteFrame(conn.Conn, frame)
}

func sendExchangeDeclare(conn *protocol.Connection, channelID uint16, name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	method := &protocol.ExchangeDeclareMethod{
		Reserved1:  0,
		Exchange:   name,
		Type:       exchangeType,
		Passive:    false,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
		NoWait:     false,
		Arguments:  arguments,
	}

	methodData, err := method.Serialize()
	if err != nil {
		return err
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 40, protocol.ExchangeDeclare, methodData)
	return protocol.WriteFrame(conn.Conn, frame)
}

func sendQueueDeclare(conn *protocol.Connection, channelID uint16, name string, durable, exclusive, autoDelete bool, arguments map[string]interface{}) error {
	method := &protocol.QueueDeclareMethod{
		Reserved1:  0,
		Queue:      name,
		Passive:    false,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
		NoWait:     false,
		Arguments:  arguments,
	}

	methodData, err := method.Serialize()
	if err != nil {
		return err
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, protocol.QueueDeclare, methodData)
	return protocol.WriteFrame(conn.Conn, frame)
}

func sendQueueBind(conn *protocol.Connection, channelID uint16, queue, exchange, routingKey string, arguments map[string]interface{}) error {
	method := &protocol.QueueBindMethod{
		Reserved1:  0,
		Queue:      queue,
		Exchange:   exchange,
		RoutingKey: routingKey,
		NoWait:     false,
		Arguments:  arguments,
	}

	methodData, err := method.Serialize()
	if err != nil {
		return err
	}

	frame := protocol.EncodeMethodFrameForChannel(channelID, 50, protocol.QueueBind, methodData)
	return protocol.WriteFrame(conn.Conn, frame)
}
