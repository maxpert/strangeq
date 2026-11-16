package server

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestReliableDeadLetter_AMQPRestartBeforeBinding(t *testing.T) {
	const source, exchange, target = "dlx-source", "dlx-restart", "dlx-target"
	dataDir := t.TempDir()
	start := func() (*Server, *amqp.Connection) {
		cfg := config.DefaultConfig()
		cfg.Storage.Path = dataDir
		srv, err := NewServerBuilder().WithConfig(cfg).Build()
		require.NoError(t, err)
		t.Cleanup(func() { stopReliableDeadLetterServer(t, srv) })
		pipe := dialRawPipe(t, srv)
		conn, err := amqp.Open(pipe.c, amqp.Config{
			Vhost: "/",
			SASL:  []amqp.Authentication{&amqp.PlainAuth{Username: "guest", Password: "guest"}},
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return srv, conn
	}
	srv, conn := start()
	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil))
	_, err = ch.QueueDeclare(source, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": exchange,
		"x-dead-letter-strategy": "at-least-once",
		"x-overflow":             "reject-publish",
	})
	require.NoError(t, err)
	require.NoError(t, ch.Confirm(false))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// A transient source record cannot be recovered as a normal source message.
	// After restart, only a durable dead-letter transfer can deliver this body.
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", source, true, false, amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Body:         []byte("rejected before target existed"),
	})
	require.NoError(t, err)
	accepted, err := confirm.WaitContext(ctx)
	require.NoError(t, err)
	require.True(t, accepted)
	message, ok, err := ch.Get(source, false)
	require.NoError(t, err)
	require.True(t, ok)
	counts := srv.Broker.(*StorageBrokerAdapter)
	require.EqualValues(t, 1, counts.QueueMessageCounts(source).Unacked)
	require.NoError(t, message.Reject(false))
	require.Eventually(t, func() bool {
		return counts.QueueMessageCounts(source).Total == 0
	}, 5*time.Second, 10*time.Millisecond, "rejection must finish while the exchange has no binding")
	require.NoError(t, conn.Close())

	stopReliableDeadLetterServer(t, srv)
	_, reopened := start()
	ch, err = reopened.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare(target, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(target, source, exchange, false, nil))
	deliveries, err := ch.Consume(target, "", false, false, false, false, nil)
	require.NoError(t, err)
	select {
	case dead, open := <-deliveries:
		require.True(t, open)
		require.Equal(t, message.Body, dead.Body)
		require.Equal(t, "rejected", dead.Headers["x-first-death-reason"])
		require.NoError(t, dead.Ack(false))
	case <-time.After(5 * time.Second):
		t.Fatal("pending dead letter did not reach the newly bound target after restart")
	}
}

func stopReliableDeadLetterServer(t *testing.T, srv *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}
