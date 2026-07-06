package main

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueuePolicyIntegration exercises SQ-7 declare-time policy resolution
// through a real AMQP 0.9.1 client (rabbitmq/amqp091-go):
//   - declaring with valid known x-arguments succeeds,
//   - unknown x-arguments are tolerated (ignored),
//   - a malformed known x-argument closes the channel with 406
//     PRECONDITION_FAILED while the connection stays usable.
func TestQueuePolicyIntegration(t *testing.T) {
	srv := server.NewServer(":5696")
	go func() {
		_ = srv.Start()
	}()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5696//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	t.Run("ValidArgumentsSucceed", func(t *testing.T) {
		channel, err := conn.Channel()
		require.NoError(t, err)
		defer channel.Close()

		queue, err := channel.QueueDeclare(
			"policy-valid-q", // name
			false,            // durable
			false,            // auto-delete
			false,            // exclusive
			false,            // no-wait
			amqp.Table{
				"x-message-ttl":             int32(60000),
				"x-expires":                 int32(1800000),
				"x-dead-letter-exchange":    "dlx",
				"x-dead-letter-routing-key": "dead",
				"x-max-length":              int32(1000),
				"x-max-length-bytes":        int32(1048576),
				"x-overflow":                "reject-publish",
			},
		)
		require.NoError(t, err, "Declare with valid x-arguments must succeed")
		assert.Equal(t, "policy-valid-q", queue.Name)

		// The channel must still be usable after the declare.
		err = channel.PublishWithContext(t.Context(),
			"", queue.Name, false, false,
			amqp.Publishing{ContentType: "text/plain", Body: []byte("ok")})
		assert.NoError(t, err, "Channel should remain usable")
	})

	t.Run("UnknownArgumentsIgnored", func(t *testing.T) {
		channel, err := conn.Channel()
		require.NoError(t, err)
		defer channel.Close()

		_, err = channel.QueueDeclare(
			"policy-unknown-q", false, false, false, false,
			amqp.Table{
				"x-queue-type":    "classic",
				"x-custom-plugin": int32(42),
			},
		)
		require.NoError(t, err, "Unknown x-arguments must be tolerated")
	})

	t.Run("InvalidArgumentIsPreconditionFailed", func(t *testing.T) {
		channel, err := conn.Channel()
		require.NoError(t, err)

		_, err = channel.QueueDeclare(
			"policy-invalid-q", false, false, false, false,
			amqp.Table{
				"x-message-ttl": "sixty-thousand", // wrong type: must be an integer
			},
		)
		require.Error(t, err, "Malformed known x-argument must fail the declare")

		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "error should be an *amqp.Error, got %T: %v", err, err)
		assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code,
			"reply-code must be 406 PRECONDITION_FAILED")

		// Channel-level error: the connection must survive and a new channel
		// must work.
		channel2, err := conn.Channel()
		require.NoError(t, err, "Connection must survive a channel-level 406")
		defer channel2.Close()
		_, err = channel2.QueueDeclare("policy-after-error-q", false, false, false, false, nil)
		assert.NoError(t, err, "Subsequent declares on a fresh channel must work")
	})

	t.Run("NegativeTTLIsPreconditionFailed", func(t *testing.T) {
		channel, err := conn.Channel()
		require.NoError(t, err)

		_, err = channel.QueueDeclare(
			"policy-negative-q", false, false, false, false,
			amqp.Table{"x-message-ttl": int32(-1)},
		)
		require.Error(t, err, "Negative TTL must fail the declare")
		amqpErr, ok := err.(*amqp.Error)
		require.True(t, ok, "error should be an *amqp.Error, got %T: %v", err, err)
		assert.Equal(t, amqp.PreconditionFailed, amqpErr.Code)
	})
}
