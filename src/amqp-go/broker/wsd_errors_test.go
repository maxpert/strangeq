package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSemanticError_Classification verifies the shared foundation A sentinels
// and the SemanticError wrapper: errors.Is must reach every sentinel the error
// classifies as, and Error() carries the RabbitMQ-shaped text (without the
// "CODE - " prefix, which the server mapping adds).
func TestSemanticError_Classification(t *testing.T) {
	err := semErr("no queue 'q' in vhost '/'", interfaces.ErrQueueNotFound)
	assert.Equal(t, "no queue 'q' in vhost '/'", err.Error())
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound))
	assert.False(t, errors.Is(err, interfaces.ErrExchangeNotFound))

	// Multi-sentinel: inequivalent-args + a specific sub-sentinel (§2.3 keeps
	// ErrExchangeTypeMismatch reachable through inequivalent()).
	multi := inequivalent("exchange", "x", "type", "'direct'", "'fanout'", ErrExchangeTypeMismatch)
	assert.True(t, errors.Is(multi, ErrInequivalentArgs))
	assert.True(t, errors.Is(multi, ErrExchangeTypeMismatch))
	assert.Contains(t, multi.Error(), "inequivalent arg 'type' for exchange 'x' in vhost '/': received 'direct' but current is 'fanout'")
}

// TestSemanticError_QueueRS verifies the queue resource-string shape RabbitMQ
// uses in reply texts: queue 'name' in vhost '/'.
func TestSemanticError_QueueRS(t *testing.T) {
	err := errNoQueue("nope")
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound))
	assert.Equal(t, "no queue 'nope' in vhost '/'", err.Error())

	xerr := errNoExchange("ex")
	assert.True(t, errors.Is(xerr, interfaces.ErrExchangeNotFound))
	assert.Equal(t, "no exchange 'ex' in vhost '/'", xerr.Error())

	// Exported constructors for the server's non-storage-broker fallback (§1.3/§1.4).
	assert.True(t, errors.Is(QueueNotFoundError("q"), interfaces.ErrQueueNotFound))
	assert.True(t, errors.Is(ExchangeNotFoundError("x"), interfaces.ErrExchangeNotFound))
}

// TestSentinelHygiene_BindQueue verifies that the bare fmt.Errorf not-found
// returns were replaced by typed errors (§0 sentinel hygiene), so WS-B's
// mapping can classify them as 404s.
func TestSentinelHygiene_BindQueue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// Missing exchange.
	err := b.BindQueue("q", "missing-exchange", "k", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrExchangeNotFound), "BindQueue missing exchange must classify as ErrExchangeNotFound: %v", err)

	// Existing exchange, missing queue.
	require.NoError(t, b.DeclareExchange("ex", "direct", false, false, false, nil))
	err = b.BindQueue("missing-queue", "ex", "k", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound), "BindQueue missing queue must classify as ErrQueueNotFound: %v", err)
}

// TestSentinelHygiene_BindExchange verifies source/destination not-found typing.
func TestSentinelHygiene_BindExchange(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	err := b.BindExchange("dst", "missing-src", "k", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrExchangeNotFound), "source: %v", err)

	require.NoError(t, b.DeclareExchange("src", "direct", false, false, false, nil))
	err = b.BindExchange("missing-dst", "src", "k", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrExchangeNotFound), "destination: %v", err)
}

// TestSentinelHygiene_PublishAndGet verifies the publish/get not-found typing.
func TestSentinelHygiene_PublishAndGet(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	msg := &protocol.Message{Exchange: "missing-exchange", RoutingKey: "k", Body: []byte("x")}
	err := b.PublishMessage("missing-exchange", "k", msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrExchangeNotFound), "PublishMessage: %v", err)

	_, _, _, gerr := b.GetMessageForGet("missing-queue", true)
	require.Error(t, gerr)
	assert.True(t, errors.Is(gerr, interfaces.ErrQueueNotFound), "GetMessageForGet: %v", gerr)

	// RegisterConsumer on a missing queue.
	consumer := &protocol.Consumer{ID: "cid"}
	cerr := b.RegisterConsumer("missing-queue", "cid", consumer)
	require.Error(t, cerr)
	assert.True(t, errors.Is(cerr, interfaces.ErrQueueNotFound), "RegisterConsumer: %v", cerr)
}
