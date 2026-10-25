package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternal_PublishRejected_E2EBindingAllowed (T11.1 broker twin, R11):
// publishing DIRECTLY to an internal exchange is refused with
// ErrExchangeInternal, but binding src→ix (e2e INTO an internal exchange) is
// allowed, and routing through src→ix→q delivers. Pre-fix mechanism:
// BindExchange rejected the internal DESTINATION (exactly backwards) and no
// publish path checked Internal at all.
func TestInternal_PublishRejected_E2EBindingAllowed(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("ix", "direct", false, false, true, nil))
	require.NoError(t, b.DeclareExchange("src", "direct", false, false, false, nil))
	mustDeclareQueue(t, b, "iq")

	// Binding src→ix (destination internal) is ALLOWED today (was refused).
	require.NoError(t, b.BindExchange("ix", "src", "k", nil), "e2e into an internal exchange is the supported use")
	require.NoError(t, b.BindQueue("iq", "ix", "k", nil))

	// Publishing to src routes through ix into iq.
	require.NoError(t, b.PublishMessage("src", "k", &protocol.Message{Exchange: "src", RoutingKey: "k", Body: []byte("via-e2e")}))
	msg, _, _, err := b.GetMessageForGet("iq", true)
	require.NoError(t, err)
	require.NotNil(t, msg, "message routed src→ix→iq must arrive")
	assert.Equal(t, "via-e2e", string(msg.Body))

	// Publishing DIRECTLY to ix is refused on every entry point.
	err = b.PublishMessage("ix", "k", &protocol.Message{Exchange: "ix", RoutingKey: "k", Body: []byte("direct")})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeInternal), "%v", err)
	assert.Contains(t, err.Error(), "cannot publish to internal exchange 'ix' in vhost '/'")

	_, _, err = b.PublishMessageAsyncConfirm("ix", "k", &protocol.Message{Exchange: "ix", RoutingKey: "k", Body: []byte("d"), DeliveryMode: 2}, nil, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeInternal), "%v", err)

	_, err = b.PublishMessageTx(b.storage, "ix", "k", &protocol.Message{Exchange: "ix", RoutingKey: "k", Body: []byte("d")})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeInternal), "%v", err)

	// ValidatePublishTarget (the tx pre-check seam).
	assert.NoError(t, b.ValidatePublishTarget("src"))
	verr := b.ValidatePublishTarget("ix")
	require.Error(t, verr)
	assert.True(t, errors.Is(verr, ErrExchangeInternal), "%v", verr)
	missErr := b.ValidatePublishTarget("missing")
	require.Error(t, missErr)
	assert.True(t, errors.Is(missErr, interfaces.ErrExchangeNotFound), "%v", missErr)
}

// TestDeadLetter_ToInternalDLX_Delivered (T11.3): a DLX may be an internal
// exchange — dead-lettering routes into it (R11 guards only basic.publish).
func TestDeadLetter_ToInternalDLX_Delivered(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("dlx-internal", "direct", false, false, true, nil))
	mustDeclareQueue(t, b, "dlq")
	require.NoError(t, b.BindQueue("dlq", "dlx-internal", "dl-key", nil))

	qs := declareMaxLen(t, b, "work-q", map[string]interface{}{
		"x-dead-letter-exchange":    "dlx-internal",
		"x-dead-letter-routing-key": "dl-key",
	})
	_ = qs

	// Publish then reject-without-requeue → dead-lettered into the internal DLX.
	publishBody(b, "work-q", "doomed")
	msg, tag, _, err := b.GetMessageForGet("work-q", false)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NoError(t, b.RejectGetDelivery(tag, false))

	dl, _, _, err := b.GetMessageForGet("dlq", true)
	require.NoError(t, err)
	require.NotNil(t, dl, "dead-letter into an internal DLX must be delivered")
	assert.Equal(t, "doomed", string(dl.Body))
}
