package server

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInternal_TxPublishRejectedAtPublishTime (T11.2, R11): on a tx channel,
// publishing to an INTERNAL exchange must fail IMMEDIATELY with a 403 channel
// close (class 60 method 40) at publish time — NOT be buffered into the tx
// and reported at commit. Pre-fix mechanism: the tx branch buffered every
// publish unconditionally, and no publish path checked Internal.
func TestInternal_TxPublishRejectedAtPublishTime(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Declare an internal exchange.
	declareX := &protocol.ExchangeDeclareMethod{Exchange: "ix", Type: "direct", Internal: true}
	payload, err := declareX.Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleExchangeDeclare(conn, 1, payload))
	drainFrame(t, frameCh) // exchange.declare-ok

	// tx.select.
	txSelect(t, srv, conn, frameCh, 1)

	// Publish to the internal exchange on the tx channel. WS-B/S2 contract:
	// the handler RETURNS the typed *ChannelError (the close is already on the
	// wire, sent=true) instead of reporting it only via the socket.
	pending := makePendingMessage("ix", "k", "payload")
	err = srv.processCompleteMessage(conn, 1, pending)
	require.Error(t, err, "the handler returns the typed channel error")
	var chErr *ChannelError
	require.ErrorAs(t, err, &chErr)
	assert.Equal(t, uint16(403), chErr.Code)

	// Expect channel.close with 403 ACCESS_REFUSED, class 60 method 40.
	// ChannelCloseMethod has no Deserialize; the payload is reply-code (2) +
	// reply-text (shortstr) + class-id (2) + method-id (2).
	classID, methodID, closePayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID, "channel class")
	assert.Equal(t, uint16(40), methodID, "channel.close")
	require.GreaterOrEqual(t, len(closePayload), 3)
	replyCode := uint16(closePayload[0])<<8 | uint16(closePayload[1])
	textLen := int(closePayload[2])
	require.GreaterOrEqual(t, len(closePayload), 3+textLen+4)
	replyText := string(closePayload[3 : 3+textLen])
	closeClassID := uint16(closePayload[3+textLen])<<8 | uint16(closePayload[3+textLen+1])
	closeMethodID := uint16(closePayload[3+textLen+2])<<8 | uint16(closePayload[3+textLen+3])
	assert.Equal(t, uint16(403), replyCode)
	assert.Contains(t, replyText, "cannot publish to internal exchange 'ix' in vhost '/'")
	assert.Equal(t, uint16(60), closeClassID)
	assert.Equal(t, uint16(40), closeMethodID)

	// Nothing was buffered into the transaction.
	ops, oerr := srv.txm(conn).GetPendingOperations(1)
	require.NoError(t, oerr)
	assert.Empty(t, ops, "a refused publish must not be buffered (R11: the check runs before buffering)")
}
