package protocol

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type countedDeliveryBodyLease struct{ calls atomic.Int32 }

func (l *countedDeliveryBodyLease) Release() { l.calls.Add(1) }

func TestDeliveryBodyReleaseClearsAliasAndTransfersOneLease(t *testing.T) {
	lease := &countedDeliveryBodyLease{}
	message := &Message{Body: []byte("immutable")}
	message.SetBodyRelease(lease.Release)
	delivery := &Delivery{Message: message}
	delivery.ReleaseBody()
	delivery.ReleaseBody()
	require.Nil(t, delivery.Message.Body)
	require.Nil(t, message.releaseBody)
	require.EqualValues(t, 1, lease.calls.Load())
	ordinary := &Delivery{Message: &Message{Body: []byte("portable")}}
	ordinary.ReleaseBody()
	require.Equal(t, []byte("portable"), ordinary.Message.Body)
}

func TestMessageBodyReleaseClearsBeforeReentrantCallback(t *testing.T) {
	message := &Message{Body: []byte("borrowed")}
	calls := 0
	message.SetBodyRelease(func() {
		calls++
		if message.releaseBody != nil || message.Body != nil {
			t.Error("owner aliases must clear before releasing storage")
		}
		message.ReleaseBody()
	})
	message.ReleaseBody()
	message.ReleaseBody()
	require.Equal(t, 1, calls)
}
