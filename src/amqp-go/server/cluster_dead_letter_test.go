package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

type deadLetterCancelFixture struct {
	localClusterMachine
	cancel   context.CancelFunc
	attempts int
}

func (*deadLetterCancelFixture) WorkShard(string) uint64 { return 2 }
func (*deadLetterCancelFixture) LocalWorkShards(func(clusterraft.Machine) bool) []uint64 {
	return []uint64{1}
}

func (f *deadLetterCancelFixture) ReadLocalWork(_ context.Context, _ uint64, capture func(clusterraft.Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	encode, _, err := capture(&f.machine)
	if err != nil {
		return nil, err
	}
	return encode()
}

func (f *deadLetterCancelFixture) SubmitLocalWork(ctx context.Context, _ uint64, data []byte) ([]byte, error) {
	return f.localClusterMachine.Submit(ctx, "", data)
}

func (f *deadLetterCancelFixture) Query(_ context.Context, _ string, data []byte) ([]byte, error) {
	return f.machine.Query(data)
}

func (f *deadLetterCancelFixture) Submit(ctx context.Context, key string, data []byte) ([]byte, error) {
	if f.cancel != nil {
		f.attempts++
		f.cancel()
		return nil, ctx.Err()
	}
	return f.localClusterMachine.Submit(ctx, key, data)
}

func TestClusterDeadLetterCancellationStopsWideFanout(t *testing.T) {
	const source, dlx = "source", "dlx"
	f := &deadLetterCancelFixture{}
	b := NewClusterBroker(f)
	ctx := context.Background()
	_, err := b.DeclareQueue(source, true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": dlx, "x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish",
	})
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange(dlx, "fanout", true, false, false, nil))
	for i := range 64 {
		name := fmt.Sprintf("destination-%02d", i)
		_, err := b.DeclareQueue(name, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(name, dlx, "", nil))
	}
	require.NoError(t, b.PublishMessage("", source, &protocol.Message{Body: []byte("rejected")}))
	_, tag, _, err := b.GetMessageForGetOwned(source, false, "reader")
	require.NoError(t, err)
	t.Cleanup(func() { b.RequeueGetDeliveries("reader") })
	require.NoError(t, b.NackGetDelivery(tag, false))

	// Install the plan through the normal worker. The next pass cancels as
	// soon as its first target is contacted.
	cursor, progress := b.advanceClusterDeadLetter(ctx, f, 1, deadLetterCursor{})
	require.True(t, progress)
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	f.cancel = cancel
	_, progress = b.advanceClusterDeadLetter(attempt, f, 1, cursor)
	f.cancel = nil
	require.False(t, progress)
	require.Equal(t, 1, f.attempts)
	require.True(t, f.machine.queues.HasDeadLetters())
}
