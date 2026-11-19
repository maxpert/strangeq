package clusterwire

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type waitAdmissionTestHandler struct {
	handlerFunc
	waitCalls *atomic.Int32
}

func (h waitAdmissionTestHandler) WaitCommand(_ context.Context, body []byte) ([]byte, error) {
	h.waitCalls.Add(1)
	return body, nil
}

func TestWaitAdmissionLeavesCommandsAndRaftAvailable(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true}
	var waits, writes, replications atomic.Int32
	server, err := NewServer(cfg, waitAdmissionTestHandler{
		waitCalls: &waits,
		handlerFunc: handlerFunc{
			command: func(_ context.Context, body []byte) ([]byte, error) {
				writes.Add(1)
				return body, nil
			},
			raft: func(context.Context, uint64, []*raftpb.Message) error {
				replications.Add(1)
				return nil
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, server.Close(ctx))
	})
	client, err := NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	address := server.Addr().String()

	func() {
		reserved := 0
		defer func() {
			for range reserved {
				server.waitAdmission.release(1)
			}
		}()
		require.Positive(t, cap(server.waitAdmission.slots))
		for range cap(server.waitAdmission.slots) {
			require.True(t, server.waitAdmission.reserve(1))
			reserved++
		}
		_, err := client.Wait(ctx, address, []byte("idle"))
		require.ErrorContains(t, err, "HTTP status 429")
		require.Zero(t, waits.Load(), "the full wait queue rejects before dispatch")

		command, err := client.Command(ctx, address, []byte("command"))
		require.NoError(t, err)
		require.Equal(t, []byte("command"), command)
		require.NoError(t, client.Raft(ctx, address, 1, []*raftpb.Message{{
			Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		}}))
		require.EqualValues(t, 1, writes.Load())
		require.EqualValues(t, 1, replications.Load())
	}()

	ready, err := client.Wait(ctx, address, []byte("ready"))
	require.NoError(t, err)
	require.Equal(t, []byte("ready"), ready)
	require.EqualValues(t, 1, waits.Load())
}
