package cluster

import (
	"context"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSubmitRetriesOnlyDefinitelyUnsentCommands(t *testing.T) {
	cfg := clusterwire.Config{
		ClusterID: "routing-test", Listen: "127.0.0.1:0", Timeout: time.Second,
		Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true,
	}
	var firstCalls atomic.Int32
	first := startRoutingPeer(t, cfg, func() ([]byte, error) {
		firstCalls.Add(1)
		return nil, errors.New("command outcome unavailable after acceptance")
	})
	var secondCalls atomic.Int32
	second := startRoutingPeer(t, cfg, func() ([]byte, error) {
		secondCalls.Add(1)
		return append(make([]byte, 9), "committed"...), nil
	})
	client, err := clusterwire.NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	node := &Node{client: client, routes: make(map[uint64]uint64)}
	run := func(address string) ([]byte, error) {
		node.current.Store(&view{
			Nodes:      []Peer{{ID: 1, Address: address}, {ID: 2, Address: second}},
			Placements: []placement{{Group: 1, Voters: []uint64{1, 2}}},
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return node.trySubmit(ctx, 1, []byte("non-idempotent command"))
	}

	_, err = run(first)
	require.ErrorIs(t, err, ErrUncertain)
	require.EqualValues(t, 1, firstCalls.Load())
	require.Zero(t, secondCalls.Load(), "a command with an uncertain outcome must not be replayed")

	// Port zero is an invalid remote endpoint, so the transport rejects
	// before a connection or write could occur.
	body, err := run("127.0.0.1:0")
	require.NoError(t, err)
	require.Equal(t, "committed", string(body))
	require.EqualValues(t, 1, secondCalls.Load())
}

type routingPeer func() ([]byte, error)

func (p routingPeer) Command(context.Context, []byte) ([]byte, error)     { return p() }
func (routingPeer) Membership(context.Context, []byte) ([]byte, error)    { return nil, nil }
func (routingPeer) Raft(context.Context, uint64, []*raftpb.Message) error { return nil }

func startRoutingPeer(t *testing.T, cfg clusterwire.Config, handler routingPeer) string {
	t.Helper()
	server, err := clusterwire.NewServer(cfg, handler)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, server.Close(ctx))
	})
	return server.Addr().String()
}
