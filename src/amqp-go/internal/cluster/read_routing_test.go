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
)

func TestQueryRetriesAfterPeerAcceptedReadWithoutSubmittingCommand(t *testing.T) {
	config := clusterwire.Config{
		ClusterID: "read-routing-test", Listen: "127.0.0.1:0", Timeout: time.Second,
		Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true,
	}
	var commandCalls, firstCalls, secondCalls atomic.Int32
	start := func(handle routingPeer) string {
		peer := readRoutingPeer{
			routingPeer: func() ([]byte, error) {
				commandCalls.Add(1)
				return nil, errors.New("must use labeled read endpoint")
			},
			read: handle,
		}
		server, err := clusterwire.NewServer(config, peer)
		require.NoError(t, err)
		require.NoError(t, server.Start())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, server.Close(ctx))
		})
		return server.Addr().String()
	}
	first := start(func() ([]byte, error) {
		firstCalls.Add(1)
		return nil, errors.New("peer accepted the read but could not complete it")
	})
	second := start(func() ([]byte, error) {
		secondCalls.Add(1)
		return encodeReply(reply{body: []byte("fenced read")}), nil
	})
	client, err := clusterwire.NewClient(config)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	node := &Node{client: client, routes: make(map[uint64]uint64)}
	node.current.Store(&view{
		Nodes:      []Peer{{ID: 1, Address: first}, {ID: 2, Address: second}},
		Placements: []placement{{Group: 1, Voters: []uint64{1, 2}}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, err := node.tryQuery(ctx, 1, []byte("read query"))
	require.NoError(t, err)
	require.Equal(t, "fenced read", string(body))
	require.Equal(t, int32(1), firstCalls.Load(), "initial peer must see the read before failing")
	require.Equal(t, int32(1), secondCalls.Load())
	require.Zero(t, commandCalls.Load(), "read retries cannot issue a write command")
}

type readRoutingPeer struct {
	routingPeer
	read routingPeer
}

func (p readRoutingPeer) ReadCommand(context.Context, []byte) ([]byte, error) { return p.read() }
