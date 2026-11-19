package cluster

import (
	"context"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
)

type testQueueReadinessWaiter interface {
	WaitQueueReady(context.Context, string, clusterqueue.QueueID) error
}

type readinessRoutingPeer struct {
	routingPeer
	wait      func(context.Context, []byte) ([]byte, error)
	readCalls *atomic.Int32
}

func (p readinessRoutingPeer) WaitCommand(ctx context.Context, request []byte) ([]byte, error) {
	return p.wait(ctx, request)
}

func (p readinessRoutingPeer) ReadCommand(context.Context, []byte) ([]byte, error) {
	p.readCalls.Add(1)
	return nil, errors.New("readiness should use its own advisory endpoint")
}

func TestWaitQueueReadyRoutesToPeerAndPropagatesCancellation(t *testing.T) {
	for _, cancelParked := range []bool{false, true} {
		name := "ready"
		if cancelParked {
			name = "request_canceled"
		}
		t.Run(name, func(t *testing.T) {
			received := make(chan []byte, 1)
			ready := make(chan struct{})
			stopped := make(chan struct{}, 1)
			var commands, reads atomic.Int32
			config := clusterwire.Config{ClusterID: "wait-routing", Listen: "127.0.0.1:0",
				Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true, Timeout: 3 * time.Second}
			peer, err := clusterwire.NewServer(config, readinessRoutingPeer{
				routingPeer: func() ([]byte, error) {
					commands.Add(1)
					return nil, errors.New("readiness must not submit a command")
				},
				readCalls: &reads,
				wait: func(ctx context.Context, request []byte) ([]byte, error) {
					select {
					case received <- append([]byte(nil), request...):
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					select {
					case <-ctx.Done():
						select {
						case stopped <- struct{}{}:
						default:
						}
						return nil, ctx.Err()
					case <-ready:
						return encodeReply(reply{}), nil
					}
				},
			})
			require.NoError(t, err)
			require.NoError(t, peer.Start())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				require.NoError(t, peer.Close(ctx))
			})
			client, err := clusterwire.NewClient(config)
			require.NoError(t, err)
			t.Cleanup(client.Close)

			ingress := Peer{ID: 1, Address: "127.0.0.1:1"}
			owner := Peer{ID: 2, Address: peer.Addr().String()}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			node := &Node{cfg: Config{Peer: ingress, Shards: 1}, nonce: "ingress", ctx: ctx,
				client: client, routes: make(map[uint64]uint64)}
			node.current.Store(&view{Nodes: []Peer{ingress, owner}, Nonces: map[uint64]string{ingress.ID: node.nonce},
				Placements: []placement{{Group: 1, Voters: []uint64{owner.ID}}}})
			require.NoError(t, node.Ready())

			waitCtx, stopWait := context.WithCancel(ctx)
			defer stopWait()
			done := make(chan error, 1)
			go func() {
				waiter, ok := any(node).(testQueueReadinessWaiter)
				if !ok {
					done <- errors.New("node has no advisory queue wait")
					return
				}
				done <- waiter.WaitQueueReady(waitCtx, "jobs", 17)
			}()
			select {
			case request := <-received:
				require.NotEmpty(t, request, "a non-replica ingress must contact the owner")
			case err := <-done:
				t.Fatalf("wait returned before reaching the peer: %v", err)
			case <-ctx.Done():
				t.Fatal("peer did not receive the advisory wait")
			}
			require.Zero(t, commands.Load())
			require.Zero(t, reads.Load())
			if cancelParked {
				stopWait()
				select {
				case <-stopped:
				case <-ctx.Done():
					t.Fatal("canceling locally must release the parked peer")
				}
			} else {
				close(ready)
			}
			select {
			case err := <-done:
				if cancelParked {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			case <-ctx.Done():
				t.Fatal("advisory wait did not finish")
			}
			require.Zero(t, commands.Load())
			require.Zero(t, reads.Load())
		})
	}
}
