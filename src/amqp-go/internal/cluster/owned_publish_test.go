package cluster

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
)

func testOwnedCommand(t *testing.T, body string) *clusterproposal.Owned {
	t.Helper()
	allocation := make([]byte, clusterproposal.PrefixBytes+len(body))
	copy(allocation[clusterproposal.PrefixBytes:], body)
	owned, err := clusterproposal.Adopt(allocation)
	require.NoError(t, err)
	return owned
}

func TestOwnedPublishNodeRoutingPreservesOrderAndStandardFallback(t *testing.T) {
	for _, process := range []bool{true, false} {
		name := "full-durability-fallback"
		if process {
			name = "local-process"
		}
		t.Run(name, func(t *testing.T) {
			cfg := localTestConfig(t)
			cfg.ProcessDurability = process
			node, err := OpenLocal(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = node.Close() })
			require.Eventually(t, func() bool { g := node.group(1); return g != nil && g.Leader() == node.cfg.Peer.ID }, 5*time.Second, 10*time.Millisecond)
			require.Equal(t, process, node.SupportsOwnedPublishes())
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err = node.Submit(ctx, "jobs", []byte("before"))
			require.NoError(t, err)
			owned := testOwnedCommand(t, "owned")
			reply, err := node.SubmitOwned(ctx, "jobs", owned)
			require.NoError(t, err)
			require.JSONEq(t, `["before","owned"]`, string(reply))
			_, err = node.SubmitOwned(ctx, "jobs", owned)
			require.ErrorIs(t, err, clusterproposal.ErrConsumed)
			reply, err = node.Submit(ctx, "jobs", []byte("after"))
			require.NoError(t, err)
			require.JSONEq(t, `["before","owned","after"]`, string(reply))
		})
	}
}

type uncertainOwnedMachine struct {
	testMachine
	calls int
}

func (m *uncertainOwnedMachine) ApplyJournaled(data []byte, _ bool, _ io.ReaderAt) ([]byte, error) {
	m.calls++
	m.Apply(data)
	return nil, errors.New("injected committed apply result failure")
}

func TestOwnedPublishNodeDoesNotRetryAmbiguousResult(t *testing.T) {
	cfg := localTestConfig(t)
	cfg.ProcessDurability = true
	machine := &uncertainOwnedMachine{}
	cfg.NewMachine = func(uint64) clusterraft.Machine { return machine }
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	require.Eventually(t, func() bool { g := node.group(1); return g != nil && g.Leader() == node.cfg.Peer.ID }, 5*time.Second, 10*time.Millisecond)
	owned := testOwnedCommand(t, "only-once")
	_, err = node.SubmitOwned(t.Context(), "jobs", owned)
	require.ErrorIs(t, err, ErrUncertain)
	require.Equal(t, 1, machine.calls, "a committed ambiguous apply must not reenter routing")
	_, err = node.SubmitOwned(t.Context(), "jobs", owned)
	require.ErrorIs(t, err, clusterproposal.ErrConsumed)
	require.Equal(t, 1, machine.calls)
}
