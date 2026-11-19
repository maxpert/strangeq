package server

import (
	"bytes"
	"context"
	"math/rand/v2"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

type compactClusterTestMachine struct {
	localClusterMachine
	lastPublish []byte
}

func (*compactClusterTestMachine) SupportsJournalBodyCompression() bool { return true }

type smallBodyClusterTestMachine struct{ compactClusterTestMachine }

func (*smallBodyClusterTestMachine) SupportsJournalSmallBodyCompression() bool { return true }

func (c *compactClusterTestMachine) Submit(ctx context.Context, queue string, data []byte) ([]byte, error) {
	command, err := clusterqueue.DecodeCommand(data)
	if err == nil && command.Kind == clusterqueue.PublishMessage {
		c.lastPublish = bytes.Clone(data)
	}
	return c.localClusterMachine.Submit(ctx, queue, data)
}

func TestClusterBodyCompressionPreservesBodyAcrossSnapshot(t *testing.T) {
	state := &compactClusterTestMachine{}
	checkClusterBodyCompression(t, state, state, 64<<10)
}

func TestClusterSmallBodyCompressionPreservesBodyAcrossSnapshot(t *testing.T) {
	t.Run("legacy keeps small records raw", func(t *testing.T) {
		state := &compactClusterTestMachine{}
		body := bytes.Repeat([]byte{0xa2}, 1<<10)
		broker := NewClusterBroker(state)
		_, err := broker.DeclareQueue("jobs", true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, broker.PublishMessage("", "jobs", &protocol.Message{Body: body}))
		require.Greater(t, len(state.lastPublish), len(body))
	})
	t.Run("small packing survives snapshot with raw fallback", func(t *testing.T) {
		state := &smallBodyClusterTestMachine{}
		checkClusterBodyCompression(t, state, &state.compactClusterTestMachine, 1<<10)
	})
}

func checkClusterBodyCompression(t *testing.T, submitter ClusterSubmitter, state *compactClusterTestMachine, size int) {
	t.Helper()
	b := NewClusterBroker(submitter)
	_, err := b.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	body := bytes.Repeat([]byte{0x41, 0xe2, 0x03, 0x04}, size/4)
	incompressible := make([]byte, len(body))
	rng := rand.NewChaCha8([32]byte{0x86})
	_, err = rng.Read(incompressible)
	require.NoError(t, err)

	require.NoError(t, b.PublishMessage("", "jobs", &protocol.Message{Body: body, ContentType: "application/octet-stream"}))
	require.Less(t, len(state.lastPublish), len(body)/2, "the serving path should replicate fewer bytes")
	require.NoError(t, b.PublishMessage("", "jobs", &protocol.Message{Body: incompressible}))
	require.GreaterOrEqual(t, len(state.lastPublish), len(incompressible))
	require.Less(t, len(state.lastPublish), len(incompressible)+1024)

	snapshot, err := state.machine.Snapshot()
	require.NoError(t, err)
	restored := &compactClusterTestMachine{}
	require.NoError(t, restored.machine.Restore(snapshot))
	reader := NewClusterBroker(restored)
	for _, expected := range [][]byte{body, incompressible} {
		message, tag, _, err := reader.GetMessageForGetOwned("jobs", false, "reader")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, expected, message.Body)
		require.NoError(t, reader.AcknowledgeGetDelivery(tag))
	}
	reader.RequeueGetDeliveries("reader")
}
