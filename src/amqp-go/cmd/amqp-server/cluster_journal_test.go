//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package main

import (
	"bytes"
	"context"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestExperimentalJournalClusterBootstrapsAndRestartsWithoutBolt(t *testing.T) {
	nodes := newTestCluster(t, 3, 3, 2)
	for i := range nodes {
		nodes[i].config.Cluster.ExperimentalRaftJournal = true
		_, err := openCluster(nodes[i].config, true)
		require.NoError(t, err)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	const queue = "journal-three-broker-restart"
	publisher := openClusterChannel(t, waitQueue(t, &nodes[0], queue, false))
	require.NoError(t, publisher.Confirm(false))
	publishClusterConfirm(t, publisher, queue, "settled-before-restart")
	consumer := openClusterChannel(t, waitQueue(t, &nodes[1], queue, true))
	delivery := getClusterMessage(t, consumer, queue, "settled-before-restart", false)
	require.NoError(t, delivery.Ack(false))
	waitClusterQueueDepth(t, &nodes[1], queue, 0, 0)
	publishClusterConfirm(t, publisher, queue, "survives-restart")
	for i := range nodes {
		nodes[i].close(t)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	restored := openClusterChannel(t, waitQueue(t, &nodes[2], queue, true))
	delivery = getClusterMessage(t, restored, queue, "survives-restart", false)
	require.NoError(t, delivery.Ack(false))
	waitClusterQueueDepth(t, &nodes[2], queue, 0, 0)

	for i := range nodes {
		dir := filepath.Join(nodes[i].config.Storage.Path, "cluster")
		var boltFiles, journals []string
		require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			switch entry.Name() {
			case "raft.db":
				boltFiles = append(boltFiles, path)
			case "extent-0000000000000001.raft-journal":
				journals = append(journals, path)
			}
			return nil
		}))
		require.Empty(t, boltFiles, "journal broker wrote bbolt Raft state")
		require.Len(t, journals, 1, "all catalog and data groups must share one node journal")
		require.FileExists(t, filepath.Join(dir, "journal-identity-v1.json"))
		require.NoFileExists(t, filepath.Join(dir, "identity.json"), "old binaries must stop at the missing legacy identity")
	}
}

func TestExperimentalAckAndClaimPersistsSettlementAndRequeuesRefill(t *testing.T) {
	nodes := newTestCluster(t, 3, 3, 2)
	for i := range nodes {
		settings := &nodes[i].config.Cluster
		settings.ExperimentalRaftJournal = true
		settings.ExperimentalAckAndClaim = true
		_, err := openCluster(nodes[i].config, true)
		require.NoError(t, err)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	const queue = "journal-refill-restart"
	const consumer = "manual-prefetch-one"
	writer := openClusterChannel(t, waitQueue(t, &nodes[0], queue, false))
	require.NoError(t, writer.Confirm(false))
	bodies := [][]byte{bytes.Repeat([]byte{0, 1}, 32<<10), bytes.Repeat([]byte{255, 0}, 32<<10)}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	for _, body := range bodies {
		confirm, err := writer.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false,
			amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent})
		require.NoError(t, err)
		require.NotNil(t, confirm)
		ok, err := confirm.WaitContext(ctx)
		require.NoError(t, err)
		require.True(t, ok)
	}

	reader := openClusterChannel(t, waitQueue(t, &nodes[1], queue, true))
	require.NoError(t, reader.Qos(1, 0, false))
	deliveries, err := reader.Consume(queue, consumer, false, false, false, false, nil)
	require.NoError(t, err)
	for i, body := range bodies {
		select {
		case delivery, ok := <-deliveries:
			require.True(t, ok)
			require.True(t, bytes.Equal(body, delivery.Body), "delivery %d must contain the original 64 KiB payload", i+1)
			require.False(t, delivery.Redelivered)
			if i == 0 {
				require.NoError(t, delivery.Ack(false))
			}
		case <-time.After(8 * time.Second):
			t.Fatalf("the broker did not deliver payload %d", i+1)
		}
	}
	require.NoError(t, reader.Cancel(consumer, false))
	waitClusterQueueDepth(t, &nodes[2], queue, 1, 0)

	for i := range nodes {
		nodes[i].close(t)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	restored := openClusterChannel(t, waitQueue(t, &nodes[2], queue, true))
	delivery, ok, err := restored.Get(queue, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, delivery.Redelivered)
	require.True(t, bytes.Equal(bodies[1], delivery.Body), "the redelivered payload must match after all brokers restart")
	require.NoError(t, delivery.Ack(false))
	waitClusterQueueDepth(t, &nodes[2], queue, 0, 0)
}
