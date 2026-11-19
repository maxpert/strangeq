//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestExperimentalAssignmentsHonorCreditAndRecoverPendingDelivery(t *testing.T) {
	checkAssignmentCreditAndPendingDelivery(t, false, false, false)
}

func TestExperimentalReplicaBodiesHonorCreditAndRecoverPendingDelivery(t *testing.T) {
	checkAssignmentCreditAndPendingDelivery(t, true, false, false)
}

func TestExperimentalCompressedBodiesHonorCreditAndRecoverPendingDelivery(t *testing.T) {
	checkAssignmentCreditAndPendingDelivery(t, true, true, false)
}

func TestExperimentalDualStreamCompressedBodiesRecoverPendingDelivery(t *testing.T) {
	checkAssignmentCreditAndPendingDelivery(t, true, true, true)
}

func checkAssignmentCreditAndPendingDelivery(t *testing.T, replicaBodies, compressBodies, dualStream bool) {
	t.Helper()
	nodes := startAssignmentBrokers(t, replicaBodies, compressBodies, dualStream)

	const queue = "committed-assignment-credit"
	const consumer = "registered-manual"
	writer := openClusterChannel(t, waitQueue(t, &nodes[0], queue, false))
	require.NoError(t, writer.Confirm(false))
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	bodies := [][]byte{bytes.Repeat([]byte{1}, 64<<10), bytes.Repeat([]byte{2}, 64<<10), bytes.Repeat([]byte{3}, 64<<10)}
	for _, body := range bodies {
		confirm, err := writer.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false,
			amqp.Publishing{Body: body, DeliveryMode: amqp.Persistent})
		require.NoError(t, err)
		ok, err := confirm.WaitContext(ctx)
		require.NoError(t, err)
		require.True(t, ok)
	}

	readerNode := 1
	if replicaBodies {
		waitClusterQueueDepth(t, &nodes[0], queue, uint64(len(bodies)), 0)
		status, _, exists := nodes[0].node.LocalQueueShardStatus(queue)
		require.True(t, exists)
		require.NotZero(t, status.Leader)
		readerNode = int(status.Leader % uint64(len(nodes)))
	}
	reader := openClusterChannel(t, waitQueue(t, &nodes[readerNode], queue, true))
	require.NoError(t, reader.Qos(2, 0, false))
	deliveries, err := reader.Consume(queue, consumer, false, false, false, false, nil)
	require.NoError(t, err)
	next := func(want []byte) amqp.Delivery {
		t.Helper()
		select {
		case delivery, ok := <-deliveries:
			require.True(t, ok)
			require.True(t, bytes.Equal(want, delivery.Body), "assignment must retain the original 64 KiB bytes")
			require.False(t, delivery.Redelivered)
			return delivery
		case <-ctx.Done():
			t.Fatal("the broker did not send the outstanding assignment")
			return amqp.Delivery{}
		}
	}
	first, second := next(bodies[0]), next(bodies[1])
	waitClusterQueueDepth(t, &nodes[2], queue, 1, 2)
	require.NoError(t, reader.Qos(1, 0, false))
	require.NoError(t, first.Ack(false))
	waitClusterQueueDepth(t, &nodes[2], queue, 1, 1)
	select {
	case <-deliveries:
		t.Fatal("credit reduction allowed a third delivery while one flight remained")
	default:
	}
	require.NoError(t, second.Ack(false))
	_ = next(bodies[2])
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
	require.True(t, bytes.Equal(bodies[2], delivery.Body), "the outstanding assignment must requeue exact bytes")
	require.NoError(t, delivery.Ack(false))
	waitClusterQueueDepth(t, &nodes[2], queue, 0, 0)
}

func startAssignmentBrokers(t *testing.T, replicaBodies, compressBodies, dualStream bool) []testClusterBroker {
	t.Helper()
	nodes := newTestCluster(t, 3, 3, 2)
	for i := range nodes {
		settings := &nodes[i].config.Cluster
		settings.ExperimentalRaftRangeSync = dualStream
		settings.ExperimentalRaftJournalDualStream = dualStream
		settings.ExperimentalAssignments = true
		settings.ExperimentalReplicaBodies = replicaBodies
		settings.ExperimentalJournalBodyCompression = compressBodies
		_, err := openCluster(nodes[i].config, true)
		require.NoError(t, err)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	return nodes
}

func TestExperimentalAssignmentsRecoverAnUnsentFlightAcrossLeaderLossAndRestart(t *testing.T) {
	nodes := startAssignmentBrokers(t, false, false, false)
	const queue = "durable-unsent-assignment"
	apply := func(index int, command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		t.Helper()
		encoded, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		wire, err := nodes[index].node.Submit(ctx, queue, encoded)
		require.NoError(t, err)
		result, err := clusterqueue.DecodeResponse(wire)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		return result
	}
	queueInfo := apply(0, clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: queue,
		QueueSpec: clusterqueue.QueueSpec{Durable: true}}).Queue
	open := clusterqueue.QueueCommand{
		Kind: clusterqueue.OpenQueueSession, Queue: queue, Incarnation: queueInfo.Incarnation,
		Session: queueInfo.NextSession, Owner: "durable-owner", Prefetch: 1,
		Consumer: true, AutoAssign: true, Deadline: time.Now().Add(2 * time.Minute).UnixNano(),
	}
	apply(1, open)
	body := bytes.Repeat([]byte{0x2a}, 64<<10)
	message := apply(0, clusterqueue.QueueCommand{
		Kind: clusterqueue.PublishMessage, Queue: queue, Incarnation: open.Incarnation, Body: body,
	}).Result.MessageID
	request := clusterqueue.AssignmentRequest{
		Queue: queue, Incarnation: open.Incarnation, Session: open.Session, Owner: open.Owner,
		Limit: 1, Bytes: clusterqueue.MaxClaimBatchBytes,
	}
	waitClusterQueueDepth(t, &nodes[0], queue, 0, 1)
	status, _, exists := nodes[0].node.LocalQueueShardStatus(queue)
	require.True(t, exists)
	require.Positive(t, status.Leader)
	leader := int(status.Leader - 1)
	require.Less(t, leader, len(nodes))
	nodes[leader].close(t)

	read := func(index int) clusterqueue.AttemptID {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		page, cursor, err := nodes[index].node.ReadAssignmentPage(ctx, request)
		require.NoError(t, err)
		require.Len(t, page, 1)
		require.Equal(t, message, page[0].MessageID)
		require.Equal(t, cursor, page[0].Attempt)
		require.Equal(t, body, page[0].Body)
		require.False(t, page[0].Redelivered)
		return cursor
	}
	survivor := (leader + 1) % len(nodes)
	cursor := read(survivor)
	for i := range nodes {
		nodes[i].close(t)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := range nodes {
		waitClusterBroker(t, &nodes[i])
	}
	require.Equal(t, cursor, read((survivor+1)%len(nodes)), "the same cursor retrieves the same durable attempt")
	ack := clusterqueue.QueueCommand{
		Kind: clusterqueue.AckMessage, Queue: queue, Incarnation: open.Incarnation,
		Session: open.Session, Owner: open.Owner, Sequence: 1, Attempt: cursor,
	}
	apply(0, ack)
	apply(1, ack)
	waitClusterQueueDepth(t, &nodes[2], queue, 0, 0)
}
