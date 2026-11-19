package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// This is an in-process broker/Node failure, rather than a process-kill or
// network-partition experiment. The surviving gateway and AMQP session stay live.
func TestClusterAMQPInProcessLeaderLossKeepsGatewaySessionUnderLoad(t *testing.T) {
	for _, profile := range []struct {
		name        string
		assignments bool
	}{
		{name: "default-journal"},
		{name: "assignment-journal", assignments: true},
	} {
		t.Run(profile.name, func(t *testing.T) {
			testClusterAMQPInProcessLeaderLossKeepsGatewaySessionUnderLoad(t, profile.assignments)
		})
	}
}

func testClusterAMQPInProcessLeaderLossKeepsGatewaySessionUnderLoad(t *testing.T, assignments bool) {
	const queue = "live-leader-loss"
	nodes := newTestCluster(t, 3, 3, 1)
	for i := range nodes {
		nodes[i].config.Cluster.ExperimentalAssignments = assignments
		_, err := openCluster(nodes[i].config, true)
		require.NoError(t, err)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	victim := waitShardLeader(t, nodes, queue, -1, true)
	gateway := (victim + 1) % len(nodes)
	connection := nodes[gateway].dial(t)
	publisher := openClusterChannel(t, connection)
	reader := openClusterChannel(t, connection)
	connectionClosed := connection.NotifyClose(make(chan *amqp.Error, 1))
	publisherClosed := publisher.NotifyClose(make(chan *amqp.Error, 1))
	readerClosed := reader.NotifyClose(make(chan *amqp.Error, 1))
	consumerCanceled := reader.NotifyCancel(make(chan string, 1))
	returned := publisher.NotifyReturn(make(chan amqp.Return, 1))
	_, err := publisher.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, publisher.Confirm(false))
	require.NoError(t, reader.Qos(8, 0, false))
	deliveries, err := reader.Consume(queue, "surviving-gateway-consumer", false, false, false, false, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	type publication struct {
		id                 string
		body               []byte
		confirmation       *amqp.DeferredConfirmation
		postLoss, positive bool
		seen               int
	}
	type confirmation struct {
		id       string
		positive bool
		err      error
		at       time.Time
	}
	publications := make(map[string]*publication)
	confirmations := make(chan confirmation, 1024)
	unresolved := 0
	issue := func(postLoss bool) *publication {
		id := fmt.Sprintf("live-failover-%04d", len(publications))
		body := bytes.Repeat([]byte(id), 1024)
		deferred, err := publisher.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, MessageId: id, Body: body})
		require.NoError(t, err, "the original surviving publisher channel must remain usable")
		require.NotNil(t, deferred)
		item := &publication{id: id, body: body, confirmation: deferred, postLoss: postLoss}
		publications[id] = item
		unresolved++
		go func() {
			wait, stop := context.WithTimeout(ctx, 6*time.Second)
			defer stop()
			positive, err := deferred.WaitContext(wait)
			confirmations <- confirmation{id: id, positive: positive, err: err, at: time.Now()}
			if err != nil {
				// A local timeout observes uncertainty; it does not settle the
				// wire tag. Keep observing the same publication without retrying.
				select {
				case <-deferred.Done():
					confirmations <- confirmation{id: id, positive: deferred.Acked(), at: time.Now()}
				case <-ctx.Done():
				}
			}
		}()
		return item
	}

	// Prove the existing consumer is active and retain a real, replicated
	// outstanding delivery so settlement also crosses the leader loss.
	warmup := issue(false)
	select {
	case confirmed := <-confirmations:
		require.Equal(t, warmup.id, confirmed.id)
		require.NoError(t, confirmed.err)
		require.True(t, confirmed.positive)
		warmup.positive = true
		unresolved--
	case <-ctx.Done():
		t.Fatal("warmup publication was not confirmed")
	}
	var held amqp.Delivery
	select {
	case delivery, open := <-deliveries:
		require.True(t, open, "the original consumer must start before the outage")
		held = delivery
		require.Equal(t, warmup.id, held.MessageId)
		require.Equal(t, warmup.body, held.Body)
		warmup.seen++
	case <-ctx.Done():
		t.Fatal("warmup publication was not delivered")
	}
	waitClusterQueueDepth(t, &nodes[gateway], queue, 0, 1)
	var pending int
	for attempts := 0; attempts < 128 && pending < 4; attempts++ {
		issue(false)
		pending = 0
		for _, item := range publications {
			select {
			case <-item.confirmation.Done():
			default:
				pending++
			}
		}
	}
	require.GreaterOrEqual(t, pending, 4, "must observe actual in-flight publisher confirmations before the fault")
	status, _, ok := nodes[gateway].node.LocalQueueShardStatus(queue)
	require.True(t, ok)
	require.Equal(t, uint64(victim+1), status.Leader, "take down the actual shard leader during activity")
	t.Logf("in-process fault: leader=%d gateway=%d pending_confirms=%d outstanding_delivery=%s commit=%d applied=%d",
		victim+1, gateway+1, pending, held.MessageId, status.Commit, status.Applied)
	loss := time.Now()
	nodes[victim].close(t)
	// Send the outstanding settlement on the existing consumer channel during
	// recovery; a later empty replicated queue proves settlement completed.
	require.NoError(t, held.Ack(false))
	issue(true) // Do not wait for election or recreate any AMQP object.

	request, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: queue})
	require.NoError(t, err)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var firstConfirm, firstDelivery time.Duration
	var negative, timeoutObservations, redeliveries, postLossSent int
	postLossSent = 1
	settled := false
	for !settled {
		select {
		case err := <-connectionClosed:
			t.Fatalf("surviving gateway connection closed during leader loss: %v", err)
		case err := <-publisherClosed:
			t.Fatalf("surviving confirm publisher channel closed during leader loss: %v", err)
		case err := <-readerClosed:
			t.Fatalf("surviving consumer channel closed during leader loss: %v", err)
		case consumer := <-consumerCanceled:
			t.Fatalf("surviving consumer was canceled during leader loss: %s", consumer)
		case message := <-returned:
			t.Fatalf("publication %s became unroutable during leader loss: %s", message.MessageId, message.ReplyText)
		case confirmed := <-confirmations:
			if confirmed.err != nil {
				timeoutObservations++
				continue // Keep this wire tag outstanding until its actual reply.
			}
			unresolved--
			item := publications[confirmed.id]
			if !confirmed.positive {
				negative++ // A negative confirm also makes no claim of delivery.
			} else {
				item.positive = true
				if item.postLoss && firstConfirm == 0 {
					firstConfirm = confirmed.at.Sub(loss)
				}
			}
		case delivery, open := <-deliveries:
			require.True(t, open, "the same consumer stream must survive leader loss")
			item := publications[delivery.MessageId]
			require.NotNil(t, item, "delivery must match an original unique publication ID")
			require.Equal(t, item.body, delivery.Body)
			if item.seen > 0 {
				require.True(t, delivery.Redelivered, "duplicate delivery must identify its uncertain prior settlement")
				redeliveries++
			}
			item.seen++
			if item.postLoss && firstDelivery == 0 {
				firstDelivery = time.Since(loss)
			}
			require.NoError(t, delivery.Ack(false))
		case <-tick.C:
			if firstConfirm == 0 || firstDelivery == 0 {
				if unresolved < 32 && postLossSent < 512 {
					issue(true) // New IDs, never retries of uncertain publications.
					postLossSent++
				}
				continue
			}
			if unresolved != 0 {
				continue
			}
			allWireSettled, allPositiveDelivered := true, true
			for _, item := range publications {
				select {
				case <-item.confirmation.Done():
					item.positive = item.confirmation.Acked()
				default:
					allWireSettled = false
				}
				allPositiveDelivered = allPositiveDelivered && (!item.positive || item.seen > 0)
			}
			if !allWireSettled || !allPositiveDelivered {
				continue
			}
			probe, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			raw, err := nodes[gateway].node.Submit(probe, queue, request)
			stop()
			if err == nil {
				response, err := clusterqueue.DecodeResponse(raw)
				require.NoError(t, err)
				settled = response.Err() == nil && response.Queue.Ready == 0 && response.Queue.Unacknowledged == 0
			}
		case <-ctx.Done():
			var missing []string
			var pendingWireTags []uint64
			for id, item := range publications {
				select {
				case <-item.confirmation.Done():
				default:
					pendingWireTags = append(pendingWireTags, item.confirmation.DeliveryTag)
				}
				if (item.positive || item.confirmation.Acked()) && item.seen == 0 {
					missing = append(missing, id)
				}
			}
			t.Fatalf("same-session recovery incomplete: first_positive_confirm=%s first_delivery=%s pending_wire_tags=%v awaiting_result_observations=%d negative=%d local_timeout_observations=%d missing_positive_ids=%v settled=%t",
				firstConfirm, firstDelivery, pendingWireTags, unresolved, negative, timeoutObservations, missing, settled)
		}
	}
	require.False(t, connection.IsClosed())
	require.False(t, publisher.IsClosed())
	require.False(t, reader.IsClosed())
	t.Logf("in-process leader-loss recovery on original gateway session: first post-loss positive confirm=%s, first post-loss delivery=%s, unique publications=%d, negative confirms=%d, local timeout observations=%d, allowed redeliveries=%d; every wire confirm resolved, all positive IDs delivered and queue settlements committed",
		firstConfirm, firstDelivery, len(publications), negative, timeoutObservations, redeliveries)
}

func TestClusterAMQPThreeBrokerFailover(t *testing.T) {
	for _, role := range []string{"leader", "follower"} {
		t.Run(role, func(t *testing.T) {
			const queue = "failover"
			nodes := newTestCluster(t, 3, 3, 1)
			for i := range nodes {
				_, err := openCluster(nodes[i].config, true)
				require.NoError(t, err)
			}
			for i := range nodes {
				nodes[i].start(t, false)
			}
			leader := waitShardLeader(t, nodes, queue, -1, true)
			victim := leader
			if role == "follower" {
				victim = (leader + 1) % len(nodes)
			}
			publisher := openClusterChannel(t, nodes[leader].dial(t))
			_, err := publisher.QueueDeclare(queue, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, publisher.Confirm(false))
			publishClusterConfirm(t, publisher, queue, "before-outage")

			nodes[victim].close(t)
			active := waitShardLeader(t, nodes, queue, victim, false)
			if role == "leader" {
				require.NotEqual(t, leader, active)
			} else {
				require.Equal(t, leader, active)
			}
			follower := 3 - victim - active // The indices of the three brokers sum to three.
			writer := openClusterChannel(t, nodes[follower].dial(t))
			require.NoError(t, writer.Confirm(false))
			publishClusterConfirm(t, writer, queue, "during-outage")
			reader := openClusterChannel(t, nodes[active].dial(t))
			message := getClusterMessage(t, reader, queue, "before-outage", false)
			require.NoError(t, message.Ack(false))
			waitClusterQueueDepth(t, &nodes[active], queue, 1, 0)
			checkpoint, _, ok := nodes[active].node.LocalQueueShardStatus(queue)
			require.True(t, ok)

			nodes[victim].start(t, false)
			waitShardLeader(t, nodes, queue, -1, true)
			require.Eventually(t, func() bool {
				status, _, ok := nodes[victim].node.LocalQueueShardStatus(queue)
				return ok && status.Applied >= checkpoint.Applied
			}, 5*time.Second, 20*time.Millisecond, "returning broker must apply the outage writes and acknowledgment")
			returned := openClusterChannel(t, nodes[victim].dial(t))
			message = getClusterMessage(t, returned, queue, "during-outage", false)
			require.NoError(t, message.Ack(false))
			waitClusterQueueDepth(t, &nodes[victim], queue, 0, 0)
			require.NoError(t, returned.Confirm(false))
			publishClusterConfirm(t, returned, queue, "after-rejoin")
			message = getClusterMessage(t, reader, queue, "after-rejoin", false)
			require.NoError(t, message.Ack(false))
			waitClusterQueueDepth(t, &nodes[active], queue, 0, 0)
			_, found, err := reader.Get(queue, false)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func waitShardLeader(t *testing.T, nodes []testClusterBroker, queue string, offline int, preferAssignedLeader bool) int {
	t.Helper()
	var leader uint64
	require.Eventually(t, func() bool {
		leader = 0
		for i := range nodes {
			if i == offline {
				continue
			}
			status, preferred, ok := nodes[i].node.LocalQueueShardStatus(queue)
			if !ok || status.Leader == 0 || len(status.Voters) != len(nodes) || int(status.Leader)-1 == offline {
				return false
			}
			if preferAssignedLeader && status.Leader != preferred {
				return false
			}
			if leader != 0 && leader != status.Leader {
				return false
			}
			leader = status.Leader
		}
		return leader > 0 && leader <= uint64(len(nodes))
	}, 5*time.Second, 20*time.Millisecond, "online brokers must agree on an online leader")
	return int(leader) - 1
}
