package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestClusterReliableDeadLetterRecovery(t *testing.T) {
	for _, journal := range []bool{false, true} {
		t.Run(fmt.Sprintf("journal=%t", journal), func(t *testing.T) {
			const source, target = "source", "target"
			nodes := newTestCluster(t, 3, 3, 4)
			for i := range nodes {
				nodes[i].config.Cluster.ExperimentalRaftJournal = journal
				_, err := openCluster(nodes[i].config, true)
				require.NoError(t, err)
			}
			for i := range nodes {
				nodes[i].start(t, false)
			}
			leader := waitShardLeader(t, nodes, source, -1, true)
			sourceStatus, _, ok := nodes[leader].node.LocalQueueShardStatus(source)
			require.True(t, ok)
			targetStatus, _, ok := nodes[leader].node.LocalQueueShardStatus(target)
			require.True(t, ok)
			require.NotEqual(t, sourceStatus.GroupID, targetStatus.GroupID)

			channel := openClusterChannel(t, nodes[(leader+1)%len(nodes)].dial(t))
			policy := clusterDeadLetterArguments("", target)
			_, err := channel.QueueDeclare(source, true, false, false, false, policy)
			require.NoError(t, err)
			require.NoError(t, channel.Confirm(false))
			confirm, err := channel.PublishWithDeferredConfirmWithContext(context.Background(), "", source, true, false, amqp.Publishing{
				Body: bytes.Repeat([]byte("dead letter"), 7000), DeliveryMode: amqp.Transient,
				MessageId: "cluster-rejection", Headers: amqp.Table{"application": "retained"},
			})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			acked, err := confirm.WaitContext(ctx)
			cancel()
			require.NoError(t, err)
			require.True(t, acked)

			message, found, err := channel.Get(source, false)
			require.NoError(t, err)
			require.True(t, found)
			require.NoError(t, message.Reject(false))
			waitClusterQueueDepth(t, &nodes[leader], source, 0, 0)

			nodes[leader].close(t)
			next := waitShardLeader(t, nodes, source, leader, false)
			dead := openClusterChannel(t, nodes[next].dial(t))
			_, err = dead.QueueDeclare(target, true, false, false, false, nil)
			require.NoError(t, err)
			waitClusterQueueDepth(t, &nodes[next], target, 1, 0)
			delivered, found, err := dead.Get(target, false)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, message.Body, delivered.Body)
			require.Equal(t, "cluster-rejection", delivered.MessageId)
			require.Equal(t, "retained", delivered.Headers["application"])
			require.Equal(t, amqp.Persistent, delivered.DeliveryMode)
			deaths, ok := delivered.Headers["x-death"].([]interface{})
			require.True(t, ok)
			require.Len(t, deaths, 1)
			death, ok := deaths[0].(amqp.Table)
			require.True(t, ok)
			require.Equal(t, source, death["queue"])
			require.Equal(t, "rejected", death["reason"])
			require.NoError(t, delivered.Ack(false))
		})
	}
}

func clusterDeadLetterArguments(exchange, key string) amqp.Table {
	return amqp.Table{
		"x-dead-letter-exchange": exchange, "x-dead-letter-routing-key": key,
		"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish",
	}
}

func TestClusterReliableDeadLetterFanoutAndMultipleNack(t *testing.T) {
	const source, first, second, exchange = "fanout-source", "fanout-first", "fanout-second", "dead-letter-fanout"
	nodes := newTestCluster(t, 3, 3, 4)
	for i := range nodes {
		nodes[i].config.Cluster.ExperimentalRaftJournal = true
		_, err := openCluster(nodes[i].config, true)
		require.NoError(t, err)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	leader := waitShardLeader(t, nodes, source, -1, true)
	channel := openClusterChannel(t, nodes[(leader+1)%len(nodes)].dial(t))
	require.NoError(t, channel.ExchangeDeclare(exchange, "fanout", true, false, true, false, nil))
	_, err := channel.QueueDeclare(source, true, false, false, false, clusterDeadLetterArguments(exchange, "rejected"))
	require.NoError(t, err)
	_, err = channel.QueueDeclare(first, true, false, false, false, nil)
	require.NoError(t, err)
	_, err = channel.QueueDeclare(second, false, false, false, false, nil)
	require.NoError(t, err)
	for _, name := range []string{first, second} {
		require.NoError(t, channel.QueueBind(name, "", exchange, false, nil))
	}
	require.NoError(t, channel.Confirm(false))
	publishClusterConfirm(t, channel, source, "acknowledged")
	publishClusterConfirm(t, channel, source, "requeued")

	message := getClusterMessage(t, channel, source, "acknowledged", false)
	require.NoError(t, message.Ack(false))
	message = getClusterMessage(t, channel, source, "requeued", false)
	require.NoError(t, message.Nack(false, true))
	message = getClusterMessage(t, channel, source, "requeued", true)
	require.NoError(t, message.Ack(false))
	publishClusterConfirm(t, channel, source, "rejected-one")
	publishClusterConfirm(t, channel, source, "rejected-two")
	_ = getClusterMessage(t, channel, source, "rejected-one", false)
	message = getClusterMessage(t, channel, source, "rejected-two", false)
	require.NoError(t, message.Nack(true, false))
	waitClusterQueueDepth(t, &nodes[leader], source, 0, 0)
	waitClusterQueueDepth(t, &nodes[leader], first, 2, 0)
	waitClusterQueueDepth(t, &nodes[leader], second, 0, 0)

	_, err = channel.QueueDelete(second, false, false, false)
	require.NoError(t, err)
	_, err = channel.QueueDeclare(second, true, false, false, false, nil)
	require.NoError(t, err)
	require.Never(t, func() bool {
		info, err := channel.QueueInspect(second)
		return err != nil || info.Messages != 0
	}, 300*time.Millisecond, 30*time.Millisecond, "stale binding must not reach the replacement")
	require.NoError(t, channel.QueueBind(second, "", exchange, false, nil))
	waitClusterQueueDepth(t, &nodes[leader], second, 2, 0)
	waitClusterQueueDepth(t, &nodes[leader], first, 2, 0)

	for _, queue := range []string{first, second} {
		seen := make(map[string]bool)
		for range 2 {
			delivery, found, err := channel.Get(queue, false)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, exchange, delivery.Exchange)
			require.Equal(t, "rejected", delivery.RoutingKey)
			seen[string(delivery.Body)] = true
			require.NoError(t, delivery.Ack(false))
		}
		require.Equal(t, map[string]bool{"rejected-one": true, "rejected-two": true}, seen)
		waitClusterQueueDepth(t, &nodes[leader], queue, 0, 0)
	}
}

const deadLetterBrokerProcessConfig = "STRANGEQ_TEST_DLQ_BROKER_CONFIG"

// Each fixture broker has its own listeners and persistent store. Run them in
// a child process to crash every replica without executing shutdown hooks.
func TestClusterDeadLetterBrokerProcess(t *testing.T) {
	path := os.Getenv(deadLetterBrokerProcessConfig)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var configs []*config.AMQPConfig
	require.NoError(t, json.Unmarshal(data, &configs))
	nodes := make([]testClusterBroker, len(configs))
	for i, cfg := range configs {
		nodes[i].config = cfg
		nodes[i].start(t, false)
	}
	select {}
}

func TestClusterReliableDeadLetterCrashRestart(t *testing.T) {
	const source, target, sentinel = "source", "target", "cluster-ready"
	nodes := newTestCluster(t, 3, 3, 4)
	configs := make([]*config.AMQPConfig, len(nodes))
	for i := range nodes {
		nodes[i].config.Cluster.ExperimentalRaftJournal = true
		configs[i] = nodes[i].config
		_, err := openCluster(configs[i], true)
		require.NoError(t, err)
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	settings := filepath.Join(t.TempDir(), "brokers.json")
	body, err := json.Marshal(configs)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(settings, body, 0600))
	start := func() func() {
		t.Helper()
		cmd := exec.Command(executable, "-test.run=^TestClusterDeadLetterBrokerProcess$")
		cmd.Env = append(os.Environ(), deadLetterBrokerProcessConfig+"="+settings)
		output, err := os.CreateTemp(filepath.Dir(settings), "broker-*.log")
		require.NoError(t, err)
		defer output.Close()
		cmd.Stdout, cmd.Stderr = output, output
		require.NoError(t, cmd.Start())
		crash := func() {
			t.Helper()
			if cmd.ProcessState != nil {
				return
			}
			killErr := cmd.Process.Kill()
			waitErr := cmd.Wait()
			if t.Failed() || killErr != nil {
				log, err := os.ReadFile(output.Name())
				if err == nil {
					t.Log(string(log[max(0, len(log)-8192):]))
				}
			}
			if !errors.Is(killErr, os.ErrProcessDone) {
				require.NoError(t, killErr)
			}
			require.ErrorContains(t, waitErr, "killed")
		}
		t.Cleanup(crash)
		return crash
	}

	crash := start()
	channel := openClusterChannel(t, waitQueue(t, &nodes[0], sentinel, false))
	_, err = channel.QueueDeclare(source, true, false, false, false, clusterDeadLetterArguments("", target))
	require.NoError(t, err)
	require.NoError(t, channel.Confirm(false))
	payload := string(bytes.Repeat([]byte("survives a hard crash"), 3500))
	publishClusterConfirm(t, channel, source, payload)
	message := getClusterMessage(t, channel, source, payload, false)
	require.NoError(t, message.Reject(false))
	// Synchronize behind the rejection on the same channel. A committed
	// source deletion cannot recover the message from its regular queue.
	removed, err := channel.QueueDelete(source, false, true, false)
	require.NoError(t, err)
	require.Zero(t, removed)
	crash()

	start()
	reopened := openClusterChannel(t, waitQueue(t, &nodes[1], sentinel, true))
	_, err = reopened.QueueDeclare(target, true, false, false, false, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, err := reopened.QueueInspect(target)
		return err == nil && info.Messages == 1
	}, 10*time.Second, 40*time.Millisecond)
	delivery := getClusterMessage(t, reopened, target, payload, false)
	require.NoError(t, delivery.Ack(false))
}
