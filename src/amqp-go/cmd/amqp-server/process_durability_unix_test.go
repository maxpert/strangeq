//go:build unix

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const processDurabilityCLIConfig = "STRANGEQ_TEST_PROCESS_DURABILITY_CLI_CONFIG"

// Use the production entry point, including config loading and engine selection.
// The parent kills this process without running broker cleanup hooks.
func TestProcessDurabilityCLIChild(t *testing.T) {
	path := os.Getenv(processDurabilityCLIConfig)
	if path == "" {
		return
	}
	os.Args = []string{"amqp-server", "--config", path}
	main()
	t.Fatal("broker CLI returned before the parent sent SIGKILL")
}

func TestLocalProcessDurabilitySIGKILLRecovery(t *testing.T) {
	const queue = "process-kill-64k"
	const count, settled = 16, 4
	nodes := newTestCluster(t, 1, 1, 1)
	cfg := nodes[0].config
	cfg.Cluster = config.ClusterConfig{Local: true, Shards: 1}
	cfg.Storage.Durability = interfaces.DurabilityProcess
	_, port, err := net.SplitHostPort(cfg.Network.Address)
	require.NoError(t, err)
	cfg.Network.Port, err = strconv.Atoi(port)
	require.NoError(t, err)
	settings := filepath.Join(t.TempDir(), "broker.yaml")
	require.NoError(t, cfg.Save(settings))
	executable, err := os.Executable()
	require.NoError(t, err)
	start := func() func() {
		t.Helper()
		cmd := exec.Command(executable, "-test.run=^TestProcessDurabilityCLIChild$")
		cmd.Env = append(os.Environ(), processDurabilityCLIConfig+"="+settings)
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
			killErr := cmd.Process.Signal(syscall.SIGKILL)
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
			var exit *exec.ExitError
			require.ErrorAs(t, waitErr, &exit)
			status, ok := exit.Sys().(syscall.WaitStatus)
			require.True(t, ok)
			require.True(t, status.Signaled())
			require.Equal(t, syscall.SIGKILL, status.Signal())
		}
		t.Cleanup(crash)
		return crash
	}
	bodyFor := func(i int) []byte {
		body := make([]byte, 64<<10)
		for j := range body {
			body[j] = byte((j*31 + i*17) % 251)
		}
		binary.BigEndian.PutUint64(body, uint64(i))
		return body
	}
	idFor := func(i int) string { return fmt.Sprintf("confirmed-%02d", i) }
	crash := start()
	writer := openClusterChannel(t, waitQueue(t, &nodes[0], queue, false))
	require.NoError(t, writer.Confirm(false))
	for i := range count {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		confirm, err := writer.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, MessageId: idFor(i), Body: bodyFor(i),
		})
		require.NoError(t, err)
		require.NotNil(t, confirm)
		ack, err := confirm.WaitContext(ctx)
		cancel()
		require.NoError(t, err)
		require.True(t, ack, "message %d must be positively confirmed before killing the process", i)
	}
	consume := func(channel *amqp.Channel, first, last int) {
		t.Helper()
		for i := first; i < last; i++ {
			message, found, err := channel.Get(queue, false)
			require.NoError(t, err)
			require.True(t, found, "confirmed message %d is missing", i)
			require.Equal(t, idFor(i), message.MessageId)
			require.Len(t, message.Body, 64<<10)
			require.Equal(t, bodyFor(i), message.Body, "all 64 KiB must survive unchanged")
			require.NoError(t, message.Ack(false))
		}
	}
	settlementBarrier := func(channel *amqp.Channel, remaining int) {
		t.Helper()
		// The synchronous passive declaration follows all acks on this channel;
		// its response fences the broker's required settlement operation.
		info, err := channel.QueueInspect(queue)
		require.NoError(t, err)
		require.Equal(t, remaining, info.Messages)
	}
	consume(writer, 0, settled)
	settlementBarrier(writer, count-settled)
	crash()

	crash = start()
	reader := openClusterChannel(t, waitQueue(t, &nodes[0], queue, true))
	settlementBarrier(reader, count-settled)
	consume(reader, settled, count)
	settlementBarrier(reader, 0)
	crash()

	start()
	reader = openClusterChannel(t, waitQueue(t, &nodes[0], queue, true))
	settlementBarrier(reader, 0)
	_, found, err := reader.Get(queue, false)
	require.NoError(t, err)
	require.False(t, found, "required settlements must survive the second SIGKILL and restart")
}
