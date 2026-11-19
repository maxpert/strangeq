//go:build unix

package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/server"
	"github.com/stretchr/testify/require"
)

// This observer changes no production behavior: successful owned submissions
// delegate to the real local Node, journal, Raft and queue machine. Fail the
// fixture if a publish takes the ordinary submission route instead.
type recoveryOwnedObserver struct {
	*cluster.Node
	path string
	mu   sync.Mutex
}

func (o *recoveryOwnedObserver) Submit(ctx context.Context, queue string, data []byte) ([]byte, error) {
	command, err := clusterqueue.DecodeCommand(data)
	if err != nil {
		return nil, err
	}
	if command.Kind == clusterqueue.PublishMessage || command.Kind == clusterqueue.PublishBatch {
		return nil, errors.New("recovery fixture publish bypassed owned admission")
	}
	return o.Node.Submit(ctx, queue, data)
}

func (o *recoveryOwnedObserver) SubmitOwned(ctx context.Context, queue string, command *clusterproposal.Owned) ([]byte, error) {
	if !o.Node.SupportsOwnedPublishes() {
		return nil, errors.New("recovery fixture lost owned-publish eligibility")
	}
	reply, err := o.Node.SubmitOwned(ctx, queue, command)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	file, err := os.OpenFile(o.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	_, err = file.WriteString("owned\n")
	return reply, errors.Join(err, file.Close())
}

// Keep the exact CLI entry-point fixture alongside this observed variant. This
// child uses the same configured local process policy and unified server builder
// while exposing owned-admission evidence through a test-only forwarding wrapper.
func TestOwnedProcessDurabilityBrokerChild(t *testing.T) {
	path := os.Getenv(processDurabilityCLIConfig)
	if path == "" {
		return
	}
	cfg, _, err := config.LoadConfig(path)
	require.NoError(t, err)
	cwd, err := os.Getwd()
	require.NoError(t, err)
	cfg.ResolvePaths(cwd)
	require.NoError(t, cfg.Validate())
	node, err := openCluster(cfg, false)
	require.NoError(t, err)
	require.NotNil(t, node)
	require.True(t, node.SupportsOwnedPublishes(), "normal local process configuration must select the owned route")
	observed := &recoveryOwnedObserver{Node: node, path: path + ".owned"}
	broker, err := server.NewServerBuilder().WithConfig(cfg).WithUnifiedBroker(server.NewClusterBroker(observed)).Build()
	require.NoError(t, err)
	err = broker.Start()
	t.Fatalf("observed unified broker returned before SIGKILL: %v", err)
}
