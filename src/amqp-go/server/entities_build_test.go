package server

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// observerlessBroker embeds a UnifiedBroker so it satisfies the interface but
// does NOT promote SetEntityObserver — that seam is optional and deliberately
// not part of UnifiedBroker, which is exactly the "custom broker" case.
type observerlessBroker struct {
	UnifiedBroker
}

// PublishMessageAdmitted forwards to the wrapped broker so Build's
// lease-enabled NonBlockingPublisher check passes (same pattern as the other
// test wrappers).
func (b observerlessBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	return b.UnifiedBroker.(NonBlockingPublisher).PublishMessageAdmitted(exchangeName, routingKey, message)
}

// TestBuild_SeedsEntityCounts pins mgmt-03 §5: Build runs InitEntityCounters
// AFTER recovery, so a queue recovered from disk is present in the snapshot
// with zero observer events (recovery runs before the observer is wired).
// Counting it twice — once by a recovery-time observer event and once by the
// seed — would surface here as Queues == 2.
func TestBuild_SeedsEntityCounts(t *testing.T) {
	dataDir := t.TempDir()

	// "First boot": persist one durable queue, then close the storage.
	store1, err := storage.NewStorageEngineWithDataDir(dataDir)
	require.NoError(t, err)
	b1 := broker.NewStorageBroker(store1, config.DefaultConfig().GetEngine())
	_, err = b1.DeclareQueue("recovered-q", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = dataDir
	cfg.Server.LogLevel = "silent"

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	snap := srv.SnapshotEntityTotals()
	require.EqualValues(t, 1, snap.Queues, "the recovered durable queue must be seeded exactly once")
	require.EqualValues(t, len(srv.Broker.GetQueues()), snap.Queues,
		"the snapshot must agree with the recovered registry")
	require.True(t, srv.EntityObserverWired(), "Build must wire the observer for the storage broker")

	// A post-build declare flows through the observer: exactly +1 from the
	// seeded baseline (no double count, no reset).
	_, err = srv.Broker.DeclareQueue("fresh-q", false, false, false, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, srv.SnapshotEntityTotals().Queues)
}

// TestBuild_ObserverWiredOnlyForStorageBroker pins mgmt-03 §5's coherence
// contract: a custom UnifiedBroker lacking the SetEntityObserver seam leaves
// EntityObserverWired() false (the management layer reports
// snapshot_coherent=false), while the default storage-broker path is wired.
func TestBuild_ObserverWiredOnlyForStorageBroker(t *testing.T) {
	// A custom broker without the seam.
	fakeDir := t.TempDir()
	fakeStore, err := storage.NewStorageEngineWithDataDir(fakeDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fakeStore.Close() })

	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = fakeDir
	cfg.Server.LogLevel = "silent"

	storageBroker := broker.NewStorageBroker(fakeStore, cfg.GetEngine())
	t.Cleanup(storageBroker.Close)
	fake := observerlessBroker{UnifiedBroker: NewStorageBrokerAdapter(storageBroker)}
	srvFake, err := NewServerBuilder().WithConfig(cfg).WithStorage(fakeStore).WithUnifiedBroker(fake).Build()
	require.NoError(t, err)
	require.False(t, srvFake.EntityObserverWired(),
		"a broker lacking the observer seam must not report wired")

	// The default path wraps a *StorageBroker in StorageBrokerAdapter, which
	// implements the seam: Build must wire it.
	realCfg := config.DefaultConfig()
	realCfg.Network.Address = "127.0.0.1:0"
	realCfg.Storage.Path = t.TempDir()
	realCfg.Server.LogLevel = "silent"
	srvReal, err := NewServerBuilder().WithConfig(realCfg).Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srvReal.Shutdown(ctx)
	})
	require.True(t, srvReal.EntityObserverWired(),
		"Build must wire the observer for the production storage broker")
}
