package cluster

import (
	"context"
	"encoding/base64"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
)

func TestDeadLetterEpochFencesOlderClusterPeersAndStores(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	cfg := Config{
		ClusterID: "dead-letter-epoch", Peer: Peer{1, address}, Seeds: []Peer{{1, address}}, Listen: address,
		Directory: t.TempDir(), Shards: 1, AllowInsecure: true,
		Token:      base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} },
	}
	require.NoError(t, Initialize(cfg))
	stub := &Node{cfg: cfg}
	_, mode, err := stub.storageIdentity()
	require.NoError(t, err)
	olderMode := strings.TrimSuffix(mode, deadLetterStorageModeSuffix)
	require.NotEqual(t, mode, olderMode)
	require.NoError(t, durableFile(stub.path(clusterStorageModeFile), []byte(olderMode)))

	node, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	raw, err := os.ReadFile(stub.path(clusterStorageModeFile))
	require.NoError(t, err)
	require.Equal(t, mode, string(raw))
	require.ErrorContains(t, stub.checkStorageModeClaim(olderMode), "conflicts with persistent mode")

	wireConfig := clusterwire.Config{ClusterID: cfg.ClusterID, Token: cfg.Token, AllowInsecure: true}
	older, err := clusterwire.NewClient(wireConfig)
	require.NoError(t, err)
	defer older.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = older.Membership(ctx, address, nil)
	require.ErrorContains(t, err, "HTTP status 401")

	wireConfig.ProtocolVersion = deadLetterPeerProtocolOffset
	current, err := clusterwire.NewClient(wireConfig)
	require.NoError(t, err)
	defer current.Close()
	_, err = current.Membership(ctx, address, nil)
	require.NoError(t, err)
}
