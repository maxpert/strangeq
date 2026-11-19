package cluster

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	pb "go.etcd.io/raft/v3/raftpb"
)

const peerActivationFile = "peer-activation-v1.json"

type peerActivation struct {
	Peer   Peer
	Listen string
}
type peerTransport struct {
	client     *clusterwire.Client
	server     *clusterwire.Server
	activation peerActivation
	ready      atomic.Bool
}

// OpenLocal opens a generated, durable one-voter lineage on the ordinary Raft
// journal. No peer socket or client exists until ActivatePeers succeeds.
func OpenLocal(cfg Config) (*Node, error) {
	activation := cfg
	cfg.local = true
	if cfg.Directory == "" || cfg.NewMachine == nil {
		return nil, errors.New("local node requires storage directory and machine")
	}
	if cfg.Shards > 1024 {
		return nil, errors.New("local node permits at most 1024 shards")
	}
	name := journalIdentityFile
	if cfg.ExperimentalJournalSmallBodyCompression {
		name = smallBodyJournalIdentityFile
	}
	raw, err := os.ReadFile(filepath.Join(cfg.Directory, name))
	if err == nil {
		var prior identity
		if json.Unmarshal(raw, &prior) != nil || !prior.Local || !clusterraft.ValidMemberID(prior.Peer.ID) || prior.Peer.Address != "" || prior.Cluster == "" || prior.Shards == 0 || prior.Shards > 1024 || len(prior.Seeds) != 1 || prior.Seeds[0] != prior.Peer || prior.Nonce != "" {
			return nil, errors.New("invalid persistent local identity")
		}
		if (cfg.ClusterID != "" && cfg.ClusterID != prior.Cluster) || (cfg.Peer.ID != 0 && cfg.Peer.ID != prior.Peer.ID) || (cfg.Shards != 0 && cfg.Shards != prior.Shards) {
			return nil, errors.New("persistent local identity does not match configuration")
		}
		cfg.ClusterID, cfg.Peer, cfg.Shards = prior.Cluster, prior.Peer, prior.Shards
	} else if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(cfg.Directory)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return nil, readErr
		}
		if !freshStorageDirectory(entries) {
			return nil, errors.New("local directory exists without its persistent identity")
		}
		// A caller may verify identity on reopen, but cannot choose a new lineage.
		if cfg.ClusterID != "" || cfg.Peer.ID != 0 {
			return nil, errors.New("fresh local identity must be generated")
		}
		var entropy [24]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return nil, err
		}
		cfg.ClusterID = hex.EncodeToString(entropy[:16])
		cfg.Peer = Peer{ID: binary.BigEndian.Uint64(entropy[16:]) & ((1 << 63) - 1)}
		if !clusterraft.ValidMemberID(cfg.Peer.ID) {
			cfg.Peer.ID = 1
		}
		if cfg.Shards == 0 {
			cfg.Shards = defaultShards
		}
	} else {
		return nil, err
	}
	cfg.Seeds = []Peer{{ID: cfg.Peer.ID}}
	cfg.Listen, cfg.Token, cfg.TLS = "", "", nil
	cfg.Discovery = nil
	n, err := open(cfg, false)
	if err != nil {
		return nil, err
	}
	intent, intentErr := os.ReadFile(n.path(peerActivationFile))
	if intentErr == nil {
		var want peerActivation
		if json.Unmarshal(intent, &want) != nil || want.Peer.ID != cfg.Peer.ID || !validAddress(want.Peer.Address) || want.Listen == "" {
			_ = n.Close()
			return nil, errors.New("invalid persistent peer activation")
		}
		if activation.Peer.Address == "" {
			_ = n.Close()
			return nil, errors.New("persistent peer activation requires peer credentials and matching endpoint configuration")
		}
	} else if !errors.Is(intentErr, os.ErrNotExist) {
		_ = n.Close()
		return nil, intentErr
	}
	if activation.Peer.Address != "" {
		if err := n.ActivatePeers(activation); err != nil {
			_ = n.Close()
			return nil, err
		}
	}
	return n, nil
}

// Identity returns the immutable lineage and node ID for peer enrollment.
func (n *Node) Identity() (string, uint64) { return n.cfg.ClusterID, n.cfg.Peer.ID }

func peerWireConfig(cfg Config) clusterwire.Config {
	w := clusterwire.Config{Listen: cfg.Listen, ClusterID: cfg.ClusterID, Token: cfg.Token, TLS: cfg.TLS,
		AllowInsecure: cfg.AllowInsecure, Timeout: requestTimeout + time.Second, MaxBodyBytes: maxWireBytes}
	if cfg.ExperimentalAckAndClaim {
		w.ProtocolVersion = ackAndClaimPeerProtocol
	}
	if cfg.ExperimentalAssignments {
		w.ProtocolVersion = assignmentPeerProtocol
	}
	if cfg.ExperimentalJournalBodyCompression {
		w.ProtocolVersion = compressedBodyPeerProtocol
	}
	if cfg.ExperimentalJournalSmallBodyCompression {
		w.ProtocolVersion = smallCompressedBodyPeerProtocol
	}
	w.ProtocolVersion += deadLetterPeerProtocolOffset
	return w
}

func (n *Node) peerClient() *clusterwire.Client {
	if transport := n.transport.Load(); transport != nil {
		return transport.client
	}
	return n.client
}
func (n *Node) peerAddress() string {
	if transport := n.transport.Load(); transport != nil {
		return transport.activation.Peer.Address
	}
	return n.cfg.Peer.Address
}

// ActivatePeers activates authenticated transport on an existing local lineage.
// The durable intent precedes exposure; enrollment stays closed until the seed
// endpoint commits. Failed binds and interrupted commits can retry the same intent.
func (n *Node) ActivatePeers(options Config) error {
	n.activationMu.Lock()
	defer n.activationMu.Unlock()
	if !n.cfg.local {
		return errors.New("peer activation requires a local node")
	}
	if n.ctx.Err() != nil {
		return ErrUnavailable
	}
	if !validAddress(options.Peer.Address) {
		return errors.New("peer activation requires a valid advertised address")
	}
	if options.ClusterID != "" && options.ClusterID != n.cfg.ClusterID || options.Peer.ID != 0 && options.Peer.ID != n.cfg.Peer.ID {
		return errors.New("peer activation cannot change persistent identity")
	}
	if options.Listen == "" {
		options.Listen = options.Peer.Address
	}
	want := peerActivation{Peer: Peer{ID: n.cfg.Peer.ID, Address: options.Peer.Address}, Listen: options.Listen}
	if active := n.transport.Load(); active != nil {
		if active.activation != want {
			return errors.New("peer activation cannot change endpoint")
		}
		return nil
	}
	options.ClusterID = n.cfg.ClusterID
	options.ExperimentalAckAndClaim = n.cfg.ExperimentalAckAndClaim
	options.ExperimentalAssignments = n.cfg.ExperimentalAssignments
	options.ExperimentalJournalBodyCompression = n.cfg.ExperimentalJournalBodyCompression
	options.ExperimentalJournalSmallBodyCompression = n.cfg.ExperimentalJournalSmallBodyCompression
	wcfg := peerWireConfig(options)
	client, err := clusterwire.NewClient(wcfg)
	if err != nil {
		return err
	}
	transport := &peerTransport{client: client, activation: want}
	server, err := clusterwire.NewServer(wcfg, &activationHandler{node: n, transport: transport})
	if err != nil {
		client.Close()
		return err
	}
	transport.server = server
	failed := true
	defer func() {
		if failed {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = server.Close(ctx)
			client.Close()
			n.transport.Store(nil)
		}
	}()
	raw, err := os.ReadFile(n.path(peerActivationFile))
	if err == nil {
		var prior peerActivation
		if json.Unmarshal(raw, &prior) != nil || prior != want {
			return errors.New("persistent peer activation does not match endpoint")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := provisionFile(n.path(peerActivationFile), encode(want)); err != nil {
			return err
		}
	} else {
		return err
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("activate peer listener: %w", err)
	}
	n.transport.Store(transport)
	// Group open replays the durable committed catalog before returning. An
	// established endpoint needs no new quorum operation on cold restart.
	recovered := n.catalog.view()
	if endpoint := address(recovered, n.cfg.Peer.ID); endpoint != "" {
		if endpoint != want.Peer.Address {
			return errors.New("committed peer endpoint does not match activation")
		}
		n.install(recovered)
		if address(n.view(), n.cfg.Peer.ID) != want.Peer.Address {
			return errors.New("committed peer endpoint could not be installed")
		}
		transport.ready.Store(true)
		failed = false
		return nil
	}
	ctx, cancel := context.WithTimeout(n.ctx, requestTimeout)
	defer cancel()
	raw, err = n.submit(ctx, catalogID, encode(catalogCommand{ActivateSeed: &want.Peer}))
	if err != nil {
		return err
	}
	var reply catalogReply
	if json.Unmarshal(raw, &reply) != nil || reply.Error != "" {
		return errors.New("catalog rejected peer activation")
	}
	n.install(reply.View)
	if address(n.view(), n.cfg.Peer.ID) != want.Peer.Address {
		return errors.New("committed peer endpoint could not be installed")
	}
	transport.ready.Store(true)
	failed = false
	return nil
}

// Every inbound peer operation is gated until endpoint publication commits.
type activationHandler struct {
	node      *Node
	transport *peerTransport
}

func (h *activationHandler) Raft(ctx context.Context, id uint64, m []*pb.Message) error {
	if !h.transport.ready.Load() {
		return ErrUnavailable
	}
	return h.node.Raft(ctx, id, m)
}
func (h *activationHandler) Command(ctx context.Context, b []byte) ([]byte, error) {
	if !h.transport.ready.Load() {
		return nil, ErrUnavailable
	}
	return h.node.Command(ctx, b)
}

// Preserve the transport's split reply fast paths while applying the same
// enrollment gate as the ordinary handler methods.
func (h *activationHandler) CommandParts(ctx context.Context, b []byte) ([]byte, []byte, error) {
	if !h.transport.ready.Load() {
		return nil, nil, ErrUnavailable
	}
	return h.node.CommandParts(ctx, b)
}
func (h *activationHandler) ReadCommandParts(ctx context.Context, b []byte) ([]byte, []byte, error) {
	if !h.transport.ready.Load() {
		return nil, nil, ErrUnavailable
	}
	return h.node.ReadCommandParts(ctx, b)
}
func (h *activationHandler) Membership(ctx context.Context, b []byte) ([]byte, error) {
	if !h.transport.ready.Load() {
		return nil, ErrUnavailable
	}
	return h.node.Membership(ctx, b)
}
func (h *activationHandler) ReadCommand(ctx context.Context, b []byte) ([]byte, error) {
	if !h.transport.ready.Load() {
		return nil, ErrUnavailable
	}
	return h.node.ReadCommand(ctx, b)
}
func (h *activationHandler) WaitCommand(ctx context.Context, b []byte) ([]byte, error) {
	if !h.transport.ready.Load() {
		return nil, ErrUnavailable
	}
	return h.node.WaitCommand(ctx, b)
}

// LocalGroupStatuses reports actual applied Raft membership, including the
// catalog, for observing learner catch-up and quorum convergence.
func (n *Node) LocalGroupStatuses() map[uint64]clusterraft.Status {
	n.groupsMu.RLock()
	defer n.groupsMu.RUnlock()
	statuses := make(map[uint64]clusterraft.Status, len(n.groups))
	for id, g := range n.groups {
		statuses[id] = g.Status()
	}
	return statuses
}
