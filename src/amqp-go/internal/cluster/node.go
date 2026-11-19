package cluster

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultShards                   = 32
	maxEntryBytes                   = 32 << 20
	maxWireBytes                    = 128 << 20
	requestTimeout                  = 6 * time.Second
	ackAndClaimPeerProtocol         = 2
	assignmentPeerProtocol          = 3
	compressedBodyPeerProtocol      = 4
	smallCompressedBodyPeerProtocol = 5
)

var ErrUnavailable = errors.New("shard leader unavailable")
var ErrUncertain = errors.New("shard command outcome uncertain")

type Config struct {
	ClusterID                               string
	Peer                                    Peer
	Seeds                                   []Peer
	Discovery                               []string
	Listen, Directory, Token                string
	Shards                                  uint64
	AllowInsecure                           bool
	ExperimentalRaftJournal                 bool
	ExperimentalRaftRangeSync               bool
	ExperimentalAckAndClaim                 bool
	ExperimentalAssignments                 bool
	ExperimentalReplicaBodies               bool
	ExperimentalRaftJournalDualStream       bool
	ExperimentalJournalBodyCompression      bool
	ExperimentalJournalSmallBodyCompression bool
	TLS                                     *tls.Config
	NewMachine                              func(uint64) clusterraft.Machine
	ExpiryCommand                           func(time.Time) []byte
}

type Node struct {
	cfg                    Config
	nonce                  string
	catalog                *catalog
	current                atomic.Pointer[view]
	viewMu                 sync.Mutex
	client                 *clusterwire.Client
	wire                   *clusterwire.Server
	ctx                    context.Context
	cancel                 context.CancelFunc
	groupsMu               sync.RWMutex
	groups                 map[uint64]*clusterraft.Group
	journal                nodeJournalOwner
	assignmentReplicaSlots chan struct{}
	routeMu                sync.RWMutex
	routes                 map[uint64]uint64
	waitCount              atomic.Int32
	sendMu                 sync.Mutex
	receipts               raftReceipts
	lanes                  map[uint64][3]chan packet
	sendBytes              atomic.Int64
	wg                     sync.WaitGroup
	closeOnce              sync.Once
	closeErr               error
}

type identity struct {
	Peer    Peer
	Cluster string
	Shards  uint64
	Seeds   []Peer
	Nonce   string
}

// Initialize provisions one original voter's empty storage. Provision all
// three original voters before starting the cluster.
func Initialize(cfg Config) error {
	n, err := open(cfg, true)
	if err != nil {
		return err
	}
	return n.Close()
}

func Open(cfg Config) (*Node, error) { return open(cfg, false) }

func open(cfg Config, initialize bool) (*Node, error) {
	if cfg.Shards == 0 {
		cfg.Shards = defaultShards
	}
	if !clusterraft.ValidMemberID(cfg.Peer.ID) || !validAddress(cfg.Peer.Address) || cfg.Listen == "" || cfg.Directory == "" || cfg.NewMachine == nil {
		return nil, errors.New("cluster requires a valid peer, listener, storage directory and machine")
	}
	if cfg.ExperimentalRaftRangeSync && !cfg.ExperimentalRaftJournal {
		return nil, errors.New("cluster range sync requires journal storage")
	}
	if cfg.ExperimentalRaftJournalDualStream && (!cfg.ExperimentalRaftJournal || !cfg.ExperimentalRaftRangeSync) {
		return nil, errors.New("cluster two-stream journal requires journal storage and range sync")
	}
	if cfg.ExperimentalAckAndClaim && !cfg.ExperimentalRaftJournal {
		return nil, errors.New("cluster ACK and claim requires journal storage")
	}
	if cfg.ExperimentalAssignments && (!cfg.ExperimentalRaftJournal || cfg.ExperimentalAckAndClaim) {
		return nil, errors.New("cluster assignments require a separate journal storage mode")
	}
	if cfg.ExperimentalReplicaBodies && !cfg.ExperimentalAssignments {
		return nil, errors.New("cluster replica bodies require assignments")
	}
	if cfg.ExperimentalJournalBodyCompression && !cfg.ExperimentalAssignments {
		return nil, errors.New("cluster journal body compression requires assignments")
	}
	if cfg.ExperimentalJournalSmallBodyCompression && !cfg.ExperimentalJournalBodyCompression {
		return nil, errors.New("cluster journal small body compression requires journal body compression")
	}
	c, err := newCatalog(cfg.ClusterID, cfg.Shards, cfg.Seeds)
	if err != nil {
		return nil, err
	}
	initialAddr := address(c.initial, cfg.Peer.ID)
	seed := initialAddr != ""
	if (seed && initialAddr != cfg.Peer.Address) || (!seed && initialize) {
		return nil, errors.New("bootstrap peer identity mismatch")
	}
	wcfg := clusterwire.Config{Listen: cfg.Listen, ClusterID: cfg.ClusterID, Token: cfg.Token, TLS: cfg.TLS,
		AllowInsecure: cfg.AllowInsecure, Timeout: requestTimeout + time.Second, MaxBodyBytes: maxWireBytes}
	if cfg.ExperimentalAckAndClaim {
		wcfg.ProtocolVersion = ackAndClaimPeerProtocol
	}
	if cfg.ExperimentalAssignments {
		wcfg.ProtocolVersion = assignmentPeerProtocol
	}
	if cfg.ExperimentalJournalBodyCompression {
		wcfg.ProtocolVersion = compressedBodyPeerProtocol
	}
	if cfg.ExperimentalJournalSmallBodyCompression {
		wcfg.ProtocolVersion = smallCompressedBodyPeerProtocol
	}
	client, err := clusterwire.NewClient(wcfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, catalog: c, client: client, ctx: ctx, cancel: cancel, groups: map[uint64]*clusterraft.Group{},
		routes: map[uint64]uint64{}, lanes: map[uint64][3]chan packet{}}
	if cfg.ExperimentalReplicaBodies && !initialize {
		n.assignmentReplicaSlots = make(chan struct{}, maxAssignmentReplicaRequests)
	}
	success := false
	defer func() {
		if !success {
			_ = n.Close()
		}
	}()
	if err := n.prepareIdentity(seed, initialize); err != nil {
		return nil, err
	}
	if cfg.ExperimentalRaftJournal {
		if err := n.prepareJournal(); err != nil {
			return nil, err
		}
	}
	n.current.Store(&c.initial)
	cache, err := os.ReadFile(n.path("catalog-cache"))
	if err == nil {
		var v view
		if json.Unmarshal(cache, &v) != nil || !c.valid(v) {
			return nil, errors.New("invalid cached catalog")
		}
		n.current.Store(&v)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := n.openExisting(seed, initialize); err != nil {
		return nil, err
	}
	if initialize {
		success = true
		return n, nil
	}
	n.wire, err = clusterwire.NewServer(wcfg, n)
	if err != nil {
		return nil, err
	}
	if err = n.wire.Start(); err != nil {
		return nil, err
	}
	n.wg.Add(1)
	go n.maintain()
	success = true
	return n, nil
}

func (n *Node) path(name string) string   { return filepath.Join(n.cfg.Directory, name) }
func (n *Node) store(id uint64) string    { return n.path(strconv.FormatUint(id, 10)) }
func (n *Node) opened(id uint64) string   { return n.path("opened-" + strconv.FormatUint(id, 10)) }
func (n *Node) retired(id uint64) string  { return n.path("retired-" + strconv.FormatUint(id, 10)) }
func (n *Node) validGroup(id uint64) bool { return id == catalogID || (id > 0 && id <= n.cfg.Shards) }

func (n *Node) prepareIdentity(seed, initialize bool) error {
	name, mode, err := n.storageIdentity()
	if err != nil {
		return err
	}
	p := n.path(name)
	want := identity{Peer: n.cfg.Peer, Cluster: n.cfg.ClusterID, Shards: n.cfg.Shards, Seeds: n.catalog.initial.Nodes}
	raw, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		if seed && !initialize {
			return errors.New("original voter is missing persistent identity")
		}
		if seed {
			addresses := make([]string, 0, len(n.catalog.initial.Nodes)+len(n.cfg.Discovery))
			for _, other := range n.catalog.initial.Nodes {
				addresses = append(addresses, other.Address)
			}
			addresses = append(addresses, n.cfg.Discovery...)
			seen := map[string]bool{n.cfg.Peer.Address: true}
			for _, address := range addresses {
				if seen[address] {
					continue
				}
				seen[address] = true
				ctx, cancel := context.WithTimeout(n.ctx, 500*time.Millisecond)
				_, err := n.client.Membership(ctx, address, nil)
				cancel()
				if err == nil {
					return errors.New("cannot initialize an original voter while the cluster is running")
				}
			}
		}
		if err := durableDirectory(n.cfg.Directory); err != nil {
			return err
		}
		entries, err := os.ReadDir(n.cfg.Directory)
		if err != nil {
			return err
		}
		if !freshStorageDirectory(entries) {
			return errors.New("cluster directory exists without its persistent identity")
		}
		if err := n.claimStorageMode(mode); err != nil {
			return err
		}
		if !seed {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				return err
			}
			want.Nonce = hex.EncodeToString(b[:])
		}
		if err := provisionFile(p, encode(want)); !errors.Is(err, os.ErrExist) {
			if err == nil {
				n.nonce = want.Nonce
			}
			return err
		}
		// A matching initializer published the identity while we created
		// the mode claim. Sync the directory before trusting that identity.
		if err := syncStorageDirectory(n.cfg.Directory); err != nil {
			return err
		}
		raw, err = os.ReadFile(p)
	}
	if err != nil {
		return err
	}
	var prior identity
	if json.Unmarshal(raw, &prior) != nil || prior.Peer != want.Peer || prior.Cluster != want.Cluster ||
		prior.Shards != want.Shards || !slices.Equal(prior.Seeds, want.Seeds) ||
		(seed && prior.Nonce != "") || (!seed && !validNonce(prior.Nonce)) {
		return errors.New("persistent cluster identity does not match configuration")
	}
	n.nonce = prior.Nonce
	return nil
}

func durableDirectory(path string) error {
	exists, err := pathExists(path)
	if err != nil || exists {
		return err
	}
	parent := filepath.Dir(path)
	if err := durableDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	dir, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func durableFile(path string, value []byte) error {
	return writeDurableFile(path, value, ".cluster-", os.Rename)
}

func provisionFile(path string, value []byte) error {
	return writeDurableFile(path, value, clusterProvisionTemp, os.Link)
}

func writeDurableFile(path string, value []byte, prefix string, publish func(string, string) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), prefix)
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(value)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = publish(f.Name(), path); err != nil {
		return err
	}
	return syncStorageDirectory(filepath.Dir(path))
}

func syncStorageDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (n *Node) openExisting(seed, initialize bool) error {
	initialized, err := pathExists(n.path("initialized"))
	if err != nil {
		return err
	}
	initializing := seed && !initialized
	if initializing && !initialize {
		return errors.New("original voter provisioning did not complete")
	}
	names, err := os.ReadDir(n.cfg.Directory)
	if err != nil {
		return err
	}
	ids := make(map[uint64]bool)
	for _, name := range names {
		text, marked := strings.CutPrefix(name.Name(), "opened-")
		if !marked && !name.IsDir() {
			continue
		}
		id, err := strconv.ParseUint(text, 10, 64)
		if err == nil && n.validGroup(id) {
			ids[id] = true
		}
	}
	if n.journal != nil {
		for _, id := range n.journal.GroupIDs() {
			if !n.validGroup(id) {
				return fmt.Errorf("persistent raft journal contains unknown group %d", id)
			}
			ids[id] = true
		}
	}
	if seed {
		ids[catalogID] = true
		for id := uint64(1); id <= n.cfg.Shards; id++ {
			ids[id] = true
		}
	}
	for id := range ids {
		retired, err := pathExists(n.retired(id))
		if err != nil {
			return err
		}
		if retired {
			if n.journal == nil {
				if err := os.RemoveAll(n.store(id)); err != nil {
					return err
				}
			}
			continue
		}
		exists, err := n.storeExists(id)
		if err != nil {
			return err
		}
		if !exists && seed && initialized {
			return fmt.Errorf("shard %d lost its persistent store", id)
		}
		var peers []uint64
		if initializing {
			peers = n.catalog.initial.Placements[0].Voters
		}
		if err := n.openGroup(id, peers); err != nil {
			return fmt.Errorf("shard %d: %w", id, err)
		}
	}
	if initializing {
		return durableFile(n.path("initialized"), []byte{1})
	}
	return nil
}

func (n *Node) openGroup(id uint64, peers []uint64) error {
	if n.group(id) != nil {
		return nil
	}
	retired, err := pathExists(n.retired(id))
	if err != nil {
		return err
	}
	if retired {
		return errors.New("shard replica identity retired")
	}
	exists, err := n.storeExists(id)
	if err != nil {
		return err
	}
	opened, err := pathExists(n.opened(id))
	if err != nil {
		return err
	}
	if opened && !exists {
		return errors.New("replica lost its persistent store")
	}
	var machine clusterraft.Machine = n.catalog
	if id != catalogID {
		machine = n.cfg.NewMachine(id)
	}
	storeOpen := clusterraft.Open
	var gatherProposal func([]byte) time.Duration
	if n.journal != nil {
		storeOpen = n.journal.OpenGroup
		if n.cfg.ExperimentalAssignments && id != catalogID {
			gatherProposal = gatherSettlement
		}
	}
	g, err := storeOpen(clusterraft.Config{
		ID: n.cfg.Peer.ID, GroupID: id, Dir: n.store(id), Peers: peers, Machine: machine, MaxEntryBytes: maxEntryBytes,
		DeferJournalCommit:           n.journal != nil && id != catalogID,
		BackgroundJournalCheckpoints: n.journal != nil && id != catalogID,
		AssignmentReplicaBodies:      n.assignmentReplicaSlots != nil && id != catalogID,
		GatherProposal:               gatherProposal,
		Send:                         func(msg *pb.Message) error { return n.send(id, msg) },
	})
	if err != nil {
		return err
	}
	if !opened {
		if err := durableFile(n.opened(id), []byte{1}); err != nil {
			_ = g.Close()
			return err
		}
	}
	n.groupsMu.Lock()
	n.groups[id] = g
	n.groupsMu.Unlock()
	return nil
}

func gatherSettlement(data []byte) time.Duration {
	if len(data) <= 4096 && clusterqueue.IsSettlementCommand(data) {
		return 125 * time.Microsecond
	}
	return 0
}

func (n *Node) group(id uint64) *clusterraft.Group {
	n.groupsMu.RLock()
	defer n.groupsMu.RUnlock()
	return n.groups[id]
}

// LocalQueueShardStatus returns the local Raft status and preferred leader for queue.
func (n *Node) LocalQueueShardStatus(queue string) (clusterraft.Status, uint64, bool) {
	id := groupFor(queue, n.cfg.Shards)
	group := n.group(id)
	if group == nil {
		return clusterraft.Status{}, 0, false
	}
	return group.Status(), rank(n.view(), id)[0], true
}

func (n *Node) view() view { return *n.current.Load() }
func (n *Node) install(v view) {
	if v.Epoch <= n.view().Epoch || !n.catalog.valid(v) {
		return
	}
	n.viewMu.Lock()
	defer n.viewMu.Unlock()
	if v.Epoch <= n.view().Epoch {
		return
	}
	if durableFile(n.path("catalog-cache"), encode(v)) == nil {
		n.current.Store(&v)
	}
}
func (n *Node) enrolled(v view) bool {
	return address(v, n.cfg.Peer.ID) == n.cfg.Peer.Address && v.Nonces[n.cfg.Peer.ID] == n.nonce
}
func (n *Node) SupportsAckAndClaim() bool { return n.cfg.ExperimentalAckAndClaim }

func (n *Node) SupportsAssignments() bool { return n.cfg.ExperimentalAssignments }

func (n *Node) SupportsJournalBodyCompression() bool { return n.cfg.ExperimentalJournalBodyCompression }

func (n *Node) SupportsJournalSmallBodyCompression() bool {
	return n.cfg.ExperimentalJournalSmallBodyCompression
}

func (n *Node) Ready() error {
	if n.ctx.Err() != nil || !n.enrolled(n.view()) {
		return ErrUnavailable
	}
	if n.journal != nil {
		if err := n.journal.Failure(); err != nil {
			return errors.Join(ErrUnavailable, err)
		}
	}
	return nil
}
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.cancel()
		// Quiesce admissions that can register workers before waiting on n.wg.
		n.sendMu.Lock()
		n.sendMu.Unlock()
		n.receipts.mu.Lock()
		n.receipts.mu.Unlock()
		if n.wire != nil {
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			n.closeErr = n.wire.Close(ctx)
			cancel()
		}
		n.wg.Wait()
		for _, g := range n.groups {
			n.closeErr = errors.Join(n.closeErr, g.Close())
		}
		if n.journal != nil {
			n.closeErr = errors.Join(n.closeErr, n.journal.Close())
		}
		n.client.Close()
	})
	return n.closeErr
}

type packet struct {
	group uint64
	msg   *pb.Message
	bytes int64
}

func (n *Node) send(group uint64, msg *pb.Message) error {
	id := msg.GetTo()
	if address(n.view(), id) == "" {
		return ErrUnavailable
	}
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	if n.ctx.Err() != nil {
		return n.ctx.Err()
	}
	lanes := n.lanes[id]
	offset := group % 2
	if msg.GetType() == pb.MsgSnap {
		offset = 2
	}
	queue := lanes[offset]
	if queue == nil {
		queue = make(chan packet, 128)
		lanes[offset] = queue
		n.lanes[id] = lanes
		n.wg.Add(1)
		go n.sendLoop(id, queue)
	}
	size := int64(proto.Size(msg))
	if n.sendBytes.Add(size) > 2*maxWireBytes {
		n.sendBytes.Add(-size)
		return errors.New("peer replication budget exhausted")
	}
	select {
	case queue <- packet{group, msg, size}:
		return nil
	default:
		n.sendBytes.Add(-size)
		return errors.New("peer replication queue full")
	}
}

func (n *Node) sendLoop(peer uint64, queue <-chan packet) {
	defer n.wg.Done()
	var ahead packet
	defer func() { n.discardRaftSendQueue(queue, ahead) }()
	var messages [16]*pb.Message
	for n.ctx.Err() == nil {
		p := ahead
		ahead = packet{}
		if p.msg == nil {
			select {
			case <-n.ctx.Done():
				return
			case p = <-queue:
			}
		}
		messages[0] = p.msg
		batch, charged := messages[:1], p.bytes
		timeout := 1500 * time.Millisecond
		snapshot := p.msg.GetType() == pb.MsgSnap
		if snapshot {
			timeout = requestTimeout
		} else {
			batch, charged, ahead = gatherRaftSendBatch(queue, p.group, batch, charged)
		}
		if n.ctx.Err() != nil {
			n.sendBytes.Add(-charged)
			return
		}
		ctx, cancel := context.WithTimeout(n.ctx, timeout)
		err := n.client.Raft(ctx, address(n.view(), peer), p.group, batch)
		cancel()
		n.sendBytes.Add(-charged)
		clear(batch)

		if !snapshot && err == nil {
			continue
		}
		g := n.group(p.group)
		if g == nil {
			continue
		}
		ctx, cancel = context.WithTimeout(n.ctx, time.Second)
		if snapshot {
			_ = g.ReportSnapshot(ctx, peer, err == nil)
		} else {
			_ = g.ReportUnreachable(ctx, peer)
		}
		cancel()
	}
}

func gatherRaftSendBatch(queue <-chan packet, group uint64, batch []*pb.Message, charged int64) ([]*pb.Message, int64, packet) {
	const target = 1 << 20
	// The wire format has a 10-byte batch header and a 4-byte length per
	// message. A first packet above the target still travels on its own.
	size := 10 + 4 + charged
	for len(batch) < cap(batch) && size < target {
		select {
		case next := <-queue:
			if next.group != group || next.msg.GetType() == pb.MsgSnap || size+4+next.bytes > target {
				return batch, charged, next
			}
			batch = append(batch, next.msg)
			size += 4 + next.bytes
			charged += next.bytes
		default:
			return batch, charged, packet{}
		}
	}
	return batch, charged, packet{}
}

func (n *Node) discardRaftSendQueue(queue <-chan packet, ahead packet) {
	// A sender that passed the cancellation check can still be enqueuing.
	// Take the admission lock before returning its packet charge.
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	charged := ahead.bytes
	for len(queue) != 0 {
		charged += (<-queue).bytes
	}
	n.sendBytes.Add(-charged)
}
