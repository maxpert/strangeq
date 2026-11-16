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

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultShards  = 32
	maxEntryBytes  = 32 << 20
	maxWireBytes   = 128 << 20
	requestTimeout = 6 * time.Second
)

var ErrUnavailable = errors.New("shard leader unavailable")
var ErrUncertain = errors.New("shard command outcome uncertain")

type Config struct {
	ClusterID                string
	Peer                     Peer
	Seeds                    []Peer
	Discovery                []string
	Listen, Directory, Token string
	Shards                   uint64
	AllowInsecure            bool
	TLS                      *tls.Config
	NewMachine               func(uint64) clusterraft.Machine
	ExpiryCommand            func(time.Time) []byte
}

type Node struct {
	cfg       Config
	nonce     string
	catalog   *catalog
	current   atomic.Pointer[view]
	viewMu    sync.Mutex
	client    *clusterwire.Client
	wire      *clusterwire.Server
	ctx       context.Context
	cancel    context.CancelFunc
	groupsMu  sync.RWMutex
	groups    map[uint64]*clusterraft.Group
	routeMu   sync.RWMutex
	routes    map[uint64]uint64
	sendMu    sync.Mutex
	lanes     map[uint64][3]chan packet
	sendBytes atomic.Int64
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
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
	client, err := clusterwire.NewClient(wcfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, catalog: c, client: client, ctx: ctx, cancel: cancel, groups: map[uint64]*clusterraft.Group{},
		routes: map[uint64]uint64{}, lanes: map[uint64][3]chan packet{}}
	success := false
	defer func() {
		if !success {
			_ = n.Close()
		}
	}()
	if err := n.prepareIdentity(seed, initialize); err != nil {
		return nil, err
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
	p := n.path("identity.json")
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
		if len(entries) != 0 {
			return errors.New("cluster directory exists without its persistent identity")
		}
		if !seed {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				return err
			}
			want.Nonce = hex.EncodeToString(b[:])
		}
		n.nonce = want.Nonce
		return durableFile(p, encode(want))
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
	f, err := os.CreateTemp(filepath.Dir(path), ".cluster-")
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
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
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
			if err := os.RemoveAll(n.store(id)); err != nil {
				return err
			}
			continue
		}
		exists, err := clusterraft.Exists(n.store(id))
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
	exists, err := clusterraft.Exists(n.store(id))
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
	g, err := clusterraft.Open(clusterraft.Config{
		ID: n.cfg.Peer.ID, GroupID: id, Dir: n.store(id), Peers: peers, Machine: machine, MaxEntryBytes: maxEntryBytes,
		Send: func(msg *pb.Message) error { return n.send(id, msg) },
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
func (n *Node) Ready() error {
	if n.ctx.Err() != nil || !n.enrolled(n.view()) {
		return ErrUnavailable
	}
	return nil
}
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.cancel()
		n.sendMu.Lock()
		n.sendMu.Unlock()
		if n.wire != nil {
			ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
			n.closeErr = n.wire.Close(ctx)
			cancel()
		}
		n.wg.Wait()
		for _, g := range n.groups {
			n.closeErr = errors.Join(n.closeErr, g.Close())
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
	for n.ctx.Err() == nil {
		select {
		case <-n.ctx.Done():
			return
		case p := <-queue:
			timeout := 1500 * time.Millisecond
			snapshot := p.msg.GetType() == pb.MsgSnap
			if snapshot {
				timeout = requestTimeout
			}
			ctx, cancel := context.WithTimeout(n.ctx, timeout)
			err := n.client.Raft(ctx, address(n.view(), peer), p.group, []*pb.Message{p.msg})
			cancel()
			n.sendBytes.Add(-p.bytes)
			g := n.group(p.group)
			if g == nil {
				continue
			}
			ctx, cancel = context.WithTimeout(n.ctx, time.Second)
			if snapshot {
				_ = g.ReportSnapshot(ctx, peer, err == nil)
			} else if err != nil {
				_ = g.ReportUnreachable(ctx, peer)
			}
			cancel()
		}
	}
}

func (n *Node) Raft(ctx context.Context, id uint64, messages []*pb.Message) error {
	g := n.group(id)
	if g == nil {
		return ErrUnavailable
	}
	v := n.view()
	if !n.enrolled(v) {
		return ErrUnavailable
	}
	for _, m := range messages {
		if m == nil || m.GetTo() != n.cfg.Peer.ID || address(v, m.GetFrom()) == "" {
			return errors.New("invalid Raft sender or recipient")
		}
	}
	for _, m := range messages {
		if err := g.Step(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
