// Package clusterraft runs independent, durable Raft groups.
package clusterraft

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

var (
	ErrStopped           = errors.New("raft group stopped")
	ErrNotLeader         = errors.New("not the raft leader")
	ErrLeadershipLost    = errors.New("raft proposal outcome unknown")
	ErrLearnerNotReady   = errors.New("learner has not caught up")
	ErrInvalidMembership = errors.New("invalid membership change")
	ErrConfChangePending = errors.New("membership change already pending")
	ErrTooLarge          = errors.New("raft command exceeds size limit")
	// ErrBusy identifies a proposal rejected before entering the Raft log.
	ErrBusy = errors.New("raft could not accept proposal")
)

// Methods on Machine execute serially in Raft log order. Returned byte slices
// belong to the caller; inputs remain immutable.
type Machine interface {
	Apply([]byte) []byte
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

type Config struct {
	ID, GroupID uint64
	Dir         string
	Peers       []uint64
	Machine     Machine
	// Send enqueues a message and returns promptly. The transport owns its
	// message and reports the eventual transport outcome of every snapshot.
	Send                                          func(*pb.Message) error
	TickInterval                                  time.Duration
	SnapshotEntries, SnapshotBytes, MaxEntryBytes uint64
	MaxPending                                    int
	Logger                                        raft.Logger
}

type Progress struct {
	Match, Next  uint64
	RecentActive bool
}

type Status struct {
	ID, GroupID, Leader, Applied, Commit, ConfigIndex uint64
	Voters, Learners                                  []uint64
	Progress                                          map[uint64]Progress
}

type key [16]byte
type response struct {
	body []byte
	err  error
}

type configProposal struct {
	id    key
	index uint64
}

type Group struct {
	mu                                            sync.Mutex
	cfg                                           Config
	db                                            *bbolt.DB
	node                                          *raft.RawNode
	memory                                        *raft.MemoryStorage
	conf                                          *pb.ConfState
	applied, configIndex, snapshotIndex, logBytes uint64
	pending                                       map[key]chan response
	configuration                                 configProposal
	terminal                                      error
	done                                          chan struct{}
	ticks                                         sync.WaitGroup
	closeOnce                                     sync.Once
	closeErr                                      error
}

func Open(config Config) (*Group, error) {
	if !ValidMemberID(config.ID) || config.GroupID == 0 || config.Dir == "" || config.Machine == nil {
		return nil, errors.New("raft group requires identity, storage and machine")
	}
	if err := validatePeers(&config); err != nil {
		return nil, err
	}
	if config.Send == nil {
		config.Send = func(*pb.Message) error { return errors.New("raft transport closed") }
	}
	if config.TickInterval == 0 {
		config.TickInterval = 100 * time.Millisecond
	}
	if config.SnapshotEntries == 0 {
		config.SnapshotEntries = 1024
	}
	if config.SnapshotBytes == 0 {
		config.SnapshotBytes = 16 << 20
	}
	if config.MaxEntryBytes == 0 {
		config.MaxEntryBytes = 16 << 20
	}
	if config.MaxEntryBytes < uint64(len(key{})) || config.MaxEntryBytes > 128<<20 {
		return nil, ErrTooLarge
	}
	if config.MaxPending == 0 {
		config.MaxPending = 1024
	}
	if config.MaxPending < 1 {
		return nil, errors.New("raft pending limit must be positive")
	}
	db, err := createStore(config)
	if err != nil {
		return nil, err
	}
	g := &Group{
		cfg: config, db: db, memory: raft.NewMemoryStorage(), conf: &pb.ConfState{},
		pending: make(map[key]chan response), done: make(chan struct{}),
	}
	fail := func(err error) (*Group, error) { _ = db.Close(); return nil, err }
	persisted, err := loadStore(db)
	if err != nil {
		return fail(err)
	}
	if !raft.IsEmptySnap(persisted.snapshot) {
		if err := g.restore(persisted.snapshot); err != nil {
			return fail(err)
		}
		if err := g.memory.ApplySnapshot(persisted.snapshot); err != nil {
			return fail(err)
		}
	}
	g.memory.SetHardState(persisted.hard)
	if err := g.memory.Append(persisted.entries); err != nil {
		return fail(err)
	}
	g.node, err = raft.NewRawNode(&raft.Config{
		ID: config.ID, Storage: g.memory, Applied: g.applied, ElectionTick: 10, HeartbeatTick: 2,
		PreVote: true, CheckQuorum: true, DisableProposalForwarding: true,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 64, MaxInflightBytes: 4 * config.MaxEntryBytes,
		MaxUncommittedEntriesSize: 4 * config.MaxEntryBytes, Logger: config.Logger,
	})
	if err != nil {
		return fail(err)
	}
	if err := g.flush(); err != nil {
		return fail(err)
	}
	if config.TickInterval > 0 {
		g.ticks.Go(g.runTicks)
	}
	return g, nil
}

// ValidMemberID reports whether id is available for a network Raft peer.
func ValidMemberID(id uint64) bool {
	return id != 0 && !raft.IsLocalMsgTarget(id)
}

func (g *Group) runTicks() {
	ticker := time.NewTicker(g.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.done:
			return
		case <-ticker.C:
			_ = g.Tick(context.Background())
		}
	}
}

func (g *Group) restore(snapshot *pb.Snapshot) error {
	confIndex, data, err := decodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := g.cfg.Machine.Restore(data); err != nil {
		return err
	}
	meta := snapshot.GetMetadata()
	g.applied = meta.GetIndex()
	g.snapshotIndex = g.applied
	g.configIndex = confIndex
	g.conf = meta.GetConfState()
	g.logBytes = 0
	return nil
}

func (g *Group) apply(entry *pb.Entry) error {
	if entry.GetIndex() != g.applied+1 {
		return errors.New("committed Raft log has an apply gap")
	}
	switch entry.GetType() {
	case pb.EntryNormal:
		data := entry.GetData()
		if len(data) > 0 {
			if len(data) < len(key{}) {
				return errors.New("invalid committed Raft command")
			}
			body := g.cfg.Machine.Apply(data[len(key{}):])
			g.complete(data[:len(key{})], body)
		}
	case pb.EntryConfChange:
		change := &pb.ConfChange{}
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return err
		}
		g.conf = g.node.ApplyConfChange(change)
		g.configIndex = entry.GetIndex()
		g.complete(change.GetContext(), nil)
	case pb.EntryConfChangeV2:
		change := &pb.ConfChangeV2{}
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return err
		}
		g.conf = g.node.ApplyConfChange(change)
		g.configIndex = entry.GetIndex()
		g.complete(change.GetContext(), nil)
	default:
		return errors.New("unknown committed Raft entry type")
	}
	g.applied = entry.GetIndex()
	if config := g.configuration; config.index == g.applied {
		// Raft can turn a configuration proposal into an empty normal
		// entry while an earlier configuration is still unapplied.
		// An actual configuration entry already completed its waiter.
		if waiting := g.pending[config.id]; waiting != nil {
			delete(g.pending, config.id)
			waiting <- response{err: ErrConfChangePending}
		}
		g.configuration = configProposal{}
	}
	return nil
}

func (g *Group) complete(data, body []byte) {
	if len(data) != len(key{}) {
		return
	}
	id := key(data)
	if waiting := g.pending[id]; waiting != nil {
		delete(g.pending, id)
		waiting <- response{body: body}
	}
}

func (g *Group) failPending(err error) {
	g.configuration = configProposal{}
	for id, waiting := range g.pending {
		delete(g.pending, id)
		waiting <- response{err: err}
	}
}

// flush runs with exclusive ownership of all consensus and application state.
func (g *Group) flush() error {
	for g.node.HasReady() {
		ready := g.node.Ready()
		if err := saveReady(g.db, ready); err != nil {
			return fmt.Errorf("persist raft entries: %w", err)
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			if err := g.restore(ready.Snapshot); err != nil {
				return err
			}
			if err := g.memory.ApplySnapshot(ready.Snapshot); err != nil {
				return err
			}
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			g.memory.SetHardState(ready.HardState)
		}
		if err := g.memory.Append(ready.Entries); err != nil {
			return err
		}
		for _, entry := range ready.Entries {
			g.logBytes += uint64(proto.Size(entry))
		}
		for _, entry := range ready.CommittedEntries {
			if err := g.apply(entry); err != nil {
				return err
			}
		}
		g.node.Advance(ready)
		// A new learner rejects a snapshot whose ConfState predates its
		// admission. Publish a checkpoint covering each membership change
		// before this group needs to send a snapshot for that membership.
		if g.configIndex > g.snapshotIndex {
			if err := g.checkpoint(); err != nil {
				return err
			}
		}
		for _, message := range ready.Messages {
			if message.GetType() == pb.MsgSnap &&
				message.GetSnapshot().GetMetadata().GetIndex() < g.configIndex {
				g.node.ReportSnapshot(message.GetTo(), raft.SnapshotFailure)
				continue
			}
			if err := g.cfg.Send(message); err != nil {
				if message.GetType() == pb.MsgSnap {
					g.node.ReportSnapshot(message.GetTo(), raft.SnapshotFailure)
				} else {
					g.node.ReportUnreachable(message.GetTo())
				}
			}
		}
	}
	if g.node.BasicStatus().RaftState != raft.StateLeader {
		g.failPending(ErrLeadershipLost)
	}
	if g.applied-g.snapshotIndex >= g.cfg.SnapshotEntries || g.logBytes >= g.cfg.SnapshotBytes {
		return g.checkpoint()
	}
	return nil
}

func (g *Group) checkpoint() error {
	if g.applied == g.snapshotIndex {
		return nil
	}
	data, err := g.cfg.Machine.Snapshot()
	if err != nil {
		return err
	}
	snapshot, err := g.memory.CreateSnapshot(g.applied, g.conf, encodeSnapshot(g.configIndex, data))
	if err != nil {
		return err
	}
	if err := saveSnapshot(g.db, snapshot); err != nil {
		return err
	}
	if err := g.memory.Compact(g.applied); err != nil {
		return err
	}
	g.snapshotIndex = g.applied
	g.logBytes = 0
	return nil
}

func (g *Group) stop(err error) {
	if g.terminal == nil {
		g.terminal = errors.Join(ErrStopped, err)
		close(g.done)
		g.failPending(errors.Join(ErrLeadershipLost, err))
	}
}

func (g *Group) drive() error {
	if err := g.flush(); err != nil {
		g.stop(err)
		return err
	}
	return nil
}

func (g *Group) change(ctx context.Context, fn func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.terminal != nil {
		return g.terminal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	return g.drive()
}

func (g *Group) Campaign(ctx context.Context) error { return g.change(ctx, g.node.Campaign) }
func (g *Group) Tick(ctx context.Context) error {
	return g.change(ctx, func() error { g.node.Tick(); return nil })
}

func (g *Group) Step(ctx context.Context, message *pb.Message) error {
	if message == nil || message.GetTo() != g.cfg.ID || !ValidMemberID(message.GetFrom()) || message.GetFrom() == g.cfg.ID {
		return errors.New("invalid raft message endpoints")
	}
	return g.change(ctx, func() error {
		if err := g.node.Step(message); !errors.Is(err, raft.ErrStepPeerNotFound) {
			return err
		}
		return nil
	})
}

func (g *Group) Propose(ctx context.Context, data []byte) ([]byte, error) {
	if uint64(len(data)) > g.cfg.MaxEntryBytes-uint64(len(key{})) {
		return nil, ErrTooLarge
	}
	return g.propose(ctx, false, func(id key) error {
		command := make([]byte, 0, len(id)+len(data))
		command = append(command, id[:]...)
		return g.node.Propose(append(command, data...))
	})
}

func (g *Group) propose(ctx context.Context, configuration bool, submit func(key) error) ([]byte, error) {
	var id key
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	result := make(chan response, 1)
	err := g.change(ctx, func() error {
		if g.node.BasicStatus().RaftState != raft.StateLeader {
			return ErrNotLeader
		}
		if len(g.pending) >= g.cfg.MaxPending {
			return ErrBusy
		}
		var configIndex uint64
		if configuration {
			if g.configuration.index != 0 {
				return ErrConfChangePending
			}
			// Previous operations flush Ready before releasing g.mu, so
			// the next accepted entry follows the durable last index.
			last, err := g.memory.LastIndex()
			if err != nil {
				return err
			}
			configIndex = last + 1
		}
		if err := submit(id); err != nil {
			// RawNode returns ErrProposalDropped before appending an entry.
			// A caller can retry this rejection without replaying a command.
			if errors.Is(err, raft.ErrProposalDropped) {
				return errors.Join(ErrBusy, err)
			}
			return err
		}
		g.pending[id] = result
		if configuration {
			g.configuration = configProposal{id: id, index: configIndex}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	select {
	case got := <-result:
		return got.body, got.err
	case <-ctx.Done():
		g.mu.Lock()
		delete(g.pending, id)
		g.mu.Unlock()
		return nil, errors.Join(ErrLeadershipLost, ctx.Err())
	}
}

func (g *Group) ChangeMember(ctx context.Context, typ pb.ConfChangeType, id uint64) error {
	if !ValidMemberID(id) {
		return ErrInvalidMembership
	}
	_, err := g.propose(ctx, true, func(request key) error {
		voter := slices.Contains(g.conf.GetVoters(), id)
		learner := slices.Contains(g.conf.GetLearners(), id)
		switch typ {
		case pb.ConfChangeAddLearnerNode:
			if learner || voter {
				return ErrInvalidMembership
			}
		case pb.ConfChangeAddNode:
			if !learner || !g.caughtUp(id) {
				return ErrLearnerNotReady
			}
		case pb.ConfChangeRemoveNode:
			if (!learner && !voter) || (voter && len(g.conf.GetVoters()) <= 1) {
				return ErrInvalidMembership
			}
		default:
			return ErrInvalidMembership
		}
		return g.node.ProposeConfChange(&pb.ConfChange{Type: typ.Enum(), NodeId: new(id), Context: request[:]})
	})
	return err
}
func (g *Group) AddLearner(ctx context.Context, id uint64) error {
	return g.ChangeMember(ctx, pb.ConfChangeAddLearnerNode, id)
}
func (g *Group) Promote(ctx context.Context, id uint64) error {
	return g.ChangeMember(ctx, pb.ConfChangeAddNode, id)
}
func (g *Group) Remove(ctx context.Context, id uint64) error {
	return g.ChangeMember(ctx, pb.ConfChangeRemoveNode, id)
}

func (g *Group) caughtUp(id uint64) bool {
	status := g.node.BasicStatus()
	if status.RaftState != raft.StateLeader {
		return false
	}
	var result bool
	g.node.WithProgress(func(peer uint64, _ raft.ProgressType, progress tracker.Progress) {
		if peer == id {
			result = progress.RecentActive && progress.Match >= status.GetCommit()
		}
	})
	return result
}
func (g *Group) CaughtUp(id uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.terminal == nil && g.caughtUp(id)
}

func (g *Group) TransferLeadership(ctx context.Context, id uint64) error {
	return g.change(ctx, func() error {
		if g.node.BasicStatus().RaftState != raft.StateLeader {
			return ErrNotLeader
		}
		if !slices.Contains(g.conf.GetVoters(), id) {
			return ErrInvalidMembership
		}
		if id == g.cfg.ID {
			return nil
		}
		if !g.caughtUp(id) {
			return ErrLearnerNotReady
		}
		g.node.TransferLeader(id)
		return nil
	})
}
func (g *Group) ReportSnapshot(ctx context.Context, id uint64, success bool) error {
	return g.change(ctx, func() error {
		status := raft.SnapshotFailure
		if success {
			status = raft.SnapshotFinish
		}
		g.node.ReportSnapshot(id, status)
		return nil
	})
}
func (g *Group) ReportUnreachable(ctx context.Context, id uint64) error {
	return g.change(ctx, func() error { g.node.ReportUnreachable(id); return nil })
}

func (g *Group) Status() Status {
	g.mu.Lock()
	defer g.mu.Unlock()
	basic := g.node.BasicStatus()
	status := Status{
		ID: g.cfg.ID, GroupID: g.cfg.GroupID, Applied: g.applied, Commit: basic.GetCommit(),
		ConfigIndex: g.configIndex, Voters: slices.Clone(g.conf.GetVoters()), Learners: slices.Clone(g.conf.GetLearners()),
	}
	slices.Sort(status.Voters)
	slices.Sort(status.Learners)
	if g.terminal == nil {
		status.Leader = basic.Lead
		if basic.RaftState == raft.StateLeader {
			status.Progress = make(map[uint64]Progress)
			g.node.WithProgress(func(peer uint64, _ raft.ProgressType, p tracker.Progress) {
				status.Progress[peer] = Progress{Match: p.Match, Next: p.Next, RecentActive: p.RecentActive}
			})
		}
	}
	return status
}

func (g *Group) Leader() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.terminal != nil {
		return 0
	}
	return g.node.BasicStatus().Lead
}
func (g *Group) Done() <-chan struct{} { return g.done }
func (g *Group) Snapshot(ctx context.Context) error {
	return g.change(ctx, func() error {
		if err := g.checkpoint(); err != nil {
			g.stop(err)
			return err
		}
		return nil
	})
}
func (g *Group) MachineSnapshot(ctx context.Context) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.terminal != nil {
		return nil, g.terminal
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return g.cfg.Machine.Snapshot()
}
func (g *Group) Close() error {
	g.closeOnce.Do(func() {
		g.mu.Lock()
		g.stop(nil)
		g.mu.Unlock()
		g.ticks.Wait()
		g.closeErr = g.db.Close()
	})
	return g.closeErr
}
