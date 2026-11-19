// Package clusterraft runs independent, durable Raft groups.
package clusterraft

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterproposal"

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

// A committed machine always applies the command. Only response materialization
// is optional. Errors indicate a local failure applying the committed entry.
type committedMachine interface {
	ApplyCommitted(data []byte, wantReply bool) ([]byte, error)
}

// A journaled machine may retain the bounded command source for the life of
// its Group. The append-only journal retains its bytes through checkpoints.
// Consumers must verify the integrity of bytes they read later.
type journaledMachine interface {
	ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error)
}

type Config struct {
	ID, GroupID uint64
	Dir         string
	Peers       []uint64
	Machine     Machine
	// DeferJournalCommit allows independent commit progress to wait for a
	// later durability fence. Only use it for journal-backed data shards.
	DeferJournalCommit bool
	// ProcessDurability permits kernel-write completion for this data group when
	// its journal owner is in isolated process mode. Catalog groups leave it off.
	ProcessDurability bool
	// BackgroundJournalCheckpoints stages routine snapshots outside the Group
	// lock. Only use it for journal-backed data shards.
	BackgroundJournalCheckpoints bool
	AssignmentReplicaBodies      bool
	// GatherProposal may assign an idle-writer wait to selected commands.
	// Later proposals and Raft inputs can flush the accepted command early.
	GatherProposal func([]byte) time.Duration
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

type key [clusterproposal.PrefixBytes]byte
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
	store                                         *journalGroupStore
	storeFailed                                   <-chan struct{}
	node                                          *raft.RawNode
	conf                                          *pb.ConfState
	applied, configIndex, snapshotIndex, logBytes uint64
	appliedLogBytes                               uint64
	pending                                       map[key]chan response
	reads                                         map[key]*raftRead
	readFence                                     readFence
	readTerm                                      uint64
	retiredRead                                   *key
	appliedWait                                   *appliedWaitSignal
	appliedWaiters                                int
	configuration                                 configProposal
	terminal                                      error
	terminalCause                                 error
	done                                          chan struct{}
	ticks                                         sync.WaitGroup
	writer                                        sync.WaitGroup
	checkpointWorkers                             sync.WaitGroup
	replyWorkers                                  sync.WaitGroup
	routineCheckpoint                             *routineJournalCheckpoint
	appendJobs                                    chan *appendJob
	appending                                     *appendJob
	proposalGather                                *time.Timer
	gatheredProposal                              []byte
	bufferedOps                                   int
	bufferedBytes                                 uint64
	maxBufferedBytes                              uint64
	exclusiveWaiters                              int
	closeOnce                                     sync.Once
	closeErr                                      error
}

// Open owns one segmented journal for a single group. Multi-group nodes open
// one NodeJournal and share it across their groups instead.
func Open(config Config) (*Group, error) {
	if config.Dir == "" {
		return nil, errors.New("raft group requires storage")
	}
	if _, err := os.Lstat(filepath.Join(config.Dir, "raft.db")); err == nil {
		return nil, errors.New("legacy raft.db storage requires a fresh directory")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	journal, err := OpenNodeJournal(config.Dir, true)
	if err != nil {
		return nil, err
	}
	group, err := journal.OpenGroup(config)
	if err != nil {
		_ = journal.Close()
		return nil, err
	}
	group.store.closeOwner = true
	return group, nil
}

func (n *journalGroupNode) openValidatedGroup(config Config) (_ *Group, err error) {
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
	store, snapshot, err := n.openStore(config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if machine, ok := config.Machine.(interface{ ReleaseJournalSources() }); ok {
				machine.ReleaseJournalSources()
			}
			_ = store.close()
		}
	}()
	maxBufferedBytes := min(4*config.MaxEntryBytes, journalReadyInputBytes)
	g := &Group{
		cfg: config, store: store, storeFailed: store.failureSignal(), conf: &pb.ConfState{},
		pending: make(map[key]chan response), done: make(chan struct{}), maxBufferedBytes: maxBufferedBytes,
	}
	defer g.stopOnStoreReadError(&err)
	if !raft.IsEmptySnap(snapshot) {
		if err := g.restore(snapshot); err != nil {
			return nil, err
		}
	}
	g.node, err = raft.NewRawNode(&raft.Config{
		ID: config.ID, Storage: g.store, Applied: g.applied, ElectionTick: 10, HeartbeatTick: 2,
		PreVote: true, CheckQuorum: true, DisableProposalForwarding: true, AsyncStorageWrites: true,
		MaxSizePerMsg: raftAppendMessageBytes, MaxInflightMsgs: 64, MaxInflightBytes: 4 * config.MaxEntryBytes,
		MaxUncommittedEntriesSize: maxBufferedBytes, Logger: config.Logger,
	})
	if err != nil {
		return nil, err
	}
	if err := g.flush(true); err != nil {
		return nil, err
	}
	if err := g.stopped(); err != nil {
		return nil, err
	}
	g.appendJobs = make(chan *appendJob, 1)
	g.writer.Go(g.runWriter)
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
	g.logBytes, g.appliedLogBytes = 0, 0
	g.discardObsoleteRoutineCheckpoint()
	return nil
}

func (g *Group) apply(entry *pb.Entry) error {
	if entry.GetIndex() != g.applied+1 {
		return errors.New("committed Raft log has an apply gap")
	}
	typ := entry.GetType()
	if typ == pb.EntryConfChange || typ == pb.EntryConfChangeV2 {
		if err := g.store.syncCommit(); err != nil {
			return err
		}
	}
	switch typ {
	case pb.EntryNormal:
		data := entry.GetData()
		if len(data) > 0 {
			if len(data) < len(key{}) {
				return errors.New("invalid committed Raft command")
			}
			if err := g.applyCommand(entry); err != nil {
				return err
			}
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
	if g.cfg.BackgroundJournalCheckpoints {
		g.appliedLogBytes += uint64(proto.Size(entry))
	}
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

func (g *Group) applyCommand(entry *pb.Entry) error {
	data := entry.GetData()
	id, command := data[:len(key{})], data[len(key{}):]
	wantReply := g.pending[key(id)] != nil
	var reply []byte
	var err error
	machine, journaled := g.cfg.Machine.(journaledMachine)
	if journaled {
		journal := g.store
		section, sourceErr := journal.commandSource(entry)
		if sourceErr != nil {
			return sourceErr
		}
		section = g.immutableCommandSource(section, cap(data))
		var source io.ReaderAt = section
		if g.cfg.AssignmentReplicaBodies {
			source = &assignmentJournalSource{journalCommandReader: section, index: entry.GetIndex(), term: entry.GetTerm()}
		}
		source, releaseChecksums := g.preparedBodyChecksumSource(source, entry)
		defer releaseChecksums()
		if captured, ok := machine.(deferredJournalReplyMachine); ok {
			var encode func() ([]byte, error)
			reply, encode, err = captured.ApplyJournaledReply(command, wantReply, source)
			if err == nil && encode != nil && wantReply {
				return g.completeCapturedReply(key(id), journal, encode)
			}
		} else {
			reply, err = machine.ApplyJournaled(command, wantReply, source)
		}
	} else if machine, ok := g.cfg.Machine.(committedMachine); ok {
		reply, err = machine.ApplyCommitted(command, wantReply)
	} else {
		reply = g.cfg.Machine.Apply(command)
	}
	if err == nil && wantReply {
		g.complete(id, reply)
	}
	return err
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

func (g *Group) checkpoint() error {
	if g.applied == g.snapshotIndex {
		return nil
	}
	data, err := g.checkpointData()
	if err != nil {
		return err
	}
	if err := g.store.checkpoint(g.applied, g.conf, data); err != nil {
		return err
	}
	g.recordCheckpoint(g.applied)
	return nil
}

func (g *Group) checkpointData() ([]byte, error) {
	var data []byte
	var err error
	if machine, ok := g.cfg.Machine.(interface {
		AppendSnapshot([]byte) ([]byte, error)
	}); ok {
		data, err = machine.AppendSnapshot(encodeSnapshot(g.configIndex, nil))
	} else {
		data, err = g.cfg.Machine.Snapshot()
		if err == nil {
			data = encodeSnapshot(g.configIndex, data)
		}
	}
	return data, err
}

func (g *Group) recordCheckpoint(index uint64) {
	g.snapshotIndex = index
	g.logBytes, g.appliedLogBytes = 0, 0
	g.logBytes, g.appliedLogBytes = g.store.remainingLogBytes(g.applied)
	g.discardObsoleteRoutineCheckpoint()
}

func (g *Group) stop(err error) {
	if g.terminal == nil {
		g.terminalCause = err
		g.terminal = errors.Join(ErrStopped, err)
		close(g.done)
		g.takeGatheredProposal()
		if work := g.routineCheckpoint; work != nil && work.ref.name != "" {
			g.releaseRoutineCheckpoint(true)
		}
		unknown := errors.Join(ErrLeadershipLost, err)
		g.failPending(unknown)
		g.failReads(unknown)
	}
}

// stopped also observes writer failure while holding the group lock. This
// closes the gap before an idle Group's worker consumes the failure signal.
func (g *Group) stopped() error {
	if g.terminal == nil && g.storeFailed != nil {
		select {
		case <-g.storeFailed:
			g.stop(g.store.failureCause())
		default:
		}
	}
	return g.terminal
}

func (g *Group) stopOnStoreReadError(err *error) {
	if fault := recover(); fault != nil {
		readErr, ok := fault.(*raftStoreReadError)
		if !ok {
			panic(fault)
		}
		g.stop(readErr)
		*err = g.terminal
	}
}

func (g *Group) drive(syncWrites bool) error {
	g.flushGatheredProposal()
	for {
		if err := g.flush(syncWrites); err != nil {
			g.stop(err)
			return err
		}
		if err := g.stopped(); err != nil {
			return err
		}
		status := g.node.BasicStatus()
		if status.RaftState != raft.StateLeader {
			g.failPending(ErrLeadershipLost)
		}
		if g.finishReads(status) {
			continue
		}
		g.notifyAppliedWaiters(status)
		return nil
	}
}

func (g *Group) change(ctx context.Context, fn func() error) error {
	return g.changeWithStorage(ctx, 0, false, fn)
}

func (g *Group) changeWithStorage(ctx context.Context, inputBytes uint64, syncWrites bool, fn func() error) error {
	return g.changeAndDrive(ctx, inputBytes, syncWrites, false, fn)
}

func (g *Group) changeAndDrive(ctx context.Context, inputBytes uint64, syncWrites, ordinaryProposal bool, fn func() error) (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	defer g.stopOnStoreReadError(&err)
	if !ordinaryProposal && g.gatheredProposal != nil {
		// Preserve proposal order across Raft inputs and exclusive operations.
		if err := g.drive(false); err != nil {
			return err
		}
	}
	if syncWrites && g.appending != nil && g.terminal == nil {
		if err := g.waitForWriter(ctx); err != nil {
			return err
		}
	}
	if err := g.stopped(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !syncWrites && g.exclusiveWaiters > 0 {
		return ErrBusy
	}
	if g.appending != nil {
		// Bound inputs RawNode can retain while the current write is stalled.
		if g.bufferedOps >= min(64, g.cfg.MaxPending) || inputBytes > g.maxBufferedBytes-g.bufferedBytes {
			return ErrBusy
		}
		g.bufferedOps++
		g.bufferedBytes += inputBytes
	}
	if err := fn(); err != nil {
		return err
	}
	if ordinaryProposal && g.gatheredProposal != nil {
		return nil
	}
	return g.drive(syncWrites)
}

// waitForWriter reserves admission for a snapshot or other exclusive change.
// It releases the group lock while waiting and reacquires it before returning.
func (g *Group) waitForWriter(ctx context.Context) error {
	g.exclusiveWaiters++
	defer func() { g.exclusiveWaiters-- }()
	for g.appending != nil && g.terminal == nil {
		done := g.appending.done
		g.mu.Unlock()
		select {
		case <-done:
		case <-g.done:
		case <-ctx.Done():
		}
		g.mu.Lock()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) Campaign(ctx context.Context) error {
	return g.changeWithStorage(ctx, 0, true, g.node.Campaign)
}
func (g *Group) Tick(ctx context.Context) error {
	return g.change(ctx, func() error {
		g.node.Tick()
		// An idle shard might have no Ready work of its own while other
		// shards rotate the journal files it references.
		if g.appending == nil && g.journalCheckpointNeeded() {
			if err := g.maintainCheckpoint(false); err != nil {
				g.stop(err)
				return err
			}
		}
		return nil
	})
}

func (g *Group) Step(ctx context.Context, message *pb.Message) error {
	if message == nil || message.GetTo() != g.cfg.ID || !ValidMemberID(message.GetFrom()) || message.GetFrom() == g.cfg.ID {
		return errors.New("invalid raft message endpoints")
	}
	if message.GetType() == pb.MsgReadIndex || message.GetType() == pb.MsgReadIndexResp {
		return errors.New("raft read index must be requested locally on the leader")
	}
	inputBytes, err := journalPeerInputBytes(message, g.cfg.MaxEntryBytes, uint64(proto.Size(message)))
	if err != nil {
		return err
	}
	// A successful snapshot transport receipt still represents a durable
	// installation (or an already-applied snapshot). Other messages are
	// accepted for processing; Raft acknowledgements carry durability.
	return g.changeWithStorage(ctx, inputBytes, message.GetType() == pb.MsgSnap, func() error {
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
	// Include the proposal key and maximum protobuf encoding overhead.
	inputBytes := uint64(len(data)) + uint64(len(key{})) + 32 + journalEntryInputOverhead
	var gather time.Duration
	if g.cfg.GatherProposal != nil {
		gather = g.cfg.GatherProposal(data)
	}
	return g.propose(ctx, inputBytes, false, func(id key) error {
		return g.proposeOrdinary(newProposalCommand(id, data), gather)
	})
}

func newProposalCommand(id key, data []byte) []byte {
	command := make([]byte, 0, len(id)+len(data))
	command = append(command, id[:]...)
	return append(command, data...)
}

// ProposeOwned consumes the final encoder allocation only after the same
// leader/pending/storage admission as Propose. It saves its full command copy;
// Raft still makes its existing immutable entry clone.
func (g *Group) ProposeOwned(ctx context.Context, owned *clusterproposal.Submission) ([]byte, error) {
	data := owned.Command()
	if data == nil {
		return nil, clusterproposal.ErrConsumed
	}
	if uint64(len(data)) > g.cfg.MaxEntryBytes-uint64(len(key{})) {
		return nil, ErrTooLarge
	}
	inputBytes := uint64(len(data)) + uint64(len(key{})) + 32 + journalEntryInputOverhead
	var gather time.Duration
	if g.cfg.GatherProposal != nil {
		gather = g.cfg.GatherProposal(data)
	}
	return g.propose(ctx, inputBytes, false, func(id key) error {
		if !g.cfg.ProcessDurability || !g.store.node.writer.durability.processAllowed() {
			// Activation can fence the process owner after the routing check.
			command := newProposalCommand(id, data)
			owned.Discard()
			return g.proposeOrdinary(command, gather)
		}
		command, err := owned.Take(id[:])
		if err != nil {
			return err
		}
		return g.proposeOrdinary(command, gather)
	})
}

func (g *Group) propose(ctx context.Context, inputBytes uint64, configuration bool, submit func(key) error) ([]byte, error) {
	var id key
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	result := make(chan response, 1)
	err := g.changeAndDrive(ctx, inputBytes, configuration, !configuration, func() error {
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
			// Configuration admission runs after the async writer has
			// drained, so this follows the durable last index.
			last, err := g.store.LastIndex()
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
	_, err := g.propose(ctx, 0, true, func(request key) error {
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
	return g.stopped() == nil && g.caughtUp(id)
}

func (g *Group) TransferLeadership(ctx context.Context, id uint64) error {
	return g.changeWithStorage(ctx, 0, true, func() error {
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
	if g.stopped() == nil {
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
	if g.stopped() != nil {
		return 0
	}
	return g.node.BasicStatus().Lead
}
func (g *Group) Done() <-chan struct{} { return g.done }
func (g *Group) Snapshot(ctx context.Context) error {
	return g.changeWithStorage(ctx, 0, true, func() error {
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
	if err := g.stopped(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return g.cfg.Machine.Snapshot()
}
func (g *Group) Close() error {
	g.closeOnce.Do(func() {
		g.mu.Lock()
		if g.stopped() == nil {
			g.stop(nil)
		}
		g.mu.Unlock()
		g.ticks.Wait()
		g.writer.Wait()
		g.checkpointWorkers.Wait()
		g.replyWorkers.Wait()
		g.mu.Lock()
		if machine, ok := g.cfg.Machine.(interface{ ReleaseJournalSources() }); ok {
			machine.ReleaseJournalSources()
		}
		g.appending, g.appendJobs = nil, nil
		if g.routineCheckpoint != nil {
			g.releaseRoutineCheckpoint(true)
		}
		g.closeErr = errors.Join(g.terminalCause, g.store.close())
		g.mu.Unlock()
	})
	return g.closeErr
}
