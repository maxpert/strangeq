package clusterraft

import (
	"errors"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const raftAppendMessageBytes = 1 << 20

func (g *Group) proposeOrdinary(command []byte, gather time.Duration) error {
	previous := g.takeGatheredProposal()
	if previous == nil {
		if gather > 0 && g.appending == nil {
			g.gatheredProposal = command
			g.scheduleProposalGather(gather)
			return nil
		}
		return g.node.Propose(command)
	}
	// Submit both entries before Raft emits the first replication message.
	// Leave room for both entries' protobuf fields.
	if len(previous)+len(command) <= raftAppendMessageBytes-64 {
		err := g.node.Step(&pb.Message{
			Type: pb.MsgProp.Enum(), From: new(g.cfg.ID),
			Entries: []*pb.Entry{{Data: previous}, {Data: command}},
		})
		if !errors.Is(err, raft.ErrProposalDropped) {
			g.finishRejectedGatheredProposal(previous, err)
			return err
		}
	}
	// A rejected vector appended neither entry. Preserve per-command
	// admission when only one fits in the remaining Raft allowance.
	g.finishRejectedGatheredProposal(previous, g.node.Propose(previous))
	err := g.node.Propose(command)
	if err != nil {
		// The prior proposal might still have been accepted and needs a drive.
		if driveErr := g.drive(false); driveErr != nil {
			return driveErr
		}
	}
	return err
}

func (g *Group) flushGatheredProposal() {
	if command := g.takeGatheredProposal(); command != nil {
		g.finishRejectedGatheredProposal(command, g.node.Propose(command))
	}
}

func (g *Group) finishRejectedGatheredProposal(command []byte, err error) {
	if err == nil {
		return
	}
	id := key(command[:len(key{})])
	if waiting := g.pending[id]; waiting != nil {
		delete(g.pending, id)
		if errors.Is(err, raft.ErrProposalDropped) {
			err = errors.Join(ErrBusy, err)
		}
		waiting <- response{err: err}
	}
}

func (g *Group) scheduleProposalGather(window time.Duration) {
	var timer *time.Timer
	timer = time.AfterFunc(window, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		var err error
		defer g.stopOnStoreReadError(&err)
		if g.proposalGather == timer && g.stopped() == nil {
			err = g.drive(false)
		}
	})
	g.proposalGather = timer
}

func (g *Group) takeGatheredProposal() []byte {
	command := g.gatheredProposal
	if g.proposalGather != nil {
		g.proposalGather.Stop()
		g.proposalGather, g.gatheredProposal = nil, nil
	}
	return command
}
