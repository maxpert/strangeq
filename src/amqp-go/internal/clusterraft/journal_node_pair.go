package clusterraft

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
)

// NodeJournalPair is a private experiment with two independent journal owners.
// Group parity is part of its storage identity: even IDs use stream 0, odd IDs
// use stream 1. Each Group retains the ordinary append/apply response fences.
// The caller must persist the two-stream mode before provisioning either file.
type NodeJournalPair struct {
	streams  [2]*NodeJournal
	peer     uint64
	mu       sync.Mutex // Serializes Group opening against complete owner closure.
	closed   bool
	closeErr error
}

// OpenNodeJournalPairWithRangeSync acquires and replays both independent owners
// before returning. create has the same provisioning-only meaning as it does
// for OpenNodeJournal. There is deliberately no single-stream fallback.
func OpenNodeJournalPairWithRangeSync(root string, create bool, peer uint64) (*NodeJournalPair, error) {
	return OpenNodeJournalPairWithOptions(root, create, peer, JournalOptions{RangeSync: true})
}

func OpenNodeJournalPairWithOptions(root string, create bool, peer uint64, options JournalOptions) (*NodeJournalPair, error) {
	var durability *journalDurability
	if options.ProcessDurability {
		durability = &journalDurability{}
	}

	quota := &journalSegmentQuota{limit: journalGroupMaxFileBytes, streams: 2}
	return openJournalPair(root, peer, func(dir string, _ int, budget *journalBudget) (*journalGroupNode, error) {
		store, err := openSegmentedGroupNode(dir, create, options.RangeSync, journalServingExtentBytes, journalGroupMaxFileBytes, budget,
			journalSegmentHooks{sharedQuota: quota})
		if err == nil {
			store.writer.durability = durability
		}
		return store, err
	})
}

func openJournalPair(root string, peer uint64, open func(string, int, *journalBudget) (*journalGroupNode, error)) (_ *NodeJournalPair, err error) {
	if !ValidMemberID(peer) {
		return nil, errors.New("two-stream journal requires a valid node identity")
	}
	n := &NodeJournalPair{peer: peer}
	budget := newJournalBudget(journalGroupWriterLimits())
	defer func() {
		if err != nil {
			for _, stream := range n.streams {
				if stream != nil {
					err = errors.Join(err, stream.Close())
				}
			}
		}
	}()
	for index := range n.streams {
		dir := filepath.Join(root, fmt.Sprintf("raft-journal-%d", index))
		store, openErr := open(dir, index, budget)
		if openErr != nil {
			return nil, fmt.Errorf("open node journal stream %d: %w", index, openErr)
		}
		stream := &NodeJournal{store: store}
		n.streams[index] = stream
		stream.store.mu.Lock()
		for id, state := range stream.store.groups {
			if id%2 != uint64(index) || !checkIdentity(state.identity, Config{ID: peer, GroupID: id}) {
				err = fmt.Errorf("node journal stream %d has incompatible group %d routing or owner", index, id)
				break
			}
		}
		stream.store.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

func (n *NodeJournalPair) OpenGroup(config Config) (*Group, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, errJournalClosed
	}
	if config.ID != n.peer {
		return nil, errors.New("two-stream journal group owner mismatch")
	}
	if err := n.Failure(); err != nil {
		return nil, err
	}
	return n.streams[config.GroupID%2].OpenGroup(config)
}

func (n *NodeJournalPair) GroupIDs() []uint64 {
	var ids []uint64
	for _, stream := range n.streams {
		ids = append(ids, stream.GroupIDs()...)
	}
	slices.Sort(ids)
	return ids
}

func (n *NodeJournalPair) HasGroup(id uint64) bool {
	return n.streams[id%2].HasGroup(id)
}

// Failure covers both writers, including a stream with no live Group handles.
func (n *NodeJournalPair) Failure() error {
	var failure error
	for index, stream := range n.streams {
		if stream == nil {
			failure = errors.Join(failure, fmt.Errorf("node journal stream %d is unavailable", index))
		} else if err := stream.Failure(); err != nil {
			failure = errors.Join(failure, fmt.Errorf("node journal stream %d: %w", index, err))
		}
	}
	return failure
}

func (n *NodeJournalPair) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return n.closeErr
	}
	// Preserve both ownership locks if any Group is still alive. New Groups
	// can only enter through OpenGroup, which shares this lifecycle mutex.
	for _, stream := range n.streams {
		stream.store.mu.Lock()
		owned := len(stream.store.owners) != 0
		stream.store.mu.Unlock()
		if owned {
			return errors.New("close node journal pair after all group handles")
		}
	}
	n.closed = true
	for _, stream := range n.streams {
		n.closeErr = errors.Join(n.closeErr, stream.Close())
	}
	return n.closeErr
}

// RequireFullDurability closes both fast paths atomically before fencing either
// stream. It attempts every fence even if an earlier stream has failed.
func (n *NodeJournalPair) RequireFullDurability() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return errJournalClosed
	}
	for _, stream := range n.streams {
		if stream.store.writer.durability != nil {
			stream.store.writer.durability.strict.Store(true)
		}
	}
	var err error
	for _, stream := range n.streams {
		err = errors.Join(err, stream.RequireFullDurability())
	}
	return err
}
