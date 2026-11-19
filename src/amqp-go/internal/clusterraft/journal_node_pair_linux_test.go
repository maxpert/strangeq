//go:build linux

package clusterraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type pairProofMachine struct{ values [][]byte }

func (m *pairProofMachine) Apply(data []byte) []byte {
	m.values = append(m.values, bytes.Clone(data))
	return bytes.Clone(data)
}
func (m *pairProofMachine) Snapshot() ([]byte, error) { return json.Marshal(m.values) }
func (m *pairProofMachine) Restore(data []byte) error { return json.Unmarshal(data, &m.values) }

type pairRangeWriteGate struct {
	enabled atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (f *pairRangeWriteGate) writeAtSync(file *os.File, data []byte, offset int64) (int, error) {
	if f.enabled.CompareAndSwap(true, false) {
		close(f.started)
		<-f.release
	}
	return (journalRangeSyncFile{journalDataFile{file}}).writeAtSync(data, offset)
}

func TestNodeJournalPairHeldRangeWriteDoesNotBlockOtherStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	root := t.TempDir()
	gate := &pairRangeWriteGate{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer release()
	quota := &journalSegmentQuota{limit: journalGroupMaxFileBytes, streams: 2}
	node, err := openJournalPair(root, 1, func(dir string, index int, budget *journalBudget) (*journalGroupNode, error) {
		hooks := journalSegmentHooks{sharedQuota: quota}
		if index == 0 {
			hooks.writeAt = gate.writeAtSync
		}
		// The fixture's gated write performs the real RWF_SYNC syscall.
		return openSegmentedGroupNode(dir, true, index != 0, journalServingExtentBytes, quota.limit, budget, hooks)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	groups := make([]*Group, 2)
	for index := range groups {
		id := uint64(10 + index)
		group, err := node.OpenGroup(Config{
			ID: 1, GroupID: id, Dir: filepath.Join(root, fmt.Sprint(id)),
			Peers: []uint64{1}, Machine: &pairProofMachine{}, TickInterval: -1,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = group.Close() })
		require.Same(t, node.streams[index].store, group.store.(*journalGroupStore).node)
		require.NoError(t, group.Campaign(ctx))
		require.NoError(t, waitTestLocalWrites(ctx, group))
		groups[index] = group
	}
	var bodies [2][]byte
	for index := range bodies {
		bodies[index] = make([]byte, 64<<10)
		_, err := rand.Read(bodies[index])
		require.NoError(t, err)
	}
	gate.enabled.Store(true)
	blocked := make(chan response, 1)
	go func() {
		body, err := groups[0].Propose(ctx, bodies[0])
		blocked <- response{body: body, err: err}
	}()
	select {
	case <-gate.started:
	case early := <-blocked:
		t.Fatalf("stream 0 proposal did not reach the held range write: %+v", early)
	case <-ctx.Done():
		t.Fatal("stream 0 never reached its range write")
	}
	// The gate establishes the actual interleaving: stream 0 is inside its
	// synchronous write, before success. Stream 1 must still durably apply.
	otherCtx, otherCancel := context.WithTimeout(ctx, time.Second)
	defer otherCancel()
	body, err := groups[1].Propose(otherCtx, bodies[1])
	require.NoError(t, err, "a held stream 0 durability fence must not hold stream 1")
	require.Equal(t, bodies[1], body)
	select {
	case early := <-blocked:
		t.Fatalf("stream 0 replied before its own durability fence completed: %+v", early)
	default:
	}
	snapshot, err := groups[0].MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, "null", string(snapshot), "stream 0 must not apply before its own fence")
	release()
	select {
	case done := <-blocked:
		require.NoError(t, done.err)
		require.Equal(t, bodies[0], done.body)
	case <-ctx.Done():
		t.Fatal("stream 0 did not complete after releasing its range write")
	}
	for _, group := range groups {
		require.NoError(t, waitTestLocalWrites(ctx, group))
		require.NoError(t, group.Close())
	}
	require.NoError(t, node.Close())

	reopened, err := OpenNodeJournalPairWithRangeSync(root, false, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	for index, expected := range bodies {
		id := uint64(10 + index)
		require.Equal(t, []uint64{id}, reopened.streams[index].GroupIDs())
		group, err := reopened.OpenGroup(Config{
			ID: 1, GroupID: id, Dir: filepath.Join(root, fmt.Sprint(id)),
			Machine: &pairProofMachine{}, TickInterval: -1,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = group.Close() })
		snapshot, err := group.MachineSnapshot(ctx)
		require.NoError(t, err)
		var values [][]byte
		require.NoError(t, json.Unmarshal(snapshot, &values))
		require.Equal(t, [][]byte{expected}, values)
		require.NoError(t, group.Close())
	}
	require.NoError(t, reopened.Close())
}

func TestNodeJournalPairReportsEitherFailureAndRetainsBothOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	failure := errors.New("injected stream 1 fence failure")
	var failSync atomic.Bool
	quota := &journalSegmentQuota{limit: journalGroupMaxFileBytes, streams: 2}
	node, err := openJournalPair(root, 1, func(dir string, index int, budget *journalBudget) (*journalGroupNode, error) {
		hooks := journalSegmentHooks{sharedQuota: quota}
		if index == 1 {
			hooks.syncFile = func(file *os.File) error {
				if failSync.Load() {
					return failure
				}
				return syncJournalFile(file)
			}
		}
		return openSegmentedGroupNode(dir, true, index != 1, journalServingExtentBytes, quota.limit, budget, hooks)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	group, err := node.OpenGroup(Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(root, "11"),
		Peers: []uint64{1}, Machine: &pairProofMachine{}, TickInterval: -1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = group.Close() })
	require.NoError(t, group.Campaign(ctx))
	require.NoError(t, waitTestLocalWrites(ctx, group))
	failSync.Store(true)
	_, err = group.Propose(ctx, []byte("not confirmed"))
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, node.Failure(), failure)
	require.ErrorContains(t, node.Close(), "after all group handles")
	for stream := range 2 {
		competing, err := OpenNodeJournalWithRangeSync(filepath.Join(root, fmt.Sprintf("raft-journal-%d", stream)), false)
		if competing != nil {
			_ = competing.Close()
		}
		require.ErrorContains(t, err, "lock node raft journal", "partial close must not release either owner")
	}
	require.ErrorIs(t, group.Close(), failure)
	require.ErrorIs(t, node.Close(), failure)
	reopened, err := OpenNodeJournalPairWithRangeSync(root, false, 1)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
