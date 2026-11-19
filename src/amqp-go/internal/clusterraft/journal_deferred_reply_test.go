package clusterraft

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type deferredReplyFixture struct {
	testMachine
	reading chan struct{}
	release chan struct{}
	readErr error
}

func (m *deferredReplyFixture) ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error) {
	body, encode, err := m.ApplyJournaledReply(data, wantReply, source)
	if encode != nil {
		return encode()
	}
	return body, err
}

func (m *deferredReplyFixture) ApplyJournaledReply(data []byte, wantReply bool, source io.ReaderAt) ([]byte, func() ([]byte, error), error) {
	body := m.Apply(data)
	if !wantReply {
		return nil, nil, nil
	}
	if string(data) != "claim" {
		return body, nil, nil
	}
	commandBytes := len(data)
	return nil, func() ([]byte, error) {
		close(m.reading)
		<-m.release
		payload := make([]byte, commandBytes)
		_, err := source.ReadAt(payload, 0)
		if err == nil {
			err = m.readErr
		}
		return append([]byte("read committed "), payload...), err
	}, nil
}

func TestJournalDeferredReplyReadFailureSurvivesGroupClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	_, group, _, machine, unblock := openDeferredReplyGroup(t, ctx)
	failure := errors.New("injected journal body read failure")
	machine.readErr = failure
	claim := proposeJournalGroup(ctx, group, "claim")
	waitForDeferredFixtureRead(t, ctx, machine)
	closed := make(chan error, 1)
	go func() { closed <- group.Close() }()
	select {
	case <-group.Done():
	case <-ctx.Done():
		t.Fatal("group close did not begin while the body read was held")
	}
	unblock()
	select {
	case got := <-claim:
		require.ErrorIs(t, got.err, failure)
		require.Empty(t, got.body, "a failed body read cannot return a partial reply")
	case <-ctx.Done():
		t.Fatal("the committed claim never reported its read failure")
	}
	select {
	case err := <-closed:
		require.ErrorIs(t, err, failure)
	case <-ctx.Done():
		t.Fatal("group close did not report the deferred failure")
	}
}

func openDeferredReplyGroup(t *testing.T, ctx context.Context) (*journalGroupNode, *Group, Config, *deferredReplyFixture, func()) {
	t.Helper()
	dir := t.TempDir()
	machine := &deferredReplyFixture{reading: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	unblock := func() { released.Do(func() { close(machine.release) }) }
	node := openTestJournalGroupNode(t, dir, nil)
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1}, Machine: machine, TickInterval: -1,
		SnapshotBytes: 1 << 20, SnapshotEntries: 1000,
	}
	group := openTestJournalGroup(t, node, config)
	t.Cleanup(unblock)
	require.NoError(t, group.Campaign(ctx))
	return node, group, config, machine, unblock
}

func waitForDeferredFixtureRead(t *testing.T, ctx context.Context, machine *deferredReplyFixture) {
	t.Helper()
	select {
	case <-machine.reading:
	case <-ctx.Done():
		t.Fatal("the committed claim never reached its journal body read")
	}
}

func TestJournalDeferredReplyReadKeepsLaterProposalMoving(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	_, group, _, machine, unblock := openDeferredReplyGroup(t, ctx)
	claim := proposeJournalGroup(ctx, group, "claim")
	waitForDeferredFixtureRead(t, ctx, machine)
	later := proposeJournalGroup(ctx, group, "independent publish")
	select {
	case got := <-later:
		require.NoError(t, got.err)
		require.Equal(t, []byte("applied independent publish"), got.body)
	case <-time.After(time.Second):
		t.Fatal("a held claim body read prevented a later proposal from committing")
	}
	unblock()
	select {
	case got := <-claim:
		require.NoError(t, got.err)
		require.Equal(t, []byte("read committed claim"), got.body)
	case <-ctx.Done():
		t.Fatal("the captured claim did not return its original reply")
	}
}

func TestJournalDeferredReplyCancellationAndCloseRetainSourceOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	node, group, config, machine, unblock := openDeferredReplyGroup(t, ctx)
	request, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	claim := proposeJournalGroup(request, group, "claim")
	waitForDeferredFixtureRead(t, ctx, machine)
	cancelRequest()
	select {
	case got := <-claim:
		require.ErrorIs(t, got.err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceling a committed claim waited for its held body read")
	}
	closed := make(chan error, 1)
	go func() { closed <- group.Close() }()
	select {
	case <-group.Done():
	case <-ctx.Done():
		t.Fatal("group close did not stop normal admission")
	}
	select {
	case err := <-closed:
		t.Fatalf("group close released storage during the held read: %v", err)
	default:
	}
	require.Error(t, node.close(), "the node must retain journal ownership until its active body read exits")
	unblock()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("the group did not close after the body read released its ownership")
	}
	require.NoError(t, node.close())

	config.Peers, config.Machine = nil, &testMachine{}
	reopened := openTestJournalGroupNode(t, node.dir, nil)
	restored := openTestJournalGroup(t, reopened, config)
	state, err := restored.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["claim"]`, string(state), "cancellation must leave the committed transition in the journal")
}
