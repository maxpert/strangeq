package clusterraft

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalCapturedReadKeepsProposalMovingAndReportsFaultOnClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	node, group, _, machine, unblock := openDeferredReplyGroup(t, ctx)
	_, err := group.Propose(ctx, []byte("ready"))
	require.NoError(t, err)
	read := make(chan response, 1)
	go func() {
		body, err := group.ReadCaptured(ctx, func(Machine) (func() ([]byte, error), int, error) {
			return func() ([]byte, error) {
				close(machine.reading)
				<-machine.release
				return []byte("partial body"), io.ErrUnexpectedEOF
			}, captureJournalReplyBytes, nil
		})
		read <- response{body: body, err: err}
	}()
	waitForDeferredFixtureRead(t, ctx, machine)
	select {
	case proposed := <-proposeJournalGroup(ctx, group, "later proposal"):
		require.NoError(t, proposed.err)
		require.Equal(t, []byte("applied later proposal"), proposed.body)
	case <-ctx.Done():
		t.Fatal("an assignment page blocked a later durable proposal")
	}

	closed := make(chan error, 1)
	go func() { closed <- group.Close() }()
	select {
	case <-group.Done():
	case <-ctx.Done():
		t.Fatal("group close did not start while the captured page was held")
	}
	require.Error(t, node.close(), "the held query must retain its journal reader")
	unblock()
	select {
	case result := <-read:
		require.ErrorIs(t, result.err, ErrLeadershipLost)
		require.ErrorIs(t, result.err, io.ErrUnexpectedEOF)
		require.Empty(t, result.body)
	case <-ctx.Done():
		t.Fatal("captured page did not report its read error")
	}
	select {
	case err := <-closed:
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	case <-ctx.Done():
		t.Fatal("the captured page failed to release its ownership")
	}
}

func TestJournalCapturedFailureCompletesReadCohortOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	_, group, _, _, _ := openDeferredReplyGroup(t, ctx)
	callbacks := 0
	waiters := [2]chan response{make(chan response, 2), make(chan response, 2)}
	group.mu.Lock()
	group.reads = make(map[key]*raftRead)
	for i, result := range waiters {
		group.reads[key{byte(i)}] = &raftRead{
			ctx: ctx, issued: true, result: result,
			query: func(Machine) ([]byte, error) {
				callbacks++
				return nil, group.failReplyEncoding(io.ErrUnexpectedEOF)
			},
		}
	}
	group.completeReads()
	group.mu.Unlock()

	require.Equal(t, 1, callbacks, "a terminal encoder must prevent later callbacks in the issued read cohort")
	for _, waiter := range waiters {
		require.Len(t, waiter, 1, "each quorum-fenced reader receives exactly one result")
		result := <-waiter
		require.ErrorIs(t, result.err, ErrLeadershipLost)
		require.Empty(t, result.body)
	}
}
