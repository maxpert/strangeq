package clusterraft

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSelectedCaptureSaturationRejectsBeforeLockedMaterialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	node, group, _, _, _ := openDeferredReplyGroup(t, ctx)
	_, err := group.Propose(ctx, []byte("ready"))
	require.NoError(t, err)
	slots := node.replySlots
	for range cap(slots) {
		slots <- struct{}{}
	}
	called := false
	_, err = group.ReadCaptured(ctx, func(Machine) (func() ([]byte, error), int, error) {
		return func() ([]byte, error) { called = true; return make([]byte, 2<<20), nil }, 2 << 20, nil
	}, CaptureOffLock)
	require.ErrorIs(t, err, ErrBusy)
	require.False(t, called)
	require.Equal(t, cap(slots), len(slots))
	for range cap(slots) {
		<-slots
	}
	body, err := group.ReadCaptured(ctx, func(Machine) (func() ([]byte, error), int, error) {
		return func() ([]byte, error) {
			unlocked := make(chan struct{})
			go func() { group.mu.Lock(); close(unlocked); group.mu.Unlock() }()
			select {
			case <-unlocked:
			case <-time.After(time.Second):
				return nil, io.ErrNoProgress
			}
			called = true
			return make([]byte, 2<<20), nil
		}, 2 << 20, nil
	}, CaptureOffLock)
	require.NoError(t, err)
	require.True(t, called)
	require.Len(t, body, 2<<20)
	require.Zero(t, len(slots))
}

func TestSelectedCaptureWrappedStorageFailureKeepsTerminalCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	node, group, _, _, _ := openDeferredReplyGroup(t, ctx)
	_, err := group.Propose(ctx, []byte("ready"))
	require.NoError(t, err)
	_, err = group.ReadCaptured(ctx, func(Machine) (func() ([]byte, error), int, error) {
		return func() ([]byte, error) { return nil, io.ErrUnexpectedEOF }, captureJournalReplyBytes, nil
	}, CaptureOffLock)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.ErrorIs(t, err, ErrLeadershipLost)
	require.Zero(t, len(node.replySlots))
	_, err = group.Propose(ctx, []byte("must-stay-fenced"))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
