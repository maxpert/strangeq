package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Item 7 (Sol): cancelling a queued waiter must promote the NEXT waiter when
// it fits — exact 6/5/4 schedule.
func TestLimiter_CancelPromotesNextWaiter(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	ctx := context.Background()

	a, err := l.Reserve(ctx, "a", 6) // used=6
	require.NoError(t, err)
	defer a.Abort()

	bLease, bWait, err := l.Begin("b", 5) // 6+5 > 10: waits
	require.NoError(t, err)
	require.False(t, bLease.Valid())
	cLease, cWait, err := l.Begin("c", 4) // waits behind b (no barging)
	require.NoError(t, err)
	require.False(t, cLease.Valid())

	require.Eventually(t, func() bool {
		_, _, w := l.Stats()
		return w == 2
	}, 30*time.Second, time.Millisecond)

	// b never fits while a is held; cancelling b must promote c (6+4 <= 10).
	bWait.Cancel()
	select {
	case g := <-cWait.C():
		require.NoError(t, g.err)
		require.True(t, g.tok.Valid(), "the cancelled head's bytes must promote c")
	case <-time.After(30 * time.Second):
		t.Fatal("c was not promoted after the head waiter cancelled")
	}
	used, _, waiters := l.Stats()
	assert.Equal(t, int64(10), used)
	assert.Equal(t, 0, waiters)
}

// Item 1 (Opus): the Begin/LeaseWait API — immediate grants, queued waits,
// idempotent race-safe Cancel.
func TestLimiter_BeginAndLeaseWaitCancel(t *testing.T) {
	t.Run("immediate grant returns no wait", func(t *testing.T) {
		l := NewPublishLimiter(10)
		defer l.Close()
		le, wait, err := l.Begin("a", 5)
		require.NoError(t, err)
		require.True(t, le.Valid())
		assert.Nil(t, wait)
		le.Abort()
	})

	t.Run("cancel before grant removes the waiter", func(t *testing.T) {
		l := NewPublishLimiter(10)
		defer l.Close()
		a, err := l.Reserve(context.Background(), "a", 10)
		require.NoError(t, err)
		defer a.Abort()

		le, wait, err := l.Begin("b", 5)
		require.NoError(t, err)
		require.False(t, le.Valid())
		require.NotNil(t, wait)
		require.Eventually(t, func() bool {
			_, _, w := l.Stats()
			return w == 1
		}, 30*time.Second, time.Millisecond)

		wait.Cancel()
		wait.Cancel() // idempotent
		select {
		case g := <-wait.C():
			t.Fatalf("cancelled wait delivered: %+v", g)
		default:
		}
		_, _, w := l.Stats()
		assert.Equal(t, 0, w)
	})

	t.Run("cancel racing a grant returns the bytes", func(t *testing.T) {
		for i := 0; i < 200; i++ {
			l := NewPublishLimiter(10)
			a, err := l.Reserve(context.Background(), "a", 10)
			require.NoError(t, err)
			le, wait, err := l.Begin("b", 6)
			require.NoError(t, err)
			require.False(t, le.Valid())
			require.Eventually(t, func() bool {
				_, _, w := l.Stats()
				return w == 1
			}, 30*time.Second, time.Millisecond)
			// Cancel races the grant triggered by a's release. Either the
			// cancel removes the waiter first (C never delivers — the caller
			// must NOT read it after Cancel) or the grant is delivered and
			// Cancel drains + releases it. Both end at used == 0, waiters == 0.
			go wait.Cancel()
			a.Abort()
			require.Eventually(t, func() bool {
				used, _, w := l.Stats()
				return used == 0 && w == 0
			}, 30*time.Second, time.Millisecond, "iteration %d", i)
			l.Close()
		}
	})

	t.Run("Begin after Close fails", func(t *testing.T) {
		l := NewPublishLimiter(10)
		l.Close()
		_, _, err := l.Begin("a", 1)
		assert.ErrorIs(t, err, ErrLimiterClosed)
		assert.True(t, l.Closed())
	})
}

// Item 8: TryReserve after Close is distinguishable from an overbudget
// TryReserve via Closed().
func TestLimiter_ClosedDistinguishableFromOverbudget(t *testing.T) {
	l := NewPublishLimiter(10)
	defer l.Close()
	a, err := l.Reserve(context.Background(), "a", 10)
	require.NoError(t, err)
	_, ok := l.TryReserve("b", 5)
	assert.False(t, ok, "overbudget")
	assert.False(t, l.Closed(), "not closed — an overbudget refusal")
	a.Abort()
	l.Close()
	_, ok = l.TryReserve("b", 1)
	assert.False(t, ok)
	assert.True(t, l.Closed(), "closed — a Close refusal")
}
