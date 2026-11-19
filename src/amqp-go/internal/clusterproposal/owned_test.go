package clusterproposal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedProposalTransfersExactAllocationOnce(t *testing.T) {
	allocation := make([]byte, PrefixBytes+64<<10)
	payload := allocation[PrefixBytes:]
	for i := range payload {
		payload[i] = byte(i)
	}
	want := bytes.Clone(payload)
	owned, err := Adopt(allocation)
	require.NoError(t, err)
	require.Same(t, &payload[0], &owned.Command()[0])
	submission, err := owned.Begin()
	require.NoError(t, err)
	_, err = owned.Begin()
	require.ErrorIs(t, err, ErrConsumed)
	require.Nil(t, owned.Command())
	key := bytes.Repeat([]byte{0xa3}, PrefixBytes)
	got, err := submission.Take(key)
	require.NoError(t, err)
	require.Same(t, &allocation[0], &got[0], "admission must transfer rather than copy the final allocation")
	require.Equal(t, key, got[:PrefixBytes])
	require.Equal(t, want, got[PrefixBytes:])
	require.False(t, submission.Available())
	_, err = submission.Take(key)
	require.ErrorIs(t, err, ErrConsumed)
	submission.Discard()
}

func TestOwnedProposalRejectsInvalidAllocationAndDropsUnsubmittedStorage(t *testing.T) {
	_, err := Adopt(make([]byte, PrefixBytes))
	require.Error(t, err)
	_, err = Adopt(make([]byte, PrefixBytes+1, PrefixBytes+2))
	require.Error(t, err)
	owned, err := Adopt(make([]byte, PrefixBytes+1))
	require.NoError(t, err)
	submission, err := owned.Begin()
	require.NoError(t, err)
	_, err = submission.Take([]byte("short"))
	require.Error(t, err)
	require.True(t, submission.Available(), "invalid key is not an admitted proposal")
	submission.Discard()
	require.Nil(t, submission.Command())
	require.False(t, submission.Available())
}
