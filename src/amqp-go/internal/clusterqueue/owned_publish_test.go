package clusterqueue

import (
	"bytes"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/stretchr/testify/require"
)

func TestOwnedPublishEncodingPreservesExactCommandWireAndValidation(t *testing.T) {
	records := []PublishRecord{
		{Prefix: []byte("header-one"), Body: bytes.Repeat([]byte{0x37}, 64<<10)},
		{Prefix: []byte("header-two"), Body: bytes.Repeat([]byte{0xe8}, 64<<10)},
	}
	plain, err := EncodePublishCommand("jobs", 7, records[0])
	require.NoError(t, err)
	owned, err := EncodeOwnedPublishCommand("jobs", 7, records[0])
	require.NoError(t, err)
	require.Equal(t, plain, owned.Command())
	require.Equal(t, len(plain), cap(owned.Command()))
	batch, err := EncodePublishBatchRecords("jobs", 7, records)
	require.NoError(t, err)
	ownedBatch, err := EncodeOwnedPublishBatchRecords("jobs", 7, records)
	require.NoError(t, err)
	require.Equal(t, batch, ownedBatch.Command())
	submission, err := ownedBatch.Begin()
	require.NoError(t, err)
	complete, err := submission.Take(bytes.Repeat([]byte{0x9a}, clusterproposal.PrefixBytes))
	require.NoError(t, err)
	require.Equal(t, batch, complete[clusterproposal.PrefixBytes:])
	decoded, err := DecodeCommand(complete[clusterproposal.PrefixBytes:])
	require.NoError(t, err)
	require.Equal(t, PublishBatch, decoded.Kind)
	_, plainErr := EncodePublishBatchRecords("jobs", 7, records[:1])
	_, ownedErr := EncodeOwnedPublishBatchRecords("jobs", 7, records[:1])
	require.ErrorIs(t, ownedErr, plainErr)
	_, plainErr = EncodePublishCommand("", 7, records[0])
	_, ownedErr = EncodeOwnedPublishCommand("", 7, records[0])
	require.ErrorIs(t, ownedErr, plainErr)
	oversized := []PublishRecord{{Body: make([]byte, MaxPublishBatchBytes)}, {Body: []byte{1}}}
	_, plainErr = EncodePublishBatchRecords("jobs", 7, oversized)
	_, ownedErr = EncodeOwnedPublishBatchRecords("jobs", 7, oversized)
	require.ErrorIs(t, ownedErr, plainErr)
}
