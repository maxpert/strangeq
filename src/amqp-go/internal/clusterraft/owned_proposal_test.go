package clusterraft

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
)

func TestOwnedProposalPreservesAdmissionOrderAndActivationFallback(t *testing.T) {
	node, err := OpenNodeJournalWithOptions(t.TempDir(), true, JournalOptions{ProcessDurability: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })
	machine := &testMachine{}
	group, err := node.OpenGroup(Config{ID: 1, GroupID: 11, Dir: filepath.Join(t.TempDir(), "group"), Peers: []uint64{1}, Machine: machine, TickInterval: -1, ProcessDurability: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = group.Close() })
	makeSubmission := func(body string) *clusterproposal.Submission {
		allocation := make([]byte, clusterproposal.PrefixBytes+len(body))
		copy(allocation[clusterproposal.PrefixBytes:], body)
		owned, err := clusterproposal.Adopt(allocation)
		require.NoError(t, err)
		submission, err := owned.Begin()
		require.NoError(t, err)
		return submission
	}
	submission := makeSubmission("owned")
	_, err = group.ProposeOwned(t.Context(), submission)
	require.ErrorIs(t, err, ErrNotLeader)
	require.True(t, submission.Available(), "leadership rejection precedes ownership transfer")
	maxBytes := group.cfg.MaxEntryBytes
	group.cfg.MaxEntryBytes = uint64(len(key{})) + 1
	_, err = group.ProposeOwned(t.Context(), submission)
	require.ErrorIs(t, err, ErrTooLarge)
	require.True(t, submission.Available(), "size rejection precedes ownership transfer")
	group.cfg.MaxEntryBytes = maxBytes
	require.NoError(t, group.Campaign(t.Context()))
	_, err = group.Propose(t.Context(), []byte("standard-before"))
	require.NoError(t, err)
	reply, err := group.ProposeOwned(t.Context(), submission)
	require.NoError(t, err)
	require.Equal(t, []byte("applied owned"), reply)
	require.False(t, submission.Available(), "process admission must consume the final encoder allocation")
	_, err = group.ProposeOwned(t.Context(), submission)
	require.ErrorIs(t, err, clusterproposal.ErrConsumed)
	require.NoError(t, node.RequireFullDurability())
	strict := makeSubmission("after-activation")
	_, err = group.ProposeOwned(t.Context(), strict)
	require.NoError(t, err)
	require.False(t, strict.Available(), "activation fallback still consumes the one-shot submission")
	strict.Discard()
	require.Equal(t, []string{"standard-before", "owned", "after-activation"}, machine.entries)
}

var proposalAssemblySink []byte

// This benchmark isolates final encoding plus the actual pre-Raft command
// assembly paths. It excludes the unchanged Raft clone, journal and delivery.
func BenchmarkOwnedPublishProposalAssembly(b *testing.B) {
	for _, count := range []int{1, 8} {
		name := "single64KiB"
		if count == 8 {
			name = "batch8x64KiB"
		}
		records := make([]clusterqueue.PublishRecord, count)
		for i := range records {
			records[i].Body = bytes.Repeat([]byte{byte(i + 1)}, 64<<10)
		}
		b.Run(name+"/standard", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(count * 64 << 10))
			for range b.N {
				var wire []byte
				var err error
				if count == 1 {
					wire, err = clusterqueue.EncodePublishCommand("jobs", 7, records[0])
				} else {
					wire, err = clusterqueue.EncodePublishBatchRecords("jobs", 7, records)
				}
				if err != nil {
					b.Fatal(err)
				}
				proposalAssemblySink = newProposalCommand(key{}, wire)
			}
		})
		b.Run(name+"/owned", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(count * 64 << 10))
			for range b.N {
				var owned *clusterproposal.Owned
				var err error
				if count == 1 {
					owned, err = clusterqueue.EncodeOwnedPublishCommand("jobs", 7, records[0])
				} else {
					owned, err = clusterqueue.EncodeOwnedPublishBatchRecords("jobs", 7, records)
				}
				if err != nil {
					b.Fatal(err)
				}
				submission, err := owned.Begin()
				if err != nil {
					b.Fatal(err)
				}
				proposalAssemblySink, err = submission.Take(make([]byte, clusterproposal.PrefixBytes))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
