package server

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
)

type clusterCRCResolver interface {
	AcceptsImmutableJournalCommands() bool
	JournalBodyCRCRanges([]byte) []clusterproposal.BodyRange
}

// This is only a fixture source. The production adapter computes the sums in
// its Group worker and validates exact Raft allocation/range identity.
type clusterCRCProofSource struct {
	*bytes.Reader
	command []byte
	spans   []clusterproposal.BodyChecksum
	hits    int
}

func (s *clusterCRCProofSource) ColdJournalSource() io.ReaderAt { return s.Reader }
func (s *clusterCRCProofSource) JournalBodyCRC32C(offset int64, body []byte) (uint32, bool) {
	if offset < 0 || len(body) == 0 || offset >= int64(len(s.command)) || len(body) > len(s.command)-int(offset) || &body[0] != &s.command[int(offset)] {
		return 0, false
	}
	for _, span := range s.spans {
		if int64(span.Offset) == offset && int(span.Length) == len(body) {
			s.hits++
			return span.CRC32C, true
		}
	}
	return 0, false
}

func TestClusterMachineForwardsChecksumResolverAndUsesItOnPublish(t *testing.T) {
	for _, assignment := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "assignment"}[assignment], func(t *testing.T) {
			cache := clusterqueue.NewHotPayloadCache()
			machine := NewClusterHotPayloadMachine(cache)
			if assignment {
				machine = NewClusterHotPayloadAssignmentMachine(cache)
			}
			defer machine.ReleaseJournalSources()
			resolver, ok := any(machine).(clusterCRCResolver)
			require.True(t, ok, "actual server wrapper must satisfy journal store's combined interface")
			require.True(t, resolver.AcceptsImmutableJournalCommands())
			declare, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "crc-wrapper", QueueSpec: clusterqueue.QueueSpec{Durable: true}})
			require.NoError(t, err)
			declared, err := clusterqueue.DecodeResponse(machine.Apply(declare))
			require.NoError(t, err)
			require.NoError(t, declared.Err())
			first, second := bytes.Repeat([]byte{0x91}, 65536), bytes.Repeat([]byte{0xc3}, 65537)
			single, err := clusterqueue.EncodePublishCommand("crc-wrapper", declared.Queue.Incarnation, clusterqueue.PublishRecord{Prefix: []byte("prefix"), Body: first})
			require.NoError(t, err)
			batch, err := clusterqueue.EncodePublishBatchRecords("crc-wrapper", declared.Queue.Incarnation, []clusterqueue.PublishRecord{{Body: first}, {Body: second}})
			require.NoError(t, err)
			for _, wire := range [][]byte{single, batch} {
				ranges := resolver.JournalBodyCRCRanges(wire)
				require.NotEmpty(t, ranges)
				want := machine.queues.JournalBodyCRCRanges(wire)
				require.Equal(t, want, ranges, "wrapper must preserve canonical offsets and sizes")
				source := &clusterCRCProofSource{Reader: bytes.NewReader(bytes.Clone(wire)), command: wire}
				table := crc32.MakeTable(crc32.Castagnoli)
				for _, span := range ranges {
					body := wire[int(span.Offset):int(span.Offset+span.Length)]
					source.spans = append(source.spans, clusterproposal.BodyChecksum{BodyRange: span, CRC32C: crc32.Checksum(body, table)})
				}
				inline, encode, err := machine.ApplyJournaledReply(wire, true, source)
				require.NoError(t, err)
				if encode != nil {
					inline, err = encode()
					require.NoError(t, err)
				}
				receipt, err := clusterqueue.DecodeResponse(inline)
				require.NoError(t, err)
				require.NoError(t, receipt.Err())
				require.Equal(t, len(ranges), source.hits, "real ClusterMachine -> queues apply must consume each exact body proof")
			}
			tiny, err := clusterqueue.EncodePublishCommand("crc-wrapper", declared.Queue.Incarnation, clusterqueue.PublishRecord{Body: make([]byte, 1024)})
			require.NoError(t, err)
			require.Nil(t, resolver.JournalBodyCRCRanges(tiny))
			require.Nil(t, resolver.JournalBodyCRCRanges([]byte{exchangePrefix}))
			// The cold source retained by each message must remain independently usable.
			_, err = machine.Snapshot()
			require.NoError(t, err)
		})
	}
}
