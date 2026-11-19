package clusterraft

import (
	"hash/crc32"
	"io"
	"sync"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// One actual-byte scan per large message body. Every intervening command byte
// is still checked; composition produces the identical full Entry CRC32C.
func prepareJournalBodyChecksums(checksum uint32, data []byte, resolve func([]byte) []clusterproposal.BodyRange) (uint32, []clusterproposal.BodyChecksum) {
	if resolve == nil || len(data) < clusterproposal.PrefixBytes+clusterproposal.MinBodyChecksumBytes {
		return crc32.Update(checksum, journalChecksum, data), nil
	}
	ranges := resolve(data[clusterproposal.PrefixBytes:])
	if !validJournalBodyRanges(len(data)-clusterproposal.PrefixBytes, ranges) {
		return crc32.Update(checksum, journalChecksum, data), nil
	}
	checksums := make([]clusterproposal.BodyChecksum, len(ranges))
	at := 0
	for i, span := range ranges {
		start := clusterproposal.PrefixBytes + int(span.Offset)
		end := start + int(span.Length)
		checksum = crc32.Update(checksum, journalChecksum, data[at:start])
		sum := crc32.Checksum(data[start:end], journalChecksum)
		checksums[i] = clusterproposal.BodyChecksum{BodyRange: span, CRC32C: sum}
		checksum = journalCombineCRC32C(checksum, sum, span.Length)
		at = end
	}
	return crc32.Update(checksum, journalChecksum, data[at:]), checksums
}

func validJournalBodyRanges(commandSize int, ranges []clusterproposal.BodyRange) bool {
	if len(ranges) == 0 || len(ranges) > clusterproposal.MaxBodyChecksumRanges {
		return false
	}
	var end uint64
	for _, span := range ranges {
		start, size := uint64(span.Offset), uint64(span.Length)
		if size < clusterproposal.MinBodyChecksumBytes || start < end || start > uint64(commandSize) || size > uint64(commandSize)-start {
			return false
		}
		end = start + size
	}
	return true
}

// This temporary wrapper is used only during one apply. Cold queue state strips
// it immediately. The defer clears any remaining Data/scalar aliases on every
// success, rejection, error and panic path; the physical reader remains valid.
type journalBodyChecksumSource struct {
	source    io.ReaderAt
	mu        sync.Mutex
	command   []byte
	checksums []clusterproposal.BodyChecksum
}

func (s *journalBodyChecksumSource) ReadAt(p []byte, offset int64) (int, error) {
	return s.source.ReadAt(p, offset)
}
func (s *journalBodyChecksumSource) Retain() (io.ReaderAt, func(), error) {
	if source, ok := s.source.(interface {
		Retain() (io.ReaderAt, func(), error)
	}); ok {
		return source.Retain()
	}
	return s.source, func() {}, nil
}
func (s *journalBodyChecksumSource) ImmutableJournalCommandBacking() (int, int) {
	if source, ok := s.source.(interface{ ImmutableJournalCommandBacking() (int, int) }); ok {
		return source.ImmutableJournalCommandBacking()
	}
	return 0, 0
}
func (s *journalBodyChecksumSource) ColdJournalSource() io.ReaderAt { return s.source }
func (s *journalBodyChecksumSource) JournalBodyCRC32C(offset int64, body []byte) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < 0 || len(body) == 0 || offset >= int64(len(s.command)) || len(body) > len(s.command)-int(offset) || &body[0] != &s.command[int(offset)] {
		return 0, false
	}
	for _, span := range s.checksums {
		if uint64(span.Offset) == uint64(offset) && uint64(span.Length) == uint64(len(body)) {
			return span.CRC32C, true
		}
	}
	return 0, false
}
func (s *journalBodyChecksumSource) clear() {
	s.mu.Lock()
	s.command = nil
	s.checksums = nil
	s.mu.Unlock()
}

type assignmentBodyChecksumSource struct {
	*journalBodyChecksumSource
	index, term uint64
	size        int64
}

func (s *assignmentBodyChecksumSource) AssignmentJournalEntry() (uint64, uint64, int64) {
	return s.index, s.term, s.size
}

func (g *Group) preparedBodyChecksumSource(source io.ReaderAt, entry *pb.Entry) (io.ReaderAt, func()) {
	if !g.cfg.ProcessDurability || g.store.bodyCRCRanges == nil || !g.store.node.writer.preparedProcessAllowed() {
		return source, func() {}
	}
	status := g.node.BasicStatus()
	conf := g.conf
	if status.RaftState != raft.StateLeader || status.Lead != g.cfg.ID || len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != g.cfg.ID || len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearners()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return source, func() {}
	}
	base, index := g.store.state.snapshot.metadata.GetIndex(), entry.GetIndex()
	if index <= base || index-base > uint64(len(g.store.state.entries)) {
		return source, func() {}
	}
	ref := g.store.state.entries[index-base-1]
	checksums := g.store.node.recentEntries.takeBodyChecksums(ref, entry)
	if len(checksums) == 0 {
		return source, func() {}
	}
	command := entry.GetData()[clusterproposal.PrefixBytes:]
	proof := &journalBodyChecksumSource{source: source, command: command, checksums: checksums}
	if assignment, ok := source.(interface {
		AssignmentJournalEntry() (uint64, uint64, int64)
	}); ok {
		index, term, size := assignment.AssignmentJournalEntry()
		return &assignmentBodyChecksumSource{journalBodyChecksumSource: proof, index: index, term: term, size: size}, proof.clear
	}
	return proof, proof.clear
}
