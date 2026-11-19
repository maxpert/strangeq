package clusterqueue

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"hash/crc32"
	"io"
)

const minJournalBodyBytes = clusterproposal.MinBodyChecksumBytes

var journalBodyChecksum = crc32.MakeTable(crc32.Castagnoli)

type journalBodyOrigin struct {
	source      io.ReaderAt
	offset      int64
	hotPayloads *HotPayloadCache
	hotCommand  *hotCommand
}

type journalBody struct {
	journalBodyOrigin
	size             int
	checksum         uint32
	assignmentDigest *[sha256.Size]byte
	releaseSource    func()
	// Only hotPayloads.mu may access these fields. Payload slices may escape
	// through an explicit charged lease that pins their cache ownership.
	hotPayload *hotPayloadEntry
	hotStart   int
}

func (origin journalBodyOrigin) message(id MessageID, body []byte) (messageState, error) {
	if origin.source == nil || len(body) < minJournalBodyBytes {
		return messageState{ID: id, Body: bytes.Clone(body)}, nil
	}
	// Apply's candidate command may borrow its whole input allocation. Never
	// embed that candidate in retained cold state or a captured delivery.
	coldOrigin := origin
	coldOrigin.hotCommand = nil
	if source, ok := origin.source.(interface{ ColdJournalSource() io.ReaderAt }); ok {
		coldOrigin.source = source.ColdJournalSource()
	}
	var checksum uint32
	var reused bool
	if source, ok := origin.source.(interface {
		JournalBodyCRC32C(int64, []byte) (uint32, bool)
	}); ok {
		checksum, reused = source.JournalBodyCRC32C(origin.offset, body)
	}
	if !reused {
		checksum = crc32.Checksum(body, journalBodyChecksum)
	}
	cold := &journalBody{
		journalBodyOrigin: coldOrigin,
		size:              len(body),
		checksum:          checksum,
	}
	if source, ok := origin.source.(interface {
		Retain() (io.ReaderAt, func(), error)
	}); ok {
		_, release, err := source.Retain()
		if err != nil {
			return messageState{}, err
		}
		// Encoders use the immutable reader. Their separate Group pin
		// lasts through encoding even if an ACK releases this owner.
		cold.releaseSource = release
	}
	if _, ok := origin.source.(assignmentJournalEntry); ok && len(body) <= maxAssignmentReferenceBody {
		digest := sha256.Sum256(body)
		cold.assignmentDigest = &digest
	}
	// Retain only after checksum provenance and source ownership exist.
	// Certified commands share one charged backing; other callers get a copy.
	origin.hotPayloads.admit(cold, body, origin.hotCommand)
	return messageState{ID: id, cold: cold}, nil
}

func (m messageState) releaseJournalSource() {
	if m.cold != nil {
		m.cold.hotPayloads.remove(m.cold)
		if m.cold.releaseSource != nil {
			m.cold.releaseSource()
		}
	}
}

// ReleaseJournalSources runs after captured readers have been joined.
func (m *Machine) ReleaseJournalSources() {
	for _, entry := range m.state.DeadLetters {
		entry.Message.releaseJournalSource()
	}
	for _, queue := range m.state.Queues {
		for _, message := range queue.Ready {
			message.releaseJournalSource()
		}
		for _, session := range queue.Sessions {
			for _, message := range session.Flights {
				message.releaseJournalSource()
			}
		}
	}
}

func (b *journalBody) read(dst []byte) error {
	n, err := b.source.ReadAt(dst, b.offset)
	if err != nil && !(n == len(dst) && errors.Is(err, io.EOF)) {
		return fmt.Errorf("read journal queue body: %w", err)
	}
	if n != len(dst) {
		return fmt.Errorf("read journal queue body: %w", io.ErrUnexpectedEOF)
	}
	if crc32.Checksum(dst, journalBodyChecksum) != b.checksum {
		return errors.New("journal queue body checksum mismatch")
	}
	return nil
}

func (m messageState) bodySize() int {
	if m.cold != nil {
		return m.cold.size
	}
	return len(m.Body)
}

func (m messageState) logicalBodySize() int {
	return max(m.LogicalSize, m.bodySize())
}

func (m messageState) delivery(attempt AttemptID) Delivery {
	return Delivery{
		MessageID: m.ID, Attempt: attempt, Body: m.Body, Redelivered: m.Redelivered,
		Packed: m.LogicalSize != 0, cold: m.cold,
	}
}

func (d Delivery) bodySize() int {
	if d.cold != nil {
		return d.cold.size
	}
	return len(d.Body)
}

func (d Delivery) appendBody(frame []byte) ([]byte, error) {
	if d.cold == nil {
		return append(frame, d.Body...), nil
	}
	start := len(frame)
	frame = frame[:start+d.cold.size]
	if !d.cold.hotPayloads.copyBody(d.cold, frame[start:]) {
		if err := d.cold.read(frame[start:]); err != nil {
			return nil, err
		}
	}
	return frame, nil
}

// Snapshot encoding continues to see the original message representation.
// The caller serializes machine operations and clears the borrowed slices on exit.
func (s *machineState) loadJournalBodies() error {
	scratch := make([]byte, int(s.coldBytes))
	load := func(message *messageState) error {
		if message.cold == nil {
			return nil
		}
		size := message.cold.size
		if size > len(scratch) {
			return errors.New("journal queue body accounting mismatch")
		}
		body := scratch[:size:size]
		if err := message.cold.read(body); err != nil {
			return err
		}
		message.Body = body
		scratch = scratch[size:]
		return nil
	}
	for _, queue := range s.Queues {
		for i := range queue.Ready {
			if err := load(&queue.Ready[i]); err != nil {
				return err
			}
		}
		for _, session := range queue.Sessions {
			for attempt, message := range session.Flights {
				if message.cold == nil {
					continue
				}
				if err := load(&message); err != nil {
					return err
				}
				session.Flights[attempt] = message
			}
		}
	}
	for id, entry := range s.DeadLetters {
		if err := load(&entry.Message); err != nil {
			return err
		}
		s.DeadLetters[id] = entry
	}
	if len(scratch) != 0 {
		return errors.New("journal queue body accounting mismatch")
	}
	return nil
}

func (s *machineState) clearJournalBodies() {
	for id, entry := range s.DeadLetters {
		if entry.Message.cold != nil {
			entry.Message.Body = nil
			s.DeadLetters[id] = entry
		}
	}
	for _, queue := range s.Queues {
		for i := range queue.Ready {
			if queue.Ready[i].cold != nil {
				queue.Ready[i].Body = nil
			}
		}
		for _, session := range queue.Sessions {
			for attempt, message := range session.Flights {
				if message.cold != nil {
					message.Body = nil
					session.Flights[attempt] = message
				}
			}
		}
	}
}
