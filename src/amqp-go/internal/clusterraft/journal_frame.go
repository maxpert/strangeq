package clusterraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

const (
	journalFrameHeaderSize  = 24
	journalRecordHeaderSize = 12
	journalFrameTrailerSize = 16
	maxJournalFrameBytes    = 128 << 20
	maxJournalFrameRecords  = 4096
)

var (
	errJournalCorrupt  = errors.New("corrupt raft journal")
	errJournalTooLarge = errors.New("raft journal record or batch too large")
	journalChecksum    = crc32.MakeTable(crc32.Castagnoli)
)

type journalLocation struct {
	offset   int64
	size     int
	sequence uint64
}

type journalRecord struct {
	group    uint64
	body     []byte
	location journalLocation
}

type journalScan struct {
	end          int64
	nextSequence uint64
	partialTail  bool
}

// Header: version magic, sequence, payload size, record count, header CRC.
// Payload: repeated group ID, body size, body. Trailer: payload CRC, magic,
// sequence. Locations point to record bodies relative to the frame start.
func encodeJournalFrame(frame []byte, sequence uint64, records []journalRecord) ([]byte, []journalLocation, error) {
	if sequence == 0 || sequence == math.MaxUint64 || len(records) == 0 {
		return nil, nil, errors.New("invalid raft journal batch")
	}
	if len(records) > maxJournalFrameRecords {
		return nil, nil, errJournalTooLarge
	}
	var size int
	for _, record := range records {
		if record.group == 0 || len(record.body) == 0 {
			return nil, nil, errors.New("invalid raft journal record")
		}
		if len(record.body) > maxJournalFrameBytes-size-journalRecordHeaderSize {
			return nil, nil, errJournalTooLarge
		}
		size += journalRecordHeaderSize + len(record.body)
	}
	frameSize := journalFrameHeaderSize + size + journalFrameTrailerSize
	if frameSize > cap(frame) {
		frame = make([]byte, frameSize)
	} else {
		frame = frame[:frameSize]
	}
	copy(frame[:4], "SQJ1")
	binary.BigEndian.PutUint64(frame[4:12], sequence)
	binary.BigEndian.PutUint32(frame[12:16], uint32(size))
	binary.BigEndian.PutUint32(frame[16:20], uint32(len(records)))
	binary.BigEndian.PutUint32(frame[20:24], crc32.Checksum(frame[:20], journalChecksum))

	locations := make([]journalLocation, 0, len(records))
	offset := journalFrameHeaderSize
	for _, record := range records {
		binary.BigEndian.PutUint64(frame[offset:offset+8], record.group)
		binary.BigEndian.PutUint32(frame[offset+8:offset+journalRecordHeaderSize], uint32(len(record.body)))
		offset += journalRecordHeaderSize
		locations = append(locations, journalLocation{offset: int64(offset), size: len(record.body), sequence: sequence})
		offset += copy(frame[offset:], record.body)
	}
	binary.BigEndian.PutUint32(frame[offset:offset+4], crc32.Checksum(frame[journalFrameHeaderSize:offset], journalChecksum))
	copy(frame[offset+4:offset+8], "SQJE")
	binary.BigEndian.PutUint64(frame[offset+8:], sequence)
	return frame, locations, nil
}

// scanJournal validates one segment. sealedSize is -1 for the active segment,
// or the independently recorded durable extent for a sealed segment. A short
// physical tail is recoverable only in the active segment. The visitor borrows
// each complete batch until it returns and must stage replay until scan succeeds.
func scanJournal(r io.ReaderAt, actualSize, sealedSize int64, firstSequence uint64, visit func([]journalRecord) error) (journalScan, error) {
	if actualSize < 0 || sealedSize < -1 || (sealedSize >= 0 && actualSize != sealedSize) || firstSequence == 0 {
		return journalScan{}, fmt.Errorf("%w: invalid segment extent or first sequence", errJournalCorrupt)
	}
	var header [journalFrameHeaderSize]byte
	var payload []byte
	var records []journalRecord
	state := journalScan{nextSequence: firstSequence}
	for state.end < actualSize {
		remaining := actualSize - state.end
		if remaining < journalFrameHeaderSize {
			break
		}
		if err := readJournalAt(r, header[:], state.end); err != nil {
			return journalScan{}, err
		}
		sequence := binary.BigEndian.Uint64(header[4:12])
		if string(header[:4]) != "SQJ1" || sequence != state.nextSequence || sequence == math.MaxUint64 ||
			binary.BigEndian.Uint32(header[20:24]) != crc32.Checksum(header[:20], journalChecksum) {
			return journalScan{}, fmt.Errorf("%w: invalid frame header at offset %d", errJournalCorrupt, state.end)
		}
		size, count := int(binary.BigEndian.Uint32(header[12:16])), int(binary.BigEndian.Uint32(header[16:20]))
		if size <= 0 || size > maxJournalFrameBytes || count <= 0 ||
			count > maxJournalFrameRecords || count > size/(journalRecordHeaderSize+1) {
			return journalScan{}, fmt.Errorf("%w: invalid frame bounds at offset %d", errJournalCorrupt, state.end)
		}
		frameSize := int64(journalFrameHeaderSize + size + journalFrameTrailerSize)
		if remaining < frameSize {
			break
		}
		needed := size + journalFrameTrailerSize
		if cap(payload) < needed {
			payload = make([]byte, needed)
			// A larger frame can leave old slices outside the next decoded
			// record count. Drop their references to the replaced buffer.
			clear(records[:cap(records)])
		} else {
			payload = payload[:needed]
		}
		start := state.end + journalFrameHeaderSize
		if err := readJournalAt(r, payload, start); err != nil {
			return journalScan{}, err
		}
		body, trailer := payload[:size], payload[size:]
		if string(trailer[4:8]) != "SQJE" || binary.BigEndian.Uint64(trailer[8:]) != sequence ||
			binary.BigEndian.Uint32(trailer[:4]) != crc32.Checksum(body, journalChecksum) {
			return journalScan{}, fmt.Errorf("%w: invalid frame trailer or payload at offset %d", errJournalCorrupt, state.end)
		}
		var err error
		records, err = decodeJournalRecords(records[:0], body, count, start, sequence)
		if err != nil {
			return journalScan{}, err
		}
		if visit != nil {
			if err := visit(records); err != nil {
				return journalScan{}, err
			}
		}
		state.end += frameSize
		state.nextSequence++
	}
	state.partialTail = state.end != actualSize
	if state.partialTail && sealedSize != -1 {
		return journalScan{}, fmt.Errorf("%w: incomplete sealed frame at offset %d", errJournalCorrupt, state.end)
	}
	return state, nil
}

func decodeJournalRecords(records []journalRecord, body []byte, count int, start int64, sequence uint64) ([]journalRecord, error) {
	if cap(records) < count {
		records = make([]journalRecord, 0, count)
	}
	var offset int
	for range count {
		if len(body)-offset < journalRecordHeaderSize {
			return nil, fmt.Errorf("%w: short record header at offset %d", errJournalCorrupt, start+int64(offset))
		}
		group := binary.BigEndian.Uint64(body[offset : offset+8])
		size := int(binary.BigEndian.Uint32(body[offset+8 : offset+journalRecordHeaderSize]))
		offset += journalRecordHeaderSize
		if group == 0 || size <= 0 || size > len(body)-offset {
			return nil, fmt.Errorf("%w: invalid record at offset %d", errJournalCorrupt, start+int64(offset))
		}
		location := journalLocation{offset: start + int64(offset), size: size, sequence: sequence}
		records = append(records, journalRecord{group: group, body: body[offset : offset+size : offset+size], location: location})
		offset += size
	}
	if offset != len(body) {
		return nil, fmt.Errorf("%w: unexpected payload bytes at offset %d", errJournalCorrupt, start+int64(offset))
	}
	return records, nil
}

func readJournalAt(r io.ReaderAt, data []byte, offset int64) error {
	n, err := r.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read raft journal at offset %d: %w", offset, err)
	}
	if n != len(data) {
		return fmt.Errorf("%w: short read at offset %d within the stated segment extent", errJournalCorrupt, offset)
	}
	return nil
}
