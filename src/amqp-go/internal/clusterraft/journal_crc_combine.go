package clusterraft

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
)

// A few hundred scalar GF(2) operations beat rescanning a large entry, but
// smaller metadata entries keep the ordinary hardware CRC path.
const journalFrameCRCReuseMinBytes = 4 << 10

// The reflected Castagnoli polynomial matches hash/crc32.Castagnoli. Powers
// are initialized only when a large staged entry can actually reuse its CRC.
// powers[n] appends 2^n zero bytes to a finalized CRC; 32 powers cover uint32
// entry lengths, and occupy 4KiB independently of the frame/entry count.
var journalCRC32CPowers struct {
	once   sync.Once
	matrix [32][32]uint32
}

func journalCRCMatrixTimes(matrix *[32]uint32, vector uint32) uint32 {
	var product uint32
	for bit := 0; vector != 0; bit++ {
		if vector&1 != 0 {
			product ^= matrix[bit]
		}
		vector >>= 1
	}
	return product
}

func journalCRCMatrixSquare(square, matrix *[32]uint32) {
	for bit := range square {
		square[bit] = journalCRCMatrixTimes(matrix, matrix[bit])
	}
}

// journalCombineCRC32C returns CRC32C(left bytes || right bytes), given their
// standard finalized CRCs and the exact right length. No original bytes are
// omitted from the checksum: this applies the CRC's linear zero-byte operator
// to the left CRC and XORs in the independently computed right CRC.
func journalCombineCRC32C(left, right, rightBytes uint32) uint32 {
	if rightBytes == 0 {
		return left
	}
	journalCRC32CPowers.once.Do(func() {
		var oneBit, twoBits, fourBits [32]uint32
		oneBit[0] = crc32.Castagnoli
		for bit := 1; bit < len(oneBit); bit++ {
			oneBit[bit] = 1 << (bit - 1)
		}
		journalCRCMatrixSquare(&twoBits, &oneBit)
		journalCRCMatrixSquare(&fourBits, &twoBits)
		journalCRCMatrixSquare(&journalCRC32CPowers.matrix[0], &fourBits)
		for power := 1; power < len(journalCRC32CPowers.matrix); power++ {
			journalCRCMatrixSquare(&journalCRC32CPowers.matrix[power], &journalCRC32CPowers.matrix[power-1])
		}
	})
	for power := 0; rightBytes != 0; power++ {
		if rightBytes&1 != 0 {
			left = journalCRCMatrixTimes(&journalCRC32CPowers.matrix[power], left)
		}
		rightBytes >>= 1
	}
	return left ^ right
}

// validJournalEntryChecksums requires a complete, contiguous group-op layout,
// not arbitrary ranges. In particular promoted absolute offsets, omitted or
// reordered entries, forged lengths, and mismatching stored CRC fields fall
// back to hashing the complete raw body. This does not parse entry protobufs:
// op.encode already serialized them and computed these CRCs before enqueue.
func validJournalEntryChecksums(record journalRecord) bool {
	body, entries := record.body, record.entryChecksums
	if len(body) < journalOpHeader+journalEntryHeader+journalFrameCRCReuseMinBytes ||
		len(entries) == 0 || len(entries) > maxJournalOpEntries || body[0] != journalGroupVersion ||
		uint64(binary.BigEndian.Uint32(body[26:journalOpHeader])) != uint64(len(entries)) {
		return false
	}
	fixedEnd := uint64(journalOpHeader)
	for _, at := range [...]int{2, 6, 10, 14} {
		fixedEnd += uint64(binary.BigEndian.Uint32(body[at : at+4]))
	}
	if fixedEnd > uint64(len(body)) {
		return false
	}
	at := int(fixedEnd)
	for _, entry := range entries {
		if len(body)-at < journalEntryHeader {
			return false
		}
		start := at + journalEntryHeader
		if entry.offset != int64(start) || entry.size == 0 || uint64(entry.size) > uint64(len(body)-start) ||
			binary.BigEndian.Uint32(body[at:at+4]) != entry.size ||
			binary.BigEndian.Uint32(body[at+4:start]) != entry.checksum {
			return false
		}
		at = start + int(entry.size)
	}
	return at == len(body)
}

func journalFramePayloadChecksum(frame []byte, records []journalRecord) uint32 {
	var checksum uint32
	rawStart, recordStart := journalFrameHeaderSize, journalFrameHeaderSize
	for _, record := range records {
		bodyStart := recordStart + journalRecordHeaderSize
		if record.prepared != nil {
			checksum = crc32.Update(checksum, journalChecksum, frame[rawStart:bodyStart])
			checksum = journalCombineCRC32C(checksum, record.prepared.checksum, uint32(record.prepared.size))
			rawStart = bodyStart + record.prepared.size
			recordStart = rawStart
			continue
		}
		if validJournalEntryChecksums(record) {
			for _, entry := range record.entryChecksums {
				if entry.size < journalFrameCRCReuseMinBytes {
					continue
				}
				start := bodyStart + int(entry.offset)
				checksum = crc32.Update(checksum, journalChecksum, frame[rawStart:start])
				checksum = journalCombineCRC32C(checksum, entry.checksum, entry.size)
				rawStart = start + int(entry.size)
			}
		}
		recordStart = bodyStart + len(record.body)
	}
	return crc32.Update(checksum, journalChecksum, frame[rawStart:])
}
