package clusterqueue

import (
	"encoding/binary"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
)

// JournalBodyCRCRanges is a stateless layout resolver for already encoded
// commands. DecodeCommand is the canonical validator, including batch bounds.
// Packed envelopes and tiny bodies deliberately retain the original CRC path.
func (*Machine) JournalBodyCRCRanges(data []byte) []clusterproposal.BodyRange {
	if len(data) < clusterproposal.MinBodyChecksumBytes || data[0] == packedPublishVersion || data[0] == smallPackedPublishVersion {
		return nil
	}
	c, err := DecodeCommand(data)
	if err != nil || c.packedVersion != 0 {
		return nil
	}
	offset := len(data) - len(c.Body)
	switch c.Kind {
	case PublishMessage:
		if len(c.Body) < clusterproposal.MinBodyChecksumBytes {
			return nil
		}
		return []clusterproposal.BodyRange{{Offset: uint32(offset), Length: uint32(len(c.Body))}}
	case PublishBatch:
		// DecodeCommand already used validPublishBatchCommand; use the same frame
		// length constant and slices as publishBatch, with no payload decoding/copy.
		var ranges []clusterproposal.BodyRange
		for body := c.Body; len(body) > 0; {
			size := int(binary.BigEndian.Uint32(body))
			end := publishBatchLengthBytes + size
			if size >= clusterproposal.MinBodyChecksumBytes {
				ranges = append(ranges, clusterproposal.BodyRange{Offset: uint32(offset + publishBatchLengthBytes), Length: uint32(size)})
			}
			offset += end
			body = body[end:]
		}
		return ranges
	default:
		return nil
	}
}
