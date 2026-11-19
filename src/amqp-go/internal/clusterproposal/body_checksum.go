package clusterproposal

// These scalar descriptions never carry or own command/payload backing bytes.
// Offsets are relative to the queue command, after its proposal-key prefix.
type BodyRange struct{ Offset, Length uint32 }
type BodyChecksum struct {
	BodyRange
	CRC32C uint32
}

const MaxBodyChecksumRanges = 64
const MinBodyChecksumBytes = 32 << 10
const BodyChecksumBytes = 12 // Three uint32 fields, including embedded range.
