package protocol

import (
	"testing"
	"unsafe"
)

// TestMessage_FitsThe288ByteSizeClass pins protocol.Message under Go's 288 B
// allocation class.
//
// Every message resident in the hot cache holds one *Message for its whole
// life, so the struct's size class is paid once per backlogged message: at the
// fsync:off depth high-water mark (~52k messages, ~43.5k of them hot at 1 KiB)
// the 320 B class costs ~1.4 MB more live heap than the 288 B class, doubled
// by the GC goal. The four one-byte fields (Redelivered, DeliveryMode,
// Priority, Mandatory) used to sit between pointer-aligned fields and padded
// three of them out to a full word each (296 B -> the 320 B class); packed
// together they share one word (280 B -> 288 B).
func TestMessage_FitsThe288ByteSizeClass(t *testing.T) {
	if got := unsafe.Sizeof(Message{}); got > 288 {
		t.Fatalf("unsafe.Sizeof(protocol.Message) = %d B, want <= 288 (the next Go size class "+
			"is 320 B): keep the one-byte fields packed together at the end", got)
	}
}
