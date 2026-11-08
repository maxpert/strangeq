package storage

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// TestPurgeQueueRemovesOnlyTheRequestedTagBand is T1 from spec-b4: PurgeQueue
// must be band-scoped, not a whole-ring wipe. A message whose composite
// delivery tag falls outside the requested [minTag, maxTag] band (as if it
// belonged to a different queue incarnation's ordinal — see
// broker/tag_packing.go) must survive a purge of another band untouched.
//
// Messages are stored transient (DeliveryMode 1) so GetMessage's WAL/segment
// fallback tiers cannot mask a ring-only purge: a transient message is never
// written to the WAL unless the ring spills, which a handful of small
// messages never does.
func TestPurgeQueueRemovesOnlyTheRequestedTagBand(t *testing.T) {
	ds, err := NewStorageEngineWithDataDir(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorageEngineWithDataDir: %v", err)
	}
	t.Cleanup(func() { _ = ds.Close() })

	const queueName = "q"
	if err := ds.StoreQueue(&protocol.Queue{Name: queueName, Durable: false}); err != nil {
		t.Fatalf("StoreQueue: %v", err)
	}

	// Mirrors broker/tag_packing.go's PackTag(ordinal, seq) = (ordinal << 44) | seq.
	// storage cannot import broker (broker imports storage), so the packing
	// scheme is replicated here rather than referenced.
	const ordinalShift = 44
	band1Base := uint64(1) << ordinalShift
	band2Base := uint64(2) << ordinalShift

	inBandTag1 := band1Base + 1
	inBandTag2 := band1Base + 2
	// AtomicRing addresses its slot purely from the LOW bits of the tag
	// (slot = tag & mask, mask sized off the ring's fixed slot count — see
	// storage/atomic_ring.go); the ordinal lives entirely above the mask, so a
	// low-seq collision with an in-band tag would alias to the same ring slot
	// and get pushed to the WAL-spill path, masking exactly the ring-purge
	// behavior this test exists to check. seq=3 keeps this message's slot
	// distinct from both in-band messages' slots (seq 1, 2).
	outOfBandTag := band2Base + 3

	for _, tag := range []uint64{inBandTag1, inBandTag2, outOfBandTag} {
		msg := &protocol.Message{
			Body:         []byte("payload"),
			DeliveryTag:  tag,
			DeliveryMode: 1, // transient: never touches the WAL absent a ring spill
		}
		if err := ds.StoreMessage(queueName, msg); err != nil {
			t.Fatalf("StoreMessage(tag=%d): %v", tag, err)
		}
	}

	lo, hi := band1Base, band1Base|((uint64(1)<<ordinalShift)-1)
	count, err := ds.PurgeQueue(queueName, lo, hi)
	if err != nil {
		t.Fatalf("PurgeQueue: unexpected error: %v", err)
	}
	// Non-fatal: the out-of-band survivor check below is the assertion this
	// test exists to isolate (a whole-ring-wipe mutant fails on it), so it
	// must still run even if the count is also wrong.
	if count != 2 {
		t.Errorf("PurgeQueue returned count=%d, want 2 (only the in-band messages)", count)
	}

	if _, err := ds.GetMessage(queueName, outOfBandTag); err != nil {
		t.Errorf("GetMessage(outOfBandTag=%d) = %v, want the message to survive a purge of a different band", outOfBandTag, err)
	}

	if _, err := ds.GetMessage(queueName, inBandTag1); err == nil {
		t.Errorf("GetMessage(inBandTag1=%d) succeeded, want it purged (interfaces.ErrMessageNotFound)", inBandTag1)
	} else if err != interfaces.ErrMessageNotFound {
		t.Errorf("GetMessage(inBandTag1=%d) = %v, want interfaces.ErrMessageNotFound", inBandTag1, err)
	}
}
