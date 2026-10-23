package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChannelTakeWireTagEntriesUpTo (T6.4, WS-D §6.2): returns each entry's
// wire tag (sorted ascending) plus its ref, and removes exactly those <= upto.
// Needed to restore entries on tx.rollback (TakeWireTagsUpTo discards the wire
// tags, so nothing could be re-tracked).
func TestChannelTakeWireTagEntriesUpTo(t *testing.T) {
	conn := NewConnection(nil)
	c := NewChannel(1, conn)

	// Track 5 deliveries across two consumers plus one basic.get.
	c.TrackDelivery(5, 500, "cons-a", false)
	c.TrackDelivery(2, 200, "cons-a", false)
	c.TrackDelivery(4, 400, "cons-b", false)
	c.TrackDelivery(1, 100, "", true) // basic.get
	c.TrackDelivery(3, 300, "cons-b", false)

	wireTags, refs := c.TakeWireTagEntriesUpTo(3)
	require.Equal(t, []uint64{1, 2, 3}, wireTags, "sorted by wire tag ascending")
	require.Len(t, refs, 3)
	// Refs align with the wire tags.
	assert.Equal(t, WireDeliveryRef{MsgID: 100, ConsumerID: "", IsGet: true}, refs[0])
	assert.Equal(t, WireDeliveryRef{MsgID: 200, ConsumerID: "cons-a", IsGet: false}, refs[1])
	assert.Equal(t, WireDeliveryRef{MsgID: 300, ConsumerID: "cons-b", IsGet: false}, refs[2])

	// Exactly those <= upto were removed; 4 and 5 remain.
	_, ok := c.ResolveWireTag(1)
	assert.False(t, ok)
	_, ok = c.ResolveWireTag(2)
	assert.False(t, ok)
	_, ok = c.ResolveWireTag(3)
	assert.False(t, ok)
	r4, ok := c.ResolveWireTag(4)
	require.True(t, ok)
	assert.Equal(t, uint64(400), r4.MsgID)
	r5, ok := c.ResolveWireTag(5)
	require.True(t, ok)
	assert.Equal(t, uint64(500), r5.MsgID)

	// Restoration (the rollback path): re-track the taken entries and they
	// resolve again.
	for i, w := range wireTags {
		c.TrackDelivery(w, refs[i].MsgID, refs[i].ConsumerID, refs[i].IsGet)
	}
	for _, w := range []uint64{1, 2, 3} {
		_, ok := c.ResolveWireTag(w)
		assert.True(t, ok, "restored wire tag %d resolves", w)
	}

	// Empty result when nothing is outstanding below upto.
	c2 := NewChannel(2, conn)
	tags, refs := c2.TakeWireTagEntriesUpTo(100)
	assert.Empty(t, tags)
	assert.Empty(t, refs)
}
