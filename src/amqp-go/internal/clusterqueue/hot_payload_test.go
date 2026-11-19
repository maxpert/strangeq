package clusterqueue

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type hotPayloadSource struct {
	*bytes.Reader
	reads atomic.Int64
}

func (s *hotPayloadSource) ReadAt(dst []byte, offset int64) (int, error) {
	s.reads.Add(1)
	return s.Reader.ReadAt(dst, offset)
}

func TestHotPayloadOwnsInputAndPreservesCheckedJournalSource(t *testing.T) {
	cache := NewHotPayloadCache()
	body := bytes.Repeat([]byte{0x5a}, 64<<10)
	stored := bytes.Clone(body)
	source := &hotPayloadSource{Reader: bytes.NewReader(stored)}
	message, err := (journalBodyOrigin{source: source, hotPayloads: cache}).message(1, body)
	require.NoError(t, err)
	clear(body) // Apply's borrowed command buffer may be reused immediately.
	got, err := message.delivery(1).appendBody(make([]byte, 0, len(stored)))
	require.NoError(t, err)
	require.Equal(t, stored, got)
	require.Zero(t, source.reads.Load())
	require.Equal(t, len(stored), cache.bytes)

	// A portable snapshot still checks durable bytes even while delivery has
	// a good immutable hot copy. Eviction restores the same checked read path.
	stored[0] ^= 1
	require.ErrorContains(t, message.cold.read(make([]byte, len(stored))), "checksum mismatch")
	message.releaseJournalSource()
	require.Zero(t, cache.bytes)
	_, err = message.delivery(1).appendBody(make([]byte, 0, len(stored)))
	require.ErrorContains(t, err, "checksum mismatch")
	message.releaseJournalSource() // Retiring a cache entry is idempotent.
	require.Zero(t, cache.bytes)
}

func TestHotPayloadBudgetIsSharedAndEvictionReleasesOwnership(t *testing.T) {
	cache := NewHotPayloadCache()
	body := bytes.Repeat([]byte{0x7e}, 64<<10)
	var messages []messageState
	for i := 0; i <= maxHotPayloadBytes/len(body); i++ {
		// Distinct origins model independent shard machines using one node cap.
		origin := journalBodyOrigin{source: bytes.NewReader(body), hotPayloads: cache}
		message, err := origin.message(MessageID(i+1), body)
		require.NoError(t, err)
		messages = append(messages, message)
		require.LessOrEqual(t, cache.bytes, maxHotPayloadBytes)
	}
	require.Nil(t, messages[0].cold.hotPayload.data)
	require.Equal(t, maxHotPayloadBytes, cache.bytes)
	live, count := 0, 0
	for entry := cache.first; entry != nil; entry = entry.next {
		live += entry.charge
		count++
	}
	require.Equal(t, cache.bytes, live, "charge includes every reachable owned payload")
	require.Equal(t, maxHotPayloadBytes/len(body), count)
	for _, message := range messages {
		message.releaseJournalSource()
	}
	require.Zero(t, cache.bytes)
	require.Nil(t, cache.first)
	require.Nil(t, cache.last)

	oversize := make([]byte, maxHotPayloadBytes+1)
	message, err := (journalBodyOrigin{source: bytes.NewReader(oversize), hotPayloads: cache}).message(1, oversize)
	require.NoError(t, err)
	require.Nil(t, message.cold.hotPayload)
	require.Zero(t, cache.bytes)
}

func TestHotPayloadCapturedEncodersSurviveAckAndClose(t *testing.T) {
	machine, session := claimBatchFixture(t, 2)
	cache := NewHotPayloadCache()
	machine.hotPayloads = cache
	hot, _ := claimBatchFixture(t, 2)
	body := bytes.Repeat([]byte{0x21}, 64<<10)
	publish, err := EncodePublishBatch(queueName, session.Incarnation, [][]byte{body, body})
	require.NoError(t, err)
	hot.Apply(publish)
	source := &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(publish))}
	_, err = machine.ApplyJournaled(publish, false, source)
	require.NoError(t, err)
	clear(publish)
	session.Sequence = 1
	claim := claimBatchRequest(t, session, 2, 1<<20)
	want := hot.Apply(claim)
	_, encode, err := machine.ApplyJournaledReply(claim, true, nil)
	require.NoError(t, err)
	require.NotNil(t, encode)
	got, err := encode()
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, source.reads.Load())

	// Multiple captures never borrow cache slices. Their concurrent encoding
	// can race with ACK/close eviction and must return the committed bytes.
	const encoders = 8
	var wg sync.WaitGroup
	errors := make(chan error, encoders)
	for i := 0; i < encoders; i++ {
		_, captured, err := machine.ApplyJournaledReply(claim, true, nil)
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := captured()
			if err == nil && !bytes.Equal(want, got) {
				err = io.ErrUnexpectedEOF
			}
			errors <- err
		}()
	}
	ack := session
	ack.Kind, ack.Sequence, ack.Attempt = AckMessage, 2, 1
	require.NoError(t, apply(t, machine, ack).Err())
	machine.ReleaseJournalSources()
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Zero(t, cache.bytes)
	require.Nil(t, cache.first)
	require.Nil(t, cache.last)
}

func TestHotPayloadAssignmentPageUsesHandoffAndChecksColdFallback(t *testing.T) {
	cache := NewHotPayloadCache()
	// This isolated fixture deliberately exercises the owned-copy path for
	// direct uncertified callers. Production constructors require provenance.
	machine := new(NewAssignmentMachine())
	machine.hotPayloads = cache
	declared := apply(t, machine, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, declared.Err())
	open := QueueCommand{
		Kind: OpenQueueSession, Queue: queueName, Incarnation: declared.Queue.Incarnation,
		Session: 1, Owner: "registered", Prefetch: 2, Consumer: true, AutoAssign: true, Deadline: 100,
	}
	require.NoError(t, apply(t, machine, open).Err())
	first, second := bytes.Repeat([]byte{0x14}, 64<<10), bytes.Repeat([]byte{0xe3}, 64<<10)
	publish, err := EncodePublishBatch(queueName, open.Incarnation, [][]byte{first, second})
	require.NoError(t, err)
	stored := bytes.Clone(publish)
	source := &hotPayloadSource{Reader: bytes.NewReader(stored)}
	_, err = machine.ApplyJournaled(publish, false, source)
	require.NoError(t, err)
	clear(publish)
	request := AssignmentRequest{
		Queue: queueName, Incarnation: open.Incarnation, Session: open.Session, Owner: open.Owner,
		Limit: 2, Bytes: MaxClaimBatchBytes,
	}
	encode, _, err := machine.CaptureAssignmentPage(request)
	require.NoError(t, err)
	page, err := encode()
	require.NoError(t, err)
	deliveries, through, err := DecodeAssignmentPage(page, request)
	require.NoError(t, err)
	require.Len(t, deliveries, 2)
	require.EqualValues(t, 2, through)
	require.Equal(t, first, deliveries[0].Body)
	require.Equal(t, second, deliveries[1].Body)
	require.Zero(t, source.reads.Load(), "assignment appendRecord must use the same delivery handoff")
	_, err = machine.Snapshot()
	require.NoError(t, err)
	require.EqualValues(t, 2, source.reads.Load(), "snapshot must read and check the journal")

	stored[bytes.Index(stored, second)] ^= 1
	_, err = machine.Snapshot()
	require.ErrorContains(t, err, "checksum mismatch")
	machine.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
	_, err = encode()
	require.ErrorContains(t, err, "checksum mismatch", "captured page must check the original cold offsets after cache eviction")
}

func TestShardedHotPayloadCacheHasOneDeterministicNodeBudget(t *testing.T) {
	caches := NewShardedHotPayloadCache()
	require.Equal(t, 0, maxNodeHotPayloadBytes%hotPayloadBuckets)
	body := bytes.Repeat([]byte{0xc5}, 64<<10)
	var messages []messageState
	var first []messageState
	for shard := 0; shard < hotPayloadBuckets; shard++ {
		cache := caches.ForShard(uint64(shard))
		require.Same(t, cache, caches.ForShard(uint64(shard+hotPayloadBuckets)))
		require.Equal(t, 4<<20, cache.limit)
		if shard > 0 {
			require.NotSame(t, cache, caches.ForShard(uint64(shard-1)))
		}
		stored := bytes.Clone(body)
		source := &hotPayloadSource{Reader: bytes.NewReader(stored)}
		for i := 0; i <= cache.limit/len(body); i++ {
			message, err := (journalBodyOrigin{source: source, hotPayloads: cache}).message(MessageID(i+1), body)
			require.NoError(t, err)
			messages = append(messages, message)
			if i == 0 {
				first = append(first, message)
			}
			live := 0
			for j := range caches.buckets {
				live += caches.buckets[j].bytes
			}
			require.LessOrEqual(t, live, maxNodeHotPayloadBytes)
		}
		require.Nil(t, first[shard].cold.hotPayload.data)
		require.Equal(t, cache.limit, cache.bytes)
		got, err := first[shard].delivery(1).appendBody(make([]byte, 0, len(body)))
		require.NoError(t, err)
		require.Equal(t, body, got)
		require.EqualValues(t, 1, source.reads.Load(), "bucket eviction uses the original checked cold source")
		stored[0] ^= 1
		_, err = first[shard].delivery(1).appendBody(make([]byte, 0, len(body)))
		require.ErrorContains(t, err, "checksum mismatch")
	}
	live := 0
	for i := range caches.buckets {
		live += caches.buckets[i].bytes
	}
	require.Equal(t, maxNodeHotPayloadBytes, live)
	for _, message := range messages {
		message.releaseJournalSource()
	}
	for i := range caches.buckets {
		require.Zero(t, caches.buckets[i].bytes)
		require.Nil(t, caches.buckets[i].first)
		require.Nil(t, caches.buckets[i].last)
	}
}

type ownedHotPayloadSource struct {
	*hotPayloadSource
	prefix, capacity int
}

func (s *ownedHotPayloadSource) ImmutableJournalCommandBacking() (int, int) {
	return s.prefix, s.capacity
}

func TestOwnedHotPayloadBatchSharesOneFullBackingCharge(t *testing.T) {
	cache := NewHotPayloadCache()
	machine, session := claimBatchFixture(t, 2)
	machine.hotPayloads = cache
	first, second := bytes.Repeat([]byte{0xa1}, 64<<10), bytes.Repeat([]byte{0x36}, 64<<10)
	publish, err := EncodePublishBatch(queueName, session.Incarnation, [][]byte{first, second})
	require.NoError(t, err)
	const prefix = 16
	// Deliberately overallocate: accounting includes the key and unused tail,
	// not merely the command's length or the sum of accepted body lengths.
	backing := make([]byte, prefix+len(publish), prefix+len(publish)+4096)
	copy(backing[prefix:], publish)
	command := backing[prefix:]
	source := &ownedHotPayloadSource{
		hotPayloadSource: &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(command))},
		prefix:           prefix, capacity: cap(backing),
	}
	_, err = machine.ApplyJournaled(command, false, source)
	require.NoError(t, err)
	ready := machine.state.Queues[queueName].Ready
	require.Len(t, ready, 2)
	a, b := ready[0].cold, ready[1].cold
	require.Same(t, a.hotPayload, b.hotPayload)
	require.Same(t, &command[0], &a.hotPayload.data[0], "certified admission must not copy body bytes")
	require.Equal(t, cap(backing), cache.bytes)
	require.Equal(t, 2, a.hotPayload.bodies)
	require.Nil(t, a.hotCommand, "retained origin must never own apply's candidate")
	require.Nil(t, b.hotCommand)
	for i, want := range [][]byte{first, second} {
		got, err := ready[i].delivery(AttemptID(i + 1)).appendBody(make([]byte, 0, len(want)))
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Zero(t, source.reads.Load())

	// One ACK cannot evict a sibling body in the same backing allocation.
	ready[0].releaseJournalSource()
	require.Equal(t, cap(backing), cache.bytes)
	require.Equal(t, 1, b.hotPayload.bodies)
	entry := b.hotPayload
	ready[1].releaseJournalSource()
	require.Zero(t, cache.bytes)
	require.Nil(t, entry.data, "captures retaining the entry cannot pin an uncharged backing")
	require.Nil(t, cache.first)
}

func TestOwnedHotPayloadEvictionDropsBackingAndUsesOriginalColdRange(t *testing.T) {
	cache := NewHotPayloadCache()
	body := bytes.Repeat([]byte{0xde}, 64<<10)
	backing := make([]byte, 16+len(body))
	copy(backing[16:], body)
	command := backing[16:]
	stored := bytes.Clone(command)
	source := &ownedHotPayloadSource{
		hotPayloadSource: &hotPayloadSource{Reader: bytes.NewReader(stored)},
		prefix:           16, capacity: cap(backing),
	}
	candidate := immutableHotCommand(command, source)
	message, err := (journalBodyOrigin{source: source, hotPayloads: cache, hotCommand: candidate}).message(1, command)
	require.NoError(t, err)
	require.Nil(t, candidate.data)
	entry := message.cold.hotPayload
	require.Equal(t, cap(backing), cache.bytes)

	// Force FIFO eviction while retaining a captured delivery and its source.
	captured := message.delivery(1)
	for i := 0; i < maxHotPayloadBytes/len(body); i++ {
		_, err := (journalBodyOrigin{source: bytes.NewReader(body), hotPayloads: cache}).message(MessageID(i+2), body)
		require.NoError(t, err)
	}
	require.Nil(t, entry.data)
	got, err := captured.appendBody(make([]byte, 0, len(body)))
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.EqualValues(t, 1, source.reads.Load())
	stored[0] ^= 1
	_, err = captured.appendBody(make([]byte, 0, len(body)))
	require.ErrorContains(t, err, "checksum mismatch")
	message.releaseJournalSource()
}

func TestOwnedHotPayloadRejectsUnrelatedEqualSizeSubspan(t *testing.T) {
	for _, sameBytes := range []bool{false, true} {
		name := "different-bytes"
		if sameBytes {
			name = "same-bytes-different-array"
		}
		t.Run(name, func(t *testing.T) {
			cache := NewHotPayloadCache()
			body := bytes.Repeat([]byte{0x31}, 64<<10)
			backing := make([]byte, 16+len(body))
			copy(backing[16:], body)
			command := backing[16:]
			source := &ownedHotPayloadSource{
				hotPayloadSource: &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(command))},
				prefix:           16, capacity: cap(backing),
			}
			borrowed := bytes.Clone(body)
			if !sameBytes {
				borrowed[0] ^= 1
			}
			candidate := immutableHotCommand(command, source)
			message, err := (journalBodyOrigin{source: source, hotPayloads: cache, hotCommand: candidate}).message(1, borrowed)
			require.NoError(t, err)
			require.Nil(t, message.cold.hotPayload)
			require.Nil(t, candidate.data)
			require.Zero(t, cache.bytes, "reject before retaining the unrelated command allocation")
			got, err := message.delivery(1).appendBody(make([]byte, 0, len(body)))
			if sameBytes {
				require.NoError(t, err)
				require.Equal(t, body, got)
			} else {
				require.ErrorContains(t, err, "checksum mismatch")
			}
			require.EqualValues(t, 1, source.reads.Load(), "unrelated bytes must use checked cold verification")
		})
	}

	t.Run("later-body-in-shared-command", func(t *testing.T) {
		cache := NewHotPayloadCache()
		body := bytes.Repeat([]byte{0x63}, 64<<10)
		backing := make([]byte, 16+2*len(body))
		copy(backing[16:], body)
		copy(backing[16+len(body):], body)
		command := backing[16:]
		source := &ownedHotPayloadSource{
			hotPayloadSource: &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(command))},
			prefix:           16, capacity: cap(backing),
		}
		candidate := immutableHotCommand(command, source)
		origin := journalBodyOrigin{source: source, hotPayloads: cache, hotCommand: candidate}
		first, err := origin.message(1, command[:len(body)])
		require.NoError(t, err)
		require.NotNil(t, first.cold.hotPayload)
		origin.offset = int64(len(body))
		second, err := origin.message(2, bytes.Clone(command[len(body):]))
		require.NoError(t, err)
		require.Nil(t, second.cold.hotPayload)
		require.Equal(t, 1, first.cold.hotPayload.bodies)
		got, err := second.delivery(2).appendBody(make([]byte, 0, len(body)))
		require.NoError(t, err)
		require.Equal(t, body, got)
		require.EqualValues(t, 1, source.reads.Load())
		first.releaseJournalSource()
		second.releaseJournalSource()
		require.Zero(t, cache.bytes)
	})
}

func TestProductionHotPayloadMachineRequiresCommandProvenance(t *testing.T) {
	for _, constructor := range []struct {
		name       string
		newMachine func(*HotPayloadCache) Machine
	}{
		{name: "ordinary", newMachine: NewHotPayloadMachine},
		{name: "assignments", newMachine: NewHotPayloadAssignmentMachine},
	} {
		for _, marker := range []string{"missing", "withdrawn"} {
			t.Run(constructor.name+"/"+marker, func(t *testing.T) {
				cache := NewHotPayloadCache()
				machine := constructor.newMachine(cache)
				declared := apply(t, &machine, QueueCommand{Kind: DeclareQueue, Queue: queueName})
				require.NoError(t, declared.Err())
				body := bytes.Repeat([]byte{0x58}, 64<<10)
				wire, err := EncodeCommand(QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: declared.Queue.Incarnation, Body: body})
				require.NoError(t, err)
				reader := &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(wire))}
				var source io.ReaderAt = reader
				if marker == "withdrawn" {
					// Assignment sources return a zero certificate after the
					// process owner crosses its peer-activation durability fence.
					source = &ownedHotPayloadSource{hotPayloadSource: reader}
				}
				_, err = machine.ApplyJournaled(wire, false, source)
				require.NoError(t, err)
				message := machine.state.Queues[queueName].Ready[0]
				require.NotNil(t, message.cold)
				require.Nil(t, message.cold.hotPayload)
				require.Zero(t, cache.bytes, "production must not create a private hot clone without provenance")
				got, err := message.delivery(1).appendBody(make([]byte, 0, len(body)))
				require.NoError(t, err)
				require.Equal(t, body, got)
				require.EqualValues(t, 1, reader.reads.Load())
			})
		}
	}
}
