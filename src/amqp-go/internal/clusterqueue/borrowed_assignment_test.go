package clusterqueue

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func borrowedAssignmentFixture(t *testing.T, count int) (*Machine, *HotPayloadCache, AssignmentRequest, []byte) {
	t.Helper()
	return borrowedAssignmentFixtureSize(t, count, 64<<10)
}

func borrowedAssignmentFixtureSize(t *testing.T, count, bodyBytes int) (*Machine, *HotPayloadCache, AssignmentRequest, []byte) {
	t.Helper()
	cache := NewHotPayloadCache()
	m := new(NewHotPayloadAssignmentMachine(cache))
	declared := apply(t, m, QueueCommand{Kind: DeclareQueue, Queue: queueName})
	require.NoError(t, declared.Err())
	open := QueueCommand{Kind: OpenQueueSession, Queue: queueName, Incarnation: declared.Queue.Incarnation,
		Session: 1, Owner: "borrower", Consumer: true, AutoAssign: true, Prefetch: uint32(count), Deadline: 100}
	require.NoError(t, apply(t, m, open).Err())
	body := bytes.Repeat([]byte{0x5a}, bodyBytes)
	var wire []byte
	var err error
	if count == 1 {
		wire, err = EncodePublishCommand(queueName, open.Incarnation, PublishRecord{Body: body})
	} else {
		bodies := make([][]byte, count)
		for i := range bodies {
			bodies[i] = body
		}
		wire, err = EncodePublishBatch(queueName, open.Incarnation, bodies)
	}
	require.NoError(t, err)
	backing := make([]byte, 16+len(wire))
	copy(backing[16:], wire)
	command := backing[16:]
	source := &ownedHotPayloadSource{hotPayloadSource: &hotPayloadSource{Reader: bytes.NewReader(bytes.Clone(command))}, prefix: 16, capacity: cap(backing)}
	_, err = m.ApplyJournaled(command, false, source)
	require.NoError(t, err)
	request := AssignmentRequest{Queue: queueName, Incarnation: open.Incarnation, Session: 1, Owner: open.Owner, Limit: uint32(count), Bytes: MaxClaimBatchBytes}
	return m, cache, request, body
}

func TestBorrowedAssignmentPinsWholeBackingUntilLastBodyAfterAck(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 2)
	page, err := borrowSelectedTestPage(m, request)
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Through)
	require.Len(t, page.Items, 2)
	entry := m.state.Queues[queueName].Sessions[1].Flights[1].cold.hotPayload
	require.Same(t, &entry.data[m.state.Queues[queueName].Sessions[1].Flights[1].cold.hotStart], &page.Items[0].Body[0])
	charge := cache.bytes
	require.Equal(t, entry.charge, charge)
	require.Equal(t, 1, len(cache.pageSlots))
	for i := 1; i <= 2; i++ {
		reply := apply(t, m, QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: request.Incarnation, Session: 1, Owner: request.Owner, Sequence: uint64(i), Attempt: AttemptID(i)})
		require.NoError(t, reply.Err())
	}
	require.False(t, entry.linked)
	require.Equal(t, charge, cache.bytes, "ACK must not release charge while a writer owns any body")
	require.Equal(t, body, page.Items[1].Body)
	lease := page.Items[0].BodyLease
	page.Items[0].ReleaseBody()
	lease.Release() // Idempotence is independent of the owning wrapper.
	require.Equal(t, charge, cache.bytes)
	require.Equal(t, 1, len(cache.pageSlots))
	page.Release()
	require.Zero(t, cache.bytes)
	require.Nil(t, entry.data)
	require.Zero(t, len(cache.pageSlots))
	page.Release()
	require.Zero(t, cache.bytes)
}

func TestBorrowedAssignmentSaturatedPinsNeverWaitOrInflateBudget(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 2)
	cache.limit = cache.bytes // Entire bucket consists of the one command.
	page, err := borrowSelectedTestPage(m, request)
	require.NoError(t, err)
	entry := m.state.Queues[queueName].Sessions[1].Flights[1].cold.hotPayload
	charge := cache.bytes
	cold, err := (journalBodyOrigin{source: bytes.NewReader(body), hotPayloads: cache}).message(99, body)
	require.NoError(t, err)
	require.Nil(t, cold.cold.hotPayload, "no free bytes means cold admission, with no copy or wait")
	require.False(t, entry.linked)
	require.NotNil(t, entry.data)
	require.Equal(t, charge, cache.bytes)
	require.Nil(t, cache.first)
	got, err := cold.delivery(1).appendBody(make([]byte, 0, len(body)))
	require.NoError(t, err)
	require.Equal(t, body, got)
	m.ReleaseJournalSources() // Close drops cache owners; loans remain valid.
	require.Equal(t, charge, cache.bytes)
	require.Equal(t, body, page.Items[0].Body)
	page.Release()
	require.Zero(t, cache.bytes)
	require.Nil(t, entry.data)
}

func TestBorrowedAssignmentPageSlotsMixedGateAndAuthorization(t *testing.T) {
	m, cache, request, _ := borrowedAssignmentFixture(t, 2)
	var pages []*BorrowedAssignmentPage
	for range maxHotAssignmentPages {
		page, err := borrowSelectedTestPage(m, request)
		require.NoError(t, err)
		pages = append(pages, page)
	}
	_, err := borrowSelectedTestPage(m, request)
	require.ErrorIs(t, err, errBorrowedAssignmentUnavailable)
	require.Equal(t, maxHotAssignmentPages, len(cache.pageSlots))
	for _, page := range pages {
		page.Release()
	}
	require.Zero(t, len(cache.pageSlots))
	bad := request
	bad.Owner = "other"
	_, err = borrowSelectedTestPage(m, bad)
	require.ErrorIs(t, err, ErrSessionOwner)
	require.Zero(t, len(cache.pageSlots))
	second := m.state.Queues[queueName].Sessions[1].Flights[2]
	cache.remove(second.cold)
	_, err = borrowSelectedTestPage(m, request)
	require.ErrorIs(t, err, errBorrowedAssignmentUnavailable)
	require.Zero(t, len(cache.pageSlots), "a mixed page must return the slot without partial loans")
	first := m.state.Queues[queueName].Sessions[1].Flights[1]
	require.Zero(t, first.cold.hotPayload.loans)
	m.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
}

// Strict loan-only helper retains the cache lifetime fixtures without
// exposing a second production read/authorization API.
func borrowSelectedTestPage(m *Machine, request AssignmentRequest) (*BorrowedAssignmentPage, error) {
	command := QueueCommand{Queue: request.Queue, Incarnation: request.Incarnation, Session: request.Session, Owner: request.Owner, ClaimLimit: request.Limit, ClaimBytes: request.Bytes}
	items, through, err := m.ReadAssignments(command, request.After)
	if err != nil {
		return nil, err
	}
	return m.borrowAssignmentItems(items, through)
}

func TestSelectedAssignmentSmallEmptyAndPackedOwnTheirCopies(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixtureSize(t, 1, 1024)
	encode, size, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Equal(t, len(body), size)
	page, err := encode()
	require.NoError(t, err)
	require.Equal(t, body, page.Items[0].Body)
	require.Nil(t, page.Items[0].BodyLease)
	page.Items[0].Body[0] ^= 1
	require.Equal(t, body, m.state.Queues[queueName].Sessions[1].Flights[1].Body, "typed copied replies must not expose mutable queue storage")
	require.Zero(t, len(cache.pageSlots))
	empty := request
	empty.After = 1
	encode, size, err = m.CaptureLocalAssignmentPage(empty, true)
	require.NoError(t, err)
	require.Zero(t, size)
	page, err = encode()
	require.NoError(t, err)
	require.Empty(t, page.Items)
	flights := m.state.Queues[queueName].Sessions[1].Flights
	message := flights[1]
	message.LogicalSize = message.bodySize() + 1
	flights[1] = message
	encode, size, err = m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Equal(t, len(body), size)
	page, err = encode()
	require.NoError(t, err)
	require.True(t, page.Items[0].Packed)
	require.Nil(t, page.Items[0].BodyLease)
	require.Zero(t, len(cache.pageSlots))
	m.ReleaseJournalSources()
}

func TestSelectedAssignmentSaturatedFallbackFreezesItemsAndThrough(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 2)
	source := m.state.Queues[queueName].Sessions[1].Flights[1].cold.source.(*ownedHotPayloadSource)
	for range cap(cache.pageSlots) {
		cache.pageSlots <- struct{}{}
	}
	encode, size, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Equal(t, 2*len(body), size)
	ack := QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: request.Incarnation, Session: 1, Owner: request.Owner, Sequence: 1, Attempt: 1}
	require.NoError(t, apply(t, m, ack).Err())
	require.NoError(t, apply(t, m, QueueCommand{Kind: PublishMessage, Queue: queueName, Incarnation: request.Incarnation, Body: []byte("later")}).Err())
	page, err := encode()
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Through)
	require.Len(t, page.Items, 2)
	require.EqualValues(t, 1, page.Items[0].Attempt)
	require.EqualValues(t, 2, page.Items[1].Attempt)
	require.Equal(t, body, page.Items[0].Body)
	require.Nil(t, page.Items[0].BodyLease)
	require.EqualValues(t, 1, source.reads.Load(), "only the item retired by ACK needs a journal read; the other retained span stays hot")
	_, err = encode()
	require.ErrorIs(t, err, ErrInvalidCommand, "one capture transfers ownership only once")
	for range cap(cache.pageSlots) {
		<-cache.pageSlots
	}
	m.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
}

func TestSelectedAssignmentColdOversizedFirstAndCorruptionRelease(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixtureSize(t, 1, 2<<20)
	request.Bytes = 4096
	encode, size, err := m.CaptureLocalAssignmentPage(request, false)
	require.NoError(t, err)
	require.Equal(t, len(body), size)
	page, err := encode()
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, body, page.Items[0].Body)
	require.Nil(t, page.Items[0].BodyLease)
	require.Zero(t, len(cache.pageSlots))
	cold := m.state.Queues[queueName].Sessions[1].Flights[1].cold
	cache.remove(cold)
	stored := make([]byte, cold.offset+int64(cold.size))
	_, err = cold.source.ReadAt(stored, 0)
	require.NoError(t, err)
	stored[cold.offset] ^= 1
	cold.source = &hotPayloadSource{Reader: bytes.NewReader(stored)}
	encode, _, err = m.CaptureLocalAssignmentPage(request, false)
	require.NoError(t, err)
	page, err = encode()
	require.ErrorContains(t, err, "checksum mismatch")
	require.Nil(t, page)
	require.Zero(t, len(cache.pageSlots))
	m.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
}

func TestSelectedAssignmentEmptyAndSettledHolesPreserveWatermark(t *testing.T) {
	m, cache, request, _ := borrowedAssignmentFixture(t, 2)
	require.NoError(t, apply(t, m, QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: request.Incarnation, Session: 1, Owner: request.Owner, Sequence: 1, Attempt: 1}).Err())
	encode, _, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	page, err := encode()
	require.NoError(t, err)
	require.EqualValues(t, 2, page.Through)
	require.Len(t, page.Items, 1)
	require.EqualValues(t, 2, page.Items[0].Attempt)
	page.Release()
	require.NoError(t, apply(t, m, QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: request.Incarnation, Session: 1, Owner: request.Owner, Sequence: 2, Attempt: 2}).Err())
	encode, size, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Zero(t, size)
	page, err = encode()
	require.NoError(t, err)
	require.Empty(t, page.Items)
	require.EqualValues(t, 2, page.Through)
	page.Release()
	require.Zero(t, cache.bytes)
}

func TestSelectedAssignmentPlainCopyDoesNotReleaseOutstandingLoanCharge(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 1)
	encode, _, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	loan, err := encode()
	require.NoError(t, err)
	charge := cache.bytes
	// The activation latch selects this same ordinary-copy branch while an
	// earlier all-hot page can still be outstanding at the network writer.
	encode, _, err = m.CaptureLocalAssignmentPage(request, false)
	require.NoError(t, err)
	copied, err := encode()
	require.NoError(t, err)
	require.Nil(t, copied.Items[0].BodyLease)
	copied.Items[0].Body[0] ^= 1
	require.Equal(t, body, loan.Items[0].Body)
	require.NoError(t, apply(t, m, QueueCommand{Kind: AckMessage, Queue: queueName, Incarnation: request.Incarnation, Session: 1, Owner: request.Owner, Sequence: 1, Attempt: 1}).Err())
	require.Equal(t, charge, cache.bytes, "a prior lease keeps the complete backing charged after ACK")
	loan.Release()
	require.Zero(t, cache.bytes)
	require.Zero(t, len(cache.pageSlots))
}

func TestSelectedAssignmentMixedFallbackReadsOnlyEvictedItem(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 2)
	flights := m.state.Queues[queueName].Sessions[1].Flights
	source := flights[1].cold.source.(*ownedHotPayloadSource)
	cache.remove(flights[2].cold)
	encode, size, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Equal(t, 2*len(body), size)
	page, err := encode()
	require.NoError(t, err)
	require.Len(t, page.Items, 2)
	require.Equal(t, body, page.Items[0].Body)
	require.Equal(t, body, page.Items[1].Body)
	require.Nil(t, page.Items[0].BodyLease)
	require.Nil(t, page.Items[1].BodyLease)
	require.EqualValues(t, 1, source.reads.Load(), "the still-hot first item must not be reread from the physical journal")
	m.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
}

func TestSelectedAssignmentPageBudgetFallbackUsesHotMemory(t *testing.T) {
	m, cache, request, body := borrowedAssignmentFixture(t, 2)
	source := m.state.Queues[queueName].Sessions[1].Flights[1].cold.source.(*ownedHotPayloadSource)
	for range cap(cache.pageSlots) {
		cache.pageSlots <- struct{}{}
	}
	encode, size, err := m.CaptureLocalAssignmentPage(request, true)
	require.NoError(t, err)
	require.Equal(t, 2*len(body), size)
	page, err := encode()
	require.NoError(t, err)
	require.Len(t, page.Items, 2)
	require.Nil(t, page.Items[0].BodyLease)
	require.Zero(t, source.reads.Load(), "a full loan budget must not reread hot items from disk")
	for range cap(cache.pageSlots) {
		<-cache.pageSlots
	}
	m.ReleaseJournalSources()
	require.Zero(t, cache.bytes)
}
