package clusterqueue

import (
	"errors"
	"sync/atomic"
)

const maxHotAssignmentPages = 64

var errBorrowedAssignmentUnavailable = errors.New("borrowed assignment page unavailable")

// BodyLease owns a read-only body through the synchronous network write.
// Release must be called after clearing all body aliases at the current owner.
type BodyLease interface{ Release() }

// BorrowedAssignmentPage is local-only and cannot be serialized. Ownership of
// Items transfers once to the dispatcher; unused pages must be released.
type BorrowedAssignmentPage struct {
	Through AttemptID
	Items   []Delivery
}

func (p *BorrowedAssignmentPage) TakeItems() []Delivery {
	items := p.Items
	p.Items = nil
	return items
}

func (p *BorrowedAssignmentPage) Release() {
	if p == nil {
		return
	}
	ReleaseDeliveryBodies(p.Items)
	p.Items = nil
}

func (d *Delivery) ReleaseBody() {
	if d.BodyLease != nil {
		lease := d.BodyLease
		d.Body, d.BodyLease = nil, nil
		lease.Release()
	}
}

func ReleaseDeliveryBodies(items []Delivery) {
	for i := range items {
		items[i].ReleaseBody()
	}
}

type hotAssignmentPageLease struct {
	slots chan struct{}
	left  atomic.Int32
}

type hotAssignmentBodyLease struct {
	cache    *HotPayloadCache
	entry    *hotPayloadEntry
	page     *hotAssignmentPageLease
	released atomic.Bool
}

func (l *hotAssignmentBodyLease) Release() {
	if !l.released.CompareAndSwap(false, true) {
		return
	}
	c := l.cache
	c.mu.Lock()
	l.entry.loans--
	if l.entry.loans == 0 && !l.entry.linked {
		c.freeLocked(l.entry)
	}
	c.mu.Unlock()
	if l.page.left.Add(-1) == 0 {
		<-l.page.slots
	}
}

// CaptureLocalAssignmentPage selects and authorizes exactly once. Its encoder
// must run through Group.ReadCaptured: cold spans require its off-lock slot and
// journal capture pin. Ordinary packed/small items receive private copies.
func (m *Machine) CaptureLocalAssignmentPage(request AssignmentRequest, allowBorrow bool) (func() (*BorrowedAssignmentPage, error), int, error) {
	if !request.valid() {
		return nil, 0, ErrInvalidCommand
	}
	command := QueueCommand{Queue: request.Queue, Incarnation: request.Incarnation,
		Session: request.Session, Owner: request.Owner, ClaimLimit: request.Limit, ClaimBytes: request.Bytes}
	items, through, err := m.ReadAssignments(command, request.After)
	if err != nil {
		return nil, 0, err
	}
	if allowBorrow {
		page, err := m.borrowAssignmentItems(items, through)
		if err == nil {
			return singleLocalPage(func() (*BorrowedAssignmentPage, error) { return page, nil }), 0, nil
		}
		if !errors.Is(err, errBorrowedAssignmentUnavailable) {
			return nil, 0, err
		}
	}
	copyBytes := 0
	for _, item := range items {
		size := item.bodySize()
		if size > maxMessageBytes-copyBytes {
			return nil, 0, ErrCapacity
		}
		copyBytes += size
	}
	return singleLocalPage(func() (*BorrowedAssignmentPage, error) {
		// This is ordinary copied response storage, bounded by the selected
		// page and existing reply slots. It is NOT charged to the hot cache.
		scratch := make([]byte, copyBytes)
		for i := range items {
			size := items[i].bodySize()
			// Preserve the established hot-copy then checked-journal fallback.
			// A page-budget/mixed miss does not make its other hot spans cold.
			data, err := items[i].appendBody(scratch[:0:size])
			if err != nil {
				return nil, err
			}
			items[i].Body, items[i].cold = data, nil
			scratch = scratch[size:]
		}
		return &BorrowedAssignmentPage{Through: through, Items: items}, nil
	}), copyBytes, nil
}

// Typed pages transfer ownership, so one capture cannot issue the same leases
// or mutable copied response twice. Group.ReadCaptured invokes it exactly once.
func singleLocalPage(encode func() (*BorrowedAssignmentPage, error)) func() (*BorrowedAssignmentPage, error) {
	var consumed atomic.Bool
	return func() (*BorrowedAssignmentPage, error) {
		if !consumed.CompareAndSwap(false, true) {
			return nil, ErrInvalidCommand
		}
		return encode()
	}
}

// A failed loan attempt never changes the selection or authorizes it again.
func (m *Machine) borrowAssignmentItems(items []Delivery, through AttemptID) (*BorrowedAssignmentPage, error) {
	c := m.hotPayloads
	if c == nil {
		return nil, errBorrowedAssignmentUnavailable
	}
	if len(items) == 0 {
		return &BorrowedAssignmentPage{Through: through}, nil
	}
	select {
	case c.pageSlots <- struct{}{}:
	default:
		return nil, errBorrowedAssignmentUnavailable
	}
	c.mu.Lock()
	for _, item := range items {
		b := item.cold
		if item.Packed || b == nil || b.hotPayloads != c || b.hotPayload == nil ||
			!b.hotPayload.linked || b.hotPayload.data == nil {
			c.mu.Unlock()
			<-c.pageSlots
			return nil, errBorrowedAssignmentUnavailable
		}
	}
	pageLease := &hotAssignmentPageLease{slots: c.pageSlots}
	pageLease.left.Store(int32(len(items)))
	for i := range items {
		b := items[i].cold
		entry := b.hotPayload
		entry.loans++
		items[i].Body = entry.data[b.hotStart : b.hotStart+b.size : b.hotStart+b.size]
		items[i].BodyLease = &hotAssignmentBodyLease{cache: c, entry: entry, page: pageLease}
		items[i].cold = nil
	}
	c.mu.Unlock()
	return &BorrowedAssignmentPage{Through: through, Items: items}, nil
}
