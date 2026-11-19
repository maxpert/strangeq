package clusterraft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
)

var (
	errJournalBusy   = errors.New("raft journal admission full")
	errJournalClosed = errors.New("raft journal closed")
)

// A pair shares this one-way latch. Nil always means full durability.
type journalDurability struct{ strict atomic.Bool }

func (d *journalDurability) processAllowed() bool { return d != nil && !d.strict.Load() }

type journalFile interface {
	io.WriterAt
	Sync() error
	Close() error
}

type journalLimits struct {
	pendingBytes, pendingRequests int // Includes the active batch's queued copies.
	batchBytes, batchRequests     int // Bounds the encoded bytes and record count per batch.
	maxFileBytes                  int64
}

type journalCompletion struct {
	location journalLocation
	err      error
}

type journalRequest struct {
	record          journalRecord
	delta           *journalGroupDelta
	staging         journalScratchLoan
	syncRequired    bool
	processEligible bool
	charge          int
	result          chan journalCompletion
}

type journalWriter struct {
	durability *journalDurability
	file       journalFile
	limits     journalLimits
	budget     *journalBudget
	staging    journalScratchPool
	done       chan struct{}
	failed     chan struct{}

	localMu          sync.Mutex
	mu               *sync.Mutex
	ready            *sync.Cond
	space            *sync.Cond
	queue            []journalRequest
	pendingBytes     int
	pendingRequests  int
	admissionWaiters int
	closing          bool
	err              error

	// Only the writer goroutine accesses these fields.
	offset       int64
	nextSequence uint64
	dirty        bool
	frameBuffer  []byte
}

// The caller supplies the validated, durable end and next sequence of this
// active segment. The writer owns file on success. Opening and repairing the
// file belong to the store owning it.
func newJournalWriterWithBudget(file journalFile, end int64, nextSequence uint64, limits journalLimits, budget *journalBudget) (*journalWriter, error) {
	if file == nil || end < 0 || nextSequence == 0 || nextSequence == math.MaxUint64 ||
		limits.pendingBytes <= 0 || limits.pendingRequests <= 0 || limits.batchBytes <= 0 ||
		limits.batchBytes > maxJournalFrameBytes || limits.batchRequests <= 0 ||
		limits.batchRequests > maxJournalFrameRecords || limits.maxFileBytes < 0 ||
		(limits.maxFileBytes != 0 && end > limits.maxFileBytes) {
		return nil, errors.New("invalid raft journal writer configuration")
	}
	w := &journalWriter{
		file: file, limits: limits, budget: budget, done: make(chan struct{}), failed: make(chan struct{}),
		offset: end, nextSequence: nextSequence,
	}
	if budget == nil {
		w.mu = &w.localMu
		w.space = sync.NewCond(w.mu)
		w.staging = newJournalScratchPool(min(limits.pendingRequests, 8), journalScratchMaxBytes)
	} else {
		if limits.pendingBytes > budget.maxPendingBytes || limits.pendingRequests > budget.maxPendingRequests {
			return nil, errors.New("raft journal writer exceeds node admission budget")
		}
		if _, segmented := file.(*journalSegmentOutput); !segmented {
			if err := budget.reserveFile(end); err != nil {
				return nil, err
			}
		}
		w.mu, w.space, w.staging = &budget.mu, budget.space, budget.staging
	}
	w.ready = sync.NewCond(w.mu)
	go w.run()
	return w, nil
}

// enqueueEncoded reserves before allocating a body. This is for a local
// append job that Raft has already accepted and cannot cancel or drop. Each
// group has one ordered append job, so encoding outside the lock cannot
// reorder two requests for the same group.
func (w *journalWriter) enqueueEncoded(group uint64, size int, syncRequired bool, delta *journalGroupDelta, encode func([]byte) error) (<-chan journalCompletion, error) {
	return w.enqueueEncodedMode(group, size, syncRequired, false, delta, encode)
}

func (w *journalWriter) enqueueEncodedMode(group uint64, size int, syncRequired, processEligible bool, delta *journalGroupDelta, encode func([]byte) error) (<-chan journalCompletion, error) {
	if group == 0 || size < 0 || size > maxJournalFrameBytes-journalRecordHeaderSize ||
		size > w.limits.pendingBytes-journalRecordHeaderSize {
		return nil, errJournalTooLarge
	}
	charge := journalRecordHeaderSize + size
	w.mu.Lock()
	for !w.closing && w.admissionFullLocked(charge) {
		w.admissionWaiters++
		w.space.Wait()
		w.admissionWaiters--
	}
	if w.closing {
		err := w.err
		w.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, errJournalClosed
	}
	w.reserveAdmissionLocked(charge)
	w.mu.Unlock()

	staging := w.staging.acquire(size)
	body := staging.buffer[:size:size]
	err := encode(body)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil || w.closing {
		w.staging.release(staging)
		w.releaseAdmissionLocked(charge)
		w.space.Broadcast()
		if w.err != nil {
			return nil, w.err
		}
		if err != nil {
			return nil, err
		}
		return nil, errJournalClosed
	}
	result := make(chan journalCompletion, 1)
	w.queue = append(w.queue, journalRequest{
		record: journalRecord{group: group, body: body}, syncRequired: syncRequired, processEligible: processEligible,
		delta: delta, staging: staging, charge: charge, result: result,
	})
	w.ready.Signal()
	return result, nil
}

// enqueue copies the body after admission. An empty body is a fence-only
// request. A request requiring durability completes only after the write and
// all preceding writes have been synced. Completion without a sync requirement
// confirms write progress and carries no standalone durability guarantee.
func (w *journalWriter) enqueue(group uint64, body []byte, syncRequired bool) (<-chan journalCompletion, error) {
	if group == 0 {
		return nil, errors.New("invalid raft journal group")
	}
	if len(body) > maxJournalFrameBytes-journalRecordHeaderSize {
		return nil, errJournalTooLarge
	}
	charge := journalRecordHeaderSize + len(body)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return nil, w.err
	}
	if w.closing {
		return nil, errJournalClosed
	}
	if charge > w.limits.pendingBytes {
		return nil, errJournalTooLarge
	}
	if w.admissionFullLocked(charge) {
		return nil, errJournalBusy
	}
	result := make(chan journalCompletion, 1)
	w.queue = append(w.queue, journalRequest{
		record: journalRecord{group: group, body: bytes.Clone(body)}, syncRequired: syncRequired,
		charge: charge, result: result,
	})
	w.reserveAdmissionLocked(charge)
	w.ready.Signal()
	return result, nil
}

func (w *journalWriter) takeBatch() []journalRequest {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.queue) == 0 && !w.closing {
		w.ready.Wait()
	}
	if len(w.queue) == 0 {
		return nil
	}
	// A single admitted record above the ordinary batch target still fits
	// the format and is written alone. Other groups keep their FIFO order.
	count, size := 1, w.queue[0].charge
	for count < len(w.queue) && count < w.limits.batchRequests && w.queue[count].charge <= w.limits.batchBytes-size {
		size += w.queue[count].charge
		count++
	}
	batch := slices.Clone(w.queue[:count])
	left := copy(w.queue, w.queue[count:])
	clear(w.queue[left:])
	w.queue = w.queue[:left]
	return batch
}

func (w *journalWriter) run() {
	defer func() {
		w.frameBuffer = nil
		closeErr := w.file.Close()
		w.mu.Lock()
		w.err = errors.Join(w.err, closeErr)
		w.mu.Unlock()
		close(w.done)
	}()
	for batch := w.takeBatch(); len(batch) != 0; batch = w.takeBatch() {
		if err := w.writeBatch(batch); err != nil {
			w.fail(batch, err)
			return
		}
		w.complete(batch)
		if segmented, ok := w.file.(*journalSegmentOutput); ok {
			if err := segmented.maintain(false); err != nil && !errors.Is(err, errJournalBusy) {
				w.fail(nil, err)
				return
			}
		}
	}
	if err := w.sync(); err != nil {
		w.fail(nil, err)
	}
}

func (w *journalWriter) writeBatch(batch []journalRequest) error {
	records := make([]journalRecord, 0, len(batch))
	var syncRequired bool
	for _, request := range batch {
		if len(request.record.body) != 0 {
			records = append(records, request.record)
		}
		// Eligibility is rechecked at execution, after any activation latch.
		syncRequired = syncRequired || request.syncRequired || (request.processEligible && !w.durability.processAllowed())
	}
	if len(records) != 0 {
		frame, locations, err := encodeJournalFrame(w.frameBuffer, w.nextSequence, records)
		if err != nil {
			return err
		}
		if len(frame) <= w.limits.batchBytes+journalFrameHeaderSize+journalFrameTrailerSize {
			w.frameBuffer = frame
		}
		if int64(len(frame)) > math.MaxInt64-w.offset ||
			(w.limits.maxFileBytes != 0 && int64(len(frame)) > w.limits.maxFileBytes-w.offset) {
			return fmt.Errorf("raft journal file limit at offset %d: %w", w.offset, errJournalTooLarge)
		}
		// A single writer already owns the full cap through its local offset.
		if _, segmented := w.file.(*journalSegmentOutput); w.budget != nil && !segmented {
			if err := w.budget.reserveFile(int64(len(frame))); err != nil {
				return err
			}
		}
		// A synchronous range write cannot fence earlier dirty frames. Only
		// select it when this entire batch starts from a durable prefix.
		n, synced, err := writeJournalFrame(w.file, frame, w.offset, syncRequired && !w.dirty)
		if err != nil {
			return fmt.Errorf("write raft journal at offset %d: %w", w.offset, err)
		}
		if n != len(frame) {
			return fmt.Errorf("write raft journal at offset %d: %w", w.offset, io.ErrShortWrite)
		}
		i := 0
		for j := range batch {
			if len(batch[j].record.body) == 0 {
				continue
			}
			location := locations[i]
			location.offset += w.offset
			batch[j].record.location = location
			i++
		}
		w.offset += int64(len(frame))
		w.nextSequence++
		w.dirty = !synced
	}
	if syncRequired {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, request := range batch {
		if request.delta == nil {
			continue
		}
		for i := range request.delta.entries {
			request.delta.entries[i].offset += request.record.location.offset
		}
		if segment, ok := w.file.(*journalSegmentOutput); ok && segment.index != nil {
			if err := segment.index.append(request.record.group, *request.delta); err != nil {
				return err
			}
		}
	}
	// Publish process-readable bytes only after frame checking and append-index
	// publication both succeeded. Strict reads retain their sync-first boundary.
	if segment, ok := w.file.(*journalSegmentOutput); ok {
		segment.store.mu.Lock()
		segment.store.completedEnd = segment.store.end
		segment.store.mu.Unlock()
	}
	return nil
}

func (w *journalWriter) sync() error {
	if !w.dirty {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync raft journal at offset %d: %w", w.offset, err)
	}
	w.dirty = false
	return nil
}

func (w *journalWriter) complete(batch []journalRequest) {
	w.mu.Lock()
	w.releaseRequestsLocked(batch)
	w.space.Broadcast()
	w.mu.Unlock()
	for _, request := range batch {
		request.result <- journalCompletion{location: request.record.location}
	}
}

func (w *journalWriter) fail(active []journalRequest, err error) {
	w.mu.Lock()
	w.err, w.closing = err, true
	close(w.failed)
	queued := w.queue
	w.queue = nil
	// Encoding jobs still own their reservations and staging buffers until
	// they return. Release only this writer's active and queued copies here;
	// neither peer-writer reservations nor unfinished encoders are reusable.
	w.releaseRequestsLocked(active)
	w.releaseRequestsLocked(queued)
	w.space.Broadcast()
	w.mu.Unlock()
	for _, batch := range [][]journalRequest{active, queued} {
		for _, request := range batch {
			request.result <- journalCompletion{err: err}
		}
	}
}

func (w *journalWriter) failure() error {
	select {
	case <-w.failed:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.err
	default:
		return nil
	}
}

func (w *journalWriter) close() error {
	w.mu.Lock()
	w.closing = true
	w.ready.Signal()
	w.space.Broadcast()
	w.mu.Unlock()
	<-w.done
	return w.err // The writer publishes its final error before closing done.
}

// requireFullDurability closes the fast path before ordering a strict fence.
// Admission waits rather than allowing a full queue to bypass activation.
func (w *journalWriter) requireFullDurability() error {
	if w.durability != nil {
		w.durability.strict.Store(true)
	}
	result, err := w.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	if err != nil {
		return err
	}
	return (<-result).err
}
