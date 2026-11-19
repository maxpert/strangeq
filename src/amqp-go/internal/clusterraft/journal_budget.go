package clusterraft

import (
	"fmt"
	"math"
	"sync"
)

// journalBudget belongs to one physical node, even when that node has two
// independent writers. Its mutex protects admission and extent reservations,
// never encoding, file I/O, durability fences, or response delivery.
type journalBudget struct {
	mu    sync.Mutex
	space *sync.Cond

	maxPendingBytes, maxPendingRequests int
	pendingBytes, pendingRequests       int
	fileBytes, maxFileBytes             int64
	staging                             journalScratchPool
}

// limits must already be validated. A zero file limit retains the original
// unlimited-writer contract, bounded by representable signed file offsets.
func newJournalBudget(limits journalLimits) *journalBudget {
	fileLimit := limits.maxFileBytes
	if fileLimit == 0 {
		fileLimit = math.MaxInt64
	}
	b := &journalBudget{
		maxPendingBytes: limits.pendingBytes, maxPendingRequests: limits.pendingRequests,
		maxFileBytes: fileLimit,
		staging:      newJournalScratchPool(min(limits.pendingRequests, 8), journalScratchMaxBytes),
	}
	b.space = sync.NewCond(&b.mu)
	return b
}

// reserveFile accounts for either a fully validated and synced recovered end,
// or an entire next frame before its first write. Charges are never refunded
// on an ambiguous write or sync failure. Only fresh recovery can reclaim a
// truncated incomplete tail. Subtraction keeps every check overflow-safe.
func (b *journalBudget) reserveFile(size int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size < 0 || size > b.maxFileBytes-b.fileBytes {
		return fmt.Errorf("node raft journal file capacity at %d bytes: %w", b.fileBytes, errJournalTooLarge)
	}
	b.fileBytes += size
	return nil
}

func (w *journalWriter) admissionFullLocked(charge int) bool {
	if w.pendingRequests >= w.limits.pendingRequests || charge > w.limits.pendingBytes-w.pendingBytes {
		return true
	}
	return w.budget != nil && (w.budget.pendingRequests >= w.budget.maxPendingRequests ||
		charge > w.budget.maxPendingBytes-w.budget.pendingBytes)
}

func (w *journalWriter) reserveAdmissionLocked(charge int) {
	w.pendingBytes += charge
	w.pendingRequests++
	if w.budget != nil {
		w.budget.pendingBytes += charge
		w.budget.pendingRequests++
	}
}

func (w *journalWriter) releaseAdmissionLocked(charge int) {
	w.pendingBytes -= charge
	w.pendingRequests--
	if w.budget != nil {
		w.budget.pendingBytes -= charge
		w.budget.pendingRequests--
	}
}

func (w *journalWriter) releaseRequestsLocked(batch []journalRequest) {
	for i := range batch {
		batch[i].record.body = nil
		batch[i].record.prepared = nil
		w.staging.release(batch[i].staging)
		batch[i].staging = journalScratchLoan{}
		w.releaseAdmissionLocked(batch[i].charge)
	}
}
