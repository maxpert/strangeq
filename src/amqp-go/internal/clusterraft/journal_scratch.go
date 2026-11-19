package clusterraft

// Cover a normal 1 MiB Raft message plus entry and journal framing.
const journalScratchMaxBytes = (1 << 20) + (4 << 10)

// Each token reserves one slot from acquisition through return. The buffers
// are lazy. Calls can use temporary scratch when a slot is unavailable.
type journalScratchPool struct {
	free     chan []byte
	maxBytes int
}

type journalScratchLoan struct {
	buffer []byte
	pooled bool
}

func newJournalScratchPool(slots, maxBytes int) journalScratchPool {
	pool := journalScratchPool{free: make(chan []byte, slots), maxBytes: maxBytes}
	for range slots {
		pool.free <- nil
	}
	return pool
}

func (p journalScratchPool) acquire(size int) journalScratchLoan {
	var loan journalScratchLoan
	if size <= p.maxBytes {
		select {
		case loan.buffer = <-p.free:
			loan.pooled = true
		default:
		}
	}
	if cap(loan.buffer) < size {
		loan.buffer = make([]byte, size)
	}
	return loan
}

func (p journalScratchPool) release(loan journalScratchLoan) {
	if loan.pooled {
		p.free <- loan.buffer[:0]
	}
}
