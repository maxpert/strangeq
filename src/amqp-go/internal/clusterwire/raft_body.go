package clusterwire

import "sync"

const (
	raftBodySlots   = 4
	minRaftBodySize = 16 << 10
	maxRaftBodySize = (1 << 20) + (4 << 10)
)

// The Raft decoder owns its protobuf entries. Raw wire bytes can be reused
// as soon as decoding finishes, even when a shard keeps the decoded entries.
type raftBodyPool struct {
	mu      sync.Mutex
	buffers [raftBodySlots][]byte
	free    int
}

func (pool *raftBodyPool) borrow(size int) ([]byte, bool) {
	if size >= minRaftBodySize && size <= maxRaftBodySize {
		pool.mu.Lock()
		if pool.free != 0 {
			pool.free--
			buffer := pool.buffers[pool.free]
			pool.buffers[pool.free] = nil
			pool.mu.Unlock()
			if cap(buffer) < size {
				buffer = make([]byte, min(maxRaftBodySize, max(size, 2*cap(buffer))))
			}
			return buffer[:size], true
		}
		pool.mu.Unlock()
	}
	return make([]byte, size), false
}

func (pool *raftBodyPool) release(buffer []byte) {
	pool.mu.Lock()
	pool.buffers[pool.free] = buffer[:0]
	pool.free++
	pool.mu.Unlock()
}
