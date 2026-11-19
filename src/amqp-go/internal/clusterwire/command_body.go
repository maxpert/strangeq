package clusterwire

import (
	"bytes"
	"io"
	"sync"
)

const maxCommandBodyRead = 32 << 10

// commandBody lends memory to the HTTP transport until Close. Each read copies
// into the transport's buffer while holding the lock, so Close can revoke the
// loan even when the transport closes the request asynchronously.
type commandBody struct {
	mu     sync.Mutex
	prefix bytes.Reader
	body   bytes.Reader
	closed bool
}

func newCommandBody(prefix, body []byte) *commandBody {
	r := &commandBody{}
	r.prefix.Reset(prefix)
	r.body.Reset(body)
	return r
}

func (r *commandBody) Read(dst []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	dst = dst[:min(len(dst), maxCommandBodyRead)]
	n, _ := r.prefix.Read(dst)
	if n == len(dst) || n != 0 && r.body.Len() == 0 {
		return n, nil
	}
	m, err := r.body.Read(dst[n:])
	return n + m, err
}

func (r *commandBody) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.prefix.Reset(nil)
	r.body.Reset(nil)
	return nil
}
