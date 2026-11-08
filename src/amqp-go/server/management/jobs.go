package management

import (
	"errors"
	"sync"
	"time"
)

// Job states (DATA-060…065).
const (
	jobQueued  = "queued"
	jobRunning = "running"
	jobDone    = "done"
	jobFailed  = "failed"
)

// ErrJobPoolFull is returned when the bounded job set is saturated.
var ErrJobPoolFull = errors.New("job pool is full")

// job is one asynchronous management mutation (FR-API-140…143). Jobs are
// in-memory only and owned by the principal that created them.
type job struct {
	id     string
	kind   string
	owner  string
	target any
	fn     func() (any, error)

	state      string
	createdAt  time.Time
	startedAt  time.Time
	finishedAt time.Time
	result     any
	err        error
}

// jobPool is a bounded worker pool owned by the management surface. It never
// shares a goroutine with the AMQP frame processor (FR-API-143).
type jobPool struct {
	mu        sync.Mutex
	jobs      map[string]*job
	order     []string
	max       int
	retention time.Duration
	now       func() time.Time
	queue     chan *job
	closed    bool
	wg        sync.WaitGroup

	running int
	done    int
	failed  int
}

func newJobPool(max int, retention time.Duration, workers int) *jobPool {
	if max < 1 {
		max = 128
	}
	if retention <= 0 {
		retention = time.Hour
	}
	if workers < 1 {
		workers = 4
	}
	p := &jobPool{
		jobs:      make(map[string]*job),
		max:       max,
		retention: retention,
		now:       time.Now,
		queue:     make(chan *job, max),
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *jobPool) worker() {
	defer p.wg.Done()
	for j := range p.queue {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		j.state = jobRunning
		j.startedAt = p.now()
		p.running++
		p.mu.Unlock()

		res, err := j.fn()

		p.mu.Lock()
		j.finishedAt = p.now()
		if err != nil {
			j.state = jobFailed
			j.err = err
			p.failed++
		} else {
			j.state = jobDone
			j.result = res
			p.done++
		}
		p.running--
		p.mu.Unlock()
	}
}

// submit enqueues a job. It returns ErrJobPoolFull when the retained set is at
// capacity (FR-API-142).
func (p *jobPool) submit(kind, owner string, target any, fn func() (any, error)) (*job, error) {
	id, err := randomHex(16)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapLocked(p.now())
	if len(p.jobs) >= p.max {
		return nil, ErrJobPoolFull
	}
	j := &job{
		id: id, kind: kind, owner: owner, target: target, fn: fn,
		state: jobQueued, createdAt: p.now(),
	}
	p.jobs[id] = j
	p.order = append(p.order, id)
	select {
	case p.queue <- j:
	default:
		// Queue full: run inline on this (management) goroutine rather than
		// dropping the mutation. Never the frame processor.
		go func() {
			p.mu.Lock()
			j.state = jobRunning
			j.startedAt = p.now()
			p.running++
			p.mu.Unlock()
			res, err := j.fn()
			p.mu.Lock()
			j.finishedAt = p.now()
			if err != nil {
				j.state = jobFailed
				j.err = err
				p.failed++
			} else {
				j.state = jobDone
				j.result = res
				p.done++
			}
			p.running--
			p.mu.Unlock()
		}()
	}
	return j, nil
}

// get returns the job with id, but only when owner matches (another principal
// sees 404, FR-API-142).
func (p *jobPool) get(id, owner string) (*job, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapLocked(p.now())
	j, ok := p.jobs[id]
	if !ok || j.owner != owner {
		return nil, false
	}
	return j, true
}

// viewOf returns the wire view of a job under the pool lock. It must be used by
// any request path (hJob): a worker mutates a job's state under the same lock,
// so reading the fields outside it is a data race.
func (p *jobPool) viewOf(id, owner string) (JobView, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapLocked(p.now())
	j, ok := p.jobs[id]
	if !ok || j.owner != owner {
		return JobView{}, false
	}
	return j.view(), true
}

// stateOf reports a job's state under the pool lock. It exists so tests (and
// any future internal caller) never read job fields unsynchronised.
func (p *jobPool) stateOf(id, owner string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j, ok := p.jobs[id]
	if !ok || j.owner != owner {
		return "", false
	}
	return j.state, true
}

func (p *jobPool) reapLocked(now time.Time) {
	if p.retention <= 0 {
		return
	}
	keep := p.order[:0]
	for _, id := range p.order {
		j := p.jobs[id]
		if j == nil {
			continue
		}
		terminal := j.state == jobDone || j.state == jobFailed
		if terminal && now.Sub(j.finishedAt) > p.retention {
			delete(p.jobs, id)
			continue
		}
		keep = append(keep, id)
	}
	p.order = keep
}

// stats reports the number of jobs in each state (label for the
// amqp_management_jobs{state} gauge).
func (p *jobPool) stats() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapLocked(p.now())
	out := map[string]int{jobQueued: 0, jobRunning: p.running, jobDone: p.done, jobFailed: p.failed}
	for _, j := range p.jobs {
		if j.state == jobQueued {
			out[jobQueued]++
		}
	}
	return out
}

// close stops the worker pool.
func (p *jobPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.queue)
	p.mu.Unlock()
	p.wg.Wait()
}

// JobView is the wire shape of a job (DATA-060…065). Nullable timestamps are
// emitted as null until the transition happens.
type JobView struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	State      string `json:"state"`
	CreatedAt  string `json:"created_at"`
	StartedAt  any    `json:"started_at"`
	FinishedAt any    `json:"finished_at"`
	Target     any    `json:"target"`
	Result     any    `json:"result"`
	Error      any    `json:"error"`
}

func (j *job) view() JobView {
	v := JobView{
		ID:        j.id,
		Kind:      j.kind,
		State:     j.state,
		CreatedAt: j.createdAt.UTC().Format(time.RFC3339),
		Target:    j.target,
	}
	if !j.startedAt.IsZero() {
		v.StartedAt = j.startedAt.UTC().Format(time.RFC3339)
	}
	if !j.finishedAt.IsZero() {
		v.FinishedAt = j.finishedAt.UTC().Format(time.RFC3339)
	}
	if j.result != nil {
		v.Result = j.result
	}
	if j.err != nil {
		v.Error = j.err.Error()
	}
	return v
}
