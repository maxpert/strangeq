// Package clusterproposal carries explicit ownership of a final encoded command
// with reserved proposal-key space. It never recovers a prefix by backtracking
// a body slice. Callers must stop using writable aliases after Adopt.
package clusterproposal

import (
	"errors"
	"sync"
)

const PrefixBytes = 16

var ErrConsumed = errors.New("owned proposal already consumed")

// Owned owns one final encoder allocation. Command returns a read-only view;
// Begin transfers ownership once, including when a submission later fails.
type Owned struct {
	mu         sync.Mutex
	allocation []byte
}

// Adopt transfers a fresh, exact-size final encoder allocation, including its
// zeroed key prefix. It introduces no copy and must not receive pooled storage.
func Adopt(allocation []byte) (*Owned, error) {
	if len(allocation) <= PrefixBytes || len(allocation) != cap(allocation) {
		return nil, errors.New("invalid owned proposal allocation")
	}
	return &Owned{allocation: allocation}, nil
}

func (p *Owned) Command() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocation == nil {
		return nil
	}
	return p.allocation[PrefixBytes:]
}

// Submission is a single routing operation. Safe rejected attempts may use the
// ordinary copied path after Take; ambiguous outcomes must never be retried.
type Submission struct {
	mu         sync.Mutex
	allocation []byte
}

func (p *Owned) Begin() (*Submission, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocation == nil {
		return nil, ErrConsumed
	}
	submission := &Submission{allocation: p.allocation}
	p.allocation = nil
	return submission, nil
}

func (p *Submission) Command() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocation == nil {
		return nil
	}
	return p.allocation[PrefixBytes:]
}

func (p *Submission) Available() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocation != nil
}

// Take runs only inside Group's admitted proposal callback. It transfers the
// complete encoder allocation to Raft/gathering, writing only the reserved key.
func (p *Submission) Take(key []byte) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocation == nil {
		return nil, ErrConsumed
	}
	if len(key) != PrefixBytes {
		return nil, errors.New("invalid owned proposal key")
	}
	allocation := p.allocation
	p.allocation = nil
	copy(allocation[:PrefixBytes], key)
	return allocation, nil
}

func (p *Submission) Discard() {
	p.mu.Lock()
	p.allocation = nil
	p.mu.Unlock()
}
