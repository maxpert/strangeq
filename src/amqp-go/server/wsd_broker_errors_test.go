package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/stretchr/testify/assert"
)

// TestBrokerErrorReply_MappingTable verifies the M1-M11 classification table
// (spec §19.1) that WS-B's central ChannelError mapping must delegate to.
func TestBrokerErrorReply_MappingTable(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode uint16
		wantOK   bool
	}{
		// M1/M2: 404s
		{"queue-not-found", broker.QueueNotFoundError("q"), amqperrors.NotFound, true},
		{"exchange-not-found", broker.ExchangeNotFoundError("x"), amqperrors.NotFound, true},
		// M4: 405
		{"queue-locked", fmt.Errorf("wrap: %w", broker.ErrQueueLocked), amqperrors.ResourceLocked, true},
		// M5: 403
		{"exchange-internal", fmt.Errorf("wrap: %w", broker.ErrExchangeInternal), amqperrors.AccessRefused, true},
		// 406 family
		{"inequivalent-args", broker.ErrInequivalentArgs, amqperrors.PreconditionFailed, true},
		{"exchange-type-mismatch", broker.ErrExchangeTypeMismatch, amqperrors.PreconditionFailed, true},
		{"invalid-queue-argument", broker.ErrInvalidQueueArgument, amqperrors.PreconditionFailed, true},
		{"queue-in-use", broker.ErrQueueInUse, amqperrors.PreconditionFailed, true},
		{"queue-not-empty", broker.ErrQueueNotEmpty, amqperrors.PreconditionFailed, true},
		{"exchange-in-use", broker.ErrExchangeInUse, amqperrors.PreconditionFailed, true},
		{"invalid-binding-argument", broker.ErrInvalidBindingArgument, amqperrors.PreconditionFailed, true},
		{"queue-limit", broker.ErrQueueLimitReached, amqperrors.PreconditionFailed, true},
		{"exchange-limit", broker.ErrExchangeLimitReached, amqperrors.PreconditionFailed, true},
		// Non-semantic broker errors must NOT classify (nack/return/drop paths).
		{"max-length", broker.ErrMaxLengthExceeded, 0, false},
		{"no-route", broker.ErrNoRoute, 0, false},
		{"queue-closed", broker.ErrQueueClosed, 0, false},
		{"unknown", errors.New("something else"), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, text, ok := brokerErrorReply(tc.err)
			assert.Equal(t, tc.wantOK, ok, "ok")
			if tc.wantOK {
				assert.Equal(t, tc.wantCode, code, "code")
				assert.Contains(t, text, "- ", "reply text must carry the CODE_NAME - prefix")
			}
		})
	}
}

// TestBrokerErrorReply_Wrapped verifies errors.Is reach through a wrapped
// SemanticError (the storage-broker paths wrap with %w in places).
func TestBrokerErrorReply_Wrapped(t *testing.T) {
	inner := broker.QueueNotFoundError("q")
	wrapped := fmt.Errorf("outer context: %w", inner)
	code, _, ok := brokerErrorReply(wrapped)
	assert.True(t, ok)
	assert.Equal(t, uint16(amqperrors.NotFound), code)
}
