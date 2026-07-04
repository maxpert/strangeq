package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAMQPError(t *testing.T) {
	err := &AMQPError{
		Code:    NotFound,
		Message: "Resource not found",
		Method:  "queue.declare",
	}

	assert.Equal(t, "AMQP Error 404 in queue.declare: Resource not found", err.Error())
	assert.Nil(t, err.Unwrap())
}

func TestAMQPErrorWithoutMethod(t *testing.T) {
	err := &AMQPError{
		Code:    InternalError,
		Message: "Internal server error",
	}

	assert.Equal(t, "AMQP Error 541: Internal server error", err.Error())
}

func TestAMQPErrorWithCause(t *testing.T) {
	cause := errors.New("underlying error")
	err := &AMQPError{
		Code:    InternalError,
		Message: "Wrapper error",
		Cause:   cause,
	}

	assert.Equal(t, cause, err.Unwrap())
}

func TestAMQPErrorAs(t *testing.T) {
	authErr := NewAuthError(AccessRefused, "denied", "alice", "queue", "read")

	var amqpErr *AMQPError
	assert.True(t, errors.As(authErr, &amqpErr))
	assert.Equal(t, AccessRefused, amqpErr.Code)
	assert.Equal(t, "denied", amqpErr.Message)
}

func TestAuthError(t *testing.T) {
	err := NewAuthError(AccessRefused, "Permission denied", "john", "queue.test", "read")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "Permission denied", err.Message)
	assert.Equal(t, "john", err.Username)
	assert.Equal(t, "queue.test", err.Resource)
	assert.Equal(t, "read", err.Operation)
}

func TestAuthorizationFailedError(t *testing.T) {
	err := NewAuthorizationFailed("bob", "exchange.logs", "configure")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "bob", err.Username)
	assert.Equal(t, "exchange.logs", err.Resource)
	assert.Equal(t, "configure", err.Operation)
	assert.Contains(t, err.Message, "not authorized for configure on exchange.logs")
}

func TestIsAccessRefused(t *testing.T) {
	accessErr := NewAuthError(AccessRefused, "denied", "user", "resource", "op")
	canonicalAccessErr := &AMQPError{Code: AccessRefused, Message: "access denied"}
	notFoundErr := &AMQPError{Code: NotFound, Message: "not found"}
	genericErr := errors.New("generic error")

	assert.True(t, IsAccessRefused(accessErr))
	assert.True(t, IsAccessRefused(canonicalAccessErr))
	assert.False(t, IsAccessRefused(notFoundErr))
	assert.False(t, IsAccessRefused(genericErr))
}

func TestDuplicateConstantsRemoved(t *testing.T) {
	codes := map[string]int{
		"ConnectionForced":   ConnectionForced,
		"InvalidPath":        InvalidPath,
		"AccessRefused":      AccessRefused,
		"NotFound":           NotFound,
		"ResourceLocked":     ResourceLocked,
		"PreconditionFailed": PreconditionFailed,
	}

	expected := map[string]int{
		"ConnectionForced":   320,
		"InvalidPath":        402,
		"AccessRefused":      403,
		"NotFound":           404,
		"ResourceLocked":     405,
		"PreconditionFailed": 406,
	}
	for name, code := range expected {
		assert.Equal(t, code, codes[name], "%s must retain its AMQP spec value", name)
	}

	seen := make(map[int]string, len(codes))
	for name, code := range codes {
		if prev, dup := seen[code]; dup {
			t.Fatalf("duplicate error code %d shared by %s and %s — *2 constants must be removed", code, prev, name)
		}
		seen[code] = name
	}
}
