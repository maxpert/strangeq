package errors

import (
	"errors"
	"fmt"
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

func TestConnectionError(t *testing.T) {
	err := NewConnectionError(ConnectionForced, "Server shutting down", "conn-123")

	assert.Equal(t, ConnectionForced, err.Code)
	assert.Equal(t, "Server shutting down", err.Message)
	assert.Equal(t, "conn-123", err.ConnectionID)
	assert.Contains(t, err.Error(), "Server shutting down")
}

func TestConnectionForcedError(t *testing.T) {
	err := NewConnectionForced("conn-456", "maintenance mode")

	assert.Equal(t, ConnectionForced, err.Code)
	assert.Equal(t, "conn-456", err.ConnectionID)
	assert.Contains(t, err.Message, "maintenance mode")
}

func TestAccessRefusedError(t *testing.T) {
	err := NewAccessRefused("conn-789", "invalid credentials")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "conn-789", err.ConnectionID)
	assert.Contains(t, err.Message, "invalid credentials")
}

func TestChannelError(t *testing.T) {
	err := NewChannelError(PreconditionFailed, "Channel state invalid", "conn-123", 5)

	assert.Equal(t, PreconditionFailed, err.Code)
	assert.Equal(t, "Channel state invalid", err.Message)
	assert.Equal(t, "conn-123", err.ConnectionID)
	assert.Equal(t, uint16(5), err.ChannelID)
}

func TestChannelNotFoundError(t *testing.T) {
	err := NewChannelNotFound("conn-123", 10)

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, uint16(10), err.ChannelID)
	assert.Contains(t, err.Message, "Channel 10 not found")
}

func TestChannelPreconditionFailedError(t *testing.T) {
	err := NewChannelPreconditionFailed("conn-123", 5, "channel already closed")

	assert.Equal(t, PreconditionFailed, err.Code)
	assert.Equal(t, uint16(5), err.ChannelID)
	assert.Contains(t, err.Message, "channel already closed")
}

func TestExchangeError(t *testing.T) {
	err := NewExchangeError(NotFound, "Exchange does not exist", "test-exchange", "exchange.bind")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, "Exchange does not exist", err.Message)
	assert.Equal(t, "test-exchange", err.ExchangeName)
	assert.Equal(t, "exchange.bind", err.Method)
}

func TestExchangeNotFoundError(t *testing.T) {
	err := NewExchangeNotFound("missing-exchange", "basic.publish")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, "missing-exchange", err.ExchangeName)
	assert.Contains(t, err.Message, "Exchange 'missing-exchange' not found")
}

func TestExchangeTypeMismatchError(t *testing.T) {
	err := NewExchangeTypeMismatch("test-exchange", "direct", "fanout", "exchange.declare")

	assert.Equal(t, PreconditionFailed, err.Code)
	assert.Equal(t, "test-exchange", err.ExchangeName)
	assert.Contains(t, err.Message, "expected direct, got fanout")
}

func TestQueueError(t *testing.T) {
	err := NewQueueError(ResourceLocked, "Queue is locked", "test-queue", "queue.delete")

	assert.Equal(t, ResourceLocked, err.Code)
	assert.Equal(t, "Queue is locked", err.Message)
	assert.Equal(t, "test-queue", err.QueueName)
	assert.Equal(t, "queue.delete", err.Method)
}

func TestQueueNotFoundError(t *testing.T) {
	err := NewQueueNotFound("missing-queue", "basic.consume")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, "missing-queue", err.QueueName)
	assert.Contains(t, err.Message, "Queue 'missing-queue' not found")
}

func TestQueueInUseError(t *testing.T) {
	err := NewQueueInUse("busy-queue", "queue.delete")

	assert.Equal(t, ResourceLocked, err.Code)
	assert.Equal(t, "busy-queue", err.QueueName)
	assert.Contains(t, err.Message, "Queue 'busy-queue' is in use")
}

func TestQueueNotEmptyError(t *testing.T) {
	err := NewQueueNotEmpty("full-queue", "queue.delete")

	assert.Equal(t, PreconditionFailed, err.Code)
	assert.Equal(t, "full-queue", err.QueueName)
	assert.Contains(t, err.Message, "Queue 'full-queue' is not empty")
}

func TestConsumerError(t *testing.T) {
	err := NewConsumerError(NotFound, "Consumer not active", "consumer-123", "test-queue")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, "Consumer not active", err.Message)
	assert.Equal(t, "consumer-123", err.ConsumerTag)
	assert.Equal(t, "test-queue", err.QueueName)
}

func TestConsumerNotFoundError(t *testing.T) {
	err := NewConsumerNotFound("missing-consumer")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, "missing-consumer", err.ConsumerTag)
	assert.Contains(t, err.Message, "Consumer 'missing-consumer' not found")
}

func TestConsumerTagInUseError(t *testing.T) {
	err := NewConsumerTagInUse("duplicate-tag", "test-queue")

	assert.Equal(t, ResourceLocked, err.Code)
	assert.Equal(t, "duplicate-tag", err.ConsumerTag)
	assert.Equal(t, "test-queue", err.QueueName)
	assert.Contains(t, err.Message, "Consumer tag 'duplicate-tag' already in use")
}

func TestMessageError(t *testing.T) {
	err := NewMessageError(ContentTooLarge, "Message exceeds limit", 123, "test-queue")

	assert.Equal(t, ContentTooLarge, err.Code)
	assert.Equal(t, "Message exceeds limit", err.Message)
	assert.Equal(t, uint64(123), err.DeliveryTag)
	assert.Equal(t, "test-queue", err.QueueName)
}

func TestMessageTooLargeError(t *testing.T) {
	err := NewMessageTooLarge(2000, 1000, "test-queue")

	assert.Equal(t, ContentTooLarge, err.Code)
	assert.Equal(t, "test-queue", err.QueueName)
	assert.Contains(t, err.Message, "2000 bytes (max: 1000)")
}

func TestMessageNoRouteError(t *testing.T) {
	err := NewMessageNoRoute("test-exchange", "routing.key")

	assert.Equal(t, NoRoute, err.Code)
	assert.Contains(t, err.Message, "exchange=test-exchange, routing_key=routing.key")
}

func TestDeliveryTagNotFoundError(t *testing.T) {
	err := NewDeliveryTagNotFound(456, "test-queue")

	assert.Equal(t, NotFound, err.Code)
	assert.Equal(t, uint64(456), err.DeliveryTag)
	assert.Equal(t, "test-queue", err.QueueName)
	assert.Contains(t, err.Message, "Delivery tag 456 not found")
}

func TestProtocolError(t *testing.T) {
	err := NewProtocolError(FrameError, "Invalid frame format", 1, 10, 20)

	assert.Equal(t, FrameError, err.Code)
	assert.Equal(t, "Invalid frame format", err.Message)
	assert.Equal(t, byte(1), err.FrameType)
	assert.Equal(t, uint16(10), err.ClassID)
	assert.Equal(t, uint16(20), err.MethodID)
}

func TestFrameError(t *testing.T) {
	err := NewFrameError("malformed frame", 2)

	assert.Equal(t, FrameError, err.Code)
	assert.Equal(t, byte(2), err.FrameType)
	assert.Contains(t, err.Message, "malformed frame")
}

func TestSyntaxError(t *testing.T) {
	err := NewSyntaxError("invalid method arguments")

	assert.Equal(t, SyntaxError, err.Code)
	assert.Contains(t, err.Message, "invalid method arguments")
}

func TestUnexpectedFrameError(t *testing.T) {
	err := NewUnexpectedFrame(1, 3)

	assert.Equal(t, UnexpectedFrame, err.Code)
	assert.Equal(t, byte(3), err.FrameType)
	assert.Contains(t, err.Message, "expected 1, got 3")
}

func TestStorageError(t *testing.T) {
	cause := errors.New("disk full")
	err := NewStorageError(ResourceError, "Storage operation failed", "write", "messages.db", cause)

	assert.Equal(t, ResourceError, err.Code)
	assert.Equal(t, "Storage operation failed", err.Message)
	assert.Equal(t, "write", err.Operation)
	assert.Equal(t, "messages.db", err.Resource)
	assert.Equal(t, cause, err.Cause)
}

func TestStorageUnavailableError(t *testing.T) {
	cause := errors.New("connection timeout")
	err := NewStorageUnavailable("read", "queue.db", cause)

	assert.Equal(t, ResourceError, err.Code)
	assert.Equal(t, "read", err.Operation)
	assert.Equal(t, "queue.db", err.Resource)
	assert.Equal(t, cause, err.Unwrap())
	assert.Contains(t, err.Message, "Storage unavailable")
}

func TestStorageCorruptionError(t *testing.T) {
	cause := errors.New("checksum mismatch")
	err := NewStorageCorruption("read", "exchanges.db", cause)

	assert.Equal(t, InternalError, err.Code)
	assert.Equal(t, "read", err.Operation)
	assert.Equal(t, "exchanges.db", err.Resource)
	assert.Contains(t, err.Message, "corruption detected")
}

func TestAuthError(t *testing.T) {
	err := NewAuthError(AccessRefused, "Permission denied", "john", "queue.test", "read")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "Permission denied", err.Message)
	assert.Equal(t, "john", err.Username)
	assert.Equal(t, "queue.test", err.Resource)
	assert.Equal(t, "read", err.Operation)
}

func TestAuthenticationFailedError(t *testing.T) {
	err := NewAuthenticationFailed("jane", "invalid password")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "jane", err.Username)
	assert.Contains(t, err.Message, "Authentication failed for user 'jane'")
	assert.Contains(t, err.Message, "invalid password")
}

func TestAuthorizationFailedError(t *testing.T) {
	err := NewAuthorizationFailed("bob", "exchange.logs", "configure")

	assert.Equal(t, AccessRefused, err.Code)
	assert.Equal(t, "bob", err.Username)
	assert.Equal(t, "exchange.logs", err.Resource)
	assert.Equal(t, "configure", err.Operation)
	assert.Contains(t, err.Message, "not authorized for configure on exchange.logs")
}

func TestConfigError(t *testing.T) {
	cause := errors.New("file not found")
	err := NewConfigError("Invalid configuration", "network", "port", cause)

	assert.Equal(t, InternalError, err.Code)
	assert.Equal(t, "Invalid configuration", err.Message)
	assert.Equal(t, "network", err.Section)
	assert.Equal(t, "port", err.Key)
	assert.Equal(t, cause, err.Unwrap())
}

func TestConfigValidationError(t *testing.T) {
	err := NewConfigValidationError("security", "tls_cert_file", "file does not exist")

	assert.Equal(t, InternalError, err.Code)
	assert.Equal(t, "security", err.Section)
	assert.Equal(t, "tls_cert_file", err.Key)
	assert.Contains(t, err.Message, "Configuration validation failed")
	assert.Contains(t, err.Message, "file does not exist")
}

func TestIsConnectionError(t *testing.T) {
	connErr := NewConnectionForced("conn-123", "shutdown")
	chanErr := NewChannelError(NotFound, "not found", "conn-123", 1)
	genericErr := errors.New("generic error")

	assert.True(t, IsConnectionError(connErr))
	assert.False(t, IsConnectionError(chanErr))
	assert.False(t, IsConnectionError(genericErr))
}

func TestIsChannelError(t *testing.T) {
	chanErr := NewChannelError(NotFound, "not found", "conn-123", 1)
	connErr := NewConnectionForced("conn-123", "shutdown")
	genericErr := errors.New("generic error")

	assert.True(t, IsChannelError(chanErr))
	assert.False(t, IsChannelError(connErr))
	assert.False(t, IsChannelError(genericErr))
}

func TestIsNotFound(t *testing.T) {
	notFoundErr := NewQueueNotFound("test-queue", "basic.consume")
	notFound2Err := &AMQPError{Code: NotFound2, Message: "not found"}
	accessErr := NewAccessRefused("conn-123", "denied")
	genericErr := errors.New("generic error")

	assert.True(t, IsNotFound(notFoundErr))
	assert.True(t, IsNotFound(notFound2Err))
	assert.False(t, IsNotFound(accessErr))
	assert.False(t, IsNotFound(genericErr))
}

func TestIsPreconditionFailed(t *testing.T) {
	precondErr := NewQueueNotEmpty("test-queue", "queue.delete")
	precond2Err := &AMQPError{Code: PreconditionFailed2, Message: "precondition failed"}
	notFoundErr := NewQueueNotFound("test-queue", "basic.consume")
	genericErr := errors.New("generic error")

	assert.True(t, IsPreconditionFailed(precondErr))
	assert.True(t, IsPreconditionFailed(precond2Err))
	assert.False(t, IsPreconditionFailed(notFoundErr))
	assert.False(t, IsPreconditionFailed(genericErr))
}

func TestIsAccessRefused(t *testing.T) {
	accessErr := NewAccessRefused("conn-123", "denied")
	access2Err := &AMQPError{Code: AccessRefused2, Message: "access denied"}
	notFoundErr := NewQueueNotFound("test-queue", "basic.consume")
	genericErr := errors.New("generic error")

	assert.True(t, IsAccessRefused(accessErr))
	assert.True(t, IsAccessRefused(access2Err))
	assert.False(t, IsAccessRefused(notFoundErr))
	assert.False(t, IsAccessRefused(genericErr))
}

func TestGetErrorCode(t *testing.T) {
	amqpErr := NewQueueNotFound("test-queue", "basic.consume")
	genericErr := errors.New("generic error")

	assert.Equal(t, NotFound, GetErrorCode(amqpErr))
	assert.Equal(t, 0, GetErrorCode(genericErr))
}

func TestWrappedErrors(t *testing.T) {
	cause := errors.New("root cause")
	storageErr := NewStorageUnavailable("write", "db.file", cause)

	// Test error wrapping
	assert.True(t, errors.Is(storageErr, cause))

	// Test error unwrapping
	unwrapped := errors.Unwrap(storageErr)
	assert.Equal(t, cause, unwrapped)

	// Test errors.As
	var amqpErr *AMQPError
	if assert.True(t, errors.As(storageErr, &amqpErr)) {
		assert.Equal(t, ResourceError, amqpErr.Code)
	}
}

func TestErrorChaining(t *testing.T) {
	rootCause := errors.New("database connection failed")

	// Create a chain: generic -> storage -> AMQP
	storageErr := NewStorageUnavailable("connect", "main.db", rootCause)
	wrapperErr := fmt.Errorf("broker initialization failed: %w", storageErr)

	// Should be able to find the root cause
	assert.True(t, errors.Is(wrapperErr, rootCause))

	// Should be able to find storage error
	var sErr *StorageError
	assert.True(t, errors.As(wrapperErr, &sErr))
	assert.Equal(t, "connect", sErr.Operation)

	// Should be able to find AMQP error
	var amqpErr *AMQPError
	if assert.True(t, errors.As(wrapperErr, &amqpErr)) {
		assert.Equal(t, ResourceError, amqpErr.Code)
	}
}
