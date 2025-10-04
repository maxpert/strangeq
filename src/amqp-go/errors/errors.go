package errors

import (
	"errors"
	"fmt"
)

// AMQPError represents a general AMQP error
type AMQPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Method  string `json:"method,omitempty"`
	Cause   error  `json:"cause,omitempty"`
}

func (e *AMQPError) Error() string {
	if e.Method != "" {
		return fmt.Sprintf("AMQP Error %d in %s: %s", e.Code, e.Method, e.Message)
	}
	return fmt.Sprintf("AMQP Error %d: %s", e.Code, e.Message)
}

func (e *AMQPError) Unwrap() error {
	return e.Cause
}

func (e *AMQPError) As(target interface{}) bool {
	if amqpErr, ok := target.(**AMQPError); ok {
		*amqpErr = e
		return true
	}
	return false
}

// AMQP Error Codes (from AMQP 0.9.1 specification)
const (
	// Connection errors
	ConnectionForced    = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406
	
	// Channel errors
	ContentTooLarge    = 311
	NoRoute           = 312
	NoConsumers       = 313
	ConnectionForced2 = 320
	InvalidPath2      = 402
	AccessRefused2    = 403
	NotFound2         = 404
	ResourceLocked2   = 405
	PreconditionFailed2 = 406
	
	// Frame errors
	FrameError        = 501
	SyntaxError       = 502
	CommandInvalid    = 503
	ChannelErrorCode  = 504
	UnexpectedFrame   = 505
	ResourceError     = 506
	NotAllowed        = 530
	NotImplemented    = 540
	InternalError     = 541
)

// Connection Errors

// ConnectionError represents connection-specific errors
type ConnectionError struct {
	AMQPError
	ConnectionID string `json:"connection_id,omitempty"`
}

func NewConnectionError(code int, message, connectionID string) *ConnectionError {
	return &ConnectionError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		ConnectionID: connectionID,
	}
}

func NewConnectionForced(connectionID, reason string) *ConnectionError {
	return NewConnectionError(ConnectionForced, fmt.Sprintf("Connection forced closed: %s", reason), connectionID)
}

func NewAccessRefused(connectionID, reason string) *ConnectionError {
	return NewConnectionError(AccessRefused, fmt.Sprintf("Access refused: %s", reason), connectionID)
}

func (e *ConnectionError) As(target interface{}) bool {
	if amqpErr, ok := target.(**AMQPError); ok {
		*amqpErr = &e.AMQPError
		return true
	}
	return false
}

// Channel Errors

// ChannelError represents channel-specific errors
type ChannelError struct {
	AMQPError
	ConnectionID string `json:"connection_id,omitempty"`
	ChannelID    uint16 `json:"channel_id"`
}

func NewChannelError(code int, message, connectionID string, channelID uint16) *ChannelError {
	return &ChannelError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		ConnectionID: connectionID,
		ChannelID:    channelID,
	}
}

func NewChannelNotFound(connectionID string, channelID uint16) *ChannelError {
	return NewChannelError(NotFound, fmt.Sprintf("Channel %d not found", channelID), connectionID, channelID)
}

func NewChannelPreconditionFailed(connectionID string, channelID uint16, reason string) *ChannelError {
	return NewChannelError(PreconditionFailed, fmt.Sprintf("Precondition failed: %s", reason), connectionID, channelID)
}

// Exchange Errors

// ExchangeError represents exchange-specific errors
type ExchangeError struct {
	AMQPError
	ExchangeName string `json:"exchange_name"`
}

func NewExchangeError(code int, message, exchangeName, method string) *ExchangeError {
	return &ExchangeError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
			Method:  method,
		},
		ExchangeName: exchangeName,
	}
}

func NewExchangeNotFound(exchangeName, method string) *ExchangeError {
	return NewExchangeError(NotFound, fmt.Sprintf("Exchange '%s' not found", exchangeName), exchangeName, method)
}

func NewExchangeTypeMismatch(exchangeName, expectedType, actualType, method string) *ExchangeError {
	message := fmt.Sprintf("Exchange '%s' type mismatch: expected %s, got %s", exchangeName, expectedType, actualType)
	return NewExchangeError(PreconditionFailed, message, exchangeName, method)
}

// Queue Errors

// QueueError represents queue-specific errors
type QueueError struct {
	AMQPError
	QueueName string `json:"queue_name"`
	Method    string `json:"method"`
}

func NewQueueError(code int, message, queueName, method string) *QueueError {
	return &QueueError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
			Method:  method,
		},
		QueueName: queueName,
		Method:    method,
	}
}

func NewQueueNotFound(queueName, method string) *QueueError {
	return NewQueueError(NotFound, fmt.Sprintf("Queue '%s' not found", queueName), queueName, method)
}

func NewQueueInUse(queueName, method string) *QueueError {
	return NewQueueError(ResourceLocked, fmt.Sprintf("Queue '%s' is in use", queueName), queueName, method)
}

func NewQueueNotEmpty(queueName, method string) *QueueError {
	return NewQueueError(PreconditionFailed, fmt.Sprintf("Queue '%s' is not empty", queueName), queueName, method)
}

func (e *QueueError) As(target interface{}) bool {
	if amqpErr, ok := target.(**AMQPError); ok {
		*amqpErr = &e.AMQPError
		return true
	}
	return false
}

// Consumer Errors

// ConsumerError represents consumer-specific errors
type ConsumerError struct {
	AMQPError
	ConsumerTag string `json:"consumer_tag"`
	QueueName   string `json:"queue_name,omitempty"`
}

func NewConsumerError(code int, message, consumerTag, queueName string) *ConsumerError {
	return &ConsumerError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		ConsumerTag: consumerTag,
		QueueName:   queueName,
	}
}

func NewConsumerNotFound(consumerTag string) *ConsumerError {
	return NewConsumerError(NotFound, fmt.Sprintf("Consumer '%s' not found", consumerTag), consumerTag, "")
}

func NewConsumerTagInUse(consumerTag, queueName string) *ConsumerError {
	return NewConsumerError(ResourceLocked, fmt.Sprintf("Consumer tag '%s' already in use", consumerTag), consumerTag, queueName)
}

// Message Errors

// MessageError represents message-specific errors
type MessageError struct {
	AMQPError
	DeliveryTag uint64 `json:"delivery_tag,omitempty"`
	QueueName   string `json:"queue_name,omitempty"`
}

func NewMessageError(code int, message string, deliveryTag uint64, queueName string) *MessageError {
	return &MessageError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		DeliveryTag: deliveryTag,
		QueueName:   queueName,
	}
}

func NewMessageTooLarge(size, maxSize int, queueName string) *MessageError {
	message := fmt.Sprintf("Message too large: %d bytes (max: %d)", size, maxSize)
	return NewMessageError(ContentTooLarge, message, 0, queueName)
}

func NewMessageNoRoute(exchange, routingKey string) *MessageError {
	message := fmt.Sprintf("No route for message: exchange=%s, routing_key=%s", exchange, routingKey)
	return NewMessageError(NoRoute, message, 0, "")
}

func NewDeliveryTagNotFound(deliveryTag uint64, queueName string) *MessageError {
	message := fmt.Sprintf("Delivery tag %d not found", deliveryTag)
	return NewMessageError(NotFound, message, deliveryTag, queueName)
}

// Protocol Errors

// ProtocolError represents protocol-specific errors
type ProtocolError struct {
	AMQPError
	FrameType byte   `json:"frame_type,omitempty"`
	ClassID   uint16 `json:"class_id,omitempty"`
	MethodID  uint16 `json:"method_id,omitempty"`
}

func NewProtocolError(code int, message string, frameType byte, classID, methodID uint16) *ProtocolError {
	return &ProtocolError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		FrameType: frameType,
		ClassID:   classID,
		MethodID:  methodID,
	}
}

func NewFrameError(message string, frameType byte) *ProtocolError {
	return NewProtocolError(FrameError, fmt.Sprintf("Frame error: %s", message), frameType, 0, 0)
}

func NewSyntaxError(message string) *ProtocolError {
	return NewProtocolError(SyntaxError, fmt.Sprintf("Syntax error: %s", message), 0, 0, 0)
}

func NewUnexpectedFrame(expected, actual byte) *ProtocolError {
	message := fmt.Sprintf("Unexpected frame: expected %d, got %d", expected, actual)
	return NewProtocolError(UnexpectedFrame, message, actual, 0, 0)
}

// Storage Errors

// StorageError represents storage-specific errors
type StorageError struct {
	AMQPError
	Operation string `json:"operation"`
	Resource  string `json:"resource"`
}

func NewStorageError(code int, message, operation, resource string, cause error) *StorageError {
	return &StorageError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
			Cause:   cause,
		},
		Operation: operation,
		Resource:  resource,
	}
}

func NewStorageUnavailable(operation, resource string, cause error) *StorageError {
	message := fmt.Sprintf("Storage unavailable for %s on %s", operation, resource)
	return NewStorageError(ResourceError, message, operation, resource, cause)
}

func NewStorageCorruption(operation, resource string, cause error) *StorageError {
	message := fmt.Sprintf("Storage corruption detected during %s on %s", operation, resource)
	return NewStorageError(InternalError, message, operation, resource, cause)
}

func (e *StorageError) As(target interface{}) bool {
	if amqpErr, ok := target.(**AMQPError); ok {
		*amqpErr = &e.AMQPError
		return true
	}
	return false
}

// Authentication Errors

// AuthError represents authentication and authorization errors
type AuthError struct {
	AMQPError
	Username  string `json:"username,omitempty"`
	Resource  string `json:"resource,omitempty"`
	Operation string `json:"operation,omitempty"`
}

func NewAuthError(code int, message, username, resource, operation string) *AuthError {
	return &AuthError{
		AMQPError: AMQPError{
			Code:    code,
			Message: message,
		},
		Username:  username,
		Resource:  resource,
		Operation: operation,
	}
}

func NewAuthenticationFailed(username, reason string) *AuthError {
	message := fmt.Sprintf("Authentication failed for user '%s': %s", username, reason)
	return NewAuthError(AccessRefused, message, username, "", "")
}

func NewAuthorizationFailed(username, resource, operation string) *AuthError {
	message := fmt.Sprintf("User '%s' not authorized for %s on %s", username, operation, resource)
	return NewAuthError(AccessRefused, message, username, resource, operation)
}

// Configuration Errors

// ConfigError represents configuration-specific errors
type ConfigError struct {
	AMQPError
	Section string `json:"section"`
	Key     string `json:"key,omitempty"`
}

func NewConfigError(message, section, key string, cause error) *ConfigError {
	return &ConfigError{
		AMQPError: AMQPError{
			Code:    InternalError,
			Message: message,
			Cause:   cause,
		},
		Section: section,
		Key:     key,
	}
}

func NewConfigValidationError(section, key, reason string) *ConfigError {
	message := fmt.Sprintf("Configuration validation failed for %s.%s: %s", section, key, reason)
	return NewConfigError(message, section, key, nil)
}

// Helper functions for common error checking

// IsConnectionError checks if an error is a ConnectionError
func IsConnectionError(err error) bool {
	var connErr *ConnectionError
	return errors.As(err, &connErr)
}

// IsChannelError checks if an error is a ChannelError
func IsChannelError(err error) bool {
	var chanErr *ChannelError
	return errors.As(err, &chanErr)
}

// IsNotFound checks if an error indicates a resource was not found
func IsNotFound(err error) bool {
	var amqpErr *AMQPError
	if errors.As(err, &amqpErr) {
		return amqpErr.Code == NotFound || amqpErr.Code == NotFound2
	}
	return false
}

// IsPreconditionFailed checks if an error indicates a precondition failed
func IsPreconditionFailed(err error) bool {
	var amqpErr *AMQPError
	if errors.As(err, &amqpErr) {
		return amqpErr.Code == PreconditionFailed || amqpErr.Code == PreconditionFailed2
	}
	return false
}

// IsAccessRefused checks if an error indicates access was refused
func IsAccessRefused(err error) bool {
	var amqpErr *AMQPError
	if errors.As(err, &amqpErr) {
		return amqpErr.Code == AccessRefused || amqpErr.Code == AccessRefused2
	}
	return false
}

// GetErrorCode returns the AMQP error code if the error is an AMQPError
func GetErrorCode(err error) int {
	var amqpErr *AMQPError
	if errors.As(err, &amqpErr) {
		return amqpErr.Code
	}
	return 0
}