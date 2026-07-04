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
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406

	// Channel errors
	ContentTooLarge = 311
	NoRoute         = 312
	NoConsumers     = 313

	// Frame errors
	FrameError       = 501
	SyntaxError      = 502
	CommandInvalid   = 503
	ChannelErrorCode = 504
	UnexpectedFrame  = 505
	ResourceError    = 506
	NotAllowed       = 530
	NotImplemented   = 540
	InternalError    = 541
)

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

func NewAuthorizationFailed(username, resource, operation string) *AuthError {
	message := fmt.Sprintf("User '%s' not authorized for %s on %s", username, operation, resource)
	return NewAuthError(AccessRefused, message, username, resource, operation)
}

// IsAccessRefused checks if an error indicates access was refused
func IsAccessRefused(err error) bool {
	var amqpErr *AMQPError
	if errors.As(err, &amqpErr) {
		return amqpErr.Code == AccessRefused
	}
	return false
}
