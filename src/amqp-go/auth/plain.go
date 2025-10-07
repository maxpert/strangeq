package auth

import (
	"bytes"
	"fmt"

	"github.com/maxpert/amqp-go/interfaces"
)

// PlainMechanism implements SASL PLAIN authentication
type PlainMechanism struct{}

// Name returns the mechanism name
func (p *PlainMechanism) Name() string {
	return "PLAIN"
}

// Authenticate authenticates a user using PLAIN mechanism
// PLAIN response format: \0username\0password
// The response is a sequence of three strings separated by NUL (0x00) bytes:
// [authorization-identity] NUL [authentication-identity] NUL [password]
// We use the authentication-identity as the username
func (p *PlainMechanism) Authenticate(response []byte, authenticator interfaces.Authenticator) (*interfaces.User, error) {
	if authenticator == nil {
		return nil, fmt.Errorf("authenticator is nil")
	}

	if len(response) == 0 {
		return nil, fmt.Errorf("empty authentication response")
	}

	// Split response by NUL bytes
	parts := bytes.Split(response, []byte{0})

	// We expect 3 parts: [authz-id] [authc-id] [password]
	// authz-id can be empty, authc-id and password should not be
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid PLAIN response format: expected 3 parts, got %d", len(parts))
	}

	// Extract username (authentication identity) and password
	username := string(parts[1])
	password := string(parts[2])

	if username == "" {
		return nil, fmt.Errorf("username cannot be empty")
	}

	if password == "" {
		return nil, fmt.Errorf("password cannot be empty")
	}

	// Authenticate using the provided authenticator
	user, err := authenticator.Authenticate(username, password)
	if err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	return user, nil
}
