package auth

import (
	"github.com/maxpert/amqp-go/interfaces"
)

// AnonymousMechanism implements SASL ANONYMOUS authentication
// WARNING: This mechanism should only be enabled in development/testing environments
type AnonymousMechanism struct{}

// Name returns the mechanism name
func (a *AnonymousMechanism) Name() string {
	return "ANONYMOUS"
}

// Authenticate authenticates a user using ANONYMOUS mechanism
// This mechanism accepts any connection and creates a guest user
// The response field is ignored as per SASL ANONYMOUS specification
func (a *AnonymousMechanism) Authenticate(response []byte, authenticator interfaces.Authenticator) (*interfaces.User, error) {
	// Create a guest user with minimal permissions
	user := &interfaces.User{
		Username:    "guest",
		Permissions: []interfaces.Permission{},
		Groups:      []string{"guest"},
		Metadata: map[string]interface{}{
			"mechanism": "ANONYMOUS",
		},
	}

	return user, nil
}
