package server

import (
	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/interfaces"
)

// MechanismAdapter adapts auth.Mechanism to server.Mechanism
type MechanismAdapter struct {
	mechanism auth.Mechanism
}

// NewMechanismAdapter creates a new mechanism adapter
func NewMechanismAdapter(mechanism auth.Mechanism) *MechanismAdapter {
	return &MechanismAdapter{mechanism: mechanism}
}

// Name returns the mechanism name
func (m *MechanismAdapter) Name() string {
	return m.mechanism.Name()
}

// Authenticate authenticates a user
func (m *MechanismAdapter) Authenticate(response []byte, authenticator interface{}) (interface{}, error) {
	// Type assert authenticator to interfaces.Authenticator
	auth, ok := authenticator.(interfaces.Authenticator)
	if !ok {
		// If no authenticator provided, use the mechanism directly
		return m.mechanism.Authenticate(response, nil)
	}
	return m.mechanism.Authenticate(response, auth)
}

// MechanismRegistryAdapter adapts auth.Registry to server.MechanismRegistry
type MechanismRegistryAdapter struct {
	registry *auth.Registry
}

// NewMechanismRegistryAdapter creates a new registry adapter
func NewMechanismRegistryAdapter(registry *auth.Registry) *MechanismRegistryAdapter {
	return &MechanismRegistryAdapter{registry: registry}
}

// String returns a space-separated list of mechanism names
func (r *MechanismRegistryAdapter) String() string {
	return r.registry.String()
}

// Get retrieves a mechanism by name
func (r *MechanismRegistryAdapter) Get(name string) (Mechanism, error) {
	mechanism, err := r.registry.Get(name)
	if err != nil {
		return nil, err
	}
	return NewMechanismAdapter(mechanism), nil
}

// List returns all registered mechanism names
func (r *MechanismRegistryAdapter) List() []string {
	return r.registry.List()
}
