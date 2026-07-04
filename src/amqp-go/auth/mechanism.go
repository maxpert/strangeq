package auth

import (
	"fmt"
	"strings"

	"github.com/maxpert/amqp-go/interfaces"
)

// Mechanism represents a SASL authentication mechanism
type Mechanism interface {
	// Name returns the mechanism name (e.g., "PLAIN", "ANONYMOUS")
	Name() string

	// Authenticate authenticates a user based on the SASL response
	Authenticate(response []byte, authenticator interfaces.Authenticator) (*interfaces.User, error)
}

// Registry manages available authentication mechanisms
type Registry struct {
	mechanisms map[string]Mechanism
}

// NewRegistry creates a new mechanism registry
func NewRegistry() *Registry {
	return &Registry{
		mechanisms: make(map[string]Mechanism),
	}
}

// Register adds a mechanism to the registry
func (r *Registry) Register(mechanism Mechanism) {
	r.mechanisms[mechanism.Name()] = mechanism
}

// Get retrieves a mechanism by name
func (r *Registry) Get(name string) (Mechanism, error) {
	mechanism, exists := r.mechanisms[name]
	if !exists {
		return nil, fmt.Errorf("unsupported authentication mechanism: %s", name)
	}
	return mechanism, nil
}

// List returns all registered mechanism names
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.mechanisms))
	for name := range r.mechanisms {
		names = append(names, name)
	}
	return names
}

// String returns a space-separated list of mechanism names for AMQP
func (r *Registry) String() string {
	return strings.Join(r.List(), " ")
}

// DefaultRegistry returns a registry with only the PLAIN mechanism.
// ANONYMOUS is NOT included by default — it must be explicitly opted in
// via RegistryForMechanisms to avoid accidentally allowing unauthenticated
// remote connections.
func DefaultRegistry() *Registry {
	registry := NewRegistry()
	registry.Register(&PlainMechanism{})
	return registry
}

// RegistryForMechanisms returns a registry populated with the mechanisms
// named in the provided list. PLAIN is always registered. ANONYMOUS is
// registered only if "ANONYMOUS" appears in names (compared case-insensitively
// via strings.EqualFold). If names is empty, only PLAIN is registered.
func RegistryForMechanisms(names []string) *Registry {
	registry := NewRegistry()
	registry.Register(&PlainMechanism{})
	for _, name := range names {
		if strings.EqualFold(name, "ANONYMOUS") {
			registry.Register(&AnonymousMechanism{})
		}
	}
	return registry
}
