package auth

import (
	"fmt"
	"slices"
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

// List returns all registered mechanism names in sorted order. The order is
// deterministic so that the wire-level mechanism advertisement (String) is
// stable across restarts.
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.mechanisms))
	for name := range r.mechanisms {
		names = append(names, name)
	}
	slices.Sort(names)
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
// named in the provided list, compared case-insensitively. Unknown names are
// skipped. If names is empty, only PLAIN is registered.
func RegistryForMechanisms(names []string) *Registry {
	registry, _ := RegistryForMechanismsReport(names)
	return registry
}

// RegistryForMechanismsReport behaves like RegistryForMechanisms and also
// returns the names that matched no known mechanism, so callers can warn the
// operator instead of silently ignoring a typo. The configured list is
// authoritative: a list that omits PLAIN yields a registry without PLAIN.
func RegistryForMechanismsReport(names []string) (*Registry, []string) {
	registry := NewRegistry()
	var unknown []string
	for _, name := range names {
		switch {
		case strings.EqualFold(name, "PLAIN"):
			registry.Register(&PlainMechanism{})
		case strings.EqualFold(name, "ANONYMOUS"):
			registry.Register(&AnonymousMechanism{})
		default:
			unknown = append(unknown, name)
		}
	}
	if len(names) == 0 {
		registry.Register(&PlainMechanism{})
	}
	return registry, unknown
}
