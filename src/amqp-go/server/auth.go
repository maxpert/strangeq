package server

// MechanismRegistry is an interface for authentication mechanism registries
type MechanismRegistry interface {
	// String returns a space-separated list of mechanism names
	String() string

	// Get retrieves a mechanism by name
	Get(name string) (Mechanism, error)

	// List returns all registered mechanism names
	List() []string
}

// Mechanism represents a SASL authentication mechanism
type Mechanism interface {
	// Name returns the mechanism name
	Name() string

	// Authenticate authenticates a user based on the SASL response
	Authenticate(response []byte, authenticator interface{}) (interface{}, error)
}
