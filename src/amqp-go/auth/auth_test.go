package auth

import (
	"fmt"
	"os"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"golang.org/x/crypto/bcrypt"
)

func TestPlainMechanism(t *testing.T) {
	plain := &PlainMechanism{}

	// Test mechanism name
	if plain.Name() != "PLAIN" {
		t.Errorf("Expected mechanism name 'PLAIN', got '%s'", plain.Name())
	}

	// Create a mock authenticator
	mockAuth := &MockAuthenticator{
		users: map[string]string{
			"testuser": "testpass",
		},
	}

	// Test successful authentication
	// PLAIN format: \0username\0password
	response := []byte{0, 't', 'e', 's', 't', 'u', 's', 'e', 'r', 0, 't', 'e', 's', 't', 'p', 'a', 's', 's'}
	user, err := plain.Authenticate(response, mockAuth)
	if err != nil {
		t.Errorf("Expected successful authentication, got error: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", user.Username)
	}

	// Test failed authentication (wrong password)
	response = []byte{0, 't', 'e', 's', 't', 'u', 's', 'e', 'r', 0, 'w', 'r', 'o', 'n', 'g', 'p', 'a', 's', 's'}
	_, err = plain.Authenticate(response, mockAuth)
	if err == nil {
		t.Error("Expected authentication to fail with wrong password")
	}

	// Test invalid format (not enough parts)
	response = []byte{'n', 'o', 'n', 'u', 'l', 's'}
	_, err = plain.Authenticate(response, mockAuth)
	if err == nil {
		t.Error("Expected authentication to fail with invalid format")
	}

	// Test empty username
	response = []byte{0, 0, 'p', 'a', 's', 's'}
	_, err = plain.Authenticate(response, mockAuth)
	if err == nil {
		t.Error("Expected authentication to fail with empty username")
	}

	// Test empty password
	response = []byte{0, 'u', 's', 'e', 'r', 0}
	_, err = plain.Authenticate(response, mockAuth)
	if err == nil {
		t.Error("Expected authentication to fail with empty password")
	}
}

func TestAnonymousMechanism(t *testing.T) {
	anonymous := &AnonymousMechanism{}

	// Test mechanism name
	if anonymous.Name() != "ANONYMOUS" {
		t.Errorf("Expected mechanism name 'ANONYMOUS', got '%s'", anonymous.Name())
	}

	// Test authentication (should always succeed)
	response := []byte("any response")
	user, err := anonymous.Authenticate(response, nil)
	if err != nil {
		t.Errorf("Expected successful authentication, got error: %v", err)
	}
	if user.Username != "guest" {
		t.Errorf("Expected username 'guest', got '%s'", user.Username)
	}

	// Test with empty response
	response = []byte{}
	user, err = anonymous.Authenticate(response, nil)
	if err != nil {
		t.Errorf("Expected successful authentication, got error: %v", err)
	}
	if user.Username != "guest" {
		t.Errorf("Expected username 'guest', got '%s'", user.Username)
	}
}

func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	// Test registration
	plain := &PlainMechanism{}
	registry.Register(plain)

	anonymous := &AnonymousMechanism{}
	registry.Register(anonymous)

	// Test retrieval
	mechanism, err := registry.Get("PLAIN")
	if err != nil {
		t.Errorf("Expected to find PLAIN mechanism, got error: %v", err)
	}
	if mechanism.Name() != "PLAIN" {
		t.Errorf("Expected mechanism name 'PLAIN', got '%s'", mechanism.Name())
	}

	mechanism, err = registry.Get("ANONYMOUS")
	if err != nil {
		t.Errorf("Expected to find ANONYMOUS mechanism, got error: %v", err)
	}
	if mechanism.Name() != "ANONYMOUS" {
		t.Errorf("Expected mechanism name 'ANONYMOUS', got '%s'", mechanism.Name())
	}

	// Test unknown mechanism
	_, err = registry.Get("UNKNOWN")
	if err == nil {
		t.Error("Expected error for unknown mechanism")
	}

	// Test list
	names := registry.List()
	if len(names) != 2 {
		t.Errorf("Expected 2 mechanisms, got %d", len(names))
	}

	// Test string representation
	str := registry.String()
	if str != "PLAIN ANONYMOUS" && str != "ANONYMOUS PLAIN" {
		t.Errorf("Expected 'PLAIN ANONYMOUS' or 'ANONYMOUS PLAIN', got '%s'", str)
	}
}

func TestDefaultRegistry(t *testing.T) {
	registry := DefaultRegistry()

	// DefaultRegistry now includes only PLAIN (ANONYMOUS is opt-in only)
	names := registry.List()
	if len(names) != 1 {
		t.Errorf("Expected 1 mechanism in default registry, got %d", len(names))
	}

	// Test PLAIN is available
	_, err := registry.Get("PLAIN")
	if err != nil {
		t.Error("Expected PLAIN mechanism in default registry")
	}

	// Test ANONYMOUS is NOT available by default
	_, err = registry.Get("ANONYMOUS")
	if err == nil {
		t.Error("ANONYMOUS should NOT be in default registry (opt-in only)")
	}

	// RegistryForMechanisms can opt in to ANONYMOUS
	regWithAnon := RegistryForMechanisms([]string{"PLAIN", "ANONYMOUS"})
	_, err = regWithAnon.Get("ANONYMOUS")
	if err != nil {
		t.Error("RegistryForMechanisms should include ANONYMOUS when explicitly listed")
	}
}

func TestFileAuthenticator(t *testing.T) {
	authFile := "/tmp/test_auth.json"
	defer os.Remove(authFile)

	hash, err := bcrypt.GenerateFromPassword([]byte("guest"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}
	authData := fmt.Sprintf(`{"users":[{"username":"guest","password_hash":%q,"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}],"tags":["administrator"],"groups":["guest"],"loopback_only":true}]}`, string(hash))
	if err := os.WriteFile(authFile, []byte(authData), 0600); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	auth, err := NewFileAuthenticator(authFile)
	if err != nil {
		t.Fatalf("Failed to create file authenticator: %v", err)
	}

	user, err := auth.Authenticate("guest", "guest")
	if err != nil {
		t.Errorf("Expected successful authentication with default guest user, got error: %v", err)
	}
	if user.Username != "guest" {
		t.Errorf("Expected username 'guest', got '%s'", user.Username)
	}

	_, err = auth.Authenticate("guest", "wrongpass")
	if err == nil {
		t.Error("Expected authentication to fail with wrong password")
	}

	_, err = auth.Authenticate("unknown", "password")
	if err == nil {
		t.Error("Expected authentication to fail with unknown user")
	}

	user, err = auth.GetUser("guest")
	if err != nil {
		t.Errorf("Expected to get guest user, got error: %v", err)
	}
	if user.Username != "guest" {
		t.Errorf("Expected username 'guest', got '%s'", user.Username)
	}

	_, err = auth.GetUser("unknown")
	if err == nil {
		t.Error("Expected error when getting unknown user")
	}

	err = auth.RefreshUser(user)
	if err != nil {
		t.Errorf("Expected successful refresh, got error: %v", err)
	}
}

func TestFileAuthenticator_MissingFileReturnsError(t *testing.T) {
	_, err := NewFileAuthenticator("/tmp/nonexistent_auth_file_12345.json")
	if err == nil {
		t.Error("NewFileAuthenticator must return error when auth file does not exist (no auto-create)")
	}
}

// MockAuthenticator is a mock authenticator for testing
type MockAuthenticator struct {
	users map[string]string // username -> password
}

func (m *MockAuthenticator) Authenticate(username, password string) (*interfaces.User, error) {
	expectedPass, exists := m.users[username]
	if !exists {
		return nil, interfaces.ErrUserNotFound
	}
	if expectedPass != password {
		return nil, interfaces.ErrInvalidCredentials
	}
	return &interfaces.User{
		Username: username,
		VHostPermissions: []interfaces.VHostPermission{
			{
				VHost: "/",
				Permission: interfaces.Permission{
					Configure: ".*",
					Write:     ".*",
					Read:      ".*",
				},
			},
		},
		Tags:     []string{},
		Groups:   []string{},
		Metadata: map[string]interface{}{},
	}, nil
}

func (m *MockAuthenticator) Authorize(user *interfaces.User, operation interfaces.Operation) error {
	return nil
}

func (m *MockAuthenticator) GetUser(username string) (*interfaces.User, error) {
	_, exists := m.users[username]
	if !exists {
		return nil, interfaces.ErrUserNotFound
	}
	return &interfaces.User{
		Username: username,
		VHostPermissions: []interfaces.VHostPermission{
			{
				VHost: "/",
				Permission: interfaces.Permission{
					Configure: ".*",
					Write:     ".*",
					Read:      ".*",
				},
			},
		},
		Tags:     []string{},
		Groups:   []string{},
		Metadata: map[string]interface{}{},
	}, nil
}

func (m *MockAuthenticator) RefreshUser(user *interfaces.User) error {
	return nil
}
