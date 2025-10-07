package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/maxpert/amqp-go/interfaces"
	"golang.org/x/crypto/bcrypt"
)

// FileAuthenticator implements file-based authentication
type FileAuthenticator struct {
	filePath string
	users    map[string]*UserEntry
	mutex    sync.RWMutex
}

// UserEntry represents a user entry in the auth file
type UserEntry struct {
	Username     string                  `json:"username"`
	PasswordHash string                  `json:"password_hash"` // bcrypt hash
	Permissions  []interfaces.Permission `json:"permissions"`
	Groups       []string                `json:"groups"`
	Metadata     map[string]interface{}  `json:"metadata"`
}

// AuthFile represents the structure of the auth file
type AuthFile struct {
	Users []UserEntry `json:"users"`
}

// NewFileAuthenticator creates a new file-based authenticator
func NewFileAuthenticator(filePath string) (*FileAuthenticator, error) {
	auth := &FileAuthenticator{
		filePath: filePath,
		users:    make(map[string]*UserEntry),
	}

	if err := auth.load(); err != nil {
		return nil, fmt.Errorf("failed to load auth file: %w", err)
	}

	return auth, nil
}

// load reads and parses the auth file
func (f *FileAuthenticator) load() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	data, err := os.ReadFile(f.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Create default auth file with guest user
			return f.createDefaultFile()
		}
		return fmt.Errorf("failed to read auth file: %w", err)
	}

	var authFile AuthFile
	if err := json.Unmarshal(data, &authFile); err != nil {
		return fmt.Errorf("failed to parse auth file: %w", err)
	}

	// Build user map
	f.users = make(map[string]*UserEntry)
	for i := range authFile.Users {
		user := &authFile.Users[i]
		f.users[user.Username] = user
	}

	return nil
}

// createDefaultFile creates a default auth file with guest user
func (f *FileAuthenticator) createDefaultFile() error {
	// Create bcrypt hash for "guest" password
	hash, err := bcrypt.GenerateFromPassword([]byte("guest"), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash default password: %w", err)
	}

	defaultFile := AuthFile{
		Users: []UserEntry{
			{
				Username:     "guest",
				PasswordHash: string(hash),
				Permissions: []interfaces.Permission{
					{
						Resource: ".*",
						Action:   "read",
						Pattern:  ".*",
					},
					{
						Resource: ".*",
						Action:   "write",
						Pattern:  ".*",
					},
					{
						Resource: ".*",
						Action:   "configure",
						Pattern:  ".*",
					},
				},
				Groups: []string{"guest"},
				Metadata: map[string]interface{}{
					"created": "default",
				},
			},
		},
	}

	data, err := json.MarshalIndent(defaultFile, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default auth file: %w", err)
	}

	if err := os.WriteFile(f.filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write default auth file: %w", err)
	}

	// Load the default file
	f.users = make(map[string]*UserEntry)
	for i := range defaultFile.Users {
		user := &defaultFile.Users[i]
		f.users[user.Username] = user
	}

	return nil
}

// Authenticate validates user credentials
func (f *FileAuthenticator) Authenticate(username, password string) (*interfaces.User, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	entry, exists := f.users[username]
	if !exists {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	// Verify password using bcrypt
	if err := bcrypt.CompareHashAndPassword([]byte(entry.PasswordHash), []byte(password)); err != nil {
		return nil, fmt.Errorf("invalid password")
	}

	// Create user object
	user := &interfaces.User{
		Username:    entry.Username,
		Permissions: entry.Permissions,
		Groups:      entry.Groups,
		Metadata:    entry.Metadata,
	}

	return user, nil
}

// Authorize checks if a user has permission for an operation
func (f *FileAuthenticator) Authorize(user *interfaces.User, operation interfaces.Operation) error {
	// TODO: Implement permission checking logic
	// For now, allow all operations for authenticated users
	return nil
}

// GetUser retrieves user information
func (f *FileAuthenticator) GetUser(username string) (*interfaces.User, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	entry, exists := f.users[username]
	if !exists {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	user := &interfaces.User{
		Username:    entry.Username,
		Permissions: entry.Permissions,
		Groups:      entry.Groups,
		Metadata:    entry.Metadata,
	}

	return user, nil
}

// RefreshUser updates user information from the backing store
func (f *FileAuthenticator) RefreshUser(user *interfaces.User) error {
	if err := f.load(); err != nil {
		return fmt.Errorf("failed to reload auth file: %w", err)
	}

	updated, err := f.GetUser(user.Username)
	if err != nil {
		return err
	}

	user.Permissions = updated.Permissions
	user.Groups = updated.Groups
	user.Metadata = updated.Metadata

	return nil
}

// Reload reloads the auth file from disk
func (f *FileAuthenticator) Reload() error {
	return f.load()
}
