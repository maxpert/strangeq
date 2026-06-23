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
	Username         string                       `json:"username"`
	PasswordHash     string                       `json:"password_hash"` // bcrypt hash
	VHostPermissions []interfaces.VHostPermission `json:"vhost_permissions"`
	Tags             []string                     `json:"tags"`
	Groups           []string                     `json:"groups"`
	LoopbackOnly     bool                         `json:"loopback_only"`
	Metadata         map[string]interface{}       `json:"metadata"`
	// LegacyPermissions is the old permission format (resource/action/pattern).
	// Migrated to VHostPermissions on load for backward compatibility.
	LegacyPermissions []legacyPermission `json:"permissions,omitempty"`
}

// legacyPermission is the old permission format for backward compatibility.
type legacyPermission struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
	Pattern  string `json:"pattern"`
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
		// Migrate old permissions format to new vhost_permissions format
		if len(user.VHostPermissions) == 0 && len(user.LegacyPermissions) > 0 {
			user.VHostPermissions = []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			}
		}
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
				Tags:         []string{"administrator"},
				Groups:       []string{"guest"},
				LoopbackOnly: true,
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
		Username:         entry.Username,
		VHostPermissions: entry.VHostPermissions,
		Tags:             entry.Tags,
		Groups:           entry.Groups,
		Metadata:         entry.Metadata,
	}

	return user, nil
}

// Authorize checks if a user has permission for an operation per the
// RabbitMQ permission model. Each user has a per-vhost permission triple
// (configure/write/read) of regex patterns. The resource name is matched
// against the appropriate pattern based on the operation action.
//
// Returns nil if authorized, or a non-nil error (ErrAccessRefused) if not.
func (f *FileAuthenticator) Authorize(user *interfaces.User, operation interfaces.Operation) error {
	if user == nil {
		return fmt.Errorf("cannot authorize nil user")
	}

	// Find the user's VHostPermission for the operation's vhost.
	// If the user has no permission entry for this vhost, refuse access.
	var vhostPerm *interfaces.VHostPermission
	for i := range user.VHostPermissions {
		if user.VHostPermissions[i].VHost == operation.VHost {
			vhostPerm = &user.VHostPermissions[i]
			break
		}
	}
	if vhostPerm == nil {
		return fmt.Errorf("access to vhost %s refused for user %s", operation.VHost, user.Username)
	}

	// Normalize the default exchange name (empty → "amq.default")
	// for permission checks, per RabbitMQ behavior.
	resourceName := operation.Resource
	if operation.ResourceType == interfaces.ResourceExchange {
		resourceName = interfaces.NormalizeExchangeName(resourceName)
	}

	// Match the resource name against the appropriate regex pattern
	// for the operation's action (configure/write/read).
	if !vhostPerm.Permission.Matches(operation.Action, resourceName) {
		return fmt.Errorf("access to %s %s refused for user %s in vhost %s",
			operation.ResourceType, operation.Resource, user.Username, operation.VHost)
	}

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
		Username:         entry.Username,
		VHostPermissions: entry.VHostPermissions,
		Tags:             entry.Tags,
		Groups:           entry.Groups,
		Metadata:         entry.Metadata,
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

	user.VHostPermissions = updated.VHostPermissions
	user.Tags = updated.Tags
	user.Groups = updated.Groups
	user.Metadata = updated.Metadata

	return nil
}

// Reload reloads the auth file from disk
func (f *FileAuthenticator) Reload() error {
	return f.load()
}
