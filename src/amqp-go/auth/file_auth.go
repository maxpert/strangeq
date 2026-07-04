package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"

	amqperrors "github.com/maxpert/amqp-go/errors"
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
			return fmt.Errorf("auth file does not exist: %s", f.filePath)
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
		// Migrate old permissions format to new vhost_permissions format.
		// Faithfully map each legacy Action to the corresponding permission field
		// using its Pattern. Actions absent from the legacy list default to "^$"
		// (deny all). Unknown actions are logged and treated as deny.
		if len(user.VHostPermissions) == 0 && len(user.LegacyPermissions) > 0 {
			migrated := interfaces.Permission{Configure: "^$", Write: "^$", Read: "^$"}
			for _, lp := range user.LegacyPermissions {
				switch lp.Action {
				case "configure":
					migrated.Configure = lp.Pattern
				case "write":
					migrated.Write = lp.Pattern
				case "read":
					migrated.Read = lp.Pattern
				default:
					log.Printf("[auth] WARNING: unknown legacy permission action %q for user %q, ignoring", lp.Action, user.Username)
				}
			}
			user.VHostPermissions = []interfaces.VHostPermission{
				{VHost: "/", Permission: migrated},
			}
			log.Printf("[auth] WARNING: migrated legacy permissions for user %q to vhost_permissions on / — please update auth file", user.Username)
		}
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

	// Create user object (clone slices to avoid sharing backing arrays with UserEntry)
	user := &interfaces.User{
		Username:         entry.Username,
		VHostPermissions: slices.Clone(entry.VHostPermissions),
		Tags:             slices.Clone(entry.Tags),
		Groups:           slices.Clone(entry.Groups),
		LoopbackOnly:     entry.LoopbackOnly,
		Metadata:         entry.Metadata,
	}

	return user, nil
}

// Authorize checks if a user has permission for an operation per the
// RabbitMQ permission model. Each user has a per-vhost permission triple
// (configure/write/read) of regex patterns. The resource name is matched
// against the appropriate pattern based on the operation action.
//
// Returns nil if authorized, or an *amqperrors.AuthError with code 403
// (access-refused) if not.
func (f *FileAuthenticator) Authorize(user *interfaces.User, operation interfaces.Operation) error {
	if user == nil {
		return amqperrors.NewAuthError(amqperrors.AccessRefused, "cannot authorize nil user", "", "", string(operation.Action))
	}

	// Hold a read lock on the user's mutable fields to prevent concurrent
	// RefreshUser from mutating them mid-check.
	user.RLock()
	defer user.RUnlock()

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
		return amqperrors.NewAuthError(amqperrors.AccessRefused,
			fmt.Sprintf("access to vhost %s refused for user %s", operation.VHost, user.Username),
			user.Username, operation.VHost, string(operation.Action))
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
		return amqperrors.NewAuthorizationFailed(
			user.Username,
			fmt.Sprintf("%s %s", operation.ResourceType, operation.Resource),
			fmt.Sprintf("%s in vhost %s", operation.Action, operation.VHost),
		)
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
		VHostPermissions: slices.Clone(entry.VHostPermissions),
		Tags:             slices.Clone(entry.Tags),
		Groups:           slices.Clone(entry.Groups),
		LoopbackOnly:     entry.LoopbackOnly,
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

	// Hold a write lock while updating mutable fields to prevent concurrent
	// Authorize from reading partially-updated data.
	user.Lock()
	defer user.Unlock()
	user.VHostPermissions = updated.VHostPermissions
	user.Tags = updated.Tags
	user.Groups = updated.Groups
	user.LoopbackOnly = updated.LoopbackOnly
	user.Metadata = updated.Metadata

	return nil
}

// Reload reloads the auth file from disk
func (f *FileAuthenticator) Reload() error {
	return f.load()
}
