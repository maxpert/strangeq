package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
)

// ============================================================================
// Permission.Matches tests — test the regex matching against the
// configure/write/read triple per the RabbitMQ authorization model.
// Each field is a regex pattern matched against resource names.
// ============================================================================

func TestPermissionMatchesConfigure(t *testing.T) {
	perm := interfaces.Permission{Configure: "^app1-.*", Write: ".*", Read: ".*"}

	if !perm.Matches(interfaces.ActionConfigure, "app1-queue") {
		t.Error("configure regex ^app1-.* should match 'app1-queue'")
	}
	if !perm.Matches(interfaces.ActionConfigure, "app1-exchange") {
		t.Error("configure regex ^app1-.* should match 'app1-exchange'")
	}
	if perm.Matches(interfaces.ActionConfigure, "app2-queue") {
		t.Error("configure regex ^app1-.* should NOT match 'app2-queue'")
	}
}

func TestPermissionMatchesWrite(t *testing.T) {
	perm := interfaces.Permission{Configure: "^$", Write: "^logs-.*", Read: ".*"}

	if !perm.Matches(interfaces.ActionWrite, "logs-events") {
		t.Error("write regex ^logs-.* should match 'logs-events'")
	}
	if perm.Matches(interfaces.ActionWrite, "metrics-queue") {
		t.Error("write regex ^logs-.* should NOT match 'metrics-queue'")
	}
}

func TestPermissionMatchesRead(t *testing.T) {
	perm := interfaces.Permission{Configure: "^$", Write: "^$", Read: "^app1-.*"}

	if !perm.Matches(interfaces.ActionRead, "app1-queue") {
		t.Error("read regex ^app1-.* should match 'app1-queue'")
	}
	if perm.Matches(interfaces.ActionRead, "app2-queue") {
		t.Error("read regex ^app1-.* should NOT match 'app2-queue'")
	}
}

func TestPermissionMatchesEmptyPattern(t *testing.T) {
	perm := interfaces.Permission{Configure: "", Write: ".*", Read: ".*"}

	if perm.Matches(interfaces.ActionConfigure, "anything") {
		t.Error("empty configure pattern should NOT match anything")
	}
}

func TestPermissionMatchesInvalidRegex(t *testing.T) {
	perm := interfaces.Permission{Configure: "[invalid", Write: ".*", Read: ".*"}

	if perm.Matches(interfaces.ActionConfigure, "test") {
		t.Error("invalid regex should NOT match (should fail safely)")
	}
}

func TestPermissionMatchesUnknownAction(t *testing.T) {
	perm := interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}

	if perm.Matches(interfaces.OperationAction("unknown"), "test") {
		t.Error("unknown action should NOT match")
	}
}

func TestPermissionMatchesWildcardAll(t *testing.T) {
	perm := interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}

	if !perm.Matches(interfaces.ActionConfigure, "any-name") {
		t.Error(".* should match 'any-name' for configure")
	}
	if !perm.Matches(interfaces.ActionWrite, "any-name") {
		t.Error(".* should match 'any-name' for write")
	}
	if !perm.Matches(interfaces.ActionRead, "any-name") {
		t.Error(".* should match 'any-name' for read")
	}
}

// ============================================================================
// NormalizeExchangeName tests — the AMQP default exchange has an empty
// name. RabbitMQ maps it to "amq.default" for permission checks.
// ============================================================================

func TestNormalizeExchangeNameEmpty(t *testing.T) {
	if got := interfaces.NormalizeExchangeName(""); got != interfaces.DefaultExchangeName {
		t.Errorf("empty exchange name should normalize to %s, got %s", interfaces.DefaultExchangeName, got)
	}
}

func TestNormalizeExchangeNameNonEmpty(t *testing.T) {
	if got := interfaces.NormalizeExchangeName("amq.direct"); got != "amq.direct" {
		t.Errorf("non-empty exchange name should be unchanged, got %s", got)
	}
}

// ============================================================================
// FileAuthenticator.Authorize tests — test the full authorization logic
// against the RabbitMQ permission table.
//
// Per the spec / RabbitMQ model:
//   exchange.declare  → configure on exchange
//   exchange.delete   → configure on exchange
//   queue.declare     → configure on queue
//   queue.delete      → configure on queue
//   queue.bind        → write on queue + read on exchange
//   queue.unbind      → write on queue + read on exchange
//   basic.publish     → write on exchange
//   basic.consume     → read on queue
//   basic.get         → read on queue
//   queue.purge       → read on queue
//
// The default exchange (empty name) is mapped to "amq.default".
// A user with no VHostPermission for the target vhost is refused.
// ============================================================================

func writeAuthFile(t *testing.T, path string, users []UserEntry) {
	t.Helper()
	authFile := AuthFile{Users: users}
	data, err := json.MarshalIndent(authFile, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal auth file: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
}

func newTestAuthenticator(t *testing.T, users []UserEntry) *FileAuthenticator {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	writeAuthFile(t, path, users)
	auth, err := NewFileAuthenticator(path)
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}
	return auth
}

func TestAuthorizeExchangeDeclareAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("admin")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("admin with configure=.* should be allowed to declare exchange: %v", err)
	}
}

func TestAuthorizeExchangeDeclareRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "producer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: ".*", Read: "^$"}},
			},
		},
	})

	user, _ := auth.GetUser("producer")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("producer with configure=^$ should be REFUSED from declaring exchange")
	}
}

func TestAuthorizeBasicPublishAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "producer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: ".*", Read: "^$"}},
			},
		},
	})

	user, _ := auth.GetUser("producer")
	op := interfaces.Operation{Action: interfaces.ActionWrite, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("producer with write=.* should be allowed to publish: %v", err)
	}
}

func TestAuthorizeBasicPublishRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "consumer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("consumer")
	op := interfaces.Operation{Action: interfaces.ActionWrite, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("consumer with write=^$ should be REFUSED from publishing")
	}
}

func TestAuthorizeBasicConsumeAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "consumer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("consumer")
	op := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("consumer with read=.* should be allowed to consume: %v", err)
	}
}

func TestAuthorizeBasicConsumeRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "producer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: ".*", Read: "^$"}},
			},
		},
	})

	user, _ := auth.GetUser("producer")
	op := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("producer with read=^$ should be REFUSED from consuming")
	}
}

func TestAuthorizeBasicGetAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "reader",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("reader")
	op := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("reader with read=.* should be allowed to basic.get: %v", err)
	}
}

func TestAuthorizeBasicGetRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "writer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: ".*", Read: "^$"}},
			},
		},
	})

	user, _ := auth.GetUser("writer")
	op := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("writer with read=^$ should be REFUSED from basic.get")
	}
}

func TestAuthorizeQueueDeclareAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("admin")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("admin with configure=.* should be allowed to declare queue: %v", err)
	}
}

func TestAuthorizeQueueDeclareRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "consumer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("consumer")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("consumer with configure=^$ should be REFUSED from declaring queue")
	}
}

func TestAuthorizeQueueDeleteAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("admin")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("admin with configure=.* should be allowed to delete queue: %v", err)
	}
}

func TestAuthorizeQueueDeleteRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "consumer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("consumer")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("consumer with configure=^$ should be REFUSED from deleting queue")
	}
}

func TestAuthorizeQueueBindAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "appuser",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^app-.*", Write: "^app-.*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("appuser")

	// queue.bind requires write on queue AND read on exchange
	writeOp := interfaces.Operation{Action: interfaces.ActionWrite, ResourceType: interfaces.ResourceQueue, Resource: "app-queue", VHost: "/"}
	readOp := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceExchange, Resource: "amq.topic", VHost: "/"}

	if err := auth.Authorize(user, writeOp); err != nil {
		t.Errorf("appuser with write=^app-.* should be allowed to bind queue 'app-queue': %v", err)
	}
	if err := auth.Authorize(user, readOp); err != nil {
		t.Errorf("appuser with read=.* should be allowed to bind to exchange 'amq.topic': %v", err)
	}
}

func TestAuthorizeQueueBindRefusedOnQueue(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "appuser",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^app-.*", Write: "^app-.*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("appuser")

	// Write on a queue that doesn't match the pattern
	writeOp := interfaces.Operation{Action: interfaces.ActionWrite, ResourceType: interfaces.ResourceQueue, Resource: "other-queue", VHost: "/"}

	if err := auth.Authorize(user, writeOp); err == nil {
		t.Error("appuser with write=^app-.* should be REFUSED from binding queue 'other-queue'")
	}
}

func TestAuthorizeQueueBindRefusedOnExchange(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "appuser",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^app-.*", Write: ".*", Read: "^app-.*"}},
			},
		},
	})

	user, _ := auth.GetUser("appuser")

	// Read on an exchange that doesn't match the pattern
	readOp := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceExchange, Resource: "other-exchange", VHost: "/"}

	if err := auth.Authorize(user, readOp); err == nil {
		t.Error("appuser with read=^app-.* should be REFUSED from binding to exchange 'other-exchange'")
	}
}

func TestAuthorizeNoVHostPermission(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "restricted",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/prod", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("restricted")

	// User has permission for /prod but not for /dev
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/dev"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("user with no permission for vhost /dev should be REFUSED")
	}
}

func TestAuthorizeDefaultExchangeMapping(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "publisher",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^amq\\.default$", Read: "^$"}},
			},
		},
	})

	user, _ := auth.GetUser("publisher")

	// Publishing to default exchange (empty name) should match "amq.default"
	op := interfaces.Operation{Action: interfaces.ActionWrite, ResourceType: interfaces.ResourceExchange, Resource: "", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("publisher with write=^amq\\.default$ should be allowed to publish to default exchange (empty name): %v", err)
	}
}

func TestAuthorizeRestrictedRegexPattern(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "user1",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^user1-.*", Write: "^user1-.*", Read: "^user1-.*"}},
			},
		},
	})

	user, _ := auth.GetUser("user1")

	// Allowed: user1's own resources
	declareOp := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "user1-queue", VHost: "/"}
	if err := auth.Authorize(user, declareOp); err != nil {
		t.Errorf("user1 should be allowed to declare 'user1-queue': %v", err)
	}

	// Refused: user2's resources
	declareOp2 := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "user2-queue", VHost: "/"}
	if err := auth.Authorize(user, declareOp2); err == nil {
		t.Error("user1 should be REFUSED from declaring 'user2-queue'")
	}
}

func TestAuthorizeLegacyPermissionsMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")

	// Write auth file with old permissions format
	writeAuthFile(t, path, []UserEntry{
		{
			Username:     "legacy-user",
			PasswordHash: "$2a$10$placeholder",
			LegacyPermissions: []legacyPermission{
				{Resource: ".*", Action: "read", Pattern: ".*"},
				{Resource: ".*", Action: "write", Pattern: ".*"},
				{Resource: ".*", Action: "configure", Pattern: ".*"},
			},
		},
	})

	auth, err := NewFileAuthenticator(path)
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}

	user, _ := auth.GetUser("legacy-user")

	// Legacy permissions should be migrated to allow-all on "/"
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceExchange, Resource: "any-exchange", VHost: "/"}
	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("legacy user with migrated permissions should be allowed all operations: %v", err)
	}
}

func TestAuthorizeExchangeDeleteAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("admin")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("admin with configure=.* should be allowed to delete exchange: %v", err)
	}
}

func TestAuthorizeExchangeDeleteRefused(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "consumer",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("consumer")
	op := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceExchange, Resource: "my-exchange", VHost: "/"}

	if err := auth.Authorize(user, op); err == nil {
		t.Error("consumer with configure=^$ should be REFUSED from deleting exchange")
	}
}

func TestAuthorizeQueuePurgeAllowed(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("admin")
	op := interfaces.Operation{Action: interfaces.ActionRead, ResourceType: interfaces.ResourceQueue, Resource: "my-queue", VHost: "/"}

	if err := auth.Authorize(user, op); err != nil {
		t.Errorf("admin with read=.* should be allowed to purge queue: %v", err)
	}
}

func TestAuthorizeMultipleVHosts(t *testing.T) {
	auth := newTestAuthenticator(t, []UserEntry{
		{
			Username:     "multi",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/prod", Permission: interfaces.Permission{Configure: "^prod-.*", Write: "^prod-.*", Read: "^prod-.*"}},
				{VHost: "/dev", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	user, _ := auth.GetUser("multi")

	// Allowed on /dev
	opDev := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "any-queue", VHost: "/dev"}
	if err := auth.Authorize(user, opDev); err != nil {
		t.Errorf("multi should be allowed to declare 'any-queue' on /dev: %v", err)
	}

	// Allowed on /prod with matching prefix
	opProd := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "prod-queue", VHost: "/prod"}
	if err := auth.Authorize(user, opProd); err != nil {
		t.Errorf("multi should be allowed to declare 'prod-queue' on /prod: %v", err)
	}

	// Refused on /prod with non-matching prefix
	opProdFail := interfaces.Operation{Action: interfaces.ActionConfigure, ResourceType: interfaces.ResourceQueue, Resource: "dev-queue", VHost: "/prod"}
	if err := auth.Authorize(user, opProdFail); err == nil {
		t.Error("multi should be REFUSED from declaring 'dev-queue' on /prod")
	}
}
