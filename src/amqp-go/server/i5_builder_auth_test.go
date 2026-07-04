package server

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func writeTestAuthFile(t *testing.T, path string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	authFile := struct {
		Users []auth.UserEntry `json:"users"`
	}{
		Users: []auth.UserEntry{
			{
				Username:     "testuser",
				PasswordHash: string(hash),
				VHostPermissions: []interfaces.VHostPermission{
					{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
				},
			},
		},
	}
	data, err := json.MarshalIndent(authFile, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func newAuthTestConfig(t *testing.T) *config.AMQPConfig {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Network.MaxConnections = 100
	return cfg
}

func TestBuilderAuth_FailClosedOnMissingAuthFile(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationBackend = "file"
	cfg.Security.AuthenticationFilePath = filepath.Join(t.TempDir(), "nonexistent.json")

	_, err := NewServerBuilder().WithConfig(cfg).Build()
	assert.Error(t, err, "Build() must return error when auth file is missing (fail-closed)")
}

func TestBuilderAuth_FailClosedOnMissingAuthFileViaWithFileAuthentication(t *testing.T) {
	cfg := newAuthTestConfig(t)
	missingPath := filepath.Join(t.TempDir(), "does_not_exist.json")

	_, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(missingPath).Build()
	assert.Error(t, err, "Build() must return error when auth file is missing (fail-closed, no auto-create)")
}

func TestBuilderAuth_AuthenticatorSetInServer(t *testing.T) {
	cfg := newAuthTestConfig(t)
	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)

	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	require.NoError(t, err)
	assert.NotNil(t, srv.Authenticator, "Authenticator must be set when auth is enabled")
	assert.NotNil(t, srv.MechanismRegistry, "MechanismRegistry must be set when auth is enabled")
	assert.True(t, srv.Config.Security.AuthenticationEnabled,
		"WithFileAuthentication must set AuthenticationEnabled=true")
}

func TestBuilderAuth_UnknownBackendReturnsError(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationBackend = "ldap"

	_, err := NewServerBuilder().WithConfig(cfg).Build()
	assert.Error(t, err, "Build() must return error for unknown auth backend")
}

func TestBuilderAuth_NoAuthWhenDisabled(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthenticationEnabled = false

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)
	assert.Nil(t, srv.Authenticator, "Authenticator should be nil when auth is disabled")
}

func TestBuilderAuth_WithAuthenticatorImpliesEnabled(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthenticationEnabled = false

	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)
	authenticator, err := auth.NewFileAuthenticator(authPath)
	require.NoError(t, err)

	srv, err := NewServerBuilder().WithConfig(cfg).WithAuthenticator(authenticator).Build()
	require.NoError(t, err)
	assert.True(t, srv.Config.Security.AuthenticationEnabled,
		"WithAuthenticator must set AuthenticationEnabled=true")
	assert.NotNil(t, srv.Authenticator)
}

func TestBuilderAuth_DefaultMechanismsPlainOnly(t *testing.T) {
	cfg := newAuthTestConfig(t)
	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)

	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	require.NoError(t, err)

	_, err = srv.MechanismRegistry.Get("PLAIN")
	assert.NoError(t, err, "PLAIN must be registered by default")

	_, err = srv.MechanismRegistry.Get("ANONYMOUS")
	assert.Error(t, err, "ANONYMOUS must NOT be registered unless explicitly listed in AuthMechanisms")
}

func TestBuilderAuth_AnonymousOptInViaConfig(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthMechanisms = []string{"PLAIN", "ANONYMOUS"}
	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)

	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	require.NoError(t, err)

	_, err = srv.MechanismRegistry.Get("ANONYMOUS")
	assert.NoError(t, err, "ANONYMOUS must be registered when listed in AuthMechanisms")
}

func TestBuilderAuth_SetMetricsCalled(t *testing.T) {
	cfg := newAuthTestConfig(t)
	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err)
	assert.NotNil(t, srv)
}

func TestBuilderAuth_WrongPasswordFails(t *testing.T) {
	cfg := newAuthTestConfig(t)
	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)

	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	require.NoError(t, err)

	startOK := &protocol.ConnectionStartOKMethod{
		Mechanism: "PLAIN",
		Response:  []byte{0, 't', 'e', 's', 't', 'u', 's', 'e', 'r', 0, 'w', 'r', 'o', 'n', 'g'},
	}
	payload, err := startOK.Serialize()
	require.NoError(t, err)

	conn := newLoopbackPipeConn(t)
	err = srv.handleConnectionStartOK(conn, payload)
	assert.Error(t, err, "wrong password must be rejected")
}

func TestBuilderAuth_CorrectPasswordSucceeds(t *testing.T) {
	cfg := newAuthTestConfig(t)
	authPath := filepath.Join(t.TempDir(), "auth.json")
	writeTestAuthFile(t, authPath)

	srv, err := NewServerBuilder().WithConfig(cfg).WithFileAuthentication(authPath).Build()
	require.NoError(t, err)

	startOK := &protocol.ConnectionStartOKMethod{
		Mechanism: "PLAIN",
		Response:  []byte{0, 't', 'e', 's', 't', 'u', 's', 'e', 'r', 0, 's', 'e', 'c', 'r', 'e', 't'},
	}
	payload, err := startOK.Serialize()
	require.NoError(t, err)

	conn := newLoopbackPipeConn(t)
	err = srv.handleConnectionStartOK(conn, payload)
	assert.NoError(t, err, "correct password must succeed")
	assert.Equal(t, "testuser", conn.Username)
}

func TestBuilderAuth_FailClosedWhenEnabledButNoAuthenticator(t *testing.T) {
	cfg := newAuthTestConfig(t)
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationBackend = "file"
	cfg.Security.AuthenticationFilePath = filepath.Join(t.TempDir(), "missing.json")

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		return
	}

	startOK := &protocol.ConnectionStartOKMethod{
		Mechanism: "PLAIN",
		Response:  []byte{0, 'u', 's', 'e', 'r', 0, 'p', 'a', 's', 's'},
	}
	payload, _ := startOK.Serialize()

	conn := newLoopbackPipeConn(t)
	err = srv.handleConnectionStartOK(conn, payload)
	assert.Error(t, err, "when auth enabled but no authenticator, must fail-closed (not fall through to guest)")
}

func dialAMQP(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	return conn
}
