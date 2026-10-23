package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfigFile writes body to a temp file and returns its path.
func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0600))
	return path
}

// repoFile returns an absolute path to a file relative to the package dir.
func repoFile(t *testing.T, rel string) string {
	t.Helper()
	abs, err := filepath.Abs(rel)
	require.NoError(t, err)
	return abs
}

// TestConfig_MinimalSnippets_LoadWithDefaults proves that a config file is
// merged onto DefaultConfig() rather than decoded onto a zero value, and that
// every shipped snippet loads with zero unknown-key warnings.
func TestConfig_MinimalSnippets_LoadWithDefaults(t *testing.T) {
	tlsDir := t.TempDir()
	certFile := filepath.Join(tlsDir, "cert.pem")
	keyFile := filepath.Join(tlsDir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, []byte("cert"), 0600))
	require.NoError(t, os.WriteFile(keyFile, []byte("key"), 0600))

	cases := []struct {
		name string
		body string
		// authEnabled is asserted when non-nil.
		authEnabled *bool
	}{
		{
			name: "README minimal snippet as shipped today",
			body: `network:
  address: :5672
  port: 5672

storage:
  path: ./data

server:
  loglevel: info
`,
		},
		{
			name: "systemd guide heredoc as shipped today",
			body: `network:
  address: 0.0.0.0:5672
  maxconnections: 10000

storage:
  path: /var/lib/amqp/data

security:
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json

server:
  loglevel: info
`,
		},
		{
			name: "new deployment/systemd/config.yaml",
			body: fileBody(t, repoFile(t, "../../../deployment/systemd/config.yaml")),
			authEnabled: func() *bool {
				b := true
				return &b
			}(),
		},
		{
			name: "new README minimal snippet",
			body: `network:
  address: 127.0.0.1:5672

storage:
  path: ./data

server:
  loglevel: info
`,
		},
		{
			name: "new README production snippet",
			body: `network:
  address: "0.0.0.0:5672"
  maxconnections: 10000
  tcpkeepalive: true

storage:
  path: /var/lib/amqp

security:
  tlsenabled: true
  tlscertfile: ` + certFile + `
  tlskeyfile: ` + keyFile + `
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json

server:
  loglevel: info

engine:
  ringbuffersize: 262144
  walbatchsize: 1000

resourcealarms:
  enabled: true
  memoryhighwatermark: 0.4

telemetry:
  enabled: true
  address: 127.0.0.1
  port: 9419
`,
		},
		{
			name: "config.sample.yaml",
			body: fileBody(t, repoFile(t, "../config.sample.yaml")),
		},
		{
			name: "snake_case keys bind",
			body: `security:
  authentication_enabled: true
  auth_mechanisms: [PLAIN, ANONYMOUS]
`,
			authEnabled: func() *bool {
				b := true
				return &b
			}(),
		},
		{
			name: "authmechanisms replaces default slice",
			body: `security:
  authmechanisms: [ANONYMOUS]
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfigFile(t, tc.body)
			cfg, report, err := LoadConfig(path)
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			assert.Empty(t, report.UnknownKeys)
			assert.Equal(t, path, report.File)

			// Unset keys retain DefaultConfig values; keys the snippet sets win.
			def := DefaultConfig()
			if !strings.Contains(tc.body, "ringbuffersize") {
				assert.Equal(t, def.Engine.RingBufferSize, cfg.Engine.RingBufferSize)
			}
			assert.Equal(t, def.Server.MaxFrameSize, cfg.Server.MaxFrameSize)
			if !strings.Contains(tc.body, "maxconnections") {
				assert.Equal(t, def.Network.MaxConnections, cfg.Network.MaxConnections)
			}
			require.NotNil(t, cfg.Storage.Fsync)
			assert.True(t, *cfg.Storage.Fsync)
			assert.Equal(t, []string{"PLAIN"}, def.Security.AuthMechanisms)

			if tc.authEnabled != nil {
				assert.Equal(t, *tc.authEnabled, cfg.Security.AuthenticationEnabled)
			}
			switch tc.name {
			case "authmechanisms replaces default slice":
				assert.Equal(t, []string{"ANONYMOUS"}, cfg.Security.AuthMechanisms)
			case "snake_case keys bind":
				assert.Equal(t, []string{"PLAIN", "ANONYMOUS"}, cfg.Security.AuthMechanisms)
			}
		})
	}
}

// fileBody reads a file that must exist. Tests that reference shipped docs
// fail loudly when the doc regresses to a non-loadable snippet.
func fileBody(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "reading %s", path)
	return string(data)
}

func TestConfig_EnvWithoutConfigFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AMQP_STORAGE_PATH", dir)
	t.Setenv("AMQP_NETWORK_PORT", "5673")
	t.Setenv("AMQP_SECURITY_AUTHENTICATION_ENABLED", "true")

	cfg, report, err := LoadConfig("")
	require.NoError(t, err)
	assert.Equal(t, dir, cfg.Storage.Path)
	assert.Equal(t, 5673, cfg.Network.Port)
	assert.Equal(t, "127.0.0.1:5673", cfg.Network.Address)
	assert.True(t, cfg.Security.AuthenticationEnabled)
	assert.Equal(t, "", report.File)
	assert.ElementsMatch(t,
		[]string{"AMQP_STORAGE_PATH", "AMQP_NETWORK_PORT", "AMQP_SECURITY_AUTHENTICATION_ENABLED"},
		report.EnvKeys)

	t.Run("ignored env names are reported without failing", func(t *testing.T) {
		t.Setenv("AMQP_URL", "amqp://x")
		_, report, err := LoadConfig("")
		require.NoError(t, err)
		assert.Contains(t, report.IgnoredEnv, "AMQP_URL")
		assert.NotContains(t, report.EnvKeys, "AMQP_URL")
	})

	t.Run("resource alarms section aliases", func(t *testing.T) {
		// Spec 1.3 step 4: both RESOURCE_ALARMS and the concatenated
		// RESOURCEALARMS alias map onto the resourcealarms section.
		for _, name := range []string{"AMQP_RESOURCE_ALARMS_ENABLED", "AMQP_RESOURCEALARMS_ENABLED"} {
			t.Run(name, func(t *testing.T) {
				t.Setenv(name, "true")
				cfg, report, err := LoadConfig("")
				require.NoError(t, err)
				assert.True(t, cfg.ResourceAlarms.Enabled)
				assert.Contains(t, report.EnvKeys, name)
				assert.NotContains(t, report.IgnoredEnv, name)
			})
		}
	})
}

func TestConfig_AuthenticationConfigMap_KeySpellings(t *testing.T) {
	// security.authenticationconfig is a free-form map: its child keys must
	// survive verbatim so AuthenticationConfig["user_file"] (read by
	// server/builder.go) binds. That exemption must hold for the snake_case
	// spelling of the section too, not just the canonical concatenated one.
	for name, yamlDoc := range map[string]string{
		"canonical":  "security:\n  authenticationconfig:\n    user_file: /etc/amqp/users.json\n",
		"snake_case": "security:\n  authentication_config:\n    user_file: /etc/amqp/users.json\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, report, err := LoadConfig(writeConfigFile(t, yamlDoc))
			require.NoError(t, err)
			assert.Empty(t, report.UnknownKeys)
			assert.Equal(t, "/etc/amqp/users.json", cfg.Security.AuthenticationConfig["user_file"])
		})
	}
}

func TestConfig_AddressPortConflict(t *testing.T) {
	path := writeConfigFile(t, `network:
  address: "127.0.0.1:5672"
  port: 5673
`)
	_, _, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "5672")
	assert.Contains(t, err.Error(), "5673")
	assert.Contains(t, err.Error(), "conflicts")

	t.Run("address without port gets configured port", func(t *testing.T) {
		path := writeConfigFile(t, `network:
  address: "0.0.0.0"
`)
		cfg, _, err := LoadConfig(path)
		require.NoError(t, err)
		assert.Equal(t, "0.0.0.0:5672", cfg.Network.Address)
	})
}

func TestConfig_UnknownKey_WarnsOrErrors(t *testing.T) {
	path := writeConfigFile(t, `server:
  memorylimitpercent: 60
engine:
  walbatchtimeout: 10ms
`)
	cfg, report, err := LoadConfig(path)
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.Contains(t, report.UnknownKeys, "server.memorylimitpercent")
	assert.Contains(t, report.UnknownKeys, "engine.walbatchtimeout")

	t.Run("duplicate normalized keys are an error", func(t *testing.T) {
		path := writeConfigFile(t, `security:
  auth_mechanisms: [PLAIN]
  authmechanisms: [ANONYMOUS]
`)
		_, _, err := LoadConfig(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate config key")
	})
}

func TestConfig_Validate_NewRules(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*AMQPConfig)
		wantErr string
	}{
		{
			name:    "telemetry port zero",
			mutate:  func(c *AMQPConfig) { c.Telemetry.Port = 0 },
			wantErr: "invalid telemetry port",
		},
		{
			name:    "telemetry port too large",
			mutate:  func(c *AMQPConfig) { c.Telemetry.Port = 70000 },
			wantErr: "invalid telemetry port",
		},
		{
			name:    "invalid log level",
			mutate:  func(c *AMQPConfig) { c.Server.LogLevel = "verbose" },
			wantErr: "allowed levels",
		},
		{
			name:    "empty log level accepted",
			mutate:  func(c *AMQPConfig) { c.Server.LogLevel = "" },
			wantErr: "",
		},
		{
			name:    "silent log level accepted",
			mutate:  func(c *AMQPConfig) { c.Server.LogLevel = "silent" },
			wantErr: "",
		},
		{
			name:    "hostname in allowed hosts",
			mutate:  func(c *AMQPConfig) { c.Security.AllowedHosts = []string{"db.example.com"} },
			wantErr: "db.example.com",
		},
		{
			name: "valid host list",
			mutate: func(c *AMQPConfig) {
				c.Security.AllowedHosts = []string{"10.0.0.0/8", "::1", "localhost"}
			},
			wantErr: "",
		},
		{
			name:    "negative auth failure limit",
			mutate:  func(c *AMQPConfig) { c.Security.AuthFailureLimit = -1 },
			wantErr: "authfailurelimit",
		},
		{
			name:    "telemetry address hostname",
			mutate:  func(c *AMQPConfig) { c.Telemetry.Address = "metrics.example.com" },
			wantErr: "telemetry address",
		},
		{
			name:    "empty user entry",
			mutate:  func(c *AMQPConfig) { c.Security.AllowedUsers = []string{""} },
			wantErr: "allowedusers",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestConfig_SaveMode0600(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generated.yaml")
	require.NoError(t, DefaultConfig().Save(path))
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestConfig_ResolvePaths(t *testing.T) {
	base := t.TempDir()
	cfg := DefaultConfig()
	cfg.Storage.Path = "./data"
	cfg.Server.LogFile = "logs/server.log"
	cfg.Server.PidFile = "run/server.pid"
	cfg.Security.AuthenticationFilePath = "auth.json"
	cfg.Security.TLSCertFile = "certs/x.pem"
	cfg.Security.TLSKeyFile = "certs/y.pem"
	cfg.Security.TLSCAFile = "certs/ca.pem"
	cfg.Security.AuthenticationConfig["user_file"] = "users.json"

	cfg.ResolvePaths(base)

	assert.Equal(t, filepath.Join(base, "data"), cfg.Storage.Path)
	assert.Equal(t, filepath.Join(base, "logs/server.log"), cfg.Server.LogFile)
	assert.Equal(t, filepath.Join(base, "run/server.pid"), cfg.Server.PidFile)
	assert.Equal(t, filepath.Join(base, "auth.json"), cfg.Security.AuthenticationFilePath)
	assert.Equal(t, filepath.Join(base, "certs/x.pem"), cfg.Security.TLSCertFile)
	assert.Equal(t, filepath.Join(base, "users.json"), cfg.Security.AuthenticationConfig["user_file"])

	t.Run("absolute and empty paths are untouched", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Storage.Path = "/abs/data"
		cfg.Server.LogFile = ""
		cfg.ResolvePaths(base)
		assert.Equal(t, "/abs/data", cfg.Storage.Path)
		assert.Equal(t, "", cfg.Server.LogFile)
	})
}

func TestLoadReport_Helpers(t *testing.T) {
	rep := &LoadReport{
		File:        "/etc/amqp/config.yaml",
		EnvKeys:     []string{"AMQP_STORAGE_PATH"},
		UnknownKeys: []string{"server.memorylimitpercent"},
	}
	var sb strings.Builder
	PrintLoadReport(&sb, rep)
	out := sb.String()
	assert.Contains(t, out, "defaults")
	assert.Contains(t, out, "file=/etc/amqp/config.yaml")
	assert.Contains(t, out, "env=[AMQP_STORAGE_PATH]")
	assert.Contains(t, out, `unknown config key "server.memorylimitpercent"`)

	t.Run("no file no env", func(t *testing.T) {
		var sb strings.Builder
		PrintLoadReport(&sb, &LoadReport{})
		assert.Contains(t, sb.String(), "Config sources: defaults")
	})
}
