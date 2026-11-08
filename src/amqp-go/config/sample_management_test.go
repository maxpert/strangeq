package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"
)

// sampleManagementKeys is every management key the shipped sample must document
// so an operator can discover the full surface. It is checked against the
// PARSED YAML (not the raw text), so comments and key order are free.
var sampleManagementKeys = []string{
	"management.enabled",
	"management.address",
	"management.port",
	"management.nodename",
	"management.authentication",
	"management.allowunauthenticated",
	"management.allowunauthenticatedmutations",
	"management.allowconnectionclose",
	"management.adminusers",
	"management.trustedproxies",
	"management.basic.username",
	"management.basic.passwordhash",
	"management.basic.admin",
	"management.tls.enabled",
	"management.tls.certfile",
	"management.tls.keyfile",
	"management.tls.cafile",
	"management.cors.allowedorigins",
	"management.session.idletimeoutseconds",
	"management.session.maxsessions",
	"management.limits.maxpagesize",
	"management.limits.requesttimeoutseconds",
	"management.limits.maxinflight",
	"management.limits.maxbodybytes",
	"management.limits.maximportbodybytes",
	"management.limits.maxresponsebytes",
	"management.limits.maxnamebytes",
	"management.rate_limits.publish",
	"management.rate_limits.get",
	"management.rate_limits.purge",
	"management.rate_limits.declare",
	"management.rate_limits.definitionsimportseconds",
	"management.definitions.maxqueues",
	"management.definitions.maxexchanges",
	"management.definitions.maxbindings",
	"management.async.messagethreshold",
	"management.async.entitythreshold",
	"management.async.maxjobs",
	"management.async.jobretentionseconds",
	"management.get.maxcount",
	"management.audit.logfile",
}

// lookupYAMLPath walks a dotted path through a parsed YAML document.
func lookupYAMLPath(doc map[string]any, path string) (any, bool) {
	var cur any = doc
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// TestConfig_SampleManagementBlock proves the shipped src/amqp-go/config.sample.yaml
// documents the whole management surface and that the real loader accepts it
// with every key bound (no unknown-key warning) and management left disabled,
// so the sample's behaviour is unchanged.
func TestConfig_SampleManagementBlock(t *testing.T) {
	const sample = "../config.sample.yaml"

	raw, err := os.ReadFile(sample)
	require.NoError(t, err, "the shipped sample must exist")

	var doc map[string]any
	require.NoError(t, yamlv3.Unmarshal(raw, &doc), "the sample must be valid YAML")
	for _, path := range sampleManagementKeys {
		if _, ok := lookupYAMLPath(doc, path); !ok {
			t.Errorf("config.sample.yaml no longer documents %s", path)
		}
	}

	cfg, report, err := LoadConfig(sample)
	require.NoError(t, err, "the shipped sample must load")
	require.Empty(t, report.UnknownKeys, "every sample key must bind to a config field")
	require.NoError(t, cfg.Validate(), "the shipped sample must validate")

	def := DefaultConfig().Management
	m := cfg.Management

	// The sample ships management OFF, so its behaviour is unchanged.
	assert.False(t, m.Enabled, "the shipped sample must leave management disabled")
	assert.Equal(t, def.Address, m.Address)
	assert.Equal(t, def.Port, m.Port)
	assert.Equal(t, def.Authentication, m.Authentication)
	assert.False(t, m.AllowUnauthenticated)
	assert.False(t, m.AllowUnauthenticatedMutations)
	assert.False(t, m.AllowConnectionClose)
	assert.Empty(t, m.AdminUsers)
	assert.Empty(t, m.TrustedProxies)
	assert.Equal(t, def.Basic, m.Basic)
	assert.Equal(t, def.TLS, m.TLS)
	assert.Empty(t, m.CORS.AllowedOrigins)
	assert.Equal(t, def.Session, m.Session)
	assert.Equal(t, def.Limits, m.Limits)
	assert.Equal(t, def.RateLimits, m.RateLimits)
	assert.Equal(t, def.Definitions, m.Definitions)
	assert.Equal(t, def.Async, m.Async)
	assert.Equal(t, def.Get, m.Get)
}
