package config

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostMatcher(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		matches []string
		misses  []string
	}{
		{
			name:    "v4 exact",
			entries: []string{"10.1.2.3"},
			matches: []string{"10.1.2.3"},
			misses:  []string{"10.1.2.4"},
		},
		{
			name:    "v4 CIDR",
			entries: []string{"10.0.0.0/8"},
			matches: []string{"10.0.0.1", "10.255.255.255"},
			misses:  []string{"11.0.0.1"},
		},
		{
			name:    "v6 exact",
			entries: []string{"2001:db8::1"},
			matches: []string{"2001:db8::1"},
			misses:  []string{"2001:db8::2"},
		},
		{
			name:    "v6 CIDR",
			entries: []string{"2001:db8::/32"},
			matches: []string{"2001:db8::1", "2001:db8:ffff::1"},
			misses:  []string{"2001:db9::1"},
		},
		{
			name:    "IPv4-mapped peer matches v4 CIDR",
			entries: []string{"10.0.0.0/8"},
			matches: []string{"::ffff:10.1.2.3"},
			misses:  []string{"::ffff:11.1.2.3"},
		},
		{
			name:    "localhost expands to v4 loopback and ::1",
			entries: []string{"localhost"},
			matches: []string{"127.0.0.5", "::1"},
			misses:  []string{"10.0.0.1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseHostList("security.allowedhosts", tc.entries)
			require.NoError(t, err)
			assert.False(t, m.Empty())
			for _, s := range tc.matches {
				a := netip.MustParseAddr(s)
				assert.True(t, m.Contains(a), "%s should match %v", s, tc.entries)
			}
			for _, s := range tc.misses {
				a := netip.MustParseAddr(s)
				assert.False(t, m.Contains(a), "%s should not match %v", s, tc.entries)
			}
		})
	}

	t.Run("empty list matches nothing", func(t *testing.T) {
		m, err := ParseHostList("security.allowedhosts", nil)
		require.NoError(t, err)
		assert.True(t, m.Empty())
	})

	t.Run("hostname rejected", func(t *testing.T) {
		_, err := ParseHostList("security.allowedhosts", []string{"db.example.com"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "security.allowedhosts")
		assert.Contains(t, err.Error(), "db.example.com")
	})
}

func TestSecurityWarnings(t *testing.T) {
	t.Run("default config yields no warnings", func(t *testing.T) {
		assert.Empty(t, DefaultConfig().SecurityWarnings())
	})

	t.Run("non-loopback bind with auth disabled", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Network.Address = "0.0.0.0:5672"
		warnings := cfg.SecurityWarnings()
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "authentication DISABLED")
		assert.Contains(t, warnings[0], "0.0.0.0:5672")
	})

	t.Run("auth enabled but cleartext", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Network.Address = "0.0.0.0:5672"
		cfg.Security.AuthenticationEnabled = true
		warnings := cfg.SecurityWarnings()
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "cleartext")
	})

	t.Run("empty host is non-loopback", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Network.Address = ":5672"
		warnings := cfg.SecurityWarnings()
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "authentication DISABLED")
	})

	t.Run("localhost and ::1 are loopback", func(t *testing.T) {
		for _, addr := range []string{"localhost:5672", "[::1]:5672", "127.0.0.1:5672"} {
			cfg := DefaultConfig()
			cfg.Network.Address = addr
			assert.Empty(t, cfg.SecurityWarnings(), addr)
		}
	})

	t.Run("user lists with auth disabled", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Security.AllowedUsers = []string{"alice"}
		warnings := cfg.SecurityWarnings()
		require.Len(t, warnings, 1)
		assert.Contains(t, warnings[0], "no effect")
	})
}
