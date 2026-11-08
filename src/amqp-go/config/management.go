package config

import (
	"fmt"
	"net/netip"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// IsLoopbackManagementAddress reports whether addr denotes a loopback bind.
// The empty string is loopback (the DefaultConfig value is "127.0.0.1", but a
// zero-value caller that omits it must still be safe). "localhost" is
// accepted as loopback; every other hostname is NOT — a hostname can resolve
// to a non-loopback address, and the gate must fail closed (SEC-002/111).
func IsLoopbackManagementAddress(addr string) bool {
	switch strings.ToLower(strings.TrimSpace(addr)) {
	case "", "localhost":
		return true
	}
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return false
	}
	return ip.IsLoopback()
}

// validateManagement enforces the startup refusal matrix of spec mgmt-01 §2.
// It is a no-op when management is disabled: a bad management.* block on a
// disabled surface must not stop the broker from booting.
func (c *AMQPConfig) validateManagement() error {
	m := c.Management
	if !m.Enabled {
		return nil
	}

	if m.Port < 1 || m.Port > 65535 {
		return fmt.Errorf("invalid management port: %d", m.Port)
	}

	mode := m.Authentication
	if mode == "" {
		mode = ManagementAuthenticationInherit
	}
	switch mode {
	case ManagementAuthenticationInherit:
		// inherit + broker auth disabled requires an explicit opt-in AND a
		// loopback bind (AUTH-002/040).
		if !c.Security.AuthenticationEnabled {
			if !m.AllowUnauthenticated {
				return fmt.Errorf("management.authentication is \"inherit\" but broker authentication is disabled; set management.allow_unauthenticated: true (and bind loopback) to serve unauthenticated on the loopback interface")
			}
			if !IsLoopbackManagementAddress(m.Address) {
				return fmt.Errorf("management.allow_unauthenticated requires a loopback management.address; got %q", m.Address)
			}
		}
	case ManagementAuthenticationBasic:
		// The hash must be a real bcrypt hash (AUTH-003). Cost parsing is the
		// cheap structural check: it rejects a plaintext password or a stray
		// empty string before any credential is accepted.
		if m.Basic.PasswordHash == "" {
			return fmt.Errorf("management.authentication is \"basic\" but management.basic.password_hash is empty")
		}
		if _, err := bcrypt.Cost([]byte(m.Basic.PasswordHash)); err != nil {
			return fmt.Errorf("management.basic.password_hash is not a valid bcrypt hash: %v", err)
		}
	case ManagementAuthenticationNone:
		// "none" is loopback-only (SEC-111).
		if !IsLoopbackManagementAddress(m.Address) {
			return fmt.Errorf("management.authentication is \"none\" but management.address %q is not loopback; unauthenticated management must bind loopback", m.Address)
		}
	default:
		return fmt.Errorf("invalid management.authentication %q: allowed values are %q, %q, %q", m.Authentication, ManagementAuthenticationInherit, ManagementAuthenticationBasic, ManagementAuthenticationNone)
	}

	// Trusted proxies must parse as CIDRs/IPs (SEC-112). A malformed entry is a
	// startup error, never a silent no-op: an operator who wrote a proxy list
	// expects it to be honoured.
	//
	// A trusted proxy MUST set or append the client address (X-Forwarded-For
	// or X-Real-IP) on every request. When it forwards no client address, the
	// socket peer is the PROXY, not the client: the request is failed closed
	// (no loopback exemption, host ACL refuses) rather than being attributed
	// to the proxy's address.
	for _, entry := range m.TrustedProxies {
		if err := parseCIDROrAddr(entry); err != nil {
			return fmt.Errorf("management.trusted_proxies entry %q: %v", entry, err)
		}
	}

	if m.Limits.MaxPageSize < 1 {
		return fmt.Errorf("management.limits.max_page_size must be positive: %d", m.Limits.MaxPageSize)
	}
	if m.Limits.MaxInFlight < 1 {
		return fmt.Errorf("management.limits.max_in_flight must be positive: %d", m.Limits.MaxInFlight)
	}
	if m.Limits.MaxBodyBytes < 1 {
		return fmt.Errorf("management.limits.max_body_bytes must be positive: %d", m.Limits.MaxBodyBytes)
	}
	if m.Limits.MaxImportBodyBytes < 1 {
		return fmt.Errorf("management.limits.max_import_body_bytes must be positive: %d", m.Limits.MaxImportBodyBytes)
	}
	if m.Limits.MaxResponseBytes < 1 {
		return fmt.Errorf("management.limits.max_response_bytes must be positive: %d", m.Limits.MaxResponseBytes)
	}
	if m.Limits.MaxNameBytes < 1 {
		return fmt.Errorf("management.limits.max_name_bytes must be positive: %d", m.Limits.MaxNameBytes)
	}
	if m.Session.MaxSessions < 1 {
		return fmt.Errorf("management.session.max_sessions must be positive: %d", m.Session.MaxSessions)
	}
	if m.Session.IdleTimeoutSeconds < 1 {
		return fmt.Errorf("management.session.idle_timeout_seconds must be positive: %d", m.Session.IdleTimeoutSeconds)
	}
	if m.Get.MaxCount < 1 {
		return fmt.Errorf("management.get.max_count must be positive: %d", m.Get.MaxCount)
	}
	if m.Async.MaxJobs < 1 {
		return fmt.Errorf("management.async.max_jobs must be positive: %d", m.Async.MaxJobs)
	}

	if m.TLS.Enabled {
		if m.TLS.CertFile == "" || m.TLS.KeyFile == "" {
			return fmt.Errorf("management.tls.enabled requires management.tls.cert_file and management.tls.key_file")
		}
		// mTLS is refused until a certificate->identity mapping exists (OQ-015):
		// accepting a CA file and then trusting the peer without mapping it to
		// a user is exactly the insecure default the spec forbids.
		if m.TLS.CAFile != "" {
			return fmt.Errorf("management.tls.ca_file (mutual TLS) is not supported: no certificate-to-identity mapping exists yet (OQ-015)")
		}
	}

	if m.CORS.AllowedOrigins != nil {
		for _, origin := range m.CORS.AllowedOrigins {
			if strings.TrimSpace(origin) == "" {
				return fmt.Errorf("management.cors.allowed_origins entries must be non-empty")
			}
			if origin == "*" {
				return fmt.Errorf("management.cors.allowed_origins cannot contain \"*\": wildcard origins with credentials are forbidden (SEC-094)")
			}
		}
	}

	return nil
}

// parseCIDROrAddr accepts either a CIDR prefix or a bare IP.
func parseCIDROrAddr(entry string) error {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return fmt.Errorf("empty entry")
	}
	if strings.Contains(entry, "/") {
		if _, err := netip.ParsePrefix(entry); err != nil {
			return err
		}
		return nil
	}
	if _, err := netip.ParseAddr(entry); err != nil {
		return err
	}
	return nil
}

// ManagementSecurityWarnings returns the operator-facing warnings the
// management surface must log at startup (SEC-110/AUTH-042/OPS-022). It never
// includes a secret.
func (c *AMQPConfig) ManagementSecurityWarnings() []string {
	m := c.Management
	if !m.Enabled {
		return nil
	}
	var out []string
	if !IsLoopbackManagementAddress(m.Address) {
		if !m.TLS.Enabled {
			out = append(out, fmt.Sprintf(
				"SECURITY WARNING: management surface is bound to non-loopback %s:%d without TLS; the admin API and session cookies travel in cleartext",
				m.Address, m.Port))
		}
	}
	switch m.Authentication {
	case ManagementAuthenticationNone:
		out = append(out, "SECURITY WARNING: management.authentication is \"none\": the management API is served unauthenticated on the loopback interface (SEC-111)")
	case ManagementAuthenticationBasic:
		if m.Basic.Admin {
			out = append(out, "SECURITY WARNING: management.basic.admin is true: the single basic-auth principal has administrator-equivalent capabilities (AUTH-042)")
		}
		if m.Basic.Username == "" {
			out = append(out, "SECURITY WARNING: management.authentication is \"basic\" but management.basic.username is empty")
		}
	default:
		if m.AllowUnauthenticated {
			out = append(out, "SECURITY WARNING: management.allow_unauthenticated is true: unauthenticated loopback reads are enabled (AUTH-041)")
		}
	}
	if m.AllowUnauthenticatedMutations {
		out = append(out, "SECURITY WARNING: management.allow_unauthenticated_mutations is true: unauthenticated loopback clients may mutate topology (AUTH-041)")
	}
	if m.AllowConnectionClose {
		out = append(out, "SECURITY WARNING: management.allow_connection_close is true (AUTH-015)")
	}
	return out
}
