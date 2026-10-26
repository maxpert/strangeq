package config

import (
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
)

// HostMatcher is a parsed host allow/block list. An empty matcher matches
// nothing (callers treat Empty() as "no restriction").
type HostMatcher struct {
	prefixes []netip.Prefix
}

// localhostPrefixes is what the literal "localhost" entry expands to.
var localhostPrefixes = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("::1/128"),
}

// ParseHostList parses host entries: IP literals, CIDR prefixes, or the
// literal "localhost". Hostnames are rejected: matching them would require
// reverse DNS on the accept path (latency, PTR spoofing, DNS outage = outage).
func ParseHostList(field string, entries []string) (HostMatcher, error) {
	m := HostMatcher{}
	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			return HostMatcher{}, fmt.Errorf("%s: empty host entry", field)
		}
		if strings.EqualFold(entry, "localhost") {
			m.prefixes = append(m.prefixes, localhostPrefixes...)
			continue
		}
		if strings.Contains(entry, "/") {
			prefix, err := netip.ParsePrefix(entry)
			if err != nil {
				return HostMatcher{}, fmt.Errorf("%s: %q is not an IP, CIDR, or \"localhost\" (hostname matching is not supported)", field, raw)
			}
			m.prefixes = append(m.prefixes, prefix)
			continue
		}
		addr, err := netip.ParseAddr(entry)
		if err != nil {
			return HostMatcher{}, fmt.Errorf("%s: %q is not an IP, CIDR, or \"localhost\" (hostname matching is not supported)", field, raw)
		}
		addr = addr.Unmap()
		m.prefixes = append(m.prefixes, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return m, nil
}

// Empty reports whether the matcher contains no entries.
func (m HostMatcher) Empty() bool { return len(m.prefixes) == 0 }

// Contains reports whether addr matches any entry. IPv4-mapped IPv6 addresses
// are unmapped first so ::ffff:10.1.2.3 matches 10.0.0.0/8.
func (m HostMatcher) Contains(addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, prefix := range m.prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// containsLoopback reports whether the matcher covers 127.0.0.1 or ::1.
func (m HostMatcher) containsLoopback() bool {
	return m.Contains(netip.MustParseAddr("127.0.0.1")) || m.Contains(netip.MustParseAddr("::1"))
}

// isLoopbackBind reports whether a listen address is loopback-only. "" (all
// interfaces), 0.0.0.0 and :: are non-loopback. A hostname is loopback only
// when it is literally "localhost".
func isLoopbackBind(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	switch host {
	case "", "0.0.0.0", "::":
		return false
	case "localhost":
		return true
	}
	addr, err := netip.ParseAddr(strings.Trim(host, "[]"))
	if err != nil {
		return false
	}
	return addr.Unmap().IsLoopback()
}

// SecurityWarnings returns one human sentence per insecure startup finding.
// It is a pure function so the checks are unit-testable; main prints each
// warning to stderr and logs it after Build.
func (c *AMQPConfig) SecurityWarnings() []string {
	if c == nil {
		return nil
	}
	var warnings []string

	address := c.Network.Address
	nonLoopback := !isLoopbackBind(address)
	authOn := c.Security.AuthenticationEnabled
	tlsOn := c.Security.TLSEnabled

	if nonLoopback && !authOn {
		warnings = append(warnings, fmt.Sprintf(
			"AMQP listener %s is reachable from the network with authentication DISABLED: "+
				"any client can publish, consume and purge as \"guest\". "+
				"Set security.authenticationenabled: true or bind network.address to 127.0.0.1.", address))
	}
	if nonLoopback && authOn && !tlsOn {
		warnings = append(warnings, fmt.Sprintf(
			"AMQP listener %s is reachable from the network with TLS DISABLED: "+
				"credentials will cross the network in cleartext (SASL PLAIN). "+
				"Enable TLS (security.tlsenabled: true) or bind network.address to 127.0.0.1.", address))
	}
	if nonLoopback {
		for _, m := range c.Security.AuthMechanisms {
			if strings.EqualFold(m, "ANONYMOUS") {
				warnings = append(warnings, fmt.Sprintf(
					"AMQP listener %s is reachable from the network and ANONYMOUS is in authmechanisms: "+
						"any client is accepted without credentials. Remove ANONYMOUS or bind to 127.0.0.1.", address))
				break
			}
		}
	}
	if !authOn && (len(c.Security.AllowedUsers) > 0 || len(c.Security.BlockedUsers) > 0) {
		warnings = append(warnings, "user allow/block lists have no effect when authentication is disabled: "+
			"enable security.authenticationenabled to enforce them")
	}
	if len(c.Security.AllowedHosts) > 0 {
		if matcher, err := ParseHostList("security.allowedhosts", c.Security.AllowedHosts); err == nil && !matcher.containsLoopback() {
			warnings = append(warnings, "security.allowedhosts is set and does not cover loopback: "+
				"local clients (including health checks) will be refused; add \"localhost\" if that is not intended")
		}
	}

	telemetryNonLoopback := c.Telemetry.Enabled && !isLoopbackBind(c.Telemetry.Address)
	if telemetryNonLoopback && c.Telemetry.PprofEnabled {
		warnings = append(warnings, fmt.Sprintf(
			"telemetry listener %s is reachable from the network with pprof ENABLED: "+
				"heap and goroutine dumps are unauthenticated and can leak message metadata. "+
				"Remove --enable-pprof or bind telemetry.address to 127.0.0.1.", c.Telemetry.Address))
	} else if telemetryNonLoopback {
		warnings = append(warnings, fmt.Sprintf(
			"telemetry listener %s is reachable from the network; /metrics and /health are unauthenticated. "+
				"Bind telemetry.address to 127.0.0.1 unless a firewall restricts access.", c.Telemetry.Address))
	}

	return warnings
}

// UnknownMechanisms returns the configured SASL mechanism names this build does
// not implement, sorted. main warns about them at startup; the registry itself
// silently skips unknown names by design (auth.RegistryForMechanismsReport).
func (c *AMQPConfig) UnknownMechanisms() []string {
	if c == nil {
		return nil
	}
	known := map[string]struct{}{"PLAIN": {}, "ANONYMOUS": {}}
	var unknown []string
	for _, name := range c.Security.AuthMechanisms {
		if _, ok := known[strings.ToUpper(name)]; !ok {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return unknown
}
