package management

import (
	"sort"
	"strings"

	"github.com/maxpert/amqp-go/interfaces"
)

// Capability names (AUTH-023).
const (
	capReadTopology = "read_topology"
	capReadCross    = "read_cross_vhost"
	capManagePolicy = "manage_policies"
	capMutate       = "mutate_topology"
	capManageUsers  = "manage_users"
	capCloseOthers  = "close_others_connections"
)

// capabilityOrder is the canonical serialisation order for a capability set.
var capabilityOrder = []string{
	capReadTopology, capReadCross, capManagePolicy, capMutate, capManageUsers, capCloseOthers,
}

// administratorCapabilities is the full set an administrator-equivalent
// principal receives.
var administratorCapabilities = []string{
	capReadTopology, capReadCross, capManagePolicy, capMutate, capManageUsers, capCloseOthers,
}

// tagCapabilities maps a broker user tag to its management capability set
// (AUTH-023). An absent tag contributes nothing — deny by default.
var tagCapabilities = map[string][]string{
	"management":    {capReadTopology},
	"monitoring":    {capReadTopology, capReadCross},
	"policymaker":   {capReadTopology, capManagePolicy},
	"administrator": administratorCapabilities,
}

// Principal is the authenticated (or unauthenticated-loopback) identity behind
// a management request.
type Principal struct {
	Name          string
	Tags          []string
	Caps          map[string]bool
	User          *interfaces.User // nil for the basic/none modes and unauthenticated
	Authenticated bool
	// ViaCookie is true when the request authenticated with the session
	// cookie, which makes CSRF protection apply to state-changing verbs
	// (SEC-020). A Basic-authenticated mutation is exempt (SEC-024).
	ViaCookie bool
	SessionID string
	// Unauthenticated marks the loopback-only `guest` identity of the
	// none / auth-disabled modes (AUTH-041).
	Unauthenticated bool
}

// Has reports whether the principal holds capability c.
func (p *Principal) Has(c string) bool { return p != nil && p.Caps[c] }

// CapabilityList returns the held capabilities in canonical order.
func (p *Principal) CapabilityList() []string {
	out := make([]string, 0, len(capabilityOrder))
	if p == nil {
		return out
	}
	for _, c := range capabilityOrder {
		if p.Caps[c] {
			out = append(out, c)
		}
	}
	return out
}

// capabilitiesFor builds a capability set from a user's tags, unioned with the
// administrator set when the username is listed in management.admin_users.
// A user with no tag and no admin membership gets the EMPTY set: 403 on every
// /api/* request, reads included (AUTH-023/OPS-030).
func capabilitiesFor(tags []string, name string, adminUsers map[string]struct{}) map[string]bool {
	caps := make(map[string]bool, len(capabilityOrder))
	for _, tag := range tags {
		for _, c := range tagCapabilities[strings.ToLower(strings.TrimSpace(tag))] {
			caps[c] = true
		}
	}
	if _, isAdmin := adminUsers[name]; isAdmin {
		for _, c := range administratorCapabilities {
			caps[c] = true
		}
	}
	return caps
}

// readTopologyOnly is the capability set for basic/none/unauthenticated
// principals.
func readTopologyOnly() map[string]bool {
	return map[string]bool{capReadTopology: true}
}

// sortedKeys is a small helper for deterministic iteration where needed.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
