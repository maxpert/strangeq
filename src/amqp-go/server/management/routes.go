package management

import (
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// route is one endpoint: a method, a bounded path template (which doubles as
// the metrics label, OPS-004), and its handler.
type route struct {
	method  string
	pattern string
	segs    []string
	handler func(http.ResponseWriter, *http.Request)
	// public marks an endpoint reachable WITHOUT prior authentication (the
	// session-login endpoint, which performs its own credential check).
	public bool
}

// router is a small ordered matcher. `{name}` segments capture a value read
// back with pathValue. A linear scan is correct and fast enough for the ~70
// management endpoints, and it keeps the endpoint map in one readable table.
type router struct {
	routes []*route
}

func (rt *router) add(method, pattern string, h func(http.ResponseWriter, *http.Request)) {
	segs := splitPath(pattern)
	rt.routes = append(rt.routes, &route{method: method, pattern: pattern, segs: segs, handler: h})
}

// addPublic registers an endpoint exempt from the authentication gate.
func (rt *router) addPublic(method, pattern string, h func(http.ResponseWriter, *http.Request)) {
	segs := splitPath(pattern)
	rt.routes = append(rt.routes, &route{method: method, pattern: pattern, segs: segs, handler: h, public: true})
}

func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// match returns the first route whose pattern matches path and whose method
// matches (method first, then any method for the Allow set).
func (rt *router) match(method, path string) *route {
	reqSegs := splitPath(path)
	for _, r := range rt.routes {
		if r.method == method && matchSegs(r.segs, reqSegs) {
			return r
		}
	}
	return nil
}

// allowedFor returns the sorted methods that have a route matching path.
func (rt *router) allowedFor(path string) []string {
	reqSegs := splitPath(path)
	seen := map[string]struct{}{}
	for _, r := range rt.routes {
		if matchSegs(r.segs, reqSegs) {
			seen[r.method] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for m := range seen {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func matchSegs(pattern, req []string) bool {
	if len(pattern) != len(req) {
		return false
	}
	for i := range pattern {
		if strings.HasPrefix(pattern[i], "{") {
			continue
		}
		if pattern[i] != req[i] {
			return false
		}
	}
	return true
}

// pathValue extracts a captured segment by name using the matched pattern.
// The returned value is percent-decoded.
func pathValue(pattern, path, name string) string {
	p := splitPath(pattern)
	req := splitPath(path)
	for i, seg := range p {
		if seg == "{"+name+"}" && i < len(req) {
			decoded, err := url.PathUnescape(req[i])
			if err != nil {
				return req[i]
			}
			return decoded
		}
	}
	return ""
}

// routeEndpointList renders the endpoint map as "METHOD /template" strings for
// GET /api/strangeq/version.
func routeEndpointList(rt *router) []string {
	out := make([]string, 0, len(rt.routes))
	for _, r := range rt.routes {
		out = append(out, r.method+" "+r.pattern)
	}
	return out
}

// buildRouter wires every documented endpoint (spec mgmt-01 §8). Honest
// divergences (501/200 []) are registered as real routes so a client never
// sees a 404 typo.
func buildRouter(s *Server) *router {
	rt := &router{}
	get := func(p string, h func(http.ResponseWriter, *http.Request)) { rt.add(http.MethodGet, p, h) }
	post := func(p string, h func(http.ResponseWriter, *http.Request)) { rt.add(http.MethodPost, p, h) }
	put := func(p string, h func(http.ResponseWriter, *http.Request)) { rt.add(http.MethodPut, p, h) }
	del := func(p string, h func(http.ResponseWriter, *http.Request)) { rt.add(http.MethodDelete, p, h) }

	// --- session / identity ---
	rt.addPublic(http.MethodPost, "/api/strangeq/session", s.hSessionCreate)
	del("/api/strangeq/session", s.hSessionDelete)
	get("/api/whoami", s.hWhoami)
	get("/api/overview", s.hOverview)
	get("/api/strangeq/version", s.hVersion)

	// --- health ---
	get("/api/health", s.hHealth)
	get("/api/health/checks", s.hHealthChecks)
	get("/api/health/checks/{check}", s.hHealthCheck)

	// --- nodes (synthetic, single) ---
	get("/api/nodes", s.hNodes)
	get("/api/nodes/{name}", s.hNode)
	post("/api/nodes/{name}", s.hNotImplemented("node operations"))
	del("/api/nodes/{name}", s.hNotImplemented("node operations"))

	// --- connections ---
	get("/api/connections", s.hConnections)
	get("/api/connections/username/{user}", s.hConnectionsByUser)
	get("/api/connections/{name}/channels", s.hNotImplementedAggregate("channel listing for a connection"))
	get("/api/connections/{name}", s.hConnection)
	del("/api/connections/username/{user}", s.hCloseConnectionsByUser)
	del("/api/connections/{name}", s.hCloseConnection)

	// --- channels ---
	get("/api/channels", s.hChannels)
	get("/api/channels/{id}", s.hChannel)

	// --- consumers ---
	get("/api/consumers", s.hConsumers)
	get("/api/consumers/{vhost}", s.hConsumers)

	// --- exchanges ---
	get("/api/exchanges", s.hExchanges)
	put("/api/exchanges/{vhost}/{name}", s.hExchangeDeclare)
	del("/api/exchanges/{vhost}/{name}", s.hExchangeDelete)
	get("/api/exchanges/{vhost}/{name}/bindings/{source}", s.hNotImplementedAggregate("aggregate binding listing"))
	get("/api/exchanges/{vhost}/{name}/bindings/{destination}", s.hNotImplementedAggregate("aggregate binding listing"))
	post("/api/exchanges/{vhost}/{name}/publish", s.hPublish)
	get("/api/exchanges/{vhost}/{name}", s.hExchange)

	// --- queues ---
	get("/api/queues/detailed", s.hQueuesDetailed)
	get("/api/queues", s.hQueues)
	get("/api/queues/{vhost}/{name}/bindings", s.hNotImplementedAggregate("queue binding listing"))
	del("/api/queues/{vhost}/{name}/contents", s.hQueuePurge)
	post("/api/queues/{vhost}/{name}/get", s.hQueueGet)
	put("/api/queues/{vhost}/{name}", s.hQueueDeclare)
	del("/api/queues/{vhost}/{name}", s.hQueueDelete)
	get("/api/queues/{vhost}/{name}", s.hQueue)
	post("/api/queues/{vhost}", s.hQueueCreate)
	get("/api/queues/{vhost}", s.hQueuesForVHost)

	// --- bindings (aggregate listing is BOB-4, unbuilt) ---
	get("/api/bindings", s.hNotImplementedAggregate("aggregate binding listing"))
	get("/api/bindings/{vhost}", s.hNotImplementedAggregate("aggregate binding listing"))
	post("/api/bindings/{vhost}/e/{source}/{destination}/{key}", s.hNotImplementedAggregate("binding creation"))
	post("/api/bindings/{vhost}/e/{source}/{destination}/{key}/{props}", s.hNotImplementedAggregate("binding creation"))
	del("/api/bindings/{vhost}/e/{source}/{destination}/{key}", s.hNotImplementedAggregate("binding deletion"))
	del("/api/bindings/{vhost}/e/{source}/{destination}/{key}/{props}", s.hNotImplementedAggregate("binding deletion"))

	// --- vhosts (synthetic "/") ---
	get("/api/vhosts", s.hVHosts)
	put("/api/vhosts/{name}", s.hNotImplemented("virtual-host creation"))
	del("/api/vhosts/{name}", s.hVHostDelete)
	get("/api/vhosts/{vhost}/permissions", s.hVHostPermissions)
	get("/api/vhosts/{vhost}/topic-permissions", s.hEmptyList)
	get("/api/vhosts/{vhost}/connections", s.hVHostConnections)
	get("/api/vhosts/{name}", s.hVHost)

	// --- users / permissions (read-only from auth.json) ---
	get("/api/users", s.hUsers)
	get("/api/users/{user}/permissions", s.hUserPermissions)
	get("/api/users/{name}", s.hUser)
	put("/api/users/{name}", s.hNotImplementedUser("runtime user management"))
	del("/api/users/{name}", s.hNotImplementedUser("runtime user management"))
	post("/api/users/{name}", s.hNotImplementedUser("runtime user management"))
	get("/api/permissions", s.hPermissions)
	get("/api/permissions/{vhost}/{user}", s.hPermission)
	put("/api/permissions/{vhost}/{user}", s.hNotImplementedUser("runtime permission management"))
	del("/api/permissions/{vhost}/{user}", s.hNotImplementedUser("runtime permission management"))
	post("/api/auth/hash_password", s.hNotImplementedUser("password hashing"))

	// --- definitions ---
	get("/api/definitions", s.hDefinitionsExport)
	get("/api/definitions/{vhost}", s.hDefinitionsExport)
	post("/api/definitions", s.hDefinitionsImport)
	post("/api/definitions/{vhost}", s.hDefinitionsImport)

	// --- jobs ---
	get("/api/strangeq/jobs/{id}", s.hJob)

	// --- rebalance (no clustering) ---
	post("/api/strangeq/rebalance", s.hNotImplemented("clustering/node rebalance"))

	// --- honest empties ---
	for _, p := range []string{
		"/api/policies", "/api/operator-policies", "/api/parameters",
		"/api/global-parameters", "/api/feature-flags",
		"/api/deprecated-features", "/api/deprecated-features/used",
		"/api/vhost-limits", "/api/user-limits", "/api/federation-links",
		"/api/topic-permissions", "/api/shovels",
	} {
		get(p, s.hEmptyList)
		rt.add(http.MethodPut, p, s.hNotImplementedAbsent())
		rt.add(http.MethodPost, p, s.hNotImplementedAbsent())
		rt.add(http.MethodDelete, p, s.hNotImplementedAbsent())
	}
	rt.add(http.MethodPut, "/api/user-limits/{user}/{name}", s.hNotImplementedAbsent())
	rt.add(http.MethodDelete, "/api/user-limits/{user}/{name}", s.hNotImplementedAbsent())
	rt.add(http.MethodPut, "/api/vhost-limits/{vhost}/{name}", s.hNotImplementedAbsent())
	rt.add(http.MethodDelete, "/api/vhost-limits/{vhost}/{name}", s.hNotImplementedAbsent())

	return rt
}
