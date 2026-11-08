package management

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
)

// This file implements the definitions surface of spec mgmt-05: the exact
// export shape (FR-API-120/121, COMPAT-020/025/026) and the two-phase
// parse -> plan -> apply import (FR-API-122…125, COMPAT-021…024/027, SEC-067).
//
// The planner is a single pure decision procedure shared by the `?dry_run=true`
// preview and the real apply, so the preview can never disagree with the apply
// (D-3). Merge is deliberately NON-ATOMIC (COMPAT-023/BOB-10): a failing entity
// is reported and the loop continues; `strangeq.atomic` is always false.

// --- wire shapes: import payload -------------------------------------------

// definitionsPayload is the whole definitions document. Unknown top-level keys
// (policies, parameters, ...) are ignored by encoding/json (FR-CONV-094), and
// the `strangeq` marker is kept as raw bytes so any producer's marker is
// tolerated.
type definitionsPayload struct {
	Strangeq    json.RawMessage  `json:"strangeq"`
	Vhosts      []defsVhost      `json:"vhosts"`
	Users       []defsUser       `json:"users"`
	Permissions []defsPermission `json:"permissions"`
	Exchanges   []defsExchange   `json:"exchanges"`
	Queues      []defsQueue      `json:"queues"`
	Bindings    []defsBinding    `json:"bindings"`
}

type defsVhost struct {
	Name      string `json:"name"`
	Synthetic bool   `json:"synthetic"`
}

type defsUser struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type defsPermission struct {
	User      string `json:"user"`
	Vhost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

type defsExchange struct {
	Name       string                 `json:"name"`
	Vhost      string                 `json:"vhost"`
	Type       string                 `json:"type"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"auto_delete"`
	Internal   bool                   `json:"internal"`
	Arguments  map[string]interface{} `json:"arguments"`
}

type defsQueue struct {
	Name       string                 `json:"name"`
	Vhost      string                 `json:"vhost"`
	Type       string                 `json:"type"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"auto_delete"`
	Exclusive  bool                   `json:"exclusive"`
	Arguments  map[string]interface{} `json:"arguments"`
	Node       string                 `json:"node"`
}

type defsBinding struct {
	Source          string                 `json:"source"`
	Vhost           string                 `json:"vhost"`
	Destination     string                 `json:"destination"`
	DestinationType string                 `json:"destination_type"`
	RoutingKey      string                 `json:"routing_key"`
	Arguments       map[string]interface{} `json:"arguments"`
	PropertiesKey   string                 `json:"properties_key"`
}

// parseDefinitions decodes a definitions document. An empty or malformed body
// is an error (the caller renders 400). It performs no validation of entity
// types: that is the planner's job, so dry-run and apply share one rule set.
func parseDefinitions(body []byte) (*definitionsPayload, error) {
	var p definitionsPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// --- wire shapes: export ----------------------------------------------------

// defsStrangeQ is the divergence marker a RabbitMQ client reads to detect the
// StrangeQ subset (FR-API-120/121, COMPAT-020/023/025).
type defsStrangeQ struct {
	Product          string   `json:"product"`
	Version          string   `json:"version"`
	ExportedAt       string   `json:"exported_at"`
	Subset           bool     `json:"subset"`
	Atomic           bool     `json:"atomic"`
	Excludes         []string `json:"excludes"`
	ExcludedSections []string `json:"excluded_sections"`
}

type defsVhostOut struct {
	Name      string `json:"name"`
	Synthetic bool   `json:"synthetic"`
}

type defsUserOut struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

type defsPermissionOut struct {
	User      string `json:"user"`
	Vhost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

type defsExchangeOut struct {
	Name       string `json:"name"`
	Vhost      string `json:"vhost"`
	Type       string `json:"type"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Internal   bool   `json:"internal"`
	Arguments  any    `json:"arguments"`
}

type defsQueueOut struct {
	Name       string `json:"name"`
	Vhost      string `json:"vhost"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Exclusive  bool   `json:"exclusive"`
	Arguments  any    `json:"arguments"`
}

type defsBindingOut struct {
	Source          string `json:"source"`
	Vhost           string `json:"vhost"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
	Arguments       any    `json:"arguments"`
	PropertiesKey   string `json:"properties_key"`
}

// definitionsExport is the exact export document (FR-API-120/121). Struct field
// order is the emitted JSON key order (FR-CONV-046). `users`/`permissions` are
// nil pointers (omitted) when the principal lacks manage_users; when included
// they are always present, as an empty array if there is no user store
// (AUTH-024). Pointers are used because a plain slice with omitempty would omit
// an empty-but-present section.
type definitionsExport struct {
	StrangeQ    defsStrangeQ         `json:"strangeq"`
	Vhosts      []defsVhostOut       `json:"vhosts"`
	Users       *[]defsUserOut       `json:"users,omitempty"`
	Permissions *[]defsPermissionOut `json:"permissions,omitempty"`
	Exchanges   []defsExchangeOut    `json:"exchanges"`
	Queues      []defsQueueOut       `json:"queues"`
	Bindings    []defsBindingOut     `json:"bindings"`
}

// defsExcludes is the fixed list of things a StrangeQ export never carries
// (SEC-070, COMPAT-025/026). It is stated on the wire so a client can see the
// divergence rather than infer it.
//
// "binary_field_table_values_become_base64_strings" (LOW-6) is a MAPPING
// divergence, not an omission: a field-table value is emitted through
// fieldTableJSON (FR-CONV-047), so a []byte (or non-UTF-8 long string) becomes
// a base64 JSON STRING and a time.Time becomes integer seconds. A headers
// binding re-imported from the export therefore carries a string, not bytes.
// The properties_key is unaffected: it is the SHA-256 of that SAME canonical
// JSON, so a key exported from GET /api/bindings still matches the DELETE
// endpoint (DIV-12). See TestFieldTableJSON_ValueMappingDivergence.
var defsExcludes = []string{
	"user_password_hashes",
	"policies",
	"parameters",
	"vhosts_other_than_root",
	"implicit_default_bindings",
	"binary_field_table_values_become_base64_strings",
}

// exportTimeFormat is RFC 3339 UTC with milliseconds (FR-CONV-042). It is the
// ONLY volatile field in an export, so two exports of an unchanged topology
// differ only here (COMPAT-026).
const exportTimeFormat = "2006-01-02T15:04:05.000Z"

// --- binding seam -----------------------------------------------------------

// The aggregate binding seam is broker.BindingView / ListBindings, declared by
// BOB-4 (broker/management_bindings.go) and discovered here through the same
// optional `bindingLister` interface handlers_bindings.go uses (snapshot.go).
// Production's *server.StorageBrokerAdapter always implements it, so bindings
// are exported; a custom UnifiedBroker without it degrades to `bindings: []`
// plus an `excluded_sections` entry (mgmt-05 §1).

// --- topology ---------------------------------------------------------------

// defsTopology is the current broker topology read ONCE by the planner. It
// performs one GetExchanges/GetQueues read plus, when the seam exists, one
// lock-free ListBindings call (mgmt-05 §6: no forbidden lock is taken).
type defsTopology struct {
	exchanges   map[string]defsExchange
	queues      map[string]defsQueue
	bindings    []broker.BindingView
	bindingSeam bool
}

// currentTopology reads the broker topology ONCE. It returns an error only when
// the aggregate binding seam is present but its ListBindings call fails: that
// must not be silently treated as an empty binding set, or the export would
// claim `bindings: []` while capabilities.bindings is true and the import could
// exceed MaxBindings (review finding 1). An absent seam is NOT an error — it
// degrades byte-for-byte to bindings:[] + excluded_sections:["bindings"].
func (s *Server) currentTopology() (defsTopology, error) {
	t := defsTopology{
		exchanges: map[string]defsExchange{},
		queues:    map[string]defsQueue{},
	}
	b := s.broker()
	if b == nil {
		return t, nil
	}
	for name, ex := range b.GetExchanges() {
		t.exchanges[name] = defsExchange{
			Name:       ex.Name,
			Vhost:      "/",
			Type:       exchangeTypeName(ex.Kind),
			Durable:    ex.Durable,
			AutoDelete: ex.AutoDelete,
			Internal:   ex.Internal,
			Arguments:  ex.Arguments,
		}
	}
	for name, q := range b.GetQueues() {
		t.queues[name] = defsQueue{
			Name:       q.Name,
			Vhost:      "/",
			Type:       "classic",
			Durable:    q.Durable,
			AutoDelete: q.AutoDelete,
			Exclusive:  q.Exclusive,
			Arguments:  q.Arguments,
		}
	}
	if lister, ok := b.(bindingLister); ok {
		views, err := lister.ListBindings()
		if err != nil {
			// A half-read topology is not a valid planning input: refuse it
			// rather than plan against an empty binding set.
			return t, fmt.Errorf("could not list bindings: %w", err)
		}
		t.bindings = views
		t.bindingSeam = true
	}
	return t, nil
}

// --- planner ----------------------------------------------------------------

type defsActionKind string

const (
	defExchange defsActionKind = "exchange"
	defQueue    defsActionKind = "queue"
	defBinding  defsActionKind = "binding"
	defVhost    defsActionKind = "vhost"
	defUser     defsActionKind = "user"
	defPerm     defsActionKind = "permission"
)

// defsCaps is the management.definitions.* cap set. Zero means unlimited.
type defsCaps struct {
	MaxQueues    int
	MaxExchanges int
	MaxBindings  int
}

// defsAction is one planned merge decision. Result is "created"/"updated" for an
// apply action, or "skipped"/"failed" for a note action. Apply is non-nil only
// for apply actions; a note action never touches the broker.
type defsAction struct {
	Kind    defsActionKind
	Name    string
	Vhost   string
	Result  string
	Reason  string
	Binding *broker.BindingView // set for binding actions (synthesised key included)
	Key     string              // synthesised properties_key (bindings)
	rtype   interfaces.ResourceType
	Apply   func() error
}

func defsNote(kind defsActionKind, name, vhost, result, reason string) defsAction {
	return defsAction{Kind: kind, Name: name, Vhost: vhost, Result: result, Reason: reason}
}

// planDefinitions is the single pure merge decision procedure (D-3). It reads
// the topology once (in cur) and returns the ordered actions in apply order:
// vhosts -> exchanges -> queues -> bindings -> users/permissions. It performs
// no broker mutation; the Apply closures it returns are invoked only by the
// apply phase, never by dry-run.
//
// Caps (COMPAT-027) are a per-vhost ceiling on the TOTAL entity count, not a
// per-request create budget: a NEW entity is refused when
// existing+planned >= cap, so a broker already at/over the cap cannot accept an
// unbounded stream of imports. A payload entity that already exists is an
// "updated" apply (no new entity), so it never counts against the cap and a
// re-import of an unchanged topology can never trip it.
//
// CAP-maxExchanges counts NON-SYSTEM exchanges only: the broker's own exchange
// cap excludes the default exchange ("") and every amq.* built-in, so a finite
// definitions cap that counted them could never be reached. See
// countUserExchanges.
func (s *Server) planDefinitions(p *definitionsPayload, cur defsTopology, caps defsCaps) []defsAction {
	var actions []defsAction

	// 1. vhosts: only the synthetic "/" exists (D-1).
	for _, v := range p.Vhosts {
		if v.Name == "/" {
			actions = append(actions, defsNote(defVhost, "/", "/", "skipped", "already exists"))
		} else {
			actions = append(actions, defsNote(defVhost, v.Name, "", "failed", "only the default vhost is supported (D-1)"))
		}
	}

	// 2. exchanges.
	createdExchanges := 0
	for i := range p.Exchanges {
		ex := p.Exchanges[i]
		name := strings.TrimSpace(ex.Name)
		if name == "" {
			actions = append(actions, defsNote(defExchange, "", "/", "failed", "missing name"))
			continue
		}
		if !validExchangeTypes[ex.Type] {
			actions = append(actions, defsNote(defExchange, name, "/", "failed", "unsupported type"))
			continue
		}
		_, existed := cur.exchanges[name]
		if !existed {
			// CAP-maxExchanges counts non-system exchanges because the broker's
			// own cap excludes ""/amq.* (see countUserExchanges).
			if caps.MaxExchanges > 0 && countUserExchanges(cur.exchanges)+createdExchanges >= caps.MaxExchanges {
				actions = append(actions, defsNote(defExchange, name, "/", "failed", "exchange cap exceeded (COMPAT-027)"))
				continue
			}
			createdExchanges++
		}
		result := "created"
		if existed {
			result = "updated"
		}
		e := ex
		e.Name = name
		actions = append(actions, defsAction{
			Kind:   defExchange,
			Name:   name,
			Vhost:  "/",
			Result: result,
			rtype:  interfaces.ResourceExchange,
			Apply: func() error {
				return s.broker().DeclareExchange(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.Arguments)
			},
		})
	}

	// 3. queues.
	createdQueues := 0
	for i := range p.Queues {
		q := p.Queues[i]
		name := strings.TrimSpace(q.Name)
		if name == "" {
			actions = append(actions, defsNote(defQueue, "", "/", "failed", "missing name"))
			continue
		}
		if q.Type != "" && q.Type != "classic" {
			actions = append(actions, defsNote(defQueue, name, "/", "failed", "unsupported queue type"))
			continue
		}
		_, existed := cur.queues[name]
		if !existed {
			if caps.MaxQueues > 0 && len(cur.queues)+createdQueues >= caps.MaxQueues {
				actions = append(actions, defsNote(defQueue, name, "/", "failed", "queue cap exceeded (COMPAT-027)"))
				continue
			}
			createdQueues++
		}
		// AMQP redeclare-equivalence contract: an existing queue whose durable/
		// auto_delete/exclusive/arguments differ is REFUSED by the broker (it is
		// never overwritten) and is reported here as `failed` with the broker's
		// equivalence reason. A "mutable conflict overwrite" primitive is out of
		// scope for mgmt-05 (§7.2 test 8 pins this behaviour).
		result := "created"
		if existed {
			result = "updated"
		}
		qq := q
		qq.Name = name
		actions = append(actions, defsAction{
			Kind:   defQueue,
			Name:   name,
			Vhost:  "/",
			Result: result,
			rtype:  interfaces.ResourceQueue,
			Apply: func() error {
				_, err := s.broker().DeclareQueue(qq.Name, qq.Durable, qq.AutoDelete, qq.Exclusive, qq.Arguments)
				return err
			},
		})
	}

	// 4. bindings. Deduplicated against cur.bindings by the SYNTHESISED
	// properties_key plus (source, destination, destination_type); a payload
	// properties_key is ignored and recomputed (D-8).
	existing := make(map[string]struct{}, len(cur.bindings))
	for _, vb := range cur.bindings {
		existing[bindingIdentity(vb.Source, vb.Destination, vb.DestinationType, propertiesKey(vb.RoutingKey, vb.Arguments))] = struct{}{}
	}
	createdBindings := 0
	for i := range p.Bindings {
		bn := p.Bindings[i]
		// A binding whose identity cannot even be computed still carries the
		// source/destination/properties_key it was given, so its failure item is
		// as identifying as its skip item (§2.4 MUST).
		view := &broker.BindingView{Source: bn.Source, Destination: bn.Destination, DestinationType: bn.DestinationType, RoutingKey: bn.RoutingKey, Arguments: bn.Arguments}
		synth := propertiesKey(bn.RoutingKey, bn.Arguments)
		if bn.DestinationType != "queue" && bn.DestinationType != "exchange" {
			actions = append(actions, defsAction{Kind: defBinding, Name: bn.Destination, Vhost: "/", Result: "failed", Reason: "destination_type must be queue or exchange", Binding: view, Key: synth, rtype: interfaces.ResourceQueue})
			continue
		}
		rtype := interfaces.ResourceQueue
		if bn.DestinationType == "exchange" {
			rtype = interfaces.ResourceExchange
		}
		key := bindingIdentity(bn.Source, bn.Destination, bn.DestinationType, synth)
		if _, dup := existing[key]; dup {
			actions = append(actions, defsAction{Kind: defBinding, Name: bn.Destination, Vhost: "/", Result: "skipped", Reason: "equivalent", Binding: view, Key: synth, rtype: rtype})
			continue
		}
		if caps.MaxBindings > 0 && len(cur.bindings)+createdBindings >= caps.MaxBindings {
			actions = append(actions, defsAction{Kind: defBinding, Name: bn.Destination, Vhost: "/", Result: "failed", Reason: "binding cap exceeded (COMPAT-027)", Binding: view, Key: synth, rtype: rtype})
			continue
		}
		createdBindings++
		existing[key] = struct{}{}
		b := bn
		action := defsAction{Kind: defBinding, Name: bn.Destination, Vhost: "/", Result: "created", Binding: view, Key: synth, rtype: rtype}
		if bn.DestinationType == "queue" {
			action.Apply = func() error { return s.broker().BindQueue(b.Destination, b.Source, b.RoutingKey, b.Arguments) }
		} else {
			action.Apply = func() error { return s.broker().BindExchange(b.Destination, b.Source, b.RoutingKey, b.Arguments) }
		}
		actions = append(actions, action)
	}

	// 5. users / permissions: always skipped with a reason (BOB-6, FR-API-125,
	// COMPAT-024). Standalone user/permission writes remain 501.
	for _, u := range p.Users {
		actions = append(actions, defsNote(defUser, u.Name, "", "skipped", defsUsersSkippedReason))
	}
	for _, pm := range p.Permissions {
		actions = append(actions, defsNote(defPerm, pm.User, pm.Vhost, "skipped", defsUsersSkippedReason))
	}

	return actions
}

const defsUsersSkippedReason = "user/permission management is not implemented (BOB-6)"

// countUserExchanges counts the NON-SYSTEM exchanges of a topology: the default
// exchange ("") and every amq.* built-in are excluded, exactly as the broker's
// own MaxExchanges backstop excludes them (COMPAT-027/CAP-maxExchanges).
func countUserExchanges(m map[string]defsExchange) int {
	n := 0
	for name := range m {
		if name == "" || strings.HasPrefix(name, "amq.") {
			continue
		}
		n++
	}
	return n
}

// --- executor ---------------------------------------------------------------

// defsResult is one wire item of applied/skipped/failed. Field order is fixed
// (FR-CONV-046). Name is used for exchanges/queues/users/permissions; the
// source/destination fields are used for bindings.
type defsResult struct {
	Type            string `json:"type"`
	Name            string `json:"name,omitempty"`
	Vhost           string `json:"vhost,omitempty"`
	Source          string `json:"source,omitempty"`
	Destination     string `json:"destination,omitempty"`
	DestinationType string `json:"destination_type,omitempty"`
	PropertiesKey   string `json:"properties_key,omitempty"`
	Result          string `json:"result,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// defsImportResultStrangeQ is the per-response divergence marker.
type defsImportResultStrangeQ struct {
	Subset bool `json:"subset"`
	Atomic bool `json:"atomic"`
	DryRun bool `json:"dry_run"`
}

// defsImportResponse is the import response body (mgmt-05 §2.4).
type defsImportResponse struct {
	Applied  []defsResult             `json:"applied"`
	Skipped  []defsResult             `json:"skipped"`
	Failed   []defsResult             `json:"failed"`
	StrangeQ defsImportResultStrangeQ `json:"strangeq"`
}

// defsImportJobResult is the async job's result payload.
type defsImportJobResult struct {
	Applied []defsResult `json:"applied"`
	Skipped []defsResult `json:"skipped"`
	Failed  []defsResult `json:"failed"`
}

// runDefinitions executes planned actions. It is shared verbatim by dry-run and
// apply, so a preview cannot diverge from the real merge (D-3). `applied` and
// `failed` are disjoint: an entity whose Apply returned an error is never
// reported applied (FR-API-123), and the loop always continues (COMPAT-023).
func (s *Server) runDefinitions(actions []defsAction, p *Principal, dryRun bool) (applied, skipped, failed []defsResult) {
	applied = []defsResult{}
	skipped = []defsResult{}
	failed = []defsResult{}
	for _, a := range actions {
		if a.Apply == nil {
			if a.Result == "failed" {
				failed = append(failed, failedResult(a, a.Reason))
			} else {
				skipped = append(skipped, noteResult(a))
			}
			continue
		}
		// Per-target configure authorization (AUTH-016/AUTH-012): a denied
		// target is reported failed with a non-disclosing reason and never
		// declared. The rest of the import still applies.
		if !s.definitionsConfigureAllowed(p, a.rtype, a.Name) {
			failed = append(failed, failedResult(a, "access refused"))
			continue
		}
		if dryRun {
			applied = append(applied, applyResult(a))
			continue
		}
		if err := a.Apply(); err != nil {
			failed = append(failed, failedResult(a, err.Error()))
			continue
		}
		applied = append(applied, applyResult(a))
	}
	return applied, skipped, failed
}

// failedResult renders one failed item. A binding failure carries its full
// identity — source, destination, destination_type and the synthesised
// properties_key — exactly like a skipped binding (§2.4 MUST); every other
// kind carries {type,name,vhost,reason}.
func failedResult(a defsAction, reason string) defsResult {
	if a.Kind == defBinding && a.Binding != nil {
		return defsResult{
			Type:            string(a.Kind),
			Source:          a.Binding.Source,
			Destination:     a.Binding.Destination,
			DestinationType: a.Binding.DestinationType,
			Vhost:           a.Vhost,
			PropertiesKey:   a.Key,
			Reason:          sanitizedReason(reason),
		}
	}
	return defsResult{Type: string(a.Kind), Name: a.Name, Vhost: a.Vhost, Reason: sanitizedReason(reason)}
}

func applyResult(a defsAction) defsResult {
	if a.Kind == defBinding && a.Binding != nil {
		return defsResult{
			Type:            string(a.Kind),
			Source:          a.Binding.Source,
			Destination:     a.Binding.Destination,
			DestinationType: a.Binding.DestinationType,
			Vhost:           a.Vhost,
			PropertiesKey:   a.Key,
			Result:          a.Result,
		}
	}
	return defsResult{Type: string(a.Kind), Name: a.Name, Vhost: a.Vhost, Result: a.Result}
}

func noteResult(a defsAction) defsResult {
	if a.Kind == defBinding && a.Binding != nil {
		return defsResult{
			Type:            string(a.Kind),
			Source:          a.Binding.Source,
			Destination:     a.Binding.Destination,
			DestinationType: a.Binding.DestinationType,
			Vhost:           a.Vhost,
			PropertiesKey:   a.Key,
			Reason:          sanitizedReason(a.Reason),
		}
	}
	return defsResult{Type: string(a.Kind), Name: a.Name, Vhost: a.Vhost, Reason: sanitizedReason(a.Reason)}
}

// sanitizedReason strips control characters and caps length (SEC-085).
func sanitizedReason(s string) string { return sanitizeField(s, 256) }

// countApply reports how many actions would touch the broker.
func countApply(actions []defsAction) int {
	n := 0
	for _, a := range actions {
		if a.Apply != nil {
			n++
		}
	}
	return n
}

// properties_key synthesis lives in jsonc.go (the canonical `propertiesKey`,
// introduced by BOB-4): routing_key alone for empty arguments, else routing_key
// + "~" + SHA-256 of the canonical field-table JSON. The binding endpoints and
// the definitions export/import MUST share that one function so a key produced
// by an export round-trips into DELETE /api/bindings/... (FR-API-090, DIV-12).

func bindingIdentity(source, destination, destinationType, propsKey string) string {
	var b strings.Builder
	b.Grow(len(source) + len(destination) + len(destinationType) + len(propsKey) + 3)
	b.WriteString(source)
	b.WriteByte(0)
	b.WriteString(destinationType)
	b.WriteByte(0)
	b.WriteString(destination)
	b.WriteByte(0)
	b.WriteString(propsKey)
	return b.String()
}

// --- sort helpers -----------------------------------------------------------

func sortExchangeOuts(items []defsExchangeOut) {
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
}

func sortQueueOuts(items []defsQueueOut) {
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
}

func sortBindingOuts(items []defsBindingOut) {
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.DestinationType != b.DestinationType {
			return a.DestinationType < b.DestinationType
		}
		if a.Destination != b.Destination {
			return a.Destination < b.Destination
		}
		return a.PropertiesKey < b.PropertiesKey
	})
}

func sortUserOuts(items []defsUserOut) {
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
}

func sortPermissionOuts(items []defsPermissionOut) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].User != items[j].User {
			return items[i].User < items[j].User
		}
		return items[i].Vhost < items[j].Vhost
	})
}

// --- export builder ---------------------------------------------------------

// buildDefinitionsExport renders the exact export document (FR-API-120/121).
// manageUsers controls whether the user/permission sections are present
// (AUTH-024); when withheld, both keys are omitted and named in
// strangeq.excluded_sections. The only volatile field is exported_at.
func (s *Server) buildDefinitionsExport(cur defsTopology, manageUsers bool) definitionsExport {
	excludes := append([]string(nil), defsExcludes...)
	excludedSections := []string{}
	if !manageUsers {
		excludedSections = append(excludedSections, "users", "permissions")
	}
	if !cur.bindingSeam {
		excludedSections = append(excludedSections, "bindings")
	}

	exchanges := make([]defsExchangeOut, 0, len(cur.exchanges))
	for _, ex := range cur.exchanges {
		// The default exchange ("") is implicit and cannot be re-declared, so it
		// is never exported (mgmt-05 §11: implicit default-exchange entities are
		// excluded by design).
		if ex.Name == "" {
			continue
		}
		exchanges = append(exchanges, defsExchangeOut{
			Name:       ex.Name,
			Vhost:      "/",
			Type:       ex.Type,
			Durable:    ex.Durable,
			AutoDelete: ex.AutoDelete,
			Internal:   ex.Internal,
			Arguments:  fieldTableJSON(ex.Arguments),
		})
	}
	sortExchangeOuts(exchanges)

	queues := make([]defsQueueOut, 0, len(cur.queues))
	for _, q := range cur.queues {
		queues = append(queues, defsQueueOut{
			Name:       q.Name,
			Vhost:      "/",
			Durable:    q.Durable,
			AutoDelete: q.AutoDelete,
			Exclusive:  q.Exclusive,
			Arguments:  fieldTableJSON(q.Arguments),
		})
	}
	sortQueueOuts(queues)

	bindings := make([]defsBindingOut, 0, len(cur.bindings))
	if cur.bindingSeam {
		for _, vb := range cur.bindings {
			bindings = append(bindings, defsBindingOut{
				Source:          vb.Source,
				Vhost:           "/",
				Destination:     vb.Destination,
				DestinationType: vb.DestinationType,
				RoutingKey:      vb.RoutingKey,
				Arguments:       fieldTableJSON(vb.Arguments),
				PropertiesKey:   propertiesKey(vb.RoutingKey, vb.Arguments),
			})
		}
	}
	sortBindingOuts(bindings)

	out := definitionsExport{
		StrangeQ: defsStrangeQ{
			Product:          "StrangeQ",
			Version:          s.version(),
			ExportedAt:       time.Now().UTC().Format(exportTimeFormat),
			Subset:           true,
			Atomic:           false,
			Excludes:         excludes,
			ExcludedSections: excludedSections,
		},
		Vhosts:    []defsVhostOut{{Name: "/", Synthetic: true}},
		Exchanges: exchanges,
		Queues:    queues,
		Bindings:  bindings,
	}
	if manageUsers {
		users := s.users.list()
		userOut := make([]defsUserOut, 0, len(users))
		for _, u := range users {
			tags := u.Tags
			if tags == nil {
				tags = []string{}
			}
			userOut = append(userOut, defsUserOut{Name: u.Name, Tags: tags})
		}
		sortUserOuts(userOut)
		permOut := make([]defsPermissionOut, 0, len(s.users.permissions()))
		for _, pm := range s.users.permissions() {
			permOut = append(permOut, defsPermissionOut{
				User: pm.User, Vhost: pm.VHost, Configure: pm.Configure, Write: pm.Write, Read: pm.Read,
			})
		}
		sortPermissionOuts(permOut)
		out.Users = &userOut
		out.Permissions = &permOut
	}
	return out
}
