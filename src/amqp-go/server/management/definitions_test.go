package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
)

// definitionsImportRequest is a loose test-side payload builder. The fields are
// maps so a test can express a partial or intentionally odd document without a
// rigid Go struct.
type definitionsImportRequest struct {
	VHosts      []map[string]any `json:"vhosts,omitempty"`
	Exchanges   []map[string]any `json:"exchanges,omitempty"`
	Queues      []map[string]any `json:"queues,omitempty"`
	Bindings    []map[string]any `json:"bindings,omitempty"`
	Users       []map[string]any `json:"users,omitempty"`
	Permissions []map[string]any `json:"permissions,omitempty"`
	Parameters  []map[string]any `json:"parameters,omitempty"`
}

// --- result helpers ---------------------------------------------------------

func appliedNames(res defsImportResponse) []string { return resultNames(res.Applied) }
func failedNames(res defsImportResponse) []string  { return resultNames(res.Failed) }

func resultNames(items []defsResult) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		name := it.Name
		if name == "" {
			name = it.Destination
		}
		out = append(out, it.Type+":"+name)
	}
	return out
}

func containsName(res defsImportResponse, list []defsResult, typ, name string) bool {
	for _, it := range list {
		got := it.Name
		if got == "" {
			got = it.Destination
		}
		if it.Type == typ && got == name {
			return true
		}
	}
	return false
}

func importDefs(t *testing.T, e *env, query string, body []byte, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	if hdr == nil {
		hdr = basicAuthHeader()
	}
	return e.do(http.MethodPost, "/api/definitions"+query, body, hdr)
}

// --- export -----------------------------------------------------------------

func TestDefinitionsExport_ShapeAndSubset(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	var doc struct {
		Strangeq struct {
			Product  string   `json:"product"`
			Version  string   `json:"version"`
			Subset   bool     `json:"subset"`
			Atomic   bool     `json:"atomic"`
			Excludes []string `json:"excludes"`
		} `json:"strangeq"`
		Vhosts []struct {
			Name      string `json:"name"`
			Synthetic bool   `json:"synthetic"`
		} `json:"vhosts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !doc.Strangeq.Subset {
		t.Fatalf("strangeq.subset must be true")
	}
	if doc.Strangeq.Atomic {
		t.Fatalf("strangeq.atomic must be false (BOB-10 unbuilt)")
	}
	if doc.Strangeq.Product != "StrangeQ" || doc.Strangeq.Version == "" {
		t.Fatalf("strangeq identity = %+v", doc.Strangeq)
	}
	for _, want := range []string{"user_password_hashes", "policies", "parameters", "vhosts_other_than_root", "implicit_default_bindings"} {
		if !containsStr(doc.Strangeq.Excludes, want) {
			t.Fatalf("excludes must state %q, got %v", want, doc.Strangeq.Excludes)
		}
	}
	if len(doc.Vhosts) != 1 || doc.Vhosts[0].Name != "/" || !doc.Vhosts[0].Synthetic {
		t.Fatalf("vhosts must be exactly the synthetic root, got %+v", doc.Vhosts)
	}
}

func TestDefinitionsExport_NoHashes(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)
	rec := e.do(http.MethodGet, "/api/definitions", nil, map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")})
	wantStatus(t, rec, http.StatusOK)
	body := rec.Body.String()
	for _, banned := range []string{`"password_hash"`, `"metadata"`, `"groups"`, "$2a$", "$2b$"} {
		if strings.Contains(body, banned) {
			t.Fatalf("export carries forbidden credential material %q: %s", banned, truncate(body))
		}
	}
}

func TestDefinitionsExport_UserSectionGated(t *testing.T) {
	e := inheritEnv(t, `{"users":[
			{"username":"adm","password_hash":"`+secretHash(t)+`","tags":["administrator"],
			 "vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]},
			{"username":"mon","password_hash":"`+secretHash(t)+`","tags":["monitoring"],
			 "vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`, nil)

	adm := e.do(http.MethodGet, "/api/definitions", nil, map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")})
	wantStatus(t, adm, http.StatusOK)
	var admDoc map[string]json.RawMessage
	_ = json.Unmarshal(adm.Body.Bytes(), &admDoc)
	if _, ok := admDoc["users"]; !ok {
		t.Fatalf("manage_users must receive the users section")
	}

	mon := e.do(http.MethodGet, "/api/definitions", nil, map[string]string{"Authorization": "Basic " + basicToken("mon", "secret")})
	wantStatus(t, mon, http.StatusOK)
	var monDoc map[string]json.RawMessage
	_ = json.Unmarshal(mon.Body.Bytes(), &monDoc)
	if _, ok := monDoc["users"]; ok {
		t.Fatalf("a principal without manage_users must not receive the users section")
	}
	if _, ok := monDoc["permissions"]; ok {
		t.Fatalf("a principal without manage_users must not receive the permissions section")
	}
	var marker struct {
		ExcludedSections []string `json:"excluded_sections"`
	}
	_ = json.Unmarshal(monDoc["strangeq"], &marker)
	for _, want := range []string{"users", "permissions"} {
		if !containsStr(marker.ExcludedSections, want) {
			t.Fatalf("excluded_sections must name %q, got %v", want, marker.ExcludedSections)
		}
	}
	// The real broker implements the binding seam, so `bindings` must NOT be
	// excluded (WS5 finding 5: the mirror seam is gone).
	if containsStr(marker.ExcludedSections, "bindings") {
		t.Fatalf("bindings must not be excluded with the real seam present: %v", marker.ExcludedSections)
	}
}

func TestDefinitionsExport_BindingsPopulated(t *testing.T) {
	e := basicEnv(t, true, nil)
	fake := installFakeBindings(t, e)
	fake.seed(broker.BindingView{Source: "events", Destination: "orders", DestinationType: "queue", RoutingKey: "order.*", Arguments: map[string]interface{}{"x-match": "all"}})

	rec := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var doc definitionsExport
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(doc.Bindings) != 1 {
		t.Fatalf("bindings = %+v, want one", doc.Bindings)
	}
	b := doc.Bindings[0]
	if b.Source != "events" || b.Destination != "orders" || b.DestinationType != "queue" {
		t.Fatalf("binding identity = %+v", b)
	}
	if want := propertiesKey("order.*", map[string]interface{}{"x-match": "all"}); b.PropertiesKey != want {
		t.Fatalf("properties_key = %q, want the synthesised %q", b.PropertiesKey, want)
	}
	if containsStr(doc.StrangeQ.ExcludedSections, "bindings") {
		t.Fatalf("with the seam present, bindings must NOT be listed as excluded: %v", doc.StrangeQ.ExcludedSections)
	}
}

// TestDefinitionsExport_BindingsAbsentSeam restores the absent-seam
// definitions-layer pin: when the injected broker has no aggregate binding seam
// (embedding UnifiedBroker does not promote ListBindings), the export degrades
// byte-for-byte to `bindings: []` plus `excluded_sections:["bindings"]` and
// stays 200 — it must NOT be swept up by finding 1's 503.
func TestDefinitionsExport_BindingsAbsentSeam(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.srv.Broker = noTotalsBroker{UnifiedBroker: e.srv.Broker}

	rec := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var doc definitionsExport
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(doc.Bindings) != 0 {
		t.Fatalf("bindings = %+v, want []", doc.Bindings)
	}
	if !strings.Contains(rec.Body.String(), `"bindings":[]`) {
		t.Fatalf("the absent seam must export an empty bindings array: %s", truncate(rec.Body.String()))
	}
	if !containsStr(doc.StrangeQ.ExcludedSections, "bindings") {
		t.Fatalf("the absent seam must be disclosed in excluded_sections: %v", doc.StrangeQ.ExcludedSections)
	}
}

// errBindingsBroker has the aggregate binding seam but its ListBindings errors.
// currentTopology must surface that error so export/import refuse with 503
// instead of silently planning against an empty binding set (review finding 1).
type errBindingsBroker struct {
	server.UnifiedBroker
}

func (b errBindingsBroker) ListBindings() ([]broker.BindingView, error) {
	return nil, errors.New("injected ListBindings failure")
}

// TestDefinitions_BindingSeamErrorRefuses503 pins review finding 1: a
// ListBindings error must make BOTH export and import an explicit 503
// (unavailable), matching /api/bindings, rather than a 200 over `bindings: []`
// that can exceed MaxBindings.
func TestDefinitions_BindingSeamErrorRefuses503(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0
	})
	e.srv.Broker = errBindingsBroker{UnifiedBroker: e.srv.Broker}

	exp := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, exp, http.StatusServiceUnavailable)
	if env := decode[errorEnvelope](t, exp); env.Error != errUnavailable {
		t.Fatalf("export token = %q, want unavailable", env.Error)
	}

	doc := definitionsImportRequest{Queues: []map[string]any{{"name": "seam-err-q"}}}
	imp := importDefs(t, e, "", e.jsonBody(t, doc), nil)
	wantStatus(t, imp, http.StatusServiceUnavailable)
	if env := decode[errorEnvelope](t, imp); env.Error != errUnavailable {
		t.Fatalf("import token = %q, want unavailable", env.Error)
	}
	// The refusal happens BEFORE any entity is declared.
	if got := e.do(http.MethodGet, "/api/queues/%2F/seam-err-q", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("a refused import must declare nothing (queue exists: %d)", got.Code)
	}
}

func TestDefinitionsExport_DeterministicOrdering(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "zeta", queueDeclareRequest{})
	e.declareQueue(t, "alpha", queueDeclareRequest{})
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/zex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader()), http.StatusCreated)
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/aex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader()), http.StatusCreated)

	first := exportDoc(t, e)
	second := exportDoc(t, e)
	first.StrangeQ.ExportedAt = ""
	second.StrangeQ.ExportedAt = ""
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("two exports of an unchanged topology differ beyond exported_at:\n%s\n%s", a, b)
	}
	if !sort.SliceIsSorted(first.Queues, func(i, j int) bool { return first.Queues[i].Name < first.Queues[j].Name }) {
		t.Fatalf("queues must be sorted by name: %+v", first.Queues)
	}
	if !sort.SliceIsSorted(first.Exchanges, func(i, j int) bool { return first.Exchanges[i].Name < first.Exchanges[j].Name }) {
		t.Fatalf("exchanges must be sorted by name: %+v", first.Exchanges)
	}
}

func TestDefinitionsExport_VhostOnlyRoot(t *testing.T) {
	e := basicEnv(t, true, nil)
	wantStatus(t, e.do(http.MethodGet, "/api/definitions/%2F", nil, basicAuthHeader()), http.StatusOK)
	other := e.do(http.MethodGet, "/api/definitions/other", nil, basicAuthHeader())
	wantStatus(t, other, http.StatusNotFound)
	if env := decode[errorEnvelope](t, other); env.Error != errNotFound {
		t.Fatalf("unknown vhost token = %q, want not_found", env.Error)
	}
}

// --- import: planning and apply --------------------------------------------

func TestDefinitionsImport_CreateApplySkipFail(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "equiv-q", queueDeclareRequest{})

	doc := definitionsImportRequest{
		VHosts: []map[string]any{{"name": "/"}},
		Exchanges: []map[string]any{
			{"name": "new-ex", "type": "direct"},
			{"name": "bad-ex", "type": "nonsense"},
		},
		Queues: []map[string]any{{"name": "equiv-q"}},
	}
	rec := importDefs(t, e, "", e.jsonBody(t, doc), nil)
	wantStatus(t, rec, http.StatusOK)
	res := decode[defsImportResponse](t, rec)

	if !containsName(res, res.Applied, "exchange", "new-ex") {
		t.Fatalf("new exchange must be applied: %+v", res.Applied)
	}
	if !containsName(res, res.Applied, "queue", "equiv-q") {
		t.Fatalf("an existing queue re-declared is an updated apply: %+v", res.Applied)
	}
	if !containsName(res, res.Skipped, "vhost", "/") {
		t.Fatalf("the synthetic vhost must be skipped: %+v", res.Skipped)
	}
	if !containsName(res, res.Failed, "exchange", "bad-ex") {
		t.Fatalf("an unsupported type must be failed: %+v", res.Failed)
	}
	if res.StrangeQ.Atomic || !res.StrangeQ.Subset || res.StrangeQ.DryRun {
		t.Fatalf("strangeq marker = %+v", res.StrangeQ)
	}
	// Topology mutated for the applied entity only.
	wantStatus(t, e.do(http.MethodGet, "/api/exchanges/%2F/new-ex", nil, basicAuthHeader()), http.StatusOK)
	if got := e.do(http.MethodGet, "/api/exchanges/%2F/bad-ex", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("a failed exchange must not exist (%d)", got.Code)
	}
}

func TestDefinitionsImport_MergeSemantics(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "untouched", queueDeclareRequest{Durable: true})
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/conflict-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct", Durable: true}), basicAuthHeader()), http.StatusCreated)

	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "conflict-ex", "type": "fanout", "durable": true}},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if !containsName(res, res.Failed, "exchange", "conflict-ex") {
		t.Fatalf("an immutable (type) conflict must be reported failed: %+v", res.Failed)
	}
	// An existing entity the payload does not name is untouched.
	wantStatus(t, e.do(http.MethodGet, "/api/queues/%2F/untouched", nil, basicAuthHeader()), http.StatusOK)
}

func TestDefinitionsImport_UsersSkipped200(t *testing.T) {
	e := basicEnv(t, true, nil)
	doc := definitionsImportRequest{
		Users:       []map[string]any{{"name": "bob", "tags": []string{"monitoring"}}},
		Permissions: []map[string]any{{"user": "bob", "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}},
	}
	rec := importDefs(t, e, "", e.jsonBody(t, doc), nil)
	wantStatus(t, rec, http.StatusOK)
	res := decode[defsImportResponse](t, rec)
	if !containsName(res, res.Skipped, "user", "bob") {
		t.Fatalf("users import must be skipped with a reason: %+v", res.Skipped)
	}
	if !containsName(res, res.Skipped, "permission", "bob") {
		t.Fatalf("permissions import must be skipped with a reason: %+v", res.Skipped)
	}
	// The standalone user write stays 501 (BOB-6, D-2).
	standalone := e.do(http.MethodPut, "/api/users/bob", e.jsonBody(t, map[string]any{"password": "x"}), basicAuthHeader())
	wantStatus(t, standalone, http.StatusNotImplemented)
}

func TestDefinitionsImport_NonAtomicContinues(t *testing.T) {
	e := basicEnv(t, true, nil)
	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "na-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "na-q"}},
		Bindings: []map[string]any{
			{"source": "na-ex", "destination": "missing-q", "destination_type": "queue", "routing_key": "k"},
			{"source": "na-ex", "destination": "na-q", "destination_type": "queue", "routing_key": "k"},
		},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if !containsName(res, res.Failed, "binding", "missing-q") {
		t.Fatalf("the failing binding must be reported failed: %+v", res.Failed)
	}
	if !containsName(res, res.Applied, "binding", "na-q") {
		t.Fatalf("a later entity must still be applied after a failure: %+v", res.Applied)
	}
}

// TestDefinitionsImport_FailedBindingCarriesIdentity pins §2.4: a FAILED
// binding carries source/destination/destination_type/properties_key, exactly
// like a skipped one — for the apply-error path AND the destination_type
// validation path.
func TestDefinitionsImport_FailedBindingCarriesIdentity(t *testing.T) {
	e := basicEnv(t, true, nil)
	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "na-ex", "type": "direct"}},
		Bindings: []map[string]any{
			{"source": "na-ex", "destination": "missing-q", "destination_type": "queue", "routing_key": "k"},
			{"source": "na-ex", "destination": "q2", "destination_type": "topic", "routing_key": "k2"},
		},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if len(res.Failed) != 2 {
		t.Fatalf("both bindings must fail, got %+v", res.Failed)
	}
	applyErr := res.Failed[0]
	if applyErr.Type != "binding" || applyErr.Source != "na-ex" || applyErr.Destination != "missing-q" ||
		applyErr.DestinationType != "queue" || applyErr.PropertiesKey != "k" || applyErr.Reason == "" {
		t.Fatalf("apply-error binding lost identity: %+v", applyErr)
	}
	badType := res.Failed[1]
	if badType.Type != "binding" || badType.Source != "na-ex" || badType.Destination != "q2" ||
		badType.DestinationType != "topic" || badType.PropertiesKey != "k2" || badType.Reason == "" {
		t.Fatalf("destination_type-failure binding lost identity: %+v", badType)
	}
}

// TestDefinitionsImport_MutableConflictFails pins §7.2 test 8 / COMPAT-022: a
// conflicting mutable property (durable) is REFUSED, never overwritten, and is
// reported failed with the broker's equivalence reason while the original
// queue is unchanged.
func TestDefinitionsImport_MutableConflictFails(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "mut-q", queueDeclareRequest{Durable: true})

	doc := definitionsImportRequest{Queues: []map[string]any{{"name": "mut-q", "durable": false}}}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if len(res.Failed) != 1 || res.Failed[0].Type != "queue" || res.Failed[0].Name != "mut-q" {
		t.Fatalf("a mutable conflict must be failed: %+v", res.Failed)
	}
	if !strings.Contains(res.Failed[0].Reason, "durable") {
		t.Fatalf("the failure must name the conflicting property: %+v", res.Failed[0])
	}
	var q struct {
		Durable bool `json:"durable"`
	}
	got := e.do(http.MethodGet, "/api/queues/%2F/mut-q", nil, basicAuthHeader())
	wantStatus(t, got, http.StatusOK)
	if err := json.Unmarshal(got.Body.Bytes(), &q); err != nil {
		t.Fatalf("decode queue: %v", err)
	}
	if !q.Durable {
		t.Fatalf("the original queue must be unchanged (durable:true)")
	}
}

func TestDefinitionsImport_OverCapFailsEntity(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Definitions.MaxQueues = 1
		cfg.Management.Definitions.MaxExchanges = 1
		cfg.Management.Definitions.MaxBindings = 1
	})
	doc := definitionsImportRequest{
		Exchanges: []map[string]any{
			{"name": "cap-ex-1", "type": "direct"},
			{"name": "cap-ex-2", "type": "direct"},
		},
		Queues: []map[string]any{
			{"name": "cap-q-1"},
			{"name": "cap-q-2"},
		},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if !containsName(res, res.Applied, "exchange", "cap-ex-1") || !containsName(res, res.Applied, "queue", "cap-q-1") {
		t.Fatalf("the first of each kind must be applied: %+v", res.Applied)
	}
	if !containsName(res, res.Failed, "exchange", "cap-ex-2") || !containsName(res, res.Failed, "queue", "cap-q-2") {
		t.Fatalf("over-cap entities must be failed: %+v", res.Failed)
	}

	// max_bindings is a TOTAL ceiling: seed one binding with max=1 and BOTH
	// payload bindings must fail (existing + planned already at the cap).
	e2 := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Definitions.MaxBindings = 1
	})
	e2.declareQueue(t, "capb-q2", queueDeclareRequest{})
	e2.declareQueue(t, "capb-q3", queueDeclareRequest{})
	fake := installFakeBindings(t, e2)
	fake.seed(broker.BindingView{Source: "amq.topic", Destination: "capb-q1", DestinationType: "queue", RoutingKey: "a"})
	bdoc := definitionsImportRequest{Bindings: []map[string]any{
		{"source": "amq.topic", "destination": "capb-q2", "destination_type": "queue", "routing_key": "b"},
		{"source": "amq.topic", "destination": "capb-q3", "destination_type": "queue", "routing_key": "c"},
	}}
	bres := decode[defsImportResponse](t, importDefs(t, e2, "", e2.jsonBody(t, bdoc), nil))
	if len(bres.Applied) != 0 {
		t.Fatalf("a broker at the binding cap must accept no new binding: %+v", bres.Applied)
	}
	for _, name := range []string{"capb-q2", "capb-q3"} {
		if !containsName(bres, bres.Failed, "binding", name) {
			t.Fatalf("binding %s must be failed at the cap: %+v", name, bres.Failed)
		}
	}
}

// TestDefinitionsImport_CapCountsExistingQueue pins CAP-maxQueues as a TOTAL
// ceiling: a broker already holding one queue at max_queues=1 refuses a new one.
func TestDefinitionsImport_CapCountsExistingQueue(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Definitions.MaxQueues = 1
		cfg.Management.Definitions.MaxExchanges = 1
	})
	e.declareQueue(t, "existing-q", queueDeclareRequest{})
	wantStatus(t, e.do(http.MethodPut, "/api/exchanges/%2F/existing-ex", e.jsonBody(t, exchangeDeclareRequest{Type: "direct"}), basicAuthHeader()), http.StatusCreated)

	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "new-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "new-q"}},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if len(res.Applied) != 0 {
		t.Fatalf("a broker at the cap must accept no new entity: %+v", res.Applied)
	}
	if !containsName(res, res.Failed, "queue", "new-q") || !containsName(res, res.Failed, "exchange", "new-ex") {
		t.Fatalf("over-cap entities must be failed: %+v", res.Failed)
	}
}

// TestDefinitionsImport_BindingDedupRealBroker pins the REAL binding seam
// (WS5 review finding 5): definitions.go's currentTopology consumes the same
// broker.BindingView seam handlers_bindings.go names, so against the real
// StorageBrokerAdapter the planner sees the live topology. Importing one
// binding TWICE must report it `skipped`/"equivalent" the second time and
// create nothing — the pre-seam code re-created it (the gap this test used to
// document).
func TestDefinitionsImport_BindingDedupRealBroker(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0
	})
	e.declareQueue(t, "seam-q", queueDeclareRequest{})
	doc := definitionsImportRequest{Bindings: []map[string]any{
		{"source": "amq.topic", "destination": "seam-q", "destination_type": "queue", "routing_key": "k"},
	}}
	body := e.jsonBody(t, doc)

	first := decode[defsImportResponse](t, importDefs(t, e, "", body, nil))
	if !containsName(first, first.Applied, "binding", "seam-q") {
		t.Fatalf("first import must report the binding created: %+v", first.Applied)
	}
	second := decode[defsImportResponse](t, importDefs(t, e, "", body, nil))
	if len(second.Applied) != 0 {
		t.Fatalf("the real seam must dedup the existing binding: applied=%+v", second.Applied)
	}
	if len(second.Skipped) != 1 || second.Skipped[0].Reason != "equivalent" {
		t.Fatalf("second import must skip the binding as equivalent: %+v", second.Skipped)
	}
	if second.Skipped[0].Destination != "seam-q" || second.Skipped[0].DestinationType != "queue" {
		t.Fatalf("the equivalent skip must carry the binding identity: %+v", second.Skipped[0])
	}
}

// TestDefinitionsExport_BindingsOnRealBroker proves production export includes
// `bindings` when the real seam is present: a binding declared through the live
// broker is exported without `excluded_sections:["bindings"]`.
func TestDefinitionsExport_BindingsOnRealBroker(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "real-ex", "topic")
	e.declareQueue(t, "real-q", queueDeclareRequest{})
	wantStatus(t, e.bindQueue(t, "real-ex", "real-q", "rk", nil), http.StatusCreated)

	doc := exportDoc(t, e)
	if containsStr(doc.StrangeQ.ExcludedSections, "bindings") {
		t.Fatalf("with the real seam present bindings must NOT be excluded: %v", doc.StrangeQ.ExcludedSections)
	}
	if len(doc.Bindings) != 1 {
		t.Fatalf("bindings = %+v, want the one live binding", doc.Bindings)
	}
	b := doc.Bindings[0]
	if b.Source != "real-ex" || b.Destination != "real-q" || b.DestinationType != "queue" || b.RoutingKey != "rk" {
		t.Fatalf("binding identity = %+v", b)
	}
}

// TestDefinitionsImport_PerTargetConfigureDenied pins AUTH-016/AUTH-012: the
// import applies the same per-target configure gate a direct PUT does, for both
// queues and exchanges, and declares neither.
func TestDefinitionsImport_PerTargetConfigureDenied(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":"^$","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}

	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "denied-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "denied-q"}},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), hdr))
	if len(res.Applied) != 0 {
		t.Fatalf("a denied target must never be applied: %+v", res.Applied)
	}
	if !containsName(res, res.Failed, "exchange", "denied-ex") || !containsName(res, res.Failed, "queue", "denied-q") {
		t.Fatalf("denied targets must be failed: %+v", res.Failed)
	}
	list := e.do(http.MethodGet, "/api/exchanges", nil, hdr)
	wantStatus(t, list, http.StatusOK)
	if strings.Contains(list.Body.String(), "denied-ex") {
		t.Fatalf("a denied exchange must not be declared")
	}
	queues := e.do(http.MethodGet, "/api/queues", nil, hdr)
	if strings.Contains(queues.Body.String(), "denied-q") {
		t.Fatalf("a denied queue must not be declared")
	}
}

// TestDefinitionsImport_MultipartChunkedOverCap413 pins the chunked-multipart
// hardening: Content-Length is absent (-1) so the pre-routing cap is skipped,
// yet the handler still bounds the stream at max_import_body_bytes ⇒ 413 with
// the generic cap reason, and nothing is applied.
func TestDefinitionsImport_MultipartChunkedOverCap413(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxBodyBytes = 1024
		cfg.Management.Limits.MaxImportBodyBytes = 1024
	})
	defs := definitionsImportRequest{Exchanges: []map[string]any{{"name": "huge-ex", "type": "direct"}}}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "definitions.json")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	payload := append(e.jsonBody(t, defs), bytes.Repeat([]byte(" "), 4096)...)
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/definitions", bytes.NewReader(buf.Bytes()))
	req.RemoteAddr = "127.0.0.1:43210"
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", basicAuthHeader()["Authorization"])
	req.ContentLength = -1 // chunked: the pre-routing Content-Length cap is skipped
	rec := httptest.NewRecorder()
	e.mgmt.Handler().ServeHTTP(rec, req)

	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
	if env := decode[errorEnvelope](t, rec); env.Reason != "request body exceeds 1024 bytes" {
		t.Fatalf("reason = %q, want the generic request-body cap text", env.Reason)
	}
	list := e.do(http.MethodGet, "/api/exchanges", nil, basicAuthHeader())
	if strings.Contains(list.Body.String(), "huge-ex") {
		t.Fatalf("an over-cap import must not declare anything: %s", truncate(list.Body.String()))
	}
}

func TestDefinitionsImport_BindingDedupByPropertiesKey(t *testing.T) {
	e := basicEnv(t, true, nil)
	fake := installFakeBindings(t, e)
	fake.seed(broker.BindingView{Source: "amq.topic", Destination: "dedup-q", DestinationType: "queue", RoutingKey: "k", Arguments: map[string]interface{}{"a": "b"}})

	doc := definitionsImportRequest{Bindings: []map[string]any{
		// A bogus payload properties_key must be ignored; the recomputed key
		// decides identity.
		{"source": "amq.topic", "destination": "dedup-q", "destination_type": "queue", "routing_key": "k", "arguments": map[string]any{"a": "b"}, "properties_key": "bogus~deadbeef"},
	}}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	if len(res.Skipped) != 1 || res.Skipped[0].Reason != "equivalent" {
		t.Fatalf("an identical binding must be skipped as equivalent: %+v", res.Skipped)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("dedup must not apply a duplicate: %+v", res.Applied)
	}
}

func TestDefinitionsImport_MultipartFile(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0 // multipart + JSON in this test
	})
	doc := definitionsImportRequest{Exchanges: []map[string]any{{"name": "mp-ex", "type": "direct"}}}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "definitions.json")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := fw.Write(e.jsonBody(t, doc)); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	rec := e.do(http.MethodPost, "/api/definitions", buf.Bytes(), map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"Content-Type":  mw.FormDataContentType(),
	})
	wantStatus(t, rec, http.StatusOK)
	if res := decode[defsImportResponse](t, rec); !containsName(res, res.Applied, "exchange", "mp-ex") {
		t.Fatalf("multipart import did not apply: %+v", res.Applied)
	}

	// A multipart body with no "file" part ⇒ 400.
	var buf2 bytes.Buffer
	mw2 := multipart.NewWriter(&buf2)
	_ = mw2.WriteField("notfile", "x")
	_ = mw2.Close()
	rec2 := e.do(http.MethodPost, "/api/definitions", buf2.Bytes(), map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"Content-Type":  mw2.FormDataContentType(),
	})
	wantStatus(t, rec2, http.StatusBadRequest)
}

func TestDefinitionsImport_UnknownKeysIgnored(t *testing.T) {
	e := basicEnv(t, true, nil)
	body := []byte(`{"strangeq":{"subset":true,"whatever":1},"exchanges":[{"name":"uk-ex","type":"direct"}],"policies":[{"name":"p"}],"parameters":[],"vhost_limits":{}}`)
	rec := importDefs(t, e, "", body, nil)
	wantStatus(t, rec, http.StatusOK)
	res := decode[defsImportResponse](t, rec)
	if !containsName(res, res.Applied, "exchange", "uk-ex") {
		t.Fatalf("an unknown-key document must still import known sections: %+v", res.Applied)
	}
}

func TestDefinitionsImport_413BeforeParse(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.Limits.MaxImportBodyBytes = 1024
	})
	// A valid JSON prefix followed by garbage PAST the limit: if the body were
	// parsed first the garbage would yield 400; the cap must win with 413.
	body := append([]byte(`{"exchanges":[{"name":"x","type":"direct"}]}`), bytes.Repeat([]byte("{"), 2048)...)
	rec := importDefs(t, e, "", body, nil)
	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
	if env := decode[errorEnvelope](t, rec); env.Error != errPayloadTooLarge {
		t.Fatalf("token = %q, want payload_too_large", env.Error)
	}
}

func TestDefinitionsImport_WrongContentType415(t *testing.T) {
	e := basicEnv(t, true, nil)
	body := e.jsonBody(t, definitionsImportRequest{Exchanges: []map[string]any{{"name": "ct-ex", "type": "direct"}}})
	rec := e.do(http.MethodPost, "/api/definitions", body, map[string]string{
		"Authorization": basicAuthHeader()["Authorization"],
		"Content-Type":  "text/plain",
	})
	wantStatus(t, rec, http.StatusUnsupportedMediaType)
	if env := decode[errorEnvelope](t, rec); env.Error != errUnsupportedMedia {
		t.Fatalf("token = %q, want unsupported_media_type", env.Error)
	}
}

func TestDefinitionsImport_ReorderDeterministic(t *testing.T) {
	e := basicEnv(t, true, nil)
	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "ord-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "ord-q"}},
		Bindings:  []map[string]any{{"source": "ord-ex", "destination": "ord-q", "destination_type": "queue", "routing_key": "k"}},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), nil))
	var order []string
	for _, it := range res.Applied {
		order = append(order, it.Type)
	}
	want := []string{"exchange", "queue", "binding"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("apply order = %v, want %v", order, want)
	}
}

// --- dry-run ----------------------------------------------------------------

func TestDefinitionsDryRun_NoMutation(t *testing.T) {
	e := basicEnv(t, true, nil)
	doc := definitionsImportRequest{Queues: []map[string]any{{"name": "dr-q"}}}
	rec := importDefs(t, e, "?dry_run=true", e.jsonBody(t, doc), nil)
	wantStatus(t, rec, http.StatusOK)
	res := decode[defsImportResponse](t, rec)
	if !res.StrangeQ.DryRun {
		t.Fatalf("dry-run response must carry strangeq.dry_run:true: %+v", res.StrangeQ)
	}
	if !containsName(res, res.Applied, "queue", "dr-q") {
		t.Fatalf("dry-run must preview the applied entity: %+v", res.Applied)
	}
	if got := e.do(http.MethodGet, "/api/queues/%2F/dr-q", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("a dry-run must not mutate topology (queue exists: %d)", got.Code)
	}
}

func TestDefinitionsDryRun_MatchesApply(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0 // dry-run + apply
	})
	doc := definitionsImportRequest{
		VHosts:    []map[string]any{{"name": "/"}},
		Exchanges: []map[string]any{{"name": "m-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "m-q"}},
		Bindings:  []map[string]any{{"source": "m-ex", "destination": "m-q", "destination_type": "queue", "routing_key": "k"}},
		Users:     []map[string]any{{"name": "bob"}},
	}
	body := e.jsonBody(t, doc)
	dry := decode[defsImportResponse](t, importDefs(t, e, "?dry_run=true", body, nil))
	apply := decode[defsImportResponse](t, importDefs(t, e, "", body, nil))

	dj, _ := json.Marshal(struct {
		A, S, F []defsResult
	}{dry.Applied, dry.Skipped, dry.Failed})
	aj, _ := json.Marshal(struct {
		A, S, F []defsResult
	}{apply.Applied, apply.Skipped, apply.Failed})
	if string(dj) != string(aj) {
		t.Fatalf("dry-run plan diverged from apply:\ndry=%s\napply=%s", dj, aj)
	}
	// applied and failed are disjoint (D-6), and the apply actually MUTATED the
	// topology: a no-op apply that reported success would be caught here.
	seen := map[string]bool{}
	for _, it := range apply.Applied {
		seen[it.Type+":"+it.Name+":"+it.Destination] = true
	}
	for _, it := range apply.Failed {
		if seen[it.Type+":"+it.Name+":"+it.Destination] {
			t.Fatalf("an entity appears in both applied and failed: %+v", it)
		}
	}
	wantStatus(t, e.do(http.MethodGet, "/api/exchanges/%2F/m-ex", nil, basicAuthHeader()), http.StatusOK)
	wantStatus(t, e.do(http.MethodGet, "/api/queues/%2F/m-q", nil, basicAuthHeader()), http.StatusOK)
}

func TestDefinitionsDryRun_InvalidPayload400(t *testing.T) {
	e := basicEnv(t, true, nil)
	rec := importDefs(t, e, "?dry_run=true", []byte("{not json"), nil)
	wantStatus(t, rec, http.StatusBadRequest)
}

// --- hardening / integration ------------------------------------------------

func TestDefinitionsImport_RateLimited429(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) { cfg.Management.RateLimits.DefinitionsImportSeconds = 300 })
	body := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "rl-q"}}})
	wantStatus(t, importDefs(t, e, "", body, nil), http.StatusOK)
	second := importDefs(t, e, "", body, nil)
	wantStatus(t, second, http.StatusTooManyRequests)
	if got := second.Header().Get("Retry-After"); got != "300" {
		t.Fatalf("Retry-After = %q, want the rate-limit window (300)", got)
	}
	// The dry-run counts against the same budget.
	wantStatus(t, importDefs(t, e, "?dry_run=true", body, nil), http.StatusTooManyRequests)
}

// TestDefinitionsImport_DefaultRateLimited429 pins the SEC-067 default: with no
// explicit configuration, the second import by one principal is 429.
func TestDefinitionsImport_DefaultRateLimited429(t *testing.T) {
	e := basicEnv(t, true, nil) // DefaultConfig: definitions_import_seconds = 10
	body := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "dflt-q"}}})
	wantStatus(t, importDefs(t, e, "", body, nil), http.StatusOK)
	second := importDefs(t, e, "", body, nil)
	wantStatus(t, second, http.StatusTooManyRequests)
	if got := second.Header().Get("Retry-After"); got != "10" {
		t.Fatalf("default Retry-After = %q, want 10", got)
	}
}

func TestDefinitionsImport_AuditDurableBeforeApply(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.mgmt.auditSink = failingSink{}
	body := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "audit-q"}}})
	rec := importDefs(t, e, "", body, nil)
	wantStatus(t, rec, http.StatusServiceUnavailable)
	if got := e.do(http.MethodGet, "/api/queues/%2F/audit-q", nil, basicAuthHeader()); got.Code != http.StatusNotFound {
		t.Fatalf("a sink overflow must apply nothing (queue exists: %d)", got.Code)
	}
}

func TestDefinitionsImport_DryRunNotAudited(t *testing.T) {
	e := basicEnv(t, true, nil)
	sink := &captureSink{}
	e.mgmt.auditSink = sink
	body := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "da-q"}}})
	wantStatus(t, importDefs(t, e, "?dry_run=true", body, nil), http.StatusOK)
	if len(sink.records()) != 0 {
		t.Fatalf("a dry-run is a read and must not be audited: %+v", sink.records())
	}
}

func TestDefinitions_NoForbiddenLock(t *testing.T) {
	// EntityThreshold=1 lets the 2-queue call below take the async/202 path with
	// the forbidden locks held; DefinitionsImportSeconds=0 keeps four import
	// requests within budget.
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0
		cfg.Management.Async.EntityThreshold = 1
	})
	e.declareQueue(t, "lock-q", queueDeclareRequest{})

	peer := e.injectConnection(t, "defs-locked-conn", "u")
	defer peer.Close()
	e.srv.Mutex.Lock()
	conn := e.srv.Connections["defs-locked-conn"]
	e.srv.Mutex.Unlock()
	if conn == nil {
		t.Fatalf("injected connection missing")
	}
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	ch.Mutex.Lock()
	defer ch.Mutex.Unlock()
	conn.WriteMutex.Lock()
	defer conn.WriteMutex.Unlock()

	body := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "lock-import-q"}}})
	asyncLoc := ""
	calls := []struct {
		name   string
		do     func() *httptest.ResponseRecorder
		status int
	}{
		{"export", func() *httptest.ResponseRecorder {
			return e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
		}, http.StatusOK},
		{"dry-run", func() *httptest.ResponseRecorder {
			return importDefs(t, e, "?dry_run=true", body, nil)
		}, http.StatusOK},
		{"apply", func() *httptest.ResponseRecorder {
			return importDefs(t, e, "", body, nil)
		}, http.StatusOK},
		{"async-apply", func() *httptest.ResponseRecorder {
			// Two applies exceed EntityThreshold=1 ⇒ 202; the job's apply also
			// runs off the management goroutine but must not take a forbidden
			// lock on this request's accessor graph.
			asyncBody := e.jsonBody(t, definitionsImportRequest{Queues: []map[string]any{{"name": "lock-async-q1"}, {"name": "lock-async-q2"}}})
			rec := importDefs(t, e, "", asyncBody, nil)
			asyncLoc = rec.Header().Get("Location")
			return rec
		}, http.StatusAccepted},
	}
	for _, c := range calls {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func(fn func() *httptest.ResponseRecorder) { done <- fn() }(c.do)
		select {
		case rec := <-done:
			if rec.Code != c.status {
				t.Fatalf("%s = %d, want %d while a forbidden lock is held (%s)", c.name, rec.Code, c.status, rec.Body.String())
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("%s did not return within 1s while a forbidden lock is held", c.name)
		}
	}
	// Drain the async apply so its job does not race the temp-dir teardown.
	if asyncLoc != "" {
		deadline := time.Now().Add(3 * time.Second)
		for {
			got := e.do(http.MethodGet, asyncLoc, nil, basicAuthHeader())
			if got.Code == http.StatusOK {
				var view struct {
					State string `json:"state"`
				}
				if json.Unmarshal(got.Body.Bytes(), &view) == nil && (view.State == jobDone || view.State == jobFailed) {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("async apply job did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	// NOT load-bearing: forbiddenLockHits is never incremented anywhere (the
	// counter is a placeholder). The 1s deadline above and the AST scan
	// (TestForbiddenLockGate_ASTScan, which now also covers definitions.go) are
	// the real proof that no forbidden accessor is on the path.
	_ = e.mgmt.forbiddenLockHits.Load()
}

func TestDefinitions_AsyncOverThreshold202(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) { cfg.Management.Async.EntityThreshold = 1 })
	doc := definitionsImportRequest{Queues: []map[string]any{{"name": "async-q1"}, {"name": "async-q2"}}}
	rec := importDefs(t, e, "", e.jsonBody(t, doc), nil)
	wantStatus(t, rec, http.StatusAccepted)
	loc := rec.Header().Get("Location")
	if !strings.HasPrefix(loc, "/api/strangeq/jobs/") {
		t.Fatalf("async import Location = %q", loc)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		got := e.do(http.MethodGet, loc, nil, basicAuthHeader())
		if got.Code == http.StatusOK {
			var view struct {
				State  string         `json:"state"`
				Result map[string]any `json:"result"`
			}
			if err := json.Unmarshal(got.Body.Bytes(), &view); err == nil {
				if view.State == jobDone {
					if n, _ := view.Result["applied"].([]any); len(n) != 2 {
						t.Fatalf("async job result applied = %v, want 2", view.Result["applied"])
					}
					return
				}
				if view.State == jobFailed {
					t.Fatalf("async job failed: %s", got.Body.String())
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("async import job did not reach done: %s", got.Body.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDefinitionsImport_BindingAuthz(t *testing.T) {
	e := inheritEnv(t, `{"users":[{"username":"adm","password_hash":"`+secretHash(t)+`",
		"tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":"^ok-.*$","write":".*","read":".*"}}]}]}`, nil)
	hdr := map[string]string{"Authorization": "Basic " + basicToken("adm", "secret")}
	doc := definitionsImportRequest{
		Exchanges: []map[string]any{{"name": "ok-ex", "type": "direct"}},
		Queues:    []map[string]any{{"name": "ok-q"}},
		Bindings:  []map[string]any{{"source": "ok-ex", "destination": "bad-q", "destination_type": "queue", "routing_key": "k"}},
	}
	res := decode[defsImportResponse](t, importDefs(t, e, "", e.jsonBody(t, doc), hdr))
	if !containsName(res, res.Failed, "binding", "bad-q") {
		t.Fatalf("a binding to an unconfigurable target must be failed: %+v", res.Failed)
	}
	// The authz-denied failure must still carry the binding identity (§2.4).
	denied := res.Failed[0]
	if denied.Type != "binding" || denied.Source != "ok-ex" || denied.Destination != "bad-q" ||
		denied.DestinationType != "queue" || denied.PropertiesKey != "k" {
		t.Fatalf("authz-denied binding lost its identity: %+v", denied)
	}
	if !containsName(res, res.Applied, "exchange", "ok-ex") || !containsName(res, res.Applied, "queue", "ok-q") {
		t.Fatalf("permitted entities must still apply: %+v", res.Applied)
	}
}

func TestDefinitionsExportImport_RoundTrip(t *testing.T) {
	src := basicEnv(t, true, nil)
	wantStatus(t, src.do(http.MethodPut, "/api/exchanges/%2F/rt-ex", src.jsonBody(t, exchangeDeclareRequest{Type: "direct", Durable: true}), basicAuthHeader()), http.StatusCreated)
	src.declareQueue(t, "rt-q", queueDeclareRequest{Durable: true})
	wantStatus(t, src.bindQueue(t, "rt-ex", "rt-q", "rk", nil), http.StatusCreated)
	body := src.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader()).Body.Bytes()
	// The export must carry the real binding (WS5 finding 5), not an excluded
	// empty section.
	var srcDoc definitionsExport
	if err := json.Unmarshal(body, &srcDoc); err != nil {
		t.Fatalf("decode source export: %v", err)
	}
	if len(srcDoc.Bindings) != 1 {
		t.Fatalf("source export bindings = %+v, want the one live binding", srcDoc.Bindings)
	}
	if containsStr(srcDoc.StrangeQ.ExcludedSections, "bindings") {
		t.Fatalf("source export must not exclude bindings: %v", srcDoc.StrangeQ.ExcludedSections)
	}

	dst := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Management.RateLimits.DefinitionsImportSeconds = 0 // two imports in this test
	})
	res := decode[defsImportResponse](t, importDefs(t, dst, "", body, nil))
	if !containsName(res, res.Applied, "exchange", "rt-ex") || !containsName(res, res.Applied, "queue", "rt-q") {
		t.Fatalf("round-trip did not apply the user entities: applied=%v failed=%v", appliedNames(res), failedNames(res))
	}
	if !containsName(res, res.Applied, "binding", "rt-q") {
		t.Fatalf("round-trip did not apply the binding: applied=%v failed=%v", appliedNames(res), failedNames(res))
	}
	wantStatus(t, dst.do(http.MethodGet, "/api/exchanges/%2F/rt-ex", nil, basicAuthHeader()), http.StatusOK)
	wantStatus(t, dst.do(http.MethodGet, "/api/queues/%2F/rt-q", nil, basicAuthHeader()), http.StatusOK)

	// A second import of unchanged bytes creates nothing.
	res2 := decode[defsImportResponse](t, importDefs(t, dst, "", body, nil))
	for _, it := range res2.Applied {
		if it.Result == "created" {
			t.Fatalf("a second import must create nothing: %+v", it)
		}
	}
	// The re-imported binding is deduped as equivalent, not re-created.
	var skippedBinding bool
	for _, it := range res2.Skipped {
		if it.Type == "binding" && it.Destination == "rt-q" && it.Reason == "equivalent" {
			skippedBinding = true
		}
	}
	if !skippedBinding {
		t.Fatalf("the exported binding must re-import as equivalent: %+v", res2.Skipped)
	}
}

func TestDefinitionsImport_NoRuntimeFileRead(t *testing.T) {
	// The handler must not read the working directory or a file per request.
	e := basicEnv(t, true, nil)
	e.mgmt.users = &userSource{path: "/definitely/does/not/exist/auth.json"}
	rec := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	// Mechanical guard: the definitions request path contains no file access.
	for _, name := range []string{"definitions.go", "handlers_mut.go"} {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, banned := range []string{"os.ReadFile", "os.Open", "ioutil.ReadFile"} {
			if bytes.Contains(src, []byte(banned)) && name == "definitions.go" {
				t.Fatalf("%s references a forbidden runtime file read (%s)", name, banned)
			}
		}
	}
}

// --- benchmark ---------------------------------------------------------------

func BenchmarkPlanDefinitions_1000Entities(b *testing.B) {
	s := &Server{}
	doc := definitionsImportRequest{}
	for i := 0; i < 1000; i++ {
		doc.Queues = append(doc.Queues, map[string]any{"name": "bench-q-" + itoa(i)})
	}
	payload := &definitionsPayload{}
	for _, q := range doc.Queues {
		payload.Queues = append(payload.Queues, defsQueue{Name: q["name"].(string), Vhost: "/"})
	}
	cur := defsTopology{exchanges: map[string]defsExchange{}, queues: map[string]defsQueue{}}
	caps := defsCaps{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := s.planDefinitions(payload, cur, caps); len(got) != 1000 {
			b.Fatalf("planned %d actions, want 1000", len(got))
		}
	}
}

// --- fake binding seam -------------------------------------------------------

// defsFakeBroker wraps the real UnifiedBroker and overrides the aggregate
// binding seam (broker.BindingView / ListBindings), so definitions tests can
// seed an isolated binding set without touching the wrapped broker's storage.
type defsFakeBroker struct {
	server.UnifiedBroker
	mu       sync.Mutex
	bindings []broker.BindingView
}

func (b *defsFakeBroker) ListBindings() ([]broker.BindingView, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]broker.BindingView(nil), b.bindings...), nil
}

func (b *defsFakeBroker) seed(v broker.BindingView) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bindings = append(b.bindings, v)
}

func (b *defsFakeBroker) BindQueue(queue, exchange, rk string, args map[string]interface{}) error {
	if err := b.UnifiedBroker.BindQueue(queue, exchange, rk, args); err != nil {
		return err
	}
	b.seed(broker.BindingView{Source: exchange, Destination: queue, DestinationType: "queue", RoutingKey: rk, Arguments: args})
	return nil
}

func (b *defsFakeBroker) BindExchange(destination, source, rk string, args map[string]interface{}) error {
	if err := b.UnifiedBroker.BindExchange(destination, source, rk, args); err != nil {
		return err
	}
	b.seed(broker.BindingView{Source: source, Destination: destination, DestinationType: "exchange", RoutingKey: rk, Arguments: args})
	return nil
}

func installFakeBindings(t *testing.T, e *env) *defsFakeBroker {
	t.Helper()
	fake := &defsFakeBroker{UnifiedBroker: e.srv.Broker}
	e.srv.Broker = fake
	return fake
}

// --- small helpers -----------------------------------------------------------

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func exportDoc(t *testing.T, e *env) definitionsExport {
	t.Helper()
	rec := e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var doc definitionsExport
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	return doc
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
