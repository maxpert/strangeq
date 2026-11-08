package management

import (
	"net/http"
	"net/url"
	"testing"
)

// TestBindingsPropertiesKey_DefinitionsRoundTrip is a CHARACTERISATION pin
// (FR-API-090/DIV-12, spec §9 LOW-4): a binding declared on the live broker
// exposes the SAME properties_key on GET /api/bindings and inside
// GET /api/definitions, and DELETE /api/bindings/…/{props} with that key
// removes it. It passes on the base tree; the branch-changing case is
// TestBindingsPropertiesKey_BytesArgumentBase64RoundTrip below.
func TestBindingsPropertiesKey_DefinitionsRoundTrip(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "rt-x", "topic")
	e.declareQueue(t, "rt-q", queueDeclareRequest{})
	args := map[string]any{"x-match": "all", "h": "v"}
	wantStatus(t, e.bindQueue(t, "rt-x", "rt-q", "rt.rk", args), http.StatusCreated)

	// GET /api/bindings: find the one binding and read its properties_key.
	bindings := decode[[]map[string]any](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	var bindingKey string
	for _, it := range bindings {
		if it["source"] == "rt-x" && it["destination"] == "rt-q" && it["routing_key"] == "rt.rk" {
			bindingKey, _ = it["properties_key"].(string)
		}
	}
	if bindingKey == "" {
		t.Fatalf("the declared binding is not listed: %v", bindings)
	}

	// GET /api/definitions: the same binding carries the same key.
	var doc struct {
		Bindings []map[string]any `json:"bindings"`
	}
	if err := jsonUnmarshalStrict(e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader()).Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode definitions: %v", err)
	}
	if len(doc.Bindings) != 1 {
		t.Fatalf("definitions must carry the one binding, got %d (%v)", len(doc.Bindings), doc.Bindings)
	}
	defKey, _ := doc.Bindings[0]["properties_key"].(string)
	if defKey != bindingKey {
		t.Fatalf("properties_key differs: /api/bindings=%q /api/definitions=%q", bindingKey, defKey)
	}

	// DELETE with the exported key: the round trip must remove the binding.
	del := e.do(http.MethodDelete, "/api/bindings/%2F/e/rt-x/q/rt-q/"+url.PathEscape(defKey), nil, basicAuthHeader())
	wantStatus(t, del, http.StatusNoContent)
	after := decode[[]map[string]any](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(after) != 0 {
		t.Fatalf("the DELETE with the exported key left %d bindings: %v", len(after), after)
	}
}

// TestBindingsPropertiesKey_BytesArgumentBase64RoundTrip covers the LOW-6
// branch: a binding whose argument is a []byte (only reachable by binding
// through the broker directly; JSON has no bytes) is exported as a base64
// STRING by both /api/bindings and /api/definitions, the divergence is named in
// strangeq.excludes, and the exported properties_key still DELETEs the binding.
func TestBindingsPropertiesKey_BytesArgumentBase64RoundTrip(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareExchange(t, "hdr-x", "headers")
	e.declareQueue(t, "hdr-q", queueDeclareRequest{})

	args := map[string]any{"x-match": "all", "h": []byte("bar")}
	if err := e.srv.Broker.BindQueue("hdr-q", "hdr-x", "hdr.rk", args); err != nil {
		t.Fatalf("BindQueue with a []byte argument: %v", err)
	}
	wantArg := base64String([]byte("bar"))

	// /api/bindings: the []byte argument is a base64 STRING, not bytes.
	bindings := decode[[]map[string]any](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	var bindingKey string
	for _, it := range bindings {
		if it["source"] == "hdr-x" && it["destination"] == "hdr-q" {
			bindingKey, _ = it["properties_key"].(string)
			m, _ := it["arguments"].(map[string]any)
			if m["h"] != wantArg {
				t.Fatalf("GET /api/bindings argument h = %#v, want base64 %q", m["h"], wantArg)
			}
		}
	}
	if bindingKey == "" {
		t.Fatalf("the []byte-argument binding is not listed: %v", bindings)
	}

	// /api/definitions: same value, same key, and the divergence is named.
	var doc struct {
		Strangeq struct {
			Excludes []string `json:"excludes"`
		} `json:"strangeq"`
		Bindings []map[string]any `json:"bindings"`
	}
	if err := jsonUnmarshalStrict(e.do(http.MethodGet, "/api/definitions", nil, basicAuthHeader()).Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode definitions: %v", err)
	}
	if len(doc.Bindings) != 1 {
		t.Fatalf("definitions must carry the one binding, got %d (%v)", len(doc.Bindings), doc.Bindings)
	}
	defArgs, _ := doc.Bindings[0]["arguments"].(map[string]any)
	if defArgs["h"] != wantArg {
		t.Fatalf("definitions argument h = %#v, want base64 %q", defArgs["h"], wantArg)
	}
	if defKey, _ := doc.Bindings[0]["properties_key"].(string); defKey != bindingKey {
		t.Fatalf("properties_key differs: /api/bindings=%q /api/definitions=%q", bindingKey, defKey)
	}
	if !containsStr(doc.Strangeq.Excludes, "binary_field_table_values_become_base64_strings") {
		t.Fatalf("the []byte->base64 divergence must be named in excludes: %v", doc.Strangeq.Excludes)
	}

	// DELETE with the exported key: the base64 round trip must remove it.
	del := e.do(http.MethodDelete, "/api/bindings/%2F/e/hdr-x/q/hdr-q/"+url.PathEscape(bindingKey), nil, basicAuthHeader())
	wantStatus(t, del, http.StatusNoContent)
	after := decode[[]map[string]any](t, e.do(http.MethodGet, "/api/bindings", nil, basicAuthHeader()))
	if len(after) != 0 {
		t.Fatalf("the DELETE with the exported base64 key left %d bindings: %v", len(after), after)
	}
}
