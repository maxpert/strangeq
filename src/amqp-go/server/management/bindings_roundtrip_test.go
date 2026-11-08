package management

import (
	"net/http"
	"net/url"
	"testing"
)

// TestBindingsPropertiesKey_DefinitionsRoundTrip (FR-API-090/DIV-12, spec §9
// LOW-4): a binding declared on the live broker exposes the SAME
// properties_key on GET /api/bindings and inside GET /api/definitions, and
// DELETE /api/bindings/…/{props} with that key removes it.
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
