package management

import "testing"

// TestFieldTableJSON_ValueMappingDivergence pins LOW-6: field-table values are
// mapped onto JSON (FR-CONV-047), so a []byte argument is exported as a base64
// STRING, not bytes. The divergence is stated in strangeq.excludes so a client
// can see it, and the properties_key still round-trips because it hashes that
// same canonical JSON.
func TestFieldTableJSON_ValueMappingDivergence(t *testing.T) {
	bytesArgs := map[string]any{"h": []byte("bar")}
	mapped, ok := fieldTableJSON(bytesArgs).(map[string]any)
	if !ok {
		t.Fatalf("fieldTableJSON(map) = %T, want map[string]any", fieldTableJSON(bytesArgs))
	}
	if _, isString := mapped["h"].(string); !isString {
		t.Fatalf("a []byte field-table value must map to a JSON string, got %T", mapped["h"])
	}

	if !containsStr(defsExcludes, "binary_field_table_values_become_base64_strings") {
		t.Fatalf("the field-table mapping divergence must be named in excludes: %v", defsExcludes)
	}

	// A key computed from the original bytes equals a key computed from the
	// exported (already-mapped) value: both hash the same canonical JSON, so
	// the properties_key survives an export -> import round trip.
	stringArgs := map[string]any{"h": mapped["h"]}
	if got, want := propertiesKey("rk", stringArgs), propertiesKey("rk", bytesArgs); got != want {
		t.Fatalf("properties_key must round-trip through the JSON mapping: %q != %q", got, want)
	}
}
