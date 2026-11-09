package web

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// vitalSelector is one entry of the VITALS table parsed from app.js: the tile
// id, the raw selector text, and the metric name / label filter it references.
type vitalSelector struct {
	id    string
	sel   string
	name  string
	label string // "" when the selector has no label filter
	op    string // "=" or "!=" ("" when absent)
	value string // "" when the selector has no label filter
}

var (
	// vitalBlockRe bounds the VITALS table in app.js.
	vitalBlockRe = regexp.MustCompile(`(?s)var VITALS = \[(.*?)\];`)
	// vitalEntryRe pulls the (id, sel) pair out of one table line. The sel value
	// is a JS double-quoted string literal, so its embedded quotes are escaped.
	vitalEntryRe = regexp.MustCompile(`\{ id: "([^"]+)",[^\n]*?sel: "((?:[^"\\]|\\.)*)"`)
	// vitalWrapRe strips the optional rate()/sum()/max() wrapper.
	vitalWrapRe = regexp.MustCompile(`^(rate|sum|max)\((.*)\)$`)
	// vitalHeadRe splits "name" or "name{...}".
	vitalHeadRe = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?$`)
	// vitalLabelRe matches the single label matcher app.js supports: "=" and
	// "!=" value matchers plus the anchored Prometheus regex matchers "=~" and
	// "!~".
	vitalLabelRe = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s*(!=|=~|!~|=)\s*"((?:[^"\\]|\\.)*)"$`)
	// inventoryNameRe pulls the amqp_* family names out of a golden list.
	inventoryNameRe = regexp.MustCompile(`"(amqp_[a-zA-Z0-9_]+)"`)
	// inventoryListRe bounds each `[]string{ ... }` block in the inventory
	// golden. loadInventoryGolden scans every such block and keeps the one
	// carrying the most amqp_* names, so the pinned set is found regardless of
	// the variable name it is bound to (want := ..., amqpFamilyGolden = ...).
	// Only names INSIDE a block count, so a family merely mentioned in a
	// comment is not treated as part of the pinned set.
	inventoryListRe = regexp.MustCompile(`(?s)\[\]string\{(.*?)\n\s*\}`)
)

// labelDomains is the enumerable label-literal domain for every family the
// VITALS selectors filter on. Each entry mirrors the Go const block that closes
// the domain: metrics/management.go (status_class is strconv.Itoa(statusCode)
// -> "2".."5") and server/recovery_metrics.go (RecoveryOutcome* and
// ShutdownResult*). Only families with a closed enum are listed; a selector on
// any other family is checked for the metric name alone.
var labelDomains = map[string]map[string]bool{
	"amqp_recovery_faults_total":        {"benign": true, "degraded": true, "fatal": true},
	"amqp_shutdown_total":               {"clean": true, "forced": true, "incomplete": true},
	"amqp_management_requests_total":    {"2": true, "3": true, "4": true, "5": true},
	"amqp_management_ui_requests_total": {"2": true, "3": true, "4": true, "5": true},
}

// parseVitalSelectors extracts and parses every entry of the VITALS table in
// app.js. It fails loudly if the table or the selector shape changes so the
// static gate cannot silently stop checking anything.
func parseVitalSelectors(t *testing.T, js string) []vitalSelector {
	t.Helper()
	block := vitalBlockRe.FindStringSubmatch(js)
	require.NotNil(t, block, "app.js no longer contains a parseable `var VITALS = [ ... ];` table")
	entries := vitalEntryRe.FindAllStringSubmatch(block[1], -1)
	require.NotEmpty(t, entries, "the VITALS table has no parseable entries")

	out := make([]vitalSelector, 0, len(entries))
	for _, e := range entries {
		s := vitalSelector{id: e[1], sel: strings.ReplaceAll(e[2], `\"`, `"`)}
		body := s.sel
		if m := vitalWrapRe.FindStringSubmatch(body); m != nil {
			body = strings.TrimSpace(m[2])
		}
		head := vitalHeadRe.FindStringSubmatch(body)
		require.NotNilf(t, head, "VITALS %q selector %q is not a bare metric or a single label matcher", s.id, s.sel)
		s.name = head[1]
		if head[2] != "" {
			lm := vitalLabelRe.FindStringSubmatch(strings.TrimSpace(head[2]))
			require.NotNilf(t, lm, "VITALS %q selector %q has an unsupported label matcher", s.id, s.sel)
			s.label, s.op, s.value = lm[1], lm[2], lm[3]
		}
		out = append(out, s)
	}
	return out
}

// vitalSelectorProblems returns one problem string per selector whose metric
// name is absent from the inventory golden, or whose label literal is outside
// that family's enumerable domain. Domains that are not enumerable are checked
// for the metric name only.
func vitalSelectorProblems(sels []vitalSelector, golden map[string]bool, domains map[string]map[string]bool) []string {
	var probs []string
	for _, s := range sels {
		if !golden[s.name] {
			probs = append(probs, fmt.Sprintf(
				"VITALS %q selects %q, but %q is not a family in the metric inventory golden",
				s.id, s.sel, s.name))
			continue
		}
		if s.label == "" {
			continue
		}
		dom, ok := domains[s.name]
		if !ok {
			continue
		}
		if s.op == "=~" || s.op == "!~" {
			// A regex matcher's value is a pattern over the domain, not a
			// domain literal; it is checked for the metric name only.
			continue
		}
		if !dom[s.value] {
			probs = append(probs, fmt.Sprintf(
				"VITALS %q selects %q, but %s%s\"%s\" is not a legal %s value (legal: %s)",
				s.id, s.sel, s.label, s.op, s.value, s.name, strings.Join(sortedKeys(dom), ", ")))
		}
	}
	return probs
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// loadInventoryGolden parses the pinned amqp_* family set out of the metrics
// inventory test, so a rename that lands in one place and not the other breaks
// here. The path is relative to this package's source directory (web -> metrics).
func loadInventoryGolden(t *testing.T) map[string]bool {
	t.Helper()
	path := filepath.Join("..", "..", "..", "metrics", "inventory_test.go")
	data, err := os.ReadFile(path)
	require.NoErrorf(t, err, "reading the metric inventory golden %s", path)
	blocks := inventoryListRe.FindAllSubmatch(data, -1)
	require.NotEmptyf(t, blocks, "the inventory golden %s no longer has a `[]string{...}` list", path)
	var names [][]string
	for _, block := range blocks {
		found := inventoryNameRe.FindAllStringSubmatch(string(block[1]), -1)
		if len(found) > len(names) {
			names = found
		}
	}
	require.Greaterf(t, len(names), 100, "the inventory golden %s yielded only %d names", path, len(names))
	golden := make(map[string]bool, len(names))
	for _, n := range names {
		golden[n[1]] = true
	}
	return golden
}

/* -------------------------------------------- METRIC_GROUPS static gate ---- */

var (
	// metricGroupsBlockRe bounds the METRIC_GROUPS table in app.js.
	metricGroupsBlockRe = regexp.MustCompile(`(?s)var METRIC_GROUPS = \[(.*?)\n\s*\];`)
	// metricGroupEntryRe pulls one ["label", ["prefix", ...]] row out of it.
	metricGroupEntryRe = regexp.MustCompile(`\["((?:[^"\\]|\\.)*)",\s*\[([^\[\]]*)\]\]`)
	// metricGroupPrefixRe pulls each prefix literal out of a row's prefix list.
	metricGroupPrefixRe = regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`)
	// jsUnicodeEscapeRe decodes a JS \uXXXX escape (the group labels carry an
	// em-dash written as \u2014).
	jsUnicodeEscapeRe = regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
)

// metricGroupEntry is one row of the METRIC_GROUPS table: a group label and the
// metric-name prefixes that route a family to it.
type metricGroupEntry struct {
	label    string
	prefixes []string
}

// parseMetricGroups extracts the ordered METRIC_GROUPS table from app.js so the
// group of a metric family can be resolved statically, with no browser.
func parseMetricGroups(t *testing.T, js string) []metricGroupEntry {
	t.Helper()
	block := metricGroupsBlockRe.FindStringSubmatch(js)
	require.NotNil(t, block, "app.js no longer contains a parseable `var METRIC_GROUPS = [ ... ];` table")
	var out []metricGroupEntry
	for _, e := range metricGroupEntryRe.FindAllStringSubmatch(block[1], -1) {
		label := jsUnescape(e[1])
		var prefixes []string
		for _, p := range metricGroupPrefixRe.FindAllStringSubmatch(e[2], -1) {
			prefixes = append(prefixes, jsUnescape(p[1]))
		}
		require.NotEmptyf(t, prefixes, "METRIC_GROUPS entry %q has no prefixes", label)
		out = append(out, metricGroupEntry{label: label, prefixes: prefixes})
	}
	require.GreaterOrEqualf(t, len(out), 15,
		"parsed only %d METRIC_GROUPS entries; the table or this parser changed", len(out))
	return out
}

// metricGroupFor mirrors metricGroup() in app.js: the first entry (in table
// order) with a matching prefix wins, and an unmatched name falls to the
// "Other" catch-all.
func metricGroupFor(groups []metricGroupEntry, name string) string {
	for _, g := range groups {
		for _, p := range g.prefixes {
			if strings.HasPrefix(name, p) {
				return g.label
			}
		}
	}
	return "Other"
}

// jsUnescape decodes the JS string escapes the METRIC_GROUPS labels use.
func jsUnescape(s string) string {
	return jsUnicodeEscapeRe.ReplaceAllStringFunc(s, func(m string) string {
		n, err := strconv.ParseUint(m[2:], 16, 32)
		if err != nil {
			return m
		}
		return string(rune(n))
	})
}

// TestUI_MetricGroupsHomeEveryEmittedFamily (audit LOW 12): metricGroup() files
// a family under the first METRIC_GROUPS prefix that matches it, and "Other" is
// only the catch-all for a family the broker does not emit (or a brand-new
// one). Every amqp_* family the broker actually emits must therefore have a
// home, so a real signal is never misfiled under the catch-all.
func TestUI_MetricGroupsHomeEveryEmittedFamily(t *testing.T) {
	groups := parseMetricGroups(t, string(readAsset(t, "app.js")))
	golden := loadInventoryGolden(t)

	for _, g := range groups {
		if g.label == "Other" {
			t.Errorf(`METRIC_GROUPS defines an explicit "Other" group; Other must stay the catch-all for unmatched names only`)
		}
	}

	var homeless []string
	for name := range golden {
		if !strings.HasPrefix(name, "amqp_") {
			continue
		}
		if metricGroupFor(groups, name) == "Other" {
			homeless = append(homeless, name)
		}
	}
	sort.Strings(homeless)
	if len(homeless) > 0 {
		t.Errorf("emitted amqp_* families fall into the Other catch-all: %s; add a prefix to METRIC_GROUPS",
			strings.Join(homeless, ", "))
	}

	// The five families the audit found had no home, and the group each must
	// now land in.
	want := map[string]string{
		"amqp_consumer_register_errors_total": "Broker \u2014 consumers",
		"amqp_deliveries_dropped_total":       "Broker \u2014 messages",
		"amqp_deliveries_requeued_total":      "Broker \u2014 messages",
		"amqp_delivery_send_errors_total":     "Broker \u2014 messages",
		"amqp_publish_refused_total":          "Back-pressure",
	}
	for fam, wantGroup := range want {
		if !golden[fam] {
			t.Errorf("the inventory golden no longer lists the emitted family %q", fam)
			continue
		}
		if got := metricGroupFor(groups, fam); got != wantGroup {
			t.Errorf("group(%q) = %q, want %q", fam, got, wantGroup)
		}
	}

	// The new amqp_consumer_ prefix must not steal amqp_consumers_total: the
	// name does not carry that exact prefix ("consumers" has an "s" before the
	// underscore), so it still resolves through amqp_consumers_ to the very
	// same group.
	if strings.HasPrefix("amqp_consumers_total", "amqp_consumer_") {
		t.Error(`"amqp_consumers_total" starts with "amqp_consumer_"; the new prefix would steal it`)
	}
	if got := metricGroupFor(groups, "amqp_consumers_total"); got != "Broker \u2014 consumers" {
		t.Errorf("group(%q) = %q, want %q", "amqp_consumers_total", got, "Broker \u2014 consumers")
	}

	// Each added prefix must match ONLY the family (families) it was added for:
	// a wider match would move an already-homed family into a new group.
	newPrefixMatches := map[string][]string{
		"amqp_consumer_":        {"amqp_consumer_register_errors_total"},
		"amqp_deliveries_":      {"amqp_deliveries_dropped_total", "amqp_deliveries_requeued_total"},
		"amqp_delivery_":        {"amqp_delivery_send_errors_total"},
		"amqp_publish_refused_": {"amqp_publish_refused_total"},
	}
	for prefix, wantFams := range newPrefixMatches {
		var gotFams []string
		for name := range golden {
			if strings.HasPrefix(name, prefix) {
				gotFams = append(gotFams, name)
			}
		}
		sort.Strings(gotFams)
		sort.Strings(wantFams)
		if !slices.Equal(gotFams, wantFams) {
			t.Errorf("prefix %q matches %v; want exactly %v — an extra match would move an already-homed family",
				prefix, gotFams, wantFams)
		}
	}
}

// TestUI_VitalsSelectorsMatchInventory is the static gate: every metric name and
// every enumerable label literal the Metrics tab's VITALS table selects must
// exist in the broker's real metric inventory. A selector that can never match
// renders a permanent "unknown" tile — a silent lie — so a typo such as
// status_class="5xx" (the metric emits "5") must fail here, not on a dashboard.
func TestUI_VitalsSelectorsMatchInventory(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	sels := parseVitalSelectors(t, js)
	if len(sels) < 40 {
		t.Fatalf("parsed %d VITALS selectors; the table or this parser changed", len(sels))
	}
	golden := loadInventoryGolden(t)

	for _, p := range vitalSelectorProblems(sels, golden, labelDomains) {
		t.Error(p)
	}
}

// TestUI_VitalsSelectorGateCatchesBadLabel pins the gate itself: the exact bug
// this test exists to prevent (a status_class="5xx" matcher, where the metric
// emits the literal "5") must be reported.
func TestUI_VitalsSelectorGateCatchesBadLabel(t *testing.T) {
	js := "var VITALS = [\n" +
		`    { id: "mgmt_5xx", label: "Management 5xx", group: "Health", sel: "sum(amqp_management_requests_total{status_class=\"5xx\"})", render: "count", sev: "bad" }` + "\n" +
		"  ];"
	sels := parseVitalSelectors(t, js)
	require.Len(t, sels, 1)
	probs := vitalSelectorProblems(sels, map[string]bool{"amqp_management_requests_total": true}, labelDomains)
	require.Len(t, probs, 1, "the gate must flag exactly the illegal label literal")
	if !strings.Contains(probs[0], `"5xx"`) {
		t.Fatalf("the gate did not name the illegal value: %s", probs[0])
	}
}

// TestUI_VitalsSelectorGateCatchesUnknownFamily pins the other half of the
// gate: a selector naming a family absent from the golden is reported.
func TestUI_VitalsSelectorGateCatchesUnknownFamily(t *testing.T) {
	js := "var VITALS = [\n" +
		`    { id: "ghost", label: "Ghost", group: "Health", sel: "amqp_not_a_metric_total", render: "count", sev: "bad" }` + "\n" +
		"  ];"
	sels := parseVitalSelectors(t, js)
	probs := vitalSelectorProblems(sels, map[string]bool{"amqp_management_requests_total": true}, labelDomains)
	require.Len(t, probs, 1)
	if !strings.Contains(probs[0], "amqp_not_a_metric_total") {
		t.Fatalf("the gate did not name the unknown family: %s", probs[0])
	}
}
