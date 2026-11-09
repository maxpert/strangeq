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

	"github.com/maxpert/amqp-go/storage"
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
// -> "2".."5"), server/recovery_metrics.go (RecoveryOutcome* and
// ShutdownResult*), and storage/syncpolicy.go (the amqp_wal_full_flush_total
// site labels, taken from storage.WALFullFlushSites so the pinned set cannot
// drift from the producer). Only families with a closed enum are listed; a
// selector on any other family is checked for the metric name alone.
var labelDomains = map[string]map[string]bool{
	"amqp_recovery_faults_total":        {"benign": true, "degraded": true, "fatal": true},
	"amqp_shutdown_total":               {"clean": true, "forced": true, "incomplete": true},
	"amqp_management_requests_total":    {"2": true, "3": true, "4": true, "5": true},
	"amqp_management_ui_requests_total": {"2": true, "3": true, "4": true, "5": true},
	"amqp_wal_full_flush_total":         walFullFlushSiteDomain(),
}

// walFullFlushSiteDomain builds the site-label domain for
// amqp_wal_full_flush_total from the producer's own list, so a new site label
// cannot silently fall outside the gate.
func walFullFlushSiteDomain() map[string]bool {
	sites := storage.WALFullFlushSites()
	dom := make(map[string]bool, len(sites))
	for _, s := range sites {
		dom[s] = true
	}
	return dom
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
// name is absent from the inventory golden, or whose label matcher cannot select
// a real value of that family's enumerable domain. Domains that are not
// enumerable are checked for the metric name only.
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
			probs = append(probs, regexMatcherProblems(s, dom)...)
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

// regexMatcherProblems checks a regex label matcher against a closed domain:
//
//	(a) the pattern must compile under Go's RE2 (the same engine the broker
//	    exposes), so a malformed pattern fails here, not on a dashboard;
//	(b) a pure "|"-alternation of literals must name only domain values, which
//	    catches a misspelled site such as "boot_tai";
//	(c) the matcher must select at least one domain value, and a "!~" must also
//	    keep at least one, so it cannot silently drop the whole domain (".*").
func regexMatcherProblems(s vitalSelector, dom map[string]bool) []string {
	var probs []string
	re, err := regexp.Compile("^(?:" + s.value + ")$")
	if err != nil {
		return append(probs, fmt.Sprintf(
			"VITALS %q selects %q, but %q is not a valid RE2 pattern: %v",
			s.id, s.sel, s.value, err))
	}
	if lits, ok := alternationLiterals(s.value); ok {
		for _, lit := range lits {
			if !dom[lit] {
				probs = append(probs, fmt.Sprintf(
					"VITALS %q selects %q, but alternative %q is not a legal %s value (legal: %s)",
					s.id, s.sel, lit, s.name, strings.Join(sortedKeys(dom), ", ")))
			}
		}
	}
	matched := 0
	for v := range dom {
		if re.MatchString(v) {
			matched++
		}
	}
	if matched == 0 {
		probs = append(probs, fmt.Sprintf(
			"VITALS %q selects %q, but the pattern %q matches no %s value (legal: %s)",
			s.id, s.sel, s.value, s.name, strings.Join(sortedKeys(dom), ", ")))
	}
	if s.op == "!~" && matched == len(dom) {
		probs = append(probs, fmt.Sprintf(
			"VITALS %q selects %q, but %q drops the entire %s domain (legal: %s)",
			s.id, s.sel, s.value, s.name, strings.Join(sortedKeys(dom), ", ")))
	}
	return probs
}

// alternationLiterals reports whether pattern is a pure "|"-separated list of
// literal strings (no other regex metacharacter) and returns its alternatives.
func alternationLiterals(pattern string) ([]string, bool) {
	if pattern == "" {
		return nil, false
	}
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '|':
		case '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\', '^', '$':
			return nil, false
		}
	}
	parts := strings.Split(pattern, "|")
	for _, p := range parts {
		if p == "" {
			return nil, false
		}
	}
	return parts, true
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

// TestUI_VitalsSelectorProblems_Regex pins the regex half of the static gate: a
// malformed pattern, a misspelled literal alternative, and a pattern that drops
// the whole domain must all be reported, while the shipped selector passes.
func TestUI_VitalsSelectorProblems_Regex(t *testing.T) {
	const family = "amqp_wal_full_flush_total"
	golden := map[string]bool{family: true}
	mk := func(id, op, value string) vitalSelector {
		return vitalSelector{
			id:    id,
			sel:   family + `{site` + op + `"` + value + `"}`,
			name:  family,
			label: "site",
			op:    op,
			value: value,
		}
	}
	cases := []struct {
		name    string
		sel     vitalSelector
		wantAny []string
		want    int
	}{
		{"malformed pattern", mk("bad", "!~", "boot["), []string{"not a valid RE2 pattern"}, 1},
		{"misspelled alternative", mk("typo", "!~", "boot_tai|boot_mkdir|close"), []string{"boot_tai"}, 1},
		{"drops the whole domain", mk("all", "!~", ".*"), []string{"drops the entire"}, 1},
		{"shipped selector", mk("ok", "!~", "boot_tail|boot_mkdir|close"), nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probs := vitalSelectorProblems([]vitalSelector{tc.sel}, golden, labelDomains)
			if len(probs) != tc.want {
				t.Fatalf("problems = %v, want %d", probs, tc.want)
			}
			for _, want := range tc.wantAny {
				if !strings.Contains(strings.Join(probs, "\n"), want) {
					t.Errorf("problems %v do not mention %q", probs, want)
				}
			}
		})
	}
}

// TestUI_VitalsSelectorWalFullFlushExclusion pins the shipped wal_full_flush
// selector's exclusion set to exactly the three off-traffic-path sites, so a
// change to the site labels or the pattern cannot silently widen or narrow the
// alarm. The domain comes from storage.WALFullFlushSites, the producer's list.
func TestUI_VitalsSelectorWalFullFlushExclusion(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	sels := parseVitalSelectors(t, js)
	var wal *vitalSelector
	for i := range sels {
		if sels[i].id == "wal_full_flush" {
			wal = &sels[i]
			break
		}
	}
	require.NotNil(t, wal, "app.js VITALS has no wal_full_flush tile")
	require.Equal(t, "!~", wal.op, "wal_full_flush must exclude sites with an anchored !~ matcher")

	dom := labelDomains["amqp_wal_full_flush_total"]
	require.NotEmpty(t, dom, "amqp_wal_full_flush_total has no pinned site domain")
	for _, site := range storage.WALFullFlushSites() {
		if !dom[site] {
			t.Errorf("producer site %q is not in the pinned domain", site)
		}
	}

	re := regexp.MustCompile("^(?:" + wal.value + ")$")
	var dropped []string
	for site := range dom {
		if re.MatchString(site) {
			dropped = append(dropped, site)
		}
	}
	sort.Strings(dropped)
	require.Equal(t, []string{"boot_mkdir", "boot_tail", "close"}, dropped,
		"the wal_full_flush exclusion must be exactly the off-traffic-path boot/close sites")
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
