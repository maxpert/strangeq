package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// benchResult is one benchstat A/B comparison row: one metric table (sec/op,
// B/op, allocs/op, ...) crossed with one benchmark name.
type benchResult struct {
	metric      string
	name        string
	significant bool    // benchstat's own two-sample test called this row non-noise
	deltaPct    float64 // meaningful only when significant; positive = new is higher than baseline
}

// parseBenchstatCSV parses `benchstat -format=csv <baseline> <new>` output.
// Each metric table looks like:
//
//	,<baseline-file>,,<new-file>,,,
//	,<metric>,CI,<metric>,CI,vs base,P
//	<name>,<value>,<ci>,<value>,<ci>,<delta-or-~>,<p-or-empty>
//	...
//	geomean,<value>,,<value>,,<delta>,
//	<blank line>
//
// repeated once per metric. The two header rows (both have an empty first
// field) and the geomean summary row are skipped: the gate acts on "any
// benchmark" (design doc, W0 section), not the geomean aggregate.
//
// benchstat itself decides significance (its "~" marker) at the -alpha
// passed to it, so this parser does not re-derive it from the P field: a
// "~" row is always non-significant and never gates, matching "gate
// allocs/op via the two-sample test (p<0.05)... not a raw 0-delta".
func parseBenchstatCSV(csvOutput []byte) ([]benchResult, error) {
	var results []benchResult
	var metric string

	scanner := bufio.NewScanner(bytes.NewReader(csvOutput))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			// Metadata (goos:/goarch:/pkg:/cpu:) or a stray benchstat
			// diagnostic line ("Bn: need >= 6 samples..."); not a table row.
			continue
		}

		name := fields[0]
		if name == "" {
			// The filenames header row's second field is a file path; the
			// units header row's second field is the metric name itself
			// (e.g. "sec/op"). Only the latter matters here.
			if isUnitHeader(fields[1]) {
				metric = fields[1]
			}
			continue
		}
		if name == "geomean" {
			continue
		}

		vsBase := fields[len(fields)-2]
		r := benchResult{metric: metric, name: name}
		switch vsBase {
		case "~", "", "?":
			results = append(results, r)
			continue
		}
		delta, err := parseDeltaPercent(vsBase)
		if err != nil {
			return nil, fmt.Errorf("benchgate: parsing delta %q for benchmark %s (%s): %w", vsBase, name, metric, err)
		}
		r.significant = true
		r.deltaPct = delta
		results = append(results, r)
	}
	return results, nil
}

// isUnitHeader reports whether s is a per-operation unit column header
// (sec/op, B/op, allocs/op, and any future "/op"-suffixed metric benchstat
// adds), as opposed to a file path in the filenames header row.
func isUnitHeader(s string) bool {
	return strings.HasSuffix(s, "/op")
}

func parseDeltaPercent(s string) (float64, error) {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	return strconv.ParseFloat(s, 64)
}
