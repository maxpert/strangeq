package main

import (
	"testing"
)

// Fixture captured from a real `benchstat -format=csv old.txt new.txt` run
// (golang.org/x/perf/cmd/benchstat, pinned via tools/go.mod) comparing 10
// samples per side: sec/op regresses +6% (significant), B/op and allocs/op
// jitter ±1 with no significant change. See cmd/benchgate's design notes
// for why -format=csv (not the classic text table) is used: this pinned
// version's CSV already emits "vs base"/"P" comparison columns, contrary to
// the older behavior the design doc's own risk note warned about — verified
// live before committing to this format.
const sampleBenchstatCSV = `goos: darwin
goarch: arm64
pkg: example.com/foo
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Foo-8,1.0005e-07,1%,1.0605e-07,1%,+6.00%,p=0.000 n=10
geomean,1.0004999999999995e-07,,1.0605000000000003e-07,,+6.00%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
Foo-8,10,10%,10,0%,~,p=0.474 n=10
geomean,10.000000000000002,,10.000000000000002,,+0.00%,

,old.txt,,new.txt,,,
,allocs/op,CI,allocs/op,CI,vs base,P
Foo-8,1,0%,1,0%,~,p=1.000 n=10
geomean,1,,1,,+0.00%,
`

func TestParseBenchstatCSV_RealFixture(t *testing.T) {
	results, err := parseBenchstatCSV([]byte(sampleBenchstatCSV))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3 (one per metric table): %+v", len(results), results)
	}

	want := map[string]benchResult{
		"sec/op":    {metric: "sec/op", name: "Foo-8", significant: true, deltaPct: 6.0},
		"B/op":      {metric: "B/op", name: "Foo-8", significant: false},
		"allocs/op": {metric: "allocs/op", name: "Foo-8", significant: false},
	}
	for _, r := range results {
		w, ok := want[r.metric]
		if !ok {
			t.Fatalf("unexpected metric %q in results", r.metric)
		}
		if r.name != w.name || r.significant != w.significant {
			t.Errorf("metric %s: got %+v, want name=%s significant=%v", r.metric, r, w.name, w.significant)
		}
		if w.significant && r.deltaPct != w.deltaPct {
			t.Errorf("metric %s: got deltaPct=%v, want %v", r.metric, r.deltaPct, w.deltaPct)
		}
	}
}

func TestParseBenchstatCSV_SkipsGeomeanAndHeaders(t *testing.T) {
	results, err := parseBenchstatCSV([]byte(sampleBenchstatCSV))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	for _, r := range results {
		if r.name == "" || r.name == "geomean" {
			t.Errorf("geomean or header row leaked into results: %+v", r)
		}
	}
}

func TestParseBenchstatCSV_MultipleBenchmarksInOneTable(t *testing.T) {
	input := `goos: darwin
goarch: arm64
pkg: example.com/foo
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
BenchmarkA-8,100n,1%,101n,1%,~,p=0.900 n=10
BenchmarkB-8,100n,1%,105n,1%,+5.00%,p=0.001 n=10
geomean,100n,,103n,,+3.00%,
`
	results, err := parseBenchstatCSV([]byte(input))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2: %+v", len(results), results)
	}
	if results[0].name != "BenchmarkA-8" || results[0].significant {
		t.Errorf("BenchmarkA-8: got %+v, want non-significant", results[0])
	}
	if results[1].name != "BenchmarkB-8" || !results[1].significant || results[1].deltaPct != 5.0 {
		t.Errorf("BenchmarkB-8: got %+v, want significant +5.00%%", results[1])
	}
}

func TestParseBenchstatCSV_MalformedDeltaIsError(t *testing.T) {
	input := `,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Foo-8,100n,1%,110n,1%,+not-a-number%,p=0.001 n=10
`
	if _, err := parseBenchstatCSV([]byte(input)); err == nil {
		t.Fatal("expected an error for an unparseable delta, got nil")
	}
}

func TestIsUnitHeader(t *testing.T) {
	cases := map[string]bool{
		"sec/op":             true,
		"B/op":               true,
		"allocs/op":          true,
		"old.txt":            false,
		"/some/path/new.txt": false,
	}
	for in, want := range cases {
		if got := isUnitHeader(in); got != want {
			t.Errorf("isUnitHeader(%q) = %v, want %v", in, got, want)
		}
	}
}
