package main

import (
	"strings"
	"testing"
)

func TestSplitByPackage_SinglePackage(t *testing.T) {
	input := "goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go/broker\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkFoo-16    	     200	        21.04 ns/op	       0 B/op	       0 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go/broker	0.239s\n"

	sections := splitByPackage([]byte(input))
	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}
	if sections[0].pkg != "github.com/maxpert/amqp-go/broker" {
		t.Fatalf("got pkg %q", sections[0].pkg)
	}
	if !strings.Contains(string(sections[0].body), "BenchmarkFoo-16") {
		t.Fatalf("section body missing benchmark line:\n%s", sections[0].body)
	}
	if !strings.Contains(string(sections[0].body), "goos: darwin") {
		t.Fatalf("section body missing preamble:\n%s", sections[0].body)
	}
}

func TestSplitByPackage_MultiplePackagesAppended(t *testing.T) {
	// Mirrors what `make bench-gate` produces: several `go test <pkg>
	// -bench=... >> file` invocations appended in sequence, each with its
	// own complete goos/goarch/pkg/cpu header.
	input := "goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkVersus_AutoAck-16    	     200	      4704 ns/op	    1472 B/op	      32 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go	0.239s\n" +
		"goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go/broker\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkQueueDispatch_Publish-16    	     200	        21.04 ns/op	       0 B/op	       0 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go/broker	0.204s\n"

	sections := splitByPackage([]byte(input))
	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}
	if sections[0].pkg != "github.com/maxpert/amqp-go" {
		t.Fatalf("section 0 pkg = %q", sections[0].pkg)
	}
	if sections[1].pkg != "github.com/maxpert/amqp-go/broker" {
		t.Fatalf("section 1 pkg = %q", sections[1].pkg)
	}
	// Cross-package isolation: a benchmark name repeated in each package
	// must not leak from one section's body into the other.
	if strings.Contains(string(sections[0].body), "QueueDispatch_Publish") {
		t.Fatalf("root section leaked broker package content:\n%s", sections[0].body)
	}
	if strings.Contains(string(sections[1].body), "Versus_AutoAck") {
		t.Fatalf("broker section leaked root package content:\n%s", sections[1].body)
	}
}

func TestMergeSections_DuplicatePackageConcatenates(t *testing.T) {
	sections := []pkgSection{
		{pkg: "a", body: []byte("BenchmarkX-1  1  1 ns/op\n")},
		{pkg: "b", body: []byte("BenchmarkY-1  1  1 ns/op\n")},
		{pkg: "a", body: []byte("BenchmarkX-1  1  2 ns/op\n")},
	}
	byPkg, order := mergeSections(sections)

	if got := []string{"a", "b"}; order[0] != got[0] || order[1] != got[1] {
		t.Fatalf("order = %v, want %v", order, got)
	}
	if count := strings.Count(string(byPkg["a"]), "BenchmarkX-1"); count != 2 {
		t.Fatalf("merged package %q has %d benchmark lines, want 2:\n%s", "a", count, byPkg["a"])
	}
}

func TestSplitByPackage_Empty(t *testing.T) {
	if sections := splitByPackage([]byte("")); len(sections) != 0 {
		t.Fatalf("got %d sections for empty input, want 0", len(sections))
	}
}
