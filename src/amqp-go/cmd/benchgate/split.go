package main

import (
	"bufio"
	"bytes"
	"strings"
)

// pkgSection is one package's slice of a combined `go test -bench` output
// stream: the "goos:"/"goarch:"/"cpu:"/"pkg:" header for that package, plus
// every following line up to (but not including) the next "pkg:" line or
// EOF.
type pkgSection struct {
	pkg  string
	body []byte
}

// splitByPackage splits a benchmark output stream that may contain results
// from several packages — e.g. `make bench-gate` appends one `go test <pkg>
// -bench=...` invocation's output per in-scope package into a single file —
// into one section per package, keyed by the import path on each
// "pkg: <import-path>" line.
//
// This is what makes per-package benchstat comparisons possible from a
// single committed baseline file: two benchmarks that happen to share a
// name in different packages (root/broker/storage/protocol) never end up in
// the same benchstat comparison, because they never share a benchstat input
// file — each package's body is diffed against its own baseline body only
// (see main.go).
//
// "goos:"/"goarch:"/"cpu:" lines are buffered and attached to the section
// started by the "pkg:" line that follows them, since each `go test`
// process prints its own complete header before its own "pkg:" line.
func splitByPackage(data []byte) []pkgSection {
	var sections []pkgSection
	var cur *pkgSection
	var preamble []string

	finish := func() {
		if cur != nil {
			sections = append(sections, *cur)
			cur = nil
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "goos:"), strings.HasPrefix(line, "goarch:"), strings.HasPrefix(line, "cpu:"):
			preamble = append(preamble, line)
			continue
		case strings.HasPrefix(line, "pkg: "):
			finish()
			cur = &pkgSection{pkg: strings.TrimSpace(strings.TrimPrefix(line, "pkg: "))}
			for _, l := range preamble {
				appendLine(cur, l)
			}
			preamble = preamble[:0]
			appendLine(cur, line)
			continue
		}
		if cur == nil {
			// Content before any "pkg:" line has no section to attribute
			// it to (shouldn't happen for well-formed `go test` output).
			continue
		}
		appendLine(cur, line)
	}
	finish()
	return sections
}

func appendLine(s *pkgSection, line string) {
	s.body = append(s.body, line...)
	s.body = append(s.body, '\n')
}

// mergeSections merges sections sharing the same package (concatenating
// their bodies, in encounter order) and returns them keyed by package,
// alongside the first-seen package order for deterministic reporting.
func mergeSections(sections []pkgSection) (byPkg map[string][]byte, order []string) {
	byPkg = make(map[string][]byte)
	for _, s := range sections {
		if _, ok := byPkg[s.pkg]; !ok {
			order = append(order, s.pkg)
		}
		byPkg[s.pkg] = append(byPkg[s.pkg], s.body...)
	}
	return byPkg, order
}
