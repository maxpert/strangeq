package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// topicSemanticsRows is the full §9.4 golden table (rows 1-74; row 75 is the
// pathological perf case, TestTopic_Pathological_Bounded). The expectations
// are RabbitMQ split/match semantics (R10): `*` matches exactly one (possibly
// empty) word, `#` matches zero or more, `""` has zero words, and a trailing
// dot yields a trailing EMPTY word ("a.b." is [a b ""]).
var topicSemanticsRows = []struct {
	pattern string
	key     string
	want    bool
}{
	{"a.b.c", "a.b.c", true},                    // 1
	{"a.b.c", "a.b", false},                     // 2
	{"a.b", "a.b.c", false},                     // 3
	{"", "", true},                              // 4
	{"", "a", false},                            // 5
	{"a", "", false},                            // 6
	{"#", "any.routing.key.value", true},        // 7
	{"#", "", true},                             // 8
	{"#", "foo", true},                          // 9
	{"a.#", "a", true},                          // 10
	{"a.#", "a.b", true},                        // 11
	{"a.#", "a.b.c.d", true},                    // 12
	{"a.#", "b", false},                         // 13
	{"a.#", "ab", false},                        // 14
	{"#.b", "b", true},                          // 15
	{"#.b", "a.b", true},                        // 16
	{"#.b", "a.x.b", true},                      // 17
	{"#.b", "a.x.c", false},                     // 18
	{"a.#.b", "a.b", true},                      // 19
	{"a.#.b", "a.x.b", true},                    // 20
	{"a.#.b", "a.x.y.b", true},                  // 21
	{"a.#.b", "a.x.b.c", false},                 // 22
	{"a.#.b", "b", false},                       // 23
	{"*", "foo", true},                          // 24
	{"*", "", false},                            // 25
	{"*", "a.b", false},                         // 26
	{"a.*", "a.foo", true},                      // 27
	{"a.*", "a.foo.bar", false},                 // 28
	{"a.*", "a", false},                         // 29
	{"*.b", "foo.b", true},                      // 30
	{"*.b", "foo.bar.b", false},                 // 31
	{"*.b", "b", false},                         // 32
	{"a.*.b", "a.foo.b", true},                  // 33
	{"a.*.b", "a.foo.bar.b", false},             // 34
	{"a.*.b", "a.b", false},                     // 35
	{"*.orange.*", "quick.orange.rabbit", true}, // 36
	{"*.orange.*", "lazy.orange.rabbit", true},  // 37
	{"*.orange.*", "quick.brown.rabbit", false}, // 38
	{"lazy.#", "lazy.orange.rabbit", true},      // 39
	{"lazy.#", "quick.orange.rabbit", false},    // 40
	{"*.*.rabbit", "quick.orange.rabbit", true}, // 41
	{"*.*.rabbit", "quick.orange.fox", false},   // 42
	{"#.rabbit", "quick.orange.rabbit", true},   // 43
	{"a.*.#", "a.b.c.d", true},                  // 44
	{"a.*.#", "a.b", true},                      // 45
	{"a.*.#", "a", false},                       // 46
	{"#.#", "a.b", true},                        // 47
	{"#.#", "", true},                           // 48
	{"#.#", "a", true},                          // 49
	{"a.#.#.b", "a.b", true},                    // 50
	// Rows 51-58: the trailing-dot defect class (T9.2). The OLD recursive
	// matcher got every one of these wrong; RabbitMQ semantics are on the left.
	{"a.b", "a.b.", false},    // 51
	{"a.", "a", false},        // 52
	{"a", "a.", false},        // 53
	{"a.b.", "a.b", false},    // 54
	{"*", ".", false},         // 55
	{"", ".", false},          // 56
	{"a.*", "a.", true},       // 57
	{"*.*", ".", true},        // 58
	{"a..b", "a..b", true},    // 59
	{"a.*.b", "a..b", true},   // 60
	{"a.#", "a.", true},       // 61
	{"#.b", ".b", true},       // 62
	{"#", ".", true},          // 63
	{".", ".", true},          // 64
	{"a.b.", "a.b.", true},    // 65
	{"a.*.", "a.x.", true},    // 66
	{".a", "a", false},        // 67
	{"*.a", ".a", true},       // 68
	{"#.*", "", false},        // 69
	{"#.*", "a", true},        // 70
	{"*.#", "", false},        // 71
	{"#.a.#", "a", true},      // 72
	{"#.a.#", "x.a.y", true},  // 73
	{"#.a.#", "x.b.y", false}, // 74
}

// TestTopic_Matching_SemanticsTable (T9.1) drives the full §9.4 table through
// the broker's matchTopicPattern. Pre-fix failure mechanism: the recursive
// matcher represents "remaining key" as a string, so "" conflates "no words
// left" with "one empty word" — rows 51-58 fail, and #/#-heavy patterns are
// exponential (row 75, covered by TestTopic_Pathological_Bounded).
func TestTopic_Matching_SemanticsTable(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	for i, row := range topicSemanticsRows {
		got := b.matchTopicPattern(row.pattern, row.key)
		assert.Equal(t, row.want, got, "row %d: matchTopicPattern(%q, %q)", i+1, row.pattern, row.key)
	}
}

// TestTopic_TrailingDot_NoMatch (T9.2) isolates the trailing-dot defect class
// (§9.4 rows 51-58): the current matcher reports "a.b"=="a.b.", "a."=="a",
// "a"=="a.", "a.b."=="a.b", "*"=="." and ""=="." (all wrong), and misses
// "a.*"=="a." and "*.*"=="." (both should match).
func TestTopic_TrailingDot_NoMatch(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	mismatches := []struct {
		pattern, key string
		want         bool
	}{
		{"a.b", "a.b.", false},
		{"a.", "a", false},
		{"a", "a.", false},
		{"a.b.", "a.b", false},
		{"*", ".", false},
		{"", ".", false},
		{"a.*", "a.", true},
		{"*.*", ".", true},
	}
	for _, tc := range mismatches {
		assert.Equal(t, tc.want, b.matchTopicPattern(tc.pattern, tc.key),
			"matchTopicPattern(%q, %q)", tc.pattern, tc.key)
	}
}

// TestTopic_Pathological_Bounded (T9.3) is §9.4 row 75: a 127-`#` pattern
// against a 128-word key must resolve in bounded time. Pre-fix mechanism: the
// recursive matcher branches per `#` with no memoisation, so it is exponential
// in the number of `#`s (the 12-hash variant takes 22.9s; 127 effectively
// never finishes). The iterative matcher is O(P·K) with P,K <= 128.
func TestTopic_Pathological_Bounded(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	pattern := strings.Repeat("#.", 127) + "x" // 255 bytes
	key := strings.Repeat("a.", 127) + "b"     // 255 bytes

	done := make(chan bool, 1)
	go func() { done <- b.matchTopicPattern(pattern, key) }()
	select {
	case got := <-done:
		assert.False(t, got)
	case <-time.After(1 * time.Second):
		t.Fatal("pathological topic match did not finish in 1s (exponential matcher)")
	}
}
