package broker

import "strings"

// Iterative allocation-free topic matcher (WS-D §9).
//
// RabbitMQ semantics (R10, rabbit_db_topic_exchange.erl): a routing/binding
// key is split on '.', so "" has ZERO words and a trailing dot yields a
// trailing EMPTY word ("a.b." is [a b ""]). `*` matches exactly one (possibly
// empty) word; `#` matches zero or more words.
//
// The algorithm is the classic glob matcher with single-star backtracking,
// lifted from characters to words: `#` is glob `*`, `*` is glob `?`, a
// literal word is a literal character. Greedy backtracking to the MOST RECENT
// `#` is correct for globs, so every earlier `#` is subsumed — no
// memoisation, no recursion, and therefore no exponential blow-up (the old
// recursive matcher took 22.9s on a 12-`#` pattern; this takes ~3µs on a
// 127-`#` pattern). Time is O(P·K) word comparisons with P,K <= 128 (shortstr
// bound); space is O(1) with ZERO allocations — it walks byte offsets with
// strings.IndexByte and never splits.

// topicSeg returns the word starting at byte offset i of s and the offset of
// the next word. len(s)+1 is the "no more words" sentinel, so "a." yields
// words "a" and "" (RabbitMQ split, R10), and "" yields zero words.
func topicSeg(s string, i int) (string, int) {
	j := strings.IndexByte(s[i:], '.')
	if j < 0 {
		return s[i:], len(s) + 1
	}
	return s[i : i+j], i + j + 1
}

func topicStart(s string) int {
	if s == "" {
		return 1 // len("")+1: zero words
	}
	return 0
}

// topicMatch reports whether a topic binding pattern matches a routing key:
// '*' = exactly one word (may be empty), '#' = zero or more words.
func topicMatch(pattern, key string) bool {
	if pattern == key || pattern == "#" {
		return true
	}
	pi, ki := topicStart(pattern), topicStart(key)
	starP, starK := -1, -1
	for ki <= len(key) {
		kw, kNext := topicSeg(key, ki)
		if pi <= len(pattern) {
			pw, pNext := topicSeg(pattern, pi)
			if pw == "#" {
				starP, starK, pi = pNext, ki, pNext
				continue
			}
			if pw == "*" || pw == kw {
				pi, ki = pNext, kNext
				continue
			}
		}
		if starP >= 0 {
			_, n := topicSeg(key, starK)
			starK, ki, pi = n, n, starP
			continue
		}
		return false
	}
	for pi <= len(pattern) {
		pw, pNext := topicSeg(pattern, pi)
		if pw != "#" {
			return false
		}
		pi = pNext
	}
	return true
}

// matchTopicPattern is kept as the single call-site seam (collectTargetQueues
// uses it per binding); it now delegates to the iterative matcher.
func (b *StorageBroker) matchTopicPattern(pattern, routingKey string) bool {
	return topicMatch(pattern, routingKey)
}
