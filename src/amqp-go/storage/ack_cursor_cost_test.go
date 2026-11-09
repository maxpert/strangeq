package storage

import (
	"fmt"
	"testing"
)

// FOCUSED micro-benchmarks for the per-ack cost of the AckCursor.
//
// The broker's manual-ack path does, per acknowledged message:
//
//	storage.DeliverToConsumer(...)   -> AckCursor.OnDeliver
//	storage.AckFromConsumer(...)     -> AckCursor.OnAck
//
// These benchmarks measure EXACTLY that sequence, one tag delivered and one
// tag acked per iteration, with the unacked window held at `window` messages
// per consumer by acking the oldest outstanding tag each iteration — the
// in-order shape a FIFO consumer produces.
//
// window is therefore the prefetch depth. At the base of this work stream
// OnAck did an O(window) sync.Map.Range per in-order ack (scanLowest) to
// maintain a min-ack cursor nothing read; the W1000_C1 row is the falsifiable
// form of that removal.

// benchAckCursorAckPath drives one deliver + one in-order ack per iteration —
// the exact per-message work of the broker's manual-ack path.
func benchAckCursorAckPath(b *testing.B, window, consumers int) {
	ac := NewAckCursor()
	ctags := make([]string, consumers)
	pending := make([][]uint64, consumers)
	for i := range ctags {
		ctags[i] = fmt.Sprintf("c%d", i)
		ac.OnConsumerRegister(ctags[i])
		pending[i] = make([]uint64, 0, window+1)
	}

	var seq uint64
	for w := 0; w < window; w++ {
		for i := 0; i < consumers; i++ {
			seq++
			ac.OnPublish(seq)
			ac.OnDeliver(seq, ctags[i])
			pending[i] = append(pending[i], seq)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := i % consumers
		seq++
		ac.OnPublish(seq)
		ac.OnDeliver(seq, ctags[c])
		ac.OnAck(pending[c][0], ctags[c])
		pending[c] = append(pending[c][1:], seq)
	}
}

func BenchmarkAckCursorAckPath(b *testing.B) {
	for _, tc := range []struct {
		name      string
		window    int
		consumers int
	}{
		{"W10_C1", 10, 1},
		{"W100_C1", 100, 1},
		{"W1000_C1", 1000, 1},
		{"W100_C32", 100, 32},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchAckCursorAckPath(b, tc.window, tc.consumers)
		})
	}
}
