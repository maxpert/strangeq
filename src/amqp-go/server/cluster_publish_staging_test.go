package server

import (
	"bytes"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestClusterPublishStagingDoesNotCopyLargeBody(t *testing.T) {
	burst := &clusterPublishBurst{}
	message := &protocol.Message{
		Exchange: "durable-direct", RoutingKey: "staging-key", ContentType: "application/octet-stream",
		Body: bytes.Repeat([]byte{0x00, 0xff}, 32<<10), DeliveryMode: 2,
	}
	_, err := burst.prepare(1, message)
	require.NoError(t, err)

	stats := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := burst.prepare(1, message); err != nil {
				b.Fatal(err)
			}
		}
	})
	require.Less(t, stats.AllocedBytesPerOp(), int64(len(message.Body)/4),
		"staging should allocate envelope metadata without allocating a full payload buffer")
}
