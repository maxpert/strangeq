package protocol

import "testing"

// FuzzBindingMethodsDecode feeds arbitrary bytes to the binding-method family
// decoders. Deserialize must never panic; if it accepts the input, Serialize
// of the decoded value must not panic and must not fail (every shortstr that
// came off the wire fits in 255 bytes by construction).
func FuzzBindingMethodsDecode(f *testing.F) {
	seeds := [][]byte{
		// queue.bind
		cat(qbPrefix, []byte{0x00}, emptyTable),
		cat(qbPrefix, []byte{0x00}, xMatchAllTable),
		cat(qbPrefix, []byte{0x01}, emptyTable),
		// queue.delete
		{0x00, 0x00, 0x01, 'q', 0x00},
		{0x00, 0x00, 0x01, 'q', 0x01},
		{0x00, 0x00, 0x01, 'q', 0x02},
		{0x00, 0x00, 0x01, 'q', 0x03},
		{0x00, 0x00, 0x01, 'q', 0x04},
		{0x00, 0x00, 0x01, 'q', 0x07},
		// queue.purge
		{0x00, 0x00, 0x01, 'q', 0x00},
		{0x00, 0x00, 0x01, 'q', 0x01},
		// exchange.delete
		{0x00, 0x00, 0x02, 'e', 'x', 0x00},
		{0x00, 0x00, 0x02, 'e', 'x', 0x01},
		{0x00, 0x00, 0x02, 'e', 'x', 0x02},
		{0x00, 0x00, 0x02, 'e', 'x', 0x03},
		// exchange.bind / exchange.unbind (identical argument bytes)
		cat(ebPrefix, []byte{0x00}, emptyTable),
		cat(ebPrefix, []byte{0x00}, xMatchAllTable),
		cat(ebPrefix, []byte{0x01}, emptyTable),
		// queue.unbind (no flag octet)
		cat(qbPrefix, emptyTable),
		cat(qbPrefix, xMatchAllTable),
		// basic.cancel
		{0x04, 'c', 't', 'a', 'g', 0x00},
		{0x02, 'c', '2', 0x01},
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		methods := []wireMethod{
			&QueueBindMethod{},
			&QueueUnbindMethod{},
			&ExchangeBindMethod{},
			&ExchangeUnbindMethod{},
			&QueueDeleteMethod{},
			&QueuePurgeMethod{},
			&ExchangeDeleteMethod{},
			&BasicCancelMethod{},
		}
		for _, m := range methods {
			if err := m.Deserialize(data); err != nil {
				continue
			}
			enc, err := m.Serialize()
			if err != nil {
				t.Fatalf("Serialize after successful Deserialize(% x) failed for %T: %v", data, m, err)
			}
			_ = enc
		}
	})
}
