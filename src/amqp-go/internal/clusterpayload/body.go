// Package clusterpayload defines the optional lossless encoding of a cluster
// message body. AMQP envelopes still advertise the original body length.
package clusterpayload

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/klauspost/compress/s2"
)

const (
	RawEnvelope         = 1
	PackedEnvelope      = 2
	SmallPackedEnvelope = 3
	minSmallPackedBody  = 1 << 10
	minPackedBody       = 32 << 10
	maxPackedBody       = 128 << 10
)

var packingScratch = sync.Pool{New: func() any {
	scratch := make([]byte, s2.MaxEncodedLen(maxPackedBody))
	return &scratch
}}

var errPackedBody = errors.New("invalid compressed cluster message body")

// Pack returns an owned block only when the size reduction warrants the codec.
func Pack(body []byte) []byte {
	if len(body) < minPackedBody || len(body) > maxPackedBody {
		return nil
	}
	return pack(body)
}

// PackSmall also considers smaller bodies, marking them with their own
// envelope version. Large bodies retain their existing encoding.
func PackSmall(body []byte) ([]byte, byte) {
	if len(body) >= minPackedBody {
		return Pack(body), PackedEnvelope
	}
	if len(body) < minSmallPackedBody {
		return nil, RawEnvelope
	}
	return pack(body), SmallPackedEnvelope
}

func pack(body []byte) []byte {
	scratch := packingScratch.Get().(*[]byte)
	encoded := s2.EncodeSnappy(*scratch, body)
	var packed []byte
	if len(encoded) <= len(body)*3/4 {
		packed = bytes.Clone(encoded)
	}
	packingScratch.Put(scratch)
	return packed
}

func IsPackedEnvelope(version byte) bool {
	return version == PackedEnvelope || version == SmallPackedEnvelope
}

func packedSize(body []byte, version byte) (int, error) {
	low, high := minPackedBody, maxPackedBody
	if version == SmallPackedEnvelope {
		low, high = minSmallPackedBody, minPackedBody-1
	}
	size, err := s2.DecodedLen(body)
	if err != nil || size < low || size > high || len(body) > size*3/4 {
		return 0, errPackedBody
	}
	return size, nil
}

func Unpack(body []byte, expected uint64, version byte) ([]byte, error) {
	if !IsPackedEnvelope(version) {
		return nil, errPackedBody
	}
	size, err := packedSize(body, version)
	if err != nil || uint64(size) != expected {
		return nil, errPackedBody
	}
	return s2.Decode(nil, body)
}

// EnvelopeSize accounts for a packed envelope at its original size. The
// queue retains the encoded bytes, which need no expansion until delivery.
func EnvelopeSize(envelope []byte) (int, error) {
	if len(envelope) == 0 {
		return 0, errPackedBody
	}
	switch envelope[0] {
	case RawEnvelope:
		return len(envelope), nil
	case PackedEnvelope, SmallPackedEnvelope:
		header, body, err := packedEnvelope(envelope)
		return header + body, err
	default:
		return 0, errPackedBody
	}
}

// ValidateEnvelope also validates every compressed token before the queue
// admits the message or restores it from a snapshot.
func ValidateEnvelope(envelope []byte) (int, error) {
	if len(envelope) == 0 || !IsPackedEnvelope(envelope[0]) {
		return EnvelopeSize(envelope)
	}
	header, body, err := packedEnvelope(envelope)
	if err != nil {
		return 0, err
	}
	scratch := packingScratch.Get().(*[]byte)
	_, err = s2.Decode(*scratch, envelope[header:])
	packingScratch.Put(scratch)
	return header + body, err
}

func packedEnvelope(envelope []byte) (header, body int, err error) {
	if len(envelope) < 7 {
		return 0, 0, errPackedBody
	}
	props := 7 + int(envelope[1]) + int(envelope[2])
	end := uint64(props) + uint64(binary.BigEndian.Uint32(envelope[3:7]))
	if end < uint64(props+14) || end >= uint64(len(envelope)) {
		return 0, 0, errPackedBody
	}
	body, err = packedSize(envelope[end:], envelope[0])
	if err != nil || uint64(body) != binary.BigEndian.Uint64(envelope[props+4:props+12]) {
		return 0, 0, errPackedBody
	}
	return int(end), body, nil
}
