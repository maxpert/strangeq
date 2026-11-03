package storage

// THE WAL'S BYTES MUST BE IDENTICAL TO THE CONTIGUOUS WRITER'S, WHATEVER
// THE BODY-UNION ARM IS.
//
// The copy-free writer stages a record as three pieces: the WAL-owned
// header, the message's body (ALIASED — handed to the kernel by reference)
// and the optional-property tail. The header carries the body union's
// DISCRIMINATOR and LENGTH:
//
//	bodyKind 0x00 (inline): [bodyLen u32][the body bytes]
//	bodyKind 0x01 (ref):   [refLen u16][the locator bytes]
//
// So the bytes that belong between the length field and the optional fields
// are the BODY for the inline arm and the LOCATOR for the reference arm. The
// record's length field, its CRC span and its staged piece must all use THAT
// selection — a selection that mixes the two arms produces a record that is
// not byte-identical to what appendMessageRecord writes for the same
// message.
//
// The failure this pins is not hypothetical: with a message that carries a
// BodyRef locator, the inline body bytes used to be staged where the
// locator belongs (or, with a nil body, the locator was dropped from the
// file entirely and the optional fields followed the reference header).

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

// captureWALWritePieces captures the exact pieces the WAL hands to the
// kernel for the next write, and lets the write through.
func captureWALWritePieces(t *testing.T) *[][]byte {
	t.Helper()
	var captured [][]byte
	restore := SetWALWriteVecForTest(func(f *os.File, parts [][]byte) (int, error) {
		captured = append(captured, parts...)
		return writevFile(f, parts)
	})
	t.Cleanup(restore)
	return &captured
}

// TestWALWrite_ReferenceArmBytesAreIdenticalToTheContiguousWriter pins the
// REFERENCE arm: a message that carries a BodyRef locator (and a DIFFERENT,
// non-nil inline body) must be written with the LOCATOR, exactly as the
// contiguous writer writes it.
func TestWALWrite_ReferenceArmBytesAreIdenticalToTheContiguousWriter(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	locator := []byte("L0C4T0R1") // 8-byte body-block locator
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte("the-inline-body-must-not-be-written-where-the-locator-belongs"),
		BodyRef:      locator,
		DeliveryMode: 2,
		DeliveryTag:  11,
		MessageID:    "mid-11",
		ContentType:  "text/plain",
		Priority:     3,
	}

	captured := captureWALWritePieces(t)

	done := make(chan error, 1)
	wm.WriteAsync("q", msg, 11, func(e error) { done <- e })
	require.NoError(t, <-done)

	var got []byte
	for _, p := range *captured {
		got = append(got, p...)
	}
	want, err := appendMessageRecord(nil, "q", msg, 11, false)
	require.NoError(t, err)

	require.True(t, bytes.Equal(got, want),
		"the reference arm must be written byte-identically to the contiguous writer:\n got %x\nwant %x",
		got, want)

	// AND THE LOCATOR — not the inline body — is what was written.
	require.Contains(t, string(got), string(locator),
		"the reference arm must carry the locator")
	require.NotContains(t, string(got), string(msg.Body),
		"the reference arm must NOT carry the inline body")
}

// TestWALWrite_InlineArmBytesAreIdenticalToTheContiguousWriter is the
// REVERSE case: a message with a body and NO locator is written with the
// body, exactly as the contiguous writer writes it.
func TestWALWrite_InlineArmBytesAreIdenticalToTheContiguousWriter(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte("the-inline-body"),
		DeliveryMode: 2,
		DeliveryTag:  12,
		MessageID:    "mid-12",
		ContentType:  "text/plain",
		Priority:     3,
	}

	captured := captureWALWritePieces(t)

	done := make(chan error, 1)
	wm.WriteAsync("q", msg, 12, func(e error) { done <- e })
	require.NoError(t, <-done)

	var got []byte
	for _, p := range *captured {
		got = append(got, p...)
	}
	want, err := appendMessageRecord(nil, "q", msg, 12, false)
	require.NoError(t, err)

	require.True(t, bytes.Equal(got, want),
		"the inline arm must be written byte-identically to the contiguous writer:\n got %x\nwant %x",
		got, want)
}

// TestWALWrite_ReferenceArmKeepsTheOptionalFieldsAfterTheHeader pins the
// other half of the reference arm: the optional fields follow the reference
// header (and the locator), and they are NOT glued onto the header — which
// is what a dropped locator would produce.
func TestWALWrite_ReferenceArmKeepsTheOptionalFieldsAfterTheHeader(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	locator := []byte("L0C4T0R1")
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte("the-inline-body-must-not-be-written-where-the-locator-belongs"),
		BodyRef:      locator,
		DeliveryMode: 2,
		DeliveryTag:  13,
		MessageID:    "mid-13",
		ContentType:  "text/plain",
	}

	captured := captureWALWritePieces(t)

	done := make(chan error, 1)
	wm.WriteAsync("q", msg, 13, func(e error) { done <- e })
	require.NoError(t, <-done)

	var got []byte
	for _, p := range *captured {
		got = append(got, p...)
	}
	want, err := appendMessageRecord(nil, "q", msg, 13, false)
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, want),
		"the reference arm must be written byte-identically to the contiguous writer")

	// The locator is present, and the optional properties follow it (the
	// contiguous writer's layout), so the record is not truncated.
	idx := bytes.Index(got, locator)
	require.GreaterOrEqual(t, idx, 0, "the locator must be written")
	require.Contains(t, string(got[idx+len(locator):]), "mid-13",
		"the optional fields must follow the reference header and the locator")
}
