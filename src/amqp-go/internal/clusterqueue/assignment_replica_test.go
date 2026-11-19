package clusterqueue

import (
	"bytes"
	"errors"
	"hash/crc32"
	"testing"
)

type missingAssignmentSource struct{}

func (missingAssignmentSource) AssignmentJournalEntry() (uint64, uint64, int64) {
	return 92, 8, 256 << 10
}

func (missingAssignmentSource) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("leader must not read a referenced body")
}

func TestAssignmentReplicaMixedPage(t *testing.T) {
	request := AssignmentRequest{Queue: "queue", Incarnation: 10, Session: 12, Owner: "owner", After: 4, Limit: 2, Bytes: 128 << 10}
	coverage := AssignmentCoverage{Term: 8, First: 90, Last: 95}
	full, err := EncodeAssignmentRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	replicaRequest := EncodeAssignmentReplicaRequest(full, coverage)
	decodedRequest, decodedCoverage, err := DecodeAssignmentReplicaRequest(replicaRequest)
	if err != nil || decodedRequest != request || decodedCoverage != coverage || !bytes.Equal(InlineAssignmentReplicaRequest(replicaRequest), full) {
		t.Fatalf("replica request did not preserve legacy request: %#v %#v %v", decodedRequest, decodedCoverage, err)
	}
	body := bytes.Repeat([]byte{0xcd}, 64<<10)
	message, err := (journalBodyOrigin{source: missingAssignmentSource{}, offset: 37}).message(9, body)
	if err != nil {
		t.Fatal(err)
	}
	cold := message.delivery(5)
	cold.Redelivered = true
	inline := Delivery{MessageID: 12, Attempt: 6, Body: []byte("restored-inline")}
	header := assignmentPageHeader{Through: 7, Count: 2}
	encode, _, err := captureAssignmentReplica([]Delivery{cold, inline}, header, coverage)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encode()
	if err != nil {
		t.Fatal(err)
	}
	if !IsAssignmentReplica(wire) || len(wire) >= len(body) || bytes.Contains(wire, body) {
		t.Fatalf("large body still transmitted in %d-byte private page", len(wire))
	}
	if _, _, err := DecodeAssignmentPage(wire, request); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("legacy decoder accepted private page: %v", err)
	}
	page, err := DecodeAssignmentReplicaPage(wire, request, coverage)
	if err != nil {
		t.Fatal(err)
	}
	refs := page.References()
	if len(refs) != 1 || refs[0].Index != 92 || refs[0].Term != 8 || refs[0].Offset != 37 ||
		refs[0].Length != uint32(len(body)) || page.omittedBytes != len(body) ||
		len(page.deliveries) != 2 || !bytes.Equal(page.deliveries[1].Body, inline.Body) {
		t.Fatalf("wrong mixed page metadata: %#v", page)
	}
	out, through, err := page.Materialize(func(i int, dst []byte) error {
		if i != 0 || len(dst) != len(body) {
			t.Fatalf("unexpected destination %d %d", i, len(dst))
		}
		copy(dst, body)
		return nil
	})
	if err != nil || through != 7 || len(out) != 2 || out[0].MessageID != cold.MessageID ||
		out[0].Attempt != cold.Attempt || !out[0].Redelivered || !bytes.Equal(out[0].Body, body) || !bytes.Equal(out[1].Body, inline.Body) {
		t.Fatalf("wrong reconstructed assignment page: through=%v err=%v", through, err)
	}
	if _, err := DecodeAssignmentReplicaPage(wire, request, AssignmentCoverage{Term: 9, First: 90, Last: 95}); err == nil {
		t.Fatal("accepted term outside advertised coverage")
	}

	wrong := bytes.Repeat([]byte{0xab}, len(body))
	page.refs[0].CRC = crc32.Checksum(wrong, journalBodyChecksum)
	out, through, err = page.Materialize(func(_ int, dst []byte) error { copy(dst, wrong); return nil })
	if !errors.Is(err, ErrAssignmentBodyDigest) || len(out) != 0 || through != 0 {
		t.Fatalf("strong checksum failure exposed a partial result: through=%v err=%v", through, err)
	}

	fault := errors.New("journal read fault")
	out, through, err = page.Materialize(func(_ int, _ []byte) error { return fault })
	if !errors.Is(err, fault) || len(out) != 0 || through != 0 {
		t.Fatalf("journal read failure exposed a partial result: through=%v err=%v", through, err)
	}
}

func TestAssignmentReplicaUncoveredPageUsesExactLegacyCodec(t *testing.T) {
	request := AssignmentRequest{Queue: "queue", Incarnation: 10, Session: 12, Owner: "owner", Limit: 1, Bytes: 128 << 10}
	deliveries := []Delivery{{MessageID: 4, Attempt: 1, Body: bytes.Repeat([]byte{7}, 64<<10)}}
	header := assignmentPageHeader{Through: 1, Count: 1}
	base, size, err := captureInlineAssignmentPage(deliveries, header)
	if err != nil {
		t.Fatal(err)
	}
	mode, modeSize, err := captureAssignmentReplica(deliveries, header, AssignmentCoverage{Term: 8, First: 90, Last: 95})
	if err != nil {
		t.Fatal(err)
	}
	a, err := base()
	if err != nil {
		t.Fatal(err)
	}
	b, err := mode()
	if err != nil || size != modeSize || !bytes.Equal(a, b) || IsAssignmentReplica(b) {
		t.Fatalf("uncovered page changed legacy encoding: %v", err)
	}
	if _, _, err := DecodeAssignmentPage(b, request); err != nil {
		t.Fatal(err)
	}
}
