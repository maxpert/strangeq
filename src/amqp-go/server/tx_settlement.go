package server

import (
	"math"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// txBufferSettlement takes the wire tag(s) NOW (RabbitMQ removes from the
// unacked map at ack time, R14) and registers an undo that re-tracks them on
// rollback/failed commit (WS-D §6). Returns known=false if wireTag is not
// outstanding.
//
// A cumulative (multiple=true) call takes EVERY outstanding entry up to the
// wire tag (all consumers plus basic.get deliveries on the channel) and buffers
// ONE single-tag settlement op per entry — mirroring cumulativeSettle, and
// fixing the old "only the highest tag's owner settles" defect.
func (s *Server) txBufferSettlement(conn *protocol.Connection, channel *protocol.Channel, channelID uint16,
	kind interfaces.TransactionOperationType, wireTag uint64, multiple, requeue bool) (known bool, err error) {
	if channel == nil {
		return false, nil
	}

	// 1. Collect the entries (wire tags taken NOW).
	var tags []uint64
	var refs []protocol.WireDeliveryRef
	if multiple {
		if wireTag == 0 {
			wireTag = math.MaxUint64 // mirrors the non-tx cumulative path
		}
		tags, refs = channel.TakeWireTagEntriesUpTo(wireTag)
		// A cumulative op with zero entries and a real (non-MaxUint64) tag is unknown.
		if len(tags) == 0 && wireTag != math.MaxUint64 {
			return false, nil
		}
	} else {
		ref, ok := channel.TakeWireTag(wireTag)
		if !ok {
			return false, nil
		}
		tags, refs = []uint64{wireTag}, []protocol.WireDeliveryRef{ref}
	}

	// 2. Buffer one settlement op per entry, each with its restore undo.
	for i := range refs {
		r := refs[i]
		w := tags[i]
		op := &interfaces.TransactionOperation{
			Type:        kind,
			ConsumerTag: r.ConsumerID, // "" routes to the *GetDelivery executor methods
			DeliveryTag: r.MsgID,
			Multiple:    false,
			Requeue:     requeue,
		}
		undo := func() { channel.TrackDelivery(w, r.MsgID, r.ConsumerID, r.IsGet) }
		if u, ok := s.TransactionManager.(interface {
			AddOperationWithUndo(uint16, *interfaces.TransactionOperation, func()) error
		}); ok {
			if aerr := u.AddOperationWithUndo(channelID, op, undo); aerr != nil {
				return true, aerr
			}
		} else if aerr := s.TransactionManager.AddOperation(channelID, op); aerr != nil {
			undo() // best-effort restore when the manager cannot carry undos
			return true, aerr
		}
	}
	return true, nil
}
