package server

import (
	"slices"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	clusterSettlementGatherDelay = 125 * time.Microsecond
	clusterSettlementGatherLimit = 8
)

// The frame processor owns this list. Tags retain the client's exact frame
// order, including gaps between settled deliveries. A run uses one wire method.
type clusterSingleSettlementBatch struct {
	broker      *ClusterBroker
	channelID   uint16
	methodID    uint16
	tags        [clusterqueue.MaxSettlementBatch]uint64
	count       int
	allowGather bool
	gatherTimer *time.Timer
	gatherReady <-chan time.Time
	gatherUntil time.Time
}

func singleSettlementFrame(frame *protocol.Frame) (uint64, uint16) {
	class, method, ok := methodIDs(frame)
	if !ok || frame.Channel == 0 || class != 60 {
		return 0, 0
	}
	args := frame.Payload[4:]
	switch method {
	case protocol.BasicAck:
		tag, multiple, err := parseBasicAckArgs(args)
		if err == nil && !multiple {
			return tag, method
		}
	case protocol.BasicReject:
		tag, requeue, err := parseBasicRejectArgs(args)
		if err == nil && !requeue {
			return tag, method
		}
	case protocol.BasicNack:
		tag, multiple, requeue, err := parseBasicNackArgs(args)
		if err == nil && !multiple && !requeue {
			return tag, method
		}
	}
	return 0, 0
}

func (a *clusterSingleSettlementBatch) add(channelID, methodID uint16, tag uint64) {
	a.channelID = channelID
	a.methodID = methodID
	a.tags[a.count] = tag
	a.count++
}

func (a *clusterSingleSettlementBatch) gatherSignal() <-chan time.Time {
	if a == nil {
		return nil
	}
	return a.gatherReady
}

func (a *clusterSingleSettlementBatch) gatherExpired() bool {
	return a.gatherReady != nil && !time.Now().Before(a.gatherUntil)
}

// Wait once at a transient idle point for another small wave of exact settlements.
// Larger runs already amortize the durable mutation and flush at idle.
func (a *clusterSingleSettlementBatch) waitAtIdle() bool {
	if !a.allowGather || a.count >= clusterSettlementGatherLimit {
		return false
	}
	if a.gatherReady != nil {
		return !a.gatherExpired()
	}
	a.gatherUntil = time.Now().Add(clusterSettlementGatherDelay)
	wait := time.Until(a.gatherUntil)
	if a.gatherTimer == nil {
		a.gatherTimer = time.NewTimer(wait)
	} else {
		a.gatherTimer.Reset(wait)
	}
	a.gatherReady = a.gatherTimer.C
	return true
}

func (a *clusterSingleSettlementBatch) stopGather() {
	if a.gatherReady != nil {
		a.gatherTimer.Stop()
		a.gatherReady = nil
	}
}

func (s *Server) flushClusterSingleSettlements(rt *connRuntime, batch *clusterSingleSettlementBatch) bool {
	if batch == nil || batch.count == 0 {
		return false
	}
	batch.stopGather()
	tags := batch.tags[:batch.count]
	batch.count = 0
	if rt.connClosing.Load() || rt.conn.Closed.Load() || rt.isChannelClosing(batch.channelID) {
		return false
	}
	if err := s.settleClusterSingleFrames(rt.conn, batch.broker, batch.channelID, batch.methodID, tags); err != nil {
		return s.handleFrameError(rt, frameCtx{channel: batch.channelID, classID: 60, methodID: batch.methodID}, err, true)
	}
	return false
}

func (s *Server) settleClusterSingleFrames(conn *protocol.Connection, broker *ClusterBroker, channelID, methodID uint16, tags []uint64) error {
	channel := s.wireChannel(conn, channelID)
	if channel == nil {
		return nil
	}
	var refs [clusterqueue.MaxSettlementBatch]protocol.WireDeliveryRef
	known := 0
	for i, tag := range tags {
		ref, ok := channel.ResolveWireTag(tag)
		if !ok || slices.Contains(tags[:i], tag) {
			break
		}
		refs[i] = ref
		known++
	}

	kind := clusterqueue.AckMessage
	if methodID != protocol.BasicAck {
		kind = clusterqueue.DropMessage
	}
	settled := 0
	for settled < known {
		ctx, cancel := clusterContext()
		count, err := broker.settleWireRun(ctx, kind, refs[settled:known])
		cancel()
		for _, tag := range tags[settled : settled+count] {
			channel.TakeWireTag(tag)
		}
		settled += count
		if s.MetricsCollector != nil && count != 0 {
			if kind == clusterqueue.AckMessage {
				s.MetricsCollector.RecordMessagesAcknowledged(count)
			} else {
				s.MetricsCollector.RecordMessagesRejected(count)
			}
		}
		if err != nil {
			// The ordinary individual settlement path consumes the failed known tag
			// before calling the broker. Later tags remain outstanding.
			channel.TakeWireTag(tags[settled])
			return err
		}
	}
	if known < len(tags) {
		return s.unknownDeliveryTag(conn, channelID, tags[known], methodID)
	}
	return nil
}
