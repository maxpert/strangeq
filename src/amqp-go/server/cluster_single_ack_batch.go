package server

import (
	"slices"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	clusterAckGatherDelay = 125 * time.Microsecond
	clusterAckGatherLimit = 8
)

// The frame processor owns this list. Tags retain the client's exact frame
// order, including any gaps between acknowledged deliveries.
type clusterSingleAckBatch struct {
	broker      *ClusterBroker
	channelID   uint16
	tags        [clusterqueue.MaxSettlementBatch]uint64
	count       int
	allowGather bool
	gatherTimer *time.Timer
	gatherReady <-chan time.Time
	gatherUntil time.Time
}

func singleAckFrame(frame *protocol.Frame) (uint64, bool) {
	class, method, ok := methodIDs(frame)
	if !ok || frame.Channel == 0 || class != 60 || method != protocol.BasicAck {
		return 0, false
	}
	tag, multiple, err := parseBasicAckArgs(frame.Payload[4:])
	return tag, err == nil && !multiple
}

func (a *clusterSingleAckBatch) add(channelID uint16, tag uint64) {
	a.channelID = channelID
	a.tags[a.count] = tag
	a.count++
}

func (a *clusterSingleAckBatch) gatherSignal() <-chan time.Time {
	if a == nil {
		return nil
	}
	return a.gatherReady
}

func (a *clusterSingleAckBatch) gatherExpired() bool {
	return a.gatherReady != nil && !time.Now().Before(a.gatherUntil)
}

// Wait once at a transient idle point for another small wave of exact ACKs.
// Larger runs already amortize the durable mutation and flush at idle.
func (a *clusterSingleAckBatch) waitAtIdle() bool {
	if !a.allowGather || a.count >= clusterAckGatherLimit {
		return false
	}
	if a.gatherReady != nil {
		return !a.gatherExpired()
	}
	a.gatherUntil = time.Now().Add(clusterAckGatherDelay)
	wait := time.Until(a.gatherUntil)
	if a.gatherTimer == nil {
		a.gatherTimer = time.NewTimer(wait)
	} else {
		a.gatherTimer.Reset(wait)
	}
	a.gatherReady = a.gatherTimer.C
	return true
}

func (a *clusterSingleAckBatch) stopGather() {
	if a.gatherReady != nil {
		a.gatherTimer.Stop()
		a.gatherReady = nil
	}
}

func (s *Server) flushClusterSingleAcks(rt *connRuntime, batch *clusterSingleAckBatch) bool {
	if batch == nil || batch.count == 0 {
		return false
	}
	batch.stopGather()
	tags := batch.tags[:batch.count]
	batch.count = 0
	if rt.connClosing.Load() || rt.conn.Closed.Load() || rt.isChannelClosing(batch.channelID) {
		return false
	}
	if err := s.settleClusterSingleAcks(rt.conn, batch.broker, batch.channelID, tags); err != nil {
		return s.handleFrameError(rt, frameCtx{channel: batch.channelID, classID: 60, methodID: protocol.BasicAck}, err, true)
	}
	return false
}

func (s *Server) settleClusterSingleAcks(conn *protocol.Connection, broker *ClusterBroker, channelID uint16, tags []uint64) error {
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

	settled := 0
	for settled < known {
		ctx, cancel := clusterContext()
		count, err := broker.settleWireRun(ctx, clusterqueue.AckMessage, refs[settled:known])
		cancel()
		for _, tag := range tags[settled : settled+count] {
			channel.TakeWireTag(tag)
		}
		settled += count
		if s.MetricsCollector != nil && count != 0 {
			s.MetricsCollector.RecordMessagesAcknowledged(count)
		}
		if err != nil {
			// The ordinary single-ACK path consumes the failed known tag
			// before calling the broker. Later tags remain outstanding.
			channel.TakeWireTag(tags[settled])
			return err
		}
	}
	if known < len(tags) {
		return s.unknownDeliveryTag(conn, channelID, tags[known], protocol.BasicAck)
	}
	return nil
}
