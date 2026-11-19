package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

type clusterPublishItem struct {
	message *protocol.Message
	header  []byte
	lease   protocol.PublishLeaseToken
	channel *protocol.Channel
	tag     uint64
	started time.Time
}

// The connection's frame processor owns the burst through synchronous apply
// and any wire outcome. A staged item retains its original publish lease.
type clusterPublishBurst struct {
	server      *Server
	broker      *ClusterBroker
	conn        *protocol.Connection
	channelID   uint16
	wireBytes   int
	items       []clusterPublishItem
	allowGather bool
	gatherTimer *time.Timer
	gatherReady <-chan time.Time
	gatherUntil time.Time
}

const clusterPublishGatherDelay = 350 * time.Microsecond

func (p *clusterPublishBurst) gatherSignal() <-chan time.Time {
	if p == nil {
		return nil
	}
	return p.gatherReady
}

// waitAtIdle lets a second wave of small, compatible frames arrive without
// splitting the confirm window into two durable submissions. A burst can
// wait only once. Existing record, byte and protocol barriers still apply.
func (p *clusterPublishBurst) waitAtIdle() bool {
	if !p.allowGather || len(p.items) < 16 || len(p.items) == clusterqueue.MaxPublishBatchMessages ||
		p.wireBytes > 128<<10 {
		return false
	}
	if !p.gatherUntil.IsZero() {
		return p.gatherReady != nil && time.Now().Before(p.gatherUntil)
	}
	p.gatherUntil = time.Now().Add(clusterPublishGatherDelay)
	if p.gatherTimer == nil {
		p.gatherTimer = time.NewTimer(time.Until(p.gatherUntil))
	} else {
		p.gatherTimer.Reset(time.Until(p.gatherUntil))
	}
	p.gatherReady = p.gatherTimer.C
	return true
}

func (p *clusterPublishBurst) continues(frame *protocol.Frame) bool {
	if len(p.items) == 0 {
		return true
	}
	if frame.Channel != p.channelID {
		return false
	}
	switch frame.Type {
	case protocol.FrameHeader, protocol.FrameBody:
		return true
	case protocol.FrameMethod:
		class, method, ok := methodIDs(frame)
		return ok && class == 60 && method == protocol.BasicPublish
	default:
		return false
	}
}

func (p *clusterPublishBurst) prepare(channelID uint16, message *protocol.Message) ([]byte, error) {
	var header []byte
	var recordBytes int
	remaining := clusterqueue.MaxPublishBatchBytes - 4 - len(message.Body)
	if message.Expiration == "" && remaining > 0 {
		envelope, err := encodeClusterMessageHeader(message)
		if err == nil && len(envelope) <= remaining {
			header = envelope
			recordBytes = 4 + len(header) + len(message.Body)
		}
	}
	if len(p.items) > 0 {
		first := p.items[0].message
		if header == nil || p.channelID != channelID || first.Exchange != message.Exchange || first.RoutingKey != message.RoutingKey ||
			len(p.items) == clusterqueue.MaxPublishBatchMessages || p.wireBytes+recordBytes > clusterqueue.MaxPublishBatchBytes {
			if err := p.flush(); err != nil {
				return nil, err
			}
		}
	}
	return header, nil
}

func (p *clusterPublishBurst) add(channelID uint16, item clusterPublishItem) {
	p.channelID = channelID
	p.wireBytes += 4 + len(item.header) + len(item.message.Body)
	p.items = append(p.items, item)
}

func (p *clusterPublishBurst) discard() {
	if p.gatherReady != nil {
		p.gatherTimer.Stop()
		p.gatherReady = nil
	}
	p.gatherUntil = time.Time{}
	for _, item := range p.items {
		item.lease.ServerFinish()
	}
	clear(p.items)
	p.items = p.items[:0]
	p.wireBytes = 0
}

func (s *Server) flushClusterPublishes(rt *connRuntime, p *clusterPublishBurst) bool {
	if p == nil || len(p.items) == 0 {
		return false
	}
	channelID := p.channelID
	if err := p.flush(); err != nil {
		return s.handleFrameError(rt, frameCtx{channel: channelID, classID: 60, methodID: protocol.BasicPublish}, err, true)
	}
	return false
}

func (p *clusterPublishBurst) finish(index int, outcome error) error {
	item := p.items[index]
	return p.server.finishPublish(p.conn, p.channelID, item.message, item.channel, item.tag, item.started, false, outcome)
}

func (p *clusterPublishBurst) flushSingles(start int) error {
	for i := start; i < len(p.items); i++ {
		item := p.items[i]
		outcome := p.broker.publishMessage(item.message.Exchange, item.message.RoutingKey, item.message, item.header)
		if err := p.finish(i, outcome); err != nil {
			return err
		}
	}
	return nil
}

func (p *clusterPublishBurst) finishCommitted(count int) error {
	var channel *protocol.Channel
	var frontier uint64
	for _, item := range p.items[:count] {
		if err := p.server.finishPublish(p.conn, p.channelID, item.message, item.channel, item.tag, item.started, true, nil); err != nil {
			return err
		}
		if item.tag != 0 {
			channel = item.channel
			frontier = channel.MarkConfirmTagDurable(item.tag)
		}
	}
	if channel == nil {
		return nil
	}
	channel.AdvanceConfirmDurable(frontier)
	if len(p.conn.FrameQueue) == 0 {
		return p.server.flushChannelConfirms(p.conn, channel)
	}
	p.conn.WakeConfirmFlusher()
	return nil
}

func (p *clusterPublishBurst) finishUnroutable() error {
	for i, item := range p.items {
		var outcome error
		if item.message.Mandatory {
			outcome = broker.ErrNoRoute
		}
		if err := p.finish(i, outcome); err != nil {
			return err
		}
	}
	return nil
}

func (p *clusterPublishBurst) flush() error {
	if p == nil || len(p.items) == 0 {
		return nil
	}
	defer p.discard()
	if p.conn.Closed.Load() {
		return nil
	}
	if len(p.items) == 1 {
		return p.flushSingles(0)
	}

	ctx, cancel := clusterContext()
	defer cancel()
	first := p.items[0].message
	target := clusterTarget{Queue: first.RoutingKey}
	if first.Exchange != "" {
		reply, err := p.broker.exchange(ctx, first.Exchange, clusterExchangeCommand{Kind: exchangeDescribe})
		if err != nil {
			return p.finish(0, err)
		}
		if reply.Exchange.Type == "headers" && !reply.Exchange.Internal {
			return p.flushSingles(0)
		}
		targets, err := routeClusterExchange(reply.Exchange, first.RoutingKey, first.Headers)
		if err != nil {
			return p.finish(0, err)
		}
		switch len(targets) {
		case 0:
			return p.finishUnroutable()
		case 1:
			target = targets[0]
		default:
			return p.flushSingles(0)
		}
	} else if target.Queue == "" {
		return p.finishUnroutable()
	}

	var records [clusterqueue.MaxPublishBatchMessages]clusterqueue.PublishRecord
	for i, item := range p.items {
		records[i] = p.broker.publishRecord(item.header, item.message.Body)
	}
	command, err := clusterqueue.EncodePublishBatchRecords(target.Queue, target.Incarnation, records[:len(p.items)])
	if err != nil {
		return p.finish(0, err)
	}
	wire, err := p.broker.cluster.Submit(ctx, target.Queue, command)
	if err != nil {
		return p.finish(0, err)
	}
	result, err := clusterqueue.DecodeResponse(wire)
	if err != nil {
		return p.finish(0, err)
	}
	outcome := result.Err()
	switch {
	case outcome == nil && result.Count == uint64(len(p.items)):
	case (errors.Is(outcome, clusterqueue.ErrCapacity) || errors.Is(outcome, clusterqueue.ErrIDsExhausted)) && result.Count < uint64(len(p.items)):
	case outcome != nil && result.Count == 0:
	default:
		return p.finish(0, fmt.Errorf("invalid cluster publish batch outcome (count %d)", result.Count))
	}
	published := int(result.Count)
	if err := p.finishCommitted(published); err != nil {
		return err
	}
	switch {
	case outcome == nil:
		return nil
	case errors.Is(outcome, clusterqueue.ErrQueueNotFound), errors.Is(outcome, clusterqueue.ErrQueueIncarnation):
		if first.Exchange != "" {
			_, _ = p.broker.exchange(ctx, first.Exchange, clusterExchangeCommand{Kind: exchangePrune, Prune: []clusterTarget{target}})
		}
		return p.finishUnroutable()
	case errors.Is(outcome, clusterqueue.ErrCapacity):
		if err := p.finish(published, broker.ErrMaxLengthExceeded); err != nil {
			return err
		}
		// The failed record ended the committed prefix. Preserve the legacy
		// opportunity for a smaller, unattempted message to fit.
		return p.flushSingles(published + 1)
	default:
		return p.finish(published, outcome)
	}
}
