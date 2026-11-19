package server

import (
	"context"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	clusterDeadLetterRetry          = 200 * time.Millisecond
	clusterDeadLetterDeadline       = 2 * time.Second
	clusterDeadLetterTargetDeadline = 750 * time.Millisecond
	clusterDeadLetterWorkBatch      = 8
	clusterDeadLetterTargetAttempts = 4
)

type clusterLocalWorker interface {
	WorkShard(string) uint64
	LocalWorkShards(func(clusterraft.Machine) bool) []uint64
	ReadLocalWork(context.Context, uint64, func(clusterraft.Machine) (func() ([]byte, error), int, error)) ([]byte, error)
	SubmitLocalWork(context.Context, uint64, []byte) ([]byte, error)
}

func (b *ClusterBroker) clusterDeadLetterPolicy(p *broker.QueuePolicy, durable bool) (*clusterqueue.DeadLetterPolicy, error) {
	if p == nil {
		return nil, nil
	}
	if !p.ReliableDeadLetter || p.HasMessageTTL || p.HasQueueExpires || p.HasMaxLength || p.HasMaxLengthBytes {
		return nil, fmt.Errorf("%w: cluster mode supports only the at-least-once dead-letter queue policy", broker.ErrInvalidQueueArgument)
	}
	if _, ok := b.cluster.(clusterLocalWorker); !ok || !durable {
		return nil, fmt.Errorf("%w: clustered at-least-once dead-lettering requires a durable source queue and local Raft workers", broker.ErrInvalidQueueArgument)
	}
	if len(p.DeadLetterExchange) > 255 || len(p.DeadLetterRoutingKey) > 255 ||
		!utf8.ValidString(p.DeadLetterExchange) || !utf8.ValidString(p.DeadLetterRoutingKey) {
		return nil, fmt.Errorf("%w: dead-letter exchange and routing key must fit AMQP short strings", broker.ErrInvalidQueueArgument)
	}
	return &clusterqueue.DeadLetterPolicy{
		Exchange: p.DeadLetterExchange, RoutingKey: p.DeadLetterRoutingKey, HasRoutingKey: p.HasDeadLetterRoutingKey,
	}, nil
}

func hasClusterDeadLetters(machine clusterraft.Machine) bool {
	m, ok := machine.(*ClusterMachine)
	return ok && m.queues.HasDeadLetters()
}

func (b *ClusterBroker) runClusterDeadLetters(ctx context.Context) {
	worker, ok := b.cluster.(clusterLocalWorker)
	if !ok {
		return
	}
	cursors := make(map[uint64]deadLetterCursor)
	for ctx.Err() == nil {
		progress := false
		for _, shard := range worker.LocalWorkShards(hasClusterDeadLetters) {
			for range clusterDeadLetterWorkBatch {
				attempt, cancel := context.WithTimeout(ctx, clusterDeadLetterDeadline)
				previous := cursors[shard]
				cursor, advanced := b.advanceClusterDeadLetter(attempt, worker, shard, previous)
				cancel()
				cursors[shard] = cursor
				progress = progress || advanced
				if ctx.Err() != nil || !advanced && cursor.after == previous.after {
					break
				}
			}
		}
		if !progress {
			timer := time.NewTimer(clusterDeadLetterRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}

type deadLetterCursor struct {
	after   uint64
	through uint64
	round   uint64
}

func (b *ClusterBroker) advanceClusterDeadLetter(ctx context.Context, worker clusterLocalWorker, shard uint64, cursor deadLetterCursor) (deadLetterCursor, bool) {
	wire, err := worker.ReadLocalWork(ctx, shard, func(machine clusterraft.Machine) (func() ([]byte, error), int, error) {
		return machine.(*ClusterMachine).queues.CaptureDeadLetter(cursor.after, cursor.through)
	})
	if err != nil {
		return cursor, false
	}
	reply, err := clusterqueue.DecodeResponse(wire)
	if err != nil || reply.DeadLetter == nil {
		return cursor, false
	}
	entry := reply.DeadLetter
	if entry.ID <= cursor.after {
		cursor.round++
	}
	cursor.after, cursor.through = entry.ID, reply.DeadLetterThrough
	stored := reply.Result.Delivery
	message, err := decodeStoredClusterMessage(stored.Body, stored.Packed)
	if err != nil {
		return cursor, false
	}
	message = broker.MakeDeadLetter(&broker.QueuePolicy{
		DeadLetterExchange: entry.Policy.Exchange, DeadLetterRoutingKey: entry.Policy.RoutingKey,
		HasDeadLetterRoutingKey: entry.Policy.HasRoutingKey,
	}, entry.Source, message, broker.DeadLetterRejected, time.Unix(0, entry.Rejected))
	message.DeliveryMode = 2

	routed, err := b.deadLetterDestinations(ctx, message)
	if err != nil || len(routed) == 0 {
		return cursor, false
	}
	update := clusterqueue.DeadLetterUpdate{ID: entry.ID}
	if len(entry.Remaining) == 0 {
		for name := range routed {
			update.Plan = append(update.Plan, name)
		}
		slices.Sort(update.Plan)
	} else {
		header, err := encodeClusterMessageHeader(message)
		if err != nil {
			return cursor, false
		}
		record := b.publishRecord(header, message.Body)
		start := int(cursor.round % uint64(len(entry.Remaining)))
		attempts := 0
		for offset := range entry.Remaining {
			if ctx.Err() != nil || attempts == clusterDeadLetterTargetAttempts {
				break
			}
			name := entry.Remaining[(start+offset)%len(entry.Remaining)]
			target, ok := routed[name]
			if !ok {
				continue
			}
			attempts++
			// Let another destination make progress when this one's leader or
			// quorum is unavailable. Keep time for the source progress commit.
			attempt, cancel := context.WithTimeout(ctx, clusterDeadLetterTargetDeadline)
			committed, moved := b.publishClusterDeadLetter(attempt, worker, shard, entry.ID, name, target, record)
			cancel()
			if moved && committed {
				return cursor, true
			}
			if committed {
				update.Completed = name
				// Commit after each success so a later unavailable destination
				// cannot exhaust the deadline and discard this progress.
				break
			}
		}
		if update.Completed == "" {
			return cursor, false
		}
	}
	command, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.UpdateDeadLetter, DeadLetter: &update})
	if err != nil {
		return cursor, false
	}
	wire, err = worker.SubmitLocalWork(ctx, shard, command)
	if err != nil {
		return cursor, false
	}
	progress, err := clusterqueue.DecodeResponse(wire)
	return cursor, err == nil && progress.Err() == nil
}

func (b *ClusterBroker) deadLetterDestinations(ctx context.Context, message *protocol.Message) (map[string]clusterqueue.QueueID, error) {
	if message.Exchange == "" {
		if message.RoutingKey == "" {
			return nil, nil
		}
		// The destination commit checks its current queue and durability.
		return map[string]clusterqueue.QueueID{message.RoutingKey: 0}, nil
	}
	reply, err := b.exchange(ctx, message.Exchange, clusterExchangeCommand{Kind: exchangeDescribe})
	if err != nil {
		return nil, err
	}
	// Broker-generated delivery can route through an internal exchange.
	ex := reply.Exchange
	ex.Internal = false
	targets, err := routeClusterExchange(ex, message.RoutingKey, message.Headers)
	if err != nil {
		return nil, err
	}
	names := make(map[string]clusterqueue.QueueID, len(targets))
	for _, target := range targets {
		names[target.Queue] = max(names[target.Queue], target.Incarnation)
	}
	return names, nil
}

func (b *ClusterBroker) publishClusterDeadLetter(ctx context.Context, worker clusterLocalWorker, shard, id uint64, name string, incarnation clusterqueue.QueueID, record clusterqueue.PublishRecord) (committed, moved bool) {
	if ctx.Err() != nil {
		return false, false
	}
	var move uint64
	if worker.WorkShard(name) == shard {
		move = id
	}
	command, err := clusterqueue.EncodeDeadLetterPublish(name, incarnation, move, record)
	if err != nil {
		return false, false
	}
	if move == 0 {
		result, err := b.executeEncoded(ctx, name, command, false)
		return err == nil && result.Err() == nil, false
	}
	data, err := worker.SubmitLocalWork(ctx, shard, command)
	if err != nil {
		return false, true
	}
	result, err := clusterqueue.DecodeResponse(data)
	return err == nil && result.Err() == nil, true
}
