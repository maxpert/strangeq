package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	clusterRPCDeadline    = 4 * time.Second
	clusterLease          = 30 * time.Second
	clusterDeliveryCredit = 2000
)

type ClusterSubmitter interface {
	Submit(context.Context, string, []byte) ([]byte, error)
}

type clusterShardReader interface {
	SubmitEachShard(context.Context, []byte) ([][]byte, error)
}

// ClusterBroker adapts AMQP to the replicated work-queue state machine.
type ClusterBroker struct {
	cluster         ClusterSubmitter
	mu              sync.Mutex
	consumers       map[string]*clusterConsumer
	gets            map[clusterGetKey]*clusterSession
	deliveries      map[uint64]clusterDelivery
	nextTag         atomic.Uint64
	cancelConsumers func([]string)
}

func NewClusterBroker(cluster ClusterSubmitter) *ClusterBroker {
	return &ClusterBroker{
		cluster: cluster, consumers: make(map[string]*clusterConsumer),
		gets: make(map[clusterGetKey]*clusterSession), deliveries: make(map[uint64]clusterDelivery),
	}
}

var _ UnifiedBroker = (*ClusterBroker)(nil)
var _ NonBlockingPublisher = (*ClusterBroker)(nil)

func clusterContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), clusterRPCDeadline)
}

func (b *ClusterBroker) execute(ctx context.Context, command clusterqueue.QueueCommand, repeatable bool) (clusterqueue.QueueResponse, error) {
	encoded, err := clusterqueue.EncodeCommand(command)
	if err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	var data []byte
	if repeatable {
		data, err = b.submitReplay(ctx, command.Queue, encoded)
	} else {
		data, err = b.cluster.Submit(ctx, command.Queue, encoded)
	}
	if err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	return clusterqueue.DecodeResponse(data)
}

func (b *ClusterBroker) submitReplay(ctx context.Context, key string, data []byte) ([]byte, error) {
	for delay := 15 * time.Millisecond; ; delay = min(2*delay, 250*time.Millisecond) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reply, err := b.cluster.Submit(ctx, key, data)
		if err == nil {
			return reply, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(err, ctx.Err())
		}
	}
}

func (b *ClusterBroker) queue(ctx context.Context, command clusterqueue.QueueCommand) (clusterqueue.QueueResponse, error) {
	result, err := b.execute(ctx, command, false)
	if err != nil {
		return result, err
	}
	return result, result.Err()
}

func (b *ClusterBroker) repeatableQueue(ctx context.Context, command clusterqueue.QueueCommand) (clusterqueue.QueueResponse, error) {
	result, err := b.execute(ctx, command, true)
	if err != nil {
		return result, err
	}
	return result, result.Err()
}

func clusterQueueError(name string, err error) error {
	switch {
	case errors.Is(err, clusterqueue.ErrQueueNotFound), errors.Is(err, clusterqueue.ErrQueueIncarnation):
		return broker.QueueNotFoundError(name)
	case errors.Is(err, clusterqueue.ErrQueueMismatch):
		return fmt.Errorf("%w: %q", broker.ErrInequivalentArgs, name)
	case errors.Is(err, clusterqueue.ErrQueueInUse):
		return fmt.Errorf("%w: %q", broker.ErrQueueInUse, name)
	case errors.Is(err, clusterqueue.ErrQueueNotEmpty):
		return fmt.Errorf("%w: %q", broker.ErrQueueNotEmpty, name)
	case errors.Is(err, clusterqueue.ErrInvalidQueueName):
		return fmt.Errorf("%w: %w", broker.ErrInvalidQueueArgument, err)
	default:
		return err
	}
}

func (b *ClusterBroker) describe(ctx context.Context, name string) (clusterqueue.QueueInfo, error) {
	result, err := b.repeatableQueue(ctx, clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: name})
	return result.Queue, err
}

func clusterCount(n uint64) uint32 { return uint32(min(n, math.MaxUint32)) }

func queueFromClusterInfo(info clusterqueue.QueueInfo) (*protocol.Queue, error) {
	spec := info.QueueSpec
	args, err := decodeClusterArguments(spec.Arguments)
	if err != nil {
		return nil, err
	}
	q := protocol.NewQueue(info.Name, spec.Durable, spec.AutoDelete, spec.Exclusive, args)
	q.Ordinal = uint64(info.Incarnation)
	q.MessageCount.Store(info.Ready)
	return q, nil
}

func decodeClusterArguments(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return protocol.DecodeFieldTable(data)
}

func (b *ClusterBroker) DeclareQueue(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) (*protocol.Queue, error) {
	queue, _, _, err := b.DeclareQueueWithCounts(name, durable, autoDelete, exclusive, args)
	return queue, err
}

func (b *ClusterBroker) DeclareQueueWithCounts(name string, durable, autoDelete, exclusive bool, args map[string]interface{}) (*protocol.Queue, uint32, uint32, error) {
	if autoDelete || exclusive {
		return nil, 0, 0, fmt.Errorf("%w: exclusive and auto-delete queues are unavailable in cluster mode", broker.ErrInvalidQueueArgument)
	}
	policy, err := broker.ResolveQueuePolicy(args)
	if err != nil {
		return nil, 0, 0, err
	}
	if policy != nil {
		return nil, 0, 0, fmt.Errorf("%w: queue policies are unavailable in cluster mode", broker.ErrInvalidQueueArgument)
	}
	encoded, err := protocol.EncodeCanonicalFieldTable(args)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("%w: %w", broker.ErrInvalidQueueArgument, err)
	}
	ctx, cancel := clusterContext()
	defer cancel()
	reply, err := b.queue(ctx, clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: name, QueueSpec: clusterqueue.QueueSpec{Durable: durable, Arguments: encoded}})
	if err != nil {
		return nil, 0, 0, clusterQueueError(name, err)
	}
	queue, err := queueFromClusterInfo(reply.Queue)
	return queue, clusterCount(reply.Queue.Ready), clusterCount(reply.Queue.Consumers), err
}

func (b *ClusterBroker) QueueDeclarePassiveWithCounts(name string) (*protocol.Queue, uint32, uint32, error) {
	if name == "" {
		return nil, 0, 0, broker.QueueNotFoundError(name)
	}
	ctx, cancel := clusterContext()
	defer cancel()
	info, err := b.describe(ctx, name)
	if err != nil {
		return nil, 0, 0, clusterQueueError(name, err)
	}
	queue, err := queueFromClusterInfo(info)
	return queue, clusterCount(info.Ready), clusterCount(info.Consumers), err
}

func (b *ClusterBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error) {
	ctx, cancel := clusterContext()
	defer cancel()
	info, err := b.describe(ctx, name)
	if errors.Is(err, clusterqueue.ErrQueueNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	reply, err := b.queue(ctx, clusterqueue.QueueCommand{
		Kind: clusterqueue.DeleteQueue, Queue: name, Incarnation: info.Incarnation, IfEmpty: ifEmpty, IfUnused: ifUnused,
	})
	return int(clusterCount(reply.Count)), clusterQueueError(name, err)
}

func (b *ClusterBroker) PurgeQueue(name string) (int, error) {
	ctx, cancel := clusterContext()
	defer cancel()
	info, err := b.describe(ctx, name)
	if err != nil {
		return 0, clusterQueueError(name, err)
	}
	result, err := b.queue(ctx, clusterqueue.QueueCommand{Kind: clusterqueue.PurgeQueue, Queue: name, Incarnation: info.Incarnation})
	return int(clusterCount(result.Count)), clusterQueueError(name, err)
}

func (b *ClusterBroker) PublishMessage(exchange, key string, message *protocol.Message) error {
	if message.Expiration != "" {
		return errors.New("per-message expiration is unavailable in cluster mode")
	}
	ctx, cancel := clusterContext()
	defer cancel()
	targets := []clusterTarget{{Queue: key}}
	if exchange != "" {
		var err error
		targets, err = b.route(ctx, exchange, key, message.Headers)
		if err != nil {
			return err
		}
	}
	if len(targets) == 0 || (exchange == "" && key == "") {
		if message.Mandatory {
			return broker.ErrNoRoute
		}
		return nil
	}

	body, err := encodeClusterMessage(message)
	if err != nil {
		return err
	}
	var stale []clusterTarget
	published := 0
	for _, target := range targets {
		_, err := b.queue(ctx, clusterqueue.QueueCommand{
			Kind: clusterqueue.PublishMessage, Queue: target.Queue, Body: body, Incarnation: target.Incarnation,
		})
		switch {
		case errors.Is(err, clusterqueue.ErrQueueNotFound), errors.Is(err, clusterqueue.ErrQueueIncarnation):
			stale = append(stale, target)
		case errors.Is(err, clusterqueue.ErrCapacity):
			return broker.ErrMaxLengthExceeded
		case err != nil:
			return err
		default:
			published++
		}
	}
	if exchange != "" && len(stale) > 0 {
		// Remove exact stale incarnations so a concurrent redeclaration is safe.
		_, _ = b.exchange(ctx, exchange, clusterExchangeCommand{Kind: exchangePrune, Prune: stale})
	}
	if published == 0 && message.Mandatory {
		return broker.ErrNoRoute
	}
	return nil
}

func (b *ClusterBroker) PublishMessageAdmitted(exchange, key string, message *protocol.Message) (protocol.DepthGate, error) {
	return nil, b.PublishMessage(exchange, key, message)
}

func (b *ClusterBroker) PublishMessageAsyncConfirm(exchange, key string, message *protocol.Message, _ func(error), lease protocol.PublishLeaseToken) (bool, protocol.DepthGate, error) {
	defer lease.BrokerDone()
	// The server confirms synchronously after a successful committed proposal.
	return false, nil, b.PublishMessage(exchange, key, message)
}

func (*ClusterBroker) RecoverQueue(string, uint64, uint64, uint64) {}
func (*ClusterBroker) RebuildDeliveryIndex(uint64, string)         {}

func (b *ClusterBroker) GetQueues() map[string]*protocol.Queue {
	result := make(map[string]*protocol.Queue)
	query, ok := b.cluster.(clusterShardReader)
	if !ok {
		return result
	}
	ctx, cancel := clusterContext()
	defer cancel()
	command, _ := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.ListQueues})
	replies, err := query.SubmitEachShard(ctx, command)
	if err != nil {
		return result
	}
	for _, data := range replies {
		reply, err := clusterqueue.DecodeResponse(data)
		if err != nil || reply.Err() != nil {
			continue
		}
		for _, info := range reply.Queues {
			q, err := queueFromClusterInfo(info)
			if err == nil {
				result[q.Name] = q
			}
		}
	}
	return result
}
