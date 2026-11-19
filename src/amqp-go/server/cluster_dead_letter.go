package server

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
	"golang.org/x/sync/semaphore"
)

const (
	clusterDeadLetterScan           = 75 * time.Millisecond
	clusterDeadLetterRetry          = 100 * time.Millisecond
	clusterDeadLetterDeadline       = 3 * time.Second
	clusterDeadLetterRouteDeadline  = 400 * time.Millisecond
	clusterDeadLetterLookupDeadline = 125 * time.Millisecond
	clusterDeadLetterTargetDeadline = 750 * time.Millisecond
	clusterDeadLetterSourceDeadline = 500 * time.Millisecond
	clusterDeadLetterActivePages    = 8
	clusterDeadLetterPayloadBudget  = 32 << 20
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
	var workers sync.WaitGroup
	var mu sync.Mutex
	active := make(map[uint64]struct{})
	pages := make(chan struct{}, clusterDeadLetterActivePages)
	payload := semaphore.NewWeighted(clusterDeadLetterPayloadBudget)
	ticker := time.NewTicker(clusterDeadLetterScan)
	defer ticker.Stop()
	for ctx.Err() == nil {
		for _, shard := range worker.LocalWorkShards(hasClusterDeadLetters) {
			mu.Lock()
			_, running := active[shard]
			if !running {
				active[shard] = struct{}{}
			}
			mu.Unlock()
			if running {
				continue
			}
			workers.Go(func() {
				defer func() {
					mu.Lock()
					delete(active, shard)
					mu.Unlock()
				}()
				b.runClusterDeadLetterShard(ctx, worker, shard, pages, payload)
			})
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	workers.Wait()
}

func (b *ClusterBroker) runClusterDeadLetterShard(ctx context.Context, worker clusterLocalWorker, shard uint64,
	pages chan struct{}, payload *semaphore.Weighted) {
	var cursor deadLetterCursor
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case pages <- struct{}{}:
		}
		// A normal page reserves two MiB of logical payload before
		// capture. An oversized singleton joins the same byte budget
		// with its actual size, without waiting under a Raft lock.
		reserved := max(clusterqueue.DeadLetterPageBytes, cursor.pageBytes)
		if err := payload.Acquire(ctx, int64(reserved)); err != nil {
			<-pages
			return
		}
		attempt, cancel := context.WithTimeout(ctx, clusterDeadLetterDeadline)
		cursor.pageBytes = reserved
		next, progressed := b.advanceClusterDeadLetter(attempt, worker, shard, cursor)
		cancel()
		payload.Release(int64(reserved))
		<-pages
		if next.pageBytes > reserved {
			cursor = next
			continue
		}
		if next.after == 0 && len(next.receipts) == 0 {
			return
		}
		cursor = next
		if !progressed {
			timer := time.NewTimer(clusterDeadLetterRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

type deadLetterCursor struct {
	after     uint64
	through   uint64
	round     uint64
	receipts  []clusterqueue.DeadLetterUpdate
	pageBytes int
}

type deadLetterCandidate struct {
	id        uint64
	planIndex int
	target    clusterTarget
	record    clusterqueue.PublishRecord
}

type deadLetterDestinationBatch struct {
	target    clusterTarget
	moves     []clusterqueue.DeadLetterPublishRecord
	ids       []uint64
	wireBytes int
}

type deadLetterPageBudgetError struct{ bytes int }

func (e *deadLetterPageBudgetError) Error() string {
	return "dead-letter page needs a larger payload reservation"
}

func (b *ClusterBroker) advanceClusterDeadLetter(ctx context.Context, worker clusterLocalWorker, shard uint64, cursor deadLetterCursor) (deadLetterCursor, bool) {
	wire, err := worker.ReadLocalWork(ctx, shard, func(machine clusterraft.Machine) (func() ([]byte, error), int, error) {
		encode, wireBytes, logicalBytes := machine.(*ClusterMachine).queues.CaptureDeadLetterPage(cursor.after, cursor.through)
		if limit := max(clusterqueue.DeadLetterPageBytes, cursor.pageBytes); logicalBytes > limit {
			return nil, 0, &deadLetterPageBudgetError{bytes: logicalBytes}
		}
		return encode, wireBytes, nil
	})
	cursor.pageBytes = 0
	if need, ok := errors.AsType[*deadLetterPageBudgetError](err); ok {
		cursor.pageBytes = need.bytes
		return cursor, false
	}
	if errors.Is(err, clusterraft.ErrNotLeader) || errors.Is(err, clusterraft.ErrLeadershipLost) || errors.Is(err, clusterraft.ErrStopped) {
		return deadLetterCursor{}, false
	}
	var page clusterqueue.DeadLetterPage
	if err == nil {
		page, err = clusterqueue.DecodeDeadLetterPage(wire)
	}
	if err != nil || len(page.Entries) == 0 {
		// A short or finished sweep still owes the source any known
		// destination commits. If the reply is uncertain, keep the receipts
		// locally while this shard worker can still retry them.
		if len(cursor.receipts) == 0 {
			return deadLetterCursor{}, false
		}
		applied := submitClusterDeadLetterUpdates(ctx, worker, shard, cursor.receipts)
		cursor.receipts = unappliedDeadLetterReceipts(cursor.receipts, applied)
		if len(cursor.receipts) == 0 {
			return deadLetterCursor{}, applied != 0
		}
		return cursor, applied != 0
	}
	if page.Entries[0].ID <= cursor.after {
		cursor.round++
	}
	cursor.after, cursor.through = page.Entries[len(page.Entries)-1].ID, page.Through

	// Each page sees a single quorum-fenced binding view per exchange. A
	// later page can use a rebound incarnation of a frozen destination.
	exchanges := make(map[string]*clusterExchange)
	updates := make([]clusterqueue.DeadLetterUpdate, len(cursor.receipts), len(cursor.receipts)+len(page.Entries))
	copy(updates, cursor.receipts)
	var candidates []deadLetterCandidate
	routeCtx, stopRoutes := context.WithTimeout(ctx, clusterDeadLetterRouteDeadline)
	startEntry := int(cursor.round % uint64(len(page.Entries)))
	for offset := range page.Entries {
		if ctx.Err() != nil {
			break
		}
		entry := page.Entries[(startEntry+offset)%len(page.Entries)]
		if slices.ContainsFunc(cursor.receipts, func(receipt clusterqueue.DeadLetterUpdate) bool {
			return receipt.ID == entry.ID
		}) {
			continue
		}
		message, err := decodeStoredClusterMessage(entry.Body, entry.Packed)
		if err != nil {
			continue
		}
		message = broker.MakeDeadLetter(&broker.QueuePolicy{
			DeadLetterExchange: entry.Policy.Exchange, DeadLetterRoutingKey: entry.Policy.RoutingKey,
			HasDeadLetterRoutingKey: entry.Policy.HasRoutingKey,
		}, entry.Source, message, broker.DeadLetterRejected, time.Unix(0, entry.Rejected))
		message.DeliveryMode = 2
		targets := b.deadLetterDestinations(routeCtx, exchanges, message)
		if len(targets) == 0 {
			continue
		}
		remaining := entry.Remaining
		planIndex := -1
		if len(remaining) == 0 {
			remaining = make([]string, 0, len(targets))
			for name := range targets {
				remaining = append(remaining, name)
			}
			slices.Sort(remaining)
			planIndex = len(updates)
			updates = append(updates, clusterqueue.DeadLetterUpdate{ID: entry.ID, Plan: remaining})
		}
		start := int(cursor.round % uint64(len(remaining)))
		for offset := range remaining {
			name := remaining[(start+offset)%len(remaining)]
			if target, ok := targets[name]; ok {
				header, err := encodeClusterMessageHeader(message)
				if err == nil {
					// A local committed capture is only a candidate list.
					// Fence every send in the same source write as the plans
					// and receipts before contacting a destination.
					if planIndex < 0 {
						planIndex = len(updates)
						updates = append(updates, clusterqueue.DeadLetterUpdate{ID: entry.ID, Authorize: name})
					}
					candidates = append(candidates, deadLetterCandidate{
						id: entry.ID, planIndex: planIndex, target: clusterTarget{Queue: name, Incarnation: target},
						record: b.publishRecord(header, message.Body),
					})
				}
				break
			}
		}
	}
	stopRoutes()
	planCtx, stopPlans := context.WithTimeout(ctx, clusterDeadLetterSourceDeadline)
	applied := submitClusterDeadLetterUpdates(planCtx, worker, shard, updates)
	stopPlans()
	progress := false
	for i, update := range updates {
		if update.Authorize == "" && applied&(1<<i) != 0 {
			progress = true
			break
		}
	}
	cursor.receipts = unappliedDeadLetterReceipts(cursor.receipts, applied)
	if len(cursor.receipts) != 0 {
		// Finish prior confirmed work before accumulating a second page of
		// receipts. Planned entries remain eligible on the next sweep.
		return cursor, progress
	}
	// Route lookups rotate to give blocked exchanges a bounded turn. Keep
	// the relative source order of records sent to the same destination.
	slices.SortFunc(candidates, func(a, b deadLetterCandidate) int {
		return cmp.Compare(a.id, b.id)
	})
	batches := groupClusterDeadLetterDestinations(candidates, applied, worker, shard)

	if len(batches) != 0 {
		start := int(cursor.round % uint64(len(batches)))
		for offset := range batches {
			if ctx.Err() != nil {
				break
			}
			batch := batches[(start+offset)%len(batches)]
			accepted := b.publishClusterDeadLetterBatch(ctx, worker, shard, batch)
			progress = progress || accepted != 0
			if batch.moves[0].Move == 0 {
				for i, id := range batch.ids {
					if accepted&(1<<i) != 0 {
						cursor.receipts = append(cursor.receipts, clusterqueue.DeadLetterUpdate{ID: id, Completed: batch.target.Queue})
					}
				}
			}
		}
	}
	// The next source commit pairs these receipts with new route plans.
	// A crash may replay an unrecorded receipt, as for any ambiguous
	// cross-shard outcome. The replicated source still owns the payload.
	return cursor, progress
}

func unappliedDeadLetterReceipts(receipts []clusterqueue.DeadLetterUpdate, applied uint64) []clusterqueue.DeadLetterUpdate {
	left := receipts[:0]
	for i, receipt := range receipts {
		if applied&(1<<i) == 0 {
			left = append(left, receipt)
		}
	}
	clear(receipts[len(left):])
	return left
}

func groupClusterDeadLetterDestinations(candidates []deadLetterCandidate, planned uint64, worker clusterLocalWorker, shard uint64) []deadLetterDestinationBatch {
	var batches []deadLetterDestinationBatch
	latest := make(map[clusterTarget]int)
	for _, candidate := range candidates {
		if planned&(1<<candidate.planIndex) == 0 {
			continue
		}
		move := clusterqueue.DeadLetterPublishRecord{Record: candidate.record}
		if worker.WorkShard(candidate.target.Queue) == shard {
			move.Move = candidate.id
		}
		index, exists := latest[candidate.target]
		size := move.WireBytes()
		if !exists || len(batches[index].moves) == clusterqueue.MaxDeadLetterPublishBatchMessages ||
			size > clusterqueue.MaxDeadLetterPublishBatchBytes-batches[index].wireBytes {
			index = len(batches)
			latest[candidate.target] = index
			batches = append(batches, deadLetterDestinationBatch{target: candidate.target})
		}
		batch := &batches[index]
		batch.ids = append(batch.ids, candidate.id)
		batch.moves = append(batch.moves, move)
		batch.wireBytes += size
	}
	return batches
}

func submitClusterDeadLetterUpdates(ctx context.Context, worker clusterLocalWorker, shard uint64, updates []clusterqueue.DeadLetterUpdate) uint64 {
	if len(updates) == 0 || ctx.Err() != nil {
		return 0
	}
	command, err := clusterqueue.EncodeDeadLetterUpdates(updates)
	if err != nil {
		if len(updates) <= 1 {
			return 0
		}
		// A page can contain wide fanout plans. Split only at the codec's
		// aggregate limit and retain bit positions for the caller.
		mid := len(updates) / 2
		return submitClusterDeadLetterUpdates(ctx, worker, shard, updates[:mid]) |
			submitClusterDeadLetterUpdates(ctx, worker, shard, updates[mid:])<<mid
	}
	data, err := worker.SubmitLocalWork(ctx, shard, command)
	if err != nil {
		return 0
	}
	result, err := clusterqueue.DecodeResponse(data)
	if err != nil || result.Err() != nil || result.DeadLetterApplied>>len(updates) != 0 {
		return 0
	}
	return result.DeadLetterApplied
}

func (b *ClusterBroker) deadLetterDestinations(ctx context.Context, exchanges map[string]*clusterExchange, message *protocol.Message) map[string]clusterqueue.QueueID {
	if message.Exchange == "" {
		if message.RoutingKey == "" {
			return nil
		}
		return map[string]clusterqueue.QueueID{message.RoutingKey: 0}
	}
	ex, found := exchanges[message.Exchange]
	if !found {
		if ctx.Err() != nil {
			return nil
		}
		lookup, cancel := context.WithTimeout(ctx, clusterDeadLetterLookupDeadline)
		reply, err := b.exchange(lookup, message.Exchange, clusterExchangeCommand{Kind: exchangeDescribe})
		cancel()
		if err == nil {
			// Broker-generated delivery can route through an internal exchange.
			ex = &reply.Exchange
			ex.Internal = false
		}
		exchanges[message.Exchange] = ex
	}
	if ex == nil {
		return nil
	}
	targets, err := routeClusterExchange(*ex, message.RoutingKey, message.Headers)
	if err != nil {
		return nil
	}
	names := make(map[string]clusterqueue.QueueID, len(targets))
	for _, target := range targets {
		names[target.Queue] = max(names[target.Queue], target.Incarnation)
	}
	return names
}

func (b *ClusterBroker) publishClusterDeadLetterBatch(ctx context.Context, worker clusterLocalWorker, shard uint64, batch deadLetterDestinationBatch) uint64 {
	if ctx.Err() != nil {
		return 0
	}
	var command []byte
	var err error
	single := len(batch.moves) == 1
	if single {
		item := batch.moves[0]
		command, err = clusterqueue.EncodeDeadLetterPublish(batch.target.Queue, batch.target.Incarnation, item.Move, item.Record)
	} else {
		command, err = clusterqueue.EncodeDeadLetterPublishBatch(batch.target.Queue, batch.target.Incarnation, batch.moves)
	}
	if err != nil {
		// This can occur when packed physical bytes fit but their logical
		// envelopes exceed the batch limit.
		if single {
			return 0
		}
		mid := len(batch.moves) / 2
		first := deadLetterDestinationBatch{target: batch.target, moves: batch.moves[:mid]}
		second := deadLetterDestinationBatch{target: batch.target, moves: batch.moves[mid:]}
		return b.publishClusterDeadLetterBatch(ctx, worker, shard, first) |
			b.publishClusterDeadLetterBatch(ctx, worker, shard, second)<<mid
	}
	attempt, cancel := context.WithTimeout(ctx, clusterDeadLetterTargetDeadline)
	defer cancel()
	var result clusterqueue.QueueResponse
	if batch.moves[0].Move == 0 {
		result, err = b.executeEncoded(attempt, batch.target.Queue, command, false)
	} else {
		var data []byte
		data, err = worker.SubmitLocalWork(attempt, shard, command)
		if err == nil {
			result, err = clusterqueue.DecodeResponse(data)
		}
	}
	if err != nil {
		return 0
	}
	if single {
		if result.Err() == nil {
			return 1
		}
		return 0
	}
	accepted := result.DeadLetterApplied
	count := bits.OnesCount64(accepted)
	if accepted>>len(batch.moves) != 0 || uint64(count) != result.Count {
		return 0
	}
	replyErr := result.Err()
	if count == len(batch.moves) && replyErr == nil ||
		count < len(batch.moves) && (errors.Is(replyErr, clusterqueue.ErrCapacity) || errors.Is(replyErr, clusterqueue.ErrIDsExhausted)) {
		return accepted
	}
	return 0
}
