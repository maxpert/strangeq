package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

type clusterGetKey struct {
	Connection, Queue string
	Channel           uint16
}

type clusterDelivery struct {
	Session  *clusterSession
	Attempt  clusterqueue.AttemptID
	Consumer string
}

type clusterConsumer struct {
	protocol *protocol.Consumer
	session  *clusterSession
	credit   atomic.Uint32
	start    sync.Once
}

type clusterSession struct {
	broker      *ClusterBroker
	queue       string
	id          clusterqueue.SessionID
	incarnation clusterqueue.QueueID
	owner       string
	ctx         context.Context
	cancel      context.CancelFunc

	mu     sync.Mutex
	seq    uint64
	failed error
	holds  atomic.Int32
}

func (b *ClusterBroker) openSession(ctx context.Context, queue string, consumer bool) (*clusterSession, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, err
	}
	owner := hex.EncodeToString(token[:])
	for range 8 {
		info, err := b.describe(ctx, queue)
		if err != nil {
			return nil, clusterQueueError(queue, err)
		}
		command := clusterqueue.QueueCommand{
			Kind: clusterqueue.OpenQueueSession, Queue: queue, Incarnation: info.Incarnation, Session: info.NextSession, Owner: owner,
			Prefetch: clusterDeliveryCredit, Consumer: consumer, Deadline: time.Now().Add(clusterLease).UnixNano(),
		}
		_, err = b.repeatableQueue(ctx, command)
		if errors.Is(err, clusterqueue.ErrSessionConflict) || errors.Is(err, clusterqueue.ErrSessionOrder) {
			continue
		}
		if err != nil {
			return nil, clusterQueueError(queue, err)
		}
		lifetime, cancel := context.WithCancel(context.Background())
		return &clusterSession{broker: b, queue: queue, id: info.NextSession, incarnation: info.Incarnation, owner: owner, ctx: lifetime, cancel: cancel}, nil
	}
	return nil, errors.New("queue session allocation is busy")
}

func (s *clusterSession) command(kind clusterqueue.QueueCommandKind) clusterqueue.QueueCommand {
	return clusterqueue.QueueCommand{Kind: kind, Queue: s.queue, Incarnation: s.incarnation, Session: s.id, Owner: s.owner}
}

func (s *clusterSession) run(ctx context.Context, kind clusterqueue.QueueCommandKind, attempt clusterqueue.AttemptID) (clusterqueue.QueueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return clusterqueue.QueueResponse{}, s.failed
	}
	if err := s.ctx.Err(); err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	command := s.command(kind)
	command.Sequence = s.seq + 1
	command.Attempt = attempt
	result, err := s.broker.execute(ctx, command, true)
	if err != nil {
		s.failed = fmt.Errorf("queue session outcome uncertain: %w", err)
		s.cancel()
		return result, s.failed
	}

	s.seq = command.Sequence
	err = result.Err()
	switch {
	case errors.Is(err, clusterqueue.ErrQueueIncarnation), errors.Is(err, clusterqueue.ErrQueueNotFound),
		errors.Is(err, clusterqueue.ErrSessionClosed), errors.Is(err, clusterqueue.ErrUnknownSession),
		errors.Is(err, clusterqueue.ErrSessionOwner), errors.Is(err, clusterqueue.ErrSequenceGap),
		errors.Is(err, clusterqueue.ErrStaleSequence), errors.Is(err, clusterqueue.ErrSequenceConflict):
		s.failed = err
		s.cancel()
	}
	if err == nil {
		if kind == clusterqueue.ClaimMessage {
			s.holds.Add(1)
		} else {
			s.holds.Add(-1)
		}
	}
	return result, err
}

func (s *clusterSession) renew() {
	timer := time.NewTicker(clusterLease / 3)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-timer.C:
			command := s.command(clusterqueue.RenewQueueSession)
			command.Deadline = now.Add(clusterLease).UnixNano()
			ctx, cancel := context.WithTimeout(s.ctx, clusterRPCDeadline)
			_, err := s.broker.repeatableQueue(ctx, command)
			cancel()
			if errors.Is(err, clusterqueue.ErrQueueIncarnation) || errors.Is(err, clusterqueue.ErrQueueNotFound) ||
				errors.Is(err, clusterqueue.ErrSessionClosed) || errors.Is(err, clusterqueue.ErrUnknownSession) {
				s.cancel()
				return
			}
		}
	}
}

func (s *clusterSession) release() error {
	ctx, cancel := clusterContext()
	defer cancel()
	return s.releaseContext(ctx)
}

func (s *clusterSession) releaseContext(ctx context.Context) error {
	s.cancel()
	_, err := s.broker.repeatableQueue(ctx, s.command(clusterqueue.ReleaseQueueSession))
	if errors.Is(err, clusterqueue.ErrQueueIncarnation) || errors.Is(err, clusterqueue.ErrQueueNotFound) ||
		errors.Is(err, clusterqueue.ErrSessionClosed) {
		return nil
	}
	return err
}

func (b *ClusterBroker) retain(s *clusterSession, attempt clusterqueue.AttemptID, consumer string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s.ctx.Err() != nil {
		return 0
	}
	tag := b.nextTag.Add(1)
	b.deliveries[tag] = clusterDelivery{s, attempt, consumer}
	return tag
}

func (b *ClusterBroker) claim(ctx context.Context, s *clusterSession, noAck bool, consumer string) (*protocol.Message, uint64, uint32, error) {
	res, err := s.run(ctx, clusterqueue.ClaimMessage, 0)
	if errors.Is(err, clusterqueue.ErrEmpty) || errors.Is(err, clusterqueue.ErrNoCredit) {
		return nil, 0, clusterCount(res.Queue.Ready), nil
	}
	if err != nil {
		return nil, 0, 0, err
	}
	delivery := res.Result.Delivery
	message, err := decodeClusterMessage(delivery.Body)
	if err != nil {
		return nil, 0, 0, err
	}
	message.Redelivered = delivery.Redelivered
	var tag uint64
	if noAck {
		if _, err := s.run(ctx, clusterqueue.AckMessage, delivery.Attempt); err != nil {
			return nil, 0, 0, err
		}
	} else {
		tag = b.retain(s, delivery.Attempt, consumer)
		if tag == 0 {
			return nil, 0, 0, context.Canceled
		}
	}
	message.DeliveryTag = tag
	return message, tag, clusterCount(res.Queue.Ready), nil
}

func (b *ClusterBroker) getSession(ctx context.Context, key clusterGetKey) (*clusterSession, error) {
	b.mu.Lock()
	old := b.gets[key]
	if old != nil && old.ctx.Err() == nil {
		b.mu.Unlock()
		return old, nil
	}
	b.mu.Unlock()
	session, err := b.openSession(ctx, key.Queue, false)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	old = b.gets[key]
	if old != nil && old.ctx.Err() == nil {
		b.mu.Unlock()
		_ = session.release()
		return old, nil
	}
	if old != nil {
		b.purgeDeliveryLocked(old)
	}
	b.gets[key] = session
	b.mu.Unlock()
	go session.renew()
	return session, nil
}

func (b *ClusterBroker) GetMessageForGet(queue string, noAck bool) (*protocol.Message, uint64, uint32, error) {
	return b.GetMessageForGetOwned(queue, noAck, "")
}
func (b *ClusterBroker) GetMessageForGetOwned(queue string, noAck bool, connection string) (*protocol.Message, uint64, uint32, error) {
	return b.getMessageForChannel(queue, noAck, connection, 0)
}

func (b *ClusterBroker) getMessageForChannel(queue string, noAck bool, connection string, channel uint16) (*protocol.Message, uint64, uint32, error) {
	ctx, cancel := clusterContext()
	defer cancel()
	key := clusterGetKey{Connection: connection, Queue: queue, Channel: channel}
	session, err := b.getSession(ctx, key)
	if err != nil {
		return nil, 0, 0, err
	}
	message, tag, ready, err := b.claim(ctx, session, noAck, "")
	if errors.Is(err, clusterqueue.ErrQueueIncarnation) {
		// The failed claim made no change, and run canceled the old session.
		// Reopen once against the current queue to handle delete/redeclare.
		session, err = b.getSession(ctx, key)
		if err != nil {
			return nil, 0, 0, err
		}
		message, tag, ready, err = b.claim(ctx, session, noAck, "")
	}
	return message, tag, ready, clusterQueueError(queue, err)
}

func (b *ClusterBroker) RequeueGetDeliveries(connection string) {
	b.requeueGets(connection, 0)
}

func (b *ClusterBroker) requeueGetChannelDeliveries(connection string, channel uint16) {
	b.requeueGets(connection, channel)
}

func (b *ClusterBroker) requeueGets(connection string, channel uint16) {
	b.mu.Lock()
	var sessions []*clusterSession
	for key, session := range b.gets {
		if key.Connection == connection && (channel == 0 || key.Channel == channel) {
			session.cancel()
			delete(b.gets, key)
			b.purgeDeliveryLocked(session)
			sessions = append(sessions, session)
		}
	}
	b.mu.Unlock()
	ctx, cancel := clusterContext()
	defer cancel()
	for _, session := range sessions {
		_ = session.releaseContext(ctx)
	}
}

func (b *ClusterBroker) purgeDeliveryLocked(session *clusterSession) {
	for tag, delivery := range b.deliveries {
		if delivery.Session == session {
			delete(b.deliveries, tag)
		}
	}
}

func (b *ClusterBroker) GetConsumerForDelivery(tag uint64) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.deliveries[tag]
	return entry.Consumer, ok
}

func (b *ClusterBroker) settle(consumer string, tag uint64, multiple bool, kind clusterqueue.QueueCommandKind) error {
	b.mu.Lock()
	var tags []uint64
	if multiple {
		for candidate, delivery := range b.deliveries {
			if delivery.Consumer == consumer && (tag == 0 || candidate <= tag) {
				tags = append(tags, candidate)
			}
		}
	} else if entry, ok := b.deliveries[tag]; ok && entry.Consumer == consumer {
		tags = append(tags, tag)
	}
	b.mu.Unlock()
	if len(tags) == 0 {
		return clusterqueue.ErrUnknownAttempt
	}
	slices.Sort(tags)
	ctx, cancel := clusterContext()
	defer cancel()
	for _, tag := range tags {
		if err := b.settleAttempt(ctx, consumer, tag, kind); err != nil {
			return err
		}
	}
	return nil
}

func (b *ClusterBroker) settleAttempt(ctx context.Context, consumer string, tag uint64, kind clusterqueue.QueueCommandKind) error {
	b.mu.Lock()
	entry, ok := b.deliveries[tag]
	b.mu.Unlock()
	if !ok || entry.Consumer != consumer {
		return clusterqueue.ErrUnknownAttempt
	}
	if _, err := entry.Session.run(ctx, kind, entry.Attempt); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.deliveries, tag)
	b.mu.Unlock()
	return nil
}

func (b *ClusterBroker) settleWireTags(channel *protocol.Channel, upto uint64, kind settleKind, requeue bool) (int, error) {
	command := clusterqueue.AckMessage
	if kind == settleNack {
		command = clusterqueue.DropMessage
		if requeue {
			command = clusterqueue.RequeueMessage
		}
	}

	tags, refs := channel.TakeWireTagEntriesUpTo(upto)
	ctx, cancel := clusterContext()
	defer cancel()
	for i, ref := range refs {
		if err := b.settleAttempt(ctx, ref.ConsumerID, ref.MsgID, command); err != nil {
			for j := i; j < len(refs); j++ {
				unsettled := refs[j]
				channel.TrackDelivery(tags[j], unsettled.MsgID, unsettled.ConsumerID, unsettled.IsGet)
			}
			return i, fmt.Errorf("cluster cumulative settlement at delivery tag %d: %w", tags[i], err)
		}
	}
	return len(refs), nil
}

func (b *ClusterBroker) AcknowledgeMessage(c string, t uint64, m bool) error {
	return b.settle(c, t, m, clusterqueue.AckMessage)
}
func (b *ClusterBroker) RejectMessage(c string, t uint64, requeue bool) error {
	return b.NacknowledgeMessage(c, t, false, requeue)
}
func (b *ClusterBroker) NacknowledgeMessage(c string, t uint64, m, requeue bool) error {
	kind := clusterqueue.DropMessage
	if requeue {
		kind = clusterqueue.RequeueMessage
	}
	return b.settle(c, t, m, kind)
}
func (b *ClusterBroker) AcknowledgeGetDelivery(t uint64) error {
	return b.AcknowledgeMessage("", t, false)
}
func (b *ClusterBroker) RejectGetDelivery(t uint64, requeue bool) error {
	return b.NacknowledgeMessage("", t, false, requeue)
}
func (b *ClusterBroker) NackGetDelivery(t uint64, requeue bool) error {
	return b.NacknowledgeMessage("", t, false, requeue)
}

func (b *ClusterBroker) RegisterConsumer(queue, id string, consumer *protocol.Consumer) error {
	if err := b.registerConsumer(queue, id, consumer); err != nil {
		return err
	}
	b.startConsumer(id)
	return nil
}

func (b *ClusterBroker) registerConsumer(queue, id string, consumer *protocol.Consumer) error {
	if consumer.Exclusive {
		return fmt.Errorf("%w: exclusive consumers are unavailable in cluster mode", broker.ErrInvalidQueueArgument)
	}
	ctx, cancel := clusterContext()
	defer cancel()
	session, err := b.openSession(ctx, queue, true)
	if err != nil {
		return err
	}
	entry := &clusterConsumer{protocol: consumer, session: session}
	entry.credit.Store(clusterCredit(consumer.PrefetchCount))
	b.mu.Lock()
	if b.consumers[id] != nil {
		b.mu.Unlock()
		_ = session.release()
		return errors.New("consumer identity already registered")
	}
	b.consumers[id] = entry
	b.mu.Unlock()
	go session.renew()
	return nil
}

func (b *ClusterBroker) startConsumer(id string) {
	b.mu.Lock()
	entry := b.consumers[id]
	b.mu.Unlock()
	if entry != nil {
		entry.start.Do(func() { go b.consume(id, entry) })
	}
}

func clusterCredit(prefetch uint16) uint32 {
	if prefetch == 0 {
		return clusterDeliveryCredit
	}
	return min(uint32(prefetch), clusterDeliveryCredit)
}

func (b *ClusterBroker) consume(id string, entry *clusterConsumer) {
	s := entry.session
	defer func() {
		select {
		case <-entry.protocol.Cancel:
			// The AMQP cancel handler owns the release before cancel-ok.
		default:
			b.consumerExited(id, entry)
		}
	}()
	delay := time.NewTimer(time.Hour)
	delay.Stop()
	defer delay.Stop()
	for s.ctx.Err() == nil {
		if uint32(s.holds.Load()) < entry.credit.Load() {
			ctx, cancel := context.WithTimeout(s.ctx, clusterRPCDeadline)
			message, tag, _, err := b.claim(ctx, s, entry.protocol.NoAck, id)
			cancel()
			if err != nil {
				return
			}
			if message != nil {
				delivery := &protocol.Delivery{Message: message, DeliveryTag: tag, ConsumerID: id,
					Exchange: message.Exchange, RoutingKey: message.RoutingKey, Redelivered: message.Redelivered, NoAck: entry.protocol.NoAck}
				select {
				case entry.protocol.Messages <- delivery:
					continue
				case <-s.ctx.Done():
					return
				case <-entry.protocol.Cancel:
					return
				}
			}
		}
		delay.Reset(100 * time.Millisecond)
		select {
		case <-delay.C:
		case <-s.ctx.Done():
			return
		case <-entry.protocol.Cancel:
			return
		}
	}
}

func (b *ClusterBroker) consumerExited(id string, entry *clusterConsumer) {
	b.mu.Lock()
	if b.consumers[id] != entry {
		b.mu.Unlock()
		return
	}
	entry.session.cancel()
	delete(b.consumers, id)
	b.purgeDeliveryLocked(entry.session)
	b.mu.Unlock()

	_ = entry.session.release()
	if b.cancelConsumers != nil {
		b.cancelConsumers([]string{id})
	}
}

func (b *ClusterBroker) UnregisterConsumer(id string) error {
	return b.unregisterConsumers([]string{id})
}

func (b *ClusterBroker) unregisterConsumers(ids []string) error {
	b.mu.Lock()
	var sessions []*clusterSession
	for _, id := range ids {
		if entry := b.consumers[id]; entry != nil {
			entry.session.cancel()
			delete(b.consumers, id)
			b.purgeDeliveryLocked(entry.session)
			sessions = append(sessions, entry.session)
		}
	}
	b.mu.Unlock()
	ctx, cancel := clusterContext()
	defer cancel()
	var releaseErr error
	for _, s := range sessions {
		releaseErr = errors.Join(releaseErr, s.releaseContext(ctx))
	}
	return releaseErr
}

func (b *ClusterBroker) RequeueAllForConsumer(id string) error {
	if err := b.settle(id, 0, true, clusterqueue.RequeueMessage); errors.Is(err, clusterqueue.ErrUnknownAttempt) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.consumers[id] == nil {
			return interfaces.ErrConsumerNotFound
		}
		return nil
	} else {
		return err
	}
}

func (b *ClusterBroker) GetConsumers() map[string]*protocol.Consumer {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make(map[string]*protocol.Consumer, len(b.consumers))
	for id, entry := range b.consumers {
		result[id] = entry.protocol
	}
	return result
}
func (b *ClusterBroker) UpdateConsumerPrefetch(id string, prefetch uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry := b.consumers[id]; entry != nil {
		entry.credit.Store(clusterCredit(prefetch))
	}
}
