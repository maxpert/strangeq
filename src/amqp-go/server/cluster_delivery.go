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

// A readiness hint never reserves a message. The next replicated claim
// decides whether this queue incarnation still has an eligible delivery.
// ErrEmpty means the leader completed a healthy wait without seeing work.
type clusterQueueWaiter interface {
	WaitQueueReady(context.Context, string, clusterqueue.QueueID) error
}

type clusterSession struct {
	broker      *ClusterBroker
	queue       string
	id          clusterqueue.SessionID
	incarnation clusterqueue.QueueID
	owner       string
	autoAssign  bool
	ctx         context.Context
	cancel      context.CancelFunc
	creditWake  chan struct{}

	mu        sync.Mutex
	seq       uint64
	failed    error
	holds     atomic.Int32
	refill    []clusterqueue.Delivery // broker.mu protects the mailbox and its contribution to holds
	readyWait context.CancelFunc

	// broker.mu serializes the dispatcher and settlement reply.
	assignmentCursor clusterqueue.AttemptID
	assignmentPage   bufferedAssignmentPage
	assignmentWait   context.CancelFunc
}

func (b *ClusterBroker) openSession(ctx context.Context, queue string, consumer bool, credit uint32, autoAssign bool) (*clusterSession, error) {
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
			Prefetch: credit, Consumer: consumer, AutoAssign: autoAssign, Deadline: time.Now().Add(clusterLease).UnixNano(),
		}
		_, err = b.repeatableQueue(ctx, command)
		if errors.Is(err, clusterqueue.ErrSessionConflict) || errors.Is(err, clusterqueue.ErrSessionOrder) {
			continue
		}
		if err != nil {
			return nil, clusterQueueError(queue, err)
		}
		lifetime, cancel := context.WithCancel(context.Background())
		return &clusterSession{
			broker: b, queue: queue, id: info.NextSession, incarnation: info.Incarnation,
			owner: owner, autoAssign: autoAssign, ctx: lifetime, cancel: cancel,
		}, nil
	}
	return nil, errors.New("queue session allocation is busy")
}

func (s *clusterSession) command(kind clusterqueue.QueueCommandKind) clusterqueue.QueueCommand {
	command := clusterqueue.QueueCommand{Kind: kind, Queue: s.queue, Incarnation: s.incarnation, Session: s.id, Owner: s.owner}
	if kind == clusterqueue.DropMessage {
		command.RejectedAt = time.Now().UnixNano()
	}
	return command
}

func (s *clusterSession) run(ctx context.Context, kind clusterqueue.QueueCommandKind, attempt clusterqueue.AttemptID) (clusterqueue.QueueResponse, error) {
	command := s.command(kind)
	command.Attempt = attempt
	return s.runCommand(ctx, command)
}

func (s *clusterSession) runCommand(ctx context.Context, command clusterqueue.QueueCommand) (clusterqueue.QueueResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runCommandLocked(ctx, command)
}

func (s *clusterSession) runCommandLocked(ctx context.Context, command clusterqueue.QueueCommand) (clusterqueue.QueueResponse, error) {
	if s.failed != nil {
		return clusterqueue.QueueResponse{}, s.failed
	}
	if err := s.ctx.Err(); err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	command.Sequence = s.seq + 1
	s.requestAssignmentPage(&command)
	result, err := s.broker.execute(ctx, command, true)
	if err != nil {
		s.failed = fmt.Errorf("queue session outcome uncertain: %w", err)
		s.cancel()
		return clusterqueue.QueueResponse{}, s.failed
	}

	err = result.Err()
	switch command.Kind {
	case clusterqueue.SetAssignmentCredit:
		// Existing flights remain reserved across a credit change.
	case clusterqueue.AckAndClaim:
		if !validAckClaimResult(command, result, err) {
			s.failed = errors.New("queue ACK and claim outcome has invalid counts")
			s.cancel()
			return clusterqueue.QueueResponse{}, s.failed
		}
		s.broker.mu.Lock()
		s.holds.Add(int32(result.Count) - int32(result.Settled))
		s.refill = result.Deliveries
		s.broker.mu.Unlock()
		if errors.Is(result.ClaimErr(), clusterqueue.ErrIDsExhausted) {
			s.failed = result.ClaimErr()
			s.cancel()
		}
		if result.Count != 0 && s.readyWait != nil {
			s.readyWait()
		}
		if result.Settled != 0 {
			s.wakeCredit()
		}
	case clusterqueue.SettleBatch:
		maxCount := uint64(len(command.Attempts))
		switch {
		case err == nil && result.Count == maxCount:
		case result.Count < maxCount && (errors.Is(err, clusterqueue.ErrUnknownAttempt) ||
			command.Settlement == clusterqueue.DropMessage && errors.Is(err, clusterqueue.ErrIDsExhausted)):
		case err != nil && result.Count == 0:
		default:
			s.failed = errors.New("queue settlement outcome has an invalid count")
			s.cancel()
			return clusterqueue.QueueResponse{}, s.failed
		}
		if result.Count != 0 {
			s.holds.Add(-int32(result.Count))
			s.wakeCredit()
		}
	case clusterqueue.ClaimBatch:
		if err == nil {
			if !result.DeliveryBatch || result.Count == 0 || result.Count > uint64(command.ClaimLimit) ||
				result.Count != uint64(len(result.Deliveries)) {
				s.failed = errors.New("queue claim batch outcome has an invalid count")
				s.cancel()
				return clusterqueue.QueueResponse{}, s.failed
			}
			s.holds.Add(int32(result.Count))
		}
	default:
		if err == nil {
			if command.Kind == clusterqueue.ClaimMessage {
				s.holds.Add(1)
			} else {
				s.holds.Add(-1)
				s.wakeCredit()
			}
		}
	}
	s.seq = command.Sequence
	if err == nil {
		s.bufferAssignmentPage(command.AssignmentAfter, result)
	}
	switch {
	case errors.Is(err, clusterqueue.ErrQueueIncarnation), errors.Is(err, clusterqueue.ErrQueueNotFound),
		errors.Is(err, clusterqueue.ErrSessionClosed), errors.Is(err, clusterqueue.ErrUnknownSession),
		errors.Is(err, clusterqueue.ErrSessionOwner), errors.Is(err, clusterqueue.ErrSequenceGap),
		errors.Is(err, clusterqueue.ErrStaleSequence), errors.Is(err, clusterqueue.ErrSequenceConflict):
		s.failed = err
		s.cancel()
	}
	return result, err
}

func (s *clusterSession) wakeCredit() {
	select {
	case s.creditWake <- struct{}{}:
	default:
	}
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
	return b.retainLocked(s, attempt, consumer)
}

func (b *ClusterBroker) retainLocked(s *clusterSession, attempt clusterqueue.AttemptID, consumer string) uint64 {
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
	message, err := decodeStoredClusterMessage(delivery.Body, delivery.Packed)
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
	session, err := b.openSession(ctx, key.Queue, false, clusterDeliveryCredit, false)
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
	clusterqueue.ReleaseDeliveryBodies(session.assignmentPage.deliveries)
	session.assignmentPage = bufferedAssignmentPage{}
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
	settled := 0
	for settled < len(refs) {
		count, err := b.settleWireRun(ctx, command, refs[settled:])
		settled += count
		if err != nil {
			for j := settled; j < len(refs); j++ {
				unsettled := refs[j]
				channel.TrackDelivery(tags[j], unsettled.MsgID, unsettled.ConsumerID, unsettled.IsGet)
			}
			return settled, fmt.Errorf("cluster cumulative settlement at delivery tag %d: %w", tags[settled], err)
		}
	}
	return settled, nil
}

// A run contains only consecutive tags in one session. Splitting at a
// session change preserves the channel-wide successful prefix on failure.
func (b *ClusterBroker) settleWireRun(ctx context.Context, kind clusterqueue.QueueCommandKind, refs []protocol.WireDeliveryRef) (int, error) {
	var attempts [clusterqueue.MaxSettlementBatch]clusterqueue.AttemptID
	count := 0
	b.mu.Lock()
	first, ok := b.deliveries[refs[0].MsgID]
	b.mu.Unlock()
	if !ok || first.Consumer != refs[0].ConsumerID {
		return 0, clusterqueue.ErrUnknownAttempt
	}
	session := first.Session
	session.mu.Lock()
	defer session.mu.Unlock()
	b.mu.Lock()
	for _, ref := range refs[:min(len(refs), len(attempts))] {
		delivery, ok := b.deliveries[ref.MsgID]
		if !ok || delivery.Consumer != ref.ConsumerID || delivery.Session != first.Session {
			break
		}
		attempts[count] = delivery.Attempt
		count++
	}
	if count == 0 {
		b.mu.Unlock()
		return 0, clusterqueue.ErrUnknownAttempt
	}
	command := session.command(kind)
	if count == 1 {
		command.Attempt = attempts[0]
	} else {
		command.Kind = clusterqueue.SettleBatch
		command.Settlement = kind
		command.Attempts = attempts[:count]
	}
	if kind == clusterqueue.AckMessage && !refs[0].IsGet {
		if limit := b.ackClaimLimitLocked(first, count); limit != 0 {
			command = session.command(clusterqueue.AckAndClaim)
			command.Attempts = attempts[:count]
			command.ClaimLimit = limit
			command.ClaimBytes = clusterqueue.MaxClaimBatchBytes
		}
	}
	b.mu.Unlock()

	result, err := session.runCommandLocked(ctx, command)
	settled := 0
	switch command.Kind {
	case clusterqueue.AckAndClaim:
		settled = int(result.Settled)
	case clusterqueue.SettleBatch:
		settled = int(result.Count)
	default:
		if err == nil {
			settled = 1
		}
	}
	b.mu.Lock()
	for _, ref := range refs[:settled] {
		delete(b.deliveries, ref.MsgID)
	}
	b.mu.Unlock()
	return settled, err
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
	credit := clusterCredit(consumer.PrefetchCount)
	reservation := uint32(clusterDeliveryCredit)
	runtime, supportsAssignments := b.cluster.(clusterAssignmentRuntime)
	autoAssign := !consumer.NoAck && supportsAssignments && runtime.SupportsAssignments()
	if autoAssign {
		reservation = credit
	}
	session, err := b.openSession(ctx, queue, true, reservation, autoAssign)
	if err != nil {
		return err
	}
	if !consumer.NoAck {
		session.creditWake = make(chan struct{}, 1)
	}
	entry := &clusterConsumer{protocol: consumer, session: session}
	entry.credit.Store(credit)
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
	if !entry.protocol.NoAck {
		b.consumeManualBatch(id, entry)
		return
	}
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

func (b *ClusterBroker) consumeManualBatch(id string, entry *clusterConsumer) {
	s := entry.session
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	var pending []clusterqueue.Delivery
	var message *protocol.Message
	defer func() {
		if message != nil {
			message.ReleaseBody()
		}
		clusterqueue.ReleaseDeliveryBodies(pending)
	}()
	var cursor clusterqueue.AttemptID
	for s.ctx.Err() == nil {
		if len(pending) == 0 {
			ctx, cancel := context.WithTimeout(s.ctx, clusterRPCDeadline)
			result, next, err := entry.nextManualPage(ctx, cursor)
			cancel()
			if errors.Is(err, clusterqueue.ErrNoCredit) {
				if !entry.waitForBatchDispatch(timer, 0) {
					return
				}
				continue
			}
			if err != nil && !errors.Is(err, clusterqueue.ErrEmpty) {
				if !s.autoAssign || assignmentEnded(err) || !entry.waitForBatchDispatch(timer, 5*time.Millisecond) {
					return
				}
				continue
			}
			pending, cursor = result, next
			if len(pending) == 0 {
				if !entry.waitForManualPage(timer, cursor, err) {
					return
				}
				continue
			}
		}

		claimed := pending[0]
		if message == nil {
			var err error
			message, err = decodeStoredClusterMessage(claimed.Body, claimed.Packed)
			if err != nil {
				return
			}
			message.Redelivered = claimed.Redelivered
			if claimed.BodyLease != nil {
				// This decoder created a fresh egress-only message. Move the
				// sole lease owner out of the pending item before publication.
				message.SetBodyRelease(claimed.BodyLease.Release)
				pending[0].BodyLease, claimed.BodyLease = nil, nil
			}
		}
		// Drop the envelope aliases before publication. A fast writer may
		// finish and release the loan as soon as the channel handoff succeeds.
		// While credit is unavailable, the decoded message and lease suffice
		// for retry; the envelope is never decoded twice.
		pending[0].Body = nil
		claimed.Body = nil
		issued, delay := b.offerBatchDelivery(id, entry, claimed.Attempt, message, len(pending))
		if !issued {
			if !entry.waitForBatchDispatch(timer, delay) {
				return
			}
			continue
		}
		pending[0] = clusterqueue.Delivery{}
		pending = pending[1:]
		message = nil
	}
}

func (entry *clusterConsumer) waitForQueueReady(timer *time.Timer, claimErr error) bool {
	session := entry.session
	waiter, ok := session.broker.cluster.(clusterQueueWaiter)
	if !ok || !errors.Is(claimErr, clusterqueue.ErrEmpty) {
		return entry.waitForBatchDispatch(timer, 100*time.Millisecond)
	}
	for session.ctx.Err() == nil {
		// Register the child wait under the sequence lock so an installed
		// refill either prevents the wait or cancels that exact request.
		session.mu.Lock()
		if session.hasRefill() {
			session.mu.Unlock()
			return session.ctx.Err() == nil
		}
		ctx, cancel := context.WithCancel(session.ctx)
		session.readyWait = cancel
		session.mu.Unlock()
		err := waiter.WaitQueueReady(ctx, session.queue, session.incarnation)
		cancel()
		session.mu.Lock()
		session.readyWait = nil
		refilled := session.hasRefill()
		session.mu.Unlock()
		if refilled {
			return session.ctx.Err() == nil
		}
		if err == nil || errors.Is(err, clusterqueue.ErrQueueNotFound) || errors.Is(err, clusterqueue.ErrQueueIncarnation) {
			return session.ctx.Err() == nil
		}
		if !errors.Is(err, clusterqueue.ErrEmpty) {
			// A busy, unreachable or older peer may be unable to wait even
			// though the queue can accept authoritative claims.
			return entry.waitForBatchDispatch(timer, 100*time.Millisecond)
		}
		// A healthy idle wait releases its admission slot periodically.
		if !entry.waitForBatchDispatch(timer, 5*time.Millisecond) {
			return false
		}
	}
	return false
}

func (entry *clusterConsumer) waitForBatchDispatch(timer *time.Timer, delay time.Duration) bool {
	var retry <-chan time.Time
	if delay > 0 {
		timer.Reset(delay)
		defer timer.Stop()
		retry = timer.C
	}
	select {
	case <-entry.session.ctx.Done():
		return false
	case <-entry.protocol.Cancel:
		return false
	case <-entry.session.creditWake:
		return true
	case <-retry:
		return true
	}
}

func (b *ClusterBroker) offerBatchDelivery(id string, entry *clusterConsumer, attempt clusterqueue.AttemptID, message *protocol.Message, pending int) (bool, time.Duration) {
	s := entry.session
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.consumers[id] != entry || s.ctx.Err() != nil {
		return false, 0
	}
	select {
	case <-entry.protocol.Cancel:
		return false, 0
	default:
	}
	// The same lock protects credit updates and the handoff. Reservations
	// wait for credit without acquiring tags or reaching the writer.
	sent := s.holds.Load() - int32(pending) - int32(len(s.refill))
	if sent >= int32(entry.credit.Load()) {
		return false, 0
	}
	messages := entry.protocol.Messages
	if cap(messages) != 0 && len(messages) == cap(messages) {
		return false, time.Millisecond
	}
	tag := b.retainLocked(s, attempt, id)
	message.DeliveryTag = tag
	delivery := &protocol.Delivery{Message: message, DeliveryTag: tag, ConsumerID: id,
		Exchange: message.Exchange, RoutingKey: message.RoutingKey, Redelivered: message.Redelivered}
	select {
	case messages <- delivery:
		return true, 0
	default:
		delete(b.deliveries, tag)
		return false, time.Millisecond
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
	// Auto-ack deliveries are already settled. The forwarder can still
	// send buffered deliveries until the server cancels the consumer.
	if !entry.protocol.NoAck {
		drainConsumerBodyLeases(entry.protocol.Messages)
	}
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
			if !entry.protocol.NoAck {
				drainConsumerBodyLeases(entry.protocol.Messages)
			}
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
	_ = b.updateConsumerPrefetch(id, prefetch)
}

func (b *ClusterBroker) updateConsumerPrefetch(id string, prefetch uint16) error {
	credit := clusterCredit(prefetch)
	b.mu.Lock()
	entry := b.consumers[id]
	if entry != nil {
		entry.credit.Store(credit)
		entry.session.wakeCredit()
	}
	b.mu.Unlock()
	if entry == nil || !entry.session.autoAssign {
		return nil
	}
	ctx, cancel := clusterContext()
	defer cancel()
	command := entry.session.command(clusterqueue.SetAssignmentCredit)
	command.Prefetch = credit
	_, err := entry.session.runCommand(ctx, command)
	return err
}
