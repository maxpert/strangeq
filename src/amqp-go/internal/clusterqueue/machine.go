package clusterqueue

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"slices"
	"unicode/utf8"
)

const (
	maxQueues         = 4096
	maxSessions       = 8192
	maxMessages       = 131072
	maxBodyBytes      = 48 << 20
	maxCommandBytes   = 32 << 20
	maxMessageBytes   = (24 << 20) - 4096
	maxArgumentsBytes = 4096
	maxExpiryBatch    = 128
)

type Machine struct {
	state       machineState
	assignments bool
}

func NewAssignmentMachine() Machine { return Machine{assignments: true} }

type machineState struct {
	LastQueue           QueueID
	Queues              map[string]*queueState
	LastDeadLetter      uint64                       `json:",omitempty" cbor:",omitempty"`
	DeadLetters         map[uint64]pendingDeadLetter `json:",omitempty" cbor:",omitempty"`
	deadLetterIDs       []uint64
	deadLetterPlanBytes uint64
	deadLetterCopyBytes uint64
	deadLetterCopySlots uint64
	bodies              uint64
	coldBytes           uint64
	packingSavings      uint64
	messages            uint64
	sessions            uint64
}

type queueState struct {
	name                string
	ID                  QueueID
	Spec                QueueSpec
	LastSession         SessionID
	LastMessage         MessageID
	LastAttempt         AttemptID
	LastAssignedSession SessionID `json:",omitempty" cbor:",omitempty"`
	Ready               []messageState
	Sessions            map[SessionID]*sessionState
	unacked             uint64
	consumers           uint64
	pushEligible        []SessionID
}

type sessionState struct {
	Owner            string
	Prefetch         uint32
	Consumer         bool
	AutoAssign       bool      `json:",omitempty" cbor:",omitempty"`
	AssignedThrough  AttemptID `json:",omitempty" cbor:",omitempty"`
	RegisteredCredit uint32    `json:",omitempty" cbor:",omitempty"`
	Deadline         int64
	Sequence         uint64
	LastKey          commandKey
	Last             outcome
	Flights          map[AttemptID]messageState
}

type messageState struct {
	ID                 MessageID
	Body               []byte `json:",omitempty"`
	Redelivered        bool   `json:",omitempty"`
	DeadLetterCopyUsed bool   `json:",omitempty" cbor:",omitempty"`
	DeadLetterReserve  uint16 `json:",omitempty" cbor:",omitempty"`
	LogicalSize        int    `json:",omitempty" cbor:",omitempty"`
	cold               *journalBody
}

type commandKey struct {
	Kind    QueueCommandKind
	Attempt AttemptID         `json:",omitempty"`
	Hash    [sha256.Size]byte `json:",omitzero"`
}

type outcome struct {
	Message    MessageID `json:",omitempty"`
	Attempt    AttemptID `json:",omitempty"`
	Code       string    `json:",omitempty"`
	Ready      uint64    `json:",omitempty"`
	Settled    uint64    `json:",omitempty" cbor:",omitempty"`
	ClaimCode  string    `json:",omitempty" cbor:",omitempty"`
	BatchFirst AttemptID `json:",omitempty" cbor:",omitempty"`
	BatchCount uint32    `json:",omitempty" cbor:",omitempty"`
}

// Apply executes one committed queue command. The caller serializes machine
// methods in log order for each replica.
func (m *Machine) Apply(data []byte) []byte {
	reply, _ := m.ApplyCommitted(data, true)
	return reply
}

// ApplyCommitted preserves all queue and session transitions. It materializes a
// response only when the local Raft request still has a waiter.
func (m *Machine) ApplyCommitted(data []byte, wantReply bool) ([]byte, error) {
	return m.ApplyJournaled(data, wantReply, nil)
}

// ApplyJournaled may retain a reference to large bodies in source. Source must
// provide immutable command bytes at offset zero for the lifetime of the group.
func (m *Machine) ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error) {
	return m.applyCommitted(data, wantReply, source, encodeQueueReply)
}

const minDeferredReplyBytes = 32 << 10

// ApplyJournaledReply applies a command and captures large replies for later
// encoding. The encoder uses only the captured outcome and its immutable
// payloads. The caller keeps journal sources readable until encoding finishes.
func (m *Machine) ApplyJournaledReply(data []byte, wantReply bool, source io.ReaderAt) (inline []byte, encode func() ([]byte, error), err error) {
	reply := m.applyCommand(data, wantReply, source)
	if reply.storageErr != nil {
		return nil, nil, reply.storageErr
	}
	if !wantReply {
		return nil, nil, nil
	}
	if reply.deliveryBytes()+reply.assignmentPageBytes >= minDeferredReplyBytes {
		return nil, func() ([]byte, error) { return encodeQueueReply(reply) }, nil
	}
	inline, err = encodeQueueReply(reply)
	return inline, nil, err
}

func (m *Machine) applyCommitted(data []byte, wantReply bool, source io.ReaderAt, encode func(QueueResponse) ([]byte, error)) ([]byte, error) {
	reply := m.applyCommand(data, wantReply, source)
	if reply.storageErr != nil {
		return nil, reply.storageErr
	}
	if !wantReply {
		return nil, nil
	}
	return encode(reply)
}

func (m *Machine) applyCommand(data []byte, wantReply bool, source io.ReaderAt) QueueResponse {
	if IsAssignmentCommand(data) && !m.assignments {
		return failure(ErrInvalidCommand)
	}
	c, err := DecodeCommand(data)
	if err != nil {
		return failure(ErrInvalidCommand)
	}
	// The validated command body is the suffix of data.
	origin := journalBodyOrigin{source: source, offset: int64(len(data) - len(c.Body))}
	return m.apply(c, wantReply, origin)
}

func encodeQueueReply(r QueueResponse) ([]byte, error) {
	if r.assignmentPageEncode != nil {
		page, err := r.assignmentPageEncode()
		if err != nil {
			return nil, err
		}
		if len(page) <= MaxSettlementAssignmentPageBytes {
			return encodeFrame(r, page)
		}
		r.AssignmentAfter = nil
	}
	if r.DeliveryBatch {
		return encodeClaimBatchResponse(r)
	}
	delivery := r.Result.Delivery
	frame, err := encodeFrameHeader(r, delivery.bodySize())
	if err != nil {
		return encodeFrame(failure(ErrCapacity), nil)
	}
	return delivery.appendBody(frame)
}

func validQueueName(name string) bool {
	return name != "" && len(name) <= 1024 && utf8.ValidString(name)
}

func (m *Machine) apply(c QueueCommand, wantReply bool, origin journalBodyOrigin) QueueResponse {
	switch c.Kind {
	case UpdateDeadLetter:
		return m.updateDeadLetter(*c.DeadLetter)
	case UpdateDeadLetterBatch:
		return m.applyDeadLetterUpdates(*c.DeadLetterUpdates)
	case ExpireSessions:
		if c.Deadline <= 0 {
			return failure(ErrInvalidCommand)
		}
		return QueueResponse{Count: m.expire(c.Deadline)}
	case ListQueues:
		if !wantReply {
			return QueueResponse{}
		}
		return m.list()
	case DeclareQueue, DescribeQueue, DeleteQueue, PurgeQueue, OpenQueueSession, RenewQueueSession,
		ReleaseQueueSession, PublishMessage, PublishDeadLetter, ClaimMessage, AckMessage, RequeueMessage, DropMessage, SettleBatch,
		PublishBatch, PublishDeadLetterBatch, ClaimBatch, AckAndClaim, SetAssignmentCredit:
	default:
		return failure(ErrInvalidCommand)
	}
	if !validQueueName(c.Queue) {
		return failure(ErrInvalidQueueName)
	}

	q := m.state.Queues[c.Queue]
	if c.Kind == DeclareQueue {
		return m.declare(c, q)
	}
	if q == nil {
		return failure(ErrQueueNotFound)
	}
	if c.Kind == DescribeQueue {
		return QueueResponse{Queue: q.info(c.Queue)}
	}
	usesCurrentQueue := (c.Kind == PublishMessage || c.Kind == PublishBatch || c.Kind == PublishDeadLetter || c.Kind == PublishDeadLetterBatch) && c.Incarnation == 0 && c.Session == 0
	if c.Incarnation != q.ID && !usesCurrentQueue {
		return failure(ErrQueueIncarnation)
	}

	switch c.Kind {
	case DeleteQueue:
		return m.delete(c, q)
	case PurgeQueue:
		return QueueResponse{Count: m.purge(q)}
	case OpenQueueSession:
		return m.open(c, q)
	case ReleaseQueueSession, RenewQueueSession:
		return m.updateLease(c, q)
	case PublishBatch:
		return m.publishBatch(q, c.Body, origin, c.packedVersion != 0)
	case PublishDeadLetter:
		return m.publishDeadLetter(c, q, origin)
	case PublishDeadLetterBatch:
		return m.applyDeadLetterPublishBatch(q, c, origin)
	case PublishMessage:
		if len(c.Body) > maxMessageBytes {
			return failure(ErrCapacity)
		}
		if c.Session == 0 {
			if c.Sequence != 0 {
				return failure(ErrInvalidSequence)
			}
			return m.publish(q, c.Body, origin, c.logicalSize)
		}
	}
	return m.operate(c, q, wantReply, origin)
}

func (m *Machine) declare(c QueueCommand, q *queueState) QueueResponse {
	spec := c.QueueSpec
	if len(spec.Arguments) > maxArgumentsBytes {
		return failure(ErrCapacity)
	}
	if spec.DeadLetter != nil && (!spec.Durable || !spec.DeadLetter.valid()) {
		return failure(ErrInvalidCommand)
	}
	if q != nil {
		old := q.Spec
		if old.Durable != spec.Durable || old.Exclusive != spec.Exclusive || old.AutoDelete != spec.AutoDelete ||
			!bytes.Equal(old.Arguments, spec.Arguments) || !sameDeadLetterPolicy(old.DeadLetter, spec.DeadLetter) {
			return failure(ErrQueueMismatch)
		}
		return QueueResponse{Queue: q.info(c.Queue)}
	}
	if len(m.state.Queues) >= maxQueues {
		return failure(ErrCapacity)
	}
	if m.state.LastQueue == math.MaxUint64 {
		return failure(ErrIDsExhausted)
	}
	m.state.LastQueue++
	q = &queueState{name: c.Queue, ID: m.state.LastQueue, Spec: spec}
	if m.state.Queues == nil {
		m.state.Queues = make(map[string]*queueState)
	}
	m.state.Queues[c.Queue] = q
	return QueueResponse{Queue: q.info(c.Queue)}
}

func (q *queueState) info(name string) QueueInfo {
	next := SessionID(0)
	if q.LastSession != math.MaxUint64 {
		next = q.LastSession + 1
	}
	return QueueInfo{
		Name: name, Incarnation: q.ID, NextSession: next, QueueSpec: q.Spec,
		Ready: uint64(len(q.Ready)), Consumers: q.consumers, Unacknowledged: q.unacked,
	}
}

func (m *Machine) delete(c QueueCommand, q *queueState) QueueResponse {
	if c.IfUnused && q.consumers != 0 {
		return failure(ErrQueueInUse)
	}
	if c.IfEmpty && (len(q.Ready) != 0 || q.unacked != 0) {
		return failure(ErrQueueNotEmpty)
	}
	count := m.purge(q) + q.unacked
	for _, sess := range q.Sessions {
		for _, message := range sess.Flights {
			m.removeBody(message)
		}
	}
	m.state.sessions -= uint64(len(q.Sessions))
	delete(m.state.Queues, c.Queue)
	return QueueResponse{Count: count}
}

func (m *Machine) purge(q *queueState) uint64 {
	count := uint64(len(q.Ready))
	for _, message := range q.Ready {
		m.removeBody(message)
	}
	q.Ready = nil
	return count
}

func (m *Machine) open(c QueueCommand, q *queueState) QueueResponse {
	if c.Deadline <= 0 || c.Owner == "" || len(c.Owner) > 128 || (c.Consumer && c.Prefetch == 0) {
		return failure(ErrInvalidCommand)
	}
	if c.Session == 0 {
		return failure(ErrSessionOrder)
	}
	if c.Session <= q.LastSession {
		prior := q.Sessions[c.Session]
		if prior == nil {
			return failure(ErrSessionClosed)
		}
		if prior.Owner != c.Owner || prior.Prefetch != c.Prefetch || prior.Consumer != c.Consumer ||
			prior.AutoAssign != c.AutoAssign {
			return failure(ErrSessionConflict)
		}
		return QueueResponse{Queue: q.info(c.Queue)}
	}
	if c.Session-q.LastSession != 1 {
		return failure(ErrSessionOrder)
	}
	if m.state.sessions >= maxSessions {
		return failure(ErrCapacity)
	}
	if q.Sessions == nil {
		q.Sessions = make(map[SessionID]*sessionState)
	}
	session := &sessionState{
		Owner: c.Owner, Deadline: c.Deadline, Prefetch: c.Prefetch, Consumer: c.Consumer, AutoAssign: c.AutoAssign,
	}
	q.Sessions[c.Session] = session
	q.LastSession = c.Session
	m.state.sessions++
	if c.Consumer {
		q.consumers++
	}
	if session.AutoAssign {
		session.RegisteredCredit = c.Prefetch
		q.addEligibleConsumer(c.Session)
		q.assignReady()
	}
	return QueueResponse{Queue: q.info(c.Queue)}
}

func (q *queueState) session(id SessionID, owner string) (*sessionState, error) {
	if id == 0 || id > q.LastSession {
		return nil, ErrUnknownSession
	}
	sess := q.Sessions[id]
	if sess == nil {
		return nil, ErrSessionClosed
	}
	if owner == "" || sess.Owner != owner {
		return nil, ErrSessionOwner
	}
	return sess, nil
}

func (m *Machine) updateLease(c QueueCommand, q *queueState) QueueResponse {
	sess, err := q.session(c.Session, c.Owner)
	if err != nil {
		if err == ErrSessionClosed && c.Kind == ReleaseQueueSession {
			return QueueResponse{}
		}
		return failure(err)
	}
	if c.Kind == ReleaseQueueSession {
		m.release(q, c.Session, sess)
		q.assignReady()
		return QueueResponse{}
	}
	if c.Deadline <= 0 {
		return failure(ErrInvalidCommand)
	}
	sess.Deadline = max(sess.Deadline, c.Deadline)
	return QueueResponse{}
}

func (m *Machine) release(q *queueState, id SessionID, sess *sessionState) {
	if sess.AutoAssign {
		q.removeEligibleConsumer(id)
	}
	attempts := make([]AttemptID, 0, len(sess.Flights))
	for attempt := range sess.Flights {
		attempts = append(attempts, attempt)
	}
	slices.Sort(attempts)
	for _, attempt := range attempts {
		q.requeue(sess.Flights[attempt])
	}
	q.unacked -= uint64(len(sess.Flights))
	if sess.Consumer {
		q.consumers--
	}
	delete(q.Sessions, id)
	if len(q.Sessions) == 0 {
		q.Sessions = nil
	}
	m.state.sessions--
}

func (m *Machine) queueNames() []string {
	names := make([]string, 0, len(m.state.Queues))
	for name := range m.state.Queues {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (m *Machine) list() QueueResponse {
	names := m.queueNames()
	out := make([]QueueInfo, 0, len(names))
	for _, name := range names {
		out = append(out, m.state.Queues[name].info(name))
	}
	return QueueResponse{Queues: out}
}

func (m *Machine) expire(cutoff int64) uint64 {
	if m.state.sessions == 0 {
		return 0
	}
	var count uint64
	for _, name := range m.queueNames() {
		q := m.state.Queues[name]
		ids := make([]SessionID, 0)
		for id, sess := range q.Sessions {
			if sess.Deadline <= cutoff {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			m.release(q, id, q.Sessions[id])
			count++
			if count == maxExpiryBatch {
				q.assignReady()
				return count
			}
		}
		q.assignReady()
	}
	return count
}

func (m *Machine) publish(q *queueState, body []byte, origin journalBodyOrigin, logical int) QueueResponse {
	return m.publishWithCredit(q, body, origin, logical, deadLetterPublishCredit{})
}

func (m *Machine) publishWithCredit(q *queueState, body []byte, origin journalBodyOrigin, logical int, credit deadLetterPublishCredit) QueueResponse {
	reserve := q.Spec.DeadLetter.reserve(q.name)
	envelopeSize := max(logical, len(body))
	if reserve != 0 && envelopeSize > maxMessageBytes-int(reserve) {
		return failure(ErrCapacity)
	}
	size := uint64(envelopeSize) + uint64(reserve)
	copyBytes, copySlots := deadLetterCopyCredit(size, reserve != 0)
	usedBytes := m.state.bodies + m.state.deadLetterCopyBytes
	usedSlots := m.state.messages + m.state.deadLetterCopySlots
	if (usedBytes+size+copyBytes > maxBodyBytes+credit.bodyBytes+credit.copyBytes ||
		usedSlots+1+copySlots > maxMessages+credit.bodySlots+credit.copySlots) &&
		!m.state.canMoveWithCredit(usedBytes, usedSlots, size, credit) {
		return failure(ErrCapacity)
	}
	if q.LastMessage == math.MaxUint64 {
		return failure(ErrIDsExhausted)
	}
	message, err := origin.message(q.LastMessage+1, body)
	if err != nil {
		return QueueResponse{storageErr: err}
	}
	q.LastMessage++
	message.DeadLetterReserve = reserve
	if logical > len(body) {
		message.LogicalSize = logical
		m.state.packingSavings += uint64(logical - len(body))
	}
	q.Ready = append(q.Ready, message)
	if message.cold != nil {
		m.state.coldBytes += uint64(len(body))
	}
	m.state.bodies += size
	m.state.deadLetterCopyBytes += copyBytes
	m.state.deadLetterCopySlots += copySlots
	m.state.messages++
	q.assignReady()
	return QueueResponse{Result: Result{MessageID: q.LastMessage}}
}

func (m *Machine) removeBody(message messageState) {
	size := uint64(message.logicalBodySize())
	m.state.bodies -= size + uint64(message.DeadLetterReserve)
	m.releaseDeadLetterCopyCredit(message)
	m.state.packingSavings -= size - uint64(message.bodySize())
	if message.cold != nil {
		m.state.coldBytes -= uint64(message.bodySize())
		message.releaseJournalSource()
	}
	m.state.messages--
}

func (m *Machine) operate(c QueueCommand, q *queueState, wantReply bool, origin journalBodyOrigin) QueueResponse {
	sess, err := q.session(c.Session, c.Owner)
	if err != nil {
		return failure(err)
	}
	if sess.AutoAssign && isPullCommand(c.Kind) || !sess.AutoAssign && c.Kind == SetAssignmentCredit {
		return failure(ErrInvalidCommand)
	}
	switch {
	case c.Sequence == 0:
		return failure(ErrInvalidSequence)
	case c.Sequence < sess.Sequence:
		return failure(ErrStaleSequence)
	case c.Sequence-sess.Sequence > 1:
		return failure(ErrSequenceGap)
	}
	key := commandKey{Kind: c.Kind}
	switch c.Kind {
	case PublishMessage:
		key.Hash = sha256.Sum256(c.Body)
	case AckMessage, RequeueMessage, DropMessage:
		key.Attempt = c.Attempt
	case SetAssignmentCredit:
		key.Hash = assignmentCreditKey(c.Prefetch)
	case SettleBatch:
		var ordered [1 + 8*MaxSettlementBatch]byte
		ordered[0] = byte(c.Settlement)
		for i, attempt := range c.Attempts {
			binary.BigEndian.PutUint64(ordered[1+8*i:], uint64(attempt))
		}
		key.Hash = sha256.Sum256(ordered[:1+8*len(c.Attempts)])
	case ClaimBatch:
		var limits [8]byte
		binary.BigEndian.PutUint32(limits[:4], c.ClaimLimit)
		binary.BigEndian.PutUint32(limits[4:], c.ClaimBytes)
		key.Hash = sha256.Sum256(limits[:])
	case AckAndClaim:
		var ordered [8 + 8*MaxSettlementBatch]byte
		binary.BigEndian.PutUint32(ordered[:4], c.ClaimLimit)
		binary.BigEndian.PutUint32(ordered[4:8], c.ClaimBytes)
		for i, attempt := range c.Attempts {
			binary.BigEndian.PutUint64(ordered[8+8*i:], uint64(attempt))
		}
		key.Hash = sha256.Sum256(ordered[:8+8*len(c.Attempts)])
	}
	previousAssignment := sess.AssignedThrough
	if c.Sequence == sess.Sequence {
		if key != sess.LastKey {
			return failure(ErrSequenceConflict)
		}
	} else {
		var result outcome
		if c.Kind == PublishMessage {
			reply := m.publish(q, c.Body, origin, 0)
			if reply.storageErr != nil {
				return reply
			}
			result = outcome{Message: reply.Result.MessageID, Code: reply.Code}
		} else {
			result = m.applyOperation(c, q, sess)
		}
		sess.Sequence = c.Sequence
		sess.LastKey = key
		sess.Last = result
		switch c.Kind {
		case AckMessage, RequeueMessage, DropMessage, SettleBatch, SetAssignmentCredit:
			if sess.AutoAssign {
				if len(sess.Flights) < int(sess.RegisteredCredit) {
					q.addEligibleConsumer(c.Session)
				} else {
					q.removeEligibleConsumer(c.Session)
				}
			}
			q.assignReady()
		}
	}
	if wantReply {
		reply := sess.outcome()
		if sess.AutoAssign && c.AssignmentAfter != nil && reply.Code == "" {
			after := max(previousAssignment, *c.AssignmentAfter)
			if sess.AssignedThrough > after {
				m.captureSettlementAssignment(c, q, after, &reply)
			}
		}
		return reply
	}
	return QueueResponse{}
}

func (sess *sessionState) outcome() QueueResponse {
	last := sess.Last
	if sess.LastKey.Kind == AckAndClaim {
		return sess.ackAndClaimOutcome(last)
	}
	if last.BatchCount != 0 {
		return sess.claimBatchOutcome(last)
	}
	if last.Attempt == 0 {
		return QueueResponse{Code: last.Code, Queue: QueueInfo{Ready: last.Ready}, Result: Result{MessageID: last.Message}, Count: last.Settled}
	}
	message := sess.Flights[last.Attempt]
	return QueueResponse{Queue: QueueInfo{Ready: last.Ready}, Result: Result{Delivery: message.delivery(last.Attempt)}}
}

func (m *Machine) applyOperation(c QueueCommand, q *queueState, sess *sessionState) outcome {
	switch c.Kind {
	case SetAssignmentCredit:
		sess.RegisteredCredit = c.Prefetch
		return outcome{}
	case ClaimMessage:
		result := q.claim(sess)
		result.Ready = uint64(len(q.Ready))
		return result
	case ClaimBatch:
		return q.claimBatch(c, sess)
	case AckAndClaim:
		return m.ackAndClaim(c, q, sess)
	case AckMessage, RequeueMessage, DropMessage:
		if err := m.settle(q, sess, c.Kind, c.Attempt, c.RejectedAt); err != nil {
			return outcome{Code: errorsToCode[err]}
		}
		return outcome{}
	case SettleBatch:
		for i, attempt := range c.Attempts {
			if err := m.settle(q, sess, c.Settlement, attempt, c.RejectedAt); err != nil {
				return outcome{Settled: uint64(i), Code: errorsToCode[err]}
			}
		}
		return outcome{Settled: uint64(len(c.Attempts))}
	default:
		return outcome{Code: errorsToCode[ErrInvalidCommand]}
	}
}

func (m *Machine) settle(q *queueState, sess *sessionState, kind QueueCommandKind, attempt AttemptID, rejectedAt int64) error {
	old, ok := sess.Flights[attempt]
	if !ok {
		return ErrUnknownAttempt
	}
	deadLetter := kind == DropMessage && q.Spec.DeadLetter != nil
	if deadLetter {
		if rejectedAt <= 0 {
			return ErrInvalidCommand
		}
		if m.state.LastDeadLetter == math.MaxUint64 {
			return ErrIDsExhausted
		}
	}
	delete(sess.Flights, attempt)
	if len(sess.Flights) == 0 {
		sess.Flights = nil
	}
	q.unacked--
	if kind == RequeueMessage {
		q.requeue(old)
	} else if deadLetter {
		m.stageDeadLetter(q, old, rejectedAt)
	} else {
		m.removeBody(old)
	}
	return nil
}

func (q *queueState) claim(sess *sessionState) outcome {
	credit := sess.Prefetch
	if sess.AutoAssign {
		credit = sess.RegisteredCredit
	}
	switch {
	case uint64(len(sess.Flights)) >= uint64(credit):
		return outcome{Code: errorsToCode[ErrNoCredit]}
	case len(q.Ready) == 0:
		return outcome{Code: errorsToCode[ErrEmpty]}
	case q.LastAttempt == math.MaxUint64:
		return outcome{Code: errorsToCode[ErrIDsExhausted]}
	}
	next := q.Ready[0]
	q.Ready[0] = messageState{}
	q.Ready = q.Ready[1:]
	if len(q.Ready) == 0 {
		q.Ready = nil
	}
	if sess.Flights == nil {
		sess.Flights = make(map[AttemptID]messageState)
	}
	q.LastAttempt++
	q.unacked++
	sess.Flights[q.LastAttempt] = next
	return outcome{Attempt: q.LastAttempt}
}

func (q *queueState) requeue(old messageState) {
	old.Redelivered = true
	q.Ready = append(q.Ready, old)
}
