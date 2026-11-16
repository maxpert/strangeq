package clusterqueue

import (
	"bytes"
	"crypto/sha256"
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
	snapshotVersion   = 1
)

type Machine struct {
	state machineState
}

type machineState struct {
	LastQueue QueueID
	Queues    map[string]*queueState
	bodies    uint64
	messages  uint64
	sessions  uint64
}

type queueState struct {
	ID          QueueID
	Spec        QueueSpec
	LastSession SessionID
	LastMessage MessageID
	LastAttempt AttemptID
	Ready       []messageState
	Sessions    map[SessionID]*sessionState
	unacked     uint64
	consumers   uint64
}

type sessionState struct {
	Owner    string
	Prefetch uint32
	Consumer bool
	Deadline int64
	Sequence uint64
	LastKey  commandKey
	Last     outcome
	Flights  map[AttemptID]messageState
}

type messageState struct {
	ID          MessageID
	Body        []byte `json:",omitempty"`
	Redelivered bool   `json:",omitempty"`
}

type commandKey struct {
	Kind    QueueCommandKind
	Attempt AttemptID         `json:",omitempty"`
	Hash    [sha256.Size]byte `json:",omitzero"`
}

type outcome struct {
	Message MessageID `json:",omitempty"`
	Attempt AttemptID `json:",omitempty"`
	Code    string    `json:",omitempty"`
	Ready   uint64    `json:",omitempty"`
}

// Apply executes one committed queue command. The caller serializes machine
// methods in log order for each replica.
func (m *Machine) Apply(data []byte) []byte {
	c, err := DecodeCommand(data)
	var r QueueResponse
	if err != nil {
		r = failure(ErrInvalidCommand)
	} else {
		r = m.apply(c)
	}
	reply, err := encodeFrame(r, r.Result.Delivery.Body)
	if err != nil {
		reply, _ = encodeFrame(failure(ErrCapacity), nil)
	}
	return reply
}

func validQueueName(name string) bool {
	return name != "" && len(name) <= 1024 && utf8.ValidString(name)
}

func (m *Machine) apply(c QueueCommand) QueueResponse {
	switch c.Kind {
	case ExpireSessions:
		if c.Deadline <= 0 {
			return failure(ErrInvalidCommand)
		}
		return QueueResponse{Count: m.expire(c.Deadline)}
	case ListQueues:
		return m.list()
	case DeclareQueue, DescribeQueue, DeleteQueue, PurgeQueue, OpenQueueSession, RenewQueueSession,
		ReleaseQueueSession, PublishMessage, ClaimMessage, AckMessage, RequeueMessage, DropMessage:
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
	if c.Incarnation != q.ID && !(c.Kind == PublishMessage && c.Incarnation == 0 && c.Session == 0) {
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
	case PublishMessage:
		if len(c.Body) > maxMessageBytes {
			return failure(ErrCapacity)
		}
		if c.Session == 0 {
			if c.Sequence != 0 {
				return failure(ErrInvalidSequence)
			}
			return m.publish(q, c.Body)
		}
	}
	return m.operate(c, q)
}

func (m *Machine) declare(c QueueCommand, q *queueState) QueueResponse {
	spec := c.QueueSpec
	if len(spec.Arguments) > maxArgumentsBytes {
		return failure(ErrCapacity)
	}
	if q != nil {
		old := q.Spec
		if old.Durable != spec.Durable || old.Exclusive != spec.Exclusive || old.AutoDelete != spec.AutoDelete ||
			!bytes.Equal(old.Arguments, spec.Arguments) {
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
	q = &queueState{ID: m.state.LastQueue, Spec: spec}
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
		if prior.Owner != c.Owner || prior.Prefetch != c.Prefetch || prior.Consumer != c.Consumer {
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
	q.Sessions[c.Session] = &sessionState{
		Owner: c.Owner, Deadline: c.Deadline, Prefetch: c.Prefetch, Consumer: c.Consumer,
	}
	q.LastSession = c.Session
	m.state.sessions++
	if c.Consumer {
		q.consumers++
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
		return QueueResponse{}
	}
	if c.Deadline <= 0 {
		return failure(ErrInvalidCommand)
	}
	sess.Deadline = max(sess.Deadline, c.Deadline)
	return QueueResponse{}
}

func (m *Machine) release(q *queueState, id SessionID, sess *sessionState) {
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
				return count
			}
		}
	}
	return count
}

func (m *Machine) publish(q *queueState, body []byte) QueueResponse {
	size := uint64(len(body))
	if m.state.bodies+size > maxBodyBytes || m.state.messages >= maxMessages {
		return failure(ErrCapacity)
	}
	if q.LastMessage == math.MaxUint64 {
		return failure(ErrIDsExhausted)
	}
	q.LastMessage++
	q.Ready = append(q.Ready, messageState{ID: q.LastMessage, Body: bytes.Clone(body)})
	m.state.bodies += size
	m.state.messages++
	return QueueResponse{Result: Result{MessageID: q.LastMessage}}
}

func (m *Machine) removeBody(message messageState) {
	m.state.bodies -= uint64(len(message.Body))
	m.state.messages--
}

func (m *Machine) operate(c QueueCommand, q *queueState) QueueResponse {
	sess, err := q.session(c.Session, c.Owner)
	if err != nil {
		return failure(err)
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
	}
	if c.Sequence == sess.Sequence {
		if key != sess.LastKey {
			return failure(ErrSequenceConflict)
		}
		return sess.outcome()
	}
	sess.Sequence = c.Sequence
	sess.LastKey = key
	sess.Last = m.applyOperation(c, q, sess)
	return sess.outcome()
}

func (sess *sessionState) outcome() QueueResponse {
	last := sess.Last
	if last.Attempt == 0 {
		return QueueResponse{Code: last.Code, Queue: QueueInfo{Ready: last.Ready}, Result: Result{MessageID: last.Message}}
	}
	message := sess.Flights[last.Attempt]
	return QueueResponse{Queue: QueueInfo{Ready: last.Ready}, Result: Result{Delivery: Delivery{
		MessageID: message.ID, Attempt: last.Attempt, Body: message.Body, Redelivered: message.Redelivered,
	}}}
}

func (m *Machine) applyOperation(c QueueCommand, q *queueState, sess *sessionState) outcome {
	switch c.Kind {
	case PublishMessage:
		result := m.publish(q, c.Body)
		return outcome{Message: result.Result.MessageID, Code: result.Code}
	case ClaimMessage:
		result := q.claim(sess)
		result.Ready = uint64(len(q.Ready))
		return result
	case AckMessage, RequeueMessage, DropMessage:
		old, ok := sess.Flights[c.Attempt]
		if !ok {
			return outcome{Code: errorsToCode[ErrUnknownAttempt]}
		}
		delete(sess.Flights, c.Attempt)
		if len(sess.Flights) == 0 {
			sess.Flights = nil
		}
		q.unacked--
		if c.Kind == RequeueMessage {
			q.requeue(old)
		} else {
			m.removeBody(old)
		}
		return outcome{}
	default:
		return outcome{Code: errorsToCode[ErrInvalidCommand]}
	}
}

func (q *queueState) claim(sess *sessionState) outcome {
	switch {
	case uint64(len(sess.Flights)) >= uint64(sess.Prefetch):
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
