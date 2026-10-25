package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/maxpert/amqp-go/broker"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/transaction"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (s *Server) processBasicMethod(conn *protocol.Connection, channelID uint16, methodID uint16, payload []byte) error {
	switch methodID {
	case protocol.BasicQos: // Method ID 10 for basic class
		return s.handleBasicQos(conn, channelID, payload)
	case protocol.BasicPublish: // Method ID 40 for basic class
		return s.handleBasicPublish(conn, channelID, payload)
	case protocol.BasicConsume: // Method ID 20 for basic class
		return s.handleBasicConsume(conn, channelID, payload)
	case protocol.BasicCancel: // Method ID 30 for basic class
		return s.handleBasicCancel(conn, channelID, payload)
	case protocol.BasicGet: // Method ID 70 for basic class
		return s.handleBasicGet(conn, channelID, payload)
	case protocol.BasicAck: // Method ID 80 for basic class
		return s.handleBasicAck(conn, channelID, payload)
	case protocol.BasicReject: // Method ID 90 for basic class
		return s.handleBasicReject(conn, channelID, payload)
	case protocol.BasicNack: // Method ID 120 for basic class
		return s.handleBasicNack(conn, channelID, payload)
	case protocol.BasicRecover: // Method ID 110 for basic class
		return s.handleBasicRecover(conn, channelID, payload)
	default:
		s.Log.Warn("Unknown basic method ID",
			zap.Uint16("method_id", methodID),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.FrameError,
			fmt.Sprintf("FRAME_ERROR - unknown method 60.%d", methodID), 60, methodID)
	}
}

// handleBasicQos handles the basic.qos method
func (s *Server) handleBasicQos(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.qos method
	qosMethod := &protocol.BasicQosMethod{}
	err := qosMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.qos",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 10)
	}

	s.Log.Debug("Basic QoS settings",
		zap.Uint32("prefetch_size", qosMethod.PrefetchSize),
		zap.Uint16("prefetch_count", qosMethod.PrefetchCount),
		zap.Bool("global", qosMethod.Global))

	if qosMethod.PrefetchSize != 0 {
		// RabbitMQ rabbit_channel raises protocol_error(not_implemented,
		// "prefetch_size!=0"), and not_implemented (540) is a HARD error in the
		// AMQP reply-code table: connection.close, never channel.close.
		return s.connectionError(conn, amqperrors.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - prefetch_size!=0 (%d)", qosMethod.PrefetchSize), 60, 10)
	}

	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 60, 10)
	}
	channel := value.(*protocol.Channel)

	// Update channel prefetch settings
	channel.Mutex.Lock()
	channel.PrefetchCount = qosMethod.PrefetchCount
	channel.PrefetchSize = qosMethod.PrefetchSize
	channel.GlobalPrefetch = qosMethod.Global
	// Collect the broker-internal identities, not the wire tags: the broker's
	// consumer map is keyed by identity, and a wire tag is only unique within
	// this channel — re-gating by tag would re-gate whichever same-tagged
	// consumer registered last, on any connection.
	consumerIDs := make([]string, 0, len(channel.Consumers))
	for _, consumer := range channel.Consumers {
		consumerIDs = append(consumerIDs, consumer.ID)
	}
	channel.Mutex.Unlock()

	for _, id := range consumerIDs {
		s.Broker.UpdateConsumerPrefetch(id, qosMethod.PrefetchCount)
	}

	return s.sendBasicQosOK(conn, channelID)
}

// handleBasicPublish handles the basic.publish method
func (s *Server) handleBasicPublish(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.publish method
	publishMethod := protocol.GetBasicPublishMethod()
	err := publishMethod.Deserialize(payload)
	if err != nil {
		protocol.PutBasicPublishMethod(publishMethod)
		s.Log.Error("Failed to deserialize basic.publish",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 40)
	}

	if publishMethod.Immediate {
		protocol.PutBasicPublishMethod(publishMethod)
		return s.connectionError(conn, amqperrors.NotImplemented, "NOT_IMPLEMENTED - immediate=true", 60, 40)
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Basic publish received"); ce != nil {
		ce.Write(
			zap.String("exchange", publishMethod.Exchange),
			zap.String("routing_key", publishMethod.RoutingKey),
			zap.Bool("mandatory", publishMethod.Mandatory),
			zap.Bool("immediate", publishMethod.Immediate))
	}

	// Authorization check: basic.publish requires write permission on exchange
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionWrite,
		ResourceType: interfaces.ResourceExchange,
		Resource:     publishMethod.Exchange,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 40)
	}

	// SQ-12 resource alarm: when a memory/disk alarm is active, RabbitMQ blocks
	// publishers but keeps the connection alive. The actual backpressure is the
	// per-connection reader-pause (server.go), which defers *subsequent* reads;
	// this in-flight publish still completes. Read the global alarm bitmask (one
	// atomic load) rather than the now-retired per-connection depth flag.
	if s.alarmState.Load() != 0 {
		// Informational only — the reader-pause throttles new publishes; this one
		// proceeds so it (and any confirm) settles normally.
		s.Log.Warn("resource alarm active; publisher throttled via reader-pause but allowing in-flight publish",
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID),
			zap.String("exchange", publishMethod.Exchange))
	}

	// Create a pending message to track this publication
	// The full message will be completed when we receive the header and body frames
	var channelRef *protocol.Channel
	if value, ok := conn.Channels.Load(channelID); ok {
		channelRef = value.(*protocol.Channel)
	}
	pendingMsg := protocol.GetPendingMessage()
	pendingMsg.Method = publishMethod
	pendingMsg.Channel = channelRef

	// Store the pending message for this channel
	conn.PendingMessages[channelID] = pendingMsg

	if ce := s.Log.Check(zapcore.DebugLevel, "Started tracking pending message"); ce != nil {
		ce.Write(zap.Uint16("channel_id", channelID), zap.String("connection_id", conn.ID))
	}

	return nil
}

// processHeaderFrame processes content header frames
func (s *Server) processHeaderFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Check if there's a pending message for this channel that needs a header
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		s.Log.Warn("Header frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.UnexpectedFrame,
			"UNEXPECTED_FRAME - content header without basic.publish", 60, 0)
	}

	// Parse the content header
	contentHeader, err := protocol.ReadContentHeader(frame)
	if err != nil {
		s.Log.Error("Failed to parse content header",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel", frame.Channel))
		return s.malformed(conn, err, 60, 0)
	}

	// Attach the header to the pending message
	pendingMsg.Header = contentHeader

	// Defense-in-depth: reject oversized messages before allocation.
	// contentHeader.BodySize is a client-controlled uint64 — without this
	// check, make([]byte, 0, BodySize) can OOM the broker with a single frame.
	if s.Config != nil && contentHeader.BodySize > uint64(s.Config.Server.MaxMessageSize) {
		s.Log.Warn("Message body size exceeds MaxMessageSize, rejecting",
			zap.Uint64("body_size", contentHeader.BodySize),
			zap.Int64("max_message_size", s.Config.Server.MaxMessageSize),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", frame.Channel))
		// 406 precondition-failed is RabbitMQ ≥3.8 parity for body-too-large
		// (506 resource-error is a hard code and would kill the connection).
		// The pending publish is dropped by teardown.
		return s.channelError(conn, frame.Channel, amqperrors.PreconditionFailed,
			fmt.Sprintf("PRECONDITION_FAILED - message size %d is larger than configured max size %d", contentHeader.BodySize, s.Config.Server.MaxMessageSize),
			60, 40)
	}

	// Leave pendingMsg.Body nil here. The allocation decision is deferred to the
	// FIRST body frame (processBodyFrame): a body that arrives in a single frame
	// is donated (zero-copy) from the frame payload, and only a multi-frame body
	// pre-allocates a BodySize-capacity buffer to assemble into. Pre-allocating
	// here would force a copy even for the common single-frame case.

	if ce := s.Log.Check(zapcore.DebugLevel, "Content header received for pending message"); ce != nil {
		ce.Write(
			zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey),
			zap.Uint64("body_size", contentHeader.BodySize))
	}

	// For empty messages (body_size = 0), no body frame will be sent
	// We should process the complete message immediately
	if contentHeader.BodySize == 0 {
		if ce := s.Log.Check(zapcore.DebugLevel, "Empty message detected - processing immediately"); ce != nil {
			ce.Write(zap.String("exchange", pendingMsg.Method.Exchange), zap.String("routing_key", pendingMsg.Method.RoutingKey))
		}

		delete(conn.PendingMessages, frame.Channel)

		err := s.processCompleteMessage(conn, frame.Channel, pendingMsg)
		protocol.PutBasicPublishMethod(pendingMsg.Method)
		protocol.PutContentHeader(pendingMsg.Header)
		protocol.PutPendingMessage(pendingMsg)
		if err != nil {
			s.Log.Error("Failed to process complete empty message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}
	}

	return nil
}

// processBodyFrame processes content body frames
func (s *Server) processBodyFrame(conn *protocol.Connection, frame *protocol.Frame) error {
	// Check if there's a pending message for this channel that needs body content
	pendingMsg, exists := conn.PendingMessages[frame.Channel]
	if !exists {
		s.Log.Warn("Body frame received for channel with no pending message",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.UnexpectedFrame,
			"UNEXPECTED_FRAME - content body without basic.publish", 60, 0)
	}

	// Check if we have a header for this message
	if pendingMsg.Header == nil {
		s.Log.Warn("Body frame received before header frame",
			zap.Uint16("channel", frame.Channel),
			zap.String("connection_id", conn.ID))
		return s.connectionError(conn, amqperrors.UnexpectedFrame,
			"UNEXPECTED_FRAME - content body before header", 60, 0)
	}

	// Assemble the body. On the FIRST body frame, if this single frame already
	// carries the entire declared body, DONATE the frame payload directly to
	// message.Body (zero copy) instead of copying it into a fresh buffer — this
	// is the common case for a body that fits in one frame (e.g. a 64KB body
	// under the default 128KB frame-max). A multi-frame body falls back to the
	// make+append assemble path.
	//
	// OWNERSHIP CONTRACT for the donated buffer: after donation, message.Body is
	// the SOLE owner of the (former) frame.Payload backing array. This is safe
	// because (a) the frame reader make()s a FRESH payload per frame
	// (protocol.ReadFrameOptimizedWithLimit) and never reuses that array for a
	// later read, and (b) PutFrame / PutPendingMessage only NIL their references
	// to it — they never recycle the backing []byte (see the ALIASING INVARIANT
	// in processCompleteMessage and PutPendingMessage). So when the frame is
	// pooled right after processFrame returns, it nils a reference the message no
	// longer needs; the array lives, owned solely by the *Message, through the
	// async WAL read, ring residency, and delivery, and is freed by GC only after
	// the message is fully done. The donated payload is therefore NEVER returned
	// to any pool.
	if pendingMsg.Body == nil {
		if uint64(len(frame.Payload)) == pendingMsg.Header.BodySize {
			pendingMsg.Body = frame.Payload // DONATE: single-frame body, zero copy
		} else {
			pendingMsg.Body = make([]byte, 0, pendingMsg.Header.BodySize)
			pendingMsg.Body = append(pendingMsg.Body, frame.Payload...) // multi-frame: assemble
		}
	} else {
		pendingMsg.Body = append(pendingMsg.Body, frame.Payload...) // subsequent frames
	}
	pendingMsg.Received += uint64(len(frame.Payload))

	if ce := s.Log.Check(zapcore.DebugLevel, "Body frame received for pending message"); ce != nil {
		ce.Write(zap.Uint16("channel", frame.Channel),
			zap.Uint64("received", pendingMsg.Received),
			zap.Uint64("expected", pendingMsg.Header.BodySize))
	}

	// Check if we've received the complete message
	if pendingMsg.Received >= pendingMsg.Header.BodySize {
		// We have received all the body data
		if pendingMsg.Received > pendingMsg.Header.BodySize {
			s.Log.Error("Received more body data than expected",
				zap.Uint64("received", pendingMsg.Received),
				zap.Uint64("expected", pendingMsg.Header.BodySize))
			return s.connectionError(conn, amqperrors.UnexpectedFrame,
				"UNEXPECTED_FRAME - content body exceeds declared size", 60, 0)
		}

		// Finalize the message by trimming any extra bytes
		if pendingMsg.Received > uint64(len(pendingMsg.Body)) {
			// This shouldn't happen, but just in case
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Header.BodySize]
		} else if pendingMsg.Received < uint64(len(pendingMsg.Body)) {
			pendingMsg.Body = pendingMsg.Body[:pendingMsg.Received]
		}

		// Remove the pending message from the map before processing
		delete(conn.PendingMessages, frame.Channel)

		err := s.processCompleteMessage(conn, frame.Channel, pendingMsg)
		protocol.PutBasicPublishMethod(pendingMsg.Method)
		protocol.PutContentHeader(pendingMsg.Header)
		protocol.PutPendingMessage(pendingMsg)
		if err != nil {
			s.Log.Error("Failed to process complete message",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel", frame.Channel))
			return err
		}

		return nil
	}

	return nil
}

// processCompleteMessage processes a message that has been fully received (method + header + body)
func (s *Server) processCompleteMessage(conn *protocol.Connection, channelID uint16, pendingMsg *protocol.PendingMessage) error {
	// SQ-12: mark this connection as a publisher so the reader-pause applies to
	// it (consumer-only connections are never paused). Load-guarded so it is a
	// single relaxed load per publish in steady state, storing only once.
	if !conn.HasPublished.Load() {
		conn.HasPublished.Store(true)
	}

	// SQ-12 publish gate: a single atomic load with a not-taken branch when no
	// resource alarm is active (the common case), preserving the unset zero-cost
	// contract. When an alarm IS active this publish still completes and (in
	// confirm mode) is confirmed — the reader-pause defers only the *subsequent*
	// unread publishes (the one-publish-plus-in-flight slack of the
	// readFrames/processFrames goroutine split). We count it as a
	// published-while-blocked observation but never withhold its ack, and tell an
	// opted-in client it is now blocked (this is the point a connection becomes
	// blocked in RabbitMQ — when it publishes under an active alarm).
	if bits := s.alarmState.Load(); bits != 0 {
		s.publishedWhileBlocked.Add(1)
		s.blockPublisherConnection(conn, bits)
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Processing complete message"); ce != nil {
		ce.Write(zap.String("exchange", pendingMsg.Method.Exchange),
			zap.String("routing_key", pendingMsg.Method.RoutingKey),
			zap.Uint64("body_size", uint64(len(pendingMsg.Body))))
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Creating message from pending message"); ce != nil {
		ce.Write(zap.String("content_type", pendingMsg.Header.ContentType),
			zap.Int("body_size", len(pendingMsg.Body)),
			zap.Uint16("property_flags", pendingMsg.Header.PropertyFlags))
	}

	// ALIASING INVARIANT: Message.Body and Message.Headers are zero-copy
	// aliases to pendingMsg.Body and pendingMsg.Header.Headers respectively.
	// After PublishMessage stores the message, the caller (processHeaderFrame
	// or processBodyFrame) recycles the PendingMessage and ContentHeader via
	// PutPendingMessage / PutContentHeader. Those pool Put helpers MUST only
	// nil out the references — they MUST NOT recycle the backing []byte or
	// map storage — because the stored delivery retains these aliases.
	// Violating this invariant would corrupt every stored message body and
	// header map on the next pool Get.
	message := &protocol.Message{
		Body:            pendingMsg.Body,
		Headers:         pendingMsg.Header.Headers,
		Exchange:        pendingMsg.Method.Exchange,
		RoutingKey:      pendingMsg.Method.RoutingKey,
		ContentType:     pendingMsg.Header.ContentType,
		ContentEncoding: pendingMsg.Header.ContentEncoding,
		DeliveryMode:    pendingMsg.Header.DeliveryMode,
		Priority:        pendingMsg.Header.Priority,
		CorrelationID:   pendingMsg.Header.CorrelationID,
		ReplyTo:         pendingMsg.Header.ReplyTo,
		Expiration:      pendingMsg.Header.Expiration,
		MessageID:       pendingMsg.Header.MessageID,
		Timestamp:       pendingMsg.Header.Timestamp,
		Type:            pendingMsg.Header.Type,
		UserID:          pendingMsg.Header.UserID,
		AppID:           pendingMsg.Header.AppID,
		ClusterID:       pendingMsg.Header.ClusterID,
		Mandatory:       pendingMsg.Method.Mandatory,
	}

	if message.UserID != "" && message.UserID != conn.Username {
		return s.channelError(conn, channelID, amqperrors.PreconditionFailed,
			fmt.Sprintf("PRECONDITION_FAILED - user_id property set to '%s' but authenticated user was '%s'", message.UserID, conn.Username), 60, 40)
	}

	// Assign confirm delivery tag if channel is in confirm mode (before
	// transactional check so the tag is sequential across all publishes).
	var confirmTag uint64
	var confirmCh *protocol.Channel
	if val, ok := conn.Channels.Load(channelID); ok {
		ch := val.(*protocol.Channel)
		if ch.ConfirmMode.Load() {
			confirmTag = ch.ConfirmSequence.Add(1)
			confirmCh = ch
		}
	}

	// Buffer into transaction if channel is in transactional mode (conn-scoped,
	// D-NEW-1)
	if txm := s.txm(conn); txm != nil && txm.IsTransactional(channelID) {
		// WS-D §11.2 (R11 check_internal_exchange runs BEFORE tx buffering, in
		// tx and non-tx mode alike): a publish to a missing/internal exchange
		// fails NOW with the channel error, not at commit.
		if sb, ok := s.Broker.(*StorageBrokerAdapter); ok {
			if verr := sb.broker.ValidatePublishTarget(message.Exchange); verr != nil {
				return s.closeChannelWithBrokerError(conn, channelID, verr, 60, 40)
			}
		}
		op := transaction.NewPublishOperation(message.Exchange, message.RoutingKey, message)
		if confirmTag > 0 {
			op.DeliveryTag = confirmTag
		}
		return txm.AddOperation(channelID, op)
	}

	var publishStart time.Time
	if s.MetricsCollector != nil {
		publishStart = time.Now()
	}

	// Iteration 2: durable and/or publisher-confirm publishes take the async
	// completion path — the durable WAL write is enqueued for group commit and
	// the confirm fires from the fsync completion, so this frame processor is
	// never blocked on an fsync. The transient, non-confirm hot path is
	// byte-for-byte unchanged (plain synchronous Broker.PublishMessage).
	var err error
	var durableInflight bool
	var backpressure protocol.DepthGate
	if confirmTag > 0 || message.DeliveryMode == 2 {
		var onDurable func(error)
		if confirmTag > 0 {
			cc := confirmCh
			ct := confirmTag
			onDurable = func(derr error) {
				// Runs on the WAL batch-writer goroutine (durable copies) — folds
				// the tag and pokes the flusher; never blocks / never does I/O.
				if derr != nil {
					s.failConfirmDurable(conn, cc, ct)
				} else {
					s.markConfirmDurable(conn, cc, ct)
				}
			}
		}
		// WS-I Phase B: onReleased is wired in the lease-transfer commit; nil
		// until then (treated as a no-op by the broker).
		durableInflight, backpressure, err = s.Broker.PublishMessageAsyncConfirm(message.Exchange, message.RoutingKey, message, onDurable, nil)
	} else {
		err = s.Broker.PublishMessage(message.Exchange, message.RoutingKey, message)
	}

	// Iteration 2 (option A): arm queue-depth reader backpressure. The async
	// durable path admits every publish (it never parks the processor — that would
	// strand the already-minted confirm tag at teardown), so pacing happens
	// UPSTREAM at the reader: when a routed target queue is at/over its high-water
	// mark, stop pulling NEW publishes off the socket (TCP backpressure) until a
	// consumer drains it. This paces the producer to the consumer's drain rate, so
	// the durable queue stays small and ring-resident (fast consume). Gated to the
	// rising edge and to PRODUCER-ONLY connections — a connection with a consumer
	// must keep reading to deliver its own acks (which drain the very queue), so
	// pausing its reader could deadlock. The reader clears the gate once the queue
	// drains below HWM.
	if backpressure != nil && !conn.DepthBackpressure.Load() && !conn.HasActiveConsumer() {
		conn.SetDepthGate(backpressure)
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordPublishLatency(time.Since(publishStart).Seconds())
	}

	if err != nil {
		if errors.Is(err, broker.ErrNoRoute) {
			if s.MetricsCollector != nil {
				s.MetricsCollector.RecordMessageUnroutable()
			}
			if retErr := s.sendBasicReturn(conn, channelID, 312, "NO_ROUTE", message.Exchange, message.RoutingKey, message); retErr != nil {
				return retErr
			}
			if confirmTag > 0 {
				// The basic.return frames are already written; folding the tag
				// only now guarantees no batched ack covering this tag can precede
				// the return on the wire.
				return s.confirmPublishDurable(conn, confirmCh, confirmTag)
			}
			return nil
		}
		if errors.Is(err, broker.ErrMaxLengthExceeded) {
			// SQ-11 x-overflow=reject-publish: a target queue refused the
			// message because it is at/over its x-max-length / x-max-length-bytes
			// limit. The broker neither stored nor dead-lettered it. Signal the
			// publisher, in priority order:
			//   - confirm mode: settle the confirm tag as a basic.nack the
			//     contiguous ack watermark can never re-confirm (deferred behind
			//     any still-pipelined durable predecessor; see settleRejectConfirm),
			//   - else mandatory: return the refused message so a non-confirm
			//     publisher gets feedback (StrangeQ extension — RabbitMQ does not
			//     return a routable-but-full message; see W7 divergence notes),
			//   - else: silently drop (RabbitMQ parity).
			if confirmTag > 0 {
				return s.settleRejectConfirm(conn, confirmCh, confirmTag)
			}
			if message.Mandatory {
				return s.sendBasicReturn(conn, channelID, 312, "NO_ROUTE", message.Exchange, message.RoutingKey, message)
			}
			return nil
		}
		if errors.Is(err, broker.ErrQueueClosed) {
			// A concurrent queue.delete tore the target queue down after routing
			// resolved its binding. The broker neither stored nor dead-lettered
			// the message, so it must never be confirmed — but the connection is
			// healthy and must not be torn down. Same priority order as the
			// max-length rejection above:
			//   - confirm mode: settle the tag as a basic.nack the contiguous ack
			//     watermark can never re-confirm,
			//   - else mandatory: return the message to the publisher,
			//   - else: silently drop.
			if confirmTag > 0 {
				return s.settleRejectConfirm(conn, confirmCh, confirmTag)
			}
			if message.Mandatory {
				return s.sendBasicReturn(conn, channelID, 312, "NO_ROUTE", message.Exchange, message.RoutingKey, message)
			}
			return nil
		}
		s.Log.Error("Failed to route message",
			zap.Error(err),
			zap.String("exchange", message.Exchange),
			zap.String("routing_key", message.RoutingKey))
		// A confirm tag was assigned for this publish; a generic routing/store
		// error would otherwise leave it un-settled, STALLING the per-channel
		// contiguous confirm fold (confirmContig never crosses this tag, so every
		// later confirm on the channel buffers in confirmAhead forever). Settle it
		// as a nack so the fold advances and the publisher learns the publish
		// failed. Best-effort: the connection is likely torn down on the returned
		// error, but the settle keeps the fold consistent if it is not.
		if confirmTag > 0 {
			_ = s.settleRejectConfirm(conn, confirmCh, confirmTag)
		}
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessagePublished(len(message.Body))
	}

	s.messagesPublished.Add(1)
	s.bytesReceived.Add(int64(len(message.Body)))

	if ce := s.Log.Check(zapcore.DebugLevel, "Message successfully routed"); ce != nil {
		ce.Write(zap.String("exchange", message.Exchange), zap.String("routing_key", message.RoutingKey))
	}

	if confirmTag > 0 && !durableInflight {
		// Transient-confirm (or unroutable-non-mandatory) success settled
		// synchronously: fold the tag and flush inline (idle) or via the
		// connection's confirm flusher (pipelined). A durableInflight publish
		// instead fires its confirm from the async fsync completion (onDurable).
		return s.confirmPublishDurable(conn, confirmCh, confirmTag)
	}

	return nil
}

// generatedTagDraws bounds the re-draw loop in claimConsumerTag. Each draw is
// 128 bits of crypto/rand, so a single collision with an occupied tag is
// already vanishingly unlikely and eight consecutive ones are impossible in
// practice — the bound exists so a degraded entropy source (crypto/rand
// failing into the counter-based fallbackID, which a squatting client could in
// principle predict) cannot spin this loop forever. Exhausting it is reported
// as a duplicate-tag error rather than silently overwriting a live consumer.
const generatedTagDraws = 8

// claimConsumerTag atomically claims a consumer tag on a channel and installs
// the consumer under it, leaving the claimed tag in consumer.Tag. It reports
// false if the tag is already in use on this channel (the 530 not-allowed
// case), in which case nothing was installed and consumer.Tag is untouched.
//
// consumer.Tag == "" means the client asked the server to generate a tag
// (AMQP 0-9-1 basic.consume, consumer-tag: "If this field is empty the server
// will generate a unique tag"). The generated tag is drawn from
// s.consumerTagGenerator() and CHECKED against
// the channel's live tags inside the same critical section that installs it,
// so it cannot collide with a client-supplied tag — the property is asserted
// at runtime rather than assumed from the width of the random draw.
//
// PUBLICATION ORDER: consumer.Tag is assigned HERE, under the channel mutex,
// BEFORE the consumer is inserted into channel.Consumers. Inserting first and
// stamping the tag afterwards at the call site is a data race: the insert
// publishes the consumer to the connection's delivery loop, which reads
// consumer.Tag under the same mutex (discoverConsumers), so any write after the
// insert races the reader — caught by -race on the first certification run.
func (s *Server) claimConsumerTag(channel *protocol.Channel, consumer *protocol.Consumer) bool {
	channel.Mutex.Lock()
	defer channel.Mutex.Unlock()

	if tag := consumer.Tag; tag != "" {
		if _, taken := channel.Consumers[tag]; taken {
			return false
		}
		channel.Consumers[tag] = consumer
		return true
	}

	generate := s.consumerTagGenerator()
	for i := 0; i < generatedTagDraws; i++ {
		tag := generate()
		if _, taken := channel.Consumers[tag]; taken {
			continue
		}
		consumer.Tag = tag
		channel.Consumers[tag] = consumer
		return true
	}
	return false
}

// handleBasicConsume handles the basic.consume method
func (s *Server) handleBasicConsume(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.consume method
	consumeMethod := &protocol.BasicConsumeMethod{}
	err := consumeMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.consume",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 20)
	}

	// Get the channel
	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 60, 20)
	}
	channel := value.(*protocol.Channel)

	s.Log.Debug("Basic consume requested",
		zap.String("queue", consumeMethod.Queue),
		zap.String("consumer_tag", consumeMethod.ConsumerTag),
		zap.Bool("no_ack", consumeMethod.NoAck),
		zap.Bool("exclusive", consumeMethod.Exclusive),
		zap.Uint16("prefetch_count", channel.PrefetchCount))

	// Authorization check: basic.consume requires read permission on queue
	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceQueue,
		Resource:     consumeMethod.Queue,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 20)
	}

	// Check if queue exists in the broker
	// This is a simplified check - in a real implementation you'd verify queue exists
	// For now, we'll proceed assuming the queue exists

	// FIXED BUFFER SIZE: Consumer.Messages channel is for TCP buffering only
	// Prefetch flow control is now handled by semaphore in broker layer
	// Small fixed buffer provides TCP/transport buffering without memory bloat
	// Blocking send provides natural backpressure to broker when consumer is slow
	bufferSize := 100 // Fixed size for all consumers

	s.Log.Debug("Creating consumer with bounded channel",
		zap.String("consumer_tag", consumeMethod.ConsumerTag),
		zap.Uint16("prefetch_count", channel.PrefetchCount),
		zap.Int("buffer_size", bufferSize))

	// Create a new consumer. The broker-internal identity is minted HERE,
	// before the consumer becomes visible to the connection's delivery loop:
	// discoverConsumers routes by Consumer.ID, so a consumer published into
	// channel.Consumers with an empty ID would be discovered unroutable.
	consumer := &protocol.Consumer{
		Tag:           consumeMethod.ConsumerTag,
		ID:            protocol.NewConsumerID(),
		Channel:       channel,
		Queue:         consumeMethod.Queue,
		NoAck:         consumeMethod.NoAck,
		Exclusive:     consumeMethod.Exclusive,
		Args:          consumeMethod.Arguments,
		PrefetchCount: channel.PrefetchCount, // CRITICAL FIX: Must set prefetch for consumer poll loop

		Messages: make(chan *protocol.Delivery, bufferSize), // BOUNDED: Buffer based on prefetch + margin
		Cancel:   make(chan struct{}, 1),                    // Channel to signal cancellation
	}

	// Claim the tag on THIS channel. AMQP 0-9-1 scopes the consumer tag to a
	// channel, so channel.Consumers is the authority on uniqueness, and the
	// check and the insert must be one critical section — two concurrent
	// basic.consume frames carrying the same tag must not both see it free.
	//
	// An empty tag means "server, generate one" (§1.8.3.3): claimConsumerTag
	// draws from protocol.GenerateConsumerTag and re-draws if the drawn tag is
	// already taken on this channel, so a generated tag can never collide with
	// a client-supplied one. The tag it returns is the one echoed in
	// consume-ok and stamped on every delivery.
	if !s.claimConsumerTag(channel, consumer) {
		// Duplicate tag on the same channel. AMQP 0-9-1: "The client MUST NOT
		// specify a tag that refers to an existing consumer. Error code:
		// not-allowed". 530 not-allowed is a CONNECTION exception in the
		// spec's reply-code table (unlike 403/404/405/406), so this closes the
		// connection, not the channel. The returned error is what makes the
		// frame loop tear the connection down.
		replyText := fmt.Sprintf("NOT_ALLOWED - attempt to reuse consumer tag '%s'", consumeMethod.ConsumerTag)
		s.Log.Warn("Duplicate consumer tag on channel",
			zap.String("consumer_tag", consumeMethod.ConsumerTag),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.connectionError(conn, amqperrors.NotAllowed, replyText, 60, 20)
	}
	// consumer.Tag now holds the claimed tag (set inside claimConsumerTag, under
	// the channel mutex and before publication). Stamping it here instead would
	// race the delivery loop's discovery scan.
	conn.ConsumersDirty.Store(true)

	// Register the consumer with the broker under its internal identity.
	err = s.Broker.RegisterConsumer(consumeMethod.Queue, consumer.ID, consumer)
	if err != nil {
		// Registration failed: release the tag so the channel does not hold a
		// consumer the broker never accepted (which would make the tag
		// permanently unusable on this channel and leak it into the delivery
		// loop's discovery scan).
		//
		// The channel map is the only thing to release. RegisterConsumer
		// persists before it publishes anything, so a failure there leaves no
		// broker-side state — no ConsumerState, no ack cursor, no
		// queueConsumers entry, no poll-loop goroutine — and there is nothing
		// for this handler to unregister.
		channel.Mutex.Lock()
		if channel.Consumers[consumer.Tag] == consumer {
			delete(channel.Consumers, consumer.Tag)
		}
		channel.Mutex.Unlock()
		conn.ConsumersDirty.Store(true)
		s.Log.Error("Failed to register consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", consumer.Tag),
			zap.String("queue", consumeMethod.Queue))
		return err
	}

	// Send basic.consume-ok response
	err = s.sendBasicConsumeOK(conn, channelID, consumer.Tag)
	if err != nil {
		s.Log.Error("Failed to send basic.consume-ok",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	s.updateConsumersTotal()

	s.Log.Debug("Consumer registered",
		zap.String("consumer_tag", consumer.Tag),
		zap.String("queue", consumer.Queue))

	return nil
}

// handleBasicCancel handles the basic.cancel method
func (s *Server) handleBasicCancel(conn *protocol.Connection, channelID uint16, payload []byte) error {
	// Deserialize the basic.cancel method
	cancelMethod := &protocol.BasicCancelMethod{}
	err := cancelMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.cancel",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 30)
	}

	s.Log.Info("Basic cancel requested",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	// Get the channel
	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 60, 30)
	}
	channel := value.(*protocol.Channel)

	// Remove the consumer
	channel.Mutex.Lock()
	consumer, exists := channel.Consumers[cancelMethod.ConsumerTag]
	if !exists {
		channel.Mutex.Unlock()
		// RabbitMQ parity: basic.cancel for an unknown consumer tag replies
		// with cancel-ok (unless nowait).
		s.Log.Debug("basic.cancel for unknown consumer tag; replying cancel-ok",
			zap.String("consumer_tag", cancelMethod.ConsumerTag),
			zap.Uint16("channel_id", channelID))
		if !cancelMethod.NoWait {
			return s.sendBasicCancelOK(conn, channelID, cancelMethod.ConsumerTag)
		}
		return nil
	}

	// Close the consumer's message channel and signal cancellation
	close(consumer.Cancel)
	delete(channel.Consumers, cancelMethod.ConsumerTag)
	channel.Mutex.Unlock()
	conn.ConsumersDirty.Store(true)

	// Unregister the consumer with the broker by its internal identity. Using
	// the wire tag here would cancel whichever same-tagged consumer registered
	// last, anywhere on the broker — one connection's basic.cancel tearing down
	// another connection's consumer. The tag resolves to a consumer only within
	// this channel, which is the resolution the lookup above just performed.
	err = s.Broker.UnregisterConsumer(consumer.ID)
	if err != nil {
		s.Log.Error("Failed to unregister consumer with broker",
			zap.Error(err),
			zap.String("consumer_tag", cancelMethod.ConsumerTag))
		// Continue anyway since we've already removed it locally
	}

	// Send basic.cancel-ok response
	if !cancelMethod.NoWait {
		err = s.sendBasicCancelOK(conn, channelID, cancelMethod.ConsumerTag)
		if err != nil {
			s.Log.Error("Failed to send basic.cancel-ok",
				zap.Error(err),
				zap.String("connection_id", conn.ID),
				zap.Uint16("channel_id", channelID))
			return err
		}
	}

	s.Log.Debug("Consumer cancelled",
		zap.String("consumer_tag", cancelMethod.ConsumerTag))

	s.updateConsumersTotal()

	return nil
}

// handleBasicGet handles the basic.get method
func (s *Server) handleBasicGet(conn *protocol.Connection, channelID uint16, payload []byte) error {
	getMethod := &protocol.BasicGetMethod{}
	err := getMethod.Deserialize(payload)
	if err != nil {
		s.Log.Error("Failed to deserialize basic.get",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 70)
	}

	if ce := s.Log.Check(zapcore.DebugLevel, "Basic get requested"); ce != nil {
		ce.Write(
			zap.String("queue", getMethod.Queue),
			zap.Bool("no_ack", getMethod.NoAck))
	}

	queueName, err := resolveQueueName(conn, channelID, getMethod.Queue)
	if err != nil {
		return err
	}

	if err := s.authorize(conn, channelID, interfaces.Operation{
		Action:       interfaces.ActionRead,
		ResourceType: interfaces.ResourceQueue,
		Resource:     queueName,
		VHost:        conn.Vhost,
	}); err != nil {
		return s.authzChannelError(conn, channelID, err, 60, 70)
	}

	message, deliveryTag, messageCount, err := s.Broker.GetMessageForGetOwned(queueName, getMethod.NoAck, conn.ID)
	if err != nil {
		s.Log.Error("Failed to get message for basic.get",
			zap.Error(err),
			zap.String("queue", queueName),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return err
	}

	if message == nil {
		return s.sendBasicGetEmpty(conn, channelID)
	}

	// SQ-18: translate the broker msgID into a per-channel monotonic wire
	// delivery tag so basic.get deliveries share the channel's tag space with
	// basic.deliver (and cumulative acks over a mixed stream stay coherent).
	// Manual-ack gets are tracked so their ack/nack/reject can be routed back to
	// the msgID-keyed broker get-delivery ledger; no-ack gets are never acked.
	wireTag := deliveryTag
	if v, ok := conn.Channels.Load(channelID); ok {
		channel := v.(*protocol.Channel)
		wireTag = channel.NextWireTag()
		if !getMethod.NoAck {
			channel.TrackDelivery(wireTag, deliveryTag, "", true)
		}
	}

	const maxPoolSize = 131 * 1024
	estimatedSize := deliveryOverheadEstimate + len(message.Body)
	var buf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		buf = &b
	} else {
		buf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*buf) <= maxPoolSize {
			protocol.PutBufferForSize(buf)
		}
	}()

	if err := s.serializeGetDeliveryInto(buf, channelID, wireTag, message.Redelivered, message.Exchange, message.RoutingKey, messageCount, message); err != nil {
		s.Log.Error("Failed to serialize basic.get-ok delivery",
			zap.Error(err),
			zap.Uint16("channel_id", channelID))
		return err
	}

	err = s.writeBytes(conn, *buf)

	if err != nil {
		s.Log.Error("Error sending basic.get-ok frames",
			zap.Error(err),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending basic.get-ok frames: %w", err)
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageDelivered(len(message.Body))
	}

	s.messagesDelivered.Add(1)
	s.bytesSent.Add(int64(len(message.Body)))

	return nil
}

// serializeDeliveryInto serializes a basic.deliver (method frame + header
// frame + body frames) directly into the provided buffer, avoiding all
// intermediate *Frame, *BasicDeliverMethod, and *ContentHeader allocations.
// This is the zero-alloc delivery serializer for hot paths.
func (s *Server) serializeDeliveryInto(buf *[]byte, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	// --- Method frame (basic.deliver) ---
	// class(2) + method(2) + consumerTag(1+N) + deliveryTag(8) + redelivered(1) + exchange(1+N) + routingKey(1+N)
	var payload []byte
	payload = binary.BigEndian.AppendUint16(payload, 60) // class ID
	payload = binary.BigEndian.AppendUint16(payload, 60) // method ID
	payload = protocol.AppendShortString(payload, consumerTag)
	payload = binary.BigEndian.AppendUint64(payload, deliveryTag)
	if redelivered {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = protocol.AppendShortString(payload, exchange)
	payload = protocol.AppendShortString(payload, routingKey)
	*buf = protocol.AppendFrame(*buf, protocol.FrameMethod, channelID, payload)

	return s.appendHeaderAndBodyFrames(buf, channelID, message)
}

// serializeGetDeliveryInto serializes a basic.get-ok (method frame + header
// frame + body frames) directly into the provided buffer, avoiding all
// intermediate allocations. This is the synchronous-response counterpart to
// serializeDeliveryInto, using basic.get-ok (60.71) instead of basic.deliver
// (60.60) and including the message-count field.
func (s *Server) serializeGetDeliveryInto(buf *[]byte, channelID uint16, deliveryTag uint64, redelivered bool, exchange, routingKey string, messageCount uint32, message *protocol.Message) error {
	// --- Method frame (basic.get-ok) ---
	var payload []byte
	payload = binary.BigEndian.AppendUint16(payload, 60) // class ID
	payload = binary.BigEndian.AppendUint16(payload, 71) // method ID (basic.get-ok)
	payload = binary.BigEndian.AppendUint64(payload, deliveryTag)
	if redelivered {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = protocol.AppendShortString(payload, exchange)
	payload = protocol.AppendShortString(payload, routingKey)
	payload = binary.BigEndian.AppendUint32(payload, messageCount)
	*buf = protocol.AppendFrame(*buf, protocol.FrameMethod, channelID, payload)

	return s.appendHeaderAndBodyFrames(buf, channelID, message)
}

// sendBasicDeliver sends a basic.deliver method frame to a consumer.
// Used for single-delivery paths (basic.get-ok, non-batched deliveries).
func (s *Server) sendBasicDeliver(conn *protocol.Connection, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	startTime := time.Now()
	defer func() {
		if s.MetricsCollector != nil {
			duration := time.Since(startTime).Seconds()
			s.MetricsCollector.RecordDeliveryLatency(duration)
		}
	}()

	// Large bodies take the zero-copy vectored path (same gate as the batched
	// consumer path): the body is referenced in place via net.Buffers rather
	// than memmoved into a contiguous frame buffer. See writeVectored for the
	// buffer-lifetime invariant (BL) — the body iovec aliases message.Body,
	// which is immutable and never pool-recycled.
	if len(message.Body) >= writevBodyThreshold {
		scratch := protocol.GetFrameSerializationBuffer()
		defer protocol.PutFrameSerializationBuffer(scratch)
		vec := &deliveryVec{scratch: scratch}
		if err := s.serializeDeliveryIntoVec(vec, channelID, consumerTag, deliveryTag, redelivered, exchange, routingKey, message); err != nil {
			s.Log.Error("Failed to serialize delivery",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Uint16("channel_id", channelID))
			return err
		}
		bufs := vec.build()
		return s.writeVectored(conn, &bufs, []int{len(message.Body)}, consumerTag, channelID)
	}

	const maxPoolSize = 131 * 1024
	estimatedSize := deliveryOverheadEstimate + len(message.Body)
	var buf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		buf = &b
	} else {
		buf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*buf) <= maxPoolSize {
			protocol.PutBufferForSize(buf)
		}
	}()

	if err := s.serializeDeliveryInto(buf, channelID, consumerTag, deliveryTag, redelivered, exchange, routingKey, message); err != nil {
		s.Log.Error("Failed to serialize delivery",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return err
	}

	err := s.writeBytes(conn, *buf)

	if err != nil {
		s.Log.Error("Error sending delivery frames",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Uint16("channel_id", channelID))
		return fmt.Errorf("error sending frames: %w", err)
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageDelivered(len(message.Body))
	}

	s.messagesDelivered.Add(1)
	s.bytesSent.Add(int64(len(message.Body)))

	return nil
}

// AMQP 0.9.1 packs the trailing bit arguments of basic.ack / basic.reject /
// basic.nack into a SINGLE OCTET at payload[8], so a spec client's payload is
// exactly 9 bytes. Reading two bytes here (a former uint16 decode) can never
// match that framing: basic.ack "multiple" always parsed false — silently
// turning every client multi-ack into a single ack until the prefetch window
// filled and delivery wedged (BenchmarkVersus_MultiAck1000) — and reject/nack
// "requeue" always parsed as the default. An 8-byte payload (no flag octet) is
// tolerated for wire-compat with the pre-fix encoder and keeps each method's
// spec defaults.

func parseBasicAckArgs(payload []byte) (deliveryTag uint64, multiple bool, err error) {
	if len(payload) < 8 {
		return 0, false, fmt.Errorf("basic.ack payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	if len(payload) >= 9 {
		multiple = payload[8]&0x01 != 0
	}
	return deliveryTag, multiple, nil
}

func parseBasicRejectArgs(payload []byte) (deliveryTag uint64, requeue bool, err error) {
	if len(payload) < 8 {
		return 0, false, fmt.Errorf("basic.reject payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	requeue = true // spec default when the flag octet is absent
	if len(payload) >= 9 {
		requeue = payload[8]&0x01 != 0
	}
	return deliveryTag, requeue, nil
}

func parseBasicNackArgs(payload []byte) (deliveryTag uint64, multiple, requeue bool, err error) {
	if len(payload) < 8 {
		return 0, false, false, fmt.Errorf("basic.nack payload too short: %d bytes", len(payload))
	}
	deliveryTag = binary.BigEndian.Uint64(payload[:8])
	requeue = true // spec default when the flag octet is absent
	if len(payload) >= 9 {
		multiple = payload[8]&0x01 != 0
		requeue = payload[8]&0x02 != 0
	}
	return deliveryTag, multiple, requeue, nil
}

// wireChannel returns the channel for channelID, or nil when it is not
// registered. Real connections always register a channel via channel.open
// before any delivery; a nil result (some unit tests) makes resolveDeliveryTag
// report the tag as unknown, since there is no per-channel wire-tag table to
// resolve it against.
func (s *Server) wireChannel(conn *protocol.Connection, channelID uint16) *protocol.Channel {
	if v, ok := conn.Channels.Load(channelID); ok {
		return v.(*protocol.Channel)
	}
	return nil
}

// resolveDeliveryTag (SQ-18) translates a wire delivery tag received on
// basic.ack/nack/reject into the broker identity behind it and removes the
// tracking entry (the tag is about to be settled). The per-channel wire-tag
// table is authoritative: wire tags (channel.NextWireTag, 1-based per
// channel) and broker-internal msgIDs (packed queueOrdinal<<44|perQueueSeq
// tags) are two independent tag spaces, so without a channel there is no way
// to recover the msgID behind a wire tag. known=false means the tag is
// unknown, already settled (duplicate/no-ack/stale), or there is no channel
// to resolve it against; handlers turn that into a 406 precondition-failed
// (RabbitMQ collect_acks parity) via unknownDeliveryTag — EXCEPT the
// nil-channel reorder case, which is ignored (see handleBasicAck).
func (s *Server) resolveDeliveryTag(channel *protocol.Channel, wireTag uint64) (msgID uint64, consumerID string, isGet, known bool) {
	if channel == nil {
		return 0, "", false, false
	}
	ref, ok := channel.TakeWireTag(wireTag)
	if !ok {
		return 0, "", false, false
	}
	return ref.MsgID, ref.ConsumerID, ref.IsGet, true
}

// unknownDeliveryTag raises a channel-level 406 for an ack/nack/reject whose
// delivery tag is not outstanding (RabbitMQ rabbit_channel:collect_acks
// raises precondition_failed("unknown delivery tag ~w")).
func (s *Server) unknownDeliveryTag(conn *protocol.Connection, ch uint16, tag uint64, methodID uint16) error {
	return s.channelError(conn, ch, amqperrors.PreconditionFailed,
		fmt.Sprintf("PRECONDITION_FAILED - unknown delivery tag %d", tag), 60, methodID)
}

// handleBasicAck handles the basic.ack method
func (s *Server) handleBasicAck(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, multiple, err := parseBasicAckArgs(payload)
	if err != nil {
		return s.malformed(conn, err, 60, protocol.BasicAck)
	}
	channel := s.wireChannel(conn, channelID)
	if channel == nil {
		// An ack on a missing channel is IGNORED, not an error: the reader
		// diverts acks to AckQueue while channel.close goes through FrameQueue,
		// so a client that acks and then closes can have its acks processed
		// after the processor tore the channel down — a legitimate reordering.
		s.Log.Debug("ack on missing channel ignored",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return nil
	}
	txm := s.txm(conn)
	tx := txm != nil && txm.IsTransactional(channelID)

	// WS-D §6: in tx mode the wire tag(s) are TAKEN now (RabbitMQ removes from
	// the unacked map at ack time, R14) and restored on rollback/failed commit;
	// neither the cumulative branch nor resolveDeliveryTag runs in tx mode (the
	// take happens inside txBufferSettlement, one single-tag op per entry).
	if tx {
		known, err := s.txBufferSettlement(conn, channel, channelID, interfaces.OpAck, wireTag, multiple, false)
		if !known {
			return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicAck)
		}
		return err
	}

	// Non-transactional cumulative ack spans every consumer on the channel
	// (SQ-18): the client's "all tags <= N" may cover deliveries that came from
	// different queues, so it cannot be routed to a single consumer's broker
	// multi-ack. The per-channel wire-tag table is the channel-wide unacked set.
	if multiple && !tx {
		if wireTag != 0 {
			// Peek first: an unknown top tag raises 406 BEFORE anything is
			// settled (RabbitMQ collect_acks). tag=0 means "all outstanding"
			// and never errors. Peeking then taking is race-free: only the
			// ackProcessor goroutine removes wire-tag entries.
			if _, ok := channel.ResolveWireTag(wireTag); !ok {
				return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicAck)
			}
		}
		s.cumulativeSettle(channel, wireTag, settleAck, false)
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordMessageAcknowledged()
		}
		return nil
	}

	msgID, consumerID, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicAck)
	}

	if isGet {
		if err := s.Broker.AcknowledgeGetDelivery(msgID); err != nil {
			return err
		}
	} else if err := s.Broker.AcknowledgeMessage(consumerID, msgID, false); err != nil {
		s.Log.Error("Failed to acknowledge message in broker",
			zap.Error(err),
			zap.String("consumer_id", consumerID),
			zap.Uint64("msg_id", msgID))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageAcknowledged()
	}

	return nil
}

// handleBasicReject handles the basic.reject method
func (s *Server) handleBasicReject(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, requeue, err := parseBasicRejectArgs(payload)
	if err != nil {
		return s.malformed(conn, err, 60, protocol.BasicReject)
	}
	channel := s.wireChannel(conn, channelID)
	if channel == nil {
		s.Log.Debug("reject on missing channel ignored",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return nil
	}

	// WS-D §6: tx reject takes the wire tag now (R14) with rollback restore.
	if txm := s.txm(conn); txm != nil && txm.IsTransactional(channelID) {
		known, err := s.txBufferSettlement(conn, channel, channelID, interfaces.OpReject, wireTag, false, requeue)
		if !known {
			return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicReject)
		}
		return err
	}

	msgID, consumerID, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicReject)
	}

	if isGet {
		if err := s.Broker.RejectGetDelivery(msgID, requeue); err != nil {
			return err
		}
	} else if err := s.Broker.RejectMessage(consumerID, msgID, requeue); err != nil {
		s.Log.Error("Failed to reject message in broker",
			zap.Error(err),
			zap.String("consumer_id", consumerID),
			zap.Uint64("msg_id", msgID),
			zap.Bool("requeue", requeue))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageRejected()
	}

	return nil
}

// handleBasicNack handles the basic.nack method
func (s *Server) handleBasicNack(conn *protocol.Connection, channelID uint16, payload []byte) error {
	wireTag, multiple, requeue, err := parseBasicNackArgs(payload)
	if err != nil {
		return s.malformed(conn, err, 60, protocol.BasicNack)
	}
	channel := s.wireChannel(conn, channelID)
	if channel == nil {
		s.Log.Debug("nack on missing channel ignored",
			zap.Uint64("delivery_tag", wireTag),
			zap.Uint16("channel_id", channelID),
			zap.String("connection_id", conn.ID))
		return nil
	}
	txm := s.txm(conn)
	tx := txm != nil && txm.IsTransactional(channelID)

	// WS-D §6: tx nack takes the wire tag(s) now (R14) with rollback restore.
	if tx {
		known, err := s.txBufferSettlement(conn, channel, channelID, interfaces.OpNack, wireTag, multiple, requeue)
		if !known {
			return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicNack)
		}
		return err
	}

	// Non-transactional cumulative nack spans every consumer on the channel
	// (SQ-18), mirroring cumulative ack.
	if multiple && !tx {
		if wireTag != 0 {
			if _, ok := channel.ResolveWireTag(wireTag); !ok {
				return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicNack)
			}
		}
		s.cumulativeSettle(channel, wireTag, settleNack, requeue)
		if s.MetricsCollector != nil {
			s.MetricsCollector.RecordMessageRejected()
		}
		return nil
	}

	msgID, consumerID, isGet, known := s.resolveDeliveryTag(channel, wireTag)
	if !known {
		return s.unknownDeliveryTag(conn, channelID, wireTag, protocol.BasicNack)
	}

	if isGet {
		if err := s.Broker.NackGetDelivery(msgID, requeue); err != nil {
			return err
		}
	} else if err := s.Broker.NacknowledgeMessage(consumerID, msgID, false, requeue); err != nil {
		s.Log.Error("Failed to nack message in broker",
			zap.Error(err),
			zap.String("consumer_id", consumerID),
			zap.Uint64("msg_id", msgID),
			zap.Bool("requeue", requeue))
		return err
	}

	if s.MetricsCollector != nil {
		s.MetricsCollector.RecordMessageRejected()
	}

	return nil
}

// settleKind selects the terminal transition applied by cumulativeSettle.
type settleKind int

const (
	settleAck settleKind = iota
	settleNack
)

// cumulativeSettle applies a cumulative (multiple=true) ack or nack to every
// still-outstanding wire tag <= wireTag on the channel — across ALL consumers
// and any tracked basic.get deliveries (SQ-18). Each tag is settled through the
// broker's single-message path (its own settle() LoadAndDelete linearization
// point), so credit accounting stays exact and requeue-reused msgIDs cannot be
// over-settled. When channel is nil (legacy tests) it falls back to routing the
// wire tag (== msgID) to the owning consumer's broker multi-ack.
func (s *Server) cumulativeSettle(channel *protocol.Channel, wireTag uint64, kind settleKind, requeue bool) {
	// Spec: delivery-tag 0 with multiple=true means "every outstanding delivery
	// on the channel". Wire tags are 1-based, so 0 would otherwise match nothing
	// and ack-all / nack-all would silently no-op.
	if wireTag == 0 {
		wireTag = math.MaxUint64
	}
	if channel == nil {
		ctag, ok := s.Broker.GetConsumerForDelivery(wireTag)
		if !ok {
			return
		}
		switch {
		case ctag == "" && kind == settleAck:
			s.Broker.AcknowledgeGetDelivery(wireTag)
		case ctag == "":
			s.Broker.NackGetDelivery(wireTag, requeue)
		case kind == settleAck:
			s.Broker.AcknowledgeMessage(ctag, wireTag, true)
		default:
			s.Broker.NacknowledgeMessage(ctag, wireTag, true, requeue)
		}
		return
	}

	refs := channel.TakeWireTagsUpTo(wireTag)
	for _, ref := range refs {
		switch {
		case ref.IsGet && kind == settleAck:
			s.Broker.AcknowledgeGetDelivery(ref.MsgID)
		case ref.IsGet:
			s.Broker.NackGetDelivery(ref.MsgID, requeue)
		case kind == settleAck:
			s.Broker.AcknowledgeMessage(ref.ConsumerID, ref.MsgID, false)
		default:
			s.Broker.NacknowledgeMessage(ref.ConsumerID, ref.MsgID, false, requeue)
		}
	}
}

func (s *Server) handleBasicRecover(conn *protocol.Connection, channelID uint16, payload []byte) error {
	recoverMethod := &protocol.BasicRecoverMethod{}
	if err := recoverMethod.Deserialize(payload); err != nil {
		s.Log.Error("Failed to deserialize basic.recover",
			zap.Error(err),
			zap.String("connection_id", conn.ID),
			zap.Uint16("channel_id", channelID))
		return s.malformed(conn, err, 60, 110)
	}

	// requeue field is deserialized for wire-format correctness but not
	// acted on — RabbitMQ also ignores requeue=false and always requeues
	// to the shared queue. Redelivery to the same consumer is not supported.
	_ = recoverMethod.Requeue

	value, exists := conn.Channels.Load(channelID)
	if !exists {
		return s.connectionError(conn, amqperrors.ChannelErrorCode, "CHANNEL_ERROR - expected 'channel.open'", 60, 110)
	}
	channel := value.(*protocol.Channel)

	// basic.recover is scoped to this channel's consumers, addressed by their
	// broker-internal identities (the broker's ledger is keyed by identity;
	// a wire tag is only channel-unique).
	channel.Mutex.RLock()
	consumerIDs := make([]string, 0, len(channel.Consumers))
	for _, consumer := range channel.Consumers {
		consumerIDs = append(consumerIDs, consumer.ID)
	}
	channel.Mutex.RUnlock()

	for _, id := range consumerIDs {
		if err := s.Broker.RequeueAllForConsumer(id); err != nil {
			s.Log.Error("Failed to requeue messages for consumer",
				zap.Error(err),
				zap.String("consumer_id", id))
		}
	}

	return s.sendBasicRecoverOK(conn, channelID)
}

func (s *Server) updateConsumersTotal() {
	if s.MetricsCollector != nil && s.Broker != nil {
		s.MetricsCollector.SetConsumersTotal(len(s.Broker.GetConsumers()))
	}
}

// Stop gracefully stops the server
