package server

import (
	"encoding/binary"
	"net"

	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

const deliveryOverheadEstimate = 256

// writevBodyThreshold is the per-message body size (bytes) at or above which a
// delivery is emitted via the zero-copy vectored (net.Buffers / writev) path
// instead of being memmoved into a contiguous frame buffer. Below it, a
// contiguous copy plus a single Write is cache-friendly and cheaper than the
// per-iovec setup; at/above it the body-proportional memmove dominates and
// zero-copy wins. 32 KiB is a safe crossover: comfortably below the 64 KiB
// durable target cell and far above the 1 KiB / small-message hot cells, whose
// batches therefore keep the byte-identical contiguous path.
const writevBodyThreshold = 32 * 1024

// sendBatchedDeliveries serializes all deliveries in a batch into a single
// buffer and performs a single conn.Conn.Write under a single WriteMutex
// acquisition. This reduces N syscalls + N mutex acquisitions to 1 of each.
func (s *Server) sendBatchedDeliveries(conn *protocol.Connection, channelID uint16, consumerTag string, deliveries []*protocol.Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}

	s.Log.Debug("Sending batched deliveries",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("batch_size", len(deliveries)))

	// SQ-18: resolve the channel so each delivery can be stamped with a
	// per-channel monotonic wire delivery tag (and manual-ack deliveries tracked
	// for later ack translation). A missing channel (e.g. some unit tests invoke
	// this without a registered channel) falls back to the broker msgID as the
	// wire tag, preserving legacy behaviour.
	var channel *protocol.Channel
	if v, ok := conn.Channels.Load(channelID); ok {
		channel = v.(*protocol.Channel)
	}

	// Large-body deliveries take the zero-copy vectored (writev) path: each body
	// is referenced in place via net.Buffers instead of being memmoved into a
	// contiguous frame buffer. The gate keys on per-message body size, NOT the
	// batch total, so a single large delivery (whose batch total may still fit
	// the pool) also avoids the body copy. Batches whose bodies are all small
	// fall through to the byte-identical contiguous path below, so the hot
	// small-message / winning cells are not touched.
	for _, d := range deliveries {
		if d.Message != nil && len(d.Message.Body) >= writevBodyThreshold {
			return s.sendBatchedDeliveriesVectored(conn, channelID, consumerTag, channel, deliveries)
		}
	}

	estimatedSize := 0
	for _, d := range deliveries {
		if d.Message != nil {
			estimatedSize += deliveryOverheadEstimate + len(d.Message.Body)
		}
	}

	const maxPoolSize = 131 * 1024
	var batchBuf *[]byte
	if estimatedSize > maxPoolSize {
		b := make([]byte, 0, estimatedSize)
		batchBuf = &b
	} else {
		batchBuf = protocol.GetBufferForSize(estimatedSize)
	}
	defer func() {
		if cap(*batchBuf) <= maxPoolSize {
			protocol.PutBufferForSize(batchBuf)
		}
	}()

	for _, delivery := range deliveries {
		// Stamp the per-channel monotonic wire tag. Manual-ack deliveries are
		// tracked (wire tag -> broker msgID) so the client's ack can be
		// translated; no-ack deliveries take a tag but are never tracked (they
		// are settled at send time and never acked).
		wireTag := delivery.DeliveryTag
		if channel != nil {
			wireTag = channel.NextWireTag()
			if !delivery.NoAck {
				channel.TrackDelivery(wireTag, delivery.DeliveryTag, delivery.ConsumerTag, false)
			}
		}
		err := s.serializeDeliveryInto(
			batchBuf,
			channelID,
			consumerTag,
			wireTag,
			delivery.Redelivered,
			delivery.Exchange,
			delivery.RoutingKey,
			delivery.Message,
		)
		if err != nil {
			s.Log.Error("Failed to serialize delivery in batch",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			return err
		}
	}

	s.Log.Debug("Writing batched deliveries atomically",
		zap.String("consumer_tag", consumerTag),
		zap.Uint16("channel_id", channelID),
		zap.Int("batch_size", len(deliveries)),
		zap.Int("total_bytes", len(*batchBuf)))

	conn.WriteMutex.Lock()
	_, err := conn.Conn.Write(*batchBuf)
	conn.WriteMutex.Unlock()

	if err != nil {
		s.Log.Error("Failed to write batched deliveries",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Int("batch_size", len(deliveries)))
		return err
	}

	if s.MetricsCollector != nil {
		for _, delivery := range deliveries {
			s.MetricsCollector.RecordMessageDelivered(len(delivery.Message.Body))
		}
	}

	for _, delivery := range deliveries {
		s.messagesDelivered.Add(1)
		s.bytesSent.Add(int64(len(delivery.Message.Body)))
	}

	s.Log.Debug("Batched deliveries sent successfully",
		zap.String("consumer_tag", consumerTag),
		zap.Int("batch_size", len(deliveries)))

	return nil
}

// vecSegKind classifies a segment in a deliveryVec's ordered iovec list.
type vecSegKind uint8

const (
	vecSegScratch vecSegKind = iota // a contiguous run of scaffolding bytes: scratch[a:b]
	vecSegBody                      // a zero-copy body chunk (slice of Message.Body)
	vecSegEnd                       // the shared read-only frame-end octet (0xCE)
)

// vecSegment is one entry in the ordered iovec list a deliveryVec produces.
type vecSegment struct {
	kind vecSegKind
	a, b int    // scratch offsets, for vecSegScratch (resolved to scratch[a:b] in build)
	body []byte // body slice, for vecSegBody
}

// deliveryVec builds the iovec list (net.Buffers) for a batch of large-body
// deliveries. All SMALL frame scaffolding — the method frame, the
// content-header frame, and each body frame's 7-byte prefix — is appended into
// ONE pooled scratch slab; each message body chunk is recorded as a zero-copy
// slice of Message.Body; each body frame's trailing 0xCE is emitted from the
// shared read-only protocol.FrameEndSlice().
//
// Two-phase build (correctness-critical): scratch is only ever appended to
// during accumulation and is sub-sliced ONLY in build(), after all appends have
// finished. Because a growing append can reallocate scratch, taking a sub-slice
// mid-accumulation could leave earlier iovecs pointing at a stale backing array
// (torn frames on the wire). Recording scratch cut OFFSETS during accumulation
// and materializing scratch[a:b] only in build() makes that class of bug
// impossible.
type deliveryVec struct {
	scratch    *[]byte
	segs       []vecSegment
	scratchPos int // start offset of the pending (not-yet-recorded) scratch run
}

// flushScratchRun records the pending scratch bytes [scratchPos:len) as one
// vecSegScratch segment (offsets only; the slice is taken in build).
func (v *deliveryVec) flushScratchRun() {
	n := len(*v.scratch)
	if n > v.scratchPos {
		v.segs = append(v.segs, vecSegment{kind: vecSegScratch, a: v.scratchPos, b: n})
		v.scratchPos = n
	}
}

// appendSmallFrame appends a complete small frame (type|chan|size|payload|0xCE)
// into scratch. Used for the method and content-header frames, whose payloads
// are small and bounded.
func (v *deliveryVec) appendSmallFrame(frameType byte, channelID uint16, payload []byte) {
	*v.scratch = protocol.AppendFrame(*v.scratch, frameType, channelID, payload)
}

// appendBodyFrame emits one AMQP body frame as three iovecs: the 7-byte prefix
// (type|chan|size) in scratch, then the zero-copy body slice, then the shared
// frame-end octet. It records the body as an alias of message.Body — never a
// copy (see the buffer-lifetime note at the write site).
func (v *deliveryVec) appendBodyFrame(channelID uint16, body []byte) {
	*v.scratch = append(*v.scratch, protocol.FrameBody)
	*v.scratch = binary.BigEndian.AppendUint16(*v.scratch, channelID)
	*v.scratch = binary.BigEndian.AppendUint32(*v.scratch, uint32(len(body)))
	v.flushScratchRun() // scaffolding up to and including this frame's prefix
	v.segs = append(v.segs, vecSegment{kind: vecSegBody, body: body})
	v.segs = append(v.segs, vecSegment{kind: vecSegEnd})
}

// build materializes the net.Buffers AFTER scratch has stopped growing. Slicing
// scratch here is safe because no further append can reallocate it.
func (v *deliveryVec) build() net.Buffers {
	v.flushScratchRun() // trailing scaffolding (e.g. method+header of an empty-body delivery)
	s := *v.scratch
	bufs := make(net.Buffers, 0, len(v.segs))
	for i := range v.segs {
		seg := &v.segs[i]
		switch seg.kind {
		case vecSegScratch:
			bufs = append(bufs, s[seg.a:seg.b])
		case vecSegBody:
			bufs = append(bufs, seg.body)
		case vecSegEnd:
			bufs = append(bufs, protocol.FrameEndSlice())
		}
	}
	return bufs
}

// serializeDeliveryIntoVec mirrors serializeDeliveryInto but emits the delivery
// into a deliveryVec (zero-copy body) rather than a contiguous buffer. The
// basic.deliver method-frame payload is byte-identical to serializeDeliveryInto.
func (s *Server) serializeDeliveryIntoVec(vec *deliveryVec, channelID uint16, consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string, message *protocol.Message) error {
	// --- Method frame (basic.deliver) --- identical to serializeDeliveryInto ---
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
	vec.appendSmallFrame(protocol.FrameMethod, channelID, payload)

	return s.appendHeaderAndBodyFramesVec(vec, channelID, message)
}

// appendHeaderAndBodyFramesVec mirrors appendHeaderAndBodyFrames: it appends the
// content-header frame into the vec's scratch and emits each body chunk as a
// zero-copy iovec. It uses the SAME buildContentHeaderPayload and the SAME
// maxBodyPerFrame chunking as the contiguous encoder, so the framing is
// byte-identical — only the body bytes move from a copy into an iovec.
func (s *Server) appendHeaderAndBodyFramesVec(vec *deliveryVec, channelID uint16, message *protocol.Message) error {
	hdrPayload, err := buildContentHeaderPayload(message)
	if err != nil {
		return err
	}
	vec.appendSmallFrame(protocol.FrameHeader, channelID, hdrPayload)

	maxFrameSize := uint32(s.Config.Server.MaxFrameSize)
	maxBodyPerFrame := int(maxFrameSize) - 8
	if maxBodyPerFrame <= 0 {
		maxBodyPerFrame = 4096
	}
	for offset := 0; offset < len(message.Body); offset += maxBodyPerFrame {
		end := offset + maxBodyPerFrame
		if end > len(message.Body) {
			end = len(message.Body)
		}
		vec.appendBodyFrame(channelID, message.Body[offset:end])
	}
	return nil
}

// sendBatchedDeliveriesVectored is the zero-copy counterpart to
// sendBatchedDeliveries for batches carrying at least one large body. It builds
// one net.Buffers spanning the whole batch and performs a single
// (*net.Buffers).WriteTo under the connection WriteMutex. On a *net.TCPConn that
// becomes one writev(2) (scattered, no body copy); on *tls.Conn / net.Pipe it
// falls back to a sequential per-iovec Write (correct bytes, no syscall saving).
// The per-message wire-tag stamping and the metrics/counter tail are lifted
// verbatim from sendBatchedDeliveries.
func (s *Server) sendBatchedDeliveriesVectored(conn *protocol.Connection, channelID uint16, consumerTag string, channel *protocol.Channel, deliveries []*protocol.Delivery) error {
	scratch := protocol.GetFrameSerializationBuffer()
	defer protocol.PutFrameSerializationBuffer(scratch)
	vec := &deliveryVec{scratch: scratch}

	for _, delivery := range deliveries {
		// Stamp the per-channel monotonic wire tag (see sendBatchedDeliveries).
		wireTag := delivery.DeliveryTag
		if channel != nil {
			wireTag = channel.NextWireTag()
			if !delivery.NoAck {
				channel.TrackDelivery(wireTag, delivery.DeliveryTag, delivery.ConsumerTag, false)
			}
		}
		if err := s.serializeDeliveryIntoVec(
			vec,
			channelID,
			consumerTag,
			wireTag,
			delivery.Redelivered,
			delivery.Exchange,
			delivery.RoutingKey,
			delivery.Message,
		); err != nil {
			s.Log.Error("Failed to serialize delivery in vectored batch",
				zap.Error(err),
				zap.String("consumer_tag", consumerTag),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			return err
		}
	}

	bufs := vec.build()

	// BUFFER-LIFETIME SAFETY (invariant BL): the body iovecs in bufs alias each
	// delivery.Message.Body DIRECTLY (zero-copy). This is safe only because a
	// published body's backing array is immutable and is NEVER returned to any
	// pool — it is GC-managed and kept alive for the entire WriteTo by the live
	// deliveries / delivery.Message references on this goroutine's stack. Ring
	// Delete/wrap, consumer ack, requeue, channel close, and iter5 shared-body
	// all only move pointers; none mutate or recycle body bytes. This is exactly
	// the lifetime the contiguous path already relied on when AppendFrame
	// memmoved the body under the same mutex — writev only removes the copy, it
	// does not extend the required lifetime.
	//
	// WARNING: iter3's deferred "pool-return the receive body on ack" (B2) would
	// turn these aliases into a use-after-free (a Put on ack could hand the array
	// to a new publish while a slow consumer's WriteTo is still reading it). B2
	// MUST NOT land without a refcount that keeps the body pinned across this
	// in-flight write. The -race mixed-size publish/consume/ack stress test
	// guards this invariant.
	conn.WriteMutex.Lock()
	_, err := bufs.WriteTo(conn.Conn)
	conn.WriteMutex.Unlock()

	if err != nil {
		s.Log.Error("Failed to write vectored batched deliveries",
			zap.Error(err),
			zap.String("consumer_tag", consumerTag),
			zap.Int("batch_size", len(deliveries)))
		return err
	}

	if s.MetricsCollector != nil {
		for _, delivery := range deliveries {
			s.MetricsCollector.RecordMessageDelivered(len(delivery.Message.Body))
		}
	}

	for _, delivery := range deliveries {
		s.messagesDelivered.Add(1)
		s.bytesSent.Add(int64(len(delivery.Message.Body)))
	}

	return nil
}
