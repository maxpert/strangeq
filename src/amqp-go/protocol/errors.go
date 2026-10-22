package protocol

import "errors"

// ErrFrameTooLarge: an inbound frame's total size (payload + FrameOverhead)
// exceeds the negotiated frame-max (or MaxInboundFrameSize). Map to
// connection.close 501 frame-error (WS-B).
var ErrFrameTooLarge = errors.New("amqp: frame exceeds frame-max")
