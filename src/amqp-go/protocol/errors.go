package protocol

import "errors"

// ErrFrameTooLarge: an inbound frame's total size (payload + FrameOverhead)
// exceeds the negotiated frame-max (or MaxInboundFrameSize). Map to
// connection.close 501 frame-error (WS-B).
var ErrFrameTooLarge = errors.New("amqp: frame exceeds frame-max")

// ErrShortStringTooLong: a shortstr method field exceeds 255 bytes. Returned
// (wrapped, with the field name) by Serialize of client-originated methods.
var ErrShortStringTooLong = errors.New("amqp: short string exceeds 255 bytes")

// ErrInvalidContentHeader wraps every ReadContentHeader failure.
var ErrInvalidContentHeader = errors.New("amqp: invalid content header")

// ErrContentHeaderClassMismatch additionally wraps a class-id other than basic (60).
var ErrContentHeaderClassMismatch = errors.New("amqp: content header class-id mismatch")
