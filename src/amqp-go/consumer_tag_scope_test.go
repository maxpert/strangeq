package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// Loop-2 item 1: consumer-tag identity is scoped PER CHANNEL, not broker-global.
//
// AMQP 0-9-1, basic.consume, field `consumer-tag`:
//
//	"Specifies the identifier for the consumer. The consumer tag is local to
//	 a channel, so two clients can use the same consumer tags."
//
// and the accompanying rule:
//
//	"The client MUST NOT specify a tag that refers to an existing consumer.
//	 Error code: not-allowed"
//
// `not-allowed` is reply code 530, and AMQP 0-9-1's reply-code table classes
// 530 as a CONNECTION exception (unlike 403/404/405/406, which are channel
// exceptions). So the two arms of the spec are:
//
//	same channel, duplicate tag      -> connection.close(530), connection dies
//	different channel, same tag      -> both consumers work independently
//
// The second arm is what makes this a scoping fix rather than a blanket
// "reject duplicates" shortcut: a fix that rejected every duplicate would pass
// arm 1 and fail arm 2.
//
// PRE-FIX BEHAVIOUR these predicates detect (mutation proof, see .notes):
// the broker keyed activeConsumers by the bare tag broker-globally, so the
// second registration silently overwrote the first. Arm 1 produced a
// consume-ok and a live connection (no 530 at all). Arm 2 produced ack
// routing into the wrong queue and starvation of the loser at exactly its
// prefetch window.
// ============================================================================

var ctagPortCounter atomic.Int64

// ctagServer starts an embedded broker on a private port with a per-test data
// directory and returns its AMQP URL plus a stop func.
func ctagServer(t *testing.T) (string, func()) {
	t.Helper()
	port := 19400 + int(ctagPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	waitForListening(t, srv)
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), func() { testShutdownServer(t, srv) }
}

// TestConsumerTagScope_DuplicateOnSameChannelIsConnectionException530 pins the
// FIRST spec arm: reusing a consumer tag on the SAME channel is a hard error,
// and a hard error in AMQP 0-9-1 is a connection exception. The predicate
// asserts BOTH halves — the code is 530 AND it arrived as a connection close
// (Connection.NotifyClose fires, Connection.IsClosed()), because a channel
// exception carrying 530 would be a different, wrong, behaviour.
func TestConsumerTagScope_DuplicateOnSameChannelIsConnectionException530(t *testing.T) {
	url, stop := ctagServer(t)
	defer stop()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	connClosed := make(chan *amqp.Error, 1)
	conn.NotifyClose(connClosed)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	chClosed := make(chan *amqp.Error, 1)
	ch.NotifyClose(chClosed)

	q, err := ch.QueueDeclare("ctag-dup-q", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}

	const tag = "duplicate-tag"
	if _, err := ch.Consume(q.Name, tag, false, false, false, false, nil); err != nil {
		t.Fatalf("first consume must succeed: %v", err)
	}

	// Second consume, SAME channel, SAME tag. Per the spec this is a hard
	// error; the broker must raise 530 as a connection exception.
	_, secondErr := ch.Consume(q.Name, tag, false, false, false, false, nil)

	select {
	case amqpErr := <-connClosed:
		if amqpErr == nil {
			t.Fatal("connection closed with a nil error; expected a 530 connection exception")
		}
		if amqpErr.Code != 530 {
			t.Fatalf("connection exception code = %d (%q); want 530 not-allowed",
				amqpErr.Code, amqpErr.Reason)
		}
		// A connection exception, per AMQP 0-9-1's classification of 530.
		if !amqpErr.Server {
			t.Errorf("530 must be raised by the SERVER; got a client-side error %q", amqpErr.Reason)
		}
	case <-time.After(5 * time.Second):
		// Report what actually happened so a channel-exception regression is
		// distinguishable from "nothing happened at all".
		select {
		case chErr := <-chClosed:
			t.Fatalf("duplicate tag on the same channel raised a CHANNEL exception (%v); "+
				"AMQP 0-9-1 classes 530 not-allowed as a CONNECTION exception", chErr)
		default:
		}
		t.Fatalf("duplicate consumer tag on the same channel was accepted "+
			"(second consume err = %v, connection still open = %v); "+
			"spec requires a 530 not-allowed connection exception",
			secondErr, !conn.IsClosed())
	}

	if !conn.IsClosed() {
		t.Error("connection must be closed after a 530 connection exception")
	}
}

// TestConsumerTagScope_SameTagOnDifferentChannelsBothConsume pins the SECOND
// spec arm: the same tag on two DIFFERENT channels of one connection is legal
// and both consumers must work independently.
//
// The predicate is deliberately built to fail under the pre-fix global keying:
// prefetch is 1 and each queue holds more messages than that window, so a
// consumer whose ConsumerState was overwritten (or whose acks were routed to
// the other queue's state) can never get its gate credit back and pins at
// exactly one delivery — the production starvation signature.
func TestConsumerTagScope_SameTagOnDifferentChannelsBothConsume(t *testing.T) {
	url, stop := ctagServer(t)
	defer stop()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	const (
		sharedTag = "shared-consumer-tag"
		perQueue  = 20
	)

	type arm struct {
		ch    *amqp.Channel
		queue string
		recv  <-chan amqp.Delivery
	}
	arms := make([]*arm, 2)
	for i := range arms {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel %d: %v", i, err)
		}
		defer ch.Close()
		if err := ch.Qos(1, 0, false); err != nil {
			t.Fatalf("qos %d: %v", i, err)
		}
		qName := fmt.Sprintf("ctag-crosschan-q%d", i)
		if _, err := ch.QueueDeclare(qName, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %d: %v", i, err)
		}
		arms[i] = &arm{ch: ch, queue: qName}
	}

	// Publish BEFORE consuming so neither consumer can be starved merely by
	// arriving late.
	for _, a := range arms {
		for j := 0; j < perQueue; j++ {
			body := fmt.Sprintf("%s-%d", a.queue, j)
			if err := a.ch.Publish("", a.queue, false, false,
				amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publish to %s: %v", a.queue, err)
			}
		}
	}

	// The same tag on two different channels. Both MUST be accepted.
	for i, a := range arms {
		recv, err := a.ch.Consume(a.queue, sharedTag, false, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume %d with tag %q must succeed on a distinct channel: %v",
				i, sharedTag, err)
		}
		a.recv = recv
	}

	// Each arm must drain its OWN queue in full. Manual ack with prefetch 1
	// means a mis-routed ack stalls the arm permanently.
	type result struct {
		idx  int
		got  []string
		fail error
	}
	results := make(chan result, len(arms))
	for i, a := range arms {
		go func(i int, a *arm) {
			got := make([]string, 0, perQueue)
			deadline := time.After(30 * time.Second)
			for len(got) < perQueue {
				select {
				case d, ok := <-a.recv:
					if !ok {
						results <- result{i, got, fmt.Errorf("delivery channel closed after %d/%d", len(got), perQueue)}
						return
					}
					got = append(got, string(d.Body))
					if d.ConsumerTag != sharedTag {
						results <- result{i, got, fmt.Errorf("delivery carried consumer tag %q; want the client-supplied %q", d.ConsumerTag, sharedTag)}
						return
					}
					if err := d.Ack(false); err != nil {
						results <- result{i, got, fmt.Errorf("ack: %w", err)}
						return
					}
				case <-deadline:
					results <- result{i, got, fmt.Errorf("consumer %d starved at %d/%d deliveries", i, len(got), perQueue)}
					return
				}
			}
			results <- result{i, got, nil}
		}(i, a)
	}

	for range arms {
		r := <-results
		if r.fail != nil {
			t.Fatalf("arm %d (queue %s): %v", r.idx, arms[r.idx].queue, r.fail)
		}
		// Cross-queue leakage check: every body must belong to this arm's queue.
		for _, body := range r.got {
			want := arms[r.idx].queue + "-"
			if len(body) < len(want) || body[:len(want)] != want {
				t.Fatalf("arm %d received %q, which belongs to another queue", r.idx, body)
			}
		}
	}
}

// TestConsumerTagScope_SameTagOnDifferentConnectionsBothConsume is the
// production shape of the defect: two instances of the SAME binary mint
// identical default consumer tags (amqp091-go derives them from os.Args[0]
// plus a package-local counter), so two independent clients silently clobber
// each other. Distinct connections are by construction distinct channels, so
// the spec says both must work.
func TestConsumerTagScope_SameTagOnDifferentConnectionsBothConsume(t *testing.T) {
	url, stop := ctagServer(t)
	defer stop()

	const (
		sharedTag = "identical-binary-tag"
		perQueue  = 20
	)

	type arm struct {
		conn  *amqp.Connection
		ch    *amqp.Channel
		queue string
		recv  <-chan amqp.Delivery
	}
	arms := make([]*arm, 2)
	for i := range arms {
		conn, err := amqp.Dial(url)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel %d: %v", i, err)
		}
		if err := ch.Qos(1, 0, false); err != nil {
			t.Fatalf("qos %d: %v", i, err)
		}
		qName := fmt.Sprintf("ctag-crossconn-q%d", i)
		if _, err := ch.QueueDeclare(qName, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %d: %v", i, err)
		}
		arms[i] = &arm{conn: conn, ch: ch, queue: qName}
	}

	for _, a := range arms {
		for j := 0; j < perQueue; j++ {
			body := fmt.Sprintf("%s-%d", a.queue, j)
			if err := a.ch.Publish("", a.queue, false, false,
				amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publish to %s: %v", a.queue, err)
			}
		}
	}

	for i, a := range arms {
		recv, err := a.ch.Consume(a.queue, sharedTag, false, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume %d with tag %q must succeed on a distinct connection: %v",
				i, sharedTag, err)
		}
		a.recv = recv
	}

	type result struct {
		idx  int
		n    int
		fail error
	}
	results := make(chan result, len(arms))
	for i, a := range arms {
		go func(i int, a *arm) {
			n := 0
			deadline := time.After(30 * time.Second)
			for n < perQueue {
				select {
				case d, ok := <-a.recv:
					if !ok {
						results <- result{i, n, fmt.Errorf("delivery channel closed after %d/%d", n, perQueue)}
						return
					}
					want := a.queue + "-"
					if body := string(d.Body); len(body) < len(want) || body[:len(want)] != want {
						results <- result{i, n, fmt.Errorf("received %q, which belongs to another queue", body)}
						return
					}
					n++
					if err := d.Ack(false); err != nil {
						results <- result{i, n, fmt.Errorf("ack: %w", err)}
						return
					}
				case <-deadline:
					results <- result{i, n, fmt.Errorf("consumer %d starved at %d/%d deliveries", i, n, perQueue)}
					return
				}
			}
			results <- result{i, n, nil}
		}(i, a)
	}

	for range arms {
		if r := <-results; r.fail != nil {
			t.Fatalf("arm %d (queue %s): %v", r.idx, arms[r.idx].queue, r.fail)
		}
	}
}
