package protocol

import (
	"regexp"
	"testing"
)

// ============================================================================
// GenerateQueueName tests
//
// AMQP 0.9.1 spec, queue.declare "default-name" rule:
//   "The queue name MAY be empty, in which case the server MUST create
//    a new queue with a unique generated name and return this to the
//    client in the Declare-Ok method."
//
// queue-name domain regexp: ^[a-zA-Z0-9-_.:]*$
// queue-name domain length assert: 127
//
// RabbitMQ convention: server-generated names use "amq.gen." prefix.
// ============================================================================

func TestGenerateQueueNamePrefix(t *testing.T) {
	name := GenerateQueueName()
	const prefix = "amq.gen."
	if len(name) <= len(prefix) || name[:len(prefix)] != prefix {
		t.Errorf("server-generated queue name should start with %q, got %q", prefix, name)
	}
}

func TestGenerateQueueNameUniqueness(t *testing.T) {
	names := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		name := GenerateQueueName()
		if names[name] {
			t.Errorf("server-generated queue name should be unique, got duplicate %q", name)
		}
		names[name] = true
	}
}

func TestGenerateQueueNameValidChars(t *testing.T) {
	// Per spec: queue-name regexp is ^[a-zA-Z0-9-_.:]*$
	validRe := regexp.MustCompile(`^[a-zA-Z0-9-_.:]*$`)
	for i := 0; i < 50; i++ {
		name := GenerateQueueName()
		if !validRe.MatchString(name) {
			t.Errorf("server-generated queue name %q contains invalid characters (spec regexp: ^[a-zA-Z0-9-_.:]*$)", name)
		}
	}
}

func TestGenerateQueueNameLengthWithinSpec(t *testing.T) {
	// Per spec: queue-name length assert is 127
	for i := 0; i < 50; i++ {
		name := GenerateQueueName()
		if len(name) > 127 {
			t.Errorf("server-generated queue name %q exceeds spec max length 127", name)
		}
	}
}

// ============================================================================
// Channel.CurrentQueue tests
//
// AMQP 0.9.1 spec, queue-name domain doc:
//   "In methods where the queue name may be blank, and that has no
//    specific significance, this refers to the 'current' queue for the
//    channel, meaning the last queue that the client declared on the
//    channel. If the client did not declare a queue, and the method
//    needs a queue name, this will result in a 502 (syntax error)
//    channel exception."
//
// The queue-known rule in queue.bind/unbind/purge/delete:
//   "The client MUST either specify a queue name or have previously
//    declared a queue on the same channel"
// ============================================================================

func TestChannelCurrentQueueDefault(t *testing.T) {
	ch := NewChannel(1, nil)
	if ch.CurrentQueue != "" {
		t.Errorf("new channel should have empty CurrentQueue, got %q", ch.CurrentQueue)
	}
}

func TestChannelCurrentQueueSetAfterDeclare(t *testing.T) {
	ch := NewChannel(1, nil)
	ch.Mutex.Lock()
	ch.CurrentQueue = "my-queue"
	ch.Mutex.Unlock()

	if ch.CurrentQueue != "my-queue" {
		t.Errorf("CurrentQueue should be 'my-queue', got %q", ch.CurrentQueue)
	}
}

func TestChannelCurrentQueueSetAfterGeneratedDeclare(t *testing.T) {
	ch := NewChannel(1, nil)
	generatedName := GenerateQueueName()
	ch.Mutex.Lock()
	ch.CurrentQueue = generatedName
	ch.Mutex.Unlock()

	if ch.CurrentQueue != generatedName {
		t.Errorf("CurrentQueue should be the generated name %q, got %q", generatedName, ch.CurrentQueue)
	}
	if ch.CurrentQueue[:len("amq.gen.")] != "amq.gen." {
		t.Errorf("CurrentQueue should be a server-generated name starting with 'amq.gen.', got %q", ch.CurrentQueue)
	}
}
