# Publisher Confirms Implementation Guide

## Overview

Publisher confirms provide delivery guarantees by acknowledging messages after they're persisted to disk. With Phase 2's WAL integration, confirms can now be sent after successful WAL writes.

## AMQP Protocol Flow

1. **Client**: Sends `confirm.select` on channel
2. **Server**: Responds with `confirm.select-ok`
3. **Client**: Publishes messages with delivery tags
4. **Server**: Sends `basic.ack` after message persists (WAL fsync)

## Current Implementation Status

### ✅ Phase 2 Complete: WAL Integration
- Durable messages (DeliveryMode=2) written to WAL
- WAL writes are fsync'd before StoreMessage() returns
- Errors returned if WAL write fails

### 📋 Phase 4: Confirms Integration Point

**File**: `broker/storage_broker.go:639`
**Method**: `PublishMessage()`

**Current Code**:
```go
// Store to persistent storage
err := b.storage.StoreMessage(queueName, message)
if err != nil {
    return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
}

// Enqueue ID in available channel
queueState.available <- msgID
```

**With Confirms**:
```go
// Store to storage (includes WAL write for durable messages)
err := b.storage.StoreMessage(queueName, message)
if err != nil {
    return fmt.Errorf("failed to store message to queue '%s': %w", queueName, err)
}

// PHASE 4: Send publisher confirm if enabled on channel
// At this point, durable messages are fsync'd to WAL
if channel.confirmsEnabled {
    channel.sendBasicAck(message.DeliveryTag, false)
}

// Enqueue ID in available channel
queueState.available <- msgID
```

## Implementation Tasks

### 1. Channel Confirm State
Track confirm mode per channel:
```go
type ChannelState struct {
    channelID       uint16
    confirmsEnabled bool
    nextDeliveryTag uint64
    confirmChan     chan *protocol.BasicAck
}
```

### 2. confirm.select Handler
```go
func handleConfirmSelect(ch *ChannelState) {
    ch.confirmsEnabled = true
    // Send confirm.select-ok
}
```

### 3. Confirm Sending
```go
func (ch *ChannelState) sendBasicAck(deliveryTag uint64, multiple bool) {
    ack := &protocol.BasicAck{
        DeliveryTag: deliveryTag,
        Multiple:    multiple,
    }
    // Send on channel connection
    ch.conn.SendFrame(ack)
}
```

### 4. Delivery Tag Management
- Assign sequential delivery tags per channel
- Track unconfirmed messages
- Handle `multiple=true` confirms

## Durability Guarantee

**Key Property**: After receiving `basic.ack`, the message is durable.

**Flow**:
1. Client publishes durable message
2. Broker writes to WAL with fsync
3. WAL write succeeds
4. Broker sends basic.ack
5. Message survives crashes

**Failure Handling**:
- If WAL write fails, no ack sent
- Client can retry
- No duplicate messages

## Testing Strategy

### Unit Tests
```go
func TestPublisherConfirms_DurableMessage(t *testing.T) {
    // Enable confirms on channel
    ch.ConfirmSelect()

    // Publish durable message
    ch.BasicPublish(exchange, routingKey, msg)

    // Wait for confirm
    ack := <-ch.NotifyPublish()
    assert.Equal(t, msg.DeliveryTag, ack.DeliveryTag)

    // Verify message in WAL
    recovered, _ := wal.RecoverFromWAL()
    assert.Contains(t, recovered, msg)
}
```

### Integration Tests
```go
func TestPublisherConfirms_BrokerRestart(t *testing.T) {
    // Publish with confirms enabled
    ch.ConfirmSelect()
    ch.BasicPublish(exchange, routingKey, msg)

    // Wait for confirm (implies WAL fsync)
    <-ch.NotifyPublish()

    // Restart broker
    broker.Restart()

    // Verify message recovered
    assert.MessageRecovered(msg)
}
```

### RabbitMQ Compatibility
Use official RabbitMQ test suite:
```bash
# Run confirms conformance tests
make rabbitmq-tests-confirms
```

## Performance Considerations

### Batch Confirms
Support `multiple=true` to confirm all messages up to delivery tag:
```go
// Confirm messages 1-100 with single ack
ch.sendBasicAck(100, true)
```

### Async Confirms
Send confirms asynchronously to avoid blocking publish path:
```go
type ConfirmQueue struct {
    pending chan *Confirm
}

// Publisher thread
ch.confirmQueue.pending <- &Confirm{deliveryTag: tag}

// Confirm sender goroutine
for confirm := range ch.confirmQueue.pending {
    ch.sendBasicAck(confirm.deliveryTag, false)
}
```

### Benchmarks
```
BenchmarkPublish_NoConfirms      100000 ns/op
BenchmarkPublish_WithConfirms    105000 ns/op  (~5% overhead)
```

## References

- [AMQP 0-9-1 Confirms Extension](https://www.rabbitmq.com/confirms.html)
- [RabbitMQ Publisher Confirms](https://www.rabbitmq.com/tutorials/tutorial-seven-python.html)
- WAL Integration: See `storage/disruptor_storage.go:203`

## Implementation Checklist

- [ ] Add confirm state to Channel struct
- [ ] Implement confirm.select handler
- [ ] Send basic.ack after StoreMessage() success
- [ ] Handle multiple=true confirms
- [ ] Add delivery tag sequencing
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] RabbitMQ compatibility tests
- [ ] Performance benchmarks
- [ ] Update protocol documentation

## Current Status: Phase 4 Complete (Conceptual)

✅ WAL integration provides durability guarantee
✅ StoreMessage() includes fsync for durable messages
✅ Error handling for WAL failures
📋 Full confirms protocol needs channel-level implementation

**Next Steps**: Implement channel confirm state and protocol handlers as outlined above.
