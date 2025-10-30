# Protocol Package - 100% Coverage & AMQP Compliance Plan

## Executive Summary

**Current Status**: 2.5% code coverage
**Goal**: 100% code coverage + Full AMQP 0.9.1 compliance
**Estimated Effort**: 3-4 weeks
**Priority**: High (for production readiness)

## Why Protocol Testing Matters

The protocol package is the **heart of AMQP compliance**. It handles:
- Binary frame encoding/decoding (AMQP wire protocol)
- 80+ method serialization/deserialization
- Content header property encoding
- Field table encoding (13 data types)
- Transaction frame handling

**Current Situation**:
- Integration tests validate protocol works end-to-end
- Unit tests for protocol internals are minimal (2.5%)
- No explicit AMQP spec compliance validation

**Risk**: Edge cases in binary encoding could cause:
- Interoperability issues with other AMQP clients
- Data corruption in field tables or headers
- Protocol violations rejected by strict clients

## Current State Analysis

### Package Structure
```
protocol/
├── content.go          (9,380 bytes)  - Content header encoding
├── durability.go       (1,677 bytes)  - Durability constants
├── frame.go           (2,770 bytes)  - Frame marshaling
├── frame_test.go      (1,945 bytes)  - Basic frame tests ✓
├── methods.go        (62,014 bytes)  - 80+ AMQP methods
├── structures.go      (4,701 bytes)  - Data structures
└── transaction.go     (5,721 bytes)  - Transaction methods
```

### Coverage Breakdown

| File | Lines | Coverage | Critical Functions Untested |
|------|-------|----------|----------------------------|
| frame.go | ~100 | 78.6% | WriteFrame (0%), FrameEnd validation |
| methods.go | ~2,400 | 0% | ALL method Serialize/Deserialize |
| content.go | ~300 | 0% | ReadContentHeader, Property encoding |
| transaction.go | ~200 | 0% | ALL Tx method serialization |
| structures.go | ~150 | 0% | Field parsing, Message structures |

### Method Inventory

**80+ AMQP Methods Across 6 Classes**:

1. **Connection** (10): Start, StartOk, Secure, SecureOk, Tune, TuneOk, Open, OpenOk, Close, CloseOk
2. **Channel** (6): Open, OpenOk, Flow, FlowOk, Close, CloseOk
3. **Exchange** (8): Declare, DeclareOk, Delete, DeleteOk, Bind, BindOk, Unbind, UnbindOk
4. **Queue** (10): Declare, DeclareOk, Bind, BindOk, Unbind, UnbindOk, Purge, PurgeOk, Delete, DeleteOk
5. **Basic** (16): Qos, QosOk, Consume, ConsumeOk, Cancel, CancelOk, Publish, Return, Deliver, Get, GetOk, GetEmpty, Ack, Reject, RecoverAsync, Recover, RecoverOk, Nack
6. **Tx** (6): Select, SelectOk, Commit, CommitOk, Rollback, RollbackOk

**Current Test Status**: 0 out of 80+ methods have unit tests

## Implementation Plan

### Phase 1: Frame Testing (Week 1)
**Goal**: 100% coverage for frame.go

**Tasks**:
1. ✅ Test all 4 frame types (Method, Header, Body, Heartbeat)
2. ✅ Test frame marshaling/unmarshaling edge cases
3. ✅ Test error conditions (truncated, invalid end marker)
4. ✅ Test WriteFrame function
5. ✅ Test multi-frame sequences
6. ✅ Test frame size limits

**Deliverable**: frame.go at 100% coverage

### Phase 2: Method Serialization - Connection & Channel (Week 2)
**Goal**: Test all Connection and Channel class methods

**Tasks**:
1. Test Connection class (10 methods)
   - ConnectionStartMethod - server properties, mechanisms, locales
   - ConnectionStartOKMethod - client properties, SASL response
   - ConnectionTuneMethod - channel-max, frame-max, heartbeat
   - ConnectionTuneOKMethod - negotiated values
   - ConnectionOpenMethod - vhost
   - ConnectionOpenOKMethod - confirmation
   - ConnectionCloseMethod - reply code/text
   - ConnectionCloseOKMethod - confirmation

2. Test Channel class (6 methods)
   - ChannelOpenMethod
   - ChannelOpenOKMethod
   - ChannelFlowMethod - active flag
   - ChannelFlowOKMethod
   - ChannelCloseMethod - reply code/text
   - ChannelCloseOKMethod

**Test Pattern for Each Method**:
```go
func TestMethodName_Serialize(t *testing.T) {
    // 1. Create method with all fields populated
    // 2. Serialize to buffer
    // 3. Verify class ID and method ID (first 4 bytes)
    // 4. Verify field encoding follows AMQP spec
}

func TestMethodName_RoundTrip(t *testing.T) {
    // 1. Create method
    // 2. Serialize
    // 3. Deserialize
    // 4. Verify equality
}

func TestMethodName_EdgeCases(t *testing.T) {
    // Test: empty strings, nil maps, max values, special chars
}
```

**Deliverable**: 16 methods tested, ~48 test functions

### Phase 3: Method Serialization - Exchange & Queue (Week 3)
**Goal**: Test Exchange and Queue class methods

**Tasks**:
1. Test Exchange class (8 methods)
   - ExchangeDeclareMethod - type, durability, auto-delete, arguments
   - ExchangeDeclareOKMethod
   - ExchangeDeleteMethod - if-unused flag
   - ExchangeDeleteOKMethod
   - ExchangeBindMethod - source, destination, routing key
   - ExchangeBindOKMethod
   - ExchangeUnbindMethod
   - ExchangeUnbindOKMethod

2. Test Queue class (10 methods)
   - QueueDeclareMethod - passive, durable, exclusive, auto-delete, arguments
   - QueueDeclareOKMethod - queue name, message count, consumer count
   - QueueBindMethod - queue, exchange, routing key, arguments
   - QueueBindOKMethod
   - QueueUnbindMethod
   - QueueUnbindOKMethod
   - QueuePurgeMethod
   - QueuePurgeOKMethod - message count
   - QueueDeleteMethod - if-unused, if-empty
   - QueueDeleteOKMethod - message count

**Deliverable**: 18 methods tested, ~54 test functions

### Phase 4: Method Serialization - Basic & Tx (Week 3-4)
**Goal**: Test Basic and Transaction class methods

**Tasks**:
1. Test Basic class (16 methods)
   - BasicQosMethod - prefetch size/count, global
   - BasicQosOKMethod
   - BasicConsumeMethod - queue, consumer tag, no-ack, exclusive, arguments
   - BasicConsumeOKMethod - consumer tag
   - BasicCancelMethod - consumer tag
   - BasicCancelOKMethod
   - BasicPublishMethod - exchange, routing key, mandatory, immediate
   - BasicReturnMethod - reply code/text, exchange, routing key
   - BasicDeliverMethod - consumer tag, delivery tag, redelivered, exchange, routing key
   - BasicGetMethod - queue
   - BasicGetOKMethod - delivery tag, redelivered, exchange, routing key, message count
   - BasicGetEmptyMethod
   - BasicAckMethod - delivery tag, multiple
   - BasicRejectMethod - delivery tag, requeue
   - BasicRecoverAsyncMethod - requeue
   - BasicRecoverMethod - requeue
   - BasicRecoverOKMethod
   - BasicNackMethod - delivery tag, multiple, requeue

2. Test Tx class (6 methods)
   - TxSelectMethod
   - TxSelectOKMethod
   - TxCommitMethod
   - TxCommitOKMethod
   - TxRollbackMethod
   - TxRollbackOKMethod

**Deliverable**: 22 methods tested, ~66 test functions

### Phase 5: Content Headers & Field Tables (Week 4)
**Goal**: Test content header and field table encoding

**Tasks**:
1. Content Header Testing
   - Test all 14 basic properties
   - Test property flag encoding (bitmask)
   - Test empty vs fully populated headers
   - Test ReadContentHeader function
   - Test Serialize function

2. Field Table Testing
   - Test all 13 AMQP field types:
     * Boolean (t/T)
     * Byte (b/B)
     * Short/UShort (s/u)
     * Long/ULong (I/i)
     * LongLong/ULongLong (L/l)
     * Float (f)
     * Double (d)
     * Decimal (D)
     * ShortString (s)
     * LongString (S)
     * FieldArray (A)
     * Timestamp (T)
     * FieldTable (F - nested)
     * Void (V)
   - Test nested field tables
   - Test empty field tables
   - Test large field tables
   - Test encoding/decoding round trips

**Deliverable**: Content and field table coverage at 100%

### Phase 6: AMQP Spec Compliance Testing (Week 4-5)
**Goal**: Validate strict AMQP 0.9.1 compliance

**Tasks**:
1. Create compliance_test.go with spec validation tests
2. Test frame format compliance
   - Type + Channel + Size + Payload + 0xCE
3. Test method encoding compliance
   - Class ID (2 bytes) + Method ID (2 bytes) + Arguments
4. Test string encoding
   - Short string: length(1) + octets
   - Long string: length(4) + octets
5. Test field table format
   - Size(4) + name-value pairs
6. Test protocol header
   - "AMQP" + 0 + 0 + 9 + 1
7. Test content encoding
   - Header frame + body frames split

**Deliverable**: Comprehensive compliance test suite

### Phase 7: Fuzz Testing & RabbitMQ Validation (Week 5)
**Goal**: Find edge cases and validate interoperability

**Tasks**:
1. Add fuzz tests
   - FuzzFrameUnmarshal
   - FuzzFieldTableDecode
   - FuzzMethodDeserialize
2. Create RabbitMQ compatibility tests
   - Test against rabbitmq/amqp091-go expectations
   - Validate capability exchange
   - Test frame-max negotiation
3. Integration with Wireshark
   - Capture and analyze protocol traces
   - Verify binary format matches spec

**Deliverable**: Fuzz tests + validation report

## Success Metrics

- ✅ Protocol package coverage: 2.5% → 100%
- ✅ All 80+ methods have unit tests
- ✅ All 14 content properties tested
- ✅ All 13 field table types tested
- ✅ Compliance test suite passes
- ✅ Fuzz testing finds no crashes
- ✅ RabbitMQ client interoperability validated

## Test Quality Checklist

For each method test:
- [ ] Tests with all fields populated
- [ ] Tests with empty/nil values
- [ ] Tests with max values
- [ ] Tests with special characters
- [ ] Tests class ID and method ID
- [ ] Tests round-trip serialization
- [ ] Tests error conditions

## Tools & Resources

1. **AMQP 0.9.1 Specification**
   - PDF: https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
   - XML: https://www.rabbitmq.com/resources/specs/amqp0-9-1.extended.xml

2. **Reference Implementation**
   - RabbitMQ Go client: github.com/rabbitmq/amqp091-go
   - RabbitMQ server source: github.com/rabbitmq/rabbitmq-server

3. **Protocol Analysis**
   - Wireshark with AMQP dissector
   - tcpdump for packet capture

4. **Testing Tools**
   - Go testing package
   - Go fuzz testing (go test -fuzz)
   - testify/assert for assertions

## Implementation Notes

### Discovered Issues

1. **Method Naming**: Structs use `MethodNameMethod` pattern (e.g., `ConnectionStartMethod`)
2. **Frame End Constant**: Hardcoded as `0xCE`, not exported constant
3. **Field Table Types**: Need to map Go types to AMQP type indicators
4. **Property Flags**: 16-bit bitmask for content header properties

### Code Structure

```go
// Current structure in methods.go
type ConnectionStartMethod struct {
    VersionMajor byte
    VersionMinor byte
    ServerProperties map[string]interface{}
    Mechanisms string
    Locales string
}

func (m *ConnectionStartMethod) Serialize(buf *bytes.Buffer) error {
    // Encoding logic
}
```

### Testing Template

```go
func TestConnectionStartMethod_Serialize(t *testing.T) {
    method := &ConnectionStartMethod{
        VersionMajor: 0,
        VersionMinor: 9,
        ServerProperties: map[string]interface{}{
            "product": "AMQP-Go",
        },
        Mechanisms: "PLAIN",
        Locales: "en_US",
    }

    buf := &bytes.Buffer{}
    err := method.Serialize(buf)
    require.NoError(t, err)

    data := buf.Bytes()
    require.GreaterOrEqual(t, len(data), 4)

    // Verify class ID (10) and method ID (10)
    classID := binary.BigEndian.Uint16(data[0:2])
    methodID := binary.BigEndian.Uint16(data[2:4])

    assert.Equal(t, uint16(10), classID)
    assert.Equal(t, uint16(10), methodID)
}
```

## Appendix: AMQP 0.9.1 Quick Reference

### Frame Types
- Type 1: Method frame
- Type 2: Content header frame
- Type 3: Content body frame
- Type 8: Heartbeat frame

### Class IDs
- 10: Connection
- 20: Channel
- 40: Exchange
- 50: Queue
- 60: Basic
- 90: Tx

### Content Properties (14 total)
1. content-type
2. content-encoding
3. headers (field-table)
4. delivery-mode (1=transient, 2=persistent)
5. priority (0-9)
6. correlation-id
7. reply-to
8. expiration
9. message-id
10. timestamp
11. type
12. user-id
13. app-id
14. cluster-id (deprecated)

### Field Table Type Indicators
- 't' / 'T': Boolean
- 'b' / 'B': Signed/Unsigned byte
- 's' / 'u': Short/UShort
- 'I' / 'i': Long/ULong
- 'L' / 'l': LongLong/ULongLong
- 'f': Float
- 'd': Double
- 'D': Decimal
- 's': ShortString
- 'S': LongString
- 'A': Array
- 'T': Timestamp
- 'F': FieldTable (nested)
- 'V': Void
