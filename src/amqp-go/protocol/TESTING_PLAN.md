# Protocol Package Testing Plan - AMQP 0.9.1 Compliance

## Goal
Achieve 100% test coverage and full RabbitMQ AMQP 0.9.1 spec compliance for the protocol package.

## Current Status
- **Current Coverage**: 2.5%
- **Lines of Code**: ~3,200 lines
- **Files**: 7 Go files (content.go, frame.go, methods.go, structures.go, transaction.go, durability.go)

## Testing Strategy

### 1. Frame-Level Testing (frame.go, frame_test.go)
**Target Coverage**: 100%

Test all frame types according to AMQP 0.9.1 spec:
- [ ] **Method frames** (type 1): Class ID + Method ID + arguments
- [ ] **Content header frames** (type 2): Class ID + weight + body size + properties
- [ ] **Content body frames** (type 3): Opaque binary payload
- [ ] **Heartbeat frames** (type 8): Empty frame for keepalive

**Test Cases**:
```go
// Frame marshaling/unmarshaling
- TestFrameMarshalBinary_AllTypes
- TestFrameUnmarshalBinary_AllTypes
- TestFrameUnmarshalBinary_InvalidMagicByte
- TestFrameUnmarshalBinary_TruncatedFrame
- TestFrameUnmarshalBinary_OversizedPayload
- TestReadFrame_Success
- TestReadFrame_EOF
- TestReadFrame_InvalidFrame
- TestWriteFrame_Success
- TestWriteFrame_Error
- TestHeartbeatFrame
```

### 2. Method Serialization Testing (methods.go)
**Target Coverage**: 100%

Test ALL AMQP methods defined in spec (80+ methods across 6 classes):

#### Connection Class (10 methods)
- [ ] Connection.Start / Connection.StartOk
- [ ] Connection.Secure / Connection.SecureOk
- [ ] Connection.Tune / Connection.TuneOk
- [ ] Connection.Open / Connection.OpenOk
- [ ] Connection.Close / Connection.CloseOk

#### Channel Class (6 methods)
- [ ] Channel.Open / Channel.OpenOk
- [ ] Channel.Flow / Channel.FlowOk
- [ ] Channel.Close / Channel.CloseOk

#### Exchange Class (8 methods)
- [ ] Exchange.Declare / Exchange.DeclareOk
- [ ] Exchange.Delete / Exchange.DeleteOk
- [ ] Exchange.Bind / Exchange.BindOk
- [ ] Exchange.Unbind / Exchange.UnbindOk

#### Queue Class (10 methods)
- [ ] Queue.Declare / Queue.DeclareOk
- [ ] Queue.Bind / Queue.BindOk
- [ ] Queue.Unbind / Queue.UnbindOk
- [ ] Queue.Purge / Queue.PurgeOk
- [ ] Queue.Delete / Queue.DeleteOk

#### Basic Class (16 methods)
- [ ] Basic.Qos / Basic.QosOk
- [ ] Basic.Consume / Basic.ConsumeOk
- [ ] Basic.Cancel / Basic.CancelOk
- [ ] Basic.Publish
- [ ] Basic.Return
- [ ] Basic.Deliver
- [ ] Basic.Get / Basic.GetOk / Basic.GetEmpty
- [ ] Basic.Ack
- [ ] Basic.Reject
- [ ] Basic.RecoverAsync
- [ ] Basic.Recover / Basic.RecoverOk
- [ ] Basic.Nack

#### Transaction Class (6 methods)
- [ ] Tx.Select / Tx.SelectOk
- [ ] Tx.Commit / Tx.CommitOk
- [ ] Tx.Rollback / Tx.RollbackOk

**Test Pattern** (for each method):
```go
func TestMethodName_Serialize(t *testing.T) {
    method := &MethodName{
        Field1: value1,
        Field2: value2,
        // ... all fields with various edge cases
    }

    // Test serialization
    buf := &bytes.Buffer{}
    err := method.Serialize(buf)
    assert.NoError(t, err)

    // Verify binary format matches AMQP spec
    // Check class ID, method ID, field encoding
}

func TestMethodName_RoundTrip(t *testing.T) {
    // Serialize then deserialize, verify equality
}

func TestMethodName_EdgeCases(t *testing.T) {
    // Empty strings, nil values, max values, etc.
}
```

### 3. Content Header Testing (content.go)
**Target Coverage**: 100%

Test content header encoding/decoding with all 14 basic properties:
- [ ] content-type (shortstr)
- [ ] content-encoding (shortstr)
- [ ] headers (field-table)
- [ ] delivery-mode (octet) - 1=non-persistent, 2=persistent
- [ ] priority (octet) - 0-9
- [ ] correlation-id (shortstr)
- [ ] reply-to (shortstr)
- [ ] expiration (shortstr)
- [ ] message-id (shortstr)
- [ ] timestamp (timestamp - 64-bit POSIX)
- [ ] type (shortstr)
- [ ] user-id (shortstr)
- [ ] app-id (shortstr)
- [ ] cluster-id (shortstr)

**Test Cases**:
```go
- TestContentHeader_AllProperties
- TestContentHeader_PropertyFlagEncoding
- TestContentHeader_EmptyProperties
- TestContentHeader_MaxSizeProperties
- TestContentHeader_RoundTrip
- TestReadContentHeader_Success
- TestReadContentHeader_TruncatedData
```

### 4. Field Table Testing (methods.go)
**Target Coverage**: 100%

Test field table encoding/decoding with all AMQP types:
- [ ] Boolean (t/T)
- [ ] Byte (b/B)
- [ ] Short (s/u)
- [ ] Long (I/i)
- [ ] Long-long (L/l)
- [ ] Float (f)
- [ ] Double (d)
- [ ] Decimal (D)
- [ ] Short string (s)
- [ ] Long string (S)
- [ ] Field array (A)
- [ ] Timestamp (T)
- [ ] Field table (F)
- [ ] Void (V)

**Test Cases**:
```go
- TestEncodeFieldTable_AllTypes
- TestDecodeFieldTable_AllTypes
- TestFieldTable_NestedTables
- TestFieldTable_EmptyTable
- TestFieldTable_LargeTable
- TestFieldTable_InvalidType
```

### 5. Transaction Methods Testing (transaction.go)
**Target Coverage**: 100%

Test all transaction-related method serialization:
```go
- TestTxSelect_Serialize
- TestTxSelectOk_Serialize
- TestTxCommit_Serialize
- TestTxCommitOk_Serialize
- TestTxRollback_Serialize
- TestTxRollbackOk_Serialize
```

### 6. AMQP Spec Compliance Testing

Create a comprehensive compliance test suite:

```go
// protocol/compliance_test.go

func TestAMQPCompliance_FrameFormat(t *testing.T) {
    // Verify frame format: type(1) + channel(2) + size(4) + payload + 0xCE
}

func TestAMQPCompliance_MaxFrameSize(t *testing.T) {
    // Verify frames respect max-frame-size from tune
}

func TestAMQPCompliance_ProtocolHeader(t *testing.T) {
    // Verify protocol header: "AMQP" + 0 + 0 + 9 + 1
}

func TestAMQPCompliance_MethodEncoding(t *testing.T) {
    // Test every method follows: class-id(2) + method-id(2) + arguments
}

func TestAMQPCompliance_ContentEncoding(t *testing.T) {
    // Test content split into: header frame + body frames
}

func TestAMQPCompliance_StringEncoding(t *testing.T) {
    // Short string: length(1) + octets
    // Long string: length(4) + octets
}

func TestAMQPCompliance_FieldTableEncoding(t *testing.T) {
    // Field table: size(4) + name-value pairs
}
```

### 7. RabbitMQ Interoperability Testing

Create tests that validate against RabbitMQ's expectations:

```go
// protocol/rabbitmq_compat_test.go

func TestRabbitMQCompat_ConnectionNegotiation(t *testing.T) {
    // Test connection handshake sequence
}

func TestRabbitMQCompat_CapabilitiesExchange(t *testing.T) {
    // Test server-properties and client-properties exchange
}

func TestRabbitMQCompat_FrameMaxNegotiation(t *testing.T) {
    // Test frame-max and heartbeat negotiation
}
```

### 8. Fuzz Testing

Add fuzz tests for robustness:

```go
func FuzzFrameUnmarshal(f *testing.F) {
    // Fuzz frame unmarshaling with random bytes
}

func FuzzFieldTableDecode(f *testing.F) {
    // Fuzz field table decoding
}

func FuzzMethodDeserialize(f *testing.F) {
    // Fuzz method deserialization
}
```

## Implementation Order

1. **Week 1: Frame Testing**
   - Complete frame.go test coverage (100%)
   - Add all frame type tests
   - Add error handling tests

2. **Week 2: Method Serialization (Part 1)**
   - Connection class (10 methods)
   - Channel class (6 methods)
   - Exchange class (8 methods)

3. **Week 3: Method Serialization (Part 2)**
   - Queue class (10 methods)
   - Basic class (16 methods)
   - Transaction class (6 methods)

4. **Week 4: Content & Field Tables**
   - Content header with all 14 properties
   - Field table with all 13 types
   - Nested structures

5. **Week 5: Compliance & Fuzz Testing**
   - AMQP spec compliance tests
   - RabbitMQ compatibility tests
   - Fuzz testing setup

## Success Criteria

- ✅ 100% code coverage for protocol package
- ✅ All 80+ AMQP methods tested
- ✅ All 14 content properties tested
- ✅ All 13 field table types tested
- ✅ Compliance tests validate spec adherence
- ✅ Fuzz tests find no crashes
- ✅ RabbitMQ official client can interoperate

## Reference Documents

- AMQP 0.9.1 Specification: https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
- RabbitMQ Protocol Reference: https://www.rabbitmq.com/protocol.html
- RabbitMQ Extensions: https://www.rabbitmq.com/extensions.html

## Tools & Resources

- RabbitMQ official Go client for comparison: github.com/rabbitmq/amqp091-go
- Protocol trace analyzer: Wireshark with AMQP dissector
- Reference implementation: RabbitMQ server source code
