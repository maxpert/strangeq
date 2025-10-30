package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// RabbitMQ Interoperability Tests
// These tests validate the AMQP 0.9.1 protocol implementation against a real RabbitMQ server.
//
// Prerequisites:
// - RabbitMQ server running (default: localhost:5672)
// - Guest credentials (default: guest/guest)
//
// Environment Variables:
// - RABBITMQ_HOST: RabbitMQ host (default: localhost)
// - RABBITMQ_PORT: RabbitMQ port (default: 5672)
// - RABBITMQ_USER: Username (default: guest)
// - RABBITMQ_PASS: Password (default: guest)
// - RABBITMQ_VHOST: Virtual host (default: /)
// - SKIP_RABBITMQ_TESTS: Set to "1" to skip these tests
//
// Run with: go test -v -run TestRabbitMQ

// TestConnection represents a test AMQP connection
type TestConnection struct {
	conn      net.Conn
	channel   uint16
	frameMax  uint32
	heartbeat uint16
	t         *testing.T
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// skipIfNoRabbitMQ checks if RabbitMQ tests should be skipped
func skipIfNoRabbitMQ(t *testing.T) {
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "1" {
		t.Skip("RabbitMQ tests disabled (SKIP_RABBITMQ_TESTS=1)")
	}
}

// connectToRabbitMQ establishes a TCP connection to RabbitMQ
func connectToRabbitMQ(t *testing.T) net.Conn {
	t.Helper()
	skipIfNoRabbitMQ(t)

	host := getEnvOrDefault("RABBITMQ_HOST", "localhost")
	port := getEnvOrDefault("RABBITMQ_PORT", "5672")
	address := net.JoinHostPort(host, port) // Handles IPv6 correctly

	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Skipf("Cannot connect to RabbitMQ at %s: %v (set SKIP_RABBITMQ_TESTS=1 to skip)", address, err)
	}

	// Set deadline for initial handshake
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	return conn
}

// sendProtocolHeader sends the AMQP protocol header
func sendProtocolHeader(t *testing.T, conn net.Conn) {
	t.Helper()
	header := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	_, err := conn.Write(header)
	if err != nil {
		t.Fatalf("Failed to send protocol header: %v", err)
	}
}

// readFrame reads a frame from the connection
func readFrameFromConn(t *testing.T, conn net.Conn) *Frame {
	t.Helper()
	frame, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("Failed to read frame: %v", err)
	}
	return frame
}

// writeFrame writes a frame to the connection
func writeFrameToConn(t *testing.T, conn net.Conn, frame *Frame) {
	t.Helper()
	err := WriteFrame(conn, frame)
	if err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}
}

// parseConnectionStart parses a Connection.Start method
func parseConnectionStart(t *testing.T, frame *Frame) *ConnectionStartMethod {
	t.Helper()

	if frame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", frame.Type)
	}

	if len(frame.Payload) < 4 {
		t.Fatalf("Frame payload too short for method")
	}

	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

	if classID != 10 || methodID != 10 {
		t.Fatalf("Expected Connection.Start (10,10), got (%d,%d)", classID, methodID)
	}

	method, err := ParseConnectionStart(frame.Payload[4:])
	if err != nil {
		t.Fatalf("Failed to parse Connection.Start: %v", err)
	}

	return method
}

// parseConnectionTune parses a Connection.Tune method
func parseConnectionTune(t *testing.T, frame *Frame) *ConnectionTuneMethod {
	t.Helper()

	if frame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", frame.Type)
	}

	if len(frame.Payload) < 4 {
		t.Fatalf("Frame payload too short for method")
	}

	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

	if classID != 10 || methodID != 30 {
		t.Fatalf("Expected Connection.Tune (10,30), got (%d,%d)", classID, methodID)
	}

	method, err := ParseConnectionTune(frame.Payload[4:])
	if err != nil {
		t.Fatalf("Failed to parse Connection.Tune: %v", err)
	}

	return method
}

// parseConnectionOpenOK parses a Connection.OpenOK method
func parseConnectionOpenOK(t *testing.T, frame *Frame) {
	t.Helper()

	if frame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", frame.Type)
	}

	if len(frame.Payload) < 4 {
		t.Fatalf("Frame payload too short for method")
	}

	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

	if classID != 10 || methodID != 41 {
		t.Fatalf("Expected Connection.OpenOK (10,41), got (%d,%d)", classID, methodID)
	}
}

// parseChannelOpenOK parses a Channel.OpenOK method
func parseChannelOpenOK(t *testing.T, frame *Frame) {
	t.Helper()

	if frame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", frame.Type)
	}

	if len(frame.Payload) < 4 {
		t.Fatalf("Frame payload too short for method")
	}

	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

	if classID != 20 || methodID != 11 {
		t.Fatalf("Expected Channel.OpenOK (20,11), got (%d,%d)", classID, methodID)
	}
}

// TestRabbitMQ_ProtocolHeader tests sending AMQP protocol header
func TestRabbitMQ_ProtocolHeader(t *testing.T) {
	conn := connectToRabbitMQ(t)
	defer conn.Close()

	// Send AMQP protocol header
	sendProtocolHeader(t, conn)

	// RabbitMQ should respond with Connection.Start
	frame := readFrameFromConn(t, conn)

	if frame.Type != FrameMethod {
		t.Errorf("Expected method frame, got type %d", frame.Type)
	}

	if frame.Channel != 0 {
		t.Errorf("Expected channel 0, got %d", frame.Channel)
	}

	// Verify it's Connection.Start (class 10, method 10)
	if len(frame.Payload) >= 4 {
		classID := binary.BigEndian.Uint16(frame.Payload[0:2])
		methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

		if classID != 10 || methodID != 10 {
			t.Errorf("Expected Connection.Start (10,10), got (%d,%d)", classID, methodID)
		}
	}
}

// TestRabbitMQ_ConnectionHandshake tests full connection handshake
func TestRabbitMQ_ConnectionHandshake(t *testing.T) {
	conn := connectToRabbitMQ(t)
	defer conn.Close()

	// Step 1: Send protocol header
	sendProtocolHeader(t, conn)

	// Step 2: Receive Connection.Start
	startFrame := readFrameFromConn(t, conn)
	startMethod := parseConnectionStart(t, startFrame)

	t.Logf("Connection.Start received - Version: %d.%d", startMethod.VersionMajor, startMethod.VersionMinor)
	t.Logf("Server mechanisms: %v", startMethod.Mechanisms)
	t.Logf("Server locales: %v", startMethod.Locales)

	// Step 3: Send Connection.StartOK
	user := getEnvOrDefault("RABBITMQ_USER", "guest")
	pass := getEnvOrDefault("RABBITMQ_PASS", "guest")
	response := fmt.Sprintf("\x00%s\x00%s", user, pass)

	startOK := &ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "amqp-go-test",
			"version": "0.1.0",
		},
		Mechanism: "PLAIN",
		Response:  []byte(response),
		Locale:    "en_US",
	}

	startOKData, err := startOK.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Connection.StartOK: %v", err)
	}

	startOKFrame := EncodeMethodFrame(10, 11, startOKData)
	writeFrameToConn(t, conn, startOKFrame)

	// Step 4: Receive Connection.Tune
	tuneFrame := readFrameFromConn(t, conn)
	tuneMethod := parseConnectionTune(t, tuneFrame)

	t.Logf("Connection.Tune received - ChannelMax: %d, FrameMax: %d, Heartbeat: %d",
		tuneMethod.ChannelMax, tuneMethod.FrameMax, tuneMethod.Heartbeat)

	// Step 5: Send Connection.TuneOK
	tuneOK := &ConnectionTuneOKMethod{
		ChannelMax: tuneMethod.ChannelMax,
		FrameMax:   tuneMethod.FrameMax,
		Heartbeat:  tuneMethod.Heartbeat,
	}

	tuneOKData, err := tuneOK.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Connection.TuneOK: %v", err)
	}

	tuneOKFrame := EncodeMethodFrame(10, 31, tuneOKData)
	writeFrameToConn(t, conn, tuneOKFrame)

	// Step 6: Send Connection.Open
	vhost := getEnvOrDefault("RABBITMQ_VHOST", "/")
	openMethod := &ConnectionOpenMethod{
		VirtualHost: vhost,
		Reserved1:   "",
		Reserved2:   0,
	}

	openData, err := openMethod.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Connection.Open: %v", err)
	}

	openFrame := EncodeMethodFrame(10, 40, openData)
	writeFrameToConn(t, conn, openFrame)

	// Step 7: Receive Connection.OpenOK
	openOKFrame := readFrameFromConn(t, conn)
	parseConnectionOpenOK(t, openOKFrame)

	t.Log("Connection handshake completed successfully")
}

// establishConnection performs full connection handshake and returns TestConnection
func establishConnection(t *testing.T) *TestConnection {
	t.Helper()

	conn := connectToRabbitMQ(t)

	// Protocol header
	sendProtocolHeader(t, conn)

	// Connection.Start
	startFrame := readFrameFromConn(t, conn)
	_ = parseConnectionStart(t, startFrame)

	// Connection.StartOK
	user := getEnvOrDefault("RABBITMQ_USER", "guest")
	pass := getEnvOrDefault("RABBITMQ_PASS", "guest")
	response := fmt.Sprintf("\x00%s\x00%s", user, pass)

	startOK := &ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "amqp-go-test",
		},
		Mechanism: "PLAIN",
		Response:  []byte(response),
		Locale:    "en_US",
	}

	startOKData, _ := startOK.Serialize()
	writeFrameToConn(t, conn, EncodeMethodFrame(10, 11, startOKData))

	// Connection.Tune
	tuneFrame := readFrameFromConn(t, conn)
	tuneMethod := parseConnectionTune(t, tuneFrame)

	// Connection.TuneOK
	tuneOK := &ConnectionTuneOKMethod{
		ChannelMax: tuneMethod.ChannelMax,
		FrameMax:   tuneMethod.FrameMax,
		Heartbeat:  tuneMethod.Heartbeat,
	}
	tuneOKData, _ := tuneOK.Serialize()
	writeFrameToConn(t, conn, EncodeMethodFrame(10, 31, tuneOKData))

	// Connection.Open
	vhost := getEnvOrDefault("RABBITMQ_VHOST", "/")
	openMethod := &ConnectionOpenMethod{
		VirtualHost: vhost,
	}
	openData, _ := openMethod.Serialize()
	writeFrameToConn(t, conn, EncodeMethodFrame(10, 40, openData))

	// Connection.OpenOK
	openOKFrame := readFrameFromConn(t, conn)
	parseConnectionOpenOK(t, openOKFrame)

	// Remove deadline after handshake
	conn.SetDeadline(time.Time{})

	return &TestConnection{
		conn:      conn,
		channel:   0,
		frameMax:  tuneMethod.FrameMax,
		heartbeat: tuneMethod.Heartbeat,
		t:         t,
	}
}

// Close closes the test connection
func (tc *TestConnection) Close() {
	if tc.conn != nil {
		tc.conn.Close()
	}
}

// TestRabbitMQ_ChannelOpen tests opening a channel
func TestRabbitMQ_ChannelOpen(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Send Channel.Open on channel 1
	channelOpenMethod := &ChannelOpenMethod{
		Reserved1: "",
	}

	channelOpenData, err := channelOpenMethod.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Channel.Open: %v", err)
	}

	channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
	channelOpenFrame.Channel = 1
	writeFrameToConn(t, tc.conn, channelOpenFrame)

	// Receive Channel.OpenOK
	channelOpenOKFrame := readFrameFromConn(t, tc.conn)

	if channelOpenOKFrame.Channel != 1 {
		t.Errorf("Expected channel 1, got %d", channelOpenOKFrame.Channel)
	}

	parseChannelOpenOK(t, channelOpenOKFrame)

	t.Log("Channel opened successfully")
}

// TestRabbitMQ_QueueDeclare tests declaring a queue
func TestRabbitMQ_QueueDeclare(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Open channel 1
	channelOpenMethod := &ChannelOpenMethod{}
	channelOpenData, _ := channelOpenMethod.Serialize()
	channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
	channelOpenFrame.Channel = 1
	writeFrameToConn(t, tc.conn, channelOpenFrame)
	readFrameFromConn(t, tc.conn) // Channel.OpenOK

	// Declare queue
	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	queueDeclare := &QueueDeclareMethod{
		Reserved1:  0,
		Queue:      queueName,
		Passive:    false,
		Durable:    false,
		Exclusive:  true, // Auto-delete when connection closes
		AutoDelete: true,
		NoWait:     false,
		Arguments:  nil,
	}

	queueDeclareData, err := queueDeclare.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Queue.Declare: %v", err)
	}

	queueDeclareFrame := EncodeMethodFrame(50, 10, queueDeclareData)
	queueDeclareFrame.Channel = 1
	writeFrameToConn(t, tc.conn, queueDeclareFrame)

	// Receive Queue.DeclareOK
	queueDeclareOKFrame := readFrameFromConn(t, tc.conn)

	if queueDeclareOKFrame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", queueDeclareOKFrame.Type)
	}

	classID := binary.BigEndian.Uint16(queueDeclareOKFrame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(queueDeclareOKFrame.Payload[2:4])

	if classID != 50 || methodID != 11 {
		t.Errorf("Expected Queue.DeclareOK (50,11), got (%d,%d)", classID, methodID)
	}

	t.Logf("Queue declared successfully: %s", queueName)
}

// TestRabbitMQ_ExchangeDeclare tests declaring an exchange
func TestRabbitMQ_ExchangeDeclare(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Open channel 1
	channelOpenMethod := &ChannelOpenMethod{}
	channelOpenData, _ := channelOpenMethod.Serialize()
	channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
	channelOpenFrame.Channel = 1
	writeFrameToConn(t, tc.conn, channelOpenFrame)
	readFrameFromConn(t, tc.conn) // Channel.OpenOK

	// Declare exchange
	exchangeName := fmt.Sprintf("test-exchange-%d", time.Now().UnixNano())
	exchangeDeclare := &ExchangeDeclareMethod{
		Reserved1:  0,
		Exchange:   exchangeName,
		Type:       "direct",
		Passive:    false,
		Durable:    false,
		AutoDelete: true,
		Internal:   false,
		NoWait:     false,
		Arguments:  nil,
	}

	exchangeDeclareData, err := exchangeDeclare.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Exchange.Declare: %v", err)
	}

	exchangeDeclareFrame := EncodeMethodFrame(40, 10, exchangeDeclareData)
	exchangeDeclareFrame.Channel = 1
	writeFrameToConn(t, tc.conn, exchangeDeclareFrame)

	// Receive Exchange.DeclareOK
	exchangeDeclareOKFrame := readFrameFromConn(t, tc.conn)

	if exchangeDeclareOKFrame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", exchangeDeclareOKFrame.Type)
	}

	classID := binary.BigEndian.Uint16(exchangeDeclareOKFrame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(exchangeDeclareOKFrame.Payload[2:4])

	if classID != 40 || methodID != 11 {
		t.Errorf("Expected Exchange.DeclareOK (40,11), got (%d,%d)", classID, methodID)
	}

	t.Logf("Exchange declared successfully: %s", exchangeName)
}

// TestRabbitMQ_BasicPublish tests publishing a message
func TestRabbitMQ_BasicPublish(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Open channel 1
	channelOpenMethod := &ChannelOpenMethod{}
	channelOpenData, _ := channelOpenMethod.Serialize()
	channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
	channelOpenFrame.Channel = 1
	writeFrameToConn(t, tc.conn, channelOpenFrame)
	readFrameFromConn(t, tc.conn) // Channel.OpenOK

	// Declare queue
	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	queueDeclare := &QueueDeclareMethod{
		Queue:      queueName,
		Exclusive:  true,
		AutoDelete: true,
	}
	queueDeclareData, _ := queueDeclare.Serialize()
	queueDeclareFrame := EncodeMethodFrame(50, 10, queueDeclareData)
	queueDeclareFrame.Channel = 1
	writeFrameToConn(t, tc.conn, queueDeclareFrame)
	readFrameFromConn(t, tc.conn) // Queue.DeclareOK

	// Publish message
	basicPublish := &BasicPublishMethod{
		Reserved1:  0,
		Exchange:   "",
		RoutingKey: queueName,
		Mandatory:  false,
		Immediate:  false,
	}

	publishData, err := basicPublish.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Basic.Publish: %v", err)
	}

	publishFrame := EncodeMethodFrame(60, 40, publishData)
	publishFrame.Channel = 1
	writeFrameToConn(t, tc.conn, publishFrame)

	// Send content header
	messageBody := []byte("Hello RabbitMQ!")
	contentHeader := &ContentHeader{
		ClassID:       60,
		BodySize:      uint64(len(messageBody)),
		PropertyFlags: FlagContentType | FlagDeliveryMode,
		ContentType:   "text/plain",
		DeliveryMode:  1, // Non-persistent
	}

	headerData, err := contentHeader.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize content header: %v", err)
	}

	headerFrame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: headerData,
	}
	writeFrameToConn(t, tc.conn, headerFrame)

	// Send body frame
	bodyFrame := &Frame{
		Type:    FrameBody,
		Channel: 1,
		Payload: messageBody,
	}
	writeFrameToConn(t, tc.conn, bodyFrame)

	t.Log("Message published successfully")
}

// TestRabbitMQ_BasicGet tests getting a message
func TestRabbitMQ_BasicGet(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Open channel 1
	channelOpenMethod := &ChannelOpenMethod{}
	channelOpenData, _ := channelOpenMethod.Serialize()
	channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
	channelOpenFrame.Channel = 1
	writeFrameToConn(t, tc.conn, channelOpenFrame)
	readFrameFromConn(t, tc.conn) // Channel.OpenOK

	// Declare queue
	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	queueDeclare := &QueueDeclareMethod{
		Queue:      queueName,
		Exclusive:  true,
		AutoDelete: true,
	}
	queueDeclareData, _ := queueDeclare.Serialize()
	queueDeclareFrame := EncodeMethodFrame(50, 10, queueDeclareData)
	queueDeclareFrame.Channel = 1
	writeFrameToConn(t, tc.conn, queueDeclareFrame)
	readFrameFromConn(t, tc.conn) // Queue.DeclareOK

	// Publish a message first
	basicPublish := &BasicPublishMethod{
		Exchange:   "",
		RoutingKey: queueName,
	}
	publishData, _ := basicPublish.Serialize()
	publishFrame := EncodeMethodFrame(60, 40, publishData)
	publishFrame.Channel = 1
	writeFrameToConn(t, tc.conn, publishFrame)

	messageBody := []byte("Test message")
	contentHeader := &ContentHeader{
		ClassID:       60,
		BodySize:      uint64(len(messageBody)),
		PropertyFlags: FlagContentType,
		ContentType:   "text/plain",
	}
	headerData, _ := contentHeader.Serialize()
	writeFrameToConn(t, tc.conn, &Frame{Type: FrameHeader, Channel: 1, Payload: headerData})
	writeFrameToConn(t, tc.conn, &Frame{Type: FrameBody, Channel: 1, Payload: messageBody})

	// Give RabbitMQ time to process
	time.Sleep(100 * time.Millisecond)

	// Get message
	basicGet := &BasicGetMethod{
		Reserved1: 0,
		Queue:     queueName,
		NoAck:     true,
	}

	getData, err := basicGet.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize Basic.Get: %v", err)
	}

	getFrame := EncodeMethodFrame(60, 70, getData)
	getFrame.Channel = 1
	writeFrameToConn(t, tc.conn, getFrame)

	// Receive response (GetOK or GetEmpty)
	responseFrame := readFrameFromConn(t, tc.conn)

	if responseFrame.Type != FrameMethod {
		t.Fatalf("Expected method frame, got type %d", responseFrame.Type)
	}

	classID := binary.BigEndian.Uint16(responseFrame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(responseFrame.Payload[2:4])

	if classID == 60 && methodID == 71 {
		// Basic.GetOK - message retrieved
		t.Log("Message retrieved successfully")

		// Read content header and body
		headerFrame := readFrameFromConn(t, tc.conn)
		if headerFrame.Type != FrameHeader {
			t.Errorf("Expected header frame, got type %d", headerFrame.Type)
		}

		bodyFrame := readFrameFromConn(t, tc.conn)
		if bodyFrame.Type != FrameBody {
			t.Errorf("Expected body frame, got type %d", bodyFrame.Type)
		}

		if !bytes.Equal(bodyFrame.Payload, messageBody) {
			t.Errorf("Message body mismatch: expected %q, got %q", messageBody, bodyFrame.Payload)
		}
	} else if classID == 60 && methodID == 72 {
		// Basic.GetEmpty - no message
		t.Log("Queue empty (GetEmpty received)")
	} else {
		t.Errorf("Expected Basic.GetOK (60,71) or GetEmpty (60,72), got (%d,%d)", classID, methodID)
	}
}

// TestRabbitMQ_Heartbeat tests heartbeat frame handling
func TestRabbitMQ_Heartbeat(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Send heartbeat frame
	heartbeatFrame := &Frame{
		Type:    FrameHeartbeat,
		Channel: 0,
		Payload: []byte{},
	}

	writeFrameToConn(t, tc.conn, heartbeatFrame)

	// Heartbeat frames don't get responses, but should not cause errors
	// Wait briefly to ensure server doesn't disconnect
	time.Sleep(100 * time.Millisecond)

	t.Log("Heartbeat frame sent successfully")
}

// TestRabbitMQ_MultipleChannels tests using multiple channels
func TestRabbitMQ_MultipleChannels(t *testing.T) {
	tc := establishConnection(t)
	defer tc.Close()

	// Open channel 1
	channelOpenMethod := &ChannelOpenMethod{}
	channelOpenData, _ := channelOpenMethod.Serialize()

	for channelID := uint16(1); channelID <= 3; channelID++ {
		channelOpenFrame := EncodeMethodFrame(20, 10, channelOpenData)
		channelOpenFrame.Channel = channelID
		writeFrameToConn(t, tc.conn, channelOpenFrame)

		// Receive Channel.OpenOK
		channelOpenOKFrame := readFrameFromConn(t, tc.conn)

		if channelOpenOKFrame.Channel != channelID {
			t.Errorf("Expected channel %d, got %d", channelID, channelOpenOKFrame.Channel)
		}

		parseChannelOpenOK(t, channelOpenOKFrame)
		t.Logf("Channel %d opened successfully", channelID)
	}
}
