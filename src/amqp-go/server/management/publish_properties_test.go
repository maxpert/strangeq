package management

// Persistence + property-mapping contract for the management publish endpoint
// (POST /api/exchanges/{vhost}/{name}/publish).
//
// DATA-074: the endpoint accepts a RabbitMQ-style `properties` map. Every AMQP
// basic.properties field must land on the protocol.Message, and delivery_mode
// must drive the broker's durability decision exactly as it does for an AMQP
// basic.publish: delivery_mode 2 ⇒ the message is stored in the WAL and
// survives a restart; delivery_mode 1 (or absent) ⇒ transient, lost on restart.
//
// These tests pin that contract. Before the fix, applyProperties omitted
// delivery_mode/priority/timestamp and the handler additionally copied the raw
// `properties` map into msg.Headers, so a `{"properties":{"delivery_mode":2}}`
// publish was silently transient AND leaked content_type/delivery_mode as
// headers.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
)

// declareDurableExchangeAndBind declares a durable direct exchange and binds
// queue to it under rk, mirroring the AMQP topology a real publisher targets.
func (e *env) declareDurableExchangeAndBind(t *testing.T, exchange, queue, rk string) {
	t.Helper()
	rec := e.do(http.MethodPut, "/api/exchanges/%2F/"+exchange,
		e.jsonBody(t, exchangeDeclareRequest{Type: "direct", Durable: true}), basicAuthHeader())
	wantStatus(t, rec, http.StatusCreated)
	if err := e.srv.Broker.BindQueue(queue, exchange, rk, nil); err != nil {
		t.Fatalf("bind %s -> %s: %v", exchange, queue, err)
	}
}

// TestApplyProperties_NumericTypesAndRejections pins the numeric conversion
// contract directly: JSON numbers arrive as float64, but the properties map may
// also be built in Go, so delivery_mode/priority/timestamp must accept any
// integer type and json.Number. Non-integral, negative, and out-of-range values
// are ignored (the field keeps its zero value) rather than wrapped.
func TestApplyProperties_NumericTypesAndRejections(t *testing.T) {
	cases := []struct {
		name         string
		deliveryMode any
		priority     any
		timestamp    any
		wantMode     uint8
		wantPriority uint8
		wantTS       uint64
	}{
		{"json float64", float64(2), float64(5), float64(1700000000), 2, 5, 1700000000},
		{"go int", int(2), int(5), int(1700000000), 2, 5, 1700000000},
		{"go int64", int64(2), int64(5), int64(1700000000), 2, 5, 1700000000},
		{"go uint64", uint64(2), uint64(5), uint64(1700000000), 2, 5, 1700000000},
		{"json.Number", json.Number("2"), json.Number("5"), json.Number("1700000000"), 2, 5, 1700000000},
		{"negative ignored", float64(-1), float64(-5), float64(-1), 0, 0, 0},
		{"non-integral ignored", float64(1.5), float64(2.5), float64(1.5), 0, 0, 0},
		{"out-of-range mode/priority ignored", float64(256), float64(300), float64(1700000000), 0, 0, 1700000000},
		{"non-numeric ignored", "2", "5", "1700000000", 0, 0, 0},
		{"absent stays zero", nil, nil, nil, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			props := map[string]interface{}{}
			if tc.deliveryMode != nil {
				props["delivery_mode"] = tc.deliveryMode
			}
			if tc.priority != nil {
				props["priority"] = tc.priority
			}
			if tc.timestamp != nil {
				props["timestamp"] = tc.timestamp
			}
			msg := &protocol.Message{}
			applyProperties(msg, props)
			if msg.DeliveryMode != tc.wantMode {
				t.Errorf("DeliveryMode = %d, want %d", msg.DeliveryMode, tc.wantMode)
			}
			if msg.Priority != tc.wantPriority {
				t.Errorf("Priority = %d, want %d", msg.Priority, tc.wantPriority)
			}
			if msg.Timestamp != tc.wantTS {
				t.Errorf("Timestamp = %d, want %d", msg.Timestamp, tc.wantTS)
			}
		})
	}
}

// --- deterministic property mapping ---

// TestPublish_PropertiesMappedOntoMessage publishes the full basic.properties
// set through the management API and inspects the assembled protocol.Message.
// It fails if any property is dropped, if delivery_mode/priority/timestamp are
// not numeric-mapped, or if any non-header property leaked into Headers.
func TestPublish_PropertiesMappedOntoMessage(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "props-q", queueDeclareRequest{Durable: true})
	e.declareDurableExchangeAndBind(t, "props-ex", "props-q", "rk")

	props := map[string]interface{}{
		"content_type":     "application/json",
		"content_encoding": "utf-8",
		"delivery_mode":    float64(2),
		"priority":         float64(5),
		"timestamp":        float64(1700000000),
		"correlation_id":   "corr-1",
		"reply_to":         "reply-q",
		"expiration":       "60000",
		"message_id":       "msg-1",
		"type":             "event",
		"user_id":          "guest",
		"app_id":           "app-1",
		"cluster_id":       "cluster-1",
		"headers":          map[string]interface{}{"x-trace": "abc"},
	}

	rec := e.do(http.MethodPost, "/api/exchanges/%2F/props-ex/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "rk", Payload: "hi", Properties: props}),
		basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	msg, _, _, err := e.srv.Broker.GetMessageForGet("props-q", true)
	if err != nil || msg == nil {
		t.Fatalf("get published message: msg=%v err=%v", msg, err)
	}

	if msg.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want application/json", msg.ContentType)
	}
	if msg.ContentEncoding != "utf-8" {
		t.Errorf("ContentEncoding = %q, want utf-8", msg.ContentEncoding)
	}
	if msg.DeliveryMode != 2 {
		t.Errorf("DeliveryMode = %d, want 2 (persistent)", msg.DeliveryMode)
	}
	if msg.Priority != 5 {
		t.Errorf("Priority = %d, want 5", msg.Priority)
	}
	if msg.Timestamp != 1700000000 {
		t.Errorf("Timestamp = %d, want 1700000000", msg.Timestamp)
	}
	if msg.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want corr-1", msg.CorrelationID)
	}
	if msg.ReplyTo != "reply-q" {
		t.Errorf("ReplyTo = %q, want reply-q", msg.ReplyTo)
	}
	if msg.Expiration != "60000" {
		t.Errorf("Expiration = %q, want 60000", msg.Expiration)
	}
	if msg.MessageID != "msg-1" {
		t.Errorf("MessageID = %q, want msg-1", msg.MessageID)
	}
	if msg.Type != "event" {
		t.Errorf("Type = %q, want event", msg.Type)
	}
	if msg.UserID != "guest" {
		t.Errorf("UserID = %q, want guest", msg.UserID)
	}
	if msg.AppID != "app-1" {
		t.Errorf("AppID = %q, want app-1", msg.AppID)
	}
	if msg.ClusterID != "cluster-1" {
		t.Errorf("ClusterID = %q, want cluster-1", msg.ClusterID)
	}

	// Headers must be ONLY the explicit `headers` field. The raw properties map
	// must never be copied in.
	if len(msg.Headers) != 1 || msg.Headers["x-trace"] != "abc" {
		t.Errorf("Headers = %#v, want exactly {x-trace: abc}", msg.Headers)
	}
	for _, leaked := range []string{
		"delivery_mode", "priority", "timestamp", "content_type",
		"content_encoding", "correlation_id", "reply_to", "expiration",
		"message_id", "type", "user_id", "app_id", "cluster_id", "headers",
	} {
		if _, ok := msg.Headers[leaked]; ok {
			t.Errorf("property %q leaked into Headers; properties must not be copied into headers", leaked)
		}
	}
}

// TestPublish_RawPropertiesNotCopiedIntoHeaders pins the second half of
// DATA-074: when a publish carries properties but NO explicit `headers` field,
// the message's Headers must be empty. The handler must not copy the raw
// `properties` map into msg.Headers — content_type/delivery_mode are message
// properties, not headers.
func TestPublish_RawPropertiesNotCopiedIntoHeaders(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "nohdr-q", queueDeclareRequest{Durable: true})
	e.declareDurableExchangeAndBind(t, "nohdr-ex", "nohdr-q", "rk")

	rec := e.do(http.MethodPost, "/api/exchanges/%2F/nohdr-ex/publish",
		e.jsonBody(t, publishRequest{
			RoutingKey: "rk", Payload: "hi",
			Properties: map[string]interface{}{
				"content_type":  "text/plain",
				"delivery_mode": float64(2),
				"priority":      float64(3),
			},
		}), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	msg, _, _, err := e.srv.Broker.GetMessageForGet("nohdr-q", true)
	if err != nil || msg == nil {
		t.Fatalf("get published message: msg=%v err=%v", msg, err)
	}
	if len(msg.Headers) != 0 {
		t.Fatalf("Headers = %#v, want empty: the raw properties map must not be copied into headers", msg.Headers)
	}
}

// TestPublish_DeliveryModeOneStaysTransient pins the parity half of the
// contract: delivery_mode 1 (and an absent delivery_mode) must never be
// upgraded to persistent.
func TestPublish_DeliveryModeOneStaysTransient(t *testing.T) {
	e := basicEnv(t, true, nil)
	e.declareQueue(t, "transient-q", queueDeclareRequest{Durable: true})
	e.declareQueue(t, "no-mode-q", queueDeclareRequest{Durable: true})
	e.declareDurableExchangeAndBind(t, "trans-ex", "transient-q", "rk")
	e.declareDurableExchangeAndBind(t, "nomode-ex", "no-mode-q", "rk")

	rec := e.do(http.MethodPost, "/api/exchanges/%2F/trans-ex/publish",
		e.jsonBody(t, publishRequest{
			RoutingKey: "rk", Payload: "t",
			Properties: map[string]interface{}{"delivery_mode": float64(1)},
		}), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)

	msg, _, _, err := e.srv.Broker.GetMessageForGet("transient-q", true)
	if err != nil || msg == nil {
		t.Fatalf("get published message: msg=%v err=%v", msg, err)
	}
	if msg.DeliveryMode != 1 {
		t.Fatalf("DeliveryMode = %d, want 1 (transient)", msg.DeliveryMode)
	}

	// Absent delivery_mode stays unset (0), which is also non-persistent.
	rec = e.do(http.MethodPost, "/api/exchanges/%2F/nomode-ex/publish",
		e.jsonBody(t, publishRequest{RoutingKey: "rk", Payload: "n"}), basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	msg, _, _, err = e.srv.Broker.GetMessageForGet("no-mode-q", true)
	if err != nil || msg == nil {
		t.Fatalf("get published message: msg=%v err=%v", msg, err)
	}
	if msg.DeliveryMode == 2 {
		t.Fatalf("absent delivery_mode must not be persistent, got %d", msg.DeliveryMode)
	}
}

// --- end-to-end durability across a real restart ---

// buildMgmtBroker builds a real broker over cfg.Storage.Path. Build performs
// durable recovery against that directory, so rebuilding over the same dir is
// a genuine restart.
func buildMgmtBroker(t *testing.T, cfg *config.AMQPConfig) *server.Server {
	t.Helper()
	srv, err := server.NewServerBuilder().
		WithConfig(cfg).
		WithZapLogger("silent").
		Build()
	if err != nil {
		t.Fatalf("build broker over %s: %v", cfg.Storage.Path, err)
	}
	return srv
}

// shutdownMgmtBroker performs the graceful close-and-flush seam: Shutdown stops
// the broker and closes storage, flushing the WAL so a subsequent Build over
// the same directory recovers exactly what was durably written.
func shutdownMgmtBroker(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
}

// TestPublish_DeliveryModePersistsAcrossRestart is the durability regression:
// a delivery_mode:2 publish through the management API into a durable queue
// must be recovered after a real broker restart, while a delivery_mode:1
// publish to the same queue must not.
func TestPublish_DeliveryModePersistsAcrossRestart(t *testing.T) {
	hash := secretHash(t)
	cfg := baseEnvConfig(t, func(cfg *config.AMQPConfig) {
		cfg.Management.Authentication = config.ManagementAuthenticationBasic
		cfg.Management.Basic.Username = "admin"
		cfg.Management.Basic.PasswordHash = hash
		cfg.Management.Basic.Admin = true
	})

	srv1 := buildMgmtBroker(t, cfg)
	e1 := &env{t: t, cfg: cfg, srv: srv1, mgmt: newManagement(t, srv1, cfg)}
	e1.declareQueue(t, "mgmt-durable-q", queueDeclareRequest{Durable: true})
	e1.declareDurableExchangeAndBind(t, "mgmt-ex", "mgmt-durable-q", "rk")

	publish := func(e *env, body string, deliveryMode float64) {
		t.Helper()
		rec := e.do(http.MethodPost, "/api/exchanges/%2F/mgmt-ex/publish",
			e.jsonBody(t, publishRequest{
				RoutingKey: "rk",
				Payload:    body,
				Properties: map[string]interface{}{"delivery_mode": deliveryMode},
			}), basicAuthHeader())
		wantStatus(t, rec, http.StatusOK)
	}
	publish(e1, "durable-body", 2)
	publish(e1, "transient-body", 1)

	// Restart over the same storage directory.
	shutdownMgmtBroker(t, srv1)

	srv2 := buildMgmtBroker(t, cfg)
	t.Cleanup(func() { shutdownMgmtBroker(t, srv2) })
	e2 := &env{t: t, cfg: cfg, srv: srv2, mgmt: newManagement(t, srv2, cfg)}

	rec := e2.do(http.MethodPost, "/api/queues/%2F/mgmt-durable-q/get",
		e2.jsonBody(t, queueGetRequest{Count: 10, AckMode: "ack_requeue_false"}),
		basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	got := decode[[]getMessage](t, rec)

	if len(got) != 1 {
		t.Fatalf("recovered %d messages after restart, want exactly the 1 persistent one: %+v", len(got), got)
	}
	if got[0].Payload != "durable-body" {
		t.Fatalf("recovered payload = %q, want durable-body (the transient message must be gone)", got[0].Payload)
	}
}
