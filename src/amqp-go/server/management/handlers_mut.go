package management

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
)

// --- exchanges ---

type exchangeDeclareRequest struct {
	Type       string                 `json:"type"`
	AutoDelete bool                   `json:"auto_delete"`
	Durable    bool                   `json:"durable"`
	Internal   bool                   `json:"internal"`
	Arguments  map[string]interface{} `json:"arguments"`
}

var validExchangeTypes = map[string]bool{"direct": true, "fanout": true, "topic": true, "headers": true}

func (s *Server) hExchangeDeclare(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionConfigure, interfaces.ResourceExchange, name, vhost); aerr != nil {
		s.auditMutation(r, "exchange", name, "declare", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if !s.declareLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.declareLimit, p.Name)
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited, "declare rate limit exceeded", withHeader("Retry-After", "1")))
		return
	}
	setRateLimitHeaders(w, s.declareLimit, p.Name)

	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req exchangeDeclareRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}
	if !validExchangeTypes[req.Type] {
		s.writeAPIError(w, r, errBad("invalid exchange type "+strconv.Quote(req.Type)+": must be direct, fanout, topic, or headers"))
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	existing := b.GetExchanges()[name]
	newEntity := existing == nil
	if existing != nil {
		if aerr := s.ifMatchGate(r, true, exchangeInfoOf(existing)); aerr != nil {
			s.auditMutation(r, "exchange", name, "declare", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
	} else if aerr := s.ifMatchGate(r, false, nil); aerr != nil {
		s.auditMutation(r, "exchange", name, "declare", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if existing != nil {
		// Equivalent re-declare ⇒ 204; any conflicting immutable property ⇒
		// 400 inequivalent naming the field (FR-API-054).
		if conflict := exchangeConflict(existing, req); conflict != "" {
			s.auditMutation(r, "exchange", name, "declare", http.StatusBadRequest)
			s.writeAPIError(w, r, newAPIError(http.StatusBadRequest, errInequivalent,
				"exchange "+strconv.Quote(name)+" already exists with a different "+conflict))
			return
		}
	} else {
		if err := b.DeclareExchange(name, req.Type, req.Durable, req.AutoDelete, req.Internal, req.Arguments); err != nil {
			s.auditMutation(r, "exchange", name, "declare", http.StatusBadRequest)
			s.writeAPIError(w, r, errBad("could not declare exchange: "+err.Error()))
			return
		}
	}

	status := http.StatusNoContent
	if newEntity {
		status = http.StatusCreated
		w.Header().Set("Location", locationFor(r, endpointLabel(r), name))
	}
	s.auditMutation(r, "exchange", name, "declare", status)
	w.WriteHeader(status)
}

func exchangeConflict(ex *protocol.Exchange, req exchangeDeclareRequest) string {
	switch {
	case ex.Durable != req.Durable:
		return "durable"
	case ex.AutoDelete != req.AutoDelete:
		return "auto_delete"
	case ex.Internal != req.Internal:
		return "internal"
	case exchangeTypeName(ex.Kind) != req.Type:
		return "type"
	}
	return ""
}

func (s *Server) hExchangeDelete(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	if name == "" || name == "amq.default" || strings.HasPrefix(name, "amq.") {
		s.auditMutation(r, "exchange", name, "delete", http.StatusForbidden)
		s.writeAPIError(w, r, errRefused("system exchanges cannot be deleted"))
		return
	}
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionConfigure, interfaces.ResourceExchange, name, vhost); aerr != nil {
		s.auditMutation(r, "exchange", name, "delete", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	if b.GetExchanges()[name] == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown exchange"))
		return
	}
	if aerr := s.ifMatchGate(r, true, exchangeInfoOf(b.GetExchanges()[name])); aerr != nil {
		s.auditMutation(r, "exchange", name, "delete", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	ifUnused := false
	if raw, ok := firstPresent(r.URL.Query(), "if-unused"); ok {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			s.writeAPIError(w, r, errBad("if-unused must be true or false"))
			return
		}
		ifUnused = v
	}
	if aerr := s.auditDestructive(r, "exchange", name, "delete", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if err := b.DeleteExchange(name, ifUnused); err != nil {
		s.auditMutation(r, "exchange", name, "delete", http.StatusBadRequest)
		if errors.Is(err, broker.ErrExchangeInUse) {
			s.writeAPIError(w, r, errBad("exchange is in use"))
			return
		}
		s.writeAPIError(w, r, errBad("could not delete exchange: "+err.Error()))
		return
	}
	s.auditMutation(r, "exchange", name, "delete", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

// --- publish ---

type publishRequest struct {
	Properties      map[string]interface{} `json:"properties"`
	RoutingKey      string                 `json:"routing_key"`
	Payload         string                 `json:"payload"`
	PayloadEncoding string                 `json:"payload_encoding"`
}

type publishResponse struct {
	Routed bool `json:"routed"`
}

func (s *Server) hPublish(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	resource := interfaces.NormalizeExchangeName(name)
	if aerr := s.mutationGates(p, interfaces.ActionWrite, interfaces.ResourceExchange, resource, vhost); aerr != nil {
		s.auditMutation(r, "exchange", name, "publish", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if !s.publishLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.publishLimit, p.Name)
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited, "publish rate limit exceeded", withHeader("Retry-After", "1")))
		return
	}
	setRateLimitHeaders(w, s.publishLimit, p.Name)

	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req publishRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeAPIError(w, r, errBad("invalid JSON body"))
		return
	}
	var payload []byte
	switch req.PayloadEncoding {
	case "", "string":
		payload = []byte(req.Payload)
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(req.Payload)
		if err != nil {
			s.writeAPIError(w, r, errBad("payload is not valid base64"))
			return
		}
		payload = decoded
	default:
		s.writeAPIError(w, r, errBad("payload_encoding must be string or base64"))
		return
	}

	maxSize := int64(s.cfg.Server.MaxMessageSize)
	if maxSize > 0 && int64(len(payload)) > maxSize {
		s.auditMutation(r, "exchange", name, "publish", http.StatusRequestEntityTooLarge)
		s.writeAPIError(w, r, newAPIError(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
			"payload exceeds server.maxmessagesize; nothing was stored"))
		return
	}

	msg := &protocol.Message{
		Body:       payload,
		Exchange:   name,
		RoutingKey: req.RoutingKey,
		Headers:    req.Properties,
		// Mandatory makes a no-route publish observable: the broker returns
		// ErrNoRoute instead of silently dropping, which is what lets the API
		// report routed:false (FR-API-058).
		Mandatory: true,
	}
	applyProperties(msg, req.Properties)

	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	if pb, ok := b.(server.NonBlockingPublisher); ok {
		gate, err := pb.PublishMessageAdmitted(name, req.RoutingKey, msg)
		if gate != nil {
			s.sink.ManagementPublishBackpressure()
			s.auditMutation(r, "exchange", name, "publish", http.StatusServiceUnavailable)
			s.writeAPIError(w, r, apiErr(http.StatusServiceUnavailable, errUnavailable,
				"queue backpressure", withHeader("Retry-After", "1")))
			return
		}
		if err != nil {
			if errors.Is(err, broker.ErrNoRoute) {
				s.auditMutation(r, "exchange", name, "publish", http.StatusOK)
				s.writeJSON(w, r, http.StatusOK, publishResponse{Routed: false})
				return
			}
			s.writePublishError(w, r, name, err)
			return
		}
		s.auditMutation(r, "exchange", name, "publish", http.StatusOK)
		s.writeJSON(w, r, http.StatusOK, publishResponse{Routed: true})
		return
	}

	err := b.PublishMessage(name, req.RoutingKey, msg)
	if errors.Is(err, broker.ErrNoRoute) {
		s.auditMutation(r, "exchange", name, "publish", http.StatusOK)
		s.writeJSON(w, r, http.StatusOK, publishResponse{Routed: false})
		return
	}
	if err != nil {
		s.writePublishError(w, r, name, err)
		return
	}
	s.auditMutation(r, "exchange", name, "publish", http.StatusOK)
	s.writeJSON(w, r, http.StatusOK, publishResponse{Routed: true})
}

func (s *Server) writePublishError(w http.ResponseWriter, r *http.Request, name string, err error) {
	if errors.Is(err, broker.ErrMaxLengthExceeded) {
		s.writeAPIError(w, r, newAPIError(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
			"a target queue rejected the publish (max-length)"))
		return
	}
	s.auditMutation(r, "exchange", name, "publish", http.StatusBadRequest)
	s.writeAPIError(w, r, errBad("publish failed: "+err.Error()))
}

// applyProperties copies the AMQP basic.properties fields from the request map
// onto the message (DATA-074).
func applyProperties(msg *protocol.Message, props map[string]interface{}) {
	asString := func(v any) string {
		if s, ok := v.(string); ok {
			return s
		}
		return ""
	}
	if v, ok := props["content_type"]; ok {
		msg.ContentType = asString(v)
	}
	if v, ok := props["content_encoding"]; ok {
		msg.ContentEncoding = asString(v)
	}
	if v, ok := props["correlation_id"]; ok {
		msg.CorrelationID = asString(v)
	}
	if v, ok := props["reply_to"]; ok {
		msg.ReplyTo = asString(v)
	}
	if v, ok := props["expiration"]; ok {
		msg.Expiration = asString(v)
	}
	if v, ok := props["message_id"]; ok {
		msg.MessageID = asString(v)
	}
	if v, ok := props["type"]; ok {
		msg.Type = asString(v)
	}
	if v, ok := props["user_id"]; ok {
		msg.UserID = asString(v)
	}
	if v, ok := props["app_id"]; ok {
		msg.AppID = asString(v)
	}
	if v, ok := props["cluster_id"]; ok {
		msg.ClusterID = asString(v)
	}
	if v, ok := props["headers"]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			msg.Headers = m
		}
	}
}

// --- queues ---

type queueDeclareRequest struct {
	Name       string                 `json:"name"`
	AutoDelete bool                   `json:"auto_delete"`
	Durable    bool                   `json:"durable"`
	Exclusive  bool                   `json:"exclusive"`
	Arguments  map[string]interface{} `json:"arguments"`
	Node       string                 `json:"node"`
	Type       string                 `json:"type"`
}

func (s *Server) hQueueDeclare(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionConfigure, interfaces.ResourceQueue, name, vhost); aerr != nil {
		s.auditMutation(r, "queue", name, "declare", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if !s.declareLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.declareLimit, p.Name)
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited, "declare rate limit exceeded", withHeader("Retry-After", "1")))
		return
	}
	setRateLimitHeaders(w, s.declareLimit, p.Name)
	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req queueDeclareRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}
	if req.Type != "" && req.Type != "classic" {
		s.writeAPIError(w, r, errBad("queue type "+strconv.Quote(req.Type)+" is not supported; only classic is implemented"))
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	existing := b.GetQueues()[name]
	newEntity := existing == nil
	if existing != nil {
		if repr, ok := s.queueRepresentation(name); ok {
			if aerr := s.ifMatchGate(r, true, repr); aerr != nil {
				s.auditMutation(r, "queue", name, "declare", aerr.status)
				s.writeAPIError(w, r, aerr)
				return
			}
		}
	} else if aerr := s.ifMatchGate(r, false, nil); aerr != nil {
		s.auditMutation(r, "queue", name, "declare", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if existing != nil {
		if conflict := queueConflict(existing, req); conflict != "" {
			s.auditMutation(r, "queue", name, "declare", http.StatusBadRequest)
			s.writeAPIError(w, r, newAPIError(http.StatusBadRequest, errInequivalent,
				"queue "+strconv.Quote(name)+" already exists with a different "+conflict))
			return
		}
	} else {
		if _, err := b.DeclareQueue(name, req.Durable, req.AutoDelete, req.Exclusive, req.Arguments); err != nil {
			s.auditMutation(r, "queue", name, "declare", http.StatusBadRequest)
			s.writeAPIError(w, r, errBad("could not declare queue: "+err.Error()))
			return
		}
	}
	status := http.StatusNoContent
	if newEntity {
		status = http.StatusCreated
		w.Header().Set("Location", locationFor(r, endpointLabel(r), name))
	}
	s.auditMutation(r, "queue", name, "declare", status)
	w.WriteHeader(status)
}

func queueConflict(q *protocol.Queue, req queueDeclareRequest) string {
	switch {
	case q.Durable != req.Durable:
		return "durable"
	case q.AutoDelete != req.AutoDelete:
		return "auto_delete"
	case q.Exclusive != req.Exclusive:
		return "exclusive"
	}
	return ""
}

// hQueueCreate is the StrangeQ extension POST /api/queues/{vhost}: an empty
// name is server-generated and returned in Location (FR-API-076/DIV-14).
func (s *Server) hQueueCreate(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionConfigure, interfaces.ResourceQueue, "", vhost); aerr != nil {
		s.auditMutation(r, "queue", "", "declare", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req queueDeclareRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}
	if req.Type != "" && req.Type != "classic" {
		s.writeAPIError(w, r, errBad("only classic queues are supported"))
		return
	}
	// An empty name is SERVER-NAMED: the management layer mints the amq.gen.*
	// name and reports it in Location (FR-API-076/DIV-14).
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = protocol.GenerateQueueName()
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errEmpty())
		return
	}
	q, err := b.DeclareQueue(name, req.Durable, req.AutoDelete, req.Exclusive, req.Arguments)
	if err != nil {
		s.auditMutation(r, "queue", name, "declare", http.StatusBadRequest)
		s.writeAPIError(w, r, errBad("could not declare queue: "+err.Error()))
		return
	}
	if q == nil || q.Name == "" {
		s.writeAPIError(w, r, apiErr(http.StatusInternalServerError, errInternal, "server failed to generate a queue name"))
		return
	}
	w.Header().Set("Location", locationFor(r, "/api/queues/{vhost}/{name}", q.Name))
	s.auditMutation(r, "queue", q.Name, "declare", http.StatusCreated)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) hQueueDelete(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionConfigure, interfaces.ResourceQueue, name, vhost); aerr != nil {
		s.auditMutation(r, "queue", name, "delete", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	args := r.URL.Query()
	// Only the HYPHENATED spellings are honoured; if_empty is ignored
	// (FR-CONV-006).
	ifEmpty, aerr := boolParam(args, "if-empty")
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	ifUnused, aerr := boolParam(args, "if-unused")
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	if b.GetQueues()[name] == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown queue"))
		return
	}
	if repr, ok := s.queueRepresentation(name); ok {
		if aerr := s.ifMatchGate(r, true, repr); aerr != nil {
			s.auditMutation(r, "queue", name, "delete", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
	}
	if aerr := s.auditDestructive(r, "queue", name, "delete", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	deleted, err := b.DeleteQueue(name, ifUnused, ifEmpty)
	if err != nil {
		s.auditMutation(r, "queue", name, "delete", http.StatusBadRequest)
		if errors.Is(err, broker.ErrQueueNotEmpty) || errors.Is(err, broker.ErrQueueInUse) {
			s.writeAPIError(w, r, errBad("queue refused the conditional delete: "+err.Error()))
			return
		}
		s.writeAPIError(w, r, errBad("could not delete queue: "+err.Error()))
		return
	}
	w.Header().Set("X-Sq-Messages-Deleted", strconv.Itoa(deleted))
	s.auditMutation(r, "queue", name, "delete", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

// hQueuePurge implements DELETE /api/queues/{vhost}/{name}/contents.
func (s *Server) hQueuePurge(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionRead, interfaces.ResourceQueue, name, vhost); aerr != nil {
		s.auditMutation(r, "queue", name, "purge", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if !s.purgeLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.purgeLimit, p.Name)
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited, "purge rate limit exceeded", withHeader("Retry-After", "1")))
		return
	}
	setRateLimitHeaders(w, s.purgeLimit, p.Name)
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	if b.GetQueues()[name] == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown queue"))
		return
	}
	if aerr := s.auditDestructive(r, "queue", name, "purge", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}

	// Over the async message threshold ⇒ 202 + a job (FR-API-078).
	if est := s.estimateQueueDepth(name); est > int64(s.mcfg.Async.MessageThreshold) {
		job, jerr := s.jobs.submit("purge", p.Name, map[string]any{"queue": name, "vhost": vhost}, func() (any, error) {
			purged, err := b.PurgeQueue(name)
			if err != nil {
				return nil, err
			}
			return map[string]any{"purged": purged}, nil
		})
		if jerr != nil {
			s.writeAPIError(w, r, errUnavailableMsg("could not enqueue purge job: "+jerr.Error()))
			return
		}
		w.Header().Set("Location", "/api/strangeq/jobs/"+job.id)
		w.Header().Set("Retry-After", "1")
		s.auditMutation(r, "queue", name, "purge", http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	purged, err := b.PurgeQueue(name)
	if err != nil {
		s.auditMutation(r, "queue", name, "purge", http.StatusBadRequest)
		s.writeAPIError(w, r, errBad("could not purge queue: "+err.Error()))
		return
	}
	w.Header().Set("X-Sq-Messages-Purged", strconv.Itoa(purged))
	s.auditMutation(r, "queue", name, "purge", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) estimateQueueDepth(name string) int64 {
	b := s.broker()
	if b == nil {
		return 0
	}
	if qt, ok := b.(queueMessageTotals); ok {
		if c := qt.QueueMessageCounts(name); c.Known {
			return c.Total
		}
	}
	if rc, ok := b.(queueReadyCounter); ok {
		return int64(rc.QueueReadyCount(name))
	}
	return 0
}

// --- basic.get ---

type queueGetRequest struct {
	Count    int    `json:"count"`
	AckMode  string `json:"ackmode"`
	Encoding string `json:"encoding"`
	Truncate int    `json:"truncate"`
}

type getMessage struct {
	Payload         string `json:"payload"`
	PayloadEncoding string `json:"payload_encoding"`
	Properties      any    `json:"properties"`
	RoutingKey      string `json:"routing_key"`
	Redelivered     bool   `json:"redelivered"`
	MessageCount    int    `json:"message_count"`
	Exchange        string `json:"exchange"`
	Truncated       bool   `json:"truncated"`
}

func (s *Server) hQueueGet(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	p := principalFrom(r)
	if aerr := s.mutationGates(p, interfaces.ActionRead, interfaces.ResourceQueue, name, vhost); aerr != nil {
		s.auditMutation(r, "queue", name, "get", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	if !s.getLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.getLimit, p.Name)
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited, "get rate limit exceeded", withHeader("Retry-After", "1")))
		return
	}
	setRateLimitHeaders(w, s.getLimit, p.Name)
	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req queueGetRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}
	if req.Count == 0 {
		req.Count = 1
	}
	if req.Count < 0 {
		s.writeAPIError(w, r, errBad("count must be positive"))
		return
	}
	if req.Count > s.mcfg.Get.MaxCount {
		s.writeAPIError(w, r, errBad("count exceeds management.get.max_count ("+strconv.Itoa(s.mcfg.Get.MaxCount)+")"))
		return
	}
	var requeue, ackMessage bool
	switch req.AckMode {
	case "", "ack_requeue_true":
		requeue, ackMessage = true, true
	case "reject_requeue_true":
		requeue, ackMessage = true, false
	case "ack_requeue_false":
		requeue, ackMessage = false, true
	case "reject_requeue_false":
		requeue, ackMessage = false, false
	default:
		s.writeAPIError(w, r, errBad("invalid ackmode "+strconv.Quote(req.AckMode)))
		return
	}
	if req.Encoding != "" && req.Encoding != "auto" && req.Encoding != "string" && req.Encoding != "base64" {
		s.writeAPIError(w, r, errBad("encoding must be auto, string, or base64"))
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	if b.GetQueues()[name] == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown queue"))
		return
	}

	out := make([]getMessage, 0, req.Count)
	for i := 0; i < req.Count; i++ {
		msg, tag, _, err := b.GetMessageForGet(name, false)
		if err != nil || msg == nil {
			break
		}
		if requeue {
			_ = b.RejectGetDelivery(tag, true)
		} else if ackMessage {
			_ = b.AcknowledgeGetDelivery(tag)
		} else {
			_ = b.RejectGetDelivery(tag, false)
		}
		out = append(out, s.getMessageView(msg, req.Truncate))
	}
	s.auditMutation(r, "queue", name, "get", http.StatusOK)
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) getMessageView(msg *protocol.Message, truncateLen int) getMessage {
	payload := msg.Body
	truncated := false
	if truncateLen > 0 && len(payload) > truncateLen {
		payload = payload[:truncateLen]
		truncated = true
	}
	encoding := "string"
	if !isPrintableUTF8(payload) {
		encoding = "base64"
	}
	var rendered string
	if encoding == "string" {
		rendered = string(payload)
	} else {
		rendered = base64.StdEncoding.EncodeToString(payload)
	}
	props := map[string]any{}
	if msg.ContentType != "" {
		props["content_type"] = msg.ContentType
	}
	if msg.ContentEncoding != "" {
		props["content_encoding"] = msg.ContentEncoding
	}
	if msg.CorrelationID != "" {
		props["correlation_id"] = msg.CorrelationID
	}
	if msg.ReplyTo != "" {
		props["reply_to"] = msg.ReplyTo
	}
	if msg.Expiration != "" {
		props["expiration"] = msg.Expiration
	}
	if msg.MessageID != "" {
		props["message_id"] = msg.MessageID
	}
	if msg.Type != "" {
		props["type"] = msg.Type
	}
	if msg.UserID != "" {
		props["user_id"] = msg.UserID
	}
	if msg.AppID != "" {
		props["app_id"] = msg.AppID
	}
	if msg.Headers != nil {
		props["headers"] = fieldTableJSON(msg.Headers)
	}
	var propsAny any
	if len(props) > 0 {
		propsAny = props
	}
	return getMessage{
		Payload:         rendered,
		PayloadEncoding: encoding,
		Properties:      propsAny,
		RoutingKey:      msg.RoutingKey,
		Redelivered:     false,
		MessageCount:    1,
		Exchange:        msg.Exchange,
		Truncated:       truncated,
	}
}

func isPrintableUTF8(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\n' && c != '\r' && c != '\t' {
			return false
		}
	}
	return true
}

// --- connection close (force-close divergence, FC §7.6) ---

func (s *Server) hCloseConnection(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	srv := s.opts.Server
	if srv == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	srv.Mutex.RLock()
	conn := srv.Connections[name]
	var username string
	if conn != nil {
		username = conn.Username
	}
	srv.Mutex.RUnlock()
	if conn == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown connection"))
		return
	}
	p := principalFrom(r)
	if aerr := s.canClose(p, username); aerr != nil {
		s.auditMutation(r, "connection", name, "close", aerr.status)
		s.writeAPIError(w, r, aerr)
		return
	}
	reason := sanitizeField(r.Header.Get("X-Reason"), s.mcfg.Limits.MaxNameBytes)
	if aerr := s.auditDestructive(r, "connection", name, "close", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if closer, ok := any(srv).(interface {
		CloseConnection(id, reason string) error
	}); ok {
		if err := closer.CloseConnection(name, reason); err != nil {
			if errors.Is(err, server.ErrConnectionNotFound) {
				s.writeAPIError(w, r, errNotFoundMsg("unknown connection"))
				return
			}
			s.writeAPIError(w, r, errBad("could not close connection: "+err.Error()))
			return
		}
	} else if conn.Conn != nil {
		_ = conn.Conn.Close()
	}
	s.auditMutation(r, "connection", name, "close", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) hCloseConnectionsByUser(w http.ResponseWriter, r *http.Request) {
	user := pathValue(endpointLabel(r), r.URL.EscapedPath(), "user")
	srv := s.opts.Server
	if srv == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	p := principalFrom(r)
	reason := sanitizeField(r.Header.Get("X-Reason"), s.mcfg.Limits.MaxNameBytes)
	closed := 0
	srv.Mutex.RLock()
	targets := make([]*protocol.Connection, 0)
	names := make([]string, 0)
	for id, conn := range srv.Connections {
		if conn.Username == user {
			targets = append(targets, conn)
			names = append(names, id)
		}
	}
	srv.Mutex.RUnlock()
	closer, hasCloser := any(srv).(interface {
		CloseConnection(id, reason string) error
	})
	for i, conn := range targets {
		if aerr := s.canClose(p, user); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.auditDestructive(r, "connection", names[i], "close", 0); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		if hasCloser {
			// One bounded graceful close per connection (BOB-5); a connection
			// that vanished between the snapshot and the call is a no-op.
			if err := closer.CloseConnection(names[i], reason); err != nil && !errors.Is(err, server.ErrConnectionNotFound) {
				s.log.Warn("could not close connection", zap.String("connection_id", names[i]), zap.Error(err))
				continue
			}
			closed++
		} else if conn.Conn != nil {
			_ = conn.Conn.Close()
			closed++
		}
	}
	s.auditMutation(r, "connection", user, "close-by-user", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

// canClose enforces AUTH-015. With broker authentication disabled there is no
// per-connection owner to compare against, so BOTH explicit opt-ins are
// required. The guest identity is NAMED "guest" but is never an owner — and
// the cookie path sets Authenticated=true on it — so the auth-disabled branch
// is keyed on p.Unauthenticated, never on the name. A principal that is
// neither authenticated nor the known guest identity fails closed.
func (s *Server) canClose(p *Principal, ownerUser string) *apiError {
	if p == nil {
		return errUnauth("authentication required")
	}
	switch {
	case p.Unauthenticated, !s.brokerAuthEnabled():
		if p.Unauthenticated && !s.mcfg.AllowUnauthenticatedMutations {
			return errRefused("unauthenticated management mutations are disabled")
		}
		if !s.mcfg.AllowConnectionClose {
			return errRefused("closing connections is disabled (management.allow_connection_close)")
		}
		return nil
	case p.Authenticated:
		if p.Name == ownerUser {
			return nil
		}
		if p.Has(capCloseOthers) {
			return nil
		}
		return errRefused("not permitted to close another user's connection")
	default:
		// Neither authenticated nor the known guest identity: never an owner.
		return errRefused("not permitted to close another user's connection")
	}
}

// --- definitions ---
//
// The export/import types and the parse -> plan -> apply engine live in
// definitions.go (spec mgmt-05). These handlers are thin: they enforce the
// vhost, body-limit, capability, and rate-limit gates, then delegate to the
// single planner so dry-run and apply share one decision procedure.

// definitionsVhostOK enforces FR-CONV-003/D-1 for the {vhost} route variant: a
// {vhost} segment that does not decode to "/" is 404 "unknown vhost".
func definitionsVhostOK(r *http.Request) bool {
	pat := endpointLabel(r)
	if !strings.Contains(pat, "{vhost}") {
		return true
	}
	return pathValue(pat, r.URL.EscapedPath(), "vhost") == "/"
}

func (s *Server) hDefinitionsExport(w http.ResponseWriter, r *http.Request) {
	if !definitionsVhostOK(r) {
		s.writeAPIError(w, r, errNotFoundMsg("unknown vhost"))
		return
	}
	p := principalFrom(r)
	cur, terr := s.currentTopology()
	if terr != nil {
		s.writeAPIError(w, r, errUnavailableMsg(terr.Error()))
		return
	}
	doc := s.buildDefinitionsExport(cur, p != nil && p.Has(capManageUsers))
	s.writeJSON(w, r, http.StatusOK, doc)
}

func (s *Server) hDefinitionsImport(w http.ResponseWriter, r *http.Request) {
	if !definitionsVhostOK(r) {
		s.writeAPIError(w, r, errNotFoundMsg("unknown vhost"))
		return
	}
	p := principalFrom(r)
	// A principal lacking mutate_topology is refused 403 BEFORE parsing
	// (FR-API-122, mgmt-05 §2.1).
	if aerr := s.requireCapability(p, capMutate); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	dryRun, aerr := boolParam(r.URL.Query(), "dry_run")
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	// Import rate limit (SEC-067). One import per definitions_import_seconds
	// window, per principal; the dry-run counts against the same budget.
	if !s.importLimit.allow(p.Name) {
		setRateLimitHeaders(w, s.importLimit, p.Name)
		// Retry-After is the rate-limit WINDOW (definitions_import_seconds),
		// the shortest a client must wait for its next import to be admitted.
		retryAfter := s.mcfg.RateLimits.DefinitionsImportSeconds
		if retryAfter < 1 {
			retryAfter = 1
		}
		s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited,
			"definitions import rate limit exceeded", withHeader("Retry-After", strconv.Itoa(retryAfter))))
		return
	}
	setRateLimitHeaders(w, s.importLimit, p.Name)

	limit := s.mcfg.Limits.MaxImportBodyBytes
	if limit <= 0 {
		// A directly constructed Options may leave the import limit unset;
		// default to the documented 20 MiB rather than 413 every import.
		limit = 20 << 20
	}
	body, aerr := s.readImportBody(w, r, limit)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	payload, perr := parseDefinitions(body)
	if perr != nil {
		s.writeAPIError(w, r, errBad("invalid definitions JSON: "+perr.Error()))
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	cur, terr := s.currentTopology()
	if terr != nil {
		// Read-only at this point: nothing was declared or audited, so a
		// dry-run and an apply refuse identically (review finding 1).
		s.writeAPIError(w, r, errUnavailableMsg(terr.Error()))
		return
	}
	caps := defsCaps{
		MaxQueues:    s.mcfg.Definitions.MaxQueues,
		MaxExchanges: s.mcfg.Definitions.MaxExchanges,
		MaxBindings:  s.mcfg.Definitions.MaxBindings,
	}
	actions := s.planDefinitions(payload, cur, caps)

	// Dry-run: plan only, no mutation, no audit (OPS-020: reads are not
	// audited). The plan preview equals the apply outcome on the same topology
	// because both run the same executor (D-3).
	if dryRun {
		applied, skipped, failed := s.runDefinitions(actions, p, true)
		s.writeJSON(w, r, http.StatusOK, defsImportResponse{
			Applied: applied, Skipped: skipped, Failed: failed,
			StrangeQ: defsImportResultStrangeQ{Subset: true, Atomic: false, DryRun: true},
		})
		return
	}

	// Import is a destructive mutation (SEC-084): the audit record is durable
	// BEFORE any entity is applied; a sink overflow ⇒ 503 and nothing applied.
	if aerr := s.auditDestructive(r, "definitions", "", "import", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}

	// A large apply goes async on the bounded job pool (FR-API-140…143); the
	// dry-run is always synchronous (the UI needs the preview immediately).
	if thr := s.mcfg.Async.EntityThreshold; thr > 0 && countApply(actions) > thr {
		job, jerr := s.jobs.submit("definitions_import", p.Name, map[string]any{"vhost": "/"}, func() (any, error) {
			applied, skipped, failed := s.runDefinitions(actions, p, false)
			return defsImportJobResult{Applied: applied, Skipped: skipped, Failed: failed}, nil
		})
		if jerr != nil {
			s.writeAPIError(w, r, errUnavailableMsg("could not enqueue import job: "+jerr.Error()))
			return
		}
		w.Header().Set("Location", "/api/strangeq/jobs/"+job.id)
		w.Header().Set("Retry-After", "1")
		s.auditMutation(r, "definitions", "", "import", http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	applied, skipped, failed := s.runDefinitions(actions, p, false)
	s.auditMutation(r, "definitions", "", "import", http.StatusOK)
	s.writeJSON(w, r, http.StatusOK, defsImportResponse{
		Applied: applied, Skipped: skipped, Failed: failed,
		StrangeQ: defsImportResultStrangeQ{Subset: true, Atomic: false},
	})
}

// readImportBody reads the definitions document, capping the body BEFORE any
// JSON parsing (SEC-065/FR-API-124: http.MaxBytesReader wraps the stream). It
// accepts application/json or a multipart/form-data "file" part.
func (s *Server) readImportBody(w http.ResponseWriter, r *http.Request, limit int) ([]byte, *apiError) {
	if isMultipart(r) {
		// The pre-routing cap uses Content-Length and is bypassed by a chunked
		// body (ContentLength == -1), so bound the reader here too:
		// ParseMultipartForm(maxMemory) treats its argument as an in-memory
		// threshold, NOT a hard cap, and would otherwise consume an unbounded
		// stream.
		r.Body = http.MaxBytesReader(w, r.Body, int64(limit))
		if err := r.ParseMultipartForm(int64(limit)); err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				return nil, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
					"request body exceeds "+strconv.Itoa(limit)+" bytes")
			}
			return nil, errBad("invalid multipart form: " + err.Error())
		}
		part, _, ferr := r.FormFile("file")
		if ferr != nil {
			return nil, errBad("multipart import requires a \"file\" part")
		}
		defer func() { _ = part.Close() }()
		data, rerr := io.ReadAll(io.LimitReader(part, int64(limit)+1))
		if rerr != nil {
			var mbe *http.MaxBytesError
			if errors.As(rerr, &mbe) {
				return nil, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
					"request body exceeds "+strconv.Itoa(limit)+" bytes")
			}
			return nil, errBad("could not read the multipart file part: " + rerr.Error())
		}
		if len(data) > limit {
			return nil, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
				"definitions import exceeds "+strconv.Itoa(limit)+" bytes")
		}
		return data, nil
	}
	// application/json: cap first, then read (the JSON is never parsed when the
	// cap trips, so an over-limit body cannot reach the decoder).
	r.Body = http.MaxBytesReader(w, r.Body, int64(limit))
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
			"request body exceeds "+strconv.Itoa(limit)+" bytes")
	}
	return data, nil
}

// --- helpers ---

// ifMatchGate implements the If-Match precondition (RFC 9110 §13.1.1) for a
// single resource. current is the resource's canonical representation (the
// same one the GET endpoint renders) and ok reports whether it exists. A
// missing resource, or a present one whose ETag matches none of the supplied
// validators, is 412 precondition_failed. An absent If-Match header is a no-op.
// `If-Match: *` requires existence.
func (s *Server) ifMatchGate(r *http.Request, ok bool, current any) *apiError {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" {
		return nil
	}
	if !ok {
		return newAPIError(http.StatusPreconditionFailed, errPrecondition,
			"the resource does not exist")
	}
	body, err := marshalCompact(current)
	if err != nil {
		return newAPIError(http.StatusInternalServerError, errInternal, "internal error")
	}
	if !etagStrongMatches(raw, etagFor(body)) {
		return newAPIError(http.StatusPreconditionFailed, errPrecondition,
			"the resource has changed since it was read")
	}
	return nil
}

// definitionsConfigureAllowed reports whether the importing principal may
// configure the named resource in the default vhost. Built-in principals
// (p.User == nil) and a nil authenticator are exempt: requireCapability(capMutate)
// already gated the request. An inherit-mode user with a per-vhost configure
// pattern that refuses the name is denied (AUTH-012).
func (s *Server) definitionsConfigureAllowed(p *Principal, rtype interfaces.ResourceType, name string) bool {
	if p == nil || p.User == nil || s.opts.Authenticator == nil {
		return true
	}
	op := interfaces.Operation{
		Action:       interfaces.ActionConfigure,
		ResourceType: rtype,
		Resource:     name,
		VHost:        "/",
	}
	return s.opts.Authenticator.Authorize(p.User, op) == nil
}

func boolParam(q url.Values, key string) (bool, *apiError) {
	raw, ok := firstPresent(q, key)
	if !ok {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, errBad(key + " must be true or false")
	}
	return v, nil
}

// locationFor builds a Location header for a created entity, percent-encoding
// the name so a CR/LF or other control character cannot split the header
// (SEC-085).
func locationFor(r *http.Request, pattern, name string) string {
	base := "/api/queues/%2F/" + url.PathEscape(name)
	if strings.Contains(pattern, "/exchanges/") {
		base = "/api/exchanges/%2F/" + url.PathEscape(name)
	}
	return base
}

func errEmpty() *apiError { return errUnavailableMsg("broker unavailable") }
