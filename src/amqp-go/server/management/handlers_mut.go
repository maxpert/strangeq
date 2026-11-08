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
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
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
	srv.Mutex.RUnlock()
	if conn == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown connection"))
		return
	}
	p := principalFrom(r)
	if aerr := s.canClose(p, conn.Username); aerr != nil {
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
	for i, conn := range targets {
		if aerr := s.canClose(p, conn.Username); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.auditDestructive(r, "connection", names[i], "close", 0); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		if conn.Conn != nil {
			_ = conn.Conn.Close()
			closed++
		}
	}
	s.auditMutation(r, "connection", user, "close-by-user", http.StatusNoContent)
	w.WriteHeader(http.StatusNoContent)
}

// canClose enforces AUTH-015. With broker authentication disabled there is no
// per-connection owner to compare against, so BOTH explicit opt-ins are
// required and they are checked BEFORE any name comparison: the
// unauthenticated principal is itself named "guest", so comparing p.Name with
// an owner would otherwise authorise a close whenever the connection's
// Username happened to be "guest" (SEC-066).
func (s *Server) canClose(p *Principal, ownerUser string) *apiError {
	if p == nil {
		return errUnauth("authentication required")
	}
	if !s.brokerAuthEnabled() {
		if p.Unauthenticated && !s.mcfg.AllowUnauthenticatedMutations {
			return errRefused("unauthenticated management mutations are disabled")
		}
		if !s.mcfg.AllowConnectionClose {
			return errRefused("closing connections is disabled (management.allow_connection_close)")
		}
		return nil
	}
	if p.Authenticated && p.Name == ownerUser {
		return nil
	}
	if p.Has(capCloseOthers) {
		return nil
	}
	return errRefused("not permitted to close another user's connection")
}

// --- definitions ---

type definitionsStrangeQ struct {
	Product    string   `json:"product"`
	Version    string   `json:"version"`
	ExportedAt string   `json:"exported_at"`
	Subset     bool     `json:"subset"`
	Excludes   []string `json:"excludes"`
}

type definitionsExport struct {
	StrangeQ    any              `json:"strangeq"`
	VHosts      []vhostInfo      `json:"vhosts"`
	Exchanges   []exchangeInfo   `json:"exchanges"`
	Queues      []queueFullInfo  `json:"queues"`
	Bindings    []any            `json:"bindings"`
	Users       []userInfo       `json:"users,omitempty"`
	Permissions []permissionInfo `json:"permissions,omitempty"`
}

func (s *Server) hDefinitionsExport(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r)
	if pathValue(endpointLabel(r), r.URL.EscapedPath(), "vhost") != "" {
		if _, aerr := vhostParam(r); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
	}
	exchanges := s.exchangeList()
	queues := s.queueFulls(s.queueSummaries())
	exItems := make([]exchangeInfo, len(exchanges))
	copy(exItems, exchanges)
	qItems := make([]queueFullInfo, len(queues))
	copy(qItems, queues)

	excludes := []string{"bindings", "policies", "parameters", "global_parameters", "vhost_limits", "user_limits"}
	resp := definitionsExport{
		StrangeQ: definitionsStrangeQ{
			Product:    "StrangeQ",
			Version:    s.version(),
			ExportedAt: time.Now().UTC().Format(time.RFC3339),
			Subset:     true,
			Excludes:   excludes,
		},
		VHosts:    []vhostInfo{singleVHost},
		Exchanges: exItems,
		Queues:    qItems,
		Bindings:  []any{},
	}
	// users/permissions are included only with manage_users (AUTH-024).
	if p.Has(capManageUsers) {
		users := s.users.list()
		resp.Users = make([]userInfo, 0, len(users))
		for _, u := range users {
			tags := u.Tags
			if tags == nil {
				tags = []string{}
			}
			resp.Users = append(resp.Users, userInfo{Name: u.Name, Tags: tags})
		}
		resp.Permissions = permissionInfos(s.users.permissions())
	} else {
		resp.StrangeQ = definitionsStrangeQ{
			Product:    "StrangeQ",
			Version:    s.version(),
			ExportedAt: resp.StrangeQ.(definitionsStrangeQ).ExportedAt,
			Subset:     true,
			Excludes:   append(excludes, "users", "permissions"),
		}
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

type definitionsImportRequest struct {
	VHosts      []map[string]any `json:"vhosts"`
	Exchanges   []map[string]any `json:"exchanges"`
	Queues      []map[string]any `json:"queues"`
	Bindings    []map[string]any `json:"bindings"`
	Users       []map[string]any `json:"users"`
	Permissions []map[string]any `json:"permissions"`
	Parameters  []map[string]any `json:"parameters"`
}

type definitionsImportResult struct {
	Applied  []string `json:"applied"`
	Skipped  []string `json:"skipped"`
	Failed   []string `json:"failed"`
	StrangeQ any      `json:"strangeq"`
}

func (s *Server) hDefinitionsImport(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r)
	if aerr := s.requireCapability(p, capMutate); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	limit := s.mcfg.Limits.MaxImportBodyBytes
	var body []byte
	if isMultipart(r) {
		// multipart/form-data import (RabbitMQ-compatible): the definitions
		// document is the "file" part. The pre-routing body cap has already
		// bounded the request; the part itself is bounded here too.
		if err := r.ParseMultipartForm(int64(limit)); err != nil {
			s.writeAPIError(w, r, errBad("invalid multipart form: "+err.Error()))
			return
		}
		part, _, ferr := r.FormFile("file")
		if ferr != nil {
			s.writeAPIError(w, r, errBad("multipart import requires a \"file\" part"))
			return
		}
		defer func() { _ = part.Close() }()
		data, rerr := io.ReadAll(io.LimitReader(part, int64(limit)+1))
		if rerr != nil {
			s.writeAPIError(w, r, errBad("could not read the multipart file part: "+rerr.Error()))
			return
		}
		if len(data) > limit {
			s.writeAPIError(w, r, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
				"definitions import exceeds "+strconv.Itoa(limit)+" bytes"))
			return
		}
		body = data
	} else {
		var aerr *apiError
		body, aerr = s.readBodyLimited(r, limit)
		if aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
	}
	var req definitionsImportRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeAPIError(w, r, errBad("invalid definitions JSON: "+err.Error()))
		return
	}
	if aerr := s.auditDestructive(r, "definitions", "", "import", 0); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	res := definitionsImportResult{Applied: []string{}, Skipped: []string{}, Failed: []string{}}
	res.StrangeQ = map[string]any{"subset": true, "atomic": false}

	declaredExchanges := 0
	for _, ex := range req.Exchanges {
		name, _ := ex["name"].(string)
		typ, _ := ex["type"].(string)
		if name == "" {
			res.Failed = append(res.Failed, "exchange:missing-name")
			continue
		}
		if s.mcfg.Definitions.MaxExchanges > 0 && declaredExchanges >= s.mcfg.Definitions.MaxExchanges {
			res.Failed = append(res.Failed, "exchange:"+name)
			continue
		}
		durable, _ := ex["durable"].(bool)
		autoDelete, _ := ex["auto_delete"].(bool)
		internal, _ := ex["internal"].(bool)
		var args map[string]interface{}
		if a, ok := ex["arguments"].(map[string]any); ok {
			args = a
		}
		if !validExchangeTypes[typ] {
			res.Failed = append(res.Failed, "exchange:"+name)
			continue
		}
		// Per-target configure authorization (AUTH-012): the import must not
		// bypass the resource gate a direct PUT /api/exchanges/{vhost}/{name}
		// would apply. A denied target is reported, never declared.
		if !s.definitionsConfigureAllowed(p, interfaces.ResourceExchange, name) {
			res.Failed = append(res.Failed, "exchange:"+name)
			continue
		}
		if existing := b.GetExchanges()[name]; existing != nil {
			res.Skipped = append(res.Skipped, "exchange:"+name)
			continue
		}
		if err := b.DeclareExchange(name, typ, durable, autoDelete, internal, args); err != nil {
			res.Failed = append(res.Failed, "exchange:"+name)
			continue
		}
		res.Applied = append(res.Applied, "exchange:"+name)
		declaredExchanges++
	}

	declaredQueues := 0
	for _, q := range req.Queues {
		name, _ := q["name"].(string)
		if name == "" {
			res.Failed = append(res.Failed, "queue:missing-name")
			continue
		}
		if s.mcfg.Definitions.MaxQueues > 0 && declaredQueues >= s.mcfg.Definitions.MaxQueues {
			res.Failed = append(res.Failed, "queue:"+name)
			continue
		}
		// Per-target configure authorization (AUTH-012), exactly as a direct
		// PUT /api/queues/{vhost}/{name} would apply.
		if !s.definitionsConfigureAllowed(p, interfaces.ResourceQueue, name) {
			res.Failed = append(res.Failed, "queue:"+name)
			continue
		}
		durable, _ := q["durable"].(bool)
		autoDelete, _ := q["auto_delete"].(bool)
		exclusive, _ := q["exclusive"].(bool)
		var args map[string]interface{}
		if a, ok := q["arguments"].(map[string]any); ok {
			args = a
		}
		if b.GetQueues()[name] != nil {
			res.Skipped = append(res.Skipped, "queue:"+name)
			continue
		}
		if _, err := b.DeclareQueue(name, durable, autoDelete, exclusive, args); err != nil {
			res.Failed = append(res.Failed, "queue:"+name)
			continue
		}
		res.Applied = append(res.Applied, "queue:"+name)
		declaredQueues++
	}

	// Sections StrangeQ cannot apply are skipped with explicit reasons
	// (COMPAT-024, FR-API-125).
	for range req.Users {
		res.Skipped = append(res.Skipped, "users:no-runtime-user-store")
	}
	for range req.Permissions {
		res.Skipped = append(res.Skipped, "permissions:no-runtime-user-store")
	}
	for range req.Bindings {
		res.Skipped = append(res.Skipped, "bindings:aggregate-listing-unavailable")
	}
	for range req.Parameters {
		res.Skipped = append(res.Skipped, "parameters:not-implemented")
	}
	for range req.VHosts {
		res.Skipped = append(res.Skipped, "vhosts:only-default-vhost")
	}

	s.auditMutation(r, "definitions", "", "import", http.StatusOK)
	s.writeJSON(w, r, http.StatusOK, res)
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
	if !etagMatches(raw, etagFor(body)) {
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
