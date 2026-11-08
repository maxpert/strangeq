package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
)

// bindingItem is the management view of one binding: EXACTLY the seven keys of
// DATA-012, no more (spec mgmt-04 §1.6). arguments is mapped by FR-CONV-047 and
// properties_key by §1.3.
type bindingItem struct {
	Source          string `json:"source"`
	VHost           string `json:"vhost"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
	Arguments       any    `json:"arguments"`
	PropertiesKey   string `json:"properties_key"`

	// rawArguments is the ORIGINAL AMQP field table (never JSON-mapped). It is
	// carried so the DELETE matcher passes the exact arguments the broker
	// stored to UnbindQueueWithArguments: the JSON mapping would turn a []byte
	// or time.Time argument into a string/int and a like-for-like comparison
	// would then silently no-op instead of removing the binding. It is never
	// serialised.
	rawArguments map[string]interface{} `json:"-"`
}

// bindingArgsUnbinder is the argument-qualified queue.unbind seam (WS-A WI-2).
// It is discovered optionally because it is not part of UnifiedBroker.
type bindingArgsUnbinder interface {
	UnbindQueueWithArguments(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error
}

// bindingViewToItem maps one broker.BindingView onto the DATA-012 item.
func bindingViewToItem(v broker.BindingView) bindingItem {
	args := fieldTableJSON(v.Arguments)
	if args == nil {
		args = map[string]any{}
	}
	return bindingItem{
		Source:          v.Source,
		VHost:           "/",
		Destination:     v.Destination,
		DestinationType: v.DestinationType,
		RoutingKey:      v.RoutingKey,
		Arguments:       args,
		PropertiesKey:   propertiesKey(v.RoutingKey, v.Arguments),
		rawArguments:    v.Arguments,
	}
}

// bindingsSnapshot returns every binding as a DATA-012 item. present is false
// when the injected broker has no aggregate binding seam, in which case the
// caller emits 501 (the pre-BOB-4 contract).
func (s *Server) bindingsSnapshot() (items []bindingItem, present bool, aerr *apiError) {
	b := s.broker()
	if b == nil {
		return nil, false, errUnavailableMsg("broker unavailable")
	}
	lister, ok := b.(bindingLister)
	if !ok {
		return nil, false, nil
	}
	views, err := lister.ListBindings()
	if err != nil {
		return nil, false, errUnavailableMsg("could not list bindings: " + err.Error())
	}
	out := make([]bindingItem, 0, len(views))
	for _, v := range views {
		out = append(out, bindingViewToItem(v))
	}
	return out, true, nil
}

// notImplementedBindings is the 501 for every binding endpoint when the seam is
// absent (spec §1.4), recording the booked snapshot_partial metric.
func (s *Server) notImplementedBindings(w http.ResponseWriter, r *http.Request) {
	s.sink.ManagementSnapshotPartial(endpointLabel(r))
	s.writeAPIError(w, r, errNotImpl("binding listing is not available in this deployment"))
}

var bindingSortFields = []string{"source", "vhost", "destination", "destination_type", "routing_key", "properties_key"}

// hBindings serves GET /api/bindings and GET /api/bindings/{vhost}
// (FR-API-090). name matches routing_key; source/destination are exact matches.
func (s *Server) hBindings(w http.ResponseWriter, r *http.Request) {
	if pathValue(endpointLabel(r), r.URL.EscapedPath(), "vhost") != "" {
		if _, aerr := vhostParam(r); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
	}
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	items, present, aerr := s.bindingsSnapshot()
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if !present {
		s.notImplementedBindings(w, r)
		return
	}
	items = filterBindingsExact(items, r.URL.Query())
	s.writeBindingList(w, r, items, q)
}

// hQueueBindings serves GET /api/queues/{vhost}/{name}/bindings (FR-API-082):
// bindings whose destination is the queue. 404 if the queue does not exist.
func (s *Server) hQueueBindings(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}
	if b.GetQueues()[name] == nil {
		s.writeAPIError(w, r, errNotFoundMsg("unknown queue"))
		return
	}
	_ = vhost
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	items, present, aerr := s.bindingsSnapshot()
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if !present {
		s.notImplementedBindings(w, r)
		return
	}
	filtered := make([]bindingItem, 0, len(items))
	for _, it := range items {
		if it.DestinationType == "queue" && it.Destination == name {
			filtered = append(filtered, it)
		}
	}
	s.writeBindingList(w, r, filtered, q)
}

// hExchangeBindings serves GET /api/exchanges/{vhost}/{name}/bindings/{source|destination}
// (FR-API-057). 404 if the exchange does not exist.
func (s *Server) hExchangeBindings(w http.ResponseWriter, r *http.Request) {
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	direction := pathValue(endpointLabel(r), r.URL.EscapedPath(), "direction")
	if direction != "source" && direction != "destination" {
		s.writeAPIError(w, r, errNotFoundMsg("unknown binding listing"))
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
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	items, present, aerr := s.bindingsSnapshot()
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if !present {
		s.notImplementedBindings(w, r)
		return
	}
	filtered := make([]bindingItem, 0, len(items))
	for _, it := range items {
		if direction == "source" {
			if it.Source == name {
				filtered = append(filtered, it)
			}
			continue
		}
		if it.DestinationType == "exchange" && it.Destination == name {
			filtered = append(filtered, it)
		}
	}
	s.writeBindingList(w, r, filtered, q)
}

// filterBindingsExact applies the FR-CONV-006 reserved source/destination exact
// filters.
func filterBindingsExact(items []bindingItem, vals url.Values) []bindingItem {
	source, hasSource := firstPresent(vals, "source")
	dest, hasDest := firstPresent(vals, "destination")
	if !hasSource && !hasDest {
		return items
	}
	out := make([]bindingItem, 0, len(items))
	for _, it := range items {
		if hasSource && it.Source != source {
			continue
		}
		if hasDest && it.Destination != dest {
			continue
		}
		out = append(out, it)
	}
	return out
}

func (s *Server) writeBindingList(w http.ResponseWriter, r *http.Request, items []bindingItem, q *listQuery) {
	anyItems := make([]any, 0, len(items))
	for _, it := range items {
		anyItems = append(anyItems, it)
	}
	s.writeList(w, r, anyItems, bindingItem{}, q,
		func(v any) string { return v.(bindingItem).RoutingKey },
		sortFieldAllowlist(bindingSortFields))
}

// --- binding mutations ---

type bindingCreateRequest struct {
	RoutingKey string                 `json:"routing_key"`
	Arguments  map[string]interface{} `json:"arguments"`
}

// resourceAllowed applies a single per-resource authorization check (AUTH-012).
// Built-in principals (p.User == nil) and a nil authenticator are exempt: the
// capability gate in mutationGates already covered them.
func (s *Server) resourceAllowed(p *Principal, action interfaces.OperationAction, rtype interfaces.ResourceType, resource, vhost string) *apiError {
	if p == nil || p.User == nil || s.opts.Authenticator == nil {
		return nil
	}
	op := interfaces.Operation{Action: action, ResourceType: rtype, Resource: resource, VHost: vhost}
	if err := s.opts.Authenticator.Authorize(p.User, op); err != nil {
		return errRefused(mutationRefusal(action, rtype, resource))
	}
	return nil
}

// hBindingCreate serves POST /api/bindings/{vhost}/e/{source}/{dtype}/{destination}
// (FR-API-091/092). dtype is "q" (queue destination) or "e" (exchange
// destination).
func (s *Server) hBindingCreate(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	pattern := endpointLabel(r)
	path := r.URL.EscapedPath()
	source := pathValue(pattern, path, "source")
	dtype := pathValue(pattern, path, "dtype")
	destination := pathValue(pattern, path, "destination")

	body, aerr := s.readBodyLimited(r, s.mcfg.Limits.MaxBodyBytes)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req bindingCreateRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}

	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	p := principalFrom(r)
	switch dtype {
	case "q":
		if aerr := s.mutationGates(p, interfaces.ActionWrite, interfaces.ResourceQueue, destination, vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "bind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.resourceAllowed(p, interfaces.ActionRead, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(source), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "bind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if err := b.BindQueue(destination, source, req.RoutingKey, req.Arguments); err != nil {
			s.auditMutation(r, "binding", destination, "bind", http.StatusBadRequest)
			s.writeBindError(w, r, source, destination, err)
			return
		}
		key := propertiesKey(req.RoutingKey, req.Arguments)
		w.Header().Set("Location", "/api/bindings/%2F/e/"+url.PathEscape(source)+"/q/"+url.PathEscape(destination)+"/"+url.PathEscape(key))
		s.auditMutation(r, "binding", destination, "bind", http.StatusCreated)
		w.WriteHeader(http.StatusCreated)
	case "e":
		if aerr := s.mutationGates(p, interfaces.ActionWrite, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(destination), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "bind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.resourceAllowed(p, interfaces.ActionRead, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(source), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "bind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if err := b.BindExchange(destination, source, req.RoutingKey, req.Arguments); err != nil {
			s.auditMutation(r, "binding", destination, "bind", http.StatusBadRequest)
			s.writeBindError(w, r, source, destination, err)
			return
		}
		key := propertiesKey(req.RoutingKey, req.Arguments)
		w.Header().Set("Location", "/api/bindings/%2F/e/"+url.PathEscape(source)+"/e/"+url.PathEscape(destination)+"/"+url.PathEscape(key))
		s.auditMutation(r, "binding", destination, "bind", http.StatusCreated)
		w.WriteHeader(http.StatusCreated)
	default:
		s.writeAPIError(w, r, errBad("destination type must be 'q' or 'e'"))
	}
}

// writeBindError maps a bind failure onto the spec's 404/400 shapes.
func (s *Server) writeBindError(w http.ResponseWriter, r *http.Request, source, destination string, err error) {
	if isNotFoundErr(err, interfaces.ErrExchangeNotFound) {
		s.writeAPIError(w, r, errNotFoundMsg("no exchange '"+source+"' in vhost '/'"))
		return
	}
	if isNotFoundErr(err, interfaces.ErrQueueNotFound) {
		s.writeAPIError(w, r, errNotFoundMsg("no queue '"+destination+"' in vhost '/'"))
		return
	}
	// headers x-match validation, cycle detection, and any other broker
	// semantic refusal are a 400 with the broker's own text (FR-API-091/092).
	s.writeAPIError(w, r, errBad(err.Error()))
}

func isNotFoundErr(err, sentinel error) bool {
	return err != nil && errors.Is(err, sentinel)
}

// hBindingDelete serves DELETE /api/bindings/{vhost}/e/{source}/{dtype}/{destination}/{props}
// (FR-API-093/DIV-12). {props} is the percent-decoded properties_key; the
// handler never deletes a binding whose synthesised key differs.
func (s *Server) hBindingDelete(w http.ResponseWriter, r *http.Request) {
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	pattern := endpointLabel(r)
	path := r.URL.EscapedPath()
	source := pathValue(pattern, path, "source")
	dtype := pathValue(pattern, path, "dtype")
	destination := pathValue(pattern, path, "destination")
	props := pathValue(pattern, path, "props")

	items, present, aerr := s.bindingsSnapshot()
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if !present {
		s.notImplementedBindings(w, r)
		return
	}

	b := s.broker()
	if b == nil {
		s.writeAPIError(w, r, errUnavailableMsg("broker unavailable"))
		return
	}

	switch dtype {
	case "q":
		var match *bindingItem
		for i := range items {
			it := &items[i]
			if it.DestinationType != "queue" || it.Destination != destination || it.Source != source {
				continue
			}
			if it.PropertiesKey == props {
				match = it
				break
			}
		}
		if match == nil {
			s.writeAPIError(w, r, errNotFoundMsg("unknown binding"))
			return
		}
		p := principalFrom(r)
		if aerr := s.mutationGates(p, interfaces.ActionWrite, interfaces.ResourceQueue, destination, vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "unbind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.resourceAllowed(p, interfaces.ActionRead, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(source), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "unbind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.auditDestructive(r, "binding", destination, "unbind", 0); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		ub, ok := b.(bindingArgsUnbinder)
		if !ok {
			s.writeAPIError(w, r, errUnavailableMsg("argument-qualified unbind is not available in this deployment"))
			return
		}
		if err := ub.UnbindQueueWithArguments(destination, source, match.RoutingKey, match.rawArguments); err != nil {
			s.auditMutation(r, "binding", destination, "unbind", http.StatusBadRequest)
			s.writeAPIError(w, r, errBad("could not unbind: "+err.Error()))
			return
		}
		s.auditMutation(r, "binding", destination, "unbind", http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
	case "e":
		var match *bindingItem
		for i := range items {
			it := &items[i]
			if it.DestinationType != "exchange" || it.Destination != destination || it.Source != source {
				continue
			}
			if it.PropertiesKey == props {
				match = it
				break
			}
		}
		if match == nil {
			s.writeAPIError(w, r, errNotFoundMsg("unknown binding"))
			return
		}
		p := principalFrom(r)
		if aerr := s.mutationGates(p, interfaces.ActionWrite, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(destination), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "unbind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.resourceAllowed(p, interfaces.ActionRead, interfaces.ResourceExchange, interfaces.NormalizeExchangeName(source), vhost); aerr != nil {
			s.auditMutation(r, "binding", destination, "unbind", aerr.status)
			s.writeAPIError(w, r, aerr)
			return
		}
		if aerr := s.auditDestructive(r, "binding", destination, "unbind", 0); aerr != nil {
			s.writeAPIError(w, r, aerr)
			return
		}
		if err := b.UnbindExchange(destination, source, match.RoutingKey); err != nil {
			s.auditMutation(r, "binding", destination, "unbind", http.StatusBadRequest)
			s.writeAPIError(w, r, errBad("could not unbind: "+err.Error()))
			return
		}
		s.auditMutation(r, "binding", destination, "unbind", http.StatusNoContent)
		w.WriteHeader(http.StatusNoContent)
	default:
		s.writeAPIError(w, r, errBad("destination type must be 'q' or 'e'"))
	}
}

// matchArguments extracts the raw (pre-JSON) field table from a matched item so
// the argument-qualified unbind compares like-for-like.
