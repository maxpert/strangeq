package management

import (
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
)

// vhostParam reads the {vhost} path segment and enforces the "only %2F
// (decoded to /)" rule (FR-CONV-003/D-1). Any other decoded value is 404
// "unknown vhost".
func vhostParam(r *http.Request) (string, *apiError) {
	v := pathValue(endpointLabel(r), r.URL.EscapedPath(), "vhost")
	if v != "/" {
		return "", errNotFoundMsg("unknown vhost")
	}
	return v, nil
}

// --- nodes ---

type nodeInfo struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Running       bool   `json:"running"`
	MemUsed       any    `json:"mem_used"`
	MemLimit      any    `json:"mem_limit"`
	DiskFree      any    `json:"disk_free"`
	DiskFreeLimit any    `json:"disk_free_limit"`
	Uptime        int64  `json:"uptime"`
	Processors    int    `json:"processors"`
	OS            any    `json:"os"`
}

func (s *Server) nodeInfo() nodeInfo {
	return nodeInfo{
		Name:       s.nodeName,
		Type:       "disc",
		Running:    true,
		Uptime:     s.uptimeSeconds(),
		Processors: runtime.NumCPU(),
		OS:         runtime.GOOS,
	}
}

func (s *Server) hNodes(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, []nodeInfo{s.nodeInfo()})
}

func (s *Server) hNode(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	if name != s.nodeName {
		s.writeAPIError(w, r, errNotFoundMsg("unknown node"))
		return
	}
	s.writeJSON(w, r, http.StatusOK, s.nodeInfo())
}

// --- connections ---

type connectionInfo struct {
	Name           string `json:"name"`
	Node           string `json:"node"`
	VHost          string `json:"vhost"`
	User           string `json:"user"`
	State          string `json:"state"`
	Channels       int    `json:"channels"`
	Protocol       string `json:"protocol"`
	Host           string `json:"host"`
	PeerHost       string `json:"peer_host"`
	PeerPort       any    `json:"peer_port"`
	ConnectedAt    any    `json:"connected_at"`
	LastActivity   any    `json:"last_activity"`
	Timeout        any    `json:"timeout"`
	RecvOct        any    `json:"recv_oct"`
	SendOct        any    `json:"send_oct"`
	ChannelsDetail any    `json:"channels_detail"`
}

// connectionsSnapshot reads Server.Connections under Server.Mutex.RLock (a
// permitted lock), copying the fields it needs before releasing it.
func (s *Server) connectionsSnapshot() []connectionInfo {
	srv := s.opts.Server
	if srv == nil {
		return nil
	}
	alarm := srv.AlarmState() != 0
	srv.Mutex.RLock()
	out := make([]connectionInfo, 0, len(srv.Connections))
	for id, conn := range srv.Connections {
		info := connectionInfo{
			Name:     id,
			Node:     s.nodeName,
			VHost:    conn.Vhost,
			User:     conn.Username,
			Protocol: "AMQP 0-9-1",
			State:    connectionState(conn, alarm),
		}
		conn.Channels.Range(func(_, _ any) bool {
			info.Channels++
			return true
		})
		if conn.Conn != nil {
			if host, port, err := net.SplitHostPort(conn.Conn.RemoteAddr().String()); err == nil {
				info.PeerHost = host
				if p, perr := strconv.Atoi(port); perr == nil {
					info.PeerPort = p
				}
			}
		}
		if !conn.ConnectedAt.IsZero() {
			info.ConnectedAt = conn.ConnectedAt.UTC().Format(time.RFC3339)
		}
		if la := conn.GetLastActivity(); !la.IsZero() {
			info.LastActivity = la.UTC().Format(time.RFC3339)
		}
		out = append(out, info)
	}
	srv.Mutex.RUnlock()
	return out
}

func connectionState(conn *protocol.Connection, alarm bool) string {
	switch {
	case conn.Closed.Load():
		return "closing"
	case alarm && conn.HasPublished.Load():
		return "blocked"
	default:
		return "running"
	}
}

func (s *Server) hConnections(w http.ResponseWriter, r *http.Request) {
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	conns := s.connectionsSnapshot()
	if user, ok := firstPresent(r.URL.Query(), "user"); ok && user != "" {
		conns = filterConnections(conns, user)
	}
	items := make([]any, 0, len(conns))
	for _, c := range conns {
		items = append(items, c)
	}
	s.writeList(w, r, items, connectionInfo{}, q,
		func(v any) string { return v.(connectionInfo).Name },
		sortFieldAllowlist([]string{"name", "user", "vhost", "state", "channels", "connected_at", "last_activity", "peer_host", "peer_port"}))
}

func filterConnections(conns []connectionInfo, user string) []connectionInfo {
	out := conns[:0]
	for _, c := range conns {
		if c.User == user {
			out = append(out, c)
		}
	}
	return out
}

func (s *Server) hConnectionsByUser(w http.ResponseWriter, r *http.Request) {
	user := pathValue(endpointLabel(r), r.URL.EscapedPath(), "user")
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	conns := filterConnections(s.connectionsSnapshot(), user)
	items := make([]any, 0, len(conns))
	for _, c := range conns {
		items = append(items, c)
	}
	s.writeList(w, r, items, connectionInfo{}, q,
		func(v any) string { return v.(connectionInfo).Name },
		sortFieldAllowlist([]string{"name", "user", "vhost", "state", "channels"}))
}

func (s *Server) hConnection(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	for _, c := range s.connectionsSnapshot() {
		if c.Name == name {
			s.writeJSON(w, r, http.StatusOK, c)
			return
		}
	}
	s.writeAPIError(w, r, errNotFoundMsg("unknown connection"))
}

// --- channels ---

type channelInfo struct {
	Number        uint16 `json:"number"`
	Connection    string `json:"connection"`
	User          string `json:"user"`
	VHost         string `json:"vhost"`
	State         string `json:"state"`
	Consumers     int    `json:"consumers"`
	Unacked       any    `json:"unacked"`
	PrefetchCount uint16 `json:"prefetch_count"`
	Confirm       bool   `json:"confirm"`
	Transactional bool   `json:"transactional"`
	StatsPartial  bool   `json:"stats_partial"`
}

// channelState derives the DATA-006 `state` with the frozen precedence
// closing > flow > running (spec mgmt-04 §3.4). It is never fabricated: an open
// channel with flow active is running, a flow-stopped one is flow, and only a
// server-initiated close awaiting close-ok is closing.
func channelState(v server.ChannelView) string {
	switch {
	case v.Closing:
		return "closing"
	case !v.FlowActive:
		return "flow"
	default:
		return "running"
	}
}

func (s *Server) channelsSnapshot() []channelInfo {
	srv := s.opts.Server
	if srv == nil {
		return nil
	}
	hasSeam := false
	if views, ok := any(srv).(channelViews); ok {
		hasSeam = true
		out := make([]channelInfo, 0)
		for _, v := range views.ChannelViews() {
			ci := channelInfo{
				Number:        v.Number,
				Connection:    v.ConnectionID,
				User:          v.User,
				VHost:         v.Vhost,
				Consumers:     v.Consumers,
				Confirm:       v.Confirm,
				PrefetchCount: v.PrefetchCount,
				Transactional: v.Transactional,
			}
			// BOB-3: state precedence closing > flow > running; never fabricated.
			ci.State = channelState(v)
			// unacked is the only remaining nullable field (mgmt-03 §2.1): it is
			// null exactly when an owning consumer's delivered set is unknown.
			if v.HasUnacked {
				ci.Unacked = v.Unacked
			} else {
				ci.StatsPartial = true
			}
			out = append(out, ci)
		}
		return out
	}
	_ = hasSeam
	// Fallback: enumerate channels from the connection registry, reading only
	// lock-free atomics (BOB-3 promoted PrefetchCount/Transactional to atomics).
	var out []channelInfo
	srv.Mutex.RLock()
	for id, conn := range srv.Connections {
		conn.Channels.Range(func(_, value any) bool {
			ch, ok := value.(*protocol.Channel)
			if !ok {
				return true
			}
			ci := channelInfo{
				Number:        ch.ID,
				Connection:    id,
				User:          conn.Username,
				VHost:         conn.Vhost,
				Confirm:       ch.ConfirmMode.Load(),
				PrefetchCount: uint16(ch.PrefetchCount.Load()),
				Transactional: ch.Transactional.Load(),
				StatsPartial:  true, // unacked is unknown without the consumer seam
			}
			if ch.FlowActive.Load() {
				ci.State = "running"
			} else {
				ci.State = "flow"
			}
			out = append(out, ci)
			return true
		})
	}
	srv.Mutex.RUnlock()
	return out
}

func (s *Server) hChannels(w http.ResponseWriter, r *http.Request) {
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	chans := s.channelsSnapshot()
	items := make([]any, 0, len(chans))
	for _, c := range chans {
		items = append(items, c)
	}
	s.writeList(w, r, items, channelInfo{}, q,
		func(v any) string { return strconv.Itoa(int(v.(channelInfo).Number)) },
		sortFieldAllowlist([]string{"number", "connection", "user", "vhost", "state", "consumers"}))
}

func (s *Server) hChannel(w http.ResponseWriter, r *http.Request) {
	raw := pathValue(endpointLabel(r), r.URL.EscapedPath(), "id")
	if c, ok := resolveChannelID(raw, s.channelsSnapshot()); ok {
		s.writeJSON(w, r, http.StatusOK, c)
		return
	}
	s.writeAPIError(w, r, errNotFoundMsg("unknown channel"))
}

// resolveChannelID resolves the `{id}` path key deterministically (WS4 finding
// 5). The canonical key is the percent-decoded "{connection}:{number}"
// composite (connection ids are colon-free, so the split is unambiguous; spec
// mgmt-04 §3.6) and always resolves to exactly one channel.
//
// A bare integer is ALSO accepted, but only when it is UNAMBIGUOUS: it must
// match exactly one channel across every connection. When two connections both
// own a channel with that number the request 404s on every call instead of
// returning whichever connection Go's map order happened to yield.
func resolveChannelID(raw string, channels []channelInfo) (channelInfo, bool) {
	if i := strings.IndexByte(raw, ':'); i >= 0 {
		connID, numStr := raw[:i], raw[i+1:]
		n, err := strconv.Atoi(numStr)
		if err != nil {
			return channelInfo{}, false
		}
		for _, c := range channels {
			if c.Connection == connID && int(c.Number) == n {
				return c, true
			}
		}
		return channelInfo{}, false
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return channelInfo{}, false
	}
	var match channelInfo
	found := false
	for _, c := range channels {
		if int(c.Number) != n {
			continue
		}
		if found {
			// A second match makes the bare number ambiguous: refuse it.
			return channelInfo{}, false
		}
		match, found = c, true
	}
	return match, found
}

// hConnectionChannels serves GET /api/connections/{name}/channels (FR-API-024):
// the connection's channels. An absent connection is 404.
func (s *Server) hConnectionChannels(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	found := false
	for _, c := range s.connectionsSnapshot() {
		if c.Name == name {
			found = true
			break
		}
	}
	if !found {
		s.writeAPIError(w, r, errNotFoundMsg("unknown connection"))
		return
	}
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	chans := s.channelsSnapshot()
	items := make([]any, 0, len(chans))
	for _, c := range chans {
		if c.Connection == name {
			items = append(items, c)
		}
	}
	s.writeList(w, r, items, channelInfo{}, q,
		func(v any) string { return strconv.Itoa(int(v.(channelInfo).Number)) },
		sortFieldAllowlist([]string{"number", "connection", "user", "vhost", "state", "consumers"}))
}

// --- consumers ---

type consumerInfo struct {
	ConsumerTag   string `json:"consumer_tag"`
	Queue         string `json:"queue"`
	Channel       any    `json:"channel"`
	Connection    any    `json:"connection"`
	AckRequired   bool   `json:"ack_required"`
	Exclusive     bool   `json:"exclusive"`
	PrefetchCount int    `json:"prefetch_count"`
	Arguments     any    `json:"arguments"`
	Unacked       any    `json:"unacked"`
}

func (s *Server) consumersSnapshot() []consumerInfo {
	b := s.broker()
	if b == nil {
		return nil
	}
	if views, ok := b.(consumerViews); ok {
		vs := views.ConsumerViews()
		out := make([]consumerInfo, 0, len(vs))
		for _, v := range vs {
			ci := consumerInfo{
				ConsumerTag:   v.Tag,
				Queue:         v.Queue,
				AckRequired:   !v.NoAck,
				Exclusive:     v.Exclusive,
				PrefetchCount: int(v.PrefetchCount),
			}
			if v.Args != nil {
				ci.Arguments = fieldTableJSON(v.Args)
			}
			if v.HasChannel {
				ci.Channel = int(v.ChannelID)
				ci.Connection = v.ConnectionID
			}
			if v.AckKnown {
				ci.Unacked = v.Unacked
			}
			out = append(out, ci)
		}
		return out
	}
	// M0 contract: core fields only; linkage/counts null.
	out := make([]consumerInfo, 0)
	for _, c := range b.GetConsumers() {
		ci := consumerInfo{
			ConsumerTag:   c.Tag,
			Queue:         c.Queue,
			AckRequired:   !c.NoAck,
			Exclusive:     c.Exclusive,
			PrefetchCount: int(c.PrefetchCount),
		}
		if c.Args != nil {
			ci.Arguments = fieldTableJSON(c.Args)
		}
		out = append(out, ci)
	}
	return out
}

func (s *Server) hConsumers(w http.ResponseWriter, r *http.Request) {
	// /api/consumers/{vhost} validates the vhost when present.
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
	cs := s.consumersSnapshot()
	if queue, ok := firstPresent(r.URL.Query(), "queue"); ok && queue != "" {
		filtered := make([]consumerInfo, 0, len(cs))
		for _, c := range cs {
			if c.Queue == queue {
				filtered = append(filtered, c)
			}
		}
		cs = filtered
	}
	if conn, ok := firstPresent(r.URL.Query(), "connection"); ok && conn != "" {
		filtered := make([]consumerInfo, 0, len(cs))
		for _, c := range cs {
			if s, isStr := c.Connection.(string); isStr && s == conn {
				filtered = append(filtered, c)
			}
		}
		cs = filtered
	}
	items := make([]any, 0, len(cs))
	for _, c := range cs {
		items = append(items, c)
	}
	s.writeList(w, r, items, consumerInfo{}, q,
		func(v any) string { return v.(consumerInfo).ConsumerTag },
		sortFieldAllowlist([]string{"consumer_tag", "queue", "channel", "connection", "ack_required", "exclusive", "prefetch_count"}))
}

// --- exchanges ---

type exchangeInfo struct {
	Name         string `json:"name"`
	VHost        string `json:"vhost"`
	Type         string `json:"type"`
	Durable      bool   `json:"durable"`
	AutoDelete   bool   `json:"auto_delete"`
	Internal     bool   `json:"internal"`
	Arguments    any    `json:"arguments"`
	MessageStats any    `json:"message_stats"`
}

func exchangeTypeName(protocolType string) string {
	switch protocolType {
	case "direct", "fanout", "topic", "headers":
		return protocolType
	default:
		return "direct"
	}
}

func (s *Server) exchangeList() []exchangeInfo {
	b := s.broker()
	if b == nil {
		return nil
	}
	exs := b.GetExchanges()
	out := make([]exchangeInfo, 0, len(exs))
	for _, ex := range exs {
		out = append(out, exchangeInfoOf(ex))
	}
	return out
}

// exchangeInfoOf renders one exchange exactly as the GET endpoint does, so an
// If-Match validator computed from it matches the ETag a client fetched.
func exchangeInfoOf(ex *protocol.Exchange) exchangeInfo {
	ei := exchangeInfo{
		Name:       ex.Name,
		VHost:      "/",
		Type:       exchangeTypeName(ex.Kind),
		Durable:    ex.Durable,
		AutoDelete: ex.AutoDelete,
		Internal:   ex.Internal,
	}
	if ex.Arguments != nil {
		ei.Arguments = fieldTableJSON(ex.Arguments)
	}
	return ei
}

func (s *Server) hExchanges(w http.ResponseWriter, r *http.Request) {
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	exs := s.exchangeList()
	if typ, ok := firstPresent(r.URL.Query(), "type"); ok && typ != "" {
		filtered := make([]exchangeInfo, 0, len(exs))
		for _, e := range exs {
			if e.Type == typ {
				filtered = append(filtered, e)
			}
		}
		exs = filtered
	}
	items := make([]any, 0, len(exs))
	for _, e := range exs {
		items = append(items, e)
	}
	s.writeList(w, r, items, exchangeInfo{}, q,
		func(v any) string { return v.(exchangeInfo).Name },
		sortFieldAllowlist([]string{"name", "vhost", "type", "durable", "auto_delete", "internal"}))
}

func (s *Server) hExchange(w http.ResponseWriter, r *http.Request) {
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	for _, e := range s.exchangeList() {
		if e.Name == name {
			s.writeJSON(w, r, http.StatusOK, e)
			return
		}
	}
	s.writeAPIError(w, r, errNotFoundMsg("unknown exchange"))
}

// --- queues ---

type queueSummaryInfo struct {
	Name                   string `json:"name"`
	VHost                  string `json:"vhost"`
	Type                   string `json:"type"`
	Durable                bool   `json:"durable"`
	AutoDelete             bool   `json:"auto_delete"`
	Exclusive              bool   `json:"exclusive"`
	Arguments              any    `json:"arguments"`
	State                  string `json:"state"`
	Consumers              int    `json:"consumers"`
	MessagesReady          any    `json:"messages_ready"`
	MessagesUnacknowledged any    `json:"messages_unacknowledged"`
	Messages               any    `json:"messages"`
	StatsPartial           bool   `json:"stats_partial"`
}

type queueFullInfo struct {
	queueSummaryInfo
	OwnerConnection           any `json:"owner_connection"`
	MessagesPersistent        any `json:"messages_persistent"`
	ConsumerUtilisation       any `json:"consumer_utilisation"`
	MessageStats              any `json:"message_stats"`
	IdleSince                 any `json:"idle_since"`
	Policy                    any `json:"policy"`
	EffectivePolicyDefinition any `json:"effective_policy_definition"`
}

// queueCounts computes a queue's ready/unacked/total from the BOB-1 seam, with
// the existing QueueReadyCount seam as the ready fallback. Unknown → nil.
func (s *Server) queueCounts(name string) (ready any, unacked any, total any, partial bool) {
	b := s.broker()
	if b == nil {
		return nil, nil, nil, true
	}
	if qt, ok := b.(queueMessageTotals); ok {
		c := qt.QueueMessageCounts(name)
		if c.Known {
			return c.Ready, c.Unacked, c.Total, false
		}
	}
	if rc, ok := b.(queueReadyCounter); ok {
		return int64(rc.QueueReadyCount(name)), nil, nil, true
	}
	return nil, nil, nil, true
}

func queueState(consumers int, messages any) string {
	hasMsgs := false
	if n, ok := messages.(int64); ok && n > 0 {
		hasMsgs = true
	}
	switch {
	case consumers > 0 || hasMsgs:
		return "running"
	case messages == nil:
		return "unknown"
	default:
		return "idle"
	}
}

func (s *Server) queueSummaries() []queueSummaryInfo {
	b := s.broker()
	if b == nil {
		return nil
	}
	consumerCounts := make(map[string]int)
	if views, ok := b.(consumerViews); ok {
		for _, v := range views.ConsumerViews() {
			consumerCounts[v.Queue]++
		}
	}
	queues := b.GetQueues()
	out := make([]queueSummaryInfo, 0, len(queues))
	for _, q := range queues {
		ready, unacked, total, partial := s.queueCounts(q.Name)
		qi := queueSummaryInfo{
			Name:                   q.Name,
			VHost:                  "/",
			Type:                   "classic",
			Durable:                q.Durable,
			AutoDelete:             q.AutoDelete,
			Exclusive:              q.Exclusive,
			State:                  queueState(consumerCounts[q.Name], total),
			Consumers:              consumerCounts[q.Name],
			MessagesReady:          ready,
			MessagesUnacknowledged: unacked,
			Messages:               total,
			StatsPartial:           partial,
		}
		if q.Arguments != nil {
			qi.Arguments = fieldTableJSON(q.Arguments)
		}
		out = append(out, qi)
	}
	return out
}

func (s *Server) queueFulls(summaries []queueSummaryInfo) []queueFullInfo {
	out := make([]queueFullInfo, 0, len(summaries))
	for _, qs := range summaries {
		out = append(out, queueFullInfo{
			queueSummaryInfo: qs,
			// messages_persistent has no source (DATA-002) and stays null.
		})
	}
	return out
}

// queueRepresentation renders one queue exactly as the GET endpoint does so an
// If-Match validator matches the ETag a client fetched. ok is false when the
// queue does not exist.
func (s *Server) queueRepresentation(name string) (queueFullInfo, bool) {
	for _, qs := range s.queueSummaries() {
		if qs.Name == name {
			return s.queueFulls([]queueSummaryInfo{qs})[0], true
		}
	}
	return queueFullInfo{}, false
}

type queueListQuery struct {
	*listQuery
	disableStats      bool
	enableQueueTotals bool
	durableFilter     *bool
	hasConsumers      bool
	nonEmpty          bool
	filtersSet        bool
}

func (s *Server) parseQueueQuery(r *http.Request) (*queueListQuery, *apiError) {
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		return nil, aerr
	}
	qq := &queueListQuery{listQuery: q}
	vals := r.URL.Query()
	if raw, ok := firstPresent(vals, "disable_stats"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("disable_stats must be true or false")
		}
		qq.disableStats = b
	}
	if raw, ok := firstPresent(vals, "enable_queue_totals"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("enable_queue_totals must be true or false")
		}
		if b && !qq.disableStats {
			return nil, errBad("enable_queue_totals requires disable_stats=true")
		}
		qq.enableQueueTotals = b
	}
	if raw, ok := firstPresent(vals, "durable"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("durable must be true or false")
		}
		qq.durableFilter = &b
		qq.filtersSet = true
	}
	if raw, ok := firstPresent(vals, "has_consumers"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("has_consumers must be true or false")
		}
		qq.hasConsumers = b
		qq.filtersSet = true
	}
	if raw, ok := firstPresent(vals, "non_empty"); ok {
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errBad("non_empty must be true or false")
		}
		qq.nonEmpty = b
		qq.filtersSet = true
	}
	return qq, nil
}

var queueSortFields = []string{"name", "vhost", "type", "durable", "auto_delete", "exclusive", "state", "consumers", "messages_ready", "messages_unacknowledged", "messages"}

func (s *Server) hQueues(w http.ResponseWriter, r *http.Request) {
	s.serveQueueList(w, r, false)
}

func (s *Server) hQueuesDetailed(w http.ResponseWriter, r *http.Request) {
	s.serveQueueList(w, r, true)
}

func (s *Server) hQueuesForVHost(w http.ResponseWriter, r *http.Request) {
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	s.serveQueueList(w, r, true)
}

func (s *Server) serveQueueList(w http.ResponseWriter, r *http.Request, full bool) {
	qq, aerr := s.parseQueueQuery(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	summaries := s.queueSummaries()
	if qq.filtersSet {
		filtered := make([]queueSummaryInfo, 0, len(summaries))
		for _, q := range summaries {
			if qq.durableFilter != nil && q.Durable != *qq.durableFilter {
				continue
			}
			if qq.hasConsumers && q.Consumers == 0 {
				continue
			}
			if qq.nonEmpty {
				if n, ok := q.Messages.(int64); !ok || n == 0 {
					continue
				}
			}
			filtered = append(filtered, q)
		}
		summaries = filtered
	}

	if full && !qq.disableStats {
		fulls := s.queueFulls(summaries)
		items := make([]any, 0, len(fulls))
		for _, q := range fulls {
			items = append(items, q)
		}
		s.writeListWithTotals(w, r, items, queueFullInfo{}, qq.listQuery, func(v any) string { return v.(queueFullInfo).Name }, sortFieldAllowlist(queueSortFields), qq.enableQueueTotals)
		return
	}
	items := make([]any, 0, len(summaries))
	for _, q := range summaries {
		items = append(items, q)
	}
	s.writeListWithTotals(w, r, items, queueSummaryInfo{}, qq.listQuery, func(v any) string { return v.(queueSummaryInfo).Name }, sortFieldAllowlist(queueSortFields), qq.enableQueueTotals)
}

func (s *Server) hQueue(w http.ResponseWriter, r *http.Request) {
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	for _, q := range s.queueSummaries() {
		if q.Name == name {
			s.writeJSON(w, r, http.StatusOK, s.queueFulls([]queueSummaryInfo{q})[0])
			return
		}
	}
	s.writeAPIError(w, r, errNotFoundMsg("unknown queue"))
}

// --- vhosts ---

type vhostInfo struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Synthetic   bool     `json:"synthetic"`
}

var singleVHost = vhostInfo{Name: "/", Description: "", Tags: []string{}, Synthetic: true}

func (s *Server) hVHosts(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, []vhostInfo{singleVHost})
}

func (s *Server) hVHost(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	if name != "/" {
		s.writeAPIError(w, r, errNotFoundMsg("unknown vhost"))
		return
	}
	s.writeJSON(w, r, http.StatusOK, singleVHost)
}

// hVHostDelete: "/" is 409 (the broker's only vhost cannot be deleted); any
// other name is 501 (multi-vhost is not implemented).
func (s *Server) hVHostDelete(w http.ResponseWriter, r *http.Request) {
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	if name == "/" {
		s.writeAPIError(w, r, apiErr(http.StatusConflict, errConflict,
			"the default virtual host \"/\" cannot be deleted"))
		return
	}
	s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
		"virtual-host deletion is not implemented by StrangeQ (only \"/\" exists)"))
}

func (s *Server) hVHostConnections(w http.ResponseWriter, r *http.Request) {
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	q, aerr := parseListQuery(r, s.mcfg.Limits.MaxPageSize)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	conns := s.connectionsSnapshot()
	items := make([]any, 0, len(conns))
	for _, c := range conns {
		items = append(items, c)
	}
	s.writeList(w, r, items, connectionInfo{}, q,
		func(v any) string { return v.(connectionInfo).Name },
		sortFieldAllowlist([]string{"name", "user", "state"}))
}

// --- users / permissions ---

type userInfo struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func (s *Server) hUsers(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	users := s.users.list()
	out := make([]userInfo, 0, len(users))
	for _, u := range users {
		tags := u.Tags
		if tags == nil {
			tags = []string{}
		}
		out = append(out, userInfo{Name: u.Name, Tags: tags})
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) hUser(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	name := pathValue(endpointLabel(r), r.URL.EscapedPath(), "name")
	u, ok := s.users.get(name)
	if !ok {
		s.writeAPIError(w, r, errNotFoundMsg("unknown user"))
		return
	}
	tags := u.Tags
	if tags == nil {
		tags = []string{}
	}
	s.writeJSON(w, r, http.StatusOK, userInfo{Name: u.Name, Tags: tags})
}

type permissionInfo struct {
	User      string `json:"user"`
	VHost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

func permissionInfos(recs []permissionRecord) []permissionInfo {
	out := make([]permissionInfo, 0, len(recs))
	for _, p := range recs {
		out = append(out, permissionInfo{User: p.User, VHost: p.VHost, Configure: p.Configure, Write: p.Write, Read: p.Read})
	}
	return out
}

func (s *Server) hPermissions(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	s.writeJSON(w, r, http.StatusOK, permissionInfos(s.users.permissions()))
}

func (s *Server) hPermission(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	vhost, aerr := vhostParam(r)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	user := pathValue(endpointLabel(r), r.URL.EscapedPath(), "user")
	p, ok := s.users.permission(vhost, user)
	if !ok {
		s.writeAPIError(w, r, errNotFoundMsg("unknown permission"))
		return
	}
	s.writeJSON(w, r, http.StatusOK, permissionInfos([]permissionRecord{p})[0])
}

func (s *Server) hUserPermissions(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	user := pathValue(endpointLabel(r), r.URL.EscapedPath(), "user")
	s.writeJSON(w, r, http.StatusOK, permissionInfos(s.users.permissionsFor(user)))
}

func (s *Server) hVHostPermissions(w http.ResponseWriter, r *http.Request) {
	if aerr := s.requireCapability(principalFrom(r), capManageUsers); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	if _, aerr := vhostParam(r); aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	all := s.users.permissions()
	filtered := make([]permissionRecord, 0, len(all))
	for _, p := range all {
		if p.VHost == "/" {
			filtered = append(filtered, p)
		}
	}
	s.writeJSON(w, r, http.StatusOK, permissionInfos(filtered))
}

// sortFieldAllowlist returns a validator that accepts only the listed fields.
func sortFieldAllowlist(fields []string) func(string) bool {
	set := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		set[f] = struct{}{}
	}
	return func(f string) bool {
		_, ok := set[f]
		return ok
	}
}
