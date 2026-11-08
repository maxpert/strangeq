package management

import (
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/server"
)

// --- optional broker/server accessor discovery (spec mgmt-03 §7) ---
//
// The management package holds *server.Server and discovers these through
// structural interfaces. It never imports broker internals; when a seam is
// absent the corresponding values are reported `null` with stats_partial.

type entitySnapshotter interface {
	SnapshotEntityTotals() server.EntityTotals
}

type entityObserverWired interface {
	EntityObserverWired() bool
}

type channelViews interface {
	ChannelViews() []server.ChannelView
}

type queueMessageTotals interface {
	QueueMessageCounts(name string) broker.QueueMessageCounts
}

type queueReadyCounter interface {
	QueueReadyCount(name string) uint32
}

type consumerViews interface {
	ConsumerViews() []broker.ConsumerView
}

// bindingLister is the optional aggregate-binding seam (BOB-4). Production's
// *server.StorageBrokerAdapter always implements it; a custom UnifiedBroker may
// not, in which case every binding endpoint returns 501 (the pre-BOB-4
// contract).
type bindingLister interface {
	ListBindings() ([]broker.BindingView, error)
}

func (s *Server) broker() server.UnifiedBroker {
	if s.opts.Server == nil {
		return nil
	}
	return s.opts.Server.Broker
}

// entityTotals returns the five entity counts and whether they are coherent.
// Coherent requires both the BOB-11 snapshot seam AND the production observer
// being wired; otherwise it falls back to independent reads (FR-API-002) with
// snapshot_coherent:false.
func (s *Server) entityTotals() (server.EntityTotals, bool) {
	var t server.EntityTotals
	if s.opts.Server != nil {
		if snap, ok := any(s.opts.Server).(entitySnapshotter); ok {
			t = snap.SnapshotEntityTotals()
			if w, ok := any(s.opts.Server).(entityObserverWired); ok {
				return t, w.EntityObserverWired()
			}
			return t, false
		}
	}
	// Fallback: independent reads. Server.Mutex.RLock and the metadata-store
	// read lock inside GetQueues/GetExchanges are permitted; Lifecycle.GetStats
	// (which takes the forbidden Channel.Mutex) is never called. The wait for
	// the permitted Server.Mutex read lock is observed (OPS-003/NFR-011); the
	// seam path above is lock-free and takes no such lock.
	if srv := s.opts.Server; srv != nil {
		lockStart := time.Now()
		srv.Mutex.RLock()
		s.sink.ManagementSnapshotLockWait(time.Since(lockStart).Seconds())
		t.Connections = int64(len(srv.Connections))
		for _, c := range srv.Connections {
			c.Channels.Range(func(_, _ any) bool {
				t.Channels++
				return true
			})
		}
		srv.Mutex.RUnlock()
		if b := srv.Broker; b != nil {
			t.Consumers = int64(len(b.GetConsumers()))
			t.Queues = int64(len(b.GetQueues()))
			t.Exchanges = int64(len(b.GetExchanges()))
		}
	}
	return t, false
}

// queueTotalsResult carries the overview queue aggregates plus whether every
// value was known.
type queueTotalsResult struct {
	messagesReady   any
	messagesUnacked any
	messages        any
	queuesWithMsgs  any
}

// queueTotals performs ONE pass over the broker's queues calling the BOB-1
// seam. When the seam is absent every aggregate is null and stats_partial is
// signalled (the pre-BOB-1 contract).
func (s *Server) queueTotals() (queueTotalsResult, bool) {
	var out queueTotalsResult
	b := s.broker()
	if b == nil {
		return out, false
	}
	qt, ok := b.(queueMessageTotals)
	// Even without the BOB-1 totals seam we can still report a ready depth if
	// the QueueReadyCount seam exists.
	rc, hasRC := b.(queueReadyCounter)
	if !ok && !hasRC {
		return out, false
	}
	var ready, unacked int64
	withMsgs := int64(0)
	for name := range b.GetQueues() {
		if hasRC {
			ready += int64(rc.QueueReadyCount(name))
		}
		if ok {
			c := qt.QueueMessageCounts(name)
			if c.Known {
				if c.Total > 0 {
					withMsgs++
				}
				if !hasRC {
					ready += c.Ready
				}
				unacked += c.Unacked
			}
		}
	}
	if hasRC {
		out.messagesReady = ready
	}
	if ok {
		out.messagesUnacked = unacked
		out.messages = ready + unacked
		out.queuesWithMsgs = withMsgs
	}
	return out, true
}

type listenerInfo struct {
	Protocol string `json:"protocol"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	TLS      bool   `json:"tls"`
}

type objectTotals struct {
	Connections int64 `json:"connections"`
	Channels    int64 `json:"channels"`
	Queues      int64 `json:"queues"`
	Consumers   int64 `json:"consumers"`
	Exchanges   int64 `json:"exchanges"`
	Bindings    any   `json:"bindings"`
}

type queueTotalsJSON struct {
	MessagesReady      any `json:"messages_ready"`
	MessagesUnacked    any `json:"messages_unacknowledged"`
	Messages           any `json:"messages"`
	QueuesWithMessages any `json:"queues_with_messages"`
}

type alarmsJSON struct {
	Memory bool   `json:"memory"`
	Disk   bool   `json:"disk"`
	Reason string `json:"reason"`
}

type unsafeRecoveryJSON struct {
	Degraded           bool `json:"degraded"`
	DiscardedArtifacts int  `json:"discarded_artifacts"`
}

type overviewResponse struct {
	ManagementVersion string             `json:"management_version"`
	Product           string             `json:"product"`
	ProductVersion    string             `json:"product_version"`
	AMQPGoVersion     string             `json:"amqp_go_version"`
	Platform          string             `json:"platform"`
	ErlangVersion     any                `json:"erlang_version"`
	Node              string             `json:"node"`
	UptimeSeconds     int64              `json:"uptime_seconds"`
	Listeners         []listenerInfo     `json:"listeners"`
	SnapshotCoherent  bool               `json:"snapshot_coherent"`
	ObjectTotals      objectTotals       `json:"object_totals"`
	QueueTotals       queueTotalsJSON    `json:"queue_totals"`
	Alarms            alarmsJSON         `json:"alarms"`
	UnsafeRecovery    unsafeRecoveryJSON `json:"unsafe_recovery"`
}

func (s *Server) hOverview(w http.ResponseWriter, r *http.Request) {
	// Time the snapshot (the coherent entity read plus the queue aggregate
	// pass) and publish it on the bounded route template (OPS-003).
	snapshotStart := time.Now()
	totals, coherent := s.entityTotals()
	if !coherent {
		s.sink.ManagementSnapshotPartial("/api/overview")
	}
	qt, _ := s.queueTotals()
	s.sink.ManagementSnapshotDuration("/api/overview", time.Since(snapshotStart).Seconds())

	alarmState := uint32(0)
	degraded := 0
	if s.opts.Server != nil {
		alarmState = s.opts.Server.AlarmState()
		degraded = len(s.opts.Server.UnsafeRecoveryFaults())
	}
	mem := alarmState&server.AlarmMemory != 0
	disk := alarmState&server.AlarmDisk != 0
	reason := ""
	switch {
	case mem && disk:
		reason = "memory and disk resource alarms active"
	case mem:
		reason = "memory resource alarm active"
	case disk:
		reason = "disk resource alarm active"
	}

	resp := overviewResponse{
		ManagementVersion: managementAPIVersion,
		Product:           "StrangeQ",
		ProductVersion:    s.version(),
		AMQPGoVersion:     s.version(),
		Platform:          "Go",
		ErlangVersion:     nil,
		Node:              s.nodeName,
		UptimeSeconds:     s.uptimeSeconds(),
		Listeners:         s.listeners(),
		SnapshotCoherent:  coherent,
		ObjectTotals: objectTotals{
			Connections: totals.Connections,
			Channels:    totals.Channels,
			Queues:      totals.Queues,
			Consumers:   totals.Consumers,
			Exchanges:   totals.Exchanges,
			Bindings:    totals.Bindings,
		},
		QueueTotals: queueTotalsJSON{
			MessagesReady:      qt.messagesReady,
			MessagesUnacked:    qt.messagesUnacked,
			Messages:           qt.messages,
			QueuesWithMessages: qt.queuesWithMsgs,
		},
		Alarms:         alarmsJSON{Memory: mem, Disk: disk, Reason: reason},
		UnsafeRecovery: unsafeRecoveryJSON{Degraded: degraded > 0, DiscardedArtifacts: degraded},
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

func (s *Server) listeners() []listenerInfo {
	cfg := s.cfg
	if cfg == nil {
		return nil
	}
	host, portStr, err := net.SplitHostPort(cfg.Network.Address)
	if err != nil {
		host = cfg.Network.Address
		portStr = strconv.Itoa(cfg.Network.Port)
	}
	port, _ := strconv.Atoi(portStr)
	return []listenerInfo{{
		Protocol: "amqp",
		IP:       host,
		Port:     port,
		TLS:      cfg.Security.TLSEnabled,
	}}
}

type capabilitiesJSON struct {
	QueueMessageTotals bool `json:"queue_message_totals"`
	Channels           bool `json:"channels"`
	Bindings           bool `json:"bindings"`
	CloseConnection    bool `json:"close_connection"`
	UserCRUD           bool `json:"user_crud"`
	TagCapabilities    bool `json:"tag_capabilities"`
	ConsumerCounts     bool `json:"consumer_counts"`
	AtomicImport       bool `json:"atomic_import"`
	MessageRates       bool `json:"message_rates"`
	VHostScoping       bool `json:"vhost_scoping"`
	CoherentSnapshot   bool `json:"coherent_snapshot"`
	Unauthenticated    bool `json:"unauthenticated"`
}

type versionResponse struct {
	Product          string           `json:"product"`
	Version          string           `json:"version"`
	Commit           string           `json:"commit"`
	BuildDate        string           `json:"build_date"`
	GoVersion        string           `json:"go_version"`
	APICompatibility string           `json:"api_compatibility"`
	Capabilities     capabilitiesJSON `json:"capabilities"`
	Endpoints        []string         `json:"endpoints"`
}

// capabilitiesReport reflects exactly which optional seams are present. It is
// how a client learns which features are honest-empty (FR-API-006).
func (s *Server) capabilitiesReport() capabilitiesJSON {
	b := s.broker()
	_, queueTotalsOK := b.(queueMessageTotals)
	_, consumerOK := b.(consumerViews)
	_, bindingsOK := b.(bindingLister)
	_, channelOK := any(s.opts.Server).(channelViews)
	_, snapOK := any(s.opts.Server).(entitySnapshotter)
	snapCoherent := false
	if w, ok := any(s.opts.Server).(entityObserverWired); ok {
		snapCoherent = w.EntityObserverWired()
	}
	return capabilitiesJSON{
		QueueMessageTotals: queueTotalsOK,
		Channels:           channelOK,
		Bindings:           bindingsOK,
		CloseConnection:    true,  // graceful close (BOB-5) with the recorded divergence
		UserCRUD:           false, // BOB-6 unbuilt
		TagCapabilities:    true,
		ConsumerCounts:     consumerOK,
		AtomicImport:       false, // BOB-10 unbuilt
		MessageRates:       false, // BOB-2 unbuilt
		VHostScoping:       false,
		CoherentSnapshot:   snapOK && snapCoherent,
		Unauthenticated:    s.mcfg.AllowUnauthenticated || s.authMode() == "none",
	}
}

func (s *Server) hVersion(w http.ResponseWriter, r *http.Request) {
	resp := versionResponse{
		Product:          "StrangeQ",
		Version:          s.version(),
		Commit:           s.opts.Commit,
		BuildDate:        s.opts.BuildDate,
		GoVersion:        s.goVersion(),
		APICompatibility: "rabbitmq-4.4-management-subset",
		Capabilities:     s.capabilitiesReport(),
		Endpoints:        s.endpoints,
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

func (s *Server) version() string {
	if s.opts.Version == "" {
		return "0.1.0-dev"
	}
	return s.opts.Version
}

func (s *Server) goVersion() string {
	if s.opts.GoVersion != "" {
		return s.opts.GoVersion
	}
	return runtime.Version()
}

// managementAPIVersion is the management-surface version advertised in the
// overview (RabbitMQ-management-compatible product generation).
const managementAPIVersion = "0.4.0"

func isVHostAccepted(vhost string) bool {
	return vhost == "/" || vhost == "%2F" || vhost == "%2f"
}

var _ = strings.TrimSpace
