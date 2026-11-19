package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	exchangePrefix          byte = 0xff
	exchangeDeclare              = "declare"
	exchangeDescribe             = "describe"
	exchangeDelete               = "delete"
	exchangeBind                 = "bind"
	exchangeUnbind               = "unbind"
	exchangePrune                = "prune"
	exchangeList                 = "list"
	maxExchangeSnapshot          = 8 << 20
	exchangeSnapshotFraming      = 2
	maxClusterExchanges          = 2048
	maxExchangeBindings          = 4096
)

type clusterTarget struct {
	Queue       string
	Incarnation clusterqueue.QueueID
}
type clusterBinding struct {
	Target    clusterTarget
	Key       string
	Arguments []byte
}
type clusterExchange struct {
	Name, Type        string
	Durable, Internal bool
	Arguments         []byte
	Bindings          []clusterBinding
}
type clusterExchangeCommand struct {
	Kind, Name string
	Exchange   clusterExchange
	Binding    clusterBinding
	IfUnused   bool
	Prune      []clusterTarget
}
type clusterExchangeReply struct {
	Code      string
	Exchange  clusterExchange
	Exchanges []clusterExchange
}

func (r clusterExchangeReply) err(name string) error {
	switch r.Code {
	case "":
		return nil
	case "missing":
		return broker.ExchangeNotFoundError(name)
	case "inequivalent":
		return fmt.Errorf("%w: exchange %q", broker.ErrInequivalentArgs, name)
	case "binding":
		return fmt.Errorf("%w: exchange %q", broker.ErrInvalidBindingArgument, name)
	case "in-use":
		return fmt.Errorf("%w: exchange %q", broker.ErrExchangeInUse, name)
	case "limit":
		return broker.ErrExchangeLimitReached
	default:
		return errors.New("invalid cluster exchange operation")
	}
}

// ClusterMachine holds queue and source-exchange state for one data shard.
type ClusterMachine struct {
	queues        clusterqueue.Machine
	exchanges     map[string]clusterExchange
	exchangeBytes int
}

func NewClusterMachine() *ClusterMachine { return &ClusterMachine{} }

// NewClusterHotPayloadMachine uses the node's shared, bounded delivery cache.
func NewClusterHotPayloadMachine(cache *clusterqueue.HotPayloadCache) *ClusterMachine {
	return &ClusterMachine{queues: clusterqueue.NewHotPayloadMachine(cache)}
}

func NewClusterAssignmentMachine() *ClusterMachine {
	return &ClusterMachine{queues: clusterqueue.NewAssignmentMachine()}
}

func NewClusterHotPayloadAssignmentMachine(cache *clusterqueue.HotPayloadCache) *ClusterMachine {
	return &ClusterMachine{queues: clusterqueue.NewHotPayloadAssignmentMachine(cache)}
}

func (m *ClusterMachine) Apply(data []byte) []byte {
	result, _ := m.ApplyCommitted(data, true)
	return result
}

func (m *ClusterMachine) ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error) {
	if len(data) == 0 || data[0] != exchangePrefix {
		return m.queues.ApplyJournaled(data, wantReply, source)
	}
	return m.ApplyCommitted(data, wantReply)
}

// ApplyJournaledReply can defer queue delivery encoding after applying the
// command. Exchange results use the inline response.
func (m *ClusterMachine) ApplyJournaledReply(data []byte, wantReply bool, source io.ReaderAt) (inline []byte, encode func() ([]byte, error), err error) {
	if len(data) == 0 || data[0] != exchangePrefix {
		return m.queues.ApplyJournaledReply(data, wantReply, source)
	}
	inline, err = m.ApplyCommitted(data, wantReply)
	return inline, nil, err
}

func (m *ClusterMachine) ApplyCommitted(data []byte, wantReply bool) ([]byte, error) {
	if len(data) == 0 || data[0] != exchangePrefix {
		return m.queues.ApplyCommitted(data, wantReply)
	}
	var request clusterExchangeCommand
	var response clusterExchangeReply
	if err := json.Unmarshal(data[1:], &request); err != nil {
		response.Code = "invalid"
	} else {
		if !wantReply && (request.Kind == exchangeDescribe || request.Kind == exchangeList) {
			return nil, nil
		}
		response = m.applyExchange(request)
	}
	if !wantReply {
		return nil, nil
	}
	return json.Marshal(response)
}

// Query accepts exchange descriptions. The caller establishes a quorum fence.
func (m *ClusterMachine) Query(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != exchangePrefix {
		return nil, errors.New("invalid cluster exchange query")
	}
	var request clusterExchangeCommand
	if err := json.Unmarshal(data[1:], &request); err != nil {
		return nil, err
	}
	if request.Kind != exchangeDescribe {
		return nil, errors.New("unsupported cluster exchange query")
	}
	return json.Marshal(m.describeExchange(request.Name))
}

func knownClusterExchange(name string) (clusterExchange, bool) {
	switch name {
	case "amq.direct", "amq.fanout", "amq.topic", "amq.headers":
		return clusterExchange{Name: name, Type: strings.TrimPrefix(name, "amq."), Durable: true}, true
	default:
		return clusterExchange{}, false
	}
}

func (m *ClusterMachine) describeExchange(name string) clusterExchangeReply {
	if len(name) == 0 || len(name) > 255 {
		return clusterExchangeReply{Code: "invalid"}
	}
	exchange, exists := m.exchanges[name]
	if !exists {
		exchange, exists = knownClusterExchange(name)
	}
	if !exists {
		return clusterExchangeReply{Code: "missing"}
	}
	return clusterExchangeReply{Exchange: exchange}
}

func (m *ClusterMachine) applyExchange(request clusterExchangeCommand) clusterExchangeReply {
	if request.Kind == exchangeDescribe {
		// Preserve replay of describe entries from existing shard logs.
		return m.describeExchange(request.Name)
	}
	if request.Kind == exchangeList {
		exchanges := make([]clusterExchange, 0, len(m.exchanges))
		for _, exchange := range m.exchanges {
			exchanges = append(exchanges, exchange)
		}
		slices.SortFunc(exchanges, func(a, b clusterExchange) int { return strings.Compare(a.Name, b.Name) })
		return clusterExchangeReply{Exchanges: exchanges}
	}
	if len(request.Name) == 0 || len(request.Name) > 255 {
		return clusterExchangeReply{Code: "invalid"}
	}
	old, exists := m.exchanges[request.Name]
	prior := old
	builtin, builtinOK := knownClusterExchange(request.Name)
	if !exists && builtinOK {
		old = builtin
	}
	if !exists && !builtinOK && request.Kind != exchangeDeclare {
		return clusterExchangeReply{Code: "missing"}
	}
	switch request.Kind {
	case exchangeDeclare:
		want := request.Exchange
		if !isSupportedExchangeType(want.Type) || want.Name != request.Name || len(want.Arguments) > 64<<10 {
			return clusterExchangeReply{Code: "invalid"}
		}
		if exists || builtinOK {
			if want.Type != old.Type || want.Durable != old.Durable || want.Internal != old.Internal || !bytes.Equal(want.Arguments, old.Arguments) {
				return clusterExchangeReply{Code: "inequivalent"}
			}
			return clusterExchangeReply{}
		}
		if len(m.exchanges) >= maxClusterExchanges {
			return clusterExchangeReply{Code: "limit"}
		}
		want.Bindings = nil
		old = want
	case exchangeDelete:
		if builtinOK {
			return clusterExchangeReply{Code: "binding"}
		}
		if request.IfUnused && len(old.Bindings) > 0 {
			return clusterExchangeReply{Code: "in-use"}
		}
		m.exchangeBytes -= exchangeRecordSize(old)
		delete(m.exchanges, request.Name)
		return clusterExchangeReply{}
	case exchangeBind:
		binding := request.Binding
		if binding.Target.Queue == "" || len(binding.Target.Queue) > 255 || binding.Target.Incarnation == 0 || len(binding.Key) > 255 || len(binding.Arguments) > 64<<10 {
			return clusterExchangeReply{Code: "binding"}
		}
		for _, existing := range old.Bindings {
			if existing.Target == binding.Target && existing.Key == binding.Key && bytes.Equal(existing.Arguments, binding.Arguments) {
				return clusterExchangeReply{}
			}
		}
		old.Bindings = slices.DeleteFunc(slices.Clone(old.Bindings), func(existing clusterBinding) bool {
			return existing.Target.Queue == binding.Target.Queue && existing.Target.Incarnation < binding.Target.Incarnation
		})
		if len(old.Bindings) >= maxExchangeBindings {
			return clusterExchangeReply{Code: "limit"}
		}
		old.Bindings = append(old.Bindings, binding)
	case exchangeUnbind:
		binding := request.Binding
		old.Bindings = slices.DeleteFunc(slices.Clone(old.Bindings), func(existing clusterBinding) bool {
			return existing.Target == binding.Target && existing.Key == binding.Key && bytes.Equal(existing.Arguments, binding.Arguments)
		})
	case exchangePrune:
		old.Bindings = slices.DeleteFunc(slices.Clone(old.Bindings), func(binding clusterBinding) bool {
			return slices.Contains(request.Prune, binding.Target)
		})
	default:
		return clusterExchangeReply{Code: "invalid"}
	}
	used := m.exchangeBytes + exchangeRecordSize(old) - exchangeRecordSize(prior)
	if used > maxExchangeSnapshot-exchangeSnapshotFraming || prior.Name == "" && len(m.exchanges) >= maxClusterExchanges {
		return clusterExchangeReply{Code: "limit"}
	}
	if m.exchanges == nil {
		m.exchanges = make(map[string]clusterExchange)
	}
	m.exchanges[request.Name] = old
	m.exchangeBytes = used
	return clusterExchangeReply{}
}

// An exchange appears both as a map key and as a record in the snapshot.
// Include the colon and reserve a comma for every record.
func exchangeRecordSize(ex clusterExchange) int {
	if ex.Name == "" {
		return 0
	}
	key, _ := json.Marshal(ex.Name)
	record, _ := json.Marshal(ex)
	return len(key) + 1 + len(record) + 1
}

// CaptureSnapshot freezes shard state for one encoder. The caller serializes
// capture with machine changes and keeps journal sources open through encoding.
func (m *ClusterMachine) CaptureSnapshot() func([]byte) ([]byte, error) {
	captured := *m
	captured.queues = m.queues.CloneForSnapshot()
	// Binding updates replace their slices by copy, and arguments are immutable.
	captured.exchanges = maps.Clone(m.exchanges)
	return captured.AppendSnapshot
}

func (m *ClusterMachine) AcceptsImmutableJournalCommands() bool {
	return m.queues.AcceptsImmutableJournalCommands()
}

func (m *ClusterMachine) JournalBodyCRCRanges(data []byte) []clusterproposal.BodyRange {
	return m.queues.JournalBodyCRCRanges(data)
}

func (m *ClusterMachine) ReleaseJournalSources() { m.queues.ReleaseJournalSources() }

func (m *ClusterMachine) CheckpointCostBytes() uint64 {
	return m.queues.EstimatedSnapshotBytes() + uint64(m.exchangeBytes)
}

func (m *ClusterMachine) Snapshot() ([]byte, error) {
	return m.AppendSnapshot(nil)
}

// AppendSnapshot appends a complete AMQP shard snapshot to dst.
func (m *ClusterMachine) AppendSnapshot(dst []byte) ([]byte, error) {
	exchangeSnapshot, err := json.Marshal(m.exchanges)
	if err != nil {
		return nil, err
	}
	if len(exchangeSnapshot) > maxExchangeSnapshot {
		return nil, errors.New("cluster exchange snapshot exceeds its size limit")
	}
	header := [5]byte{1}
	binary.BigEndian.PutUint32(header[1:], uint32(len(exchangeSnapshot)))
	return m.queues.AppendSnapshot(dst, header[:], exchangeSnapshot)
}

func (m *ClusterMachine) Restore(data []byte) error {
	if len(data) < 5 || data[0] != 1 {
		return errors.New("invalid AMQP shard snapshot")
	}
	length := int(binary.BigEndian.Uint32(data[1:5]))
	if length > maxExchangeSnapshot || length > len(data)-5 {
		return errors.New("invalid AMQP exchange snapshot length")
	}
	restored := ClusterMachine{queues: m.queues}
	if err := json.Unmarshal(data[5:5+length], &restored.exchanges); err != nil {
		return err
	}
	if len(restored.exchanges) > maxClusterExchanges {
		return errors.New("too many exchanges in snapshot")
	}
	for name, exchange := range restored.exchanges {
		if name != exchange.Name || name == "" || !isSupportedExchangeType(exchange.Type) || len(exchange.Bindings) > maxExchangeBindings {
			return errors.New("invalid exchange in snapshot")
		}
		restored.exchangeBytes += exchangeRecordSize(exchange)
	}
	if err := restored.queues.Restore(data[5+length:]); err != nil {
		return err
	}
	*m = restored
	return nil
}

func exchangeRequest(request clusterExchangeCommand) ([]byte, error) {
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return append([]byte{exchangePrefix}, data...), nil
}

type exchangeReader interface {
	Query(context.Context, string, []byte) ([]byte, error)
}

func (b *ClusterBroker) exchange(ctx context.Context, name string, request clusterExchangeCommand) (clusterExchangeReply, error) {
	request.Name = name
	encoded, err := exchangeRequest(request)
	if err != nil {
		return clusterExchangeReply{}, err
	}
	// A source exchange's catalog lives in a normal data shard, so different
	// named exchanges can route concurrently on different shard leaders.
	key := "\x00exchange:" + name
	var raw []byte
	if request.Kind == exchangeDescribe {
		if reader, ok := b.cluster.(exchangeReader); ok {
			raw, err = reader.Query(ctx, key, encoded)
		} else {
			raw, err = b.submitReplay(ctx, key, encoded)
		}
	} else {
		raw, err = b.cluster.Submit(ctx, key, encoded)
	}
	if err != nil {
		return clusterExchangeReply{}, err
	}
	var reply clusterExchangeReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		return reply, err
	}
	return reply, reply.err(name)
}

func (b *ClusterBroker) DeclareExchange(name, kind string, durable, autoDelete, internal bool, args map[string]interface{}) error {
	if autoDelete || args["alternate-exchange"] != nil {
		return fmt.Errorf("%w: auto-delete and alternate exchanges are unavailable in cluster mode", broker.ErrInvalidBindingArgument)
	}
	wire, err := protocol.EncodeCanonicalFieldTable(args)
	if err != nil {
		return fmt.Errorf("%w: %w", broker.ErrInvalidBindingArgument, err)
	}
	ctx, cancel := clusterContext()
	defer cancel()
	_, err = b.exchange(ctx, name, clusterExchangeCommand{Kind: exchangeDeclare, Exchange: clusterExchange{
		Name: name, Type: kind, Durable: durable, Internal: internal, Arguments: wire,
	}})
	return err
}

func (b *ClusterBroker) ExchangeDeclarePassive(name string) error {
	ctx, cancel := clusterContext()
	defer cancel()
	_, err := b.exchange(ctx, name, clusterExchangeCommand{Kind: exchangeDescribe})
	return err
}

func (b *ClusterBroker) DeleteExchange(name string, ifUnused bool) error {
	ctx, cancel := clusterContext()
	defer cancel()
	request := clusterExchangeCommand{Kind: exchangeDelete, IfUnused: ifUnused}
	_, err := b.exchange(ctx, name, request)
	if errors.Is(err, broker.ErrExchangeInUse) {
		if err := b.pruneObsoleteBindings(ctx, name); err != nil {
			return err
		}
		_, err = b.exchange(ctx, name, request)
	}
	if errors.Is(err, interfaces.ErrExchangeNotFound) {
		return nil
	}
	return err
}

func (b *ClusterBroker) pruneObsoleteBindings(ctx context.Context, name string) error {
	reply, err := b.exchange(ctx, name, clusterExchangeCommand{Kind: exchangeDescribe})
	if errors.Is(err, interfaces.ErrExchangeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var stale []clusterTarget
	seen := make(map[clusterTarget]struct{})
	for _, binding := range reply.Exchange.Bindings {
		target := binding.Target
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		info, err := b.describe(ctx, target.Queue)
		switch {
		case errors.Is(err, clusterqueue.ErrQueueNotFound):
		case err != nil:
			return err
		case info.Incarnation == target.Incarnation:
			return fmt.Errorf("%w: exchange %q", broker.ErrExchangeInUse, name)
		}
		stale = append(stale, target)
	}
	if len(stale) == 0 {
		return nil
	}
	_, err = b.exchange(ctx, name, clusterExchangeCommand{Kind: exchangePrune, Prune: stale})
	if errors.Is(err, interfaces.ErrExchangeNotFound) {
		return nil
	}
	return err
}

func (b *ClusterBroker) BindQueue(queue, exchange, key string, args map[string]interface{}) error {
	ctx, cancel := clusterContext()
	defer cancel()
	info, err := b.describe(ctx, queue)
	if err != nil {
		return clusterQueueError(queue, err)
	}
	ex, err := b.exchange(ctx, exchange, clusterExchangeCommand{Kind: exchangeDescribe})
	if err != nil {
		return err
	}
	if ex.Exchange.Type == "headers" {
		if err := broker.ValidateHeadersBinding(args); err != nil {
			return err
		}
	}
	wire, err := protocol.EncodeCanonicalFieldTable(args)
	if err != nil {
		return fmt.Errorf("%w: %w", broker.ErrInvalidBindingArgument, err)
	}
	_, err = b.exchange(ctx, exchange, clusterExchangeCommand{Kind: exchangeBind, Binding: clusterBinding{
		Target: clusterTarget{Queue: queue, Incarnation: info.Incarnation}, Key: key, Arguments: wire,
	}})
	return err
}

func (b *ClusterBroker) UnbindQueue(queue, exchange, key string) error {
	return b.UnbindQueueWithArguments(queue, exchange, key, nil)
}
func (b *ClusterBroker) UnbindQueueWithArguments(queue, exchange, key string, args map[string]interface{}) error {
	ctx, cancel := clusterContext()
	defer cancel()
	info, err := b.describe(ctx, queue)
	if err != nil {
		return clusterQueueError(queue, err)
	}
	wire, err := protocol.EncodeCanonicalFieldTable(args)
	if err != nil {
		return fmt.Errorf("%w: %w", broker.ErrInvalidBindingArgument, err)
	}
	_, err = b.exchange(ctx, exchange, clusterExchangeCommand{Kind: exchangeUnbind, Binding: clusterBinding{
		Target: clusterTarget{Queue: queue, Incarnation: info.Incarnation}, Key: key, Arguments: wire,
	}})
	return err
}

func (*ClusterBroker) BindExchange(string, string, string, map[string]interface{}) error {
	return fmt.Errorf("%w: exchange-to-exchange bindings are unavailable in cluster mode", broker.ErrInvalidBindingArgument)
}
func (*ClusterBroker) UnbindExchange(string, string, string) error {
	return fmt.Errorf("%w: exchange-to-exchange bindings are unavailable in cluster mode", broker.ErrInvalidBindingArgument)
}

func (b *ClusterBroker) route(ctx context.Context, name, key string, headers map[string]interface{}) ([]clusterTarget, error) {
	reply, err := b.exchange(ctx, name, clusterExchangeCommand{Kind: exchangeDescribe})
	if err != nil {
		return nil, err
	}
	return routeClusterExchange(reply.Exchange, key, headers)
}

func routeClusterExchange(ex clusterExchange, key string, headers map[string]interface{}) ([]clusterTarget, error) {
	if ex.Internal {
		return nil, broker.ErrExchangeInternal
	}
	targets := make([]clusterTarget, 0)
	seen := make(map[clusterTarget]struct{})
	for _, binding := range ex.Bindings {
		var match bool
		switch ex.Type {
		case "direct":
			match = binding.Key == key
		case "fanout":
			match = true
		case "topic":
			match = broker.MatchTopicPattern(binding.Key, key)
		case "headers":
			args, err := decodeClusterArguments(binding.Arguments)
			if err != nil {
				return nil, err
			}
			match = broker.MatchHeaders(args, headers)
		}
		if _, ok := seen[binding.Target]; match && !ok {
			seen[binding.Target] = struct{}{}
			targets = append(targets, binding.Target)
		}
	}
	return targets, nil
}

func (b *ClusterBroker) GetExchanges() map[string]*protocol.Exchange {
	result := make(map[string]*protocol.Exchange)
	result[""] = &protocol.Exchange{Kind: "direct", Durable: true}
	for _, kind := range []string{"direct", "fanout", "topic", "headers"} {
		name := "amq." + kind
		result[name] = &protocol.Exchange{Name: name, Kind: kind, Durable: true}
	}
	query, ok := b.cluster.(clusterShardReader)
	if !ok {
		return result
	}
	ctx, cancel := clusterContext()
	defer cancel()
	data, _ := exchangeRequest(clusterExchangeCommand{Kind: exchangeList})
	replies, err := query.SubmitEachShard(ctx, data)
	if err != nil {
		return result
	}
	for _, raw := range replies {
		var reply clusterExchangeReply
		if err := json.Unmarshal(raw, &reply); err != nil || reply.Code != "" {
			continue
		}
		for _, ex := range reply.Exchanges {
			args, err := decodeClusterArguments(ex.Arguments)
			if err == nil {
				result[ex.Name] = &protocol.Exchange{Name: ex.Name, Kind: ex.Type, Durable: ex.Durable, Internal: ex.Internal, Arguments: args}
			}
		}
	}
	return result
}
