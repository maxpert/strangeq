package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	MetadataDir         = "metadata"
	ExchangesDir        = "exchanges"
	QueuesDir           = "queues"
	BindingsDir         = "bindings"
	ExchangeBindingsDir = "exchange_bindings"
	ConsumersDir        = "consumers"
	FileExtension       = ".cbor"
	TempFileExtension   = ".tmp"

	// bindingComponentMaxLen is the per-component budget for the three-part
	// binding filenames. A binding name is "<A>_<B>_<C>" + FileExtension, and
	// atomicWriteFile writes through a sibling carrying TempFileExtension too,
	// so the longest path component this can produce must fit NAME_MAX (255)
	// WITH the temp suffix on it: 255 - len(".cbor") - len(".tmp") - 2
	// separators = 244, split three ways.
	//
	// The old join had no bound at all, so three legal 255-byte AMQP shortstrs
	// produced a ~772-byte name and failed ENAMETOOLONG on APFS and ext4 for
	// entirely non-adversarial input.
	bindingComponentMaxLen = (255 - len(FileExtension) - len(TempFileExtension) - 2) / 3
)

// ErrQueueRecordUnreadable is returned by StoreQueue when a metadata record
// for the queue name is PRESENT ON DISK but cannot be read or decoded.
//
// It exists because "absent" and "present but unreadable" demanded opposite
// answers and were indistinguishable: existingOrdinalLocked returned 0 for
// both, so StoreQueue treated an unreadable record as "no record" and wrote a
// FRESH composite-tag ordinal over it. Every confirmed durable record already
// on disk under the old ordinal then took recovery's Case B "dead incarnation"
// branch and was discarded with a Warn while the boot reported success. The
// write-once-ordinal invariant now fails closed instead: a declare against an
// unreadable record errors loudly rather than silently retiring the queue's
// entire tag band.
var ErrQueueRecordUnreadable = errors.New("queue metadata record exists but cannot be read; refusing to overwrite its delivery-tag ordinal")

// PersistentMetadataStore implements persistent metadata storage using CBOR binary format
// Phase 3: Simple, debuggable file-based metadata
// Phase 6D: In-memory cache for hot path optimization
// Phase 6F: Migrated from JSON to CBOR for 2-3x faster serialization
// Phase 6H: Added binding cache for routing performance
type PersistentMetadataStore struct {
	baseDir string
	mutex   sync.RWMutex

	// In-memory cache for hot path (Phase 6D)
	exchangeCache sync.Map // name -> *protocol.Exchange
	queueCache    sync.Map // name -> *protocol.Queue

	// Binding cache (Phase 6H) - critical for routing performance
	bindingCache          sync.Map // makeBindingCacheKey(triple) -> *interfaces.QueueBinding
	queueBindingsCache    sync.Map // queueName -> []*interfaces.QueueBinding
	exchangeBindingsCache sync.Map // exchangeName -> []*interfaces.QueueBinding

	// Exchange-to-exchange binding cache. Keyed by source only: the per-triple
	// map that used to sit beside this one was written and deleted but never
	// read once, so it cost a key-collision bug and bought nothing.
	exchBindingsFromCache sync.Map // source -> []*interfaces.ExchangeBinding

	cacheEnabled bool
}

// NewPersistentMetadataStore creates a new persistent metadata store
func NewPersistentMetadataStore(dataDir string) (*PersistentMetadataStore, error) {
	baseDir := filepath.Join(dataDir, MetadataDir)

	// Create directory structure
	dirs := []string{
		filepath.Join(baseDir, ExchangesDir),
		filepath.Join(baseDir, QueuesDir),
		filepath.Join(baseDir, BindingsDir),
		filepath.Join(baseDir, ExchangeBindingsDir),
		filepath.Join(baseDir, ConsumersDir),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metadata directory %s: %w", dir, err)
		}
	}

	store := &PersistentMetadataStore{
		baseDir:      baseDir,
		cacheEnabled: true, // Enable cache - investigating consumer bottleneck at high load
	}

	// Pre-populate cache on startup (if enabled)
	if store.cacheEnabled {
		store.loadCacheFromDisk()
	}

	return store, nil
}

// loadCacheFromDisk pre-populates the cache on startup
func (pm *PersistentMetadataStore) loadCacheFromDisk() {
	// Load all exchanges into cache
	// The RECORD names its owner, not the filename: a slot whose name is a
	// generated one cannot be turned back into a queue name by inspection, and
	// trying would attribute it to the wrong entity (review-4 N-1 / B-1).
	exchangePaths, _ := metadataSlotPaths(filepath.Join(pm.baseDir, ExchangesDir))
	for _, path := range exchangePaths {
		exchange, err := pm.decodeExchangeFile(path)
		if err != nil {
			continue
		}
		pm.exchangeCache.Store(exchange.Name, exchange)
	}

	// Load all queues into cache
	queuePaths, _ := metadataSlotPaths(filepath.Join(pm.baseDir, QueuesDir))
	for _, path := range queuePaths {
		queue, err := pm.decodeQueueFile(path)
		if err != nil {
			continue
		}
		pm.queueCache.Store(queue.Name, queue)
	}

	// Load all bindings into cache (Phase 6H)
	if bindings, err := pm.loadBindingsFromDisk(); err == nil {
		// Build per-queue and per-exchange indexes
		queueBindings := make(map[string][]*interfaces.QueueBinding)
		exchangeBindings := make(map[string][]*interfaces.QueueBinding)

		for _, binding := range bindings {
			// Cache individual binding
			cacheKey := makeBindingCacheKey(binding.QueueName, binding.ExchangeName, binding.RoutingKey)
			pm.bindingCache.Store(cacheKey, binding)

			// Index by queue
			queueBindings[binding.QueueName] = append(queueBindings[binding.QueueName], binding)

			// Index by exchange
			exchangeBindings[binding.ExchangeName] = append(exchangeBindings[binding.ExchangeName], binding)
		}

		// Store indexed caches
		for queueName, bindings := range queueBindings {
			pm.queueBindingsCache.Store(queueName, bindings)
		}
		for exchangeName, bindings := range exchangeBindings {
			pm.exchangeBindingsCache.Store(exchangeName, bindings)
		}

		// Load all exchange-to-exchange bindings into cache
		if exchBindings, err := pm.loadExchangeBindingsFromDisk(); err == nil {
			fromIndex := make(map[string][]*interfaces.ExchangeBinding)
			for _, eb := range exchBindings {
				fromIndex[eb.Source] = append(fromIndex[eb.Source], eb)
			}
			for source, bindings := range fromIndex {
				pm.exchBindingsFromCache.Store(source, bindings)
			}
		}
	}
}

// makeBindingCacheKey maps a binding's triple to its cache key.
//
// The bare "%s:%s:%s" join this replaced had makeBindingFilename's collision in
// a second place: ("q", "a:b", "c") and ("q:a", "b", "c") both keyed "q:a:b:c",
// so one binding's cache entry displaced another's and DeleteBinding could
// evict an entry belonging to a different triple. Length-prefixing each
// component makes the join injective without bounding or escaping anything —
// a cache key has no NAME_MAX to respect, so it needs neither.
func makeBindingCacheKey(queueName, exchangeName, routingKey string) string {
	return fmt.Sprintf("%d:%s:%d:%s:%s",
		len(queueName), queueName,
		len(exchangeName), exchangeName,
		routingKey)
}

// loadBindingsFromDisk loads all bindings from disk (helper for cache preload)
func (pm *PersistentMetadataStore) loadBindingsFromDisk() ([]*interfaces.QueueBinding, error) {
	dir := filepath.Join(pm.baseDir, BindingsDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*interfaces.QueueBinding{}, nil
		}
		return nil, fmt.Errorf("failed to read bindings directory: %w", err)
	}

	bindings := make([]*interfaces.QueueBinding, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		path := filepath.Join(dir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}

		var binding interfaces.QueueBinding
		if err := cbor.Unmarshal(data, &binding); err != nil {
			continue // Skip corrupted files
		}

		bindings = append(bindings, &binding)
	}

	return bindings, nil
}

// atomicWrite writes data to a file atomically and durably using a
// temp file + fsync + rename + directory fsync sequence.
//
// Durability (SQ-3): plain os.WriteFile + os.Rename leaves both the file's data
// and the rename itself in the page cache. On a crash or power loss before the
// OS flushes, topology (queues/exchanges/bindings) can vanish, or the rename
// can land pointing at a zero-length or partially written file on some
// filesystems. To prevent that we:
//  1. write the temp file, fsync it (data durable) and close it,
//  2. rename it into place (atomic replace),
//  3. fsync the parent directory so the rename entry is durable.
//
// This is a declare-time path (not the message hot path), so correctness is
// favored over speed. There is exactly one file fsync and one directory fsync
// per write — no more syncing than required.
func (pm *PersistentMetadataStore) atomicWrite(path string, data []byte) error {
	return atomicWriteFile(path, data, 0644)
}

// StoreExchange persists an exchange to disk and updates cache
func (pm *PersistentMetadataStore) StoreExchange(exchange *protocol.Exchange) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	data, err := cbor.Marshal(exchange)
	if err != nil {
		return fmt.Errorf("failed to marshal exchange: %w", err)
	}

	path := filepath.Join(pm.baseDir, ExchangesDir, metadataSlotName(exchange.Name))
	if metadataSlotIsEscaped(exchange.Name) {
		if cerr := pm.metadataSlotIsFree(path, exchange.Name, decodeExchangeName); cerr != nil {
			return cerr
		}
	}
	if err := pm.atomicWrite(path, data); err != nil {
		return err
	}

	// Update cache (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Store(exchange.Name, exchange)
	}

	return nil
}

// GetExchange loads an exchange from cache or disk
func (pm *PersistentMetadataStore) GetExchange(name string) (*protocol.Exchange, error) {
	// Try cache first (Phase 6D: lock-free hot path!)
	if pm.cacheEnabled {
		if cached, ok := pm.exchangeCache.Load(name); ok {
			return cached.(*protocol.Exchange), nil
		}
	}

	// Cache miss - load from disk
	return pm.loadExchangeFromDisk(name)
}

// loadExchangeFromDisk loads an exchange from disk (cache miss path)
func (pm *PersistentMetadataStore) loadExchangeFromDisk(name string) (*protocol.Exchange, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, ExchangesDir, metadataSlotName(name))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrExchangeNotFound
		}
		return nil, fmt.Errorf("failed to read exchange file: %w", err)
	}

	var exchange protocol.Exchange
	if err := cbor.Unmarshal(data, &exchange); err != nil {
		return nil, fmt.Errorf("failed to unmarshal exchange: %w", err)
	}

	// IDENTITY CHECK — see loadQueueFromDisk.
	if exchange.Name != name {
		return nil, interfaces.ErrExchangeNotFound
	}

	// Update cache on load (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Store(name, &exchange)
	}

	return &exchange, nil
}

// DeleteExchange removes an exchange file and cache entry
func (pm *PersistentMetadataStore) DeleteExchange(name string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, ExchangesDir, metadataSlotName(name))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete exchange file: %w", err)
	}

	// Remove from cache (Phase 6D)
	if pm.cacheEnabled {
		pm.exchangeCache.Delete(name)
	}

	return nil
}

// metadataSlotPaths lists every slot in a tier directory: the literal files
// directly in it, then the generated ones inside metadataEscapeDir. ONE function,
// so no scan site can forget the subdirectory (canon rule 12) — an enumeration
// that missed it would leave a record invisible at boot, which is the ordinal
// loss this whole change exists to prevent.
// It RETURNS ITS ERROR. An earlier revision of this helper swallowed the ReadDir
// failure and returned nil, which turned an unreadable queues directory into "no
// queues exist" with err == nil — every queue's write-once Ordinal invisible at
// boot, a fresh one minted over each, and recovery discarding the confirmed
// durable records already on disk. That is the exact silent-loss shape this file
// exists to close, reintroduced by the fix for it. It was caught by
// TestListQueues_FaultNamesAnAbsolutePath, which reported a SKIP rather than a
// failure; see that test for the guard added so a skip can no longer hide it.
func metadataSlotPaths(dir string) ([]string, error) {
	var out []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), FileExtension) {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	escapedDir := filepath.Join(dir, metadataEscapeDir)
	escaped, err := os.ReadDir(escapedDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No generated slots have ever been written here. Not an error.
			return out, nil
		}
		return out, err
	}
	for _, e := range escaped {
		if e.IsDir() || !strings.HasSuffix(e.Name(), FileExtension) {
			continue
		}
		out = append(out, filepath.Join(escapedDir, e.Name()))
	}
	return out, nil
}

// decodeQueueFile reads ONE queue record from an explicit path and takes the
// queue's name from the record. It is the enumeration counterpart of
// loadQueueFromDisk's identity check: a directory scan must never invent a name
// from a filename, because a generated slot name is not a queue name.
func (pm *PersistentMetadataStore) decodeQueueFile(path string) (*protocol.Queue, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var queue protocol.Queue
	if err := cbor.Unmarshal(data, &queue); err != nil {
		return nil, err
	}
	return &queue, nil
}

// decodeExchangeFile is decodeQueueFile for exchanges.
func (pm *PersistentMetadataStore) decodeExchangeFile(path string) (*protocol.Exchange, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var exchange protocol.Exchange
	if err := cbor.Unmarshal(data, &exchange); err != nil {
		return nil, err
	}
	return &exchange, nil
}

// ErrMetadataSlotConflict is returned when the file a record would occupy
// already holds a DIFFERENT record's data.
//
// It is reachable only for a name that cannot be a path element (so it needs a
// generated slot) whose generated slot happens to be the LITERAL slot of a name
// that can: queue "/" generates "%2f.cbor", which is queue "%2f"'s own file.
// Refusing is the conservative answer in both directions — the queue that owns
// its literal file is never disturbed (no behaviour any deployment relies on
// changes), and the queue asking for a generated slot is one that could not be
// persisted AT ALL before this change, so it loses nothing it had. Silently
// sharing the file would destroy one of the two records' write-once Ordinal.
var ErrMetadataSlotConflict = errors.New("metadata slot is already occupied by a different record; refusing to overwrite it")

// metadataSlotIsFree reports whether path is unoccupied or already holds a
// record whose own name is `name`. `nameOf` extracts the name from the decoded
// record. An unreadable occupant is treated as a conflict, never as free.
func (pm *PersistentMetadataStore) metadataSlotIsFree(path, name string, nameOf func([]byte) (string, error)) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("%w: %s: %v", ErrMetadataSlotConflict, path, err)
	}
	got, derr := nameOf(data)
	if derr != nil {
		return fmt.Errorf("%w: %s: %v", ErrMetadataSlotConflict, path, derr)
	}
	if got != name {
		return fmt.Errorf("%w: %s is owned by %q, not %q", ErrMetadataSlotConflict, path, got, name)
	}
	return nil
}

func decodeQueueName(data []byte) (string, error) {
	var q protocol.Queue
	if err := cbor.Unmarshal(data, &q); err != nil {
		return "", err
	}
	return q.Name, nil
}

func decodeExchangeName(data []byte) (string, error) {
	var e protocol.Exchange
	if err := cbor.Unmarshal(data, &e); err != nil {
		return "", err
	}
	return e.Name, nil
}

// ListExchanges returns all exchanges
func (pm *PersistentMetadataStore) ListExchanges() ([]*protocol.Exchange, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, ExchangesDir)
	paths, err := metadataSlotPaths(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Exchange{}, nil
		}
		return nil, fmt.Errorf("failed to read exchanges directory: %w", err)
	}

	// Content-authoritative, for the same reason as ListQueues.
	exchanges := make([]*protocol.Exchange, 0, len(paths))
	for _, path := range paths {
		exchange, err := pm.decodeExchangeFile(path)
		if err != nil {
			continue // Skip corrupted files
		}
		exchanges = append(exchanges, exchange)
	}

	return exchanges, nil
}

// StoreQueue persists a queue to disk and updates cache.
//
// INVARIANT: a queue's composite-tag ordinal (broker/tag_packing.go) is
// write-once per queue incarnation. It is assigned exactly once, at
// creation (broker.DeclareQueue / broker.resolveQueueOrdinal), and the only
// way it is ever cleared is DeleteQueue removing the record entirely — a
// redeclare of the same name after a delete finds no existing record here
// and correctly gets a fresh allocation. Enforced here, not just at the
// call sites that assign it: if a record for this queue name already
// exists and its Ordinal is non-zero, that ordinal is authoritative and the
// incoming queue's Ordinal is forced to match it, discarding whatever the
// caller passed — 0 (a caller that built its object before an ordinal was
// resolved) or any other non-zero value (a caller that raced a concurrent
// allocation and lost). This makes the entire class of "two goroutines
// both declare/persist a brand-new queue and interleave their StoreQueue
// calls" bugs impossible regardless of ordering, not just the specific
// case where the loser's write reverts Ordinal to 0: silently overwriting
// band N with a different non-zero band M would be equally catastrophic —
// a live queue whose durable WAL records don't match its persisted
// ordinal, which recovery refuses to boot from.
func (pm *PersistentMetadataStore) StoreQueue(queue *protocol.Queue) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	existingOrdinal, oerr := pm.existingOrdinalLocked(queue.Name)
	if oerr != nil {
		// FAIL CLOSED. Treating an unreadable record as "no record" would write
		// a fresh ordinal over it and retire the queue's entire delivery-tag
		// band; see ErrQueueRecordUnreadable.
		return oerr
	}
	if existingOrdinal != 0 {
		queue.Ordinal = existingOrdinal
	}

	data, err := cbor.Marshal(queue)
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	path := filepath.Join(pm.baseDir, QueuesDir, metadataSlotName(queue.Name))
	if metadataSlotIsEscaped(queue.Name) {
		if cerr := pm.metadataSlotIsFree(path, queue.Name, decodeQueueName); cerr != nil {
			return cerr
		}
	}
	if err := pm.atomicWrite(path, data); err != nil {
		return err
	}

	// Update cache (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Store(queue.Name, queue)
	}

	return nil
}

// existingOrdinalLocked returns the Ordinal already persisted for name, 0 if
// no record exists (or it has never had one assigned), or
// ErrQueueRecordUnreadable if a record IS present and cannot be read.
//
// The tri-state is the whole point. It used to return a bare uint64, and both
// error paths returned 0 — indistinguishable from "no record". StoreQueue then
// treated a present-but-unreadable record as absent and wrote a fresh ordinal
// over it, after which every confirmed durable record already on disk under
// the old ordinal took recovery's Case B "dead incarnation" branch and was
// discarded with a Warn while the boot reported success. The durability
// guarantee rode not on one flag but on (cacheEnabled) ∧ (the cache preload
// succeeded for this name) ∧ (that name is still cached) — three conditions,
// none of them written down. Only the third of those is still load-bearing and
// it is now checked rather than assumed.
//
// Callers MUST already hold pm.mutex (it reads the cache and, on a miss, the
// disk file directly rather than through GetQueue/loadQueueFromDisk, which
// would re-acquire pm.mutex and deadlock against StoreQueue's write lock —
// Go's sync.RWMutex is not reentrant). Used solely to enforce StoreQueue's
// write-once-ordinal invariant.
//
// COLD PATH. The sole caller is StoreQueue, which runs at declare, never per
// publish. Deliberately NOT changed: the cacheEnabled fast path stays exactly
// as it was. Decoupling the ordinal guard from the cache flag would mean a
// disk read next to GetQueue, which is on the publish path.
func (pm *PersistentMetadataStore) existingOrdinalLocked(name string) (uint64, error) {
	if pm.cacheEnabled {
		if cached, ok := pm.queueCache.Load(name); ok {
			return cached.(*protocol.Queue).Ordinal, nil
		}
	}

	path := filepath.Join(pm.baseDir, QueuesDir, metadataSlotName(name))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("%w: %s: %v", ErrQueueRecordUnreadable, path, err)
	}

	var existing protocol.Queue
	if err := cbor.Unmarshal(data, &existing); err != nil {
		return 0, fmt.Errorf("%w: %s: %v", ErrQueueRecordUnreadable, path, err)
	}
	return existing.Ordinal, nil
}

// GetQueue loads a queue from cache or disk
func (pm *PersistentMetadataStore) GetQueue(name string) (*protocol.Queue, error) {
	// Try cache first (Phase 6D: lock-free hot path!)
	if pm.cacheEnabled {
		if cached, ok := pm.queueCache.Load(name); ok {
			return cached.(*protocol.Queue), nil
		}
	}

	// Cache miss - load from disk
	return pm.loadQueueFromDisk(name)
}

// loadQueueFromDisk loads a queue from disk (cache miss path)
func (pm *PersistentMetadataStore) loadQueueFromDisk(name string) (*protocol.Queue, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, QueuesDir, metadataSlotName(name))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrQueueNotFound
		}
		return nil, fmt.Errorf("failed to read queue file: %w", err)
	}

	var queue protocol.Queue
	if err := cbor.Unmarshal(data, &queue); err != nil {
		return nil, fmt.Errorf("failed to unmarshal queue: %w", err)
	}

	// IDENTITY CHECK. A generated slot can, in principle, be the LITERAL slot of
	// a different name (queue "/" generates "%2f.cbor", which is also queue
	// "%2f"'s own file). The record says who it belongs to, so a mismatch is
	// "not this queue" rather than "close enough" — the alternative is returning
	// another queue's Ordinal, which is the silent-loss shape this closes.
	if queue.Name != name {
		return nil, interfaces.ErrQueueNotFound
	}

	// Update cache on load (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Store(name, &queue)
	}

	return &queue, nil
}

// DeleteQueue removes a queue file and cache entry
func (pm *PersistentMetadataStore) DeleteQueue(name string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, QueuesDir, metadataSlotName(name))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete queue file: %w", err)
	}

	// Remove from cache (Phase 6D)
	if pm.cacheEnabled {
		pm.queueCache.Delete(name)
	}

	return nil
}

// ListQueues returns all queues
func (pm *PersistentMetadataStore) ListQueues() ([]*protocol.Queue, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, QueuesDir)
	paths, err := metadataSlotPaths(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Queue{}, nil
		}
		return nil, fmt.Errorf("failed to read queues directory: %w", err)
	}

	// The record names its own queue. Deriving the name from the filename would
	// strand every record in a generated slot — and a stranded record means its
	// write-once Ordinal is invisible at boot, after which a fresh one is minted
	// and recovery discards the confirmed durable records already on disk under
	// the old one (review-4 N-1).
	queues := make([]*protocol.Queue, 0, len(paths))
	for _, path := range paths {
		queue, err := pm.decodeQueueFile(path)
		if err != nil {
			continue // Skip corrupted files
		}
		queues = append(queues, queue)
	}

	return queues, nil
}

// makeBindingFilename maps a binding's (queue, exchange, routing key) triple to
// the file that holds it.
//
// The mapping this replaced was NOT INJECTIVE, and the collision needed no
// adversarial input: it mapped each component's unsafe bytes onto "_" while
// using a bare "_" as the JOIN separator, and never escaped a "_" already
// inside a component. So ("orders_us", "east", "orders.created") and
// ("orders", "us_east", "orders.created") both named
// orders_us_east_orders.created.cbor. StoreBinding writes with no occupancy
// check, so the second declare silently overwrote the first, and ListBindings —
// which LoadAllMetadata calls at boot — then saw only the survivor. One binding
// was lost permanently at the next restart, with no error anywhere.
//
// filenameComponent makes the join unambiguous (see its own comment for the
// argument), and bounds each component so that three legal 255-byte AMQP
// shortstrs cannot exceed NAME_MAX. Names the old build spelled literally keep
// their file: a component that is literal and contains no "_" is returned
// unchanged, and those are exactly the components the old join was already
// unambiguous for.
func makeBindingFilename(queueName, exchangeName, routingKey string) string {
	return fmt.Sprintf("%s_%s_%s%s",
		filenameComponent(queueName, bindingComponentMaxLen),
		filenameComponent(exchangeName, bindingComponentMaxLen),
		filenameComponent(routingKey, bindingComponentMaxLen),
		FileExtension)
}

// legacyBindingComponents folds the bytes the pre-injective-join builds treated
// as unsafe. It is the exact set that build replaced, kept as data rather than
// prose because removeLegacySlot has to reproduce that spelling byte for byte
// to find the file it wrote.
var legacyBindingComponents = strings.NewReplacer(
	"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "<", "_", ">", "_", "|", "_")

// legacyBindingFilename spells a binding triple the way every build before the
// injective join did: unsafe bytes folded onto "_", no bound on the result, and
// no escape for a "_" already inside a component.
//
// It serves BOTH tiers because the exchange-to-exchange encoder carried a
// byte-for-byte copy of the queue-binding one, so there is one legacy spelling,
// not two. Nothing writes this spelling; it exists only so a store can find and
// remove what an older build left behind.
func legacyBindingFilename(a, b, c string) string {
	return fmt.Sprintf("%s_%s_%s%s",
		legacyBindingComponents.Replace(a),
		legacyBindingComponents.Replace(b),
		legacyBindingComponents.Replace(c),
		FileExtension)
}

// removeLegacySlot deletes the file an older build would have written for this
// triple, once the record has been rewritten under the current spelling — but
// ONLY after reading that file and confirming the record inside it names this
// triple.
//
// Without any cleanup, a re-declare after upgrade leaves TWO files decoding to
// one triple, because ListBindings does not dedupe by identity — it appends
// every file that decodes. That is not a double delivery: the routing path
// dedupes by queue name before enqueuing. It is worse. DeleteBinding removes
// the CURRENT filename first and only falls back to the identity scan on
// IsNotExist, so with both files present the legacy twin is never looked for:
// unbind reports success and ListBindings keeps returning the binding, on this
// run and every restart after it.
//
// THE VERIFICATION IS NOT BELT-AND-BRACES; WITHOUT IT THIS FUNCTION DESTROYS
// LIVE DATA. The legacy spelling of one triple can equal the CURRENT spelling
// of a DIFFERENT one, because both encoders emit "_" into the same namespace
// for different reasons: filenameComponent("") is "_" (empty is not literal and
// hex("") is empty), while the legacy fold maps eight characters onto "_". So
//
//	legacyBindingFilename("orders","amq.topic","*") == makeBindingFilename("orders","amq.topic","")
//
// and unlinking on the strength of the name alone deletes the live
// ("orders","amq.topic","") binding when ("orders","amq.topic","*") is
// declared. Fresh install, no upgrade, two ordinary AMQP bindings. The class is
// wider than the empty component: legacyBindingFilename(":615f62",…) collides
// with makeBindingFilename("a_b",…) with no empty string anywhere, because the
// fold can synthesise the "_"+hex shape the current encoder uses for escapes.
//
// The record names its owner; the filename is only a slot. That is the same
// invariant removeBindingByIdentity applies on the delete side, and this is it
// applied on the store side — one file read rather than a directory scan, so
// declare stays O(1) instead of O(n) (and building a topology O(n) instead of
// O(n²)). For an ordinary name the two spellings are equal and this returns
// before touching the disk at all.
//
// THE READ IS LOAD-BEARING AND THE OVERLAP IT DEFENDS AGAINST STILL EXISTS.
// Nothing here narrowed the encoders; the collision is exactly as reachable as
// it was. A binding is attackable whenever its current filename carries three
// or more "_" — the legacy fold is the identity on any string free of its eight
// bytes — and a component is generated (hence "_"-prefixed) when it is empty,
// ".", "..", over-length, or contains "/", NUL, OR AN UNDERSCORE. Measured on
// this tree: 27 distinct attacking triples for each of ("orders","events",
// "order_created"), ("user_events","amq.topic","signup"),
// ("orders","billing_ex","invoice.paid") and ("orders","amq.fanout","") —
// C(k,2)·9^(k-2) for k underscores, exact rather than sampled. So it is not the
// empty routing key that is at risk; it is ANY binding with an underscore
// anywhere in its triple. Only the ATTRIBUTION changed. Simplify this read away
// — "we already know the name, why re-read the file" — and the entire class
// reopens silently. The real fix is structural and is not here: the metadata
// tier next door makes the namespaces disjoint by construction with a
// %escaped/ subdirectory (see metadataEscapeDir), and this tier never adopted
// it.
//
// IT CANNOT FAIL THE CALLER, AND THE SIGNATURE ENFORCES THAT. By the time this
// runs, atomicWrite has already returned nil and the binding IS persisted. An
// errno here would report failure for an operation that succeeded: the client
// would see queue.bind fail, believe no binding exists, and routing would use
// it anyway — and a retry would rewrite the same record and fail identically,
// forever. Leaving a duplicate is bounded and the next declare clears it, so
// the unlink is best-effort housekeeping, not part of the caller's contract.
// (Compare DeleteBinding swallowing IsNotExist, which WAS the caller's own
// operation reporting success while doing nothing. Swallow housekeeping, never
// the contract.)
//
// WHY THE DISCARDED ERROR ON THE UNLINK IS NOT THE DEFECT THIS CHANGE IS ABOUT.
// A bare `_ = os.Remove` inside a change whose whole theme was silent no-ops
// deserves the suspicion, so: the difference is that this site has ALREADY
// ATTRIBUTED the file. The record was read, decoded and matched, so a failed
// unlink cannot destroy anyone's data and cannot hide a mistake about WHOSE
// file this is — the worst case is the bounded duplicate above, on a contract
// that was already met. The no-op defects this change fixes were the opposite
// shape: they swallowed an error on a path that could not tell "nothing to do"
// from "what I tried to do failed". This one can, and did, one line earlier.
//
// THE ORDER IS LOAD-BEARING: callers must write the new record FIRST and call
// this second. The two files are not updated atomically, so a crash in between
// leaves one of two states, and only one of them is survivable:
//
//	write, then remove -> both files present. No binding is lost, but this is
//	                      NOT a benign state: it IS the defect this function
//	                      exists to prevent. While both files sit there unbind
//	                      silently fails — DeleteBinding removes the current
//	                      filename, err == nil, the identity fallback is never
//	                      reached, and the binding survives every restart. A
//	                      later declare of the triple clears it, and NOTHING
//	                      GUARANTEES ONE EVER COMES. The bound is "no worse
//	                      than every build before this one", not "harmless".
//	remove, then write -> neither file present. The binding is GONE, with the
//	                      client's original declare confirmed long ago.
//
// So the tidier-looking order trades a cleanup lag for silent data loss. Do not
// swap these.
func removeLegacySlot(dir, current, legacy string, ownsRecord func(data []byte) bool) {
	if legacy == current {
		return
	}
	path := filepath.Join(dir, legacy)

	// A read failure is not an error to report: the file is absent, or the
	// legacy spelling is one no build could ever have written — the old join
	// bounded nothing and did not fold NUL, so it spells 776-byte names
	// (ENAMETOOLONG) and NUL-bearing paths (EINVAL) for entirely legal AMQP
	// input. In every case there is nothing of ours here to remove.
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if !ownsRecord(data) {
		return // another triple's live record sitting in this slot
	}
	_ = os.Remove(path) // best-effort; see the signature note above
}

// StoreBinding persists a binding to disk and updates cache
func (pm *PersistentMetadataStore) StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	binding := &interfaces.QueueBinding{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Arguments:    arguments,
	}

	data, err := cbor.Marshal(binding)
	if err != nil {
		return fmt.Errorf("failed to marshal binding: %w", err)
	}

	dir := filepath.Join(pm.baseDir, BindingsDir)
	filename := makeBindingFilename(queueName, exchangeName, routingKey)
	if err := pm.atomicWrite(filepath.Join(dir, filename), data); err != nil {
		return err
	}
	// AFTER the write, never before, and inside the lock above so a concurrent
	// declare of this triple cannot land between the two. See removeLegacySlot
	// on why swapping these two statements loses bindings, and why the slot's
	// record must be read before it is unlinked.
	removeLegacySlot(dir, filename,
		legacyBindingFilename(queueName, exchangeName, routingKey),
		func(data []byte) bool {
			var b interfaces.QueueBinding
			if cbor.Unmarshal(data, &b) != nil {
				return false // undecodable: cannot claim it, so leave it alone
			}
			return b.QueueName == queueName &&
				b.ExchangeName == exchangeName &&
				b.RoutingKey == routingKey
		})

	// Update cache (Phase 6H)
	if pm.cacheEnabled {
		cacheKey := makeBindingCacheKey(queueName, exchangeName, routingKey)
		pm.bindingCache.Store(cacheKey, binding)

		// Invalidate queue and exchange binding caches to force rebuild on next access
		pm.queueBindingsCache.Delete(queueName)
		pm.exchangeBindingsCache.Delete(exchangeName)
	}

	return nil
}

// GetBinding loads a binding from cache or disk
func (pm *PersistentMetadataStore) GetBinding(queueName, exchangeName, routingKey string) (*interfaces.QueueBinding, error) {
	// Try cache first (Phase 6H: lock-free hot path!)
	if pm.cacheEnabled {
		cacheKey := makeBindingCacheKey(queueName, exchangeName, routingKey)
		if cached, ok := pm.bindingCache.Load(cacheKey); ok {
			return cached.(*interfaces.QueueBinding), nil
		}
	}

	// Cache miss - load from disk
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	filename := makeBindingFilename(queueName, exchangeName, routingKey)
	path := filepath.Join(pm.baseDir, BindingsDir, filename)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrBindingNotFound
		}
		return nil, fmt.Errorf("failed to read binding file: %w", err)
	}

	var binding interfaces.QueueBinding
	if err := cbor.Unmarshal(data, &binding); err != nil {
		return nil, fmt.Errorf("failed to unmarshal binding: %w", err)
	}

	// Update cache on load (Phase 6H)
	if pm.cacheEnabled {
		cacheKey := makeBindingCacheKey(queueName, exchangeName, routingKey)
		pm.bindingCache.Store(cacheKey, &binding)
	}

	return &binding, nil
}

// DeleteBinding removes a binding file and cache entry
func (pm *PersistentMetadataStore) DeleteBinding(queueName, exchangeName, routingKey string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	dir := filepath.Join(pm.baseDir, BindingsDir)
	filename := makeBindingFilename(queueName, exchangeName, routingKey)

	switch err := os.Remove(filepath.Join(dir, filename)); {
	case err == nil:
	case os.IsNotExist(err):
		// The record may predate this build's filename encoding, so fall back to
		// identity: find the file whose RECORD names this triple and remove that.
		//
		// Without this, changing the encoding turns unbind into a silent no-op
		// for every binding the old build spelled differently — os.Remove misses,
		// IsNotExist is swallowed, DeleteBinding reports success, and ListBindings
		// keeps returning the binding across restarts. The filename is a slot; the
		// record names its owner (the same invariant loadCacheFromDisk states).
		if rerr := pm.removeBindingByIdentity(dir, func(b *interfaces.QueueBinding) bool {
			return b.QueueName == queueName && b.ExchangeName == exchangeName && b.RoutingKey == routingKey
		}); rerr != nil {
			return rerr
		}
	default:
		return fmt.Errorf("failed to delete binding file: %w", err)
	}

	// Remove from cache (Phase 6H)
	if pm.cacheEnabled {
		cacheKey := makeBindingCacheKey(queueName, exchangeName, routingKey)
		pm.bindingCache.Delete(cacheKey)

		// Invalidate queue and exchange binding caches to force rebuild
		pm.queueBindingsCache.Delete(queueName)
		pm.exchangeBindingsCache.Delete(exchangeName)
	}

	return nil
}

// removeBindingByIdentity deletes every binding file under dir whose decoded
// record satisfies match. It is the upgrade path for a filename-encoding change:
// a record written under an older spelling is still found, because the record —
// not the filename — carries the identity.
//
// It removes ALL matches rather than the first, because a directory really can
// hold more than one file decoding to one identity: a record written under an
// older spelling, plus one written under the current spelling for the same
// triple. Leaving either behind resurrects the binding at the next boot.
//
// Note the old non-injective join is NOT what produces that state — two triples
// sharing one file leaves one file holding one identity, the survivor. The
// duplicate comes from the encoding change itself, and removeLegacySlot is what
// stops a re-declare creating one. This fallback is the reader for whatever a
// build without that removal already left on disk.
func (pm *PersistentMetadataStore) removeBindingByIdentity(dir string, match func(*interfaces.QueueBinding) bool) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read bindings directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}
		path := filepath.Join(dir, file.Name())
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			continue // Skip unreadable files, as every other reader here does.
		}
		var binding interfaces.QueueBinding
		if cbor.Unmarshal(data, &binding) != nil {
			continue // Skip corrupted files.
		}
		if !match(&binding) {
			continue
		}
		if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) {
			return fmt.Errorf("failed to delete binding file %s: %w", path, rerr)
		}
	}

	return nil
}

// ListBindings returns all bindings
func (pm *PersistentMetadataStore) ListBindings() ([]*interfaces.QueueBinding, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	dir := filepath.Join(pm.baseDir, BindingsDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*interfaces.QueueBinding{}, nil
		}
		return nil, fmt.Errorf("failed to read bindings directory: %w", err)
	}

	bindings := make([]*interfaces.QueueBinding, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		path := filepath.Join(dir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue // Skip unreadable files
		}

		var binding interfaces.QueueBinding
		if err := cbor.Unmarshal(data, &binding); err != nil {
			continue // Skip corrupted files
		}

		bindings = append(bindings, &binding)
	}

	return bindings, nil
}

// GetQueueBindings returns all bindings for a specific queue (cached)
func (pm *PersistentMetadataStore) GetQueueBindings(queueName string) ([]*interfaces.QueueBinding, error) {
	// Try cache first (Phase 6H: O(1) lookup instead of O(n) scan!)
	if pm.cacheEnabled {
		if cached, ok := pm.queueBindingsCache.Load(queueName); ok {
			return cached.([]*interfaces.QueueBinding), nil
		}
	}

	// Cache miss - load all bindings and rebuild index
	allBindings, err := pm.ListBindings()
	if err != nil {
		return nil, err
	}

	result := make([]*interfaces.QueueBinding, 0)
	for _, binding := range allBindings {
		if binding.QueueName == queueName {
			result = append(result, binding)
		}
	}

	// Update cache with result (Phase 6H)
	if pm.cacheEnabled {
		pm.queueBindingsCache.Store(queueName, result)
	}

	return result, nil
}

// GetExchangeBindings returns all bindings for a specific exchange (cached)
func (pm *PersistentMetadataStore) GetExchangeBindings(exchangeName string) ([]*interfaces.QueueBinding, error) {
	// Try cache first (Phase 6H: O(1) lookup instead of O(n) scan!)
	if pm.cacheEnabled {
		if cached, ok := pm.exchangeBindingsCache.Load(exchangeName); ok {
			return cached.([]*interfaces.QueueBinding), nil
		}
	}

	// Cache miss - load all bindings and rebuild index
	allBindings, err := pm.ListBindings()
	if err != nil {
		return nil, err
	}

	result := make([]*interfaces.QueueBinding, 0)
	for _, binding := range allBindings {
		if binding.ExchangeName == exchangeName {
			result = append(result, binding)
		}
	}

	// Update cache with result (Phase 6H)
	if pm.cacheEnabled {
		pm.exchangeBindingsCache.Store(exchangeName, result)
	}

	return result, nil
}

// makeExchangeBindingFilename maps an exchange-to-exchange binding's (source,
// destination, routing key) triple to the file that holds it. It carried a
// byte-for-byte copy of makeBindingFilename's non-injective join, with the same
// absent occupancy check, feeding loadExchangeBindingsFromDisk on the boot path.
// See makeBindingFilename for the argument; the fix is the same.
func makeExchangeBindingFilename(source, destination, routingKey string) string {
	return fmt.Sprintf("%s_%s_%s%s",
		filenameComponent(source, bindingComponentMaxLen),
		filenameComponent(destination, bindingComponentMaxLen),
		filenameComponent(routingKey, bindingComponentMaxLen),
		FileExtension)
}

// loadExchangeBindingsFromDisk loads all exchange-to-exchange bindings from disk
func (pm *PersistentMetadataStore) loadExchangeBindingsFromDisk() ([]*interfaces.ExchangeBinding, error) {
	dir := filepath.Join(pm.baseDir, ExchangeBindingsDir)
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*interfaces.ExchangeBinding{}, nil
		}
		return nil, fmt.Errorf("failed to read exchange bindings directory: %w", err)
	}

	bindings := make([]*interfaces.ExchangeBinding, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		path := filepath.Join(dir, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var binding interfaces.ExchangeBinding
		if err := cbor.Unmarshal(data, &binding); err != nil {
			continue
		}

		bindings = append(bindings, &binding)
	}

	return bindings, nil
}

// StoreExchangeBinding persists an exchange-to-exchange binding to disk and updates cache
func (pm *PersistentMetadataStore) StoreExchangeBinding(source, destination, routingKey string, arguments map[string]interface{}) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	binding := &interfaces.ExchangeBinding{
		Source:      source,
		Destination: destination,
		RoutingKey:  routingKey,
		Arguments:   arguments,
	}

	data, err := cbor.Marshal(binding)
	if err != nil {
		return fmt.Errorf("failed to marshal exchange binding: %w", err)
	}

	dir := filepath.Join(pm.baseDir, ExchangeBindingsDir)
	filename := makeExchangeBindingFilename(source, destination, routingKey)
	if err := pm.atomicWrite(filepath.Join(dir, filename), data); err != nil {
		return err
	}
	// Same upgrade path as StoreBinding, same ordering constraint, same lock,
	// same cross-triple collision — see removeLegacySlot. The fallback that gets
	// bypassed on this tier is DeleteExchangeBinding's own IsNotExist arm, and
	// the record decodes as an ExchangeBinding rather than a QueueBinding, which
	// is the only reason ownership is a closure instead of one shared helper.
	removeLegacySlot(dir, filename,
		legacyBindingFilename(source, destination, routingKey),
		func(data []byte) bool {
			var b interfaces.ExchangeBinding
			if cbor.Unmarshal(data, &b) != nil {
				return false
			}
			return b.Source == source &&
				b.Destination == destination &&
				b.RoutingKey == routingKey
		})

	if pm.cacheEnabled {
		pm.exchBindingsFromCache.Delete(source)
	}

	return nil
}

// DeleteExchangeBinding removes an exchange-to-exchange binding from disk and cache
func (pm *PersistentMetadataStore) DeleteExchangeBinding(source, destination, routingKey string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	dir := filepath.Join(pm.baseDir, ExchangeBindingsDir)
	filename := makeExchangeBindingFilename(source, destination, routingKey)

	switch err := os.Remove(filepath.Join(dir, filename)); {
	case err == nil:
	case os.IsNotExist(err):
		// Same upgrade path as DeleteBinding — see removeBindingByIdentity.
		if rerr := pm.removeExchangeBindingByIdentity(dir, func(b *interfaces.ExchangeBinding) bool {
			return b.Source == source && b.Destination == destination && b.RoutingKey == routingKey
		}); rerr != nil {
			return rerr
		}
	default:
		return fmt.Errorf("failed to delete exchange binding file: %w", err)
	}

	if pm.cacheEnabled {
		pm.exchBindingsFromCache.Delete(source)
	}

	return nil
}

// removeExchangeBindingByIdentity is removeBindingByIdentity's twin for
// exchange-to-exchange bindings. See that function for why identity, not
// filename, drives the fallback.
func (pm *PersistentMetadataStore) removeExchangeBindingByIdentity(dir string, match func(*interfaces.ExchangeBinding) bool) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read exchange bindings directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}
		path := filepath.Join(dir, file.Name())
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			continue
		}
		var binding interfaces.ExchangeBinding
		if cbor.Unmarshal(data, &binding) != nil {
			continue
		}
		if !match(&binding) {
			continue
		}
		if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) {
			return fmt.Errorf("failed to delete exchange binding file %s: %w", path, rerr)
		}
	}

	return nil
}

// GetExchangeBindingsFrom returns all exchange-to-exchange bindings from the given source exchange
func (pm *PersistentMetadataStore) GetExchangeBindingsFrom(source string) ([]*interfaces.ExchangeBinding, error) {
	if pm.cacheEnabled {
		if cached, ok := pm.exchBindingsFromCache.Load(source); ok {
			return cached.([]*interfaces.ExchangeBinding), nil
		}
	}

	allBindings, err := pm.loadExchangeBindingsFromDisk()
	if err != nil {
		return nil, err
	}

	result := make([]*interfaces.ExchangeBinding, 0)
	for _, binding := range allBindings {
		if binding.Source == source {
			result = append(result, binding)
		}
	}

	if pm.cacheEnabled {
		pm.exchBindingsFromCache.Store(source, result)
	}

	return result, nil
}

// StoreConsumer is a no-op - consumers are runtime state and should not be persisted
// Consumers contain channel pointers, goroutines (Messages chan, Cancel chan), and other
// non-serializable state that causes circular references when attempting JSON marshalling.
// Consumer registration is ephemeral - they're recreated on each connection.
func (pm *PersistentMetadataStore) StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	// No-op: Consumers are not persisted
	return nil
}

// GetConsumer loads a consumer from disk
func (pm *PersistentMetadataStore) GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	path := filepath.Join(pm.baseDir, ConsumersDir, metadataPathComponent(queueName), metadataSlotName(consumerTag))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, interfaces.ErrConsumerNotFound
		}
		return nil, fmt.Errorf("failed to read consumer file: %w", err)
	}

	var consumer protocol.Consumer
	if err := cbor.Unmarshal(data, &consumer); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer: %w", err)
	}

	return &consumer, nil
}

// DeleteConsumer removes a consumer file
func (pm *PersistentMetadataStore) DeleteConsumer(queueName, consumerTag string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	path := filepath.Join(pm.baseDir, ConsumersDir, metadataPathComponent(queueName), metadataSlotName(consumerTag))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete consumer file: %w", err)
	}

	return nil
}

// GetQueueConsumers returns all consumers for a queue
func (pm *PersistentMetadataStore) GetQueueConsumers(queueName string) ([]*protocol.Consumer, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	queueDir := filepath.Join(pm.baseDir, ConsumersDir, metadataPathComponent(queueName))
	files, err := os.ReadDir(queueDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*protocol.Consumer{}, nil
		}
		return nil, fmt.Errorf("failed to read consumers directory: %w", err)
	}

	consumers := make([]*protocol.Consumer, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), FileExtension) {
			continue
		}

		// Content-authoritative: the record carries its own tag, so a
		// generated slot name never has to be decoded.
		data, rerr := os.ReadFile(filepath.Join(queueDir, file.Name()))
		if rerr != nil {
			continue
		}
		var consumer protocol.Consumer
		if uerr := cbor.Unmarshal(data, &consumer); uerr != nil {
			continue // Skip corrupted files
		}
		consumers = append(consumers, &consumer)
	}

	return consumers, nil
}

// LoadAllMetadata loads all metadata from disk for recovery
func (pm *PersistentMetadataStore) LoadAllMetadata() (
	exchanges []*protocol.Exchange,
	queues []*protocol.Queue,
	bindings []*interfaces.QueueBinding,
	consumers map[string][]*protocol.Consumer,
	err error,
) {
	consumers = make(map[string][]*protocol.Consumer)

	// Load exchanges
	exchanges, err = pm.ListExchanges()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load exchanges: %w", err)
	}

	// Load queues
	queues, err = pm.ListQueues()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load queues: %w", err)
	}

	// Load bindings
	bindings, err = pm.ListBindings()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load bindings: %w", err)
	}

	// Load consumers for each queue
	for _, queue := range queues {
		queueConsumers, err := pm.GetQueueConsumers(queue.Name)
		if err != nil {
			continue // Skip errors for individual queues
		}
		if len(queueConsumers) > 0 {
			consumers[queue.Name] = queueConsumers
		}
	}

	return exchanges, queues, bindings, consumers, nil
}

// Close closes the metadata store (no-op for JSON files)
func (pm *PersistentMetadataStore) Close() error {
	return nil
}
