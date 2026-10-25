package server

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/maxpert/amqp-go/transaction"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ServerBuilder provides a fluent API for building AMQP servers
type ServerBuilder struct {
	config            *config.AMQPConfig
	logger            interfaces.Logger
	broker            UnifiedBroker
	storage           interfaces.Storage
	authenticator     interfaces.Authenticator
	connectionHandler interfaces.ConnectionHandler
	metrics           MetricsCollector
}

// NewServerBuilder creates a new server builder with default configuration
func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{
		config:            config.DefaultConfig(),
		logger:            nil,
		broker:            nil,
		storage:           nil,
		authenticator:     nil,
		connectionHandler: nil,
	}
}

// NewServerBuilderWithConfig creates a server builder with the given configuration
func NewServerBuilderWithConfig(cfg *config.AMQPConfig) *ServerBuilder {
	return &ServerBuilder{
		config:            cfg,
		logger:            nil,
		broker:            nil,
		storage:           nil,
		authenticator:     nil,
		connectionHandler: nil,
	}
}

// WithConfig sets the server configuration
func (b *ServerBuilder) WithConfig(config *config.AMQPConfig) *ServerBuilder {
	b.config = config
	return b
}

// WithAddress sets the server address
func (b *ServerBuilder) WithAddress(address string) *ServerBuilder {
	b.config.Network.Address = address
	return b
}

// WithPort sets the server port
func (b *ServerBuilder) WithPort(port int) *ServerBuilder {
	b.config.Network.Port = port
	return b
}

// WithLogger sets a custom logger implementation
func (b *ServerBuilder) WithLogger(logger interfaces.Logger) *ServerBuilder {
	b.logger = logger
	return b
}

// WithZapLogger creates a logger using zap with the specified level
func (b *ServerBuilder) WithZapLogger(level string) *ServerBuilder {
	var zapConfig zap.Config

	switch level {
	case "debug":
		zapConfig = zap.NewDevelopmentConfig()
	case "info", "warn", "error":
		zapConfig = zap.NewProductionConfig()
		zapConfig.Level = parseZapLevel(level)
	default:
		zapConfig = zap.NewProductionConfig()
	}

	logger, err := zapConfig.Build()
	if err != nil {
		// Fallback to a basic logger if configuration fails
		logger, _ = zap.NewProduction()
	}

	b.logger = &ZapLoggerAdapter{logger: logger}
	return b
}

// WithMetrics sets a custom metrics collector implementation
func (b *ServerBuilder) WithMetrics(metrics MetricsCollector) *ServerBuilder {
	b.metrics = metrics
	return b
}

// WithUnifiedBroker sets a custom unified broker implementation
func (b *ServerBuilder) WithUnifiedBroker(unifiedBroker UnifiedBroker) *ServerBuilder {
	b.broker = unifiedBroker
	return b
}

// WithStorage sets a custom storage implementation
func (b *ServerBuilder) WithStorage(storage interfaces.Storage) *ServerBuilder {
	b.storage = storage
	return b
}

// WithAuthenticator sets a custom authenticator implementation
func (b *ServerBuilder) WithAuthenticator(auth interfaces.Authenticator) *ServerBuilder {
	b.authenticator = auth
	return b
}

// WithFileAuthentication enables file-based authentication
func (b *ServerBuilder) WithFileAuthentication(userFile string) *ServerBuilder {
	b.config.Security.AuthenticationEnabled = true
	b.config.Security.AuthenticationBackend = "file"
	b.config.Security.AuthenticationFilePath = userFile
	if b.config.Security.AuthenticationConfig == nil {
		b.config.Security.AuthenticationConfig = make(map[string]interface{})
	}
	b.config.Security.AuthenticationConfig["user_file"] = userFile
	return b
}

// WithConnectionHandler sets a custom connection handler
func (b *ServerBuilder) WithConnectionHandler(handler interfaces.ConnectionHandler) *ServerBuilder {
	b.connectionHandler = handler
	return b
}

// WithTLS enables TLS with the specified certificate and key files
func (b *ServerBuilder) WithTLS(certFile, keyFile string) *ServerBuilder {
	b.config.Security.TLSEnabled = true
	b.config.Security.TLSCertFile = certFile
	b.config.Security.TLSKeyFile = keyFile
	return b
}

// WithTLSMutualAuth enables mutual TLS by setting the CA file for client
// certificate verification. Also sets TLSEnabled=true so that missing
// cert/key files fail explicitly in Validate() rather than silently
// downgrading to plaintext.
func (b *ServerBuilder) WithTLSMutualAuth(caFile string) *ServerBuilder {
	b.config.Security.TLSEnabled = true
	b.config.Security.TLSCAFile = caFile
	return b
}

// WithMaxConnections sets the maximum number of concurrent connections
func (b *ServerBuilder) WithMaxConnections(max int) *ServerBuilder {
	b.config.Network.MaxConnections = max
	return b
}

// WithProtocolLimits sets AMQP protocol limits
func (b *ServerBuilder) WithProtocolLimits(maxChannels, maxFrameSize int, maxMessageSize int64) *ServerBuilder {
	b.config.Server.MaxChannelsPerConnection = maxChannels
	b.config.Server.MaxFrameSize = maxFrameSize
	b.config.Server.MaxMessageSize = maxMessageSize
	return b
}

// Build constructs the server with all configured components
func (b *ServerBuilder) Build() (*Server, error) {
	// Validate configuration
	if err := b.config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Memory configuration removed - no longer using mailboxes
	// TODO: Implement memory limits at disruptor/queue level

	// Create logger if not provided
	logger := b.logger
	if logger == nil {
		zapLogger, err := createZapLogger(b.config.Server.LogLevel, b.config.Server.LogFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
		logger = &ZapLoggerAdapter{logger: zapLogger}
	}

	// Phase 1: Use disruptor-based in-memory storage
	var storageImpl interfaces.Storage
	if b.storage != nil {
		storageImpl = b.storage
	} else {
		// Create new disruptor storage with the engine config
		var err error
		storageImpl, err = storage.NewDisruptorStorageWithEngineConfig(
			b.config.Storage.Path,
			b.config.GetEngine(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize durable storage at %q: %w", b.config.Storage.Path, err)
		}
		logger.Info("Using disruptor-based storage")
	}

	// The storage tiers have cold-path durability failures the call stack cannot
	// return: a checkpoint that cannot run leaves a WAL file unreclaimed
	// forever, and a segment withdrawn from compaction is invisible without a
	// line naming the file (review-4 B-2, B-4, N-7). Wired HERE, next to the
	// storage, rather than inside the broker branch below — an injected broker
	// must not decide whether the storage tier can report.
	if ls, ok := storageImpl.(interface {
		SetLogger(interfaces.Logger)
	}); ok {
		ls.SetLogger(logger)
	}

	// Create broker if not provided.
	//
	// ordinalGating carries any boot-gating fault raised HERE forward to the
	// degraded-boot bookkeeping below. The Server object does not exist yet at
	// this point, so a broker gated ONLY by this branch used to log once at
	// startup and then be silent forever: no pinned gauge, no 5-minute nag,
	// UnsafeRecoveryFaults() empty. Nothing guaranteed the recovery branch
	// would cover for it.
	var ordinalGating []*interfaces.RecoveryFault
	var unifiedBroker UnifiedBroker
	if b.broker != nil {
		unifiedBroker = b.broker
	} else {
		// Create storage-backed broker using the storage we just created
		// Phase 6G: Pass engine config for tunable parameters
		storageBroker := broker.NewStorageBroker(storageImpl, b.config.GetEngine())
		storageBroker.SetLogger(logger)
		// WS-D §15: wire the MaxQueues/MaxExchanges topology limits.
		storageBroker.SetTopologyLimits(b.config.GetServer().MaxQueues, b.config.GetServer().MaxExchanges)
		// initOrdinalAllocator establishes the delivery-tag ordinal high-water
		// mark. It cannot report through the constructor's signature, and a
		// zero mark hands a brand-new queue an ordinal whose records are still
		// physically on disk — a cross-queue tag collision in a WAL keyed by a
		// bare uint64. Checked HERE rather than relying on PerformRecovery to
		// hit the same underlying error a few lines below, because "the other
		// check will catch it" is exactly the unwritten load-bearing premise
		// this step exists to remove.
		//
		// The gate is `len(gating) > 0`, NOT `ierr != nil`: InitError can carry
		// a benign-only error (it folds in GetRecoverableMessages' fault, and
		// the segment tier lands a new producer there in Step 4), and a refusal
		// whose artifact list is empty tells an operator nothing. The
		// unreachability of that today rests on storage.joinFaults filtering
		// benign faults out — an unwritten load-bearing premise in a different
		// package — so it is checked here rather than inherited.
		if ierr := storageBroker.InitError(); ierr != nil {
			ordinalGating = gatingFaultList(interfaces.ExplainedFaults("ordinal-allocator", ierr))
			if len(ordinalGating) > 0 {
				if !b.config.Storage.UnsafeRecovery {
					return nil, errors.New(interfaces.RecoveryRefusalMessage(
						"delivery-tag ordinal recovery", ordinalGating))
				}
				logUnsafeRecovery(logger, "ordinal-allocator", ordinalGating)
			}
		}
		unifiedBroker = NewStorageBrokerAdapter(storageBroker)
	}

	// Create transaction manager with atomic storage support if available
	var transactionManager interfaces.TransactionManager
	if atomicStorage, ok := storageImpl.(interfaces.AtomicStorage); ok {
		transactionManager = transaction.NewTransactionManagerWithStorage(atomicStorage)
	} else {
		transactionManager = transaction.NewTransactionManager()
	}

	// Set the broker as the transaction executor
	executor := transaction.NewUnifiedBrokerExecutor(unifiedBroker)
	transactionManager.SetExecutor(executor)

	// Use provided metrics collector or default to NoOp
	metricsCollector := b.metrics
	if metricsCollector == nil {
		metricsCollector = &NoOpMetricsCollector{}
	}

	// Wire authentication: if WithAuthenticator was called, it implies auth is on.
	var authenticator interfaces.Authenticator
	if b.authenticator != nil {
		b.config.Security.AuthenticationEnabled = true
		authenticator = b.authenticator
	}

	// If auth is enabled but no authenticator was provided, construct from config.
	if authenticator == nil && b.config.Security.AuthenticationEnabled {
		var err error
		authenticator, err = b.constructAuthenticator()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize authenticator: %w", err)
		}
		if authenticator == nil {
			return nil, fmt.Errorf("authentication enabled but authenticator construction yielded nil (fail-closed)")
		}
	}

	// Build mechanism registry from configured mechanisms (default PLAIN only).
	// Do NOT use auth.DefaultRegistry() — it does not include ANONYMOUS (opt-in only).
	var mechanismRegistry MechanismRegistry
	mechanisms := b.config.Security.AuthMechanisms
	if len(mechanisms) == 0 {
		mechanisms = []string{"PLAIN"}
	}
	registry := auth.RegistryForMechanisms(mechanisms)
	mechanismRegistry = NewMechanismRegistryAdapter(registry)

	// If auth is enabled, fail-closed if authenticator or registry is nil.
	if b.config.Security.AuthenticationEnabled {
		if authenticator == nil {
			return nil, fmt.Errorf("authentication enabled but no authenticator available (fail-closed)")
		}
		if mechanismRegistry == nil {
			return nil, fmt.Errorf("authentication enabled but no mechanism registry available (fail-closed)")
		}
	}

	// Create the server
	server := &Server{
		Addr:               b.config.Network.Address,
		Connections:        make(map[string]*protocol.Connection),
		Log:                logger.(*ZapLoggerAdapter).logger, // TODO: Remove this dependency
		Config:             b.config,
		Broker:             unifiedBroker,
		TransactionManager: transactionManager,
		Authenticator:      authenticator,
		MechanismRegistry:  mechanismRegistry,
		MetricsCollector:   metricsCollector,
		StartTime:          time.Now(),
		// WS-B (§11.4): retain the storage/broker handles so Shutdown can close
		// exactly what the builder created (and nothing injected by the caller).
		storage:     storageImpl,
		ownsStorage: b.storage == nil,
		ownsBroker:  b.broker == nil,
		// SQ-12: resolve resource-alarm thresholds + samplers. nil when alarms
		// are disabled, in which case the monitor is never spawned (zero-cost).
		alarm: buildAlarmThresholds(b.config),
	}

	// Propagate metrics collector to storage if it supports SetMetrics.
	if sm, ok := storageImpl.(interface{ SetMetrics(storage.StorageMetrics) }); ok {
		sm.SetMetrics(metricsCollector)
	}

	// Create and attach lifecycle manager
	server.Lifecycle = NewLifecycleManager(server, b.config)

	// WS-I Phase B: with the publish-inflight lease enabled, the broker MUST
	// support non-blocking transient admission — otherwise a transient
	// publish parked in WaitForCapacity would hold leased bytes behind queue
	// backpressure, defeating the bound. (PublishInflightBudgetBytes = -1
	// disables the lease and this requirement.)
	if b.config.PublishInflightBudget() > 0 {
		if _, ok := unifiedBroker.(NonBlockingPublisher); !ok {
			return nil, fmt.Errorf("publish inflight budget is enabled but the configured broker lacks NonBlockingPublisher (PublishMessageAdmitted); set network.publishinflightbudgetbytes: -1 to disable the lease")
		}
	}

	// WS-I Phase B: build the publish-inflight lease limiter (it is otherwise
	// built lazily on the first publish — building it here wires the metrics
	// and the budget gauge at startup).
	_ = server.publishLimiter()

	// Always perform recovery (storage is always persistent)
	recoveryManager := NewRecoveryManager(storageImpl, unifiedBroker, logger.(*ZapLoggerAdapter).logger)

	recoveryStats, err := recoveryManager.PerformRecovery()

	// THE INVERSION. This used to be an ALLOW-LIST of two fatal sentinels:
	// anything else logged "Recovery failed" and fell through to
	// `return server, nil`, so the DEFAULT for every recovery error nobody had
	// thought of yet was "boot with an empty broker and start accepting and
	// confirming durable publishes". Measured end to end: recovery aborts with
	// `unsupported WAL file format version: got 1, want 4`, a durable
	// StoreMessage then returns OK — which is where the publisher confirm is
	// sent — the bytes land on disk, and the next boot recovers zero.
	//
	// Now the severity comes from the site that produced the error
	// (interfaces.RecoveryOutcome), an unclassified error is FATAL by the
	// type's zero value, and only --unsafe-recovery proceeds.
	faults := interfaces.ExplainedFaults("recovery", err)
	gating := gatingFaultList(faults)
	if len(gating) > 0 && !b.config.Storage.UnsafeRecovery {
		return nil, errors.New(interfaces.RecoveryRefusalMessage("recovery", gating))
	}
	if len(gating) > 0 {
		logUnsafeRecovery(logger, "recovery", gating)
	}
	// EVERY gating fault this boot overrode is marked and measured, wherever it
	// was raised — the ordinal-allocator branch above included — and each
	// ARTIFACT is counted once. initOrdinalAllocator and PerformRecovery both
	// read GetRecoverableMessages, so the same damaged file legitimately
	// surfaces twice; a gauge that said "2 artifacts discarded" for one
	// discarded file would be a cost statement that overstates, which is
	// exactly as wrong as one that understates.
	if degraded := dedupeFaultsByArtifact(ordinalGating, gating); len(degraded) > 0 {
		server.markUnsafeRecovery(degraded)
		metricsCollector.SetUnsafeRecoveryArtifacts(len(degraded))
	}
	for _, f := range faults {
		if !f.Outcome.GatesBoot() {
			logger.Warn("recovery discarded records it was allowed to discard",
				interfaces.LogField{Key: "stage", Value: f.Stage},
				interfaces.LogField{Key: "artifact", Value: f.Artifact},
				interfaces.LogField{Key: "detail", Value: f.Detail})
		}
	}
	if recoveryStats != nil {
		logger.Info("Recovery completed",
			interfaces.LogField{Key: "exchanges_recovered", Value: recoveryStats.DurableExchangesRecovered},
			interfaces.LogField{Key: "queues_recovered", Value: recoveryStats.DurableQueuesRecovered},
			interfaces.LogField{Key: "messages_recovered", Value: recoveryStats.PersistentMessagesRecovered},
			interfaces.LogField{Key: "faults", Value: len(faults)})
	}

	return server, nil
}

// dedupeFaultsByArtifact concatenates fault lists, keeping the first fault for
// each (outcome, artifact) pair. Order is preserved so an operator reads them
// in the order recovery hit them.
func dedupeFaultsByArtifact(lists ...[]*interfaces.RecoveryFault) []*interfaces.RecoveryFault {
	type key struct {
		outcome  interfaces.RecoveryOutcome
		artifact string
	}
	seen := make(map[key]struct{})
	var out []*interfaces.RecoveryFault
	for _, l := range lists {
		for _, f := range l {
			k := key{f.Outcome, f.Artifact}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, f)
		}
	}
	return out
}

func gatingFaultList(faults []*interfaces.RecoveryFault) []*interfaces.RecoveryFault {
	out := make([]*interfaces.RecoveryFault, 0, len(faults))
	for _, f := range faults {
		if f.Outcome.GatesBoot() {
			out = append(out, f)
		}
	}
	return out
}

// logUnsafeRecovery names EVERY discarded artifact by path, one line each, at
// ERROR level. A single startup line is precisely what let two contaminated
// data directories sit unnoticed in this repository for three weeks.
func logUnsafeRecovery(logger interfaces.Logger, context string, faults []*interfaces.RecoveryFault) {
	for _, f := range faults {
		if !f.Outcome.GatesBoot() {
			continue
		}
		logger.Error("unsafe-recovery: DISCARDING an artifact this broker could not recover",
			interfaces.LogField{Key: "context", Value: context},
			interfaces.LogField{Key: "outcome", Value: f.Outcome.String()},
			interfaces.LogField{Key: "stage", Value: f.Stage},
			interfaces.LogField{Key: "path", Value: f.Artifact},
			interfaces.LogField{Key: "cause", Value: f.Detail},
			interfaces.LogField{Key: "cost", Value: f.Cost})
	}
}

// constructAuthenticator builds an authenticator from the security config.
// It is called when AuthenticationEnabled is true but no authenticator was
// explicitly provided via WithAuthenticator. Returns nil, nil if auth is
// disabled. Returns an error if the backend is unknown or the auth file
// does not exist (fail-closed — never auto-create a default guest file).
func (b *ServerBuilder) constructAuthenticator() (interfaces.Authenticator, error) {
	backend := b.config.Security.AuthenticationBackend
	if backend == "" {
		backend = "file"
	}

	switch backend {
	case "file":
		authPath := b.config.Security.AuthenticationFilePath
		if authPath == "" {
			if uf, ok := b.config.Security.AuthenticationConfig["user_file"]; ok {
				if s, ok := uf.(string); ok {
					authPath = s
				}
			}
		}
		if authPath == "" {
			return nil, fmt.Errorf("file authentication enabled but no auth file path configured")
		}
		if _, err := os.Stat(authPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("auth file does not exist: %s", authPath)
		}
		authenticator, err := auth.NewFileAuthenticator(authPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file authenticator: %w", err)
		}
		return authenticator, nil
	default:
		return nil, fmt.Errorf("unsupported authentication backend: %s", backend)
	}
}

// ZapLoggerAdapter adapts zap.Logger to interfaces.Logger
type ZapLoggerAdapter struct {
	logger *zap.Logger
}

func (z *ZapLoggerAdapter) Debug(msg string, fields ...interfaces.LogField) {
	z.logger.Debug(msg, z.convertFields(fields)...)
}

func (z *ZapLoggerAdapter) Info(msg string, fields ...interfaces.LogField) {
	z.logger.Info(msg, z.convertFields(fields)...)
}

func (z *ZapLoggerAdapter) Warn(msg string, fields ...interfaces.LogField) {
	z.logger.Warn(msg, z.convertFields(fields)...)
}

func (z *ZapLoggerAdapter) Error(msg string, fields ...interfaces.LogField) {
	z.logger.Error(msg, z.convertFields(fields)...)
}

func (z *ZapLoggerAdapter) Fatal(msg string, fields ...interfaces.LogField) {
	z.logger.Fatal(msg, z.convertFields(fields)...)
}

func (z *ZapLoggerAdapter) With(fields ...interfaces.LogField) interfaces.Logger {
	return &ZapLoggerAdapter{logger: z.logger.With(z.convertFields(fields)...)}
}

func (z *ZapLoggerAdapter) Sync() error {
	return z.logger.Sync()
}

func (z *ZapLoggerAdapter) convertFields(fields []interfaces.LogField) []zap.Field {
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = zap.Any(field.Key, field.Value)
	}
	return zapFields
}

// Helper functions

func parseZapLevel(level string) zap.AtomicLevel {
	switch level {
	case "debug":
		return zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "info":
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
}

func createZapLogger(level, logFile string) (*zap.Logger, error) {
	// "silent" bypasses zap.Config entirely rather than mapping to a level
	// above Fatal: a real core still pays for encoding + a sink write on
	// every log-site guard check, and every "info"-level connection
	// lifecycle log plus the unconditional Error() on read-loop teardown
	// (server.go's readFrames) would otherwise land on stdout — go test
	// folds a test binary's stdout and stderr into one stream, so those
	// writes interleave mid-line with -bench output and corrupt it. Used by
	// benchmarks that spin up an embedded server (see versusURI in
	// versus_bench_test.go) where any log write is noise, not signal.
	if level == "silent" {
		return zap.NewNop(), nil
	}

	var zapConfig zap.Config

	if level == "debug" {
		zapConfig = zap.NewDevelopmentConfig()
	} else {
		zapConfig = zap.NewProductionConfig()
		zapConfig.Level = parseZapLevel(level)
	}

	if logFile != "" {
		zapConfig.OutputPaths = []string{logFile}
	}

	return zapConfig.Build()
}
