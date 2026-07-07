package server

import (
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
		// Create new disruptor storage with configurable checkpoint interval and engine config
		checkpointInterval := time.Duration(b.config.Storage.CheckpointIntervalMS) * time.Millisecond
		var err error
		storageImpl, err = storage.NewDisruptorStorageWithEngineConfig(
			b.config.Storage.Path,
			checkpointInterval,
			b.config.GetEngine(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize durable storage at %q: %w", b.config.Storage.Path, err)
		}
		if checkpointInterval == 0 {
			logger.Info("Using disruptor-based storage with offset checkpointing disabled")
		} else {
			logger.Info("Using disruptor-based storage",
				interfaces.LogField{Key: "offset_checkpoint_interval", Value: b.config.Storage.CheckpointIntervalMS})
		}
	}

	// Create broker if not provided
	var unifiedBroker UnifiedBroker
	if b.broker != nil {
		unifiedBroker = b.broker
	} else {
		// Create storage-backed broker using the storage we just created
		// Phase 6G: Pass engine config for tunable parameters
		storageBroker := broker.NewStorageBroker(storageImpl, b.config.GetEngine())
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
		// SQ-5: default durability barrier — a publish is confirmable when
		// Broker.PublishMessage returns (WAL group-commit fsync for durables).
		ConfirmBarrier: brokerSyncBarrier{broker: unifiedBroker},
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

	// Always perform recovery (storage is always persistent)
	recoveryManager := NewRecoveryManager(storageImpl, unifiedBroker, logger.(*ZapLoggerAdapter).logger)

	recoveryStats, err := recoveryManager.PerformRecovery()
	if err != nil {
		logger.Error("Recovery failed", interfaces.LogField{Key: "error", Value: err})
		// Continue with server startup even if recovery fails
	} else {
		logger.Info("Recovery completed successfully",
			interfaces.LogField{Key: "exchanges_recovered", Value: recoveryStats.DurableExchangesRecovered},
			interfaces.LogField{Key: "queues_recovered", Value: recoveryStats.DurableQueuesRecovered},
			interfaces.LogField{Key: "messages_recovered", Value: recoveryStats.PersistentMessagesRecovered})
	}

	return server, nil
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
