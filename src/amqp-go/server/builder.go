package server

import (
	"fmt"
	"time"

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
	broker            interfaces.Broker
	storage           interfaces.Storage
	authenticator     interfaces.Authenticator
	connectionHandler interfaces.ConnectionHandler
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

// WithBroker sets a custom broker implementation
func (b *ServerBuilder) WithBroker(broker interfaces.Broker) *ServerBuilder {
	b.broker = broker
	return b
}

// WithUnifiedBroker sets a custom unified broker implementation
func (b *ServerBuilder) WithUnifiedBroker(unifiedBroker UnifiedBroker) *ServerBuilder {
	// Store as interfaces.Broker to maintain compatibility
	// We'll handle the type assertion in Build()
	b.broker = &brokerWrapper{unifiedBroker: unifiedBroker}
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
		// Create new disruptor storage with configurable checkpoint interval (Phase 4)
		checkpointInterval := time.Duration(b.config.Storage.CheckpointIntervalMS) * time.Millisecond
		storageImpl = storage.NewDisruptorStorageWithCheckpointInterval(
			b.config.Storage.Path,
			checkpointInterval,
		)
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
		// Check if it's a broker wrapper
		if wrapper, ok := b.broker.(*brokerWrapper); ok {
			unifiedBroker = wrapper.unifiedBroker
		} else {
			// For backward compatibility, wrap interfaces.Broker implementations
			// This would need a more sophisticated adapter if needed
			return nil, fmt.Errorf("unsupported broker type - use WithUnifiedBroker or WithDefaultBroker")
		}
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

	// Create the server
	server := &Server{
		Addr:               b.config.Network.Address,
		Connections:        make(map[string]*protocol.Connection),
		Log:                logger.(*ZapLoggerAdapter).logger, // TODO: Remove this dependency
		Config:             b.config,
		Broker:             unifiedBroker,
		TransactionManager: transactionManager,
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

// brokerWrapper wraps UnifiedBroker to implement interfaces.Broker
type brokerWrapper struct {
	unifiedBroker UnifiedBroker
}

// Implement interfaces.Broker methods by delegating to UnifiedBroker
func (w *brokerWrapper) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return w.unifiedBroker.DeclareExchange(name, exchangeType, durable, autoDelete, internal, arguments)
}

func (w *brokerWrapper) DeleteExchange(name string, ifUnused bool) error {
	return w.unifiedBroker.DeleteExchange(name, ifUnused)
}

func (w *brokerWrapper) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return w.unifiedBroker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

func (w *brokerWrapper) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	return w.unifiedBroker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (w *brokerWrapper) PurgeQueue(name string) (int, error) {
	// Not directly supported in UnifiedBroker interface - would need extension
	return 0, fmt.Errorf("purge queue not supported in unified broker")
}

func (w *brokerWrapper) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return w.unifiedBroker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (w *brokerWrapper) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return w.unifiedBroker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (w *brokerWrapper) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return w.unifiedBroker.PublishMessage(exchangeName, routingKey, message)
}

func (w *brokerWrapper) GetMessage(queueName string) (*protocol.Message, error) {
	// Not directly supported in UnifiedBroker interface
	return nil, fmt.Errorf("get message not supported in unified broker")
}

func (w *brokerWrapper) AckMessage(queueName, messageID string) error {
	// Not directly supported - would need different signature
	return fmt.Errorf("ack message not supported in unified broker wrapper")
}

func (w *brokerWrapper) NackMessage(queueName, messageID string, requeue bool) error {
	// Not directly supported - would need different signature
	return fmt.Errorf("nack message not supported in unified broker wrapper")
}

func (w *brokerWrapper) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return w.unifiedBroker.RegisterConsumer(queueName, consumerTag, consumer)
}

func (w *brokerWrapper) UnregisterConsumer(consumerTag string) error {
	return w.unifiedBroker.UnregisterConsumer(consumerTag)
}

func (w *brokerWrapper) GetConsumers(queueName string) []*protocol.Consumer {
	// Not directly supported - UnifiedBroker returns map, not slice
	allConsumers := w.unifiedBroker.GetConsumers()
	var result []*protocol.Consumer
	for _, consumer := range allConsumers {
		if consumer.Queue == queueName {
			result = append(result, consumer)
		}
	}
	return result
}

func (w *brokerWrapper) GetQueueInfo(queueName string) (*interfaces.QueueInfo, error) {
	// Not supported in UnifiedBroker interface
	return nil, fmt.Errorf("get queue info not supported in unified broker")
}

func (w *brokerWrapper) GetExchangeInfo(exchangeName string) (*interfaces.ExchangeInfo, error) {
	// Not supported in UnifiedBroker interface
	return nil, fmt.Errorf("get exchange info not supported in unified broker")
}

func (w *brokerWrapper) GetBrokerStats() (*interfaces.BrokerStats, error) {
	// Not supported in UnifiedBroker interface
	return nil, fmt.Errorf("get broker stats not supported in unified broker")
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
