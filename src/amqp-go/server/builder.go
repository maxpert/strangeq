package server

import (
	"fmt"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ServerBuilder provides a fluent API for building AMQP servers
type ServerBuilder struct {
	config          *config.AMQPConfig
	logger          interfaces.Logger
	broker          interfaces.Broker
	storage         interfaces.Storage
	authenticator   interfaces.Authenticator
	connectionHandler interfaces.ConnectionHandler
}

// NewServerBuilder creates a new server builder with default configuration
func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{
		config: config.DefaultConfig(),
		logger: nil,
		broker: nil,
		storage: nil,
		authenticator: nil,
		connectionHandler: nil,
	}
}

// NewServerBuilderWithConfig creates a server builder with the given configuration
func NewServerBuilderWithConfig(cfg *config.AMQPConfig) *ServerBuilder {
	return &ServerBuilder{
		config: cfg,
		logger: nil,
		broker: nil,
		storage: nil,
		authenticator: nil,
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

// WithDefaultBroker uses the default in-memory broker
func (b *ServerBuilder) WithDefaultBroker() *ServerBuilder {
	b.broker = &BrokerAdapter{broker: broker.NewBroker()}
	return b
}

// WithStorage sets a custom storage implementation
func (b *ServerBuilder) WithStorage(storage interfaces.Storage) *ServerBuilder {
	b.storage = storage
	return b
}

// WithMemoryStorage uses in-memory storage (non-persistent)
func (b *ServerBuilder) WithMemoryStorage() *ServerBuilder {
	// For now, memory storage is implicit in the default broker
	// Future implementations will provide explicit memory storage
	b.config.Storage.Backend = "memory"
	b.config.Storage.Persistent = false
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

	// Create logger if not provided
	logger := b.logger
	if logger == nil {
		zapLogger, err := createZapLogger(b.config.Server.LogLevel, b.config.Server.LogFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
		logger = &ZapLoggerAdapter{logger: zapLogger}
	}

	// Create broker if not provided
	brokerImpl := b.broker
	if brokerImpl == nil {
		brokerImpl = &BrokerAdapter{broker: broker.NewBroker()}
	}

	// Create the server
	server := &Server{
		Addr:        b.config.Network.Address,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger.(*ZapLoggerAdapter).logger, // TODO: Remove this dependency
		Broker:      brokerImpl.(*BrokerAdapter).broker, // TODO: Remove this dependency
		Config:      b.config,
	}

	// Create and attach lifecycle manager
	server.Lifecycle = NewLifecycleManager(server, b.config)

	return server, nil
}

// BuildUnsafe constructs the server without validation
func (b *ServerBuilder) BuildUnsafe() *Server {
	// Create logger if not provided
	logger := b.logger
	if logger == nil {
		zapLogger, _ := createZapLogger(b.config.Server.LogLevel, b.config.Server.LogFile)
		logger = &ZapLoggerAdapter{logger: zapLogger}
	}

	// Create broker if not provided
	brokerImpl := b.broker
	if brokerImpl == nil {
		brokerImpl = &BrokerAdapter{broker: broker.NewBroker()}
	}

	// Create the server without validation
	server := &Server{
		Addr:        b.config.Network.Address,
		Connections: make(map[string]*protocol.Connection),
		Log:         logger.(*ZapLoggerAdapter).logger,
		Broker:      brokerImpl.(*BrokerAdapter).broker,
		Config:      b.config,
	}

	// Create and attach lifecycle manager
	server.Lifecycle = NewLifecycleManager(server, b.config)

	return server
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

// BrokerAdapter adapts broker.Broker to interfaces.Broker
type BrokerAdapter struct {
	broker *broker.Broker
}

func (b *BrokerAdapter) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return b.broker.DeclareExchange(name, exchangeType, durable, autoDelete, internal, arguments)
}

func (b *BrokerAdapter) DeleteExchange(name string, ifUnused bool) error {
	return b.broker.DeleteExchange(name, ifUnused)
}

func (b *BrokerAdapter) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return b.broker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

func (b *BrokerAdapter) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	return b.broker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (b *BrokerAdapter) PurgeQueue(name string) (int, error) {
	// Not implemented in current broker
	return 0, fmt.Errorf("purge queue not implemented")
}

func (b *BrokerAdapter) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return b.broker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (b *BrokerAdapter) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return b.broker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (b *BrokerAdapter) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return b.broker.PublishMessage(exchangeName, routingKey, message)
}

func (b *BrokerAdapter) GetMessage(queueName string) (*protocol.Message, error) {
	// Not implemented in current broker
	return nil, fmt.Errorf("get message not implemented")
}

func (b *BrokerAdapter) AckMessage(queueName, messageID string) error {
	// Not implemented in current broker
	return fmt.Errorf("ack message not implemented")
}

func (b *BrokerAdapter) NackMessage(queueName, messageID string, requeue bool) error {
	// Not implemented in current broker
	return fmt.Errorf("nack message not implemented")
}

func (b *BrokerAdapter) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return b.broker.RegisterConsumer(queueName, consumerTag, consumer)
}

func (b *BrokerAdapter) UnregisterConsumer(consumerTag string) error {
	return b.broker.UnregisterConsumer(consumerTag)
}

func (b *BrokerAdapter) GetConsumers(queueName string) []*protocol.Consumer {
	// Not implemented in current broker
	return nil
}

func (b *BrokerAdapter) GetQueueInfo(queueName string) (*interfaces.QueueInfo, error) {
	// Not implemented in current broker
	return nil, fmt.Errorf("get queue info not implemented")
}

func (b *BrokerAdapter) GetExchangeInfo(exchangeName string) (*interfaces.ExchangeInfo, error) {
	// Not implemented in current broker
	return nil, fmt.Errorf("get exchange info not implemented")
}

func (b *BrokerAdapter) GetBrokerStats() (*interfaces.BrokerStats, error) {
	// Not implemented in current broker
	return nil, fmt.Errorf("get broker stats not implemented")
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