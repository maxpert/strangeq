package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// LifecycleState represents the current state of the server
type LifecycleState int

const (
	StateStopped LifecycleState = iota
	StateStarting
	StateRunning
	StateStopping
	StateError
)

func (s LifecycleState) String() string {
	switch s {
	case StateStopped:
		return "stopped"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateError:
		return "error"
	default:
		return "unknown"
	}
}

// LifecycleManager manages the server's lifecycle states and transitions
type LifecycleManager struct {
	server     *Server
	state      LifecycleState
	stateMutex sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	startTime  time.Time
	stopTime   time.Time
	lastError  error
	hooks      []LifecycleHook
	config     *config.AMQPConfig
}

// LifecycleHook defines a hook that can be called during lifecycle events
type LifecycleHook struct {
	Name     string
	OnStart  func(ctx context.Context) error
	OnStop   func(ctx context.Context) error
	OnError  func(err error)
	Priority int // Lower numbers execute first
}

// NewLifecycleManager creates a new lifecycle manager for the server
func NewLifecycleManager(server *Server, config *config.AMQPConfig) *LifecycleManager {
	return &LifecycleManager{
		server: server,
		state:  StateStopped,
		config: config,
		hooks:  make([]LifecycleHook, 0),
	}
}

// RegisterHook registers a lifecycle hook
func (lm *LifecycleManager) RegisterHook(hook LifecycleHook) {
	lm.stateMutex.Lock()
	defer lm.stateMutex.Unlock()

	lm.hooks = append(lm.hooks, hook)

	// Sort hooks by priority (lower numbers first)
	for i := len(lm.hooks) - 1; i > 0; i-- {
		if lm.hooks[i].Priority < lm.hooks[i-1].Priority {
			lm.hooks[i], lm.hooks[i-1] = lm.hooks[i-1], lm.hooks[i]
		}
	}
}

// GetState returns the current lifecycle state
func (lm *LifecycleManager) GetState() LifecycleState {
	lm.stateMutex.RLock()
	defer lm.stateMutex.RUnlock()
	return lm.state
}

// setState changes the current state (internal use)
func (lm *LifecycleManager) setState(state LifecycleState) {
	lm.stateMutex.Lock()
	defer lm.stateMutex.Unlock()
	lm.state = state
}

// GetUptime returns how long the server has been running
func (lm *LifecycleManager) GetUptime() time.Duration {
	lm.stateMutex.RLock()
	defer lm.stateMutex.RUnlock()

	if lm.state == StateRunning {
		return time.Since(lm.startTime)
	}
	if !lm.stopTime.IsZero() {
		return lm.stopTime.Sub(lm.startTime)
	}
	return 0
}

// GetLastError returns the last error that occurred during lifecycle operations
func (lm *LifecycleManager) GetLastError() error {
	lm.stateMutex.RLock()
	defer lm.stateMutex.RUnlock()
	return lm.lastError
}

// Start starts the server with lifecycle management
func (lm *LifecycleManager) Start(ctx context.Context) error {
	// Check if we can transition to starting state
	if !lm.canTransitionTo(StateStarting) {
		return fmt.Errorf("cannot start server in state: %s", lm.GetState())
	}

	lm.setState(StateStarting)
	lm.ctx, lm.cancel = context.WithCancel(ctx)
	lm.startTime = time.Now()
	lm.stopTime = time.Time{}
	lm.lastError = nil

	// Execute start hooks
	for _, hook := range lm.hooks {
		if hook.OnStart != nil {
			if err := hook.OnStart(lm.ctx); err != nil {
				lm.setError(fmt.Errorf("start hook '%s' failed: %w", hook.Name, err))
				return lm.lastError
			}
		}
	}

	// Start the server in a goroutine
	lm.wg.Add(1)
	go func() {
		defer lm.wg.Done()

		if err := lm.server.Start(); err != nil {
			lm.setError(fmt.Errorf("server start failed: %w", err))
			return
		}

		lm.setState(StateRunning)

		// Wait for context cancellation
		<-lm.ctx.Done()
		lm.setState(StateStopping)
	}()

	// Wait a moment to see if the server started successfully
	time.Sleep(100 * time.Millisecond)

	if lm.GetState() == StateError {
		return lm.GetLastError()
	}

	return nil
}

// Stop gracefully stops the server
func (lm *LifecycleManager) Stop(ctx context.Context) error {
	currentState := lm.GetState()

	if currentState == StateStopped {
		return nil // Already stopped
	}

	if !lm.canTransitionTo(StateStopping) {
		return fmt.Errorf("cannot stop server in state: %s", currentState)
	}

	lm.setState(StateStopping)
	lm.stopTime = time.Now()

	// Cancel the server context
	if lm.cancel != nil {
		lm.cancel()
	}

	// Create a timeout context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, lm.getShutdownTimeout())
	defer shutdownCancel()

	// Execute stop hooks
	for i := len(lm.hooks) - 1; i >= 0; i-- { // Reverse order for cleanup
		hook := lm.hooks[i]
		if hook.OnStop != nil {
			if err := hook.OnStop(shutdownCtx); err != nil {
				// Log error but continue shutdown
				if hook.OnError != nil {
					hook.OnError(fmt.Errorf("stop hook '%s' failed: %w", hook.Name, err))
				}
			}
		}
	}

	// Wait for server to stop gracefully or timeout
	done := make(chan struct{})
	go func() {
		lm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		lm.setState(StateStopped)
		return nil
	case <-shutdownCtx.Done():
		// Force shutdown if graceful shutdown times out
		return lm.forceShutdown()
	}
}

// Shutdown forcefully shuts down the server
func (lm *LifecycleManager) Shutdown() error {
	currentState := lm.GetState()

	if currentState == StateStopped {
		return nil // Already stopped
	}

	lm.setState(StateStopping)
	lm.stopTime = time.Now()

	// Cancel context immediately
	if lm.cancel != nil {
		lm.cancel()
	}

	return lm.forceShutdown()
}

// forceShutdown performs a forceful shutdown
func (lm *LifecycleManager) forceShutdown() error {
	// Close server listener if it exists
	if lm.server.Listener != nil {
		if err := lm.server.Listener.Close(); err != nil {
			lm.setError(fmt.Errorf("failed to close server listener: %w", err))
		}
	}

	// Force close all connections
	lm.server.Mutex.Lock()
	lm.server.Shutdown = true
	for _, conn := range lm.server.Connections {
		if conn.Conn != nil {
			conn.Conn.Close()
		}
	}
	lm.server.Mutex.Unlock()

	// Wait briefly for goroutines to cleanup
	done := make(chan struct{})
	go func() {
		lm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		lm.setState(StateStopped)
		return lm.lastError
	case <-time.After(5 * time.Second):
		// Timeout - set error state but continue
		lm.setError(fmt.Errorf("force shutdown timed out"))
		lm.setState(StateStopped)
		return lm.lastError
	}
}

// Health returns the server health status
func (lm *LifecycleManager) Health() interfaces.HealthStatus {
	state := lm.GetState()
	uptime := lm.GetUptime()
	lastError := lm.GetLastError()

	status := interfaces.HealthStatus{
		Uptime:    uptime,
		Timestamp: time.Now(),
	}

	switch state {
	case StateRunning:
		status.Status = "healthy"
	case StateStarting:
		status.Status = "starting"
	case StateStopping:
		status.Status = "stopping"
	case StateStopped:
		status.Status = "stopped"
	case StateError:
		status.Status = "unhealthy"
		if lastError != nil {
			status.Errors = []string{lastError.Error()}
		}
	default:
		status.Status = "unknown"
		status.Warnings = []string{"unknown server state"}
	}

	return status
}

// GetStats returns server statistics
func (lm *LifecycleManager) GetStats() *interfaces.ServerStats {
	lm.server.Mutex.RLock()
	defer lm.server.Mutex.RUnlock()

	connectionCount := len(lm.server.Connections)

	// Count channels and consumers across all connections
	channelCount := 0
	consumerCount := 0

	for _, conn := range lm.server.Connections {
		conn.Channels.Range(func(key, value interface{}) bool {
			channelCount++
			ch := value.(*protocol.Channel)
			ch.Mutex.RLock()
			consumerCount += len(ch.Consumers)
			ch.Mutex.RUnlock()
			return true
		})
	}

	// Get broker stats if available
	var exchangeCount, queueCount int
	if lm.server.Broker != nil {
		exchangeCount = len(lm.server.Broker.GetExchanges())
		queueCount = len(lm.server.Broker.GetQueues())
	}

	return &interfaces.ServerStats{
		Uptime:      lm.GetUptime(),
		Connections: connectionCount,
		Channels:    channelCount,
		Exchanges:   exchangeCount,
		Queues:      queueCount,
		Consumers:   consumerCount,
		// TODO: Add message counters when available
		MessagesPublished: 0,
		MessagesDelivered: 0,
		BytesReceived:     0,
		BytesSent:         0,
	}
}

// GetConnections returns information about active connections
func (lm *LifecycleManager) GetConnections() []interfaces.ConnectionInfo {
	lm.server.Mutex.RLock()
	defer lm.server.Mutex.RUnlock()

	connections := make([]interfaces.ConnectionInfo, 0, len(lm.server.Connections))

	for id, conn := range lm.server.Connections {
		remoteAddr := ""
		if conn.Conn != nil {
			remoteAddr = conn.Conn.RemoteAddr().String()
		}

		// Count channels for this connection
		channelCount := 0
		conn.Channels.Range(func(key, value interface{}) bool {
			channelCount++
			return true
		})

		connInfo := interfaces.ConnectionInfo{
			ID:            id,
			RemoteAddress: remoteAddr,
			Username:      "guest", // TODO: Add username field to Connection struct
			VirtualHost:   conn.Vhost,
			Channels:      channelCount,
			ConnectedAt:   time.Now(), // TODO: Add ConnectedAt field to Connection struct
			LastActivity:  time.Now(), // TODO: Add LastActivity field to Connection struct
		}

		connections = append(connections, connInfo)
	}

	return connections
}

// canTransitionTo checks if we can transition to the given state
func (lm *LifecycleManager) canTransitionTo(target LifecycleState) bool {
	current := lm.GetState()

	switch target {
	case StateStarting:
		return current == StateStopped
	case StateRunning:
		return current == StateStarting
	case StateStopping:
		return current == StateStarting || current == StateRunning || current == StateError
	case StateStopped:
		return current == StateStopping
	case StateError:
		return true // Can always transition to error state
	default:
		return false
	}
}

// setError sets the error state and stores the error
func (lm *LifecycleManager) setError(err error) {
	lm.stateMutex.Lock()
	defer lm.stateMutex.Unlock()

	lm.state = StateError
	lm.lastError = err

	// Notify error hooks
	for _, hook := range lm.hooks {
		if hook.OnError != nil {
			hook.OnError(err)
		}
	}
}

// getShutdownTimeout returns the shutdown timeout from configuration
func (lm *LifecycleManager) getShutdownTimeout() time.Duration {
	if lm.config != nil && lm.config.Server.CleanupIntervalMS > 0 {
		cleanupInterval := time.Duration(lm.config.Server.CleanupIntervalMS) * time.Millisecond
		return cleanupInterval * 2 // Use 2x cleanup interval as shutdown timeout
	}
	return 30 * time.Second // Default timeout
}
