package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLifecycleManager(t *testing.T) {
	server := &Server{}
	config := config.DefaultConfig()

	lm := NewLifecycleManager(server, config)

	assert.NotNil(t, lm)
	assert.Equal(t, server, lm.server)
	assert.Equal(t, config, lm.config)
	assert.Equal(t, StateStopped, lm.GetState())
	assert.Empty(t, lm.hooks)
}

func TestLifecycleStateString(t *testing.T) {
	states := map[LifecycleState]string{
		StateStopped:  "stopped",
		StateStarting: "starting",
		StateRunning:  "running",
		StateStopping: "stopping",
		StateError:    "error",
	}

	for state, expected := range states {
		assert.Equal(t, expected, state.String())
	}

	// Test unknown state
	unknownState := LifecycleState(999)
	assert.Equal(t, "unknown", unknownState.String())
}

func TestRegisterHook(t *testing.T) {
	lm := NewLifecycleManager(&Server{}, config.DefaultConfig())

	hook1 := LifecycleHook{Name: "hook1", Priority: 10}
	hook2 := LifecycleHook{Name: "hook2", Priority: 5}
	hook3 := LifecycleHook{Name: "hook3", Priority: 15}

	lm.RegisterHook(hook1)
	lm.RegisterHook(hook2)
	lm.RegisterHook(hook3)

	// Hooks should be sorted by priority (ascending)
	assert.Len(t, lm.hooks, 3)
	assert.Equal(t, "hook2", lm.hooks[0].Name) // Priority 5
	assert.Equal(t, "hook1", lm.hooks[1].Name) // Priority 10
	assert.Equal(t, "hook3", lm.hooks[2].Name) // Priority 15
}

func TestCanTransitionTo(t *testing.T) {
	lm := NewLifecycleManager(&Server{}, config.DefaultConfig())

	tests := []struct {
		from          LifecycleState
		to            LifecycleState
		canTransition bool
	}{
		{StateStopped, StateStarting, true},
		{StateStopped, StateRunning, false},
		{StateStarting, StateRunning, true},
		{StateStarting, StateStopping, true},
		{StateRunning, StateStopping, true},
		{StateStopping, StateStopped, true},
		{StateError, StateStopping, true},
		{StateRunning, StateStarting, false},
	}

	for _, tt := range tests {
		lm.setState(tt.from)
		result := lm.canTransitionTo(tt.to)
		assert.Equal(t, tt.canTransition, result,
			"transition from %s to %s should be %v", tt.from, tt.to, tt.canTransition)
	}
}

func TestLifecycleHooks(t *testing.T) {
	server := &Server{
		Connections: make(map[string]*protocol.Connection),
	}
	lm := NewLifecycleManager(server, config.DefaultConfig())

	var callOrder []string
	var mu sync.Mutex

	startHook := LifecycleHook{
		Name:     "start_hook",
		Priority: 1,
		OnStart: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			callOrder = append(callOrder, "start")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			callOrder = append(callOrder, "stop")
			return nil
		},
		OnError: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			callOrder = append(callOrder, "error")
		},
	}

	lm.RegisterHook(startHook)

	// Test hook registration
	mu.Lock()
	assert.Len(t, lm.hooks, 1)
	assert.Equal(t, "start_hook", lm.hooks[0].Name)
	mu.Unlock()

	// Test that we can register hooks
	assert.True(t, startHook.OnStart != nil)
	assert.True(t, startHook.OnStop != nil)
	assert.True(t, startHook.OnError != nil)
}

func TestGetUptime(t *testing.T) {
	lm := NewLifecycleManager(&Server{}, config.DefaultConfig())

	// Initially no uptime
	assert.Equal(t, time.Duration(0), lm.GetUptime())

	// Set start time and running state
	lm.startTime = time.Now().Add(-5 * time.Minute)
	lm.setState(StateRunning)

	uptime := lm.GetUptime()
	assert.True(t, uptime >= 4*time.Minute && uptime <= 6*time.Minute)

	// Set stop time
	lm.stopTime = lm.startTime.Add(3 * time.Minute)
	lm.setState(StateStopped)

	uptime = lm.GetUptime()
	assert.Equal(t, 3*time.Minute, uptime)
}

func TestHealth(t *testing.T) {
	lm := NewLifecycleManager(&Server{}, config.DefaultConfig())

	tests := []struct {
		state          LifecycleState
		expectedStatus string
		setError       bool
	}{
		{StateRunning, "healthy", false},
		{StateStarting, "starting", false},
		{StateStopping, "stopping", false},
		{StateStopped, "stopped", false},
		{StateError, "unhealthy", true},
	}

	for _, tt := range tests {
		lm.setState(tt.state)

		if tt.setError {
			lm.setError(assert.AnError)
		}

		health := lm.Health()
		assert.Equal(t, tt.expectedStatus, health.Status)
		assert.NotZero(t, health.Timestamp)

		if tt.setError {
			assert.NotEmpty(t, health.Errors)
		}
	}
}

func TestGetStats(t *testing.T) {
	server := &Server{
		Connections: make(map[string]*protocol.Connection),
		Broker:      NewOriginalBrokerAdapter(broker.NewBroker()),
	}
	lm := NewLifecycleManager(server, config.DefaultConfig())

	// Set some uptime
	lm.startTime = time.Now().Add(-10 * time.Minute)
	lm.setState(StateRunning)

	stats := lm.GetStats()

	assert.NotNil(t, stats)
	assert.True(t, stats.Uptime >= 9*time.Minute)
	assert.Equal(t, 0, stats.Connections)
	// Broker should have some default exchanges/queues
	assert.GreaterOrEqual(t, stats.Exchanges, 0)
	assert.GreaterOrEqual(t, stats.Queues, 0)
}

func TestGetConnections(t *testing.T) {
	server := &Server{
		Connections: make(map[string]*protocol.Connection),
	}
	lm := NewLifecycleManager(server, config.DefaultConfig())

	// Initially no connections
	connections := lm.GetConnections()
	assert.Empty(t, connections)

	// Add a mock connection
	mockConn := &protocol.Connection{
		ID:       "test-conn-1",
		Vhost:    "/",
		Channels: make(map[uint16]*protocol.Channel),
	}

	server.Connections["test-conn-1"] = mockConn

	connections = lm.GetConnections()
	assert.Len(t, connections, 1)
	assert.Equal(t, "test-conn-1", connections[0].ID)
	assert.Equal(t, "guest", connections[0].Username)
	assert.Equal(t, "/", connections[0].VirtualHost)
}

func TestSetError(t *testing.T) {
	lm := NewLifecycleManager(&Server{}, config.DefaultConfig())

	var errorCalled bool
	var capturedError error

	hook := LifecycleHook{
		Name: "error_hook",
		OnError: func(err error) {
			errorCalled = true
			capturedError = err
		},
	}
	lm.RegisterHook(hook)

	testError := assert.AnError
	lm.setError(testError)

	assert.Equal(t, StateError, lm.GetState())
	assert.Equal(t, testError, lm.GetLastError())
	assert.True(t, errorCalled)
	assert.Equal(t, testError, capturedError)
}

func TestShutdownTimeout(t *testing.T) {
	config := config.DefaultConfig()
	config.Server.CleanupInterval = 5 * time.Second

	lm := NewLifecycleManager(&Server{}, config)

	timeout := lm.getShutdownTimeout()
	assert.Equal(t, 10*time.Second, timeout) // 2x cleanup interval

	// Test default timeout
	lm.config.Server.CleanupInterval = 0
	timeout = lm.getShutdownTimeout()
	assert.Equal(t, 30*time.Second, timeout)
}

func TestLifecycleStateTransitions(t *testing.T) {
	server := &Server{
		Connections: make(map[string]*protocol.Connection),
	}
	lm := NewLifecycleManager(server, config.DefaultConfig())

	// Test invalid transitions
	lm.setState(StateRunning)

	ctx := context.Background()
	err := lm.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot start server in state: running")

	// Test valid transitions
	lm.setState(StateStopped)

	// Just test the state transition logic
	assert.True(t, lm.canTransitionTo(StateStarting))
	assert.False(t, lm.canTransitionTo(StateRunning))
}

func TestLifecycleManagerIntegration(t *testing.T) {
	// Create a server using the builder
	server, err := NewServerBuilder().
		WithAddress(":0").      // Use port 0 to get any available port
		WithZapLogger("error"). // Reduce log noise
		Build()

	require.NoError(t, err)
	require.NotNil(t, server.Lifecycle)

	// Test initial state
	assert.Equal(t, StateStopped, server.Lifecycle.GetState())

	// Test health when stopped
	health := server.Lifecycle.Health()
	assert.Equal(t, "stopped", health.Status)

	// Test stats when stopped
	stats := server.Lifecycle.GetStats()
	assert.NotNil(t, stats)
	assert.Equal(t, time.Duration(0), stats.Uptime)
}
