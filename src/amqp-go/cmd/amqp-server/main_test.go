package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestVersion_Stamps(t *testing.T) {
	// Spec 15.12: the assignment below fails to compile if version, commit,
	// or date is a const — which is exactly the -ldflags -X precondition.
	origV, origC, origD := version, commit, date
	t.Cleanup(func() { version, commit, date = origV, origC, origD })

	version, commit, date = "9.9.9", "abc1234", "2026-01-02T03:04:05Z"
	assert.Equal(t,
		"amqp-server 9.9.9 (commit abc1234, built 2026-01-02T03:04:05Z, "+runtime.Version()+")",
		versionString())
}

func TestStartup_NonLoopback_AuthDisabled_Warns(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Network.Address = "0.0.0.0:5672"

	core, logs := observer.New(zap.WarnLevel)
	var buf bytes.Buffer
	emitSecurityWarnings(&buf, zap.New(core), cfg.SecurityWarnings())

	assert.Contains(t, buf.String(), "SECURITY WARNING")
	assert.Contains(t, buf.String(), "authentication DISABLED")

	entries := logs.FilterMessageSnippet("authentication DISABLED").All()
	require.Len(t, entries, 1)
	assert.Equal(t, "startup-security", entries[0].ContextMap()["component"])

	t.Run("default config is silent", func(t *testing.T) {
		var buf bytes.Buffer
		core, logs := observer.New(zap.WarnLevel)
		emitSecurityWarnings(&buf, zap.New(core), config.DefaultConfig().SecurityWarnings())
		assert.Empty(t, buf.String())
		assert.Equal(t, 0, logs.Len())
	})
}

func TestConfig_Order_FlagsOverrideEnvOverrideFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`server:
  loglevel: warn
storage:
  path: /f
telemetry:
  port: 9500
network:
  maxconnections: 7
`), 0600))

	t.Setenv("AMQP_SERVER_LOGLEVEL", "error")
	t.Setenv("AMQP_STORAGE_PATH", "/e")

	cfg, _, err := config.LoadConfig(path)
	require.NoError(t, err)

	fs, _ := newFlagSet()
	require.NoError(t, fs.Parse([]string{"--telemetry-port", "9600"}))
	require.NoError(t, applyFlagOverrides(cfg, fs))

	assert.Equal(t, "error", cfg.Server.LogLevel)
	assert.Equal(t, "/e", cfg.Storage.Path)
	assert.Equal(t, 9600, cfg.Telemetry.Port)
	assert.Equal(t, 7, cfg.Network.MaxConnections)

	t.Run("unset flag defaults do not clobber config", func(t *testing.T) {
		cfg, _, err := config.LoadConfig(path)
		require.NoError(t, err)
		fs, _ := newFlagSet()
		require.NoError(t, fs.Parse(nil))
		require.NoError(t, applyFlagOverrides(cfg, fs))
		assert.Equal(t, 9500, cfg.Telemetry.Port)
	})
}

func TestConfig_UnknownKey_WarnsOrErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server:\n  memorylimitpercent: 60\n"), 0600))
	_, report, err := config.LoadConfig(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	printLoadReport(&buf, report)
	assert.Contains(t, buf.String(), `unknown config key "server.memorylimitpercent"`)
}

func TestReadiness_Func(t *testing.T) {
	var shutting atomic.Bool
	listening := true
	var healthErr error

	ready := readyFunc(
		func() bool { return listening },
		func() error { return healthErr },
		&shutting,
	)
	assert.NoError(t, ready())

	listening = false
	require.Error(t, ready())
	assert.Contains(t, ready().Error(), "not bound")
	listening = true

	healthErr = errors.New("recovery still running")
	assert.ErrorIs(t, ready(), healthErr)
	healthErr = nil

	shutting.Store(true)
	require.Error(t, ready())
	assert.Contains(t, ready().Error(), "shutting down")
}

type fakeQueueLister struct {
	queues    map[string]*protocol.Queue
	consumers map[string]*protocol.Consumer
	ready     map[string]uint32
}

func (f fakeQueueLister) GetQueues() map[string]*protocol.Queue       { return f.queues }
func (f fakeQueueLister) GetConsumers() map[string]*protocol.Consumer { return f.consumers }

type fakeQueueListerReady struct {
	fakeQueueLister
	ready map[string]uint32
}

func (f fakeQueueListerReady) QueueReadyCount(name string) uint32 { return f.ready[name] }

func TestQueueStatsProvider(t *testing.T) {
	base := fakeQueueLister{
		queues: map[string]*protocol.Queue{
			"q1": {Name: "q1"},
			"q2": {Name: "q2"},
		},
		consumers: map[string]*protocol.Consumer{
			"c1": {Tag: "c1", Queue: "q1"},
			"c2": {Tag: "c2", Queue: "q1"},
			"c3": {Tag: "c3", Queue: "q2"},
		},
	}
	stats := queueStatsFrom(base)
	require.Len(t, stats, 2)
	byName := map[string]metrics.QueueStat{}
	for _, s := range stats {
		byName[s.Name] = s
	}
	assert.Equal(t, 2, byName["q1"].Consumers)
	assert.Equal(t, 1, byName["q2"].Consumers)
	assert.False(t, byName["q1"].ReadyKnown, "no QueueReadyCount implementation")

	withReady := fakeQueueListerReady{fakeQueueLister: base, ready: map[string]uint32{"q1": 5}}
	stats = queueStatsFrom(withReady)
	for _, s := range stats {
		if s.Name == "q1" {
			assert.True(t, s.ReadyKnown)
			assert.Equal(t, 5, s.Ready)
			assert.Equal(t, "/", s.VHost)
		}
	}
}

func TestDaemonReadiness_PipeProtocol(t *testing.T) {
	assert.NoError(t, waitDaemonReady(strings.NewReader("READY\n")))
	assert.Error(t, waitDaemonReady(strings.NewReader("")))
	assert.Error(t, waitDaemonReady(strings.NewReader("garbage\n")))
}

func TestFlagSet_HasDocumentedFlags(t *testing.T) {
	fs, _ := newFlagSet()
	for _, name := range []string{
		"config", "version", "generate-config", "tls", "tls-cert", "tls-key",
		"tls-ca", "unsafe-recovery", "enable-telemetry", "telemetry-port",
		"telemetry-address", "enable-pprof", "healthcheck",
	} {
		assert.NotNil(t, fs.Lookup(name), "missing flag --%s", name)
	}
}

func TestApplyFlagOverrides_TLSAndRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	fs, _ := newFlagSet()
	require.NoError(t, fs.Parse([]string{
		"--tls", "--tls-cert", "c.pem", "--tls-key", "k.pem", "--tls-ca", "ca.pem",
		"--unsafe-recovery", "--telemetry-address", "0.0.0.0", "--enable-pprof",
	}))
	require.NoError(t, applyFlagOverrides(cfg, fs))
	assert.True(t, cfg.Security.TLSEnabled)
	assert.Equal(t, "c.pem", cfg.Security.TLSCertFile)
	assert.True(t, cfg.Storage.UnsafeRecovery)
	assert.Equal(t, "0.0.0.0", cfg.Telemetry.Address)
	assert.True(t, cfg.Telemetry.PprofEnabled)
}

func TestAlarmAnnotation(t *testing.T) {
	assert.Equal(t, "", alarmAnnotation(0))
	assert.Equal(t, "resource alarm active: memory", alarmAnnotation(1))
	assert.Equal(t, "resource alarm active: disk", alarmAnnotation(2))
	assert.Equal(t, "resource alarm active: memory,disk", alarmAnnotation(3))
}

func TestShutdownServer_Fallback(t *testing.T) {
	// The concrete Server in this worktree has no Shutdown(ctx) yet (WS-B
	// seam); shutdownServer must compile and fall back to Stop. The interface
	// assertion is the merge point.
	var called bool
	err := shutdownWith(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
}

func TestHealthcheckAddress(t *testing.T) {
	assert.Equal(t, "http://127.0.0.1:9419/health", healthcheckURL("127.0.0.1", 9419))
	assert.Equal(t, "http://127.0.0.1:9419/health", healthcheckURL("0.0.0.0", 9419))
	assert.Equal(t, "http://127.0.0.1:9419/health", healthcheckURL("", 9419))
	assert.Equal(t, "http://127.0.0.1:9419/health", healthcheckURL("::", 9419))
	assert.Equal(t, "http://[::1]:9419/health", healthcheckURL("::1", 9419))
}
