package storage

// The crash-model transport mapping is platform-neutral: crash_model has no
// effect off darwin, but WALConfigFromEngine must still resolve the engine's
// CrashModel to the right WALConfig.ConfirmSync everywhere, and the zero value
// must stay the barrier.

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
)

func TestWALConfigFromEngine_CrashModelMapping(t *testing.T) {
	// Omitted / zero value / "barrier" all mean the barrier (the zero value of
	// walConfirmSync), so a zero-value EngineConfig{} stays safe by default.
	for _, model := range []string{"", interfaces.CrashModelBarrier} {
		cfg := WALConfigFromEngine(interfaces.EngineConfig{CrashModel: model})
		assert.Equal(t, walConfirmBarrier, cfg.ConfirmSync,
			"engine CrashModel %q must map to the barrier", model)
	}

	// Only the explicit "force_flush" selects the full drive-cache flush.
	cfg := WALConfigFromEngine(interfaces.EngineConfig{CrashModel: interfaces.CrashModelForceFlush})
	assert.Equal(t, walConfirmForceFlush, cfg.ConfirmSync,
		"engine CrashModel %q must map to the full flush", interfaces.CrashModelForceFlush)

	// The zero value of the enum IS the barrier, so DefaultWALConfig() and a
	// zero-value WALConfig{} stay on the shipped default.
	assert.Equal(t, walConfirmBarrier, WALConfig{}.ConfirmSync)
	assert.Equal(t, walConfirmBarrier, DefaultWALConfig().ConfirmSync)
}
