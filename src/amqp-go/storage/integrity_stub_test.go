package storage

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

// interfacesEngineConfigZero is a zero EngineConfig (all defaults).
func interfacesEngineConfigZero() interfaces.EngineConfig {
	return interfaces.EngineConfig{}
}

// WS-C C6: ValidateStorageIntegrity/RepairCorruption returned an empty
// SUCCESS, which reports a validation that never ran. A real implementation
// would have to scan every WAL file and segment twice per boot (the
// classified recovery scan that follows does the work once), so the honest
// answer is "not implemented" — fail closed — and the server seam treats it
// as "capability unavailable", never as success and never as corruption.

// TestValidateStorageIntegrity_FailsClosed (TDD test 22).
func TestValidateStorageIntegrity_FailsClosed(t *testing.T) {
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfacesEngineConfigZero())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	stats, verr := ds.ValidateStorageIntegrity()
	require.NotNil(t, stats, "the stats pointer must be non-nil so a caller that ignores the error cannot nil-deref")
	require.Error(t, verr, "an empty success would claim a validation that never ran")
	require.True(t, errors.Is(verr, ErrNotImplemented),
		"the error must be recognisable as ErrNotImplemented (capability unavailable), got %v", verr)

	rstats, rerr := ds.RepairCorruption(true)
	require.NotNil(t, rstats)
	require.Error(t, rerr)
	require.True(t, errors.Is(rerr, ErrNotImplemented),
		"the error must be recognisable as ErrNotImplemented (capability unavailable), got %v", rerr)
}
