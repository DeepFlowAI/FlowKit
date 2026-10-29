package session

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager_BeginShutdown_RejectsNewCreate(t *testing.T) {
	m := newTestManager(t)

	c := m.Create(DirectionInbound)
	require.NotNil(t, c, "Create should succeed before shutdown")
	require.False(t, m.IsDraining())

	m.BeginShutdown()
	require.True(t, m.IsDraining())

	c2 := m.Create(DirectionInbound)
	require.Nil(t, c2, "Create must return nil once draining")
}

func TestManager_BeginShutdown_ExistingCallsContinue(t *testing.T) {
	m := newTestManager(t)
	c := m.Create(DirectionInbound)
	require.NotNil(t, c)

	m.BeginShutdown()

	// Existing call survives until explicit hangup
	require.False(t, c.IsEnded())
	require.NoError(t, c.Hangup("done"))
	require.True(t, c.IsEnded())
}

func TestManager_BeginShutdown_Idempotent(t *testing.T) {
	m := newTestManager(t)
	m.BeginShutdown()
	m.BeginShutdown()
	require.True(t, m.IsDraining())
}

func TestManager_Shutdown_AlsoHangsUp(t *testing.T) {
	// Original Shutdown(reason) still hangs up everything.
	m := NewManager(quietLogger())
	c := m.Create(DirectionInbound)
	require.NotNil(t, c)

	m.Shutdown("test_stop")
	require.True(t, c.IsEnded())
}
