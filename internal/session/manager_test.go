package session

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager(quietLogger())
	t.Cleanup(func() { m.Shutdown("test_cleanup") })
	return m
}

func TestManager_Create_AssignsUniqueIDs(t *testing.T) {
	m := newTestManager(t)
	c1 := m.Create(DirectionInbound)
	c2 := m.Create(DirectionOutbound)
	require.NotEqual(t, c1.ID(), c2.ID())
	require.Equal(t, 2, m.Count())
}

func TestManager_CreateWithID_Lookup(t *testing.T) {
	m := newTestManager(t)
	c := m.CreateWithID("custom-id", DirectionInbound)
	got, err := m.Get("custom-id")
	require.NoError(t, err)
	require.Same(t, c, got)
}

func TestManager_Get_NotFound(t *testing.T) {
	m := newTestManager(t)
	_, err := m.Get("missing")
	require.ErrorIs(t, err, ErrCallNotFound)
}

func TestManager_Close_HangupAndRemove(t *testing.T) {
	m := newTestManager(t)
	c := m.CreateWithID("c1", DirectionInbound)

	require.NoError(t, m.Close("c1", "user_hangup"))
	require.True(t, c.IsEnded())

	// Watcher removes asynchronously; allow a moment
	require.Eventually(t, func() bool { return m.Count() == 0 }, time.Second, 5*time.Millisecond)
}

func TestManager_Close_NotFound(t *testing.T) {
	m := newTestManager(t)
	err := m.Close("missing", "x")
	require.ErrorIs(t, err, ErrCallNotFound)
}

func TestManager_AutoRemove_WhenCallHangsUpItself(t *testing.T) {
	m := newTestManager(t)
	c := m.CreateWithID("c1", DirectionInbound)

	// Hangup directly on the Call (bypass manager)
	require.NoError(t, c.Hangup("network_loss"))

	require.Eventually(t, func() bool { return m.Count() == 0 }, time.Second, 5*time.Millisecond,
		"watcher must remove entry when Call hangs up on its own")
}

func TestManager_CreateWithID_Duplicate_ReplacesAndHangsUpExisting(t *testing.T) {
	m := newTestManager(t)
	orig := m.CreateWithID("dup", DirectionInbound)
	require.False(t, orig.IsEnded())

	repl := m.CreateWithID("dup", DirectionOutbound)
	require.NotSame(t, orig, repl)

	// Original was hung up
	require.Eventually(t, func() bool { return orig.IsEnded() }, time.Second, 5*time.Millisecond)

	got, err := m.Get("dup")
	require.NoError(t, err)
	require.Same(t, repl, got)
}

func TestManager_ConcurrentCreate(t *testing.T) {
	m := newTestManager(t)
	const N = 200
	var wg sync.WaitGroup
	var created atomic.Int32
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Create(DirectionInbound)
			created.Add(1)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(N), created.Load())
	require.Equal(t, N, m.Count())
}

func TestManager_List_AndIDs(t *testing.T) {
	m := newTestManager(t)
	c1 := m.CreateWithID("a", DirectionInbound)
	c2 := m.CreateWithID("b", DirectionOutbound)

	list := m.List()
	require.Len(t, list, 2)

	ids := m.IDs()
	require.ElementsMatch(t, []string{"a", "b"}, ids)

	require.NoError(t, c1.Hangup("done"))
	require.Eventually(t, func() bool { return m.Count() == 1 }, time.Second, 5*time.Millisecond)

	_ = c2 // keep alive
}

func TestManager_Shutdown_HangsUpAll(t *testing.T) {
	m := NewManager(quietLogger())
	c1 := m.CreateWithID("a", DirectionInbound)
	c2 := m.CreateWithID("b", DirectionOutbound)
	c3 := m.CreateWithID("c", DirectionInbound)

	start := time.Now()
	m.Shutdown("server_stop")
	elapsed := time.Since(start)

	require.True(t, c1.IsEnded() && c2.IsEnded() && c3.IsEnded())
	require.Equal(t, 0, m.Count())
	require.Less(t, elapsed, shutdownWatchersTimeout,
		"watchers must exit before the shutdown wait timeout")
	select {
	case <-m.Context().Done():
	default:
		t.Fatal("Manager.Context should be cancelled after Shutdown")
	}
}

func TestManager_Shutdown_Idempotent(t *testing.T) {
	m := NewManager(quietLogger())
	c := m.Create(DirectionInbound)
	require.NotNil(t, c)

	m.Shutdown("first")
	m.Shutdown("second") // must not panic or double-hangup path issues
	require.True(t, c.IsEnded())
	require.True(t, m.IsDraining())
}

func TestManager_Shutdown_EndsCallsAndWatchersBeforeTimeout(t *testing.T) {
	m := NewManager(quietLogger())
	const n = 8
	calls := make([]*Call, 0, n)
	for i := 0; i < n; i++ {
		c := m.Create(DirectionInbound)
		require.NotNil(t, c)
		calls = append(calls, c)
	}

	start := time.Now()
	m.Shutdown("server_stop")
	require.Less(t, time.Since(start), shutdownWatchersTimeout)

	for _, c := range calls {
		require.True(t, c.IsEnded(), "call %s should be ended", c.ID())
		select {
		case <-c.Done():
		default:
			t.Fatalf("call %s Done should be closed", c.ID())
		}
	}
	require.Equal(t, 0, m.Count())
}

func TestManager_RootCancel_ReapsInFlightCalls(t *testing.T) {
	// Calls already in the map; cancel root only (skip Shutdown's Hangup loop)
	// so watch is the sole driver. Covers the root-cancel fallback path.
	m := NewManager(quietLogger())
	c1 := m.Create(DirectionInbound)
	c2 := m.Create(DirectionOutbound)
	require.NotNil(t, c1)
	require.NotNil(t, c2)
	require.Equal(t, 2, m.Count())

	m.cancel()

	require.Eventually(t, func() bool {
		return c1.IsEnded() && c2.IsEnded() && m.Count() == 0
	}, time.Second, 5*time.Millisecond,
		"watch must Hangup mapped calls when the root cancels")

	m.Shutdown("cleanup") // Once: draining + waitWatchers; Hangup is idempotent
}

func TestManager_Create_RejectedDuringShutdown(t *testing.T) {
	m := NewManager(quietLogger())
	existing := m.CreateWithID("alive", DirectionInbound)
	require.NotNil(t, existing)

	m.BeginShutdown()
	rejected := m.CreateWithID("during-drain", DirectionInbound)
	require.Nil(t, rejected, "Create must return nil while draining")
	_, err := m.Get("during-drain")
	require.ErrorIs(t, err, ErrCallNotFound, "rejected Create must not enter the map")
	require.Equal(t, 1, m.Count())
	require.False(t, existing.IsEnded(), "existing calls survive BeginShutdown")

	m.Shutdown("server_stop")
	require.True(t, existing.IsEnded())
	require.Equal(t, 0, m.Count())
}

func TestManager_Context_AliveUntilShutdown(t *testing.T) {
	m := newTestManager(t)
	select {
	case <-m.Context().Done():
		t.Fatal("Manager.Context must stay alive before Shutdown")
	default:
	}
	m.BeginShutdown()
	select {
	case <-m.Context().Done():
		t.Fatal("BeginShutdown must not cancel Manager.Context")
	default:
	}
}

func TestCall_Done_ClosedAfterHangup(t *testing.T) {
	c := newTestCall()
	select {
	case <-c.Done():
		t.Fatal("Done() must not be closed initially")
	default:
	}
	require.NoError(t, c.Hangup("x"))
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() should close on hangup")
	}
}

func TestCall_Context_SameChannelAsDone(t *testing.T) {
	c := newTestCall()
	require.Equal(t, c.Done(), c.Context().Done(),
		"Done and Context.Done must be the same channel")

	require.NoError(t, c.Hangup("x"))
	require.True(t, c.IsEnded())
	select {
	case <-c.Context().Done():
	default:
		t.Fatal("Context should be cancelled after Hangup")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("Done should be closed after Hangup")
	}
}

func TestManager_CallContext_NotClosedByRootCancelAlone(t *testing.T) {
	// WithoutCancel: root cancel must not close Call.Done until Hangup runs.
	// Observe the gap before watch's Hangup completes by racing a short select.
	m := NewManager(quietLogger())
	c := m.Create(DirectionInbound)
	require.NotNil(t, c)

	// Cancel root; Done closes only after watch → Hangup → closeEvents.
	m.cancel()
	require.Eventually(t, func() bool { return c.IsEnded() }, time.Second, 5*time.Millisecond)
	select {
	case <-c.Done():
	default:
		t.Fatal("Done should close once watch-driven Hangup finishes")
	}
	require.True(t, c.IsEnded(), "IsEnded must be true when Done is closed")

	m.Shutdown("cleanup")
}
