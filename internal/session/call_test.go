package session

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newTestCall() *Call {
	return NewCall("test-id", DirectionInbound, quietLogger())
}

func TestState_String(t *testing.T) {
	require.Equal(t, "idle", StateIdle.String())
	require.Equal(t, "ringing", StateRinging.String())
	require.Equal(t, "ended", StateEnded.String())
	require.Contains(t, State(99).String(), "unknown")
}

func TestCanTransition_LegalTransitions(t *testing.T) {
	legal := []struct{ from, to State }{
		{StateIdle, StateRinging},
		{StateIdle, StateAnswered},
		{StateIdle, StateEnded},
		{StateRinging, StateAnswered},
		{StateRinging, StateEnded},
		{StateAnswered, StateActive},
		{StateAnswered, StateEnded},
		{StateActive, StateHeld},
		{StateActive, StateBridged},
		{StateActive, StateEnded},
		{StateHeld, StateActive},
		{StateHeld, StateEnded},
		{StateBridged, StateActive},
		{StateBridged, StateEnded},
	}
	for _, tt := range legal {
		require.True(t, canTransition(tt.from, tt.to), "%s → %s should be legal", tt.from, tt.to)
	}
}

func TestCanTransition_IllegalTransitions(t *testing.T) {
	illegal := []struct{ from, to State }{
		{StateIdle, StateActive},   // skip ringing+answered
		{StateRinging, StateActive}, // skip answered
		{StateEnded, StateActive},   // ended is terminal
		{StateEnded, StateRinging},
		{StateActive, StateRinging}, // can't go back
		{StateIdle, StateIdle},      // same state
	}
	for _, tt := range illegal {
		require.False(t, canTransition(tt.from, tt.to), "%s → %s should be illegal", tt.from, tt.to)
	}
}

func TestCall_NewCall_DefaultState(t *testing.T) {
	c := newTestCall()
	require.Equal(t, StateIdle, c.State())
	require.Equal(t, DirectionInbound, c.Direction())
	require.False(t, c.IsEnded())
	require.NotZero(t, c.CreatedAt())
	require.Equal(t, "test-id", c.ID())
}

func TestCall_SetParties(t *testing.T) {
	c := newTestCall()
	c.SetParties("sip:alice@example.com", "sip:bob@example.com")
	require.Equal(t, "sip:alice@example.com", c.From())
	require.Equal(t, "sip:bob@example.com", c.To())
}

func TestCall_SetState_LegalEmitsEvent(t *testing.T) {
	c := newTestCall()
	ch := c.Subscribe()

	require.NoError(t, c.SetState(StateRinging))
	require.Equal(t, StateRinging, c.State())

	select {
	case e := <-ch:
		require.Equal(t, EventStateChange, e.Type)
		require.Equal(t, StateIdle, e.From)
		require.Equal(t, StateRinging, e.To)
	case <-time.After(time.Second):
		t.Fatal("expected state_change event")
	}
}

func TestCall_SetState_IllegalReturnsError(t *testing.T) {
	c := newTestCall()
	err := c.SetState(StateActive) // idle → active is illegal
	require.ErrorIs(t, err, ErrInvalidTransition)
	require.Equal(t, StateIdle, c.State(), "state must not change on illegal transition")
}

func TestCall_Hangup_TransitionsToEnded(t *testing.T) {
	c := newTestCall()
	ch := c.Subscribe()

	// Walk idle → ringing → answered → active first
	require.NoError(t, c.SetState(StateRinging))
	require.NoError(t, c.SetState(StateAnswered))
	require.NoError(t, c.SetState(StateActive))

	// Drain queued state_change events
	for i := 0; i < 3; i++ {
		<-ch
	}

	require.NoError(t, c.Hangup("user_hangup"))
	require.Equal(t, StateEnded, c.State())
	require.True(t, c.IsEnded())

	// Expect: StateChange(active→ended), Hangup
	gotTypes := []EventType{}
	for e := range ch {
		gotTypes = append(gotTypes, e.Type)
	}
	require.Equal(t, []EventType{EventStateChange, EventHangup}, gotTypes)
}

func TestCall_Hangup_Idempotent(t *testing.T) {
	c := newTestCall()
	require.NoError(t, c.Hangup("first"))
	require.NoError(t, c.Hangup("second"), "second hangup must be a noop, not an error")
	require.True(t, c.IsEnded())
}

func TestCall_Subscribe_ClosedAfterHangup(t *testing.T) {
	c := newTestCall()
	ch := c.Subscribe()
	require.NoError(t, c.Hangup("done"))

	// Drain channel until close
	for range ch {
	}
	// Now reading should yield zero value (channel closed)
	_, ok := <-ch
	require.False(t, ok, "event channel must be closed after hangup")
}

func TestCall_Emit_AfterEnded_IsDropped(t *testing.T) {
	c := newTestCall()
	require.NoError(t, c.Hangup("end"))
	// Should not panic or block
	c.Emit(NewSpeechPartial("stray", 1))
}

func TestCall_Emit_BufferFull_DropsWithoutBlocking(t *testing.T) {
	c := newTestCall()
	// Fill the buffer
	for i := 0; i < EventBufferSize+10; i++ {
		c.Emit(NewSpeechPartial("x", i))
	}
	// Test passes if we got here (didn't block)
}

func TestCall_ConcurrentEmitAndStateChange(t *testing.T) {
	c := newTestCall()
	var receivedAny atomic.Int32

	go func() {
		for range c.Subscribe() {
			receivedAny.Add(1)
		}
	}()

	const writers = 8
	const eventsPerWriter = 50

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < eventsPerWriter; j++ {
				c.Emit(NewSpeechPartial("x", j))
			}
		}(i)
	}
	wg.Wait()

	require.NoError(t, c.SetState(StateRinging))
	require.NoError(t, c.Hangup("done"))

	// Allow the consumer goroutine to drain
	time.Sleep(50 * time.Millisecond)
	require.Greater(t, int(receivedAny.Load()), 0, "consumer must have received at least one event")
}

func TestCall_DirectionString(t *testing.T) {
	require.Equal(t, "inbound", DirectionInbound.String())
	require.Equal(t, "outbound", DirectionOutbound.String())
}

func TestEvent_Constructors(t *testing.T) {
	e := NewStateChange(StateIdle, StateRinging)
	require.Equal(t, EventStateChange, e.Type)
	require.Equal(t, StateIdle, e.From)
	require.Equal(t, StateRinging, e.To)
	require.False(t, e.At.IsZero())

	e2 := NewSpeechPartial("hello", 3)
	require.Equal(t, EventSpeechPartial, e2.Type)
	require.Equal(t, "hello", e2.Text)
	require.Equal(t, 3, e2.Sequence)

	e3 := NewHangup("done", 1500)
	require.Equal(t, EventHangup, e3.Type)
	require.Equal(t, "done", e3.Reason)
	require.Equal(t, int64(1500), e3.DurationMS)

	e4 := NewDTMF("#")
	require.Equal(t, "#", e4.Digit)

	e5 := NewError("boom")
	require.Equal(t, "boom", e5.ErrMessage)

	e6 := NewPlayEnd("p1", "completed", 800)
	require.Equal(t, "p1", e6.PlayID)
	require.Equal(t, int64(800), e6.PlayedMS)

	e7 := NewSpeechEnd("text", 2000)
	require.Equal(t, "text", e7.Text)
	require.Equal(t, int64(2000), e7.DurationMS)
}
