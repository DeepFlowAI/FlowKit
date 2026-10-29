package session

import (
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

// compile-time check
var _ transport.MediaIO = (*fakeio.FakeIO)(nil)

func TestCall_BindMediaIO_NilArg(t *testing.T) {
	c := newTestCall()
	require.Error(t, c.BindMediaIO(nil))
}

func TestCall_BindMediaIO_AfterEnded(t *testing.T) {
	c := newTestCall()
	require.NoError(t, c.Hangup("done"))
	require.ErrorIs(t, c.BindMediaIO(fakeio.New(4)), ErrEnded)
}

func TestCall_BindMediaIO_TwiceIsNoop(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io1 := fakeio.New(4)
	io2 := fakeio.New(4)
	require.NoError(t, c.BindMediaIO(io1))
	require.NoError(t, c.BindMediaIO(io2))
	require.Same(t, transport.MediaIO(io1), c.MediaIO(), "second bind must be ignored")
}

func TestCall_PumpIncoming_FansToBus(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io := fakeio.New(8)
	require.NoError(t, c.BindMediaIO(io))

	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	// Push 3 frames
	frames := [][]int16{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
	}
	for _, f := range frames {
		io.PushIncoming(f)
	}

	// Receive on bus subscriber
	for i := 0; i < 3; i++ {
		select {
		case got := <-sub.Channel():
			require.Equal(t, frames[i], got.Samples)
			require.Equal(t, "transport", got.Source)
		case <-time.After(time.Second):
			t.Fatalf("missing frame %d on bus", i)
		}
	}
}

func TestCall_PumpIncoming_AutoHangupOnEOF(t *testing.T) {
	c := newTestCall()
	io := fakeio.New(4)
	require.NoError(t, c.BindMediaIO(io))

	io.PushIncoming([]int16{1, 2})
	io.EndIncoming()

	// Auto-hangup should fire shortly
	require.Eventually(t, c.IsEnded, time.Second, 10*time.Millisecond,
		"closing IncomingPCM must auto-hangup the Call")
}

type eofReasonIO struct {
	*fakeio.FakeIO
	reason string
}

func (e *eofReasonIO) MediaEOFReason() string { return e.reason }

func TestCall_PumpIncoming_UsesMediaEOFReason(t *testing.T) {
	c := newTestCall()
	io := &eofReasonIO{FakeIO: fakeio.New(4), reason: "media_timeout"}
	require.NoError(t, c.BindMediaIO(io))

	ch := c.Subscribe()
	io.EndIncoming()

	require.Eventually(t, c.IsEnded, time.Second, 10*time.Millisecond)
	var hangupReason string
	deadline := time.After(time.Second)
	for hangupReason == "" {
		select {
		case ev := <-ch:
			if ev.Type == EventHangup {
				hangupReason = ev.Reason
			}
		case <-deadline:
			t.Fatal("missing hangup event")
		}
	}
	require.Equal(t, "media_timeout", hangupReason)
}

func TestCall_PumpIncoming_DefaultMediaEOFReason(t *testing.T) {
	c := newTestCall()
	io := fakeio.New(4)
	require.NoError(t, c.BindMediaIO(io))

	ch := c.Subscribe()
	io.EndIncoming()

	require.Eventually(t, c.IsEnded, time.Second, 10*time.Millisecond)
	var hangupReason string
	deadline := time.After(time.Second)
	for hangupReason == "" {
		select {
		case ev := <-ch:
			if ev.Type == EventHangup {
				hangupReason = ev.Reason
			}
		case <-deadline:
			t.Fatal("missing hangup event")
		}
	}
	require.Equal(t, "media_eof", hangupReason)
}

func TestCall_WriteOutgoing_ReachesTransport(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io := fakeio.New(8)
	require.NoError(t, c.BindMediaIO(io))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		io.CollectOutgoing()
	}()

	require.NoError(t, c.WriteOutgoing([]int16{1, 2, 3}))
	require.NoError(t, c.WriteOutgoing([]int16{4, 5, 6}))

	// Trigger Close so CollectOutgoing exits
	require.NoError(t, c.Hangup("test_end"))
	wg.Wait()

	got := io.Outgoing()
	require.Equal(t, [][]int16{{1, 2, 3}, {4, 5, 6}}, got)
}

func TestCall_RenderSink_ReceivesViaFakeIOSentTap(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io := fakeio.New(8)
	require.NoError(t, c.BindMediaIO(io))
	require.Equal(t, transport.KindFake, io.Kind())

	var mu sync.Mutex
	var taps [][]int16
	c.AddRenderSink("test", func(samples []int16) {
		cp := append([]int16(nil), samples...)
		mu.Lock()
		taps = append(taps, cp)
		mu.Unlock()
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		io.CollectOutgoing()
	}()

	require.NoError(t, c.WriteOutgoing([]int16{9, 8, 7}))
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(taps) >= 1
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, c.Hangup("end"))
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]int16{{9, 8, 7}}, taps)
}

func TestCall_WriteOutgoing_NoMediaIO(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")
	require.Error(t, c.WriteOutgoing([]int16{1, 2}))
}

func TestCall_WriteOutgoing_AfterEnded(t *testing.T) {
	c := newTestCall()
	io := fakeio.New(4)
	go io.CollectOutgoing()
	require.NoError(t, c.BindMediaIO(io))
	require.NoError(t, c.Hangup("done"))
	require.ErrorIs(t, c.WriteOutgoing([]int16{1, 2}), ErrEnded)
}

func TestCall_Hangup_ClosesMediaIO(t *testing.T) {
	c := newTestCall()

	io := fakeio.New(4)
	require.NoError(t, c.BindMediaIO(io))

	// Drain outgoing in a goroutine so Close() doesn't deadlock anyone
	go io.CollectOutgoing()

	require.NoError(t, c.Hangup("user"))

	// After Hangup, MediaIO should be closed: IncomingPCM is closed.
	select {
	case _, ok := <-io.IncomingPCM():
		require.False(t, ok, "incoming should be closed after Hangup")
	case <-time.After(time.Second):
		t.Fatal("incoming channel not closed after Hangup")
	}
}

func TestCall_AudioBus_AvailableBeforeBind(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	// Subscribe BEFORE binding — must not crash, must work after bind
	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	io := fakeio.New(4)
	require.NoError(t, c.BindMediaIO(io))
	io.PushIncoming([]int16{42})

	select {
	case f := <-sub.Channel():
		require.Equal(t, []int16{42}, f.Samples)
	case <-time.After(time.Second):
		t.Fatal("subscriber didn't receive frame")
	}
}

func TestCall_MediaTime_AdvancesWithInboundFrames(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))

	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)
	require.Equal(t, time.Duration(0), c.MediaTime())

	// 16000 samples @ 16 kHz = 1 s of media time per frame.
	frame := make([]int16, bus.SampleRate)
	pushIncoming(t, io, frame)
	select {
	case got := <-sub.Channel():
		require.Equal(t, time.Duration(0), got.Timestamp)
	case <-time.After(time.Second):
		t.Fatal("missing frame on bus")
	}
	require.Equal(t, time.Second, c.MediaTime())

	pushIncoming(t, io, frame)
	select {
	case got := <-sub.Channel():
		require.Equal(t, time.Second, got.Timestamp)
	case <-time.After(time.Second):
		t.Fatal("missing second frame on bus")
	}
	require.Equal(t, 2*time.Second, c.MediaTime())
}

func TestCall_MediaTime_DoesNotAdvanceWhileHeld(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	require.NoError(t, c.SetState(StateAnswered))
	// Unbuffered so PushIncoming returns only after pumpIncoming has observed the frame.
	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))

	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	frame := make([]int16, bus.SampleRate)
	pushIncoming(t, io, frame)
	select {
	case <-sub.Channel():
	case <-time.After(time.Second):
		t.Fatal("missing frame on bus")
	}
	require.Equal(t, time.Second, c.MediaTime())

	require.NoError(t, c.SetHeld(true))
	pushIncoming(t, io, frame)
	pushIncoming(t, io, frame)
	// Stay held until the pump has finished those iterations; otherwise
	// SetHeld(false) can race the held check and publish a just-received frame.
	require.Never(t, func() bool { return c.MediaTime() != time.Second }, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, c.SetHeld(false))
	pushIncoming(t, io, frame)
	select {
	case got := <-sub.Channel():
		require.Equal(t, time.Second, got.Timestamp)
	case <-time.After(time.Second):
		t.Fatal("missing frame after resume")
	}
	require.Equal(t, 2*time.Second, c.MediaTime())
}

// Mute is the opposite trade to hold: hold stops the media clock because the
// call is suspended, while a muted party is still in the call, so the frames
// have to keep coming and keep their timing — otherwise the recording drifts
// away from the other leg by however long the mute lasted.
func TestCall_MuteEmptiesFramesButKeepsTheClockRunning(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	require.NoError(t, c.SetState(StateAnswered))
	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))

	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	speech := make([]int16, bus.SampleRate)
	for i := range speech {
		speech[i] = 1000
	}
	pushIncoming(t, io, speech)
	require.NotZero(t, loudness(t, sub), "audio should reach the bus before muting")
	require.Equal(t, time.Second, c.MediaTime())

	require.NoError(t, c.SetMuted(true))
	require.True(t, c.IsMuted())
	pushIncoming(t, io, speech)
	require.Zero(t, loudness(t, sub), "a muted party must not be audible")
	require.Equal(t, 2*time.Second, c.MediaTime(), "the clock must keep running while muted")

	require.NoError(t, c.SetMuted(false))
	pushIncoming(t, io, speech)
	require.NotZero(t, loudness(t, sub), "unmuting should take effect on the next frame")
	require.Equal(t, 3*time.Second, c.MediaTime())
}

func TestCall_SetMutedRejectedAfterHangup(t *testing.T) {
	c := newTestCall()
	require.NoError(t, c.Hangup("end"))
	require.ErrorIs(t, c.SetMuted(true), ErrEnded)
}

// loudness reads the next frame off the bus and returns its peak amplitude.
func loudness(t *testing.T, sub *bus.Subscriber) int16 {
	t.Helper()
	select {
	case frame, ok := <-sub.Channel():
		require.True(t, ok, "bus closed")
		var peak int16
		for _, s := range frame.Samples {
			if s > peak {
				peak = s
			}
		}
		return peak
	case <-time.After(time.Second):
		t.Fatal("missing frame on bus")
		return 0
	}
}

// pushIncoming delivers one frame and waits until the pump has accepted it.
func pushIncoming(t *testing.T, io *fakeio.FakeIO, samples []int16) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		io.PushIncoming(samples)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PushIncoming timed out")
	}
}

func TestCall_TraceAttrs_IncludesMediaMs(t *testing.T) {
	c := newTestCall()
	defer c.Hangup("end")

	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))
	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	frame := make([]int16, bus.SampleRate) // 1 s
	pushIncoming(t, io, frame)
	pushIncoming(t, io, frame)
	for i := 0; i < 2; i++ {
		select {
		case <-sub.Channel():
		case <-time.After(time.Second):
			t.Fatalf("missing frame %d on bus", i)
		}
	}

	attrs := c.TraceAttrs()
	var mediaMs any
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i] == "media_ms" {
			mediaMs = attrs[i+1]
			break
		}
	}
	require.Equal(t, int64(2000), mediaMs)
}
