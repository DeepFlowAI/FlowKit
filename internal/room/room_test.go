package room

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// legFixture is one activated call wired to a FakeIO whose outgoing frames are
// collected for assertions.
type legFixture struct {
	call *session.Call
	io   *fakeio.FakeIO
}

func newLeg(t *testing.T, mgr *session.Manager, dir session.Direction) *legFixture {
	t.Helper()
	call := mgr.Create(dir)
	io := fakeio.New(256)
	require.NoError(t, call.BindMediaIO(io))
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))
	go io.CollectOutgoing()
	return &legFixture{call: call, io: io}
}

// constFrame is a 20 ms frame filled with a constant value.
func constFrame(v int16) []int16 {
	f := make([]int16, bus.SamplesPerFrame)
	for i := range f {
		f[i] = v
	}
	return f
}

// pumpIncoming feeds a leg's incoming frame repeatedly until stop closes, so
// the leg's capture slot always has a fresh frame for the mixer to read.
func pumpIncoming(io *fakeio.FakeIO, v int16, stop <-chan struct{}) {
	f := constFrame(v)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			io.PushIncoming(f)
		}
	}
}

// lastNonZeroOutgoing returns the most recent outgoing frame's first sample,
// skipping empty snapshots. ok=false when nothing has been written yet.
func firstSampleOfLatest(io *fakeio.FakeIO) (int16, bool) {
	out := io.Outgoing()
	if len(out) == 0 {
		return 0, false
	}
	last := out[len(out)-1]
	if len(last) == 0 {
		return 0, false
	}
	return last[0], true
}

func TestRoom_ThreeWay_SumsOtherLegs(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)
	c := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.NoError(t, rm.AddLeg(r.ID(), b.call))
	require.NoError(t, rm.AddLeg(r.ID(), c.call))
	t.Cleanup(func() { rm.Close(r.ID(), "cleanup") })

	stop := make(chan struct{})
	go pumpIncoming(a.io, 100, stop)
	go pumpIncoming(b.io, 200, stop)
	go pumpIncoming(c.io, 400, stop)
	defer close(stop)

	// A should hear B+C = 200+400 = 600.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(a.io)
		return ok && v == 600
	}, 3*time.Second, 20*time.Millisecond, "A should mix B+C")

	// B should hear A+C = 100+400 = 500.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(b.io)
		return ok && v == 500
	}, 3*time.Second, 20*time.Millisecond, "B should mix A+C")

	// C should hear A+B = 100+200 = 300.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(c.io)
		return ok && v == 300
	}, 3*time.Second, 20*time.Millisecond, "C should mix A+B")

	rm.Close(r.ID(), "test_done")
}

// pumpJustAheadOfTheMixer feeds frames slightly faster than the 20 ms mixer
// tick. That is enough supply for one listener but not for two sharing a
// speaker, so a mixer that consumes what it reads starves its peer — while the
// small surplus keeps the two independent 20 ms clocks from drifting into
// false failures.
func pumpJustAheadOfTheMixer(io *fakeio.FakeIO, v int16, stop <-chan struct{}) {
	f := constFrame(v)
	tick := time.NewTicker(bus.FrameDuration * 3 / 4)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			io.PushIncoming(f)
		}
	}
}

// One speaker, two listeners: both must keep hearing them. A mixer that
// consumed the speaker's frame would leave the other listener with nothing to
// mix, and a listener with nothing to mix is sent no audio at all — heard as
// chopped, stuttering speech that clears up the moment the third party leaves.
func TestRoom_ThreeWay_BothListenersHearTheSameSpeaker(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	speaker := newLeg(t, mgr, session.DirectionInbound)
	listenerA := newLeg(t, mgr, session.DirectionOutbound)
	listenerB := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), speaker.call))
	require.NoError(t, rm.AddLeg(r.ID(), listenerA.call))
	require.NoError(t, rm.AddLeg(r.ID(), listenerB.call))
	t.Cleanup(func() { rm.Close(r.ID(), "cleanup") })

	stop := make(chan struct{})
	go pumpJustAheadOfTheMixer(speaker.io, 1000, stop)
	const window = time.Second
	time.Sleep(window)
	close(stop)

	// Sent frames, not their contents: a starved mixer skips the write entirely.
	ticks := int(window / bus.FrameDuration)
	floor := ticks * 9 / 10
	gotA := len(listenerA.io.Outgoing())
	gotB := len(listenerB.io.Outgoing())
	require.GreaterOrEqualf(t, gotA, floor,
		"listener A got %d of ~%d frames — the speaker's audio is being taken by another listener", gotA, ticks)
	require.GreaterOrEqualf(t, gotB, floor,
		"listener B got %d of ~%d frames — the speaker's audio is being taken by another listener", gotB, ticks)
}

func TestFrameSlot_BurstFramesRemainAvailableAcrossMixerTicks(t *testing.T) {
	slot := newFrameSlot()
	slot.set(constFrame(100))
	slot.set(constFrame(200))

	first, seen := slot.read(0)
	second, seen := slot.read(seen)
	third, _ := slot.read(seen)

	require.Equal(t, int16(100), first[0])
	require.Equal(t, int16(200), second[0])
	require.Nil(t, third)
}

func TestRoom_Whisper_DirectionalGain(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	agent := newLeg(t, mgr, session.DirectionInbound)
	customer := newLeg(t, mgr, session.DirectionOutbound)
	supervisor := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), agent.call))
	require.NoError(t, rm.AddLeg(r.ID(), customer.call))
	require.NoError(t, rm.AddLeg(r.ID(), supervisor.call))
	t.Cleanup(func() { rm.Close(r.ID(), "cleanup") })

	// Whisper: supervisor is audible to the agent only. Mute supervisor→customer.
	require.NoError(t, rm.SetMixGain(r.ID(), customer.call.ID(), supervisor.call.ID(), 0))

	stop := make(chan struct{})
	go pumpIncoming(agent.io, 10, stop)
	go pumpIncoming(customer.io, 20, stop)
	go pumpIncoming(supervisor.io, 4000, stop)
	defer close(stop)

	// Agent hears customer + supervisor = 20 + 4000 = 4020.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(agent.io)
		return ok && v == 4020
	}, 3*time.Second, 20*time.Millisecond, "agent hears supervisor")

	// Customer hears agent only (supervisor muted) = 10.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(customer.io)
		return ok && v == 10
	}, 3*time.Second, 20*time.Millisecond, "customer must NOT hear supervisor")
}

func TestRoom_Replace_Takeover(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	customer := newLeg(t, mgr, session.DirectionInbound)
	agent := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), customer.call))
	require.NoError(t, rm.AddLeg(r.ID(), agent.call))
	t.Cleanup(func() { rm.Close(r.ID(), "cleanup") })

	stop := make(chan struct{})
	go pumpIncoming(customer.io, 30, stop)
	go pumpIncoming(agent.io, 70, stop)
	defer close(stop)

	// Customer hears the agent (70) before takeover.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(customer.io)
		return ok && v == 70
	}, 3*time.Second, 20*time.Millisecond)

	// Supervisor takes over the agent leg.
	supervisor := newLeg(t, mgr, session.DirectionOutbound)
	go pumpIncoming(supervisor.io, 90, stop)
	require.NoError(t, rm.ReplaceLeg(r.ID(), agent.call.ID(), supervisor.call))

	// Customer now hears the supervisor (90) — takeover, inherited relationship.
	require.Eventually(t, func() bool {
		v, ok := firstSampleOfLatest(customer.io)
		return ok && v == 90
	}, 3*time.Second, 20*time.Millisecond, "customer should hear supervisor after takeover")

	// Old agent leg no longer receives mixed output. Record count, wait, assert
	// it does not grow (its mix goroutine stopped).
	before := len(agent.io.Outgoing())
	time.Sleep(200 * time.Millisecond)
	require.LessOrEqual(t, len(agent.io.Outgoing())-before, 1, "old leg output must stop after replace")
}

func TestRoom_AddRemove_DestroysWhenEmpty(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)
	c := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.NoError(t, rm.AddLeg(r.ID(), b.call))
	require.NoError(t, rm.AddLeg(r.ID(), c.call))
	require.Equal(t, 3, r.LegCount())

	require.NoError(t, rm.RemoveLeg(r.ID(), c.call.ID()))
	require.Equal(t, 2, r.LegCount())
	require.Equal(t, 1, rm.Count(), "room still alive with two legs")

	// Removing down to one leg auto-destroys the room WITHOUT hanging up the
	// survivor.
	require.NoError(t, rm.RemoveLeg(r.ID(), b.call.ID()))
	require.Eventually(t, func() bool { return rm.Count() == 0 }, time.Second, 10*time.Millisecond)
	require.False(t, a.call.IsEnded(), "surviving leg must not be hung up")
	require.False(t, b.call.IsEnded())
}

func TestRoom_LegEnd_DetachesAndDestroys(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.NoError(t, rm.AddLeg(r.ID(), b.call))

	// Leg b ends on its own → watcher detaches it → room drops to 1 → destroyed.
	require.NoError(t, mgr.Close(b.call.ID(), "peer_gone"))
	require.Eventually(t, func() bool { return rm.Count() == 0 }, 2*time.Second, 10*time.Millisecond)
	require.False(t, a.call.IsEnded(), "surviving leg keeps running")
}

func TestRoom_LegCallEnded_OnlyFiresOnCallDeath(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	var mu sync.Mutex
	var ended []string
	rm.OnLegCallEnded(func(callID string) {
		mu.Lock()
		ended = append(ended, callID)
		mu.Unlock()
	})
	got := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), ended...)
	}

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)
	c := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.NoError(t, rm.AddLeg(r.ID(), b.call))
	require.NoError(t, rm.AddLeg(r.ID(), c.call))

	// RPC-driven remove: no callback.
	require.NoError(t, rm.RemoveLeg(r.ID(), c.call.ID()))
	require.Empty(t, got())

	// The leg's own call ending fires the callback exactly for that leg.
	require.NoError(t, mgr.Close(b.call.ID(), "media_eof"))
	require.Eventually(t, func() bool { return len(got()) == 1 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{b.call.ID()}, got())

	// b's death dropped the room to one leg, which auto-drains (RPC-free):
	// the drain must not fire an extra callback either.
	require.Eventually(t, func() bool { return rm.Count() == 0 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{b.call.ID()}, got())
}

func TestRoom_Close_HangsUpAllLegs(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)
	c := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.NoError(t, rm.AddLeg(r.ID(), b.call))
	require.NoError(t, rm.AddLeg(r.ID(), c.call))

	require.NoError(t, rm.Close(r.ID(), "whole_chain_hangup"))

	// Every leg force-hung-up, room gone.
	require.Eventually(t, func() bool {
		return a.call.IsEnded() && b.call.IsEnded() && c.call.IsEnded()
	}, 2*time.Second, 10*time.Millisecond, "all legs hung up on room.close")
	require.Equal(t, 0, rm.Count())
}

func TestRoom_Errors(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	a := newLeg(t, mgr, session.DirectionInbound)
	b := newLeg(t, mgr, session.DirectionOutbound)

	r := rm.Create()
	require.NoError(t, rm.AddLeg(r.ID(), a.call))
	require.ErrorIs(t, rm.AddLeg(r.ID(), a.call), ErrLegExists)
	require.NoError(t, rm.AddLeg(r.ID(), b.call))

	require.ErrorIs(t, rm.SetMixGain(r.ID(), a.call.ID(), b.call.ID(), 1.5), ErrGainRange)
	require.ErrorIs(t, rm.SetMixGain(r.ID(), a.call.ID(), "nope", 0.5), ErrLegNotFound)

	_, err := rm.Get("no-such-room")
	require.ErrorIs(t, err, ErrRoomNotFound)

	rm.Close(r.ID(), "cleanup")
}

// TestRoom_Concurrency stresses add/remove/gain/close concurrently; run under
// -race to catch data races in the mixer's hot path.
func TestRoom_Concurrency(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	rm := NewManager(mgr, quietLogger())

	legs := make([]*legFixture, 4)
	for i := range legs {
		legs[i] = newLeg(t, mgr, session.DirectionOutbound)
	}
	r := rm.Create()
	for _, l := range legs {
		require.NoError(t, rm.AddLeg(r.ID(), l.call))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i, l := range legs {
		wg.Add(1)
		go func(v int16, io *fakeio.FakeIO) {
			defer wg.Done()
			pumpIncoming(io, v, stop)
		}(int16(100*(i+1)), l.io)
	}

	// Churn gains while mixing runs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = rm.SetMixGain(r.ID(), legs[0].call.ID(), legs[1].call.ID(), float64(i%2))
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	rm.Close(r.ID(), "done")
}
