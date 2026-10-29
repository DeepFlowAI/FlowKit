package recording

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

func TestLocalTimelineIgnoresCallbackJitter(t *testing.T) {
	var clock localTimeline
	walls := []time.Duration{100 * time.Millisecond, 121 * time.Millisecond, 139 * time.Millisecond, 161 * time.Millisecond}
	for i, wall := range walls {
		got, _ := clock.timestamp(wall, bus.SamplesPerFrame)
		want := time.Duration(5+i) * bus.FrameDuration
		if got != want {
			t.Fatalf("frame %d timestamp=%v want=%v", i, got, want)
		}
	}
}

func TestLocalTimelineDoesNotDriftBehindSlowStream(t *testing.T) {
	// A continuous stream delivered slightly slower than real time (never a
	// gap long enough to resync) must not fall behind the wall clock: once the
	// lag passes the encoder holdback, every frame on that side is dropped.
	var clock localTimeline
	wall := 100 * time.Millisecond
	for i := 0; i < 500; i++ {
		got, _ := clock.timestamp(wall, bus.SamplesPerFrame)
		if lag := wall - got; lag > localTimelineGap {
			t.Fatalf("frame %d: timestamp %v lags wall %v by %v", i, got, wall, lag)
		}
		wall += 21 * time.Millisecond
	}
	if n, jump := clock.resyncStats(); n == 0 || jump < localTimelineGap {
		t.Fatalf("resyncStats() = %d, %v; want resyncs of at least %v", n, jump, localTimelineGap)
	}
}

func TestBurstAfterUplinkStallKeepsOtherSideRecorded(t *testing.T) {
	// The caller's audio stalls on the network for 1.74s and then lands all
	// at once. Counting the backlog on from the resync point would put the
	// caller's side over a second ahead of the wall clock for the rest of the
	// call, and every agent frame stamped at wall time would then arrive
	// behind the encoder's holdback and be dropped.
	enc := NewStereoEncoder()
	var remote, local localTimeline
	frame := make([]int16, bus.SamplesPerFrame)
	stallFrom, stallTo := 2*time.Second, 3740*time.Millisecond
	for wall := time.Duration(0); wall < 8*time.Second; wall += bus.FrameDuration {
		switch {
		case wall < stallFrom || wall > stallTo:
			if ts, ok := remote.timestamp(wall, len(frame)); ok {
				enc.WriteRemote(ts, frame)
			}
		case wall == stallTo:
			for range int((stallTo - stallFrom) / bus.FrameDuration) {
				if ts, ok := remote.timestamp(wall, len(frame)); ok {
					enc.WriteRemote(ts, frame)
				}
			}
		}
		if wall >= 5*time.Second && wall < 7*time.Second {
			if ts, ok := local.timestamp(wall, len(frame)); ok {
				enc.WriteLocal(ts, frame)
			}
		}
	}
	if _, late := enc.LateFrames(); late != 0 {
		t.Fatalf("late local frames = %d, want 0", late)
	}
}

func TestLocalTimelineKeepsSendAheadLead(t *testing.T) {
	// A peer that is sent audio ahead of real time plays it later; the lead
	// is real and the frames must keep consecutive timestamps.
	var clock localTimeline
	for i := range 15 {
		got, ok := clock.timestamp(100*time.Millisecond, bus.SamplesPerFrame)
		want := 100*time.Millisecond + time.Duration(i)*bus.FrameDuration
		if !ok || got != want {
			t.Fatalf("frame %d timestamp=%v ok=%v want=%v", i, got, ok, want)
		}
	}
}

func TestLocalTimelinePreservesRealPlaybackGap(t *testing.T) {
	var clock localTimeline
	if got, _ := clock.timestamp(100*time.Millisecond, bus.SamplesPerFrame); got != 100*time.Millisecond {
		t.Fatalf("first timestamp=%v", got)
	}
	if got, _ := clock.timestamp(120*time.Millisecond, bus.SamplesPerFrame); got != 120*time.Millisecond {
		t.Fatalf("second timestamp=%v", got)
	}
	if got, _ := clock.timestamp(400*time.Millisecond, bus.SamplesPerFrame); got != 400*time.Millisecond {
		t.Fatalf("timestamp after gap=%v", got)
	}
}
