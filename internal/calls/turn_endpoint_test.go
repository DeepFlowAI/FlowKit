package calls

import (
	"sync/atomic"
	"testing"
)

// An endpointer with no worker goroutine: the model is never consulted, so these
// tests see the text rules alone.
func newTextOnlyEndpointer(flushes *atomic.Int32) *turnEndpointer {
	e := &turnEndpointer{
		flush:       func() error { flushes.Add(1); return nil },
		log:         quietLogger(),
		callID:      "call-turn",
		rmsThr:      350,
		triggerMs:   250,
		reAskMs:     300,
		maxMs:       1500,
		minVoicedMs: 400,
		threshold:   0.7,
		req:         make(chan turnRequest, 1),
	}
	e.gen.Store(1)
	e.nextAskMs = e.triggerMs
	return e
}

// feed pushes n frames of 20 ms at the given amplitude (0 = silence).
func (e *turnEndpointer) feedFrames(amp int16, n int) {
	frame := make([]int16, 320)
	for i := range frame {
		frame[i] = amp
	}
	for i := 0; i < n; i++ {
		e.Feed(frame)
	}
}

// A whole short answer must not wait out the long fallback: 300 ms of silence is
// all it takes, and only one release per turn.
func TestEndpointerTextPathReleasesShortAnswer(t *testing.T) {
	var flushes atomic.Int32
	e := newTextOnlyEndpointer(&flushes)
	e.ObservePartial("好")
	e.ObservePartial("的")

	e.feedFrames(6000, 25) // 500 ms voiced, past the min-voiced bar
	e.feedFrames(0, 14)    // 280 ms of pause: still under the finished bar
	if got := flushes.Load(); got != 0 {
		t.Fatalf("released after 280ms, want no release yet (flushes=%d)", got)
	}
	e.feedFrames(0, 2) // 320 ms
	if got := flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d after 320ms of pause, want 1", got)
	}
	e.feedFrames(0, 20) // pause keeps growing
	if got := flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d, a turn must be released once", got)
	}
}

// A caller mid-sentence whose partials happen to be short must not be treated
// as having given a short answer. ASR partials lag and arrive in fragments, so
// this is exactly what "你会干什么呀，还有…" looks like a moment in: releasing on
// the short bar here talks over the caller.
func TestEndpointerTextPathDoesNotRushAShortTranscript(t *testing.T) {
	var flushes atomic.Int32
	e := newTextOnlyEndpointer(&flushes)
	e.ObservePartial("你")
	e.ObservePartial("会干什")

	e.feedFrames(6000, 46) // 920 ms voiced, as measured on a real call
	e.feedFrames(0, 17)    // 340 ms of pause: past the finished bar
	if got := flushes.Load(); got != 0 {
		t.Fatalf("released a mid-sentence pause on the short bar (flushes=%d)", got)
	}
	e.feedFrames(0, 7) // 480 ms, past the neutral bar
	if got := flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d after 480ms, want the neutral bar to release it", got)
	}
}

// "我想" is someone about to say what they want. Releasing there cuts them off,
// so the text rules must hand the turn to the ASR's own long fallback.
func TestEndpointerTextPathHoldsMidThought(t *testing.T) {
	var flushes atomic.Int32
	e := newTextOnlyEndpointer(&flushes)
	e.ObservePartial("我想")

	e.feedFrames(6000, 25)
	e.feedFrames(0, 50) // a full second of pause
	if got := flushes.Load(); got != 0 {
		t.Fatalf("flushes=%d, a mid-thought pause must not be released early", got)
	}
}

// A turn barely into a syllable is a fragment, whatever its text looks like.
func TestEndpointerTextPathWaitsForMinVoiced(t *testing.T) {
	var flushes atomic.Int32
	e := newTextOnlyEndpointer(&flushes)
	e.ObservePartial("好的")

	e.feedFrames(6000, 10) // 200 ms voiced, under the bar
	e.feedFrames(0, 30)
	if got := flushes.Load(); got != 0 {
		t.Fatalf("flushes=%d, a 200ms fragment must not be released", got)
	}
}

// Each turn gets its own release, and the transcript must not leak into the next
// one — otherwise a "好的" would keep releasing every later pause early.
func TestEndpointerReleasesOncePerTurn(t *testing.T) {
	var flushes atomic.Int32
	e := newTextOnlyEndpointer(&flushes)
	e.ObservePartial("好的")
	e.feedFrames(6000, 25)
	e.feedFrames(0, 20)
	if got := flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d after the first turn, want 1", got)
	}

	e.Reset()
	if got := e.partialText(); got != "" {
		t.Fatalf("transcript %q survived the reset", got)
	}
	e.ObservePartial("我想")
	e.feedFrames(6000, 25)
	e.feedFrames(0, 50)
	if got := flushes.Load(); got != 1 {
		t.Fatalf("flushes=%d, the second turn is mid-thought and must hold", got)
	}
}
