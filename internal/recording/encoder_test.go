package recording

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

func TestStereoEncoder_EmitsWhenBothSidesPresent(t *testing.T) {
	e := NewStereoEncoder()
	want := bus.SamplesPerFrame * 4
	samples := make([]int16, bus.SamplesPerFrame)

	// Both sides present for slot 0 → emit immediately (timely pairing).
	_ = e.WriteRemote(0, append([]int16(nil), samples...))
	out := e.WriteLocal(0, append([]int16(nil), samples...))
	if len(out) != want {
		t.Fatalf("both sides at slot 0 should emit: got %d want %d", len(out), want)
	}
}

func TestStereoEncoder_HoldsRemoteOnlyWithinHoldback(t *testing.T) {
	e := NewStereoEncoder()
	samples := make([]int16, bus.SamplesPerFrame)

	// Remote-only slots within the holdback window are held back, giving the
	// lagging local side time to arrive.
	emitted := 0
	for slot := int64(0); slot < emitHoldbackSlots; slot++ {
		emitted += len(e.WriteRemote(time.Duration(slot)*bus.FrameDuration, append([]int16(nil), samples...)))
	}
	if emitted != 0 {
		t.Fatalf("remote-only slots within holdback should not emit yet: got %d bytes", emitted)
	}
	// Once slot 0 is holdback slots behind, it is force-emitted.
	out := e.WriteRemote(time.Duration(emitHoldbackSlots)*bus.FrameDuration, append([]int16(nil), samples...))
	if len(out) == 0 {
		t.Fatal("slot 0 should emit once it is holdback slots behind")
	}
}

func TestStereoEncoder_LaggingLocalNotDropped(t *testing.T) {
	// Regression: the agent (local) leg arrives later than the steady remote
	// stream. Within the holdback window its frame must still fill the slot
	// rather than being dropped as "late" (which left the agent channel silent).
	e := NewStereoEncoder()
	samples := make([]int16, bus.SamplesPerFrame)

	_ = e.WriteRemote(0, append([]int16(nil), samples...))
	_ = e.WriteRemote(2*bus.FrameDuration, append([]int16(nil), samples...)) // remote 2 slots ahead
	_ = e.WriteLocal(0, append([]int16(nil), samples...))                    // local for slot 0, late but within holdback
	if e.HadIssues() {
		t.Fatal("local frame within holdback must not be dropped as late")
	}
}

func TestStereoEncoder_TrulyLateSlotDropped(t *testing.T) {
	e := NewStereoEncoder()
	samples := make([]int16, bus.SamplesPerFrame)

	// Push the newest slot past the holdback so slot 0 is force-emitted.
	_ = e.WriteRemote(time.Duration(emitHoldbackSlots+1)*bus.FrameDuration, append([]int16(nil), samples...))
	// A frame for an already-emitted slot is genuinely late → dropped.
	if out := e.WriteRemote(0, append([]int16(nil), samples...)); len(out) != 0 {
		t.Fatal("truly-late slot should not emit")
	}
	if !e.HadIssues() {
		t.Fatal("expected issues flag for truly-late slot")
	}
	_ = e.WriteLocal(0, append([]int16(nil), samples...))
	_ = e.WriteLocal(bus.FrameDuration, append([]int16(nil), samples...))
	if remote, local := e.LateFrames(); remote != 1 || local != 2 {
		t.Fatalf("LateFrames() = %d remote, %d local; want 1, 2", remote, local)
	}
}

func TestStereoEncoder_FlushFillsSilenceGap(t *testing.T) {
	e := NewStereoEncoder()
	// Only slot 2 has audio; Flush emits slots 0,1 (silence) + slot 2.
	_ = e.WriteRemote(2*bus.FrameDuration, make([]int16, bus.SamplesPerFrame))
	out := e.Flush()
	if len(out) != bus.SamplesPerFrame*4*3 {
		t.Fatalf("expected 3 slots (0,1 silence + 2), got %d bytes", len(out))
	}
}

func TestStereoEncoder_FlushTrailingPartial(t *testing.T) {
	e := NewStereoEncoder()
	samples := make([]int16, bus.SamplesPerFrame)
	_ = e.WriteRemote(0, append([]int16(nil), samples...))
	if len(e.Flush()) == 0 {
		t.Fatal("Flush should emit trailing partial slot")
	}
}
