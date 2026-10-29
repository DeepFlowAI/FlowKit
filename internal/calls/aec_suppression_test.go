package calls

import (
	"testing"
	"time"
)

// tone returns a frame at roughly the requested RMS.
func tone(rms int16) []int16 {
	f := make([]int16, 320) // 20ms at 16kHz
	for i := range f {
		if i%2 == 0 {
			f[i] = rms
		} else {
			f[i] = -rms
		}
	}
	return f
}

// Speech gone after the canceller is charged to the canceller.
func TestAccountSuppressionBlamesTheCanceller(t *testing.T) {
	p := &aecPipeline{}
	for i := 0; i < 10; i++ {
		p.accountSuppression(tone(4000), tone(10), tone(10))
	}
	if got := p.voicedFrames.Load(); got != 10 {
		t.Fatalf("voiced frames = %d, want 10", got)
	}
	if got := p.eatenByAEC.Load(); got != 10 {
		t.Fatalf("frames charged to AEC = %d, want 10", got)
	}
	if got := p.eatenByNR.Load(); got != 0 {
		t.Fatalf("frames charged to the denoiser = %d, want 0", got)
	}
}

// Speech that survives the canceller and dies in the enhancer is the enhancer's,
// once the run is long enough that the enhancer's delay cannot explain it.
func TestAccountSuppressionBlamesTheDenoiser(t *testing.T) {
	p := &aecPipeline{}
	for i := 0; i < 5; i++ {
		p.accountSuppression(tone(4000), tone(3500), tone(10))
	}
	want := int64(5 - pipelineNRSettleFrames + 1)
	if got := p.eatenByNR.Load(); got != want {
		t.Fatalf("frames charged to the denoiser = %d, want %d", got, want)
	}
	if got := p.eatenByAEC.Load(); got != 0 {
		t.Fatalf("frames charged to AEC = %d, want 0", got)
	}
}

// The enhancer returns audio a hop behind its input, so at an onset the output
// is still the silence from before. Charging that to the enhancer inflated a
// production tally by roughly half.
func TestAccountSuppressionIgnoresTheEnhancerDelayAtOnset(t *testing.T) {
	p := &aecPipeline{}
	// Onset: input is already loud, output still carries the preceding silence.
	p.accountSuppression(tone(4000), tone(3500), tone(0))
	p.accountSuppression(tone(4000), tone(3500), tone(3500))
	if got := p.eatenByNR.Load(); got != 0 {
		t.Fatalf("onset counted as loss, eaten=%d", got)
	}
	if got := p.voicedFrames.Load(); got != 2 {
		t.Fatalf("voiced frames = %d, want 2", got)
	}
}

// A gap in the speech restarts the run, so the next onset is not judged either.
func TestAccountSuppressionRestartsTheRunAfterSilence(t *testing.T) {
	p := &aecPipeline{}
	for i := 0; i < 4; i++ {
		p.accountSuppression(tone(4000), tone(3500), tone(3500))
	}
	p.accountSuppression(tone(10), tone(10), tone(10)) // silence
	p.accountSuppression(tone(4000), tone(3500), tone(0))
	if got := p.eatenByNR.Load(); got != 0 {
		t.Fatalf("onset after a gap counted as loss, eaten=%d", got)
	}
}

// Speech that survives the pipeline is the normal case and must not be counted.
func TestAccountSuppressionIgnoresSpeechThatSurvives(t *testing.T) {
	p := &aecPipeline{}
	p.accountSuppression(tone(4000), tone(3600), tone(3500))
	if got := p.eatenByAEC.Load() + p.eatenByNR.Load(); got != 0 {
		t.Fatalf("eaten frames = %d, want 0", got)
	}
	if got := p.voicedFrames.Load(); got != 1 {
		t.Fatalf("voiced frames = %d, want 1", got)
	}
}

// Cancelling the bot's own echo out of the uplink is the canceller working, not
// speech going missing. Without this the counter would fire on every turn.
func TestAccountSuppressionIgnoresEchoUnderPlayback(t *testing.T) {
	p := &aecPipeline{}
	p.lastRenderNS.Store(time.Now().UnixNano())
	p.accountSuppression(tone(4000), tone(10), tone(10))
	if got := p.voicedFrames.Load(); got != 0 {
		t.Fatalf("counted a frame while playback was live, voiced=%d", got)
	}

	// Once the echo tail has passed, the same frame is the caller's. Nothing
	// survives cancellation here, which needs no settling run.
	p.lastRenderNS.Store(time.Now().Add(-2 * pipelineEchoTail).UnixNano())
	p.accountSuppression(tone(4000), tone(10), tone(10))
	if got := p.eatenByAEC.Load(); got != 1 {
		t.Fatalf("eaten frames after the echo tail = %d, want 1", got)
	}
}

func TestAccountSuppressionIgnoresSilentUplink(t *testing.T) {
	p := &aecPipeline{}
	p.accountSuppression(tone(50), tone(0), tone(0))
	if got := p.voicedFrames.Load(); got != 0 {
		t.Fatalf("silence counted as speech, voiced=%d", got)
	}
}

func TestFrameRMS(t *testing.T) {
	if got := frameRMS(nil); got != 0 {
		t.Fatalf("frameRMS(nil)=%v want 0", got)
	}
	if got := frameRMS(tone(1000)); got < 999 || got > 1001 {
		t.Fatalf("frameRMS(±1000)=%v want ~1000", got)
	}
}
