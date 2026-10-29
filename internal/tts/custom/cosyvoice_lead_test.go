package custom

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

func int16sToBytesLE(s []int16) []byte {
	b := make([]byte, len(s)*2)
	for i, v := range s {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(v))
	}
	return b
}

// makeClip builds native-rate PCM: `silenceMs` of near-silence then `voiceMs`
// of loud tone, at 24 kHz.
func makeClip(silenceMs, voiceMs int) []int16 {
	const sr = 24000
	n := (silenceMs + voiceMs) * sr / 1000
	out := make([]int16, n)
	cut := silenceMs * sr / 1000
	for i := cut; i < n; i++ {
		if i%2 == 0 {
			out[i] = 8000
		} else {
			out[i] = -8000
		}
	}
	return out
}

func firstNonZero(s []int16) int {
	for i, v := range s {
		if v != 0 {
			return i
		}
	}
	return -1
}

func TestLeadingSilence_FindsOnset(t *testing.T) {
	clip := makeClip(600, 1000) // 600ms silence + 1s voice @24k
	drop := leadingSilence(clip, leadTrimRMS, leadTrimWin)
	if drop < 0 {
		t.Fatal("expected to find onset, got -1")
	}
	onsetMs := drop * 1000 / 24000
	// Onset should land near 600ms (one window = 10ms of soft-attack headroom).
	if onsetMs < 560 || onsetMs > 610 {
		t.Fatalf("onset at %dms, want ~590-600ms", onsetMs)
	}
}

func TestLeadingSilence_AllSilent(t *testing.T) {
	if got := leadingSilence(make([]int16, 24000), leadTrimRMS, leadTrimWin); got != -1 {
		t.Fatalf("all-silent clip should return -1, got %d", got)
	}
}

// loud appends n samples of alternating ±amp to s.
func loud(s []int16, n int, amp int16) []int16 {
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			s = append(s, amp)
		} else {
			s = append(s, -amp)
		}
	}
	return s
}

func TestSpeechStartIndex_SkipsArtifact(t *testing.T) {
	const sr = 24000
	// [50ms loud blip][150ms silence][800ms speech] — CosyVoice "哦" pattern.
	var s []int16
	s = loud(s, 50*sr/1000, 8000)
	s = append(s, make([]int16, 150*sr/1000)...)
	speechAt := len(s)
	s = loud(s, 800*sr/1000, 8000)

	idx := speechStartIndex(s, leadTrimRMS, leadTrimWin, 20, false)
	// Should land at the sustained speech (one window of headroom), not the blip.
	if idx < speechAt-leadTrimWin || idx > speechAt+leadTrimWin {
		t.Fatalf("artifact-skip: got %d, want ~%d (real speech)", idx, speechAt)
	}
}

func TestSpeechStartIndex_SkipsLoudArtifact(t *testing.T) {
	const sr = 24000
	// A *loud* but short blip (amplitude > real speech) must still be skipped —
	// the skip is by duration, not loudness.
	var s []int16
	s = loud(s, 60*sr/1000, 15000) // louder than the speech below
	s = append(s, make([]int16, 120*sr/1000)...)
	speechAt := len(s)
	s = loud(s, 800*sr/1000, 6000)
	idx := speechStartIndex(s, leadTrimRMS, leadTrimWin, 20, false)
	if idx < speechAt-leadTrimWin || idx > speechAt+leadTrimWin {
		t.Fatalf("loud-artifact-skip: got %d, want ~%d", idx, speechAt)
	}
}

func TestSpeechStartIndex_NormalSpeechUnskipped(t *testing.T) {
	const sr = 24000
	// [80ms silence][long speech] — no artifact; onset kept (one window headroom).
	var s []int16
	s = append(s, make([]int16, 80*sr/1000)...)
	onset := len(s)
	s = loud(s, 1000*sr/1000, 8000)
	idx := speechStartIndex(s, leadTrimRMS, leadTrimWin, 20, false)
	if idx < onset-2*leadTrimWin || idx > onset+leadTrimWin {
		t.Fatalf("normal speech: got %d, want ~%d (first onset, not skipped)", idx, onset)
	}
}

func TestSpeechStartIndex_UndecidedWaits(t *testing.T) {
	const sr = 24000
	// Only a short blip so far (no lookahead past it): must defer (-1) unless final.
	var s []int16
	s = loud(s, 50*sr/1000, 8000)
	if idx := speechStartIndex(s, leadTrimRMS, leadTrimWin, 20, false); idx != -1 {
		t.Fatalf("short blip at buffer end should defer, got %d", idx)
	}
}

// streamBody (with trim on) must drop the lead-in so the first emitted sample
// is voiced, while (trim off) preserves the silence.
func TestStreamBody_TrimsLeadIn(t *testing.T) {
	clip := makeClip(600, 800)
	body := int16sToBytesLE(clip)

	collect := func(trim bool) []int16 {
		p := &Provider{cfg: Config{SourceSampleRate: 24000, TrimLeadingSilence: trim}}
		out := make(chan tts.PCMChunk, 64)
		go func() {
			defer close(out)
			_ = p.streamBody(context.Background(), bytes.NewReader(body), out, targetSampleRate, p.cfg.LeadPadMs, 0, 0, time.Now())
		}()
		var all []int16
		for c := range out {
			all = append(all, c.Samples...)
		}
		return all
	}

	trimmed := collect(true)
	kept := collect(false)

	// Trimmed output starts (almost) immediately with voice.
	if nz := firstNonZero(trimmed); nz < 0 || nz > 16000/10 { // within 100ms @16k
		t.Fatalf("trimmed: first voiced sample at %d (want near 0)", nz)
	}
	// Untrimmed keeps the ~600ms lead-in (≈9600 samples @16k).
	if nz := firstNonZero(kept); nz < 16000/2 {
		t.Fatalf("untrimmed: first voiced sample at %d (want ~9600)", nz)
	}
	if len(trimmed) >= len(kept) {
		t.Fatalf("trimmed (%d) should be shorter than kept (%d)", len(trimmed), len(kept))
	}
}
