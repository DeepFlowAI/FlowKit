package custom

import (
	"math"
	"testing"
)

func makeTone(ms, sr, hz int, amp int16) []int16 {
	n := ms * sr / 1000
	out := make([]int16, n)
	if hz <= 0 {
		return out
	}
	for i := range out {
		out[i] = int16(float64(amp) * math.Sin(2*math.Pi*float64(hz)*float64(i)/float64(sr)))
	}
	return out
}

func TestVowelHoldGate_DropsStuckPlateauKeepsNextPhoneme(t *testing.T) {
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	// 200ms onset + 4000ms same vowel + 200ms new phoneme.
	in := append(append(makeTone(200, sr, 200, 6000), makeTone(4000, sr, 200, 5500)...), makeTone(200, sr, 800, 7000)...)
	out := append(g.Filter(in), g.Flush()...)
	if g.DroppedMs() < 2800 {
		t.Fatalf("dropped_ms=%d, want >= 2800 of the 4s hold", g.DroppedMs())
	}
	outMs := len(out) * 1000 / sr
	if outMs < 900 || outMs > 1400 {
		t.Fatalf("out_ms=%d, want ~1100 (200+700+200)", outMs)
	}
}

func TestVowelHoldGate_KeepsNaturalShortVowel(t *testing.T) {
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	in := makeTone(400, sr, 220, 5000)
	out := append(g.Filter(in), g.Flush()...)
	if g.DroppedMs() != 0 {
		t.Fatalf("dropped_ms=%d, short vowel must pass", g.DroppedMs())
	}
	if len(out) < len(in)*9/10 {
		t.Fatalf("kept %d samples of %d", len(out), len(in))
	}
}

// wobblyVowel is a 220Hz vowel whose loudness dips to 40% for 20ms every
// 250ms, like the 1.3s "继" CosyVoice2 produced in call 06a2de82.
func wobblyVowel(ms, sr int) []int16 {
	out := makeTone(ms, sr, 220, 6000)
	period, dip := 250*sr/1000, 20*sr/1000
	for i := range out {
		if i%period < dip {
			out[i] = int16(float64(out[i]) * 0.4)
		}
	}
	return out
}

func TestVowelHoldGate_DropsWobblyPlateau(t *testing.T) {
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	in := append(wobblyVowel(1300, sr), makeTone(200, sr, 900, 7000)...)
	out := append(g.Filter(in), g.Flush()...)
	if d := g.DroppedMs(); d < 450 || d > 650 {
		t.Fatalf("dropped_ms=%d, want ~600 of the 1.3s wobbly hold", d)
	}
	if got := len(out) * 1000 / sr; got < 850 || got > 1050 {
		t.Fatalf("out_ms=%d, want ~900 (700 kept + 200 next phoneme)", got)
	}
}

func TestVowelHoldGate_FricativeBetweenSimilarVowelsResets(t *testing.T) {
	// 您-x-u-您-x-u…: 300ms vowels split by 30ms fricatives. The fricative is
	// shorter than the grace but its ZCR jump must still end each plateau.
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	var in []int16
	for i := 0; i < 8; i++ {
		in = append(in, makeTone(300, sr, 220, 6000)...)
		in = append(in, makeTone(30, sr, 4000, 3000)...)
	}
	_ = append(g.Filter(in), g.Flush()...)
	if g.DroppedMs() != 0 {
		t.Fatalf("dropped_ms=%d, natural syllables must pass", g.DroppedMs())
	}
}

func TestVowelHoldGate_SeamHasNoClick(t *testing.T) {
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	// One continuous 233Hz waveform (not a whole number of cycles per 10ms
	// window, so dropped windows shift the phase): 2s held loud, then 300ms at
	// 30% as the next plateau. The source itself never jumps; only the cut can.
	in := makeTone(2300, sr, 233, 6000)
	for i := 2000 * sr / 1000; i < len(in); i++ {
		in[i] = int16(float64(in[i]) * 0.3)
	}
	out := append(g.Filter(in), g.Flush()...)
	if g.DroppedMs() < 1000 {
		t.Fatalf("dropped_ms=%d, want the hold cut", g.DroppedMs())
	}
	// A 233Hz sine at amp 6000 moves at most ~370 per sample at 24kHz; a raw
	// cut can jump up to 12000.
	maxStep := 0
	for i := 1; i < len(out); i++ {
		d := int(out[i]) - int(out[i-1])
		if d < 0 {
			d = -d
		}
		maxStep = max(maxStep, d)
	}
	if maxStep > 1200 {
		t.Fatalf("max sample step %d at the cut, want a smooth seam", maxStep)
	}
}

func TestVowelHoldGate_NilWhenDisabled(t *testing.T) {
	if g := newVowelHoldGate(24000, 0); g != nil {
		t.Fatal("maxHoldMs=0 must disable the gate")
	}
}

func TestVowelHoldGate_ResetOnSilence(t *testing.T) {
	const sr = 24000
	g := newVowelHoldGate(sr, 700)
	in := append(append(makeTone(900, sr, 200, 6000), make([]int16, sr/10)...), makeTone(900, sr, 200, 6000)...)
	_ = append(g.Filter(in), g.Flush()...)
	// Each 900ms vowel keeps 700ms and drops ~200ms. Two holds → ~400ms dropped.
	if g.DroppedMs() < 250 || g.DroppedMs() > 600 {
		t.Fatalf("dropped_ms=%d, want ~400 from two separate holds", g.DroppedMs())
	}
}
