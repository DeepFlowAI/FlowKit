package custom

import "math"

// defaultMaxVowelHoldMs is how long a near-stationary voiced plateau may play
// before later frames of the same plateau are dropped. Chinese vowels in
// customer-service speech are typically 80–300ms; 700ms still keeps a natural
// elongated 您/好 and cuts the 3–4s CosyVoice2 zero-shot holds.
const defaultMaxVowelHoldMs = 700

const (
	// vowelHoldGraceWin is how many consecutive off-plateau windows (10ms each)
	// a stuck vowel may contain without ending the plateau. CosyVoice2's held
	// vowels wobble in loudness every few hundred ms; without this grace a
	// single dip restarts the count and a 1.3s "继" never reaches the limit.
	vowelHoldGraceWin = 4
	// vowelHoldBreakZCR is a zero-crossing jump that always ends the plateau
	// (a fricative such as the x in 续), so a short consonant between two
	// similar vowels is never absorbed by the grace.
	vowelHoldBreakZCR = 0.1
	// plateauRefAlpha is how fast the plateau reference follows the vowel.
	plateauRefAlpha = 0.2
	// vowelHoldSeamMs is the crossfade at the point where playback resumes after
	// dropped frames, so the cut does not click.
	vowelHoldSeamMs = 5
)

// vowelHoldGate drops trailing frames of a stuck vowel so a CosyVoice duration
// blow-up does not play as one character stretched for seconds. Each window is
// compared with a running reference of the plateau, not just the previous
// window; a jump in RMS or zero-crossing rate that lasts longer than the grace
// (or a fricative-sized ZCR jump) starts the next phoneme and resets the hold.
type vowelHoldGate struct {
	sr         int
	win        int
	maxHoldWin int
	seamN      int
	buf        []int16

	inPlateau bool
	hold      int
	refRMS    float64
	refZCR    float64
	// miss holds off-plateau windows seen while dropping: they are dropped if
	// the plateau resumes, or played if they turn out to start the next phoneme.
	miss  []int16
	missN int
	// seam is the first dropped window; it continues the waveform from the last
	// kept sample and is crossfaded into the resume point.
	dropping    bool
	seam        []int16
	droppedSamp int
}

func newVowelHoldGate(sr, maxHoldMs int) *vowelHoldGate {
	if sr <= 0 {
		sr = 24000
	}
	if maxHoldMs <= 0 {
		return nil
	}
	win := leadTrimWin
	if sr != 24000 {
		win = sr / 100 // 10ms
		if win < 1 {
			win = 1
		}
	}
	maxHoldWin := maxHoldMs * sr / 1000 / win
	if maxHoldWin < 1 {
		maxHoldWin = 1
	}
	return &vowelHoldGate{sr: sr, win: win, maxHoldWin: maxHoldWin, seamN: vowelHoldSeamMs * sr / 1000}
}

func (g *vowelHoldGate) Filter(in []int16) []int16 {
	if g == nil || len(in) == 0 {
		return in
	}
	g.buf = append(g.buf, in...)
	out := make([]int16, 0, len(g.buf))
	for len(g.buf) >= g.win {
		w := g.buf[:g.win]
		g.buf = g.buf[g.win:]
		rms := windowRMS(w, 0, g.win)
		zcr := windowZCR(w)
		if rms < leadTrimRMS {
			out = g.endPlateau(out, w)
			continue
		}
		if !g.inPlateau {
			g.startPlateau(rms, zcr)
			out = append(out, w...)
			continue
		}
		dz := math.Abs(zcr - g.refZCR)
		if rmsClose(rms, g.refRMS) && dz < 0.05 {
			g.refRMS += plateauRefAlpha * (rms - g.refRMS)
			g.refZCR += plateauRefAlpha * (zcr - g.refZCR)
			if g.dropping {
				g.droppedSamp += len(g.miss)
			}
			g.miss, g.missN = nil, 0
			g.hold++
			out = g.keepOrDrop(out, w)
			continue
		}
		if g.missN < vowelHoldGraceWin && dz < vowelHoldBreakZCR {
			g.missN++
			g.hold++
			if g.dropping {
				g.miss = append(g.miss, w...)
			} else {
				out = append(out, w...)
			}
			continue
		}
		out = g.endPlateau(out, w)
		g.startPlateau(rms, zcr)
	}
	return out
}

func (g *vowelHoldGate) startPlateau(rms, zcr float64) {
	g.inPlateau = true
	g.hold = 1
	g.refRMS, g.refZCR = rms, zcr
}

func (g *vowelHoldGate) keepOrDrop(out, w []int16) []int16 {
	if g.hold <= g.maxHoldWin {
		return append(out, w...)
	}
	if !g.dropping {
		g.dropping = true
		g.seam = append([]int16(nil), w...)
	}
	g.droppedSamp += len(w)
	return out
}

// endPlateau plays any held-back windows plus w, crossfading from the dropped
// waveform when frames were cut, and resets the plateau.
func (g *vowelHoldGate) endPlateau(out, w []int16) []int16 {
	tail := append(append([]int16(nil), g.miss...), w...)
	if g.dropping {
		crossfadeInto(g.seam, tail, g.seamN)
	}
	g.inPlateau, g.hold = false, 0
	g.miss, g.missN = nil, 0
	g.dropping, g.seam = false, nil
	return append(out, tail...)
}

// crossfadeInto blends the first n samples of dst from src to dst.
func crossfadeInto(src, dst []int16, n int) {
	n = min(n, len(src), len(dst))
	for i := 0; i < n; i++ {
		t := float64(i+1) / float64(n+1)
		dst[i] = int16(float64(src[i])*(1-t) + float64(dst[i])*t)
	}
}

func (g *vowelHoldGate) Flush() []int16 {
	if g == nil {
		return nil
	}
	left := g.endPlateau(nil, g.buf)
	g.buf = nil
	if len(left) == 0 {
		return nil
	}
	return left
}

func (g *vowelHoldGate) DroppedMs() int {
	if g == nil || g.sr <= 0 {
		return 0
	}
	return g.droppedSamp * 1000 / g.sr
}

func windowZCR(s []int16) float64 {
	if len(s) < 2 {
		return 0
	}
	var n int
	prev := s[0]
	for i := 1; i < len(s); i++ {
		if (prev >= 0) != (s[i] >= 0) {
			n++
		}
		prev = s[i]
	}
	return float64(n) / float64(len(s)-1)
}

func rmsClose(a, b float64) bool {
	ref := b
	if a > ref {
		ref = a
	}
	if ref < 1 {
		return true
	}
	return math.Abs(a-b)/ref < 0.35
}
