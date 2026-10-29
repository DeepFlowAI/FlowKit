//go:build aec

package aec

import (
	"math"
	"testing"
)

// TestEchoReduction feeds a broadband far-end (played) signal plus a delayed,
// attenuated copy of it as the mic echo, then asserts AEC3 attenuates the echo
// by a healthy margin once converged (ERLE measured over the tail).
func TestEchoReduction(t *testing.T) {
	const (
		frame20 = 320 // 20 ms @ 16 kHz (kernel frame)
		frames  = 400 // ~8 s, enough to converge
		delayFr = 3   // echo delayed ~60 ms
		atten   = 0.6 // echo path gain
	)

	c := New(Options{NSEnabled: true, NSLevel: 2, StreamDelayMS: 60})
	if c == nil {
		t.Fatal("apm create failed")
	}
	defer c.Close()

	var rng uint32 = 12345
	nextNoise := func() float64 {
		rng = rng*1664525 + 1013904223
		return float64(int32(rng>>16)-32768) * 0.3
	}

	far := make([][]int16, frames)
	for i := range far {
		f := make([]int16, frame20)
		for j := range f {
			f[j] = int16(nextNoise())
		}
		far[i] = f
	}

	var micEnergy, outEnergy float64
	measureFrom := frames - 100
	for i := 0; i < frames; i++ {
		c.ProcessRender(far[i])

		mic := make([]int16, frame20)
		if i >= delayFr {
			src := far[i-delayFr]
			for j := range mic {
				mic[j] = int16(float64(src[j]) * atten)
			}
		}
		out := c.ProcessCapture(mic)

		if i >= measureFrom {
			for _, s := range mic {
				micEnergy += float64(s) * float64(s)
			}
			for _, s := range out {
				outEnergy += float64(s) * float64(s)
			}
		}
	}

	if outEnergy == 0 {
		t.Fatal("no output produced")
	}
	erle := 10 * math.Log10(micEnergy/outEnergy)
	t.Logf("ERLE = %.1f dB (micE=%.3g outE=%.3g)", erle, micEnergy, outEnergy)
	if erle < 6 {
		t.Fatalf("echo not sufficiently cancelled: ERLE=%.1f dB (want >= 6)", erle)
	}
}

// TestPassthroughWhenNoEcho sanity-checks that a near-end-only signal (no render
// reference) survives processing without being zeroed out.
func TestPassthroughWhenNoEcho(t *testing.T) {
	c := New(Options{NSEnabled: false})
	if c == nil {
		t.Fatal("apm create failed")
	}
	defer c.Close()

	const frame20 = 320
	var in, out float64
	for i := 0; i < 100; i++ {
		mic := make([]int16, frame20)
		for j := range mic {
			mic[j] = int16(3000 * math.Sin(2*math.Pi*440*float64(i*frame20+j)/sampleRate))
		}
		cleaned := c.ProcessCapture(mic)
		for _, s := range mic {
			in += float64(s) * float64(s)
		}
		for _, s := range cleaned {
			out += float64(s) * float64(s)
		}
	}
	if out < in*0.2 {
		t.Fatalf("near-end signal over-suppressed: in=%.3g out=%.3g", in, out)
	}
}
