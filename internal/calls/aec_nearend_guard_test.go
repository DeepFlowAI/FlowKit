package calls

import (
	"math/rand"
	"testing"
	"time"
)

func TestNearEndGuardKeepsCorrelatedEchoCancelled(t *testing.T) {
	g := &nearEndGuard{}
	now := time.Unix(1, 0)
	rng := rand.New(rand.NewSource(7))
	ref := noiseFrame(rng, nearEndWindowSamples, 3000)
	for off := 0; off < len(ref); off += 320 {
		g.observeRender(ref[off:off+320], now)
		now = now.Add(20 * time.Millisecond)
	}
	quiet := make([]int16, 320)
	for off := 0; off < len(ref); off += 320 {
		got := g.preserveNearEnd(ref[off:off+320], quiet, now)
		if &got[0] != &quiet[0] {
			t.Fatal("render-correlated echo must remain cancelled")
		}
		now = now.Add(20 * time.Millisecond)
	}
}

func TestNearEndGuardRestoresUnrelatedDoubleTalk(t *testing.T) {
	g := &nearEndGuard{}
	now := time.Unix(1, 0)
	renderRNG := rand.New(rand.NewSource(7))
	nearRNG := rand.New(rand.NewSource(99))
	for i := 0; i < 5; i++ {
		g.observeRender(noiseFrame(renderRNG, 320, 3000), now)
		now = now.Add(20 * time.Millisecond)
	}
	quiet := make([]int16, 320)
	restored := false
	for i := 0; i < 8; i++ {
		raw := noiseFrame(nearRNG, 320, 3000)
		got := g.preserveNearEnd(raw, quiet, now)
		if &got[0] == &raw[0] {
			restored = true
		}
		now = now.Add(20 * time.Millisecond)
	}
	if !restored {
		t.Fatal("sustained unrelated near-end speech must survive cancellation")
	}
}

func TestNearEndGuardDoesNotReplaceHealthyAECOutput(t *testing.T) {
	g := &nearEndGuard{}
	rng := rand.New(rand.NewSource(7))
	raw := noiseFrame(rng, 320, 3000)
	cancelled := append([]int16(nil), raw...)
	got := g.preserveNearEnd(raw, cancelled, time.Unix(1, 0))
	if &got[0] != &cancelled[0] {
		t.Fatal("healthy AEC output must pass through unchanged")
	}
}

func TestNearEndGuardKeepsUtteranceRescueStable(t *testing.T) {
	g := &nearEndGuard{}
	now := time.Unix(1, 0)
	renderRNG := rand.New(rand.NewSource(7))
	nearRNG := rand.New(rand.NewSource(99))
	for i := 0; i < 5; i++ {
		g.observeRender(noiseFrame(renderRNG, 320, 3000), now)
		now = now.Add(20 * time.Millisecond)
	}
	quiet := make([]int16, 320)
	for i := 0; i < 5; i++ {
		raw := noiseFrame(nearRNG, 320, 3000)
		_ = g.preserveNearEnd(raw, quiet, now)
		now = now.Add(20 * time.Millisecond)
	}
	// Even if a later frame happens to resemble the render, a detected near-end
	// utterance must not alternate between raw and cancelled every 20 ms.
	raw := append([]int16(nil), g.renderHistory[len(g.renderHistory)-320:]...)
	got := g.preserveNearEnd(raw, quiet, now)
	if &got[0] != &raw[0] {
		t.Fatal("near-end rescue must stay latched until the utterance goes quiet")
	}
}

func noiseFrame(rng *rand.Rand, n int, amplitude int) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = int16(rng.Intn(amplitude*2) - amplitude)
	}
	return out
}
