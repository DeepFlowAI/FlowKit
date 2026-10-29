package calls

import (
	"math"
	"sync"
	"time"
)

const (
	nearEndWindowSamples = 640  // 40 ms at 16 kHz
	renderHistorySamples = 8000 // 500 ms at 16 kHz
	correlationStride    = 4
	correlationLagStep   = 40
	nearEndMaxEchoScore  = 0.45
	severeSuppression    = 0.25
	nearEndSpeechRMS     = pipelineSpeechRMS / 2
)

// nearEndGuard keeps AEC from erasing double-talk. AEC remains authoritative
// for capture that resembles the recent render reference. When sustained
// speech is heavily suppressed but does not resemble anything played to the
// peer, the capture is near-end speech and the untouched frame is restored
// before noise reduction.
type nearEndGuard struct {
	mu sync.Mutex

	renderHistory []int16
	suppressedRun []int16
	lastRender    time.Time
	rescuing      bool
	quietFrames   int
}

func (g *nearEndGuard) observeRender(frame []int16, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lastRender = now
	g.renderHistory = append(g.renderHistory, frame...)
	if extra := len(g.renderHistory) - renderHistorySamples; extra > 0 {
		copy(g.renderHistory, g.renderHistory[extra:])
		g.renderHistory = g.renderHistory[:renderHistorySamples]
	}
}

func (g *nearEndGuard) preserveNearEnd(raw, cancelled []int16, now time.Time) []int16 {
	if len(raw) == 0 || len(cancelled) == 0 {
		return cancelled
	}
	rawRMS := frameRMS(raw)
	if rawRMS < nearEndSpeechRMS {
		g.noteQuietFrame()
		return cancelled
	}
	if frameRMS(cancelled) >= rawRMS*severeSuppression {
		g.noteVoicedFrame()
		return cancelled
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.quietFrames = 0
	if g.rescuing {
		return raw
	}
	if g.lastRender.IsZero() || now.Sub(g.lastRender) > pipelineEchoTail {
		g.suppressedRun = g.suppressedRun[:0]
		g.rescuing = true
		return raw
	}
	g.suppressedRun = append(g.suppressedRun, raw...)
	if len(g.suppressedRun) > nearEndWindowSamples {
		g.suppressedRun = append(g.suppressedRun[:0], g.suppressedRun[len(g.suppressedRun)-nearEndWindowSamples:]...)
	}
	if len(g.suppressedRun) < nearEndWindowSamples {
		return cancelled
	}
	if maxRenderCorrelation(g.suppressedRun, g.renderHistory) < nearEndMaxEchoScore {
		g.rescuing = true
		return raw
	}
	return cancelled
}

func (g *nearEndGuard) noteQuietFrame() {
	g.mu.Lock()
	g.quietFrames++
	if g.quietFrames >= 5 {
		g.suppressedRun = g.suppressedRun[:0]
		g.rescuing = false
	}
	g.mu.Unlock()
}

func (g *nearEndGuard) noteVoicedFrame() {
	g.mu.Lock()
	g.quietFrames = 0
	g.mu.Unlock()
}

func maxRenderCorrelation(capture, render []int16) float64 {
	if len(capture) < nearEndWindowSamples || len(render) < nearEndWindowSamples {
		return 0
	}
	best := 0.0
	for start := 0; start+nearEndWindowSamples <= len(render); start += correlationLagStep {
		if score := normalizedCorrelation(capture[len(capture)-nearEndWindowSamples:], render[start:start+nearEndWindowSamples]); score > best {
			best = score
		}
	}
	return best
}

func normalizedCorrelation(a, b []int16) float64 {
	var sumA, sumB float64
	count := 0
	for i := 0; i < len(a) && i < len(b); i += correlationStride {
		sumA += float64(a[i])
		sumB += float64(b[i])
		count++
	}
	if count == 0 {
		return 0
	}
	meanA, meanB := sumA/float64(count), sumB/float64(count)
	var dot, energyA, energyB float64
	for i := 0; i < len(a) && i < len(b); i += correlationStride {
		x := float64(a[i]) - meanA
		y := float64(b[i]) - meanB
		dot += x * y
		energyA += x * x
		energyB += y * y
	}
	if energyA == 0 || energyB == 0 {
		return 0
	}
	return math.Abs(dot / math.Sqrt(energyA*energyB))
}
