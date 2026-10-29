// Package asrgate filters VAD-less ASR (FunASR) output: it decides whether the
// audio behind a recognition result is real human speech (accept / allow
// barge-in) or silence/line-noise that FunASR hallucinated into a whole
// sentence (reject). Shared by the SIP call path (internal/calls) and the
// standalone agent ASR path (internal/agentmedia) so both gate identically.
//
// When the neural VAD (Silero) is initialised it drives the decision — "is a
// human speaking" — so real voice barges in immediately (no char count) and
// noise is dropped. Otherwise it falls back to an RMS energy meter. Either way
// it only observes; it never drops frames (which would break FunASR
// segmentation).
package asrgate

import (
	"math"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/media/vad"
)

// Meter accounts the voiced/speech time behind each ASR utterance.
type Meter struct {
	det *vad.Detector // neural VAD; nil = RMS fallback

	threshold    float64       // RMS open threshold (int16 scale); fallback + hybrid gate
	minVoiced    time.Duration // min voiced/speech time for a final to count
	hybrid       bool          // Speaking() also honors strong RMS energy (double-talk VAD miss)
	hybridMargin float64       // how far above the tracked noise floor the hybrid gate sits
	relLevel     float64       // min utterance level as a fraction of the caller's; 0 = off
	bargeLevel   float64       // same, for an utterance over bot playback; 0 = off

	mu          sync.Mutex
	voiced      time.Duration
	utterance   []int16 // voiced audio behind the current utterance (capped)
	recentRMS   float64 // decayed peak RMS, for the hybrid Speaking() gate
	noiseFloor  float64 // tracked background level: falls fast, rises slowly
	callerLevel float64 // speech level of the utterances accepted so far on this call
	onset       time.Time // when the voiced run behind the current utterance began
	lastVoiced  time.Time // last frame that added voiced time
}

const (
	// Noise-floor tracker: drop quickly toward a new quiet level, creep up so a
	// long utterance cannot drag the floor into the speaker's own level.
	floorFallRate = 0.25
	floorRiseRate = 0.002
	// Voiced audio kept per utterance, for callers that want to know whose voice
	// it was. Two seconds is plenty to identify a speaker, and bounds the memory
	// a long utterance can take.
	maxUtteranceSamples = 2 * bus.SampleRate
	vadWindowDur        = time.Duration(vad.WindowSamples) * time.Second / bus.SampleRate
	// Caller level tracker: accepted utterances pull the reference halfway
	// toward themselves; each one rejected as too quiet lowers it by ~3dB, so a
	// caller who really did step away from the phone gets back in within a turn
	// or two instead of being locked out.
	callerLevelAlpha = 0.5
	callerLevelDecay = 0.7
	// A pause longer than this between voiced frames starts a new run, so a
	// stray blip well before the caller speaks does not date the utterance.
	onsetGap = time.Second
)

// New builds a meter from the process dialogpolicy (SIP_LLM_ASR_* /
// SIP_LLM_VAD_HYBRID_*). If Silero VAD is enabled it drives the decision;
// otherwise the RMS threshold is used.
func New() *Meter {
	p := dialogpolicy.Get()
	m := &Meter{
		threshold:    p.ASRGateRMS,
		minVoiced:    time.Duration(p.ASRMinVoicedMs) * time.Millisecond,
		hybrid:       p.VADHybridRMS,
		hybridMargin: p.VADHybridMargin,
	}
	if p.ASRRelLevelDB > 0 {
		m.relLevel = math.Pow(10, -p.ASRRelLevelDB/20)
	}
	if p.ASRBargeRelLevelDB > 0 {
		m.bargeLevel = math.Pow(10, -p.ASRBargeRelLevelDB/20)
	}
	if vad.Enabled() {
		m.det = vad.NewDetector()
	}
	return m
}

// Observe accounts a frame's speech/voiced duration. Called per frame from the
// audio pump (single goroutine).
func (m *Meter) Observe(samples []int16) {
	if m == nil {
		return
	}
	if m.det != nil {
		rms := frameRMS(samples)
		if m.hybrid && m.threshold > 0 {
			m.observeLevel(rms)
		}
		m.observeDetectedSpeech(samples, m.det.Feed(samples), rms)
		return
	}
	if m.threshold <= 0 || frameRMS(samples) < m.threshold {
		return
	}
	d := time.Duration(len(samples)) * time.Second / bus.SampleRate
	m.mu.Lock()
	m.voiced += d
	m.keepVoiced(samples)
	m.markVoiced(time.Now())
	m.mu.Unlock()
}

// observeDetectedSpeech accounts the same acoustic evidence used by Speaking.
// Neural VAD remains authoritative when it detects speech. When AEC suppresses
// double-talk enough for neural VAD to miss it, the adaptive RMS path may still
// open the onset gate; that path must also back the final utterance verdict, or
// one component announces real speech while the next rejects it as 0 ms.
func (m *Meter) observeDetectedSpeech(samples []int16, speechWindows int, rms float64) {
	voiced := time.Duration(speechWindows) * vadWindowDur
	if voiced == 0 && m.hybrid && m.threshold > 0 {
		m.mu.Lock()
		gate := m.hybridGate()
		m.mu.Unlock()
		if rms >= gate {
			voiced = time.Duration(len(samples)) * time.Second / bus.SampleRate
		}
	}
	if voiced == 0 {
		return
	}
	m.mu.Lock()
	m.voiced += voiced
	m.keepVoiced(samples)
	m.markVoiced(time.Now())
	m.mu.Unlock()
}

// markVoiced records when the voiced run behind the current utterance began.
// Caller holds mu.
func (m *Meter) markVoiced(now time.Time) {
	if m.onset.IsZero() || now.Sub(m.lastVoiced) > onsetGap {
		m.onset = now
	}
	m.lastVoiced = now
}

// keepVoiced collects the speech of the utterance being accumulated, up to the
// cap. Caller holds mu.
func (m *Meter) keepVoiced(samples []int16) {
	if room := maxUtteranceSamples - len(m.utterance); room > 0 {
		if len(samples) > room {
			samples = samples[:room]
		}
		m.utterance = append(m.utterance, samples...)
	}
}

// observeLevel folds one frame's RMS into the recent-speech peak and the
// background level the hybrid gate is measured against.
func (m *Meter) observeLevel(r float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r > m.recentRMS {
		m.recentRMS = r // fast attack
	} else {
		// ~800ms release hold: FunASR emits a stop-word's trailing syllable
		// ("等一下"→"下") after the user already paused, when instantaneous
		// energy has dropped. Holding recent speech keeps that lagging
		// fragment "speaking" so it reaches the barge classifier.
		m.recentRMS *= 0.97
	}
	switch {
	case m.noiseFloor == 0:
		m.noiseFloor = r
	case r < m.noiseFloor:
		m.noiseFloor += floorFallRate * (r - m.noiseFloor)
	default:
		m.noiseFloor += floorRiseRate * (r - m.noiseFloor)
	}
}

// hybridGate is the RMS bar the hybrid gate applies: the configured threshold,
// or a margin over the tracked background level when the caller's surroundings
// are louder than that. Caller holds mu.
func (m *Meter) hybridGate() float64 {
	if adaptive := m.noiseFloor * m.hybridMargin; adaptive > m.threshold {
		return adaptive
	}
	return m.threshold
}

// Verdict is the outcome of gating one finalised utterance, carrying the
// numbers behind the decision so a rejection can be explained in a log line.
type Verdict struct {
	OK       bool
	VoicedMs int
	MinMs    int
	FloorRMS int
	// TooQuiet means the utterance had enough speech but sat far below the
	// level the caller has been speaking at on this call: on a phone the voice
	// at the handset is much louder than one across the room, so this is a
	// distant voice or background audio that ASR turned into words.
	TooQuiet bool
	// TooQuietToBarge means the utterance arrived while the bot was talking
	// and sat below the stricter bar that applies then: a nearby voice that
	// would otherwise cut the bot off. Implies TooQuiet.
	TooQuietToBarge bool
	LevelRMS        int
	CallerRMS       int
	// Audio is the voiced audio behind the utterance (16 kHz mono, capped at a
	// couple of seconds), for callers that go on to ask whose voice it was.
	Audio []int16
}

// Accept reports whether enough real speech backed the utterance just finalised,
// then resets the accumulator.
func (m *Meter) Accept() bool {
	return m.AcceptUtterance(0).OK
}

// AcceptUtterance is Accept with an explicit minimum voiced time (0 = the
// configured default), for callers that relax the bar in context — e.g. right
// after the bot asked a question, where "对" is a legitimate whole answer.
func (m *Meter) AcceptUtterance(min time.Duration) Verdict {
	return m.AcceptUtteranceOver(min, false)
}

// AcceptUtteranceOver is AcceptUtterance for an utterance that may have been
// spoken over the bot's own playback. Cutting the bot off on a voice from
// across the room is far worse than asking a quiet caller to speak up, so over
// playback the utterance must come much closer to the caller's level. That
// rejection leaves the caller-level reference alone: it says nothing about
// how loud the caller is.
func (m *Meter) AcceptUtteranceOver(min time.Duration, overPlayback bool) Verdict {
	if m == nil || (m.det == nil && m.threshold <= 0) {
		return Verdict{OK: true}
	}
	if min <= 0 {
		min = m.minVoiced
	}
	m.mu.Lock()
	v, audio := m.voiced, m.utterance
	m.voiced, m.utterance, m.onset = 0, nil, time.Time{}
	floor := m.noiseFloor
	ok := v >= min
	level, caller := frameRMS(audio), m.callerLevel
	tooQuiet, bargeQuiet := false, false
	if ok && overPlayback && m.bargeLevel > 0 && level > 0 && caller > 0 && level < caller*m.bargeLevel {
		tooQuiet, bargeQuiet = true, true
	} else if ok && m.relLevel > 0 && level > 0 {
		switch {
		case caller == 0:
			m.callerLevel = level
		case level < caller*m.relLevel:
			tooQuiet = true
			m.callerLevel *= callerLevelDecay
		default:
			m.callerLevel += callerLevelAlpha * (level - caller)
		}
	}
	m.mu.Unlock()
	return Verdict{
		OK:              ok && !tooQuiet,
		VoicedMs:        int(v / time.Millisecond),
		MinMs:           int(min / time.Millisecond),
		FloorRMS:        int(floor),
		TooQuiet:        tooQuiet,
		TooQuietToBarge: bargeQuiet,
		LevelRMS:        int(level),
		CallerRMS:       int(caller),
		Audio:           audio,
	}
}

// MinVoicedMs is the voiced-time bar an utterance has to clear to be accepted,
// resolving the override the same way AcceptUtterance does (0 = the configured
// default). Callers gate barge-in on it so a fragment that could never be
// accepted as speech cannot duck the playback either: a lower bar for ducking
// than for accepting means noise wins the playback for as long as it takes the
// final to be thrown out, which the caller hears as the bot faltering.
func (m *Meter) MinVoicedMs(override time.Duration) int {
	if m == nil {
		return 0
	}
	if override <= 0 {
		override = m.minVoiced
	}
	return int(override / time.Millisecond)
}

// VoicedMs is the voiced time accumulated for the utterance in progress, for
// callers that gate an action on how much real speech is behind it so far.
// Resets with the utterance, like the bar AcceptUtterance applies at the end.
func (m *Meter) VoicedMs() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return int(m.voiced / time.Millisecond)
}

// UtteranceOnset is when the voiced run behind the utterance in progress began
// (zero when nothing has been voiced since the last final). Partials arrive
// well after the audio they describe, so this is the time to judge an
// utterance by, not when its text showed up.
func (m *Meter) UtteranceOnset() time.Time {
	if m == nil {
		return time.Time{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.onset
}

// Reset drops the utterance in progress without judging it, leaving the
// caller-level reference untouched: for an utterance thrown out on other
// grounds that must not teach the meter how loud the caller is.
func (m *Meter) Reset() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.voiced, m.utterance, m.onset = 0, nil, time.Time{}
	m.mu.Unlock()
}

// Speaking reports whether a human is currently talking — used to gate barge-in
// so noise partials never interrupt the bot. With no VAD it returns true.
func (m *Meter) Speaking() bool {
	if m == nil || m.det == nil {
		return true
	}
	if m.det.Speaking() {
		return true
	}
	// Hybrid: strong energy overrides a neural-VAD miss. In hands-free double-talk
	// the caller's own AEC suppresses their onset so Silero under-scores it, but
	// the residual is still well above the echo/noise floor. The bar is the
	// configured threshold or a margin over the tracked noise floor, whichever is
	// higher, so a loud environment raises it instead of holding the gate open.
	if m.hybrid && m.threshold > 0 {
		m.mu.Lock()
		r, gate := m.recentRMS, m.hybridGate()
		m.mu.Unlock()
		return r >= gate
	}
	return false
}

// HasVAD reports whether a neural VAD backs this meter. When false, Speaking()
// is a constant true and its rising edge is meaningless (callers that watch the
// edge for a voice-onset signal must skip it).
func (m *Meter) HasVAD() bool {
	return m != nil && m.det != nil
}

func frameRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		f := float64(s)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(samples)))
}
