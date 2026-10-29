package asrgate

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

func frames(v int16, n int) []int16 {
	s := make([]int16, n)
	for i := range s {
		s[i] = v
	}
	return s
}

func TestMeter(t *testing.T) {
	loud := frames(6000, 320) // 320 samples @16kHz = 20ms

	// Disabled meter accepts everything (no filtering).
	off := &Meter{threshold: 0}
	if !off.Accept() {
		t.Fatal("disabled meter must accept")
	}

	m := &Meter{threshold: 300, minVoiced: 100 * time.Millisecond}
	// No voiced audio → reject (FunASR silence hallucination).
	if m.Accept() {
		t.Fatal("no voiced audio should be rejected")
	}
	// 6 loud 20ms frames = 120ms ≥ 100ms → accept.
	for i := 0; i < 6; i++ {
		m.Observe(loud)
	}
	if !m.Accept() {
		t.Fatal("enough voiced audio should be accepted")
	}
	// Accept resets the accumulator.
	if m.Accept() {
		t.Fatal("accumulator should reset after accept")
	}
}

func TestMeterUtteranceOnsetAndReset(t *testing.T) {
	loud := frames(6000, 320)
	m := &Meter{threshold: 300, minVoiced: 20 * time.Millisecond, relLevel: 0.05}
	if !m.UtteranceOnset().IsZero() {
		t.Fatal("no speech yet: onset must be zero")
	}
	before := time.Now()
	m.Observe(loud)
	onset := m.UtteranceOnset()
	if onset.Before(before) {
		t.Fatal("onset must date the first voiced frame")
	}
	m.Observe(loud)
	if !m.UtteranceOnset().Equal(onset) {
		t.Fatal("continuing speech must keep the original onset")
	}

	// A voiced blip long before the utterance does not date it.
	m.mu.Lock()
	m.lastVoiced = time.Now().Add(-2 * onsetGap)
	m.mu.Unlock()
	m.Observe(loud)
	if !m.UtteranceOnset().After(onset) {
		t.Fatal("speech after a long pause starts a new run")
	}

	// Reset drops the utterance without learning the caller's level from it.
	m.Reset()
	if !m.UtteranceOnset().IsZero() || m.VoicedMs() != 0 || m.callerLevel != 0 {
		t.Fatal("Reset must clear the utterance and leave the caller level alone")
	}
	m.Observe(loud)
	if !m.AcceptUtterance(0).OK || !m.UtteranceOnset().IsZero() {
		t.Fatal("a judged utterance clears its onset")
	}
}

func TestMeterSilenceIgnored(t *testing.T) {
	silent := frames(0, 320)
	m := &Meter{threshold: 300, minVoiced: 100 * time.Millisecond}
	for i := 0; i < 100; i++ {
		m.Observe(silent)
	}
	if m.Accept() {
		t.Fatal("silence must not count as voiced")
	}
}

// On a street the fixed threshold is not enough: traffic alone peaks past it, so
// the gate must rise with the background level. Indoors it must not move.
func TestMeterHybridGateFollowsNoiseFloor(t *testing.T) {
	quiet := &Meter{threshold: 800, hybridMargin: dialogpolicy.Defaults().VADHybridMargin}
	for i := 0; i < 500; i++ {
		quiet.observeLevel(40)
	}
	if got := quiet.hybridGate(); got != 800 {
		t.Fatalf("quiet room should keep the configured gate, got %v", got)
	}

	street := &Meter{threshold: 800, hybridMargin: dialogpolicy.Defaults().VADHybridMargin}
	// A few seconds of traffic at the level seen in production (RMS 150~500).
	for i := 0; i < 2000; i++ {
		street.observeLevel(350)
	}
	if got := street.hybridGate(); got <= 800 {
		t.Fatalf("street noise should raise the gate above the configured 800, got %v", got)
	}
	// The 900-ish noise bursts that used to read as speech must now fall short.
	street.observeLevel(900)
	if street.recentRMS >= street.hybridGate() {
		t.Fatalf("a 900 burst must not clear the raised gate (%v)", street.hybridGate())
	}
}

// Barge-in is gated on how much speech the turn carries so far, so the count has
// to be readable mid-utterance and reset with it.
func TestMeterVoicedMsTracksUtterance(t *testing.T) {
	m := &Meter{threshold: 300, minVoiced: 450 * time.Millisecond}
	if got := m.VoicedMs(); got != 0 {
		t.Fatalf("fresh meter reports %dms", got)
	}
	for i := 0; i < 5; i++ { // 100ms: a far-field voice never gets much further
		m.Observe(frames(6000, 320))
	}
	if got := m.VoicedMs(); got != 100 {
		t.Fatalf("VoicedMs=%d want 100", got)
	}
	for i := 0; i < 10; i++ {
		m.Observe(frames(6000, 320))
	}
	if got := m.VoicedMs(); got != 300 {
		t.Fatalf("VoicedMs=%d want 300", got)
	}
	m.AcceptUtterance(0)
	if got := m.VoicedMs(); got != 0 {
		t.Fatalf("VoicedMs=%d after the utterance closed, want 0", got)
	}
}

// AEC can attenuate double-talk below neural VAD while leaving it clearly above
// the adaptive noise gate. The onset and final verdict must use that same
// evidence; otherwise playback stops but the transcript is discarded at 0 ms.
func TestMeterHybridEvidenceBacksFinalVerdict(t *testing.T) {
	m := &Meter{
		threshold:    800,
		minVoiced:    100 * time.Millisecond,
		hybrid:       true,
		hybridMargin: 4,
		noiseFloor:   100,
	}
	voice := frames(1200, 320)
	for i := 0; i < 6; i++ {
		m.observeDetectedSpeech(voice, 0, frameRMS(voice))
	}
	verdict := m.AcceptUtterance(0)
	if !verdict.OK || verdict.VoicedMs != 120 {
		t.Fatalf("hybrid-detected speech must back the final verdict: %+v", verdict)
	}
}

// MinVoicedMs has to resolve the override exactly as AcceptUtterance does, or
// barge-in would be gated on a different bar than acceptance.
func TestMeterMinVoicedMsMatchesAcceptUtterance(t *testing.T) {
	m := &Meter{threshold: 300, minVoiced: 450 * time.Millisecond}
	if got := m.MinVoicedMs(0); got != 450 {
		t.Fatalf("MinVoicedMs(0)=%d want the configured 450", got)
	}
	if got := m.MinVoicedMs(150 * time.Millisecond); got != 150 {
		t.Fatalf("MinVoicedMs(150ms)=%d want 150", got)
	}
	var nilMeter *Meter
	if got := nilMeter.MinVoicedMs(0); got != 0 {
		t.Fatalf("a nil meter gates nothing, got %d", got)
	}
}

// A one-word answer clears the relaxed bar but not the strict one.
func TestMeterAcceptUtteranceExplicitMin(t *testing.T) {
	m := &Meter{threshold: 300, minVoiced: 450 * time.Millisecond}
	for i := 0; i < 10; i++ { // 10 × 20ms = 200ms voiced
		m.Observe(frames(6000, 320))
	}
	if v := m.AcceptUtterance(150 * time.Millisecond); !v.OK || v.VoicedMs != 200 || v.MinMs != 150 {
		t.Fatalf("relaxed bar should accept: %+v", v)
	}
	for i := 0; i < 10; i++ {
		m.Observe(frames(6000, 320))
	}
	if v := m.AcceptUtterance(0); v.OK {
		t.Fatalf("strict bar should reject 200ms: %+v", v)
	}
}

// The speaker gate identifies the voice from what the meter kept, so the audio
// must be the utterance's own speech, bounded, and gone once it is handed over.
func TestMeterKeepsUtteranceAudio(t *testing.T) {
	m := &Meter{threshold: 300}
	for i := 0; i < 5; i++ {
		m.Observe(frames(6000, 320))
	}
	m.Observe(frames(10, 320)) // below the threshold: not this utterance's speech
	v := m.AcceptUtterance(0)
	if len(v.Audio) != 5*320 {
		t.Fatalf("kept %d samples, want %d", len(v.Audio), 5*320)
	}
	if next := m.AcceptUtterance(0); len(next.Audio) != 0 {
		t.Fatalf("audio must reset with the utterance, got %d samples", len(next.Audio))
	}

	for i := 0; i < maxUtteranceSamples/320+50; i++ {
		m.Observe(frames(6000, 320))
	}
	if got := len(m.AcceptUtterance(0).Audio); got != maxUtteranceSamples {
		t.Fatalf("a long utterance kept %d samples, want the %d cap", got, maxUtteranceSamples)
	}
}

func TestMeterFromEnv(t *testing.T) {
	t.Setenv("SIP_LLM_ASR_GATE_RMS", "")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "")
	p, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)
	if m := New(); m.threshold != 0 {
		t.Fatalf("empty env: threshold=%v", m.threshold)
	}
	t.Setenv("SIP_LLM_ASR_GATE_RMS", "400")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "250")
	p2, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dialogpolicy.Set(p2)
	m := New()
	if m.threshold != 400 {
		t.Fatalf("threshold=%v", m.threshold)
	}
	if m.minVoiced != 250*time.Millisecond {
		t.Fatalf("minVoiced=%v", m.minVoiced)
	}
}

// utterance feeds n 20ms frames at the given amplitude and finalises them.
func utterance(m *Meter, amp int16, n int) Verdict {
	for i := 0; i < n; i++ {
		m.Observe(frames(amp, 320))
	}
	return m.AcceptUtterance(0)
}

// A production call: the caller spoke at about -20dBFS, then a voice across the
// room at about -57dBFS was transcribed as "被QQQ也经打开" and answered.
func TestMeterRejectsVoiceFarBelowCaller(t *testing.T) {
	m := &Meter{threshold: 100, minVoiced: 100 * time.Millisecond, relLevel: 0.05}
	if v := utterance(m, 3300, 30); !v.OK {
		t.Fatal("the first utterance sets the caller level and must pass")
	}
	v := utterance(m, 150, 30)
	if v.OK || !v.TooQuiet {
		t.Fatalf("a voice ~27dB under the caller must be rejected, got %+v", v)
	}
	if v.LevelRMS != 150 || v.CallerRMS != 3300 {
		t.Fatalf("verdict must carry both levels, got level=%d caller=%d", v.LevelRMS, v.CallerRMS)
	}
}

// The caller's own level swings ~17dB within a call (speakerphone, moving the
// handset); that must never trip the gate.
func TestMeterKeepsCallerSpeakingSofter(t *testing.T) {
	m := &Meter{threshold: 100, minVoiced: 100 * time.Millisecond, relLevel: 0.05}
	utterance(m, 7678, 30)
	if v := utterance(m, 1086, 30); !v.OK {
		t.Fatalf("a 17dB softer caller must pass, got %+v", v)
	}
}

// A caller who really did step away keeps getting quieter answers through
// after a turn or two, instead of being locked out of their own call.
func TestMeterRecoversWhenCallerStaysQuiet(t *testing.T) {
	m := &Meter{threshold: 100, minVoiced: 100 * time.Millisecond, relLevel: 0.05}
	utterance(m, 6000, 30)
	accepted := false
	for i := 0; i < 4 && !accepted; i++ {
		accepted = utterance(m, 200, 30).OK
	}
	if !accepted {
		t.Fatal("a persistently quiet caller must be let back in within a few turns")
	}
}

func TestMeterRelativeLevelOff(t *testing.T) {
	m := &Meter{threshold: 100, minVoiced: 100 * time.Millisecond}
	utterance(m, 6000, 30)
	if v := utterance(m, 150, 30); !v.OK {
		t.Fatalf("with the relative gate off only absolute energy counts, got %+v", v)
	}
}

func utteranceOver(m *Meter, amp int16, n int) Verdict {
	for i := 0; i < n; i++ {
		m.Observe(frames(amp, 320))
	}
	return m.AcceptUtteranceOver(0, true)
}

// A browser test call: the caller spoke at ~4300-6000 RMS while voices nearby,
// 600-750 RMS, were transcribed as "你是个是头" / "我吃了你" and cut the bot off.
// Over playback they are rejected; the same level with the bot silent is still
// a turn, and the rejection does not lower the caller reference.
func TestMeterStricterLevelOverPlayback(t *testing.T) {
	m := &Meter{threshold: 100, minVoiced: 100 * time.Millisecond, relLevel: 0.05, bargeLevel: 0.2}
	utterance(m, 5000, 30)

	for _, amp := range []int16{745, 613} {
		v := utteranceOver(m, amp, 30)
		if v.OK || !v.TooQuietToBarge || !v.TooQuiet {
			t.Fatalf("a voice at %d under a 5000 caller must not interrupt, got %+v", amp, v)
		}
		if v.CallerRMS != 5000 {
			t.Fatalf("a barge rejection must not move the caller reference, got %d", v.CallerRMS)
		}
	}
	if v := utteranceOver(m, 4300, 30); !v.OK {
		t.Fatalf("the caller's own barge must pass, got %+v", v)
	}
	if v := utterance(m, 745, 30); !v.OK {
		t.Fatalf("with the bot silent only the normal far-voice bar applies, got %+v", v)
	}
}
