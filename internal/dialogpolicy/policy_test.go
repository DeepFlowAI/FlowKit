package dialogpolicy

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// isolateEnv blanks every SIP_LLM_* var for the duration of the test. Without
// it a developer who sourced a real .env file (to run the service locally) sees
// failures that CI never reproduces, because these tests assert that knobs they
// did not set stay at their defaults. An empty value reads as unset here.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if k, _, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, "SIP_LLM_") {
			t.Setenv(k, "")
		}
	}
}

func TestDefaultsMatchPreCentralisationBaseline(t *testing.T) {
	p := Defaults()
	// Baseline recorded from the scattered readers before centralisation.
	// One assertion per SIP_LLM_* knob — behaviour-unchanged contract.
	cases := []struct {
		name string
		got  any
		want any
	}{
		{"SIP_LLM_GREETING", p.Greeting, "您好，请问有什么可以帮您？"},
		{"SIP_LLM_BARGE_IN_MIN_CHARS", p.BargeInMinChars, 2},
		{"SIP_LLM_BARGE_IN_ON_START", p.BargeInOnStart, false},
		{"SIP_LLM_GREETING_LEAD_SILENCE_MS", p.GreetingLeadPadMs, 1000},
		{"SIP_LLM_ADAPTIVE_BARGE_IN", p.AdaptiveBargeIn, false},
		{"SIP_LLM_BARGE_CANDIDATE_CHARS", p.BargeCandidateChars, 2},
		{"SIP_LLM_BARGE_CONFIRM_CHARS", p.BargeConfirmChars, 4},
		{"SIP_LLM_BARGE_MAX_PAUSE_MS", p.BargeMaxPauseMs, 2500},
		{"SIP_LLM_BARGE_ONSET_GRACE_MS", p.BargeOnsetGraceMs, 600},
		{"SIP_LLM_BARGE_DUCK_GAIN", p.BargeDuckGain, 0.25},
		{"SIP_LLM_BARGE_FULL_PAUSE", p.BargeFullPause, true},
		{"SIP_LLM_BARGE_ONSET_ACTION", p.BargeOnsetAction, "none"},
		{"SIP_LLM_BARGE_FALSE_INTERRUPT_MS", p.BargeFalseInterruptMs, 2000},
		{"SIP_LLM_BARGE_RESUME_FALSE_INTERRUPT", p.BargeResumeFalseInterrupt, true},
		{"SIP_LLM_ASR_GATE_RMS", p.ASRGateRMS, 0.0},
		{"SIP_LLM_ASR_MIN_VOICED_MS", p.ASRMinVoicedMs, 200},
		{"SIP_LLM_ASR_MIN_VOICED_SHORT_MS", p.ASRMinVoicedShortMs, 150},
		{"SIP_LLM_ASR_REL_LEVEL_DB", p.ASRRelLevelDB, 26.0},
		{"SIP_LLM_ASR_BARGE_REL_LEVEL_DB", p.ASRBargeRelLevelDB, 14.0},
		{"SIP_LLM_COMMAND_MIN_VOICED_MS", p.CommandMinVoicedMs, 500},
		{"SIP_LLM_VAD_HYBRID_RMS", p.VADHybridRMS, true},
		{"SIP_LLM_VAD_HYBRID_MARGIN", p.VADHybridMargin, 4.0},
		{"SIP_LLM_DENOISE_MIX", p.DenoiseMix, 0.7},
		{"SIP_LLM_SPK_THRESHOLD", p.SpkThreshold, 0.45},
		{"SIP_LLM_SPK_MIN_ENROLL_MS", p.SpkMinEnrollMs, 600},
		{"SIP_LLM_SPK_REENROLL_AFTER", p.SpkReenrollAfter, 3},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s default=%v want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadFromEnvMarksSources(t *testing.T) {
	isolateEnv(t)
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "450")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_SHORT_MS", "150")
	t.Setenv("SIP_LLM_COMMAND_MIN_VOICED_MS", "500")
	t.Setenv("SIP_LLM_BARGE_DUCK_GAIN", "0.3")
	p, err := LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if p.ASRMinVoicedMs != 450 || p.ASRMinVoicedMsSource != SourceEnv {
		t.Fatalf("ASRMinVoicedMs=%d source=%s", p.ASRMinVoicedMs, p.ASRMinVoicedMsSource)
	}
	if p.BargeDuckGain != 0.3 || p.BargeDuckGainSource != SourceEnv {
		t.Fatalf("BargeDuckGain=%v source=%s", p.BargeDuckGain, p.BargeDuckGainSource)
	}
	if p.BargeCandidateChars != 2 || p.BargeCandidateCharsSource != SourceDefault {
		t.Fatalf("unset candidate should stay default, got %d source=%s",
			p.BargeCandidateChars, p.BargeCandidateCharsSource)
	}
}

func TestValidateShortVoicedAboveRegular(t *testing.T) {
	p := Defaults()
	p.ASRMinVoicedMs = 200
	p.ASRMinVoicedShortMs = 300
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	msg := err.Error()
	if !strings.Contains(msg, "300") || !strings.Contains(msg, "200") {
		t.Fatalf("error should name both values, got %q", msg)
	}
	if !strings.Contains(msg, "SIP_LLM_ASR_MIN_VOICED_SHORT_MS") ||
		!strings.Contains(msg, "SIP_LLM_ASR_MIN_VOICED_MS") {
		t.Fatalf("error should name both env keys, got %q", msg)
	}
}

func TestValidateCommandWeakerThanASR(t *testing.T) {
	p := Defaults()
	p.ASRMinVoicedMs = 450
	p.CommandMinVoicedMs = 200
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	msg := err.Error()
	if !strings.Contains(msg, "200") || !strings.Contains(msg, "450") {
		t.Fatalf("error should name both values, got %q", msg)
	}
	if !strings.Contains(msg, "SIP_LLM_COMMAND_MIN_VOICED_MS") ||
		!strings.Contains(msg, "SIP_LLM_ASR_MIN_VOICED_MS") {
		t.Fatalf("error should name both env keys, got %q", msg)
	}
}

func TestValidateCandidateAboveConfirm(t *testing.T) {
	p := Defaults()
	p.BargeCandidateChars = 6
	p.BargeConfirmChars = 4
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	msg := err.Error()
	if !strings.Contains(msg, "6") || !strings.Contains(msg, "4") {
		t.Fatalf("error should name both values, got %q", msg)
	}
	if !strings.Contains(msg, "SIP_LLM_BARGE_CANDIDATE_CHARS") ||
		!strings.Contains(msg, "SIP_LLM_BARGE_CONFIRM_CHARS") {
		t.Fatalf("error should name both env keys, got %q", msg)
	}
}

func TestValidateDuckGainOutOfRange(t *testing.T) {
	p := Defaults()
	p.BargeDuckGain = 1.5
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "1.5") || !strings.Contains(err.Error(), "SIP_LLM_BARGE_DUCK_GAIN") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestValidateDenoiseMixOutOfRange(t *testing.T) {
	p := Defaults()
	p.DenoiseMix = -0.1
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "SIP_LLM_DENOISE_MIX") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestValidateSpkThresholdOutOfRange(t *testing.T) {
	p := Defaults()
	p.SpkThreshold = 1.2
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	msg := err.Error()
	if !strings.Contains(msg, "1.2") || !strings.Contains(msg, "SIP_LLM_SPK_THRESHOLD") {
		t.Fatalf("got %q", msg)
	}
}

func TestValidateHybridMarginNonPositive(t *testing.T) {
	p := Defaults()
	p.VADHybridMargin = 0
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "SIP_LLM_VAD_HYBRID_MARGIN") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestValidateOnsetActionInvalid(t *testing.T) {
	p := Defaults()
	p.BargeOnsetAction = "yell"
	err := p.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "yell") || !strings.Contains(err.Error(), "SIP_LLM_BARGE_ONSET_ACTION") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestLoadFromEnvRejectsConflictingEnv(t *testing.T) {
	isolateEnv(t)
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "150")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_SHORT_MS", "450")
	_, err := LoadFromEnv()
	if err == nil {
		t.Fatal("expected LoadFromEnv to reject conflicting voiced bars")
	}
	if !strings.Contains(err.Error(), "450") || !strings.Contains(err.Error(), "150") {
		t.Fatalf("got %q", err.Error())
	}
}

func TestLogEffectiveIncludesSources(t *testing.T) {
	isolateEnv(t)
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "450")
	t.Setenv("SIP_LLM_COMMAND_MIN_VOICED_MS", "500")
	p, err := LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	p.LogEffective(log)
	out := buf.String()
	if !strings.Contains(out, "dialogpolicy effective") {
		t.Fatalf("missing message: %s", out)
	}
	if !strings.Contains(out, "asr_min_voiced_ms=450") {
		t.Fatalf("missing value: %s", out)
	}
	if !strings.Contains(out, "asr_min_voiced_ms_source=env") {
		t.Fatalf("missing env source: %s", out)
	}
	if !strings.Contains(out, "barge_candidate_chars_source=default") {
		t.Fatalf("missing default source: %s", out)
	}
}

func TestGetFallsBackToDefaults(t *testing.T) {
	ResetForTest()
	p := Get()
	if p.ASRMinVoicedMs != 200 {
		t.Fatalf("Get without Set should return defaults, got %d", p.ASRMinVoicedMs)
	}
}
