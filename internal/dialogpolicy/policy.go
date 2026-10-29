// Package dialogpolicy is the single source of truth for SIP_LLM_* dialog-behaviour
// thresholds. Values are parsed once at process start, cross-checked for
// contradictions that would otherwise surface as "weird call behaviour", then
// read by ASR gating, barge-in, denoise, and speaker verification.
//
// Per-call PerformanceOverrides (VoiceSpeed) still layer on top of the process
// Dialog subset via calls.mergeDialogConfig — they never re-parse env.
package dialogpolicy

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Source records whether a field's effective value came from the environment or
// from the compiled-in default. Used only for the startup dump.
type Source string

const (
	SourceEnv     Source = "env"
	SourceDefault Source = "default"
)

// Policy is the resolved, validated set of dialog-behaviour knobs.
type Policy struct {
	Greeting       string
	GreetingSource Source

	BargeInMinChars       int
	BargeInMinCharsSource Source

	BargeInOnStart       bool
	BargeInOnStartSource Source

	GreetingLeadPadMs       int
	GreetingLeadPadMsSource Source

	AdaptiveBargeIn       bool
	AdaptiveBargeInSource Source

	BargeCandidateChars       int
	BargeCandidateCharsSource Source

	BargeConfirmChars       int
	BargeConfirmCharsSource Source

	BargeMaxPauseMs       int
	BargeMaxPauseMsSource Source

	BargeOnsetGraceMs       int
	BargeOnsetGraceMsSource Source

	BargeDuckGain       float64
	BargeDuckGainSource Source

	BargeFullPause       bool
	BargeFullPauseSource Source

	// BargeDuck lowers playback volume at the barge candidate stage. False
	// never lowers it: every stage that would duck pauses playback instead.
	BargeDuck       bool
	BargeDuckSource Source

	BargeOnsetAction       string
	BargeOnsetActionSource Source

	BargeFalseInterruptMs       int
	BargeFalseInterruptMsSource Source

	BargeResumeFalseInterrupt       bool
	BargeResumeFalseInterruptSource Source

	ASRGateRMS       float64
	ASRGateRMSSource Source

	ASRMinVoicedMs       int
	ASRMinVoicedMsSource Source

	ASRMinVoicedShortMs       int
	ASRMinVoicedShortMsSource Source

	ASRRelLevelDB       float64
	ASRRelLevelDBSource Source

	// ASRBargeRelLevelDB is the stricter caller-relative level bar for an
	// utterance finalised while the bot is talking (0 = same as ASRRelLevelDB).
	ASRBargeRelLevelDB       float64
	ASRBargeRelLevelDBSource Source

	CommandMinVoicedMs       int
	CommandMinVoicedMsSource Source

	// AECWarmupMs is how long after the bot's first audio on a SIP call an
	// utterance that starts over playback may only interrupt with a stop word:
	// the handset's echo canceller is still adapting and often leaks the bot's
	// opening words back up the line (0 = off).
	AECWarmupMs       int
	AECWarmupMsSource Source

	VADHybridRMS       bool
	VADHybridRMSSource Source

	VADHybridMargin       float64
	VADHybridMarginSource Source

	DenoiseMix       float64
	DenoiseMixSource Source

	SpkThreshold       float64
	SpkThresholdSource Source

	SpkMinEnrollMs       int
	SpkMinEnrollMsSource Source

	SpkReenrollAfter       int
	SpkReenrollAfterSource Source

	SpkMaxSpeakers       int
	SpkMaxSpeakersSource Source

	SpkHandoverTurns       int
	SpkHandoverTurnsSource Source

	SpkHandoverLevelRatio       float64
	SpkHandoverLevelRatioSource Source
}

// Defaults match the pre-centralisation per-site fallbacks exactly. Changing a
// number here changes live call behaviour — do not "tune" without an explicit ask.
func Defaults() Policy {
	return Policy{
		Greeting:                        "您好，请问有什么可以帮您？",
		GreetingSource:                  SourceDefault,
		BargeInMinChars:                 2,
		BargeInMinCharsSource:           SourceDefault,
		BargeInOnStart:                  false,
		BargeInOnStartSource:            SourceDefault,
		GreetingLeadPadMs:               1000,
		GreetingLeadPadMsSource:         SourceDefault,
		AdaptiveBargeIn:                 false,
		AdaptiveBargeInSource:           SourceDefault,
		BargeCandidateChars:             2,
		BargeCandidateCharsSource:       SourceDefault,
		BargeConfirmChars:               4,
		BargeConfirmCharsSource:         SourceDefault,
		BargeMaxPauseMs:                 2500,
		BargeMaxPauseMsSource:           SourceDefault,
		BargeOnsetGraceMs:               600,
		BargeOnsetGraceMsSource:         SourceDefault,
		BargeDuckGain:                   0.25,
		BargeDuckGainSource:             SourceDefault,
		BargeFullPause:                  true,
		BargeFullPauseSource:            SourceDefault,
		BargeDuck:                       true,
		BargeDuckSource:                 SourceDefault,
		BargeOnsetAction:                "none",
		BargeOnsetActionSource:          SourceDefault,
		BargeFalseInterruptMs:           2000,
		BargeFalseInterruptMsSource:     SourceDefault,
		BargeResumeFalseInterrupt:       true,
		BargeResumeFalseInterruptSource: SourceDefault,
		ASRGateRMS:                      0,
		ASRGateRMSSource:                SourceDefault,
		ASRMinVoicedMs:                  200,
		ASRMinVoicedMsSource:            SourceDefault,
		ASRMinVoicedShortMs:             150,
		ASRMinVoicedShortMsSource:       SourceDefault,
		ASRRelLevelDB:                   26,
		ASRRelLevelDBSource:             SourceDefault,
		ASRBargeRelLevelDB:              14,
		ASRBargeRelLevelDBSource:        SourceDefault,
		CommandMinVoicedMs:              500,
		CommandMinVoicedMsSource:        SourceDefault,
		AECWarmupMs:                     3000,
		AECWarmupMsSource:               SourceDefault,
		VADHybridRMS:                    true,
		VADHybridRMSSource:              SourceDefault,
		VADHybridMargin:                 4.0,
		VADHybridMarginSource:           SourceDefault,
		DenoiseMix:                      0.7,
		DenoiseMixSource:                SourceDefault,
		SpkThreshold:                    0.45,
		SpkThresholdSource:              SourceDefault,
		SpkMinEnrollMs:                  600,
		SpkMinEnrollMsSource:            SourceDefault,
		SpkReenrollAfter:                3,
		SpkReenrollAfterSource:          SourceDefault,
		SpkMaxSpeakers:                  4,
		SpkMaxSpeakersSource:            SourceDefault,
		SpkHandoverTurns:                2,
		SpkHandoverTurnsSource:          SourceDefault,
		SpkHandoverLevelRatio:           0.5,
		SpkHandoverLevelRatioSource:     SourceDefault,
	}
}

// LoadFromEnv parses every SIP_LLM_* dialog knob with the same acceptance rules
// the scattered readers used (invalid/empty → keep default), then Validate.
func LoadFromEnv() (*Policy, error) {
	p := Defaults()
	parseEnv(&p)
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

func parseEnv(p *Policy) {
	if v := os.Getenv("SIP_LLM_GREETING"); v != "" {
		p.Greeting = v
		p.GreetingSource = SourceEnv
	}
	if v := os.Getenv("SIP_LLM_BARGE_IN_MIN_CHARS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.BargeInMinChars = n
			p.BargeInMinCharsSource = SourceEnv
		}
	}
	if v := os.Getenv("SIP_LLM_GREETING_LEAD_SILENCE_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.GreetingLeadPadMs = n
			p.GreetingLeadPadMsSource = SourceEnv
		}
	}
	if v := os.Getenv("SIP_LLM_BARGE_IN_ON_START"); v != "" {
		lv := strings.ToLower(strings.TrimSpace(v))
		p.BargeInOnStart = lv == "1" || lv == "true" || lv == "yes"
		p.BargeInOnStartSource = SourceEnv
	}
	if v := envFlag("SIP_LLM_ADAPTIVE_BARGE_IN"); v != nil {
		p.AdaptiveBargeIn = *v
		p.AdaptiveBargeInSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_BARGE_CANDIDATE_CHARS"); n > 0 {
		p.BargeCandidateChars = n
		p.BargeCandidateCharsSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_BARGE_CONFIRM_CHARS"); n > 0 {
		p.BargeConfirmChars = n
		p.BargeConfirmCharsSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_BARGE_MAX_PAUSE_MS"); n > 0 {
		p.BargeMaxPauseMs = n
		p.BargeMaxPauseMsSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_BARGE_ONSET_GRACE_MS"); n > 0 {
		p.BargeOnsetGraceMs = n
		p.BargeOnsetGraceMsSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_BARGE_DUCK_GAIN")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			p.BargeDuckGain = f
			p.BargeDuckGainSource = SourceEnv
		}
	}
	if v := envFlag("SIP_LLM_BARGE_FULL_PAUSE"); v != nil {
		p.BargeFullPause = *v
		p.BargeFullPauseSource = SourceEnv
	}
	if v := envFlag("SIP_LLM_BARGE_DUCK"); v != nil {
		p.BargeDuck = *v
		p.BargeDuckSource = SourceEnv
	}
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv("SIP_LLM_BARGE_ONSET_ACTION"))); v {
	case "none", "duck", "pause":
		p.BargeOnsetAction = v
		p.BargeOnsetActionSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_BARGE_FALSE_INTERRUPT_MS"); n > 0 {
		p.BargeFalseInterruptMs = n
		p.BargeFalseInterruptMsSource = SourceEnv
	}
	if v := envFlag("SIP_LLM_BARGE_RESUME_FALSE_INTERRUPT"); v != nil {
		p.BargeResumeFalseInterrupt = *v
		p.BargeResumeFalseInterruptSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_ASR_GATE_RMS")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			p.ASRGateRMS = f
			p.ASRGateRMSSource = SourceEnv
		}
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_ASR_MIN_VOICED_MS")); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			p.ASRMinVoicedMs = ms
			p.ASRMinVoicedMsSource = SourceEnv
		}
	}
	if n := envPosInt("SIP_LLM_ASR_MIN_VOICED_SHORT_MS"); n > 0 {
		p.ASRMinVoicedShortMs = n
		p.ASRMinVoicedShortMsSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_ASR_REL_LEVEL_DB")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			p.ASRRelLevelDB = f
			p.ASRRelLevelDBSource = SourceEnv
		}
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_ASR_BARGE_REL_LEVEL_DB")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			p.ASRBargeRelLevelDB = f
			p.ASRBargeRelLevelDBSource = SourceEnv
		}
	}
	if n := envPosInt("SIP_LLM_COMMAND_MIN_VOICED_MS"); n > 0 {
		p.CommandMinVoicedMs = n
		p.CommandMinVoicedMsSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_AEC_WARMUP_MS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.AECWarmupMs = n
			p.AECWarmupMsSource = SourceEnv
		}
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_VAD_HYBRID_RMS")); v != "" {
		lv := strings.ToLower(v)
		p.VADHybridRMS = lv == "1" || lv == "true" || lv == "yes"
		p.VADHybridRMSSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_VAD_HYBRID_MARGIN")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			p.VADHybridMargin = f
			p.VADHybridMarginSource = SourceEnv
		}
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_DENOISE_MIX")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			p.DenoiseMix = f
			p.DenoiseMixSource = SourceEnv
		}
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_SPK_THRESHOLD")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			p.SpkThreshold = f
			p.SpkThresholdSource = SourceEnv
		}
	}
	if n := envPosInt("SIP_LLM_SPK_MIN_ENROLL_MS"); n > 0 {
		p.SpkMinEnrollMs = n
		p.SpkMinEnrollMsSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_SPK_REENROLL_AFTER"); n > 0 {
		p.SpkReenrollAfter = n
		p.SpkReenrollAfterSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_SPK_MAX_SPEAKERS"); n > 0 {
		p.SpkMaxSpeakers = n
		p.SpkMaxSpeakersSource = SourceEnv
	}
	if n := envPosInt("SIP_LLM_SPK_HANDOVER_TURNS"); n > 0 {
		p.SpkHandoverTurns = n
		p.SpkHandoverTurnsSource = SourceEnv
	}
	if v := strings.TrimSpace(os.Getenv("SIP_LLM_SPK_HANDOVER_LEVEL_RATIO")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			p.SpkHandoverLevelRatio = f
			p.SpkHandoverLevelRatioSource = SourceEnv
		}
	}
}

// Validate enforces relationships that the previously scattered readers could
// not see. Each check names both sides of the conflict for the error message.
func (p *Policy) Validate() error {
	if p == nil {
		return fmt.Errorf("dialogpolicy: nil policy")
	}
	// Partial-driven barge-in and final acceptance share the same voiced bars
	// (see asrgate.Meter.MinVoicedMs). The short bar is a relaxation after a
	// question — it must not exceed the regular bar, or "relaxed" becomes stricter
	// and one-word answers fail while barge-in still expects them.
	if p.ASRMinVoicedShortMs > p.ASRMinVoicedMs {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_ASR_MIN_VOICED_SHORT_MS (%d) must be <= SIP_LLM_ASR_MIN_VOICED_MS (%d): a higher short bar rejects answers the regular bar would accept, and barge-in uses the same pair",
			p.ASRMinVoicedShortMs, p.ASRMinVoicedMs,
		)
	}
	// Irreversible commands need at least as much voiced evidence as a normal
	// accepted turn; a lower command bar is dead/weak safety (every accepted
	// utterance would already clear it).
	if p.CommandMinVoicedMs < p.ASRMinVoicedMs {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_COMMAND_MIN_VOICED_MS (%d) must be >= SIP_LLM_ASR_MIN_VOICED_MS (%d): transfer/hang-up must not be weaker than utterance acceptance",
			p.CommandMinVoicedMs, p.ASRMinVoicedMs,
		)
	}
	// Adaptive barge escalates duck → yield by content length; confirm below
	// candidate skips the duck stage for every utterance that reaches yield.
	if p.BargeCandidateChars > p.BargeConfirmChars {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_BARGE_CANDIDATE_CHARS (%d) must be <= SIP_LLM_BARGE_CONFIRM_CHARS (%d): duck stage would never run before yield",
			p.BargeCandidateChars, p.BargeConfirmChars,
		)
	}
	if p.BargeDuckGain <= 0 || p.BargeDuckGain > 1 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_BARGE_DUCK_GAIN (%v) must be in (0, 1]",
			p.BargeDuckGain,
		)
	}
	if p.DenoiseMix < 0 || p.DenoiseMix > 1 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_DENOISE_MIX (%v) must be in [0, 1]",
			p.DenoiseMix,
		)
	}
	// Cosine similarity is bounded; values above 1 reject every enrolled match.
	if p.SpkThreshold <= 0 || p.SpkThreshold > 1 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_SPK_THRESHOLD (%v) must be in (0, 1]",
			p.SpkThreshold,
		)
	}
	// Speaker tracking needs room for at least the caller plus one other voice;
	// with a single slot every new voice would evict the caller's own cluster.
	if p.SpkMaxSpeakers < 2 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_SPK_MAX_SPEAKERS (%d) must be >= 2",
			p.SpkMaxSpeakers,
		)
	}
	// A handover on the strength of one utterance would let a single loud
	// passer-by inherit the call.
	if p.SpkHandoverTurns < 2 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_SPK_HANDOVER_TURNS (%d) must be >= 2",
			p.SpkHandoverTurns,
		)
	}
	if p.SpkHandoverLevelRatio <= 0 || p.SpkHandoverLevelRatio > 1 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_SPK_HANDOVER_LEVEL_RATIO (%v) must be in (0, 1]",
			p.SpkHandoverLevelRatio,
		)
	}
	if p.VADHybridMargin <= 0 {
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_VAD_HYBRID_MARGIN (%v) must be > 0",
			p.VADHybridMargin,
		)
	}
	switch p.BargeOnsetAction {
	case "none", "duck", "pause":
	default:
		return fmt.Errorf(
			"dialogpolicy: SIP_LLM_BARGE_ONSET_ACTION (%q) must be none|duck|pause",
			p.BargeOnsetAction,
		)
	}
	return nil
}

// LogEffective emits one structured line with every effective value and whether
// it came from env or the compiled default — the first question when debugging
// a live call ("what was this process running?").
func (p *Policy) LogEffective(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if p == nil {
		log.Warn("dialogpolicy: no policy installed")
		return
	}
	log.Info("dialogpolicy effective",
		"greeting", p.Greeting, "greeting_source", p.GreetingSource,
		"barge_in_min_chars", p.BargeInMinChars, "barge_in_min_chars_source", p.BargeInMinCharsSource,
		"barge_in_on_start", p.BargeInOnStart, "barge_in_on_start_source", p.BargeInOnStartSource,
		"greeting_lead_pad_ms", p.GreetingLeadPadMs, "greeting_lead_pad_ms_source", p.GreetingLeadPadMsSource,
		"adaptive_barge_in", p.AdaptiveBargeIn, "adaptive_barge_in_source", p.AdaptiveBargeInSource,
		"barge_candidate_chars", p.BargeCandidateChars, "barge_candidate_chars_source", p.BargeCandidateCharsSource,
		"barge_confirm_chars", p.BargeConfirmChars, "barge_confirm_chars_source", p.BargeConfirmCharsSource,
		"barge_max_pause_ms", p.BargeMaxPauseMs, "barge_max_pause_ms_source", p.BargeMaxPauseMsSource,
		"barge_onset_grace_ms", p.BargeOnsetGraceMs, "barge_onset_grace_ms_source", p.BargeOnsetGraceMsSource,
		"barge_duck_gain", p.BargeDuckGain, "barge_duck_gain_source", p.BargeDuckGainSource,
		"barge_full_pause", p.BargeFullPause, "barge_full_pause_source", p.BargeFullPauseSource,
		"barge_duck", p.BargeDuck, "barge_duck_source", p.BargeDuckSource,
		"barge_onset_action", p.BargeOnsetAction, "barge_onset_action_source", p.BargeOnsetActionSource,
		"barge_false_interrupt_ms", p.BargeFalseInterruptMs, "barge_false_interrupt_ms_source", p.BargeFalseInterruptMsSource,
		"barge_resume_false_interrupt", p.BargeResumeFalseInterrupt, "barge_resume_false_interrupt_source", p.BargeResumeFalseInterruptSource,
		"asr_gate_rms", p.ASRGateRMS, "asr_gate_rms_source", p.ASRGateRMSSource,
		"asr_min_voiced_ms", p.ASRMinVoicedMs, "asr_min_voiced_ms_source", p.ASRMinVoicedMsSource,
		"asr_min_voiced_short_ms", p.ASRMinVoicedShortMs, "asr_min_voiced_short_ms_source", p.ASRMinVoicedShortMsSource,
		"asr_rel_level_db", p.ASRRelLevelDB, "asr_rel_level_db_source", p.ASRRelLevelDBSource,
		"asr_barge_rel_level_db", p.ASRBargeRelLevelDB, "asr_barge_rel_level_db_source", p.ASRBargeRelLevelDBSource,
		"command_min_voiced_ms", p.CommandMinVoicedMs, "command_min_voiced_ms_source", p.CommandMinVoicedMsSource,
		"aec_warmup_ms", p.AECWarmupMs, "aec_warmup_ms_source", p.AECWarmupMsSource,
		"vad_hybrid_rms", p.VADHybridRMS, "vad_hybrid_rms_source", p.VADHybridRMSSource,
		"vad_hybrid_margin", p.VADHybridMargin, "vad_hybrid_margin_source", p.VADHybridMarginSource,
		"denoise_mix", p.DenoiseMix, "denoise_mix_source", p.DenoiseMixSource,
		"spk_threshold", p.SpkThreshold, "spk_threshold_source", p.SpkThresholdSource,
		"spk_min_enroll_ms", p.SpkMinEnrollMs, "spk_min_enroll_ms_source", p.SpkMinEnrollMsSource,
		"spk_reenroll_after", p.SpkReenrollAfter, "spk_reenroll_after_source", p.SpkReenrollAfterSource,
		"spk_max_speakers", p.SpkMaxSpeakers, "spk_max_speakers_source", p.SpkMaxSpeakersSource,
		"spk_handover_turns", p.SpkHandoverTurns, "spk_handover_turns_source", p.SpkHandoverTurnsSource,
		"spk_handover_level_ratio", p.SpkHandoverLevelRatio,
		"spk_handover_level_ratio_source", p.SpkHandoverLevelRatioSource,
	)
}

var (
	mu     sync.RWMutex
	global *Policy
)

// Set installs the process-wide policy. Called once from main after LoadFromEnv.
func Set(p *Policy) {
	mu.Lock()
	defer mu.Unlock()
	global = p
}

// Get returns the installed policy. When none is installed (unit tests that
// never call Set), returns Defaults so readers keep working with stock values.
func Get() Policy {
	mu.RLock()
	p := global
	mu.RUnlock()
	if p == nil {
		return Defaults()
	}
	return *p
}

// ResetForTest clears the process policy so the next Set/LoadFromEnv sees a
// clean slate. Production code must not call this.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()
	global = nil
}

func envFlag(key string) *bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return nil
	}
	b := v == "1" || v == "true" || v == "yes"
	return &b
}

func envPosInt(key string) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}
