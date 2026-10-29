package calls

import "strings"

// PerformanceOverrides are optional per-call dialog-timing knobs supplied by
// callers (via call.listen, call.converse_external_agent, or /media/pcm start). Nil / zero /
// omitted fields mean "keep the FlowKit process env default".
type PerformanceOverrides struct {
	EndpointSilenceMs      *int     `json:"endpoint_silence_ms,omitempty"`
	EndpointSilenceRMS     *int     `json:"endpoint_silence_rms,omitempty"`
	TurnDetectionEnabled   *bool    `json:"turn_detection_enabled,omitempty"`
	TurnTriggerSilenceMs   *int     `json:"turn_trigger_silence_ms,omitempty"`
	TurnReaskSilenceMs     *int     `json:"turn_reask_silence_ms,omitempty"`
	TurnMinVoicedMs        *int     `json:"turn_min_voiced_ms,omitempty"`
	TurnCompleteThreshold  *float32 `json:"turn_complete_threshold,omitempty"`
	AdaptiveBargeIn        *bool    `json:"adaptive_barge_in,omitempty"`
	BargeSensitivity       string   `json:"barge_sensitivity,omitempty"` // sensitive|balanced|stable
	BargeFullPause         *bool    `json:"barge_full_pause,omitempty"`
	BargeDuck              *bool    `json:"barge_duck,omitempty"`
	BargeOnsetGraceMs      *int     `json:"barge_onset_grace_ms,omitempty"`
	BargeMaxPauseMs        *int     `json:"barge_max_pause_ms,omitempty"`
	GreetingLeadPadMs      *int     `json:"greeting_lead_pad_ms,omitempty"`
	TTSPrebufferMs         *int     `json:"tts_prebuffer_ms,omitempty"`
}

func (p *PerformanceOverrides) empty() bool {
	return p == nil || *p == (PerformanceOverrides{})
}

func mergeDialogConfig(base dialogConfig, p *PerformanceOverrides) dialogConfig {
	if p == nil {
		return base
	}
	if p.AdaptiveBargeIn != nil {
		base.Adaptive = *p.AdaptiveBargeIn
	}
	switch strings.ToLower(strings.TrimSpace(p.BargeSensitivity)) {
	case "sensitive":
		base.CandidateChars = 1
		base.ConfirmChars = 2
	case "balanced":
		base.CandidateChars = 2
		base.ConfirmChars = 4
	case "stable":
		base.CandidateChars = 3
		base.ConfirmChars = 6
	}
	if p.BargeFullPause != nil {
		base.FullPause = *p.BargeFullPause
	}
	if p.BargeDuck != nil {
		base.Duck = *p.BargeDuck
	}
	if p.BargeOnsetGraceMs != nil && *p.BargeOnsetGraceMs > 0 {
		base.OnsetGraceMs = *p.BargeOnsetGraceMs
	}
	if p.BargeMaxPauseMs != nil && *p.BargeMaxPauseMs > 0 {
		base.MaxPauseMs = *p.BargeMaxPauseMs
	}
	return base
}

func applyTurnOverrides(ep *turnEndpointer, p *PerformanceOverrides) {
	if ep == nil || p == nil {
		return
	}
	if p.EndpointSilenceRMS != nil && *p.EndpointSilenceRMS > 0 {
		ep.rmsThr = float64(*p.EndpointSilenceRMS)
	}
	if p.TurnTriggerSilenceMs != nil && *p.TurnTriggerSilenceMs > 0 {
		ep.triggerMs = *p.TurnTriggerSilenceMs
		ep.nextAskMs = ep.triggerMs
	}
	if p.TurnReaskSilenceMs != nil && *p.TurnReaskSilenceMs > 0 {
		ep.reAskMs = *p.TurnReaskSilenceMs
	}
	if p.EndpointSilenceMs != nil && *p.EndpointSilenceMs > 0 {
		// When Smart Turn is on, FunASR fallback silence tracks this max.
		ep.maxMs = *p.EndpointSilenceMs
	}
	if p.TurnMinVoicedMs != nil && *p.TurnMinVoicedMs > 0 {
		ep.minVoicedMs = *p.TurnMinVoicedMs
	}
	if p.TurnCompleteThreshold != nil && *p.TurnCompleteThreshold > 0 {
		ep.threshold = *p.TurnCompleteThreshold
	}
}

// endpointSilenceOverride returns (ms, rms, ok) when the call wants a FunASR
// forced-endpoint override (used when Smart Turn is off, or as fallback max).
func endpointSilenceOverride(p *PerformanceOverrides) (ms, rms int, ok bool) {
	if p == nil {
		return 0, 0, false
	}
	if p.EndpointSilenceMs != nil && *p.EndpointSilenceMs > 0 {
		ms = *p.EndpointSilenceMs
		ok = true
	}
	if p.EndpointSilenceRMS != nil && *p.EndpointSilenceRMS > 0 {
		rms = *p.EndpointSilenceRMS
		ok = true
	}
	return ms, rms, ok
}
