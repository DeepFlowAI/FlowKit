package calls

import (
	"log/slog"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

// dialogConfig holds the adaptive barge-in / turn-taking parameters shared by
// every agent that owns a call leg — the kernel auto-LLM agent (InboundLLMAgent)
// and the external-brain converse agent (call.converse_external_agent). Loading
// them once, from the same env keys, keeps behaviour identical across SIP and
// WebRTC so the logic lives in exactly one place.
type dialogConfig struct {
	Adaptive       bool    // SIP_LLM_ADAPTIVE_BARGE_IN: watch→duck→yield→confirm escalation
	CandidateChars int     // content runes → duck (candidate)
	ConfirmChars   int     // content runes → yield (lossless pause, reply kept)
	MaxPauseMs     int     // safety cap: unwind a duck never resolved by speech_end
	OnsetGraceMs   int     // watching with no real content → back to idle
	FullPause      bool    // yielded stage uses lossless pause (true) vs duck (false)
	Duck           bool    // SIP_LLM_BARGE_DUCK: false pauses wherever a duck would apply
	DuckGain       float64 // playback gain while ducked

	OnsetAction             string // action at VAD onset: none (default) | duck | pause
	FalseInterruptTimeoutMs int    // yielded with no speech_end → resume playback
	ResumeFalseInterrupt    bool   // resume after a false interrupt (false: stay stopped)
}

// loadDialogConfig copies the barge/turn knobs from the process dialogpolicy.
// Both agents call it; per-call PerformanceOverrides merge on top afterwards.
func loadDialogConfig() dialogConfig {
	p := dialogpolicy.Get()
	return dialogConfig{
		Adaptive:                p.AdaptiveBargeIn,
		CandidateChars:          p.BargeCandidateChars,
		ConfirmChars:            p.BargeConfirmChars,
		MaxPauseMs:              p.BargeMaxPauseMs,
		OnsetGraceMs:            p.BargeOnsetGraceMs,
		FullPause:               p.BargeFullPause,
		Duck:                    p.BargeDuck,
		DuckGain:                p.BargeDuckGain,
		OnsetAction:             p.BargeOnsetAction,
		FalseInterruptTimeoutMs: p.BargeFalseInterruptMs,
		ResumeFalseInterrupt:    p.BargeResumeFalseInterrupt,
	}
}

// dialogControl wires the adaptive barge state machine to a call's ASR event
// stream. It is the ONE implementation of the watch→duck→yield→confirm
// escalation (plus, via call.listen, turn detection) that any leg-owning
// agent reuses, so SIP and WebRTC behave identically. cancelReply aborts the
// agent's in-flight reply on a confirmed interruption; stopping playback is
// handled here so callers don't repeat it.
type dialogControl struct {
	h      *Handlers
	callID string
	barge  *adaptiveBarge
}

// newDialogControl builds the barge state machine bound to a call. Returns nil
// when adaptive barge-in is disabled (cfg.Adaptive=false); callers then fall back
// to their legacy hard barge-in.
func (h *Handlers) newDialogControl(callID string, cfg dialogConfig, cancelReply func(), log *slog.Logger) *dialogControl {
	if !cfg.Adaptive {
		return nil
	}
	// The yielded stage pauses losslessly by default (FullPause=true) so a false
	// interrupt can resume mid-sentence. Resume also restores gain to unity in
	// case the pause escalated from a duck. FullPause=false keeps everything on
	// gain ducking for transports/deployments that prefer it.
	pause := func() bool { return h.PausePlay(callID) }
	resume := func() {
		h.ResumePlay(callID)
		h.RestorePlay(callID)
	}
	duck := func() bool { return h.DuckPlay(callID, cfg.DuckGain) }
	restore := func() { h.RestorePlay(callID) }
	switch {
	case !cfg.Duck:
		// Never lower the volume: the candidate stage goes silent instead.
		duck, restore = pause, resume
	case !cfg.FullPause:
		pause, resume = duck, restore
	}
	barge := newAdaptiveBarge(callID, bargeCfg{
		candidateChars: cfg.CandidateChars,
		confirmChars:   cfg.ConfirmChars,
		maxPause:       time.Duration(cfg.MaxPauseMs) * time.Millisecond,
		onsetGrace:     time.Duration(cfg.OnsetGraceMs) * time.Millisecond,
		falseInterrupt: time.Duration(cfg.FalseInterruptTimeoutMs) * time.Millisecond,
		onsetAction:    cfg.OnsetAction,
		resumeOnFalse:  cfg.ResumeFalseInterrupt,
	}, log,
		duck,
		restore,
		pause,
		resume,
		func() {
			cancelReply()
			_ = h.StopPlay(callID)
		},
	)
	return &dialogControl{h: h, callID: callID, barge: barge}
}

// onSpeechStart starts watching the utterance at VAD onset while the bot is
// speaking (inaudible by default; see dialogConfig.OnsetAction).
func (dc *dialogControl) onSpeechStart() {
	if dc.h.IsInterruptible(dc.callID) {
		dc.barge.onSpeechStart()
	}
}

// onPartial feeds one dispatched ASR partial to the classifier while playing.
func (dc *dialogControl) onPartial(text string) {
	if dc.h.IsInterruptible(dc.callID) {
		dc.barge.onPartial(text)
	}
}

// onSpeechEnd resolves the finalized utterance. Returns true if the caller should
// start a turn (real content) or false to keep the current reply (backchannel /
// false trigger — playback was resumed internally).
func (dc *dialogControl) onSpeechEnd(final string) bool {
	return dc.barge.onSpeechEnd(final)
}

// onSpeechDiscard reports that the upstream ASR gate rejected the utterance
// (noise / low energy / echo): a confirmed false interrupt, so any duck/pause
// is unwound immediately instead of waiting for a timeout.
func (dc *dialogControl) onSpeechDiscard() {
	dc.barge.onSpeechDiscard()
}

// onEvent applies the shared dialog policy to events emitted by call.listen.
// Agents with their own turn lifecycle may call the typed methods directly;
// plain listen users can opt in without reimplementing media stop rules.
func (dc *dialogControl) onEvent(method string, params map[string]any) {
	switch method {
	case "call.speech_start":
		dc.onSpeechStart()
	case "call.speech_partial":
		text, _ := params["text"].(string)
		dc.onPartial(text)
	case "call.speech_end":
		text, _ := params["text"].(string)
		dc.onSpeechEnd(text)
	case "call.speech_discard":
		dc.onSpeechDiscard()
	}
}

func (dc *dialogControl) close() {
	if dc != nil {
		dc.barge.close()
	}
}
