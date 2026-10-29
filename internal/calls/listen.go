package calls

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/media/spk"
	"github.com/DeepFlowAI/flowkit/internal/media/turn"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type listenParams struct {
	CallID          string                `json:"call_id"`
	TimeoutMS       int                   `json:"timeout_ms,omitempty"`
	AdaptiveBargeIn bool                  `json:"adaptive_barge_in,omitempty"`
	Performance     *PerformanceOverrides `json:"performance,omitempty"`
}

func (h *Handlers) handleListen(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p listenParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	_, span := tracer.Start(ctx, "call.listen")
	defer span.End()
	span.SetAttributes(attribute.String("call_id", p.CallID))

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		span.SetStatus(codes.Error, "call_not_found")
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.listen"); rpcErr != nil {
		return nil, rpcErr
	}
	cs := h.stateFor(call.ID(), conn)

	h.mu.Lock()
	if !p.Performance.empty() {
		cs.performance = p.Performance
	}
	if cs.listening.Load() {
		provider := cs.asrProvider
		perf := cs.performance
		h.mu.Unlock()
		if ms, rms, ok := endpointSilenceOverride(perf); ok {
			if setter, ok := provider.(interface{ SetEndpointSilence(ms, rms int) }); ok {
				setter.SetEndpointSilence(ms, rms)
			}
		}
		if p.AdaptiveBargeIn {
			h.enableAdaptiveListenBarge(call.ID(), cs)
		}
		return map[string]bool{"ok": true}, nil
	}
	oldSub := cs.asrSub
	oldProvider := cs.asrProvider
	cs.asrSub = nil
	cs.asrProvider = nil
	h.mu.Unlock()
	if oldSub != nil {
		call.AudioBus().Unsubscribe(oldSub)
	}
	if oldProvider != nil {
		_ = oldProvider.Stop()
	}

	provider, err := h.factories.ASR()
	if err != nil {
		cs.listening.Store(false)
		return nil, rpc.NewErrorf(rpc.CodeASRUnavailable, "%v", err)
	}
	// Dial uses ctx: abort the handshake if the call ends mid-connect.
	if err := provider.Start(call.Context()); err != nil {
		cs.listening.Store(false)
		return nil, rpc.NewErrorf(rpc.CodeASRUnavailable, "%v", err)
	}
	if ms, rms, ok := endpointSilenceOverride(cs.performance); ok {
		if setter, ok := provider.(interface{ SetEndpointSilence(ms, rms int) }); ok {
			setter.SetEndpointSilence(ms, rms)
		}
	}

	// The bus refuses new subscribers once the call has ended, which happens if
	// the caller hangs up between answering and this RPC. Every other subscriber
	// checks for it; without the check the pump below dereferences nil and takes
	// the process down with it.
	sub := call.AudioBus().Subscribe()
	if sub == nil {
		_ = provider.Stop()
		cs.listening.Store(false)
		return nil, rpc.ErrCallStateInvalid("call has ended")
	}
	h.mu.Lock()
	cs.asrProvider = provider
	cs.asrSub = sub
	cs.listening.Store(true)
	perf := cs.performance
	startWatch := cs.mediaWatchStarts == 0
	if startWatch {
		cs.mediaWatchStarts++
	}
	h.mu.Unlock()
	if p.AdaptiveBargeIn {
		h.enableAdaptiveListenBarge(call.ID(), cs)
	}

	// Spawn the two pumps: bus → asr.SendPCM, and asr.Events → notify.
	// A shared voice meter lets pumpASREvents reject silence-hallucinated finals
	// without dropping frames from the stream (which breaks FunASR segmentation).
	meter := asrgate.New()
	// Optional Smart Turn v3 endpointer: model-driven early release on a real
	// end-of-turn, with FunASR's long silence timer as the fallback. Only when
	// enabled and the provider supports mid-stream Flush (FunASR does).
	// Per-call VoiceSpeed override can force on/off when the model is loaded.
	wantTurn := turn.Enabled()
	if perf != nil && perf.TurnDetectionEnabled != nil {
		wantTurn = turn.Enabled() && *perf.TurnDetectionEnabled
	}
	var ep *turnEndpointer
	if wantTurn {
		if fl, ok := provider.(asr.Flusher); ok {
			ep = newTurnEndpointer(call.ID(), fl.Flush, h.log)
			applyTurnOverrides(ep, perf)
			h.log.Info("turn endpointer attached", "call_id", call.ID())
		}
	}
	go h.pumpBusToASR(call, provider, sub, meter, ep)
	// Nil unless the speaker gate is enabled, in which case it learns the
	// caller's voice and drops utterances belonging to people around them.
	go h.pumpASREvents(call, provider, meter, ep, spk.NewGate())
	if startWatch {
		go h.watchCallMediaEnd(p.CallID, call)
	}

	observability.CallTraceCall(ctx, h.log, "rpc.listen", call)
	return map[string]bool{"ok": true}, nil
}

// enableAdaptiveListenBarge binds the same dialog controller used by the
// built-in and external agents to a plain call.listen session. Opt-in forces
// the adaptive path on; all thresholds and false-trigger handling still come
// from the shared dialog policy and per-call performance overrides.
func (h *Handlers) enableAdaptiveListenBarge(callID string, cs *callState) {
	h.mu.Lock()
	if cs.listenDialog != nil {
		h.mu.Unlock()
		return
	}
	perf := cs.performance
	h.mu.Unlock()

	cfg := mergeDialogConfig(loadDialogConfig(), perf)
	cfg.Adaptive = true
	dc := h.newDialogControl(callID, cfg, func() {}, h.log)
	if dc == nil {
		return
	}

	h.mu.Lock()
	current := h.callState[callID]
	if current == cs && cs.listening.Load() && cs.listenDialog == nil {
		cs.listenDialog = dc
		cs.adaptiveListen.Store(true)
		dc = nil
	}
	h.mu.Unlock()
	if dc != nil {
		dc.close()
	}
}

func (h *Handlers) pumpBusToASR(call *session.Call, p asr.Provider, sub *bus.Subscriber, meter *asrgate.Meter, ep *turnEndpointer) {
	if ep != nil {
		defer ep.Close()
	}
	// FunASR emits no VAD-start event, so the only early voice-onset signal is
	// the neural VAD's rising edge. Surface it as call.speech_start so adaptive
	// barge-in can pause playback the instant the user speaks (minimising the
	// double-talk window that corrupts ASR), before any transcript exists.
	watchOnset := meter.HasVAD()
	prevSpeaking := false
	for frame := range sub.Channel() {
		meter.Observe(frame.Samples)
		if watchOnset {
			if sp := meter.Speaking(); sp != prevSpeaking {
				prevSpeaking = sp
				if sp {
					h.dispatch(call.ID(), "call.speech_start", nil)
				}
			}
		}
		if ep != nil {
			ep.Feed(frame.Samples)
		}
		if err := p.SendPCM(frame.Samples); err != nil {
			h.log.Debug("asr SendPCM failed", "call_id", call.ID(), "err", err)
			return
		}
	}
}

func (h *Handlers) pumpASREvents(call *session.Call, p asr.Provider, meter *asrgate.Meter, ep *turnEndpointer, gate *spk.Gate) {
	// Partials of the open utterance, tagged by whether caller speech backed
	// them, so background voice can be cut off the final.
	var parts []partialSpan
	prevVoiced := 0
	// Partial text of the open utterance, and whether some of it was held back
	// by the AEC warmup, so a stop word split across partials still reaches
	// the barge classifier whole.
	uttText, warmHeld := "", false
	for ev := range p.Events() {
		switch ev.Type {
		case asr.EventVADStart:
			observability.ASRStreamTrace(call.Context(), h.log, "asr.speech_start", call)
			h.dispatch(call.ID(), "call.speech_start", nil)
		case asr.EventPartial:
			accepted := asrgate.PlausibleUserSpeech(ev.Text)
			speaking := meter.Speaking()
			// Instantaneous energy is not enough to earn an interruption: the
			// release hold on the caller's own last words keeps reading as
			// "speaking" for about a second afterwards, so a far-field voice
			// picked up in that window used to duck the playback and only get
			// thrown out at the final — the bot had already stuttered.
			//
			// Ducking costs exactly what being understood costs. A cheaper bar
			// for interrupting means a fragment ducks the playback and is then
			// thrown out by the final's bar, and the caller hears the bot dip
			// and recover for no reason. The same context applies to both, so a
			// one-word answer to a question still interrupts immediately.
			h.mu.Lock()
			pcs := h.callState[call.ID()]
			h.mu.Unlock()
			voicedMs := meter.VoicedMs()
			if meter.HasVAD() {
				parts = append(parts, partialSpan{text: ev.Text, speech: voicedMs-prevVoiced >= partialSpeechMinGrowthMs})
				prevVoiced = voicedMs
			}
			if speaking && meter.HasVAD() && voicedMs < meter.MinVoicedMs(listenMinVoiced(pcs)) {
				speaking = false
			}
			// Neural-VAD gate: only let a partial drive barge-in when a human is
			// actually speaking. Noise that FunASR hallucinates into text arrives
			// with no VAD speech behind it → suppressed, so it never interrupts
			// the bot. (No-op when VAD is disabled; speaking() returns true.)
			if accepted && !speaking {
				accepted = false
			} else if !accepted && speaking && strings.TrimSpace(ev.Text) != "" {
				// Real speech energy present but the fragment is too short for
				// PlausibleUserSpeech (single-char increments like "等"/"一"/"下").
				// Let it through so the barge classifier can assemble multi-char
				// stop-words; its own stop-word/length thresholds still gate the
				// actual interruption. This is the double-talk "秒停" fast path.
				accepted = true
			}
			uttText += ev.Text
			out := ev.Text
			warm := false
			if accepted && pcs != nil && pcs.playing.Load() && inAECWarmup(call, pcs, meter.UtteranceOnset()) {
				if !containsStopWord(uttText) {
					accepted, warmHeld, warm = false, true, true
				} else if warmHeld {
					out, warmHeld = uttText, false
				}
			}
			observability.ASRStreamTrace(call.Context(), h.log, "asr.speech_partial", call,
				"text", ev.Text, "seq", ev.Sequence, "accepted", accepted, "voiced_ms", voicedMs,
				"aec_warmup", warm)
			if !accepted {
				continue
			}
			if ep != nil {
				// What the caller has said so far decides how long the next pause
				// has to run before the turn is released.
				ep.ObservePartial(out)
			}
			h.dispatch(call.ID(), "call.speech_partial", map[string]any{
				"text": out,
				"seq":  ev.Sequence,
			})
		case asr.EventFinal:
			text := strings.TrimSpace(ev.Text)
			if trimmed := trimBackgroundSpeech(text, parts); trimmed != text {
				h.log.Info("asr final trimmed (background speech)", "call_id", call.ID(),
					"text", text, "kept", trimmed)
				text = trimmed
			}
			parts, prevVoiced = nil, 0
			uttText, warmHeld = "", false
			// Every provider final closes one acoustic segment, even when its
			// transcript is later rejected. Reset both acoustic accumulators
			// before filtering so fillers/noise cannot pollute a later utterance.
			if ep != nil {
				ep.Reset()
			}
			h.mu.Lock()
			cs := h.callState[call.ID()]
			h.mu.Unlock()
			overPlayback := cs != nil && cs.playing.Load()
			// Judged before the meter is touched: an echo of the opening words
			// must neither interrupt nor become the caller's level reference.
			if overPlayback && inAECWarmup(call, cs, meter.UtteranceOnset()) && !containsStopWord(text) {
				meter.Reset()
				h.log.Info("asr final ignored (aec warmup)", "call_id", call.ID(), "text", text)
				h.dispatch(call.ID(), "call.speech_discard", nil)
				continue
			}
			verdict := meter.AcceptUtteranceOver(finalMinVoiced(listenMinVoiced(cs), text), overPlayback)
			// Rejected finals still tell the barge state machine the utterance
			// was a false trigger (call.speech_discard), so a duck/pause is
			// unwound immediately instead of waiting for a safety timeout.
			if !asrgate.PlausibleUserSpeech(text) {
				// Record the acoustic evidence even though the text alone decided
				// this. A one-word transcript backed by a few tens of ms is a
				// hallucination, but the same transcript backed by a full second
				// means real speech reached the gate and ASR only transcribed a
				// fragment of it — opposite problems, indistinguishable without
				// this field.
				h.log.Info("asr final ignored (noise)", "call_id", call.ID(), "text", text,
					"voiced_ms", verdict.VoicedMs)
				h.dispatch(call.ID(), "call.speech_discard", nil)
				continue
			}
			if verdict.TooQuietToBarge {
				h.log.Info("asr final ignored (too quiet to interrupt)", "call_id", call.ID(), "text", text,
					"voiced_ms", verdict.VoicedMs, "level_rms", verdict.LevelRMS,
					"caller_rms", verdict.CallerRMS)
				h.dispatch(call.ID(), "call.speech_discard", nil)
				continue
			}
			if verdict.TooQuiet {
				h.log.Info("asr final ignored (far below caller level)", "call_id", call.ID(), "text", text,
					"voiced_ms", verdict.VoicedMs, "level_rms", verdict.LevelRMS,
					"caller_rms", verdict.CallerRMS)
				h.dispatch(call.ID(), "call.speech_discard", nil)
				continue
			}
			if !verdict.OK {
				h.log.Info("asr final ignored (low energy)", "call_id", call.ID(), "text", text,
					"voiced_ms", verdict.VoicedMs, "min_voiced_ms", verdict.MinMs,
					"noise_rms", verdict.FloorRMS)
				h.dispatch(call.ID(), "call.speech_discard", nil)
				continue
			}
			if cs != nil {
				cs.sayMu.Lock()
				prevSay := cs.prevSayText
				lastSay := cs.lastSayText
				lastAt := cs.lastSayAt
				cs.sayMu.Unlock()
				if time.Since(lastAt) < 8*time.Second && (likelyEchoASR(text, lastSay) ||
					overPlayback && likelyEchoOverPlayback(text, prevSay, lastSay)) {
					h.log.Info("asr final ignored (echo)", "call_id", call.ID(), "text", text)
					h.dispatch(call.ID(), "call.speech_discard", nil)
					continue
				}
			}
			// Whose voice was it? Last of the gates, so the embedding is only
			// computed for utterances that are otherwise ready to become a turn.
			//
			// A mismatch annotates the turn, it does not swallow it. The caller's
			// own score moves with how much of their speech survived the uplink,
			// far enough to overlap what a bystander scores, so a threshold that
			// keeps every stranger out also locks the caller out of their own
			// call — and a caller repeating themselves is a worse failure than
			// answering a passer-by. What the score still guards is the
			// irreversible commands, which is the agent's call to make.
			res := gate.Check(verdict.Audio)
			if res.Switched {
				// Who the caller is has changed. "handover" means somebody else
				// now carries the conversation at handset level; "recovery" means
				// the first voice enrolled was the wrong one and the call is being
				// handed to whoever is actually speaking.
				h.log.Info("caller voice changed", "call_id", call.ID(),
					"reason", res.SwitchReason, "speaker_id", res.SpeakerID,
					"speakers", res.Speakers, "text", text)
			}
			switch {
			case !res.OK:
				h.log.Info("asr final from an unrecognised voice", "call_id", call.ID(), "text", text,
					"similarity", roundedSim(res.Similarity), "threshold", roundedSim(res.Threshold),
					"speaker_id", res.SpeakerID, "speakers", res.Speakers)
			case res.Enrolled:
				h.log.Info("caller voice enrolled", "call_id", call.ID(), "voiced_ms", verdict.VoicedMs,
					"speaker_id", res.SpeakerID)
			case res.Compared:
				// Log the passing scores too. Only the rejections used to be
				// recorded, which shows how close a bystander came to the
				// threshold but not how much headroom the caller has — the two
				// distributions together are what says where to put it.
				h.log.Info("caller voice matched", "call_id", call.ID(),
					"similarity", roundedSim(res.Similarity), "threshold", roundedSim(res.Threshold),
					"speaker_id", res.SpeakerID, "speakers", res.Speakers)
			}
			observability.CallTraceCall(call.Context(), h.log, "asr.speech_end", call, "text", text,
				"voiced_ms", verdict.VoicedMs, "min_voiced_ms", verdict.MinMs, "level_rms", verdict.LevelRMS)
			observability.DialogTurn(call.Context(), h.log, call.ConversationID(), call.ID(), "user", text, call.DialogAttrs()...)
			h.dispatch(call.ID(), "call.speech_end", map[string]any{
				"text": text,
				// How much real speech backed this utterance. Lets an agent hold
				// back irreversible actions on a barely-voiced turn.
				"voiced_ms": verdict.VoicedMs,
				// Whether the voice matched the enrolled caller. False means the
				// utterance is answered normally but must not trigger anything
				// that cannot be undone.
				"voice_match": res.OK,
				// Which voice on the call said it, counting from 1 in the order
				// the voices were first heard. Lets an agent notice the handset
				// changed hands; 0 when speaker verification is off.
				"speaker_id": res.SpeakerID,
			})
			h.broadcast("call.user_text", call.ID(), map[string]any{
				"text": text,
			})
		case asr.EventVADEnd:
			// silent — speech_end already conveys the same boundary
		case asr.EventError:
			h.dispatch(call.ID(), "call.error", map[string]any{
				"code":    rpc.CodeASRUnavailable,
				"message": ev.Err.Error(),
			})
		}
	}
}

// inAECWarmup reports whether an utterance whose voice began at onset falls in
// the opening window of a SIP call, right after the bot's first audio. The
// caller's handset echo canceller is still adapting then and can send the
// bot's own opening words back up the line, loud and garbled enough to pass
// every acoustic gate. Browser transports cancel echo locally with the exact
// playback signal and are left alone.
func inAECWarmup(call *session.Call, cs *callState, onset time.Time) bool {
	if cs == nil || onset.IsZero() {
		return false
	}
	warmup := time.Duration(dialogpolicy.Get().AECWarmupMs) * time.Millisecond
	first := cs.firstAudioNano.Load()
	if warmup <= 0 || first == 0 {
		return false
	}
	if mio := call.MediaIO(); mio == nil || mio.Kind() != transport.KindSIP {
		return false
	}
	start := time.Unix(0, first)
	return !onset.Before(start) && onset.Sub(start) < warmup
}
