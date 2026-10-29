package calls

// Dual-leg bridge transcription.
//
// When BRIDGE_ASR_ENABLED is on, a bridged human↔human call gets an
// independent ASR stream on each leg's AudioBus. Recognized text is pushed to
// the controller as `call.bridge_transcript` notifications, every line tagged
// with the speaker ("user" / "agent") and reported under a single anchor
// call_id (the inbound/user leg) so the whole conversation shares one id
// regardless of who is talking. Nothing here writes to disk — it is a live
// passthrough for a real-time transcript UI.

import (
	"strings"
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// bridgeASRSession is one leg's ASR stream. speaker and anchorID travel with
// it so the same leg can keep its identity in the transcript when it moves
// from a two-leg bridge into a mixing room.
type bridgeASRSession struct {
	provider asr.Provider
	sub      *bus.Subscriber
	leg      *session.Call
	speaker  string
	anchorID string
}

// SetBridgeASR enables continuous transcription on both legs of a bridged call.
// factory (optional) lets the caller supply a transcription-tuned ASR provider
// (longer endpoint silence + native chunk size) instead of the IVR call-path
// provider, which is tuned for fast barge-in and fragments human speech. nil
// falls back to the default call-path factory.
func (h *Handlers) SetBridgeASR(enabled bool, factory func() (asr.Provider, error)) {
	h.bridgeASREnabled = enabled
	h.bridgeASRFactory = factory
}

// bridgeASRProvider builds one ASR stream, preferring the transcription-tuned
// factory when configured.
func (h *Handlers) bridgeASRProvider() (asr.Provider, error) {
	if h.bridgeASRFactory != nil {
		return h.bridgeASRFactory()
	}
	return h.factories.ASR()
}

// startBridgeASR spins up one ASR stream per leg. Best-effort: a leg whose
// provider fails to start is logged and skipped without aborting the bridge.
func (h *Handlers) startBridgeASR(pair *bridgePair, user, agent *session.Call) {
	anchorID := user.ID()
	legs := []struct {
		call    *session.Call
		speaker string
	}{
		{user, "user"},
		{agent, "agent"},
	}
	for _, l := range legs {
		s := h.openLegTranscript(l.call, l.speaker, anchorID, &pair.wg)
		if s == nil {
			continue
		}
		pair.asrMu.Lock()
		pair.asr = append(pair.asr, s)
		pair.asrMu.Unlock()
	}
}

// openLegTranscript starts one leg's ASR stream plus the two pump goroutines
// that drive it, counting them on wg. Recognized text is emitted as
// call.bridge_transcript tagged with speaker and reported under anchorID.
// Best-effort: returns nil (already cleaned up) if the stream cannot start.
func (h *Handlers) openLegTranscript(call *session.Call, speaker, anchorID string, wg *sync.WaitGroup) *bridgeASRSession {
	provider, err := h.bridgeASRProvider()
	if err != nil {
		h.log.Warn("transcript: provider build failed", "leg", call.ID(), "speaker", speaker, "err", err)
		return nil
	}
	if err := provider.Start(call.Context()); err != nil {
		h.log.Warn("transcript: provider start failed", "leg", call.ID(), "speaker", speaker, "err", err)
		return nil
	}
	sub := call.AudioBus().Subscribe()
	if sub == nil {
		_ = provider.Stop()
		return nil
	}

	// A shared meter lets the events pump gate noise/hallucinated finals
	// without dropping frames from the audio pump (which breaks FunASR
	// segmentation) — same arrangement as call.listen.
	meter := asrgate.New()
	wg.Add(2)
	go func() {
		defer wg.Done()
		h.pumpBusToASR(call, provider, sub, meter, nil)
	}()
	go func() {
		defer wg.Done()
		h.pumpBridgeASREvents(anchorID, speaker, call, provider, meter)
	}()
	return &bridgeASRSession{
		provider: provider,
		sub:      sub,
		leg:      call,
		speaker:  speaker,
		anchorID: anchorID,
	}
}

// closeLegTranscript ends one leg's stream. Unsubscribing closes the bus
// channel (ending pumpBusToASR); Stop() closes the provider Events() channel
// (ending pumpBridgeASREvents). Nil-safe.
func closeLegTranscript(s *bridgeASRSession) {
	if s == nil {
		return
	}
	if s.sub != nil {
		s.leg.AudioBus().Unsubscribe(s.sub)
	}
	if s.provider != nil {
		_ = s.provider.Stop()
	}
}

// stopBridgeASR tears down both legs' streams. Idempotent — the session list
// is cleared.
func (h *Handlers) stopBridgeASR(pair *bridgePair) {
	pair.asrMu.Lock()
	sessions := pair.asr
	pair.asr = nil
	pair.asrMu.Unlock()
	for _, s := range sessions {
		closeLegTranscript(s)
	}
}

// transcriptRole is how a leg is labelled in the live transcript: which side
// of the conversation it is, and the call id every line is reported under. The
// zero value means the leg is not being transcribed.
type transcriptRole struct {
	speaker  string
	anchorID string
}

func (r transcriptRole) active() bool { return r.speaker != "" }

// transcriptRoleOf reports how a leg is currently being transcribed in its
// bridge. Joining a room tears that bridge down, so the room asks first and
// carries the answer forward — a leg nobody was transcribing stays untranscribed.
func (h *Handlers) transcriptRoleOf(callID string) transcriptRole {
	h.bridgeMu.Lock()
	peer, hasPeer := h.bridgePeers[callID]
	var pair *bridgePair
	if hasPeer {
		pair = h.bridgePairs[bridgeKey(callID, peer)]
	}
	h.bridgeMu.Unlock()
	if pair == nil {
		return transcriptRole{}
	}
	pair.asrMu.Lock()
	defer pair.asrMu.Unlock()
	for _, s := range pair.asr {
		if s.leg.ID() == callID {
			return transcriptRole{speaker: s.speaker, anchorID: s.anchorID}
		}
	}
	return transcriptRole{}
}

// pumpBridgeASREvents forwards one leg's ASR output as `call.bridge_transcript`
// notifications tagged with the speaker. Mirrors the gating in pumpASREvents
// (neural-VAD on partials, plausibility + energy on finals) but omits the
// TTS-echo check — no bot is speaking during a human↔human bridge.
func (h *Handlers) pumpBridgeASREvents(anchorID, speaker string, call *session.Call, p asr.Provider, meter *asrgate.Meter) {
	// Online partials are either single-char increments (FunASR 2pass-online) or
	// cumulative hypotheses (Aliyun result_changed). Keep a per-segment snapshot
	// so every dispatched partial is the text so far, not a raw fragment; a
	// final clears it so the next segment starts clean.
	var acc string
	for ev := range p.Events() {
		switch ev.Type {
		case asr.EventPartial:
			incoming := strings.TrimSpace(ev.Text)
			if strings.HasPrefix(incoming, acc) {
				acc = incoming
			} else {
				acc += incoming
			}
			text := asrgate.TrimLeadingPunct(acc)
			if !asrgate.PlausibleUserSpeech(text) || !meter.Speaking() {
				continue
			}
			h.dispatch(anchorID, "call.bridge_transcript", map[string]any{
				"speaker": speaker,
				"text":    text,
				"seq":     ev.Sequence,
				"final":   false,
			})
		case asr.EventFinal:
			acc = ""
			text := asrgate.TrimLeadingPunct(strings.TrimSpace(ev.Text))
			if !asrgate.PlausibleUserSpeech(text) || !meter.Accept() {
				continue
			}
			h.dispatch(anchorID, "call.bridge_transcript", map[string]any{
				"speaker": speaker,
				"text":    text,
				"final":   true,
			})
		case asr.EventError:
			h.log.Debug("bridge asr error", "leg", call.ID(), "speaker", speaker, "err", ev.Err)
		}
	}
}
