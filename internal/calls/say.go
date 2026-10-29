package calls

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type sayParams struct {
	CallID  string `json:"call_id"`
	Text    string `json:"text"`
	Voice   string `json:"voice,omitempty"`
	BargeIn bool   `json:"barge_in,omitempty"`
	// LeadPadMs, when > 0, prepends this much leading silence to this utterance
	// (see tts.Options.LeadPadMs). Used for the initial greeting so its opening
	// words are not clipped by the SIP answer→media cut-through gap.
	LeadPadMs int `json:"lead_pad_ms,omitempty"`
}

type sayResult struct {
	PlayID string `json:"play_id"`
}

// _firstSayLeadPadMs is the leading silence prepended to the first prompt on a
// call, to cover the SIP answer→media cut-through gap where the far end drops
// the opening RTP (confirmed to save the greeting's opening word).
const _firstSayLeadPadMs = 900

// _sayTailPadMs is trailing silence appended to every spoken prompt so the far
// end's jitter buffer plays out the final syllable before the RTP stream goes
// quiet, instead of clipping it ("half sound" on the last character).
const _sayTailPadMs = 300

func (h *Handlers) handleSay(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p sayParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Text == "" {
		return nil, rpc.ErrInvalidParams("missing text")
	}
	ctx, span := tracer.Start(ctx, "call.say")
	defer span.End()
	span.SetAttributes(
		attribute.String("call_id", p.CallID),
		attribute.Int("text_len", len([]rune(p.Text))),
		attribute.String("voice", p.Voice),
	)

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		span.SetStatus(codes.Error, "call_not_found")
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.say"); rpcErr != nil {
		return nil, rpcErr
	}
	h.stateFor(call.ID(), conn)

	tp, err := h.factories.TTS()
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "%v", err)
	}

	playID := newShortID()
	cs := h.stateFor(call.ID(), conn)

	// Register play cancel before Synthesize so barge-in can abort in-flight TTS
	// and IsInterruptible is true during synthesis (not only during RTP playback).
	// Child of call.Context: hangup cancels synthesis/playback with the call tree.
	playCtx, playCancel := context.WithCancel(call.Context())
	cs.playMu.Lock()
	if cs.playCancel != nil {
		cs.playCancel()
	}
	cs.playSeq++
	myPlaySeq := cs.playSeq
	cs.playCancel = playCancel
	cs.playMu.Unlock()

	// The first prompt on a call plays into the SIP answer→media cut-through
	// gap, where the far end commonly drops the opening few hundred ms of RTP
	// (the caller hears the greeting missing its first word/character). Pad the
	// first say with extra leading silence so the opening survives; later
	// prompts (media already flowing) keep the small default. A preceding
	// call.play also primes the media path, even though no TTS has run yet. A
	// caller-specified lead pad always wins.
	leadPad := p.LeadPadMs
	if leadPad == 0 {
		cs.sayMu.Lock()
		firstSay := cs.lastSayAt.IsZero()
		cs.sayMu.Unlock()
		if firstSay && !cs.mediaPrimed.Load() {
			leadPad = _firstSayLeadPadMs
		}
	}
	prebufferMs := 0
	if cs.performance != nil && cs.performance.TTSPrebufferMs != nil {
		prebufferMs = *cs.performance.TTSPrebufferMs
	}
	synthSubmit := time.Now()
	chunks, err := tp.Synthesize(playCtx, p.Text, tts.Options{
		Voice: p.Voice, LeadPadMs: leadPad, TailPadMs: _sayTailPadMs, PrebufferMs: prebufferMs,
	})
	if err != nil {
		cs.playMu.Lock()
		if cs.playSeq == myPlaySeq {
			cs.playCancel = nil
		}
		cs.playMu.Unlock()
		playCancel()
		_ = tp.Close()
		if playCtx.Err() != nil {
			h.signalPlayWait(call.ID(), "interrupted")
			return sayResult{PlayID: playID}, nil
		}
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "%v", err)
	}

	// Mirror the assistant text out to every connected client so a chat-UI
	// can render it without intercepting the agent's WS.
	h.broadcast("call.assistant_text", call.ID(), map[string]any{
		"play_id": playID,
		"text":    p.Text,
		"voice":   p.Voice,
	})
	cs.sayMu.Lock()
	cs.prevSayText = cs.lastSayText
	cs.lastSayText = p.Text
	cs.lastSayAt = time.Now()
	cs.sayMu.Unlock()

	go h.playChunks(ctx, call, cs, playCtx, playCancel, myPlaySeq, playID, synthSubmit, chunks, tp)

	observability.CallTraceCall(ctx, h.log, "rpc.say", call, "play_id", playID, "text_len", len([]rune(p.Text)))
	observability.DialogTurn(ctx, h.log, call.ConversationID(), call.ID(), "assistant", p.Text, call.DialogAttrs()...)
	return sayResult{PlayID: playID}, nil
}

// playChunks renders synthesized PCM to the call's outbound media, emitting
// tts.first_audio on the first frame and call.play_end on completion. It owns
// the play slot teardown (clears cs.playCancel for myPlaySeq, closes tp) and
// signals playWait with the terminal reason. Blocks until done/interrupted, so
// callers may run it synchronously (PlayPrepared) or in a goroutine (handleSay).
func (h *Handlers) playChunks(ctx context.Context, call *session.Call, cs *callState,
	playCtx context.Context, playCancel context.CancelFunc, myPlaySeq uint64,
	playID string, synthSubmit time.Time, chunks <-chan tts.PCMChunk, tp tts.Provider) (heardMS int64) {
	cs.playing.Store(true)
	cs.playStartedNano.Store(time.Now().UnixNano())
	start := time.Now()
	// Tell the transport we're actively feeding frames, so a bursty/slow TTS
	// backend that momentarily falls behind real-time is ridden out with paced
	// silence instead of an audible gap. Cleared once the chunk stream ends (and
	// via defer on any early/interrupted return).
	setFeeding := func(v bool) {
		if mio := call.MediaIO(); mio != nil {
			mio.SetPlaybackFeeding(v)
		}
	}
	setFeeding(true)
	defer setFeeding(false)
	defer cs.playing.Store(false)
	defer cs.playStartedNano.Store(0)
	defer func() { _ = tp.Close() }()
	defer func() {
		playCancel()
		cs.playMu.Lock()
		if cs.playSeq == myPlaySeq {
			cs.playCancel = nil
		}
		cs.playMu.Unlock()
	}()
	var totalSamples int64
	const sampleRate = bus.SampleRate
	var firstAudioAt time.Time
	startStats, statsOK := playbackStatsSnapshot(call)
	// ttsWait*: time spent ready for the next chunk while the TTS backend had
	// none, after the first audio. A slow or bursty backend shows up here.
	var ttsWaitTotal, ttsWaitMax time.Duration
	// played_ms is duration of PCM submitted to the outgoing queue (via
	// WriteOutgoingCtx), not bytes confirmed on the wire. On interrupt,
	// StopOutgoing drops queued-but-unsent audio, so played_ms can overestimate
	// what the far end heard; wall_ms is elapsed wall time including TTS wait /
	// prebuffer. heard_ms caps played_ms at the real time since the first frame
	// on interrupt: the pacer sends at real-time rate, so nothing queued beyond
	// that point reached the caller.
	finish := func(reason string) {
		playedMS := totalSamples * 1000 / int64(sampleRate)
		wallMS := time.Since(start).Milliseconds()
		heardMS = heardPlaybackMs(playedMS, reason == "completed", firstAudioAt, time.Now())
		fields := []any{
			"play_id", playID, "reason", reason,
			"played_ms", playedMS, "heard_ms", heardMS, "wall_ms", wallMS,
			"tts_wait_ms", ttsWaitTotal.Milliseconds(),
			"tts_max_wait_ms", ttsWaitMax.Milliseconds(),
		}
		if reason == "completed" && !firstAudioAt.IsZero() {
			// Playback time beyond the audio itself: underrun silence plus
			// send jitter. Near zero on a smooth play.
			fields = append(fields, "stall_ms", wallMS-firstAudioAt.Sub(start).Milliseconds()-playedMS)
		}
		if statsOK {
			if endStats, ok := playbackStatsSnapshot(call); ok {
				fields = append(fields,
					"underrun_events", endStats.UnderrunEvents-startStats.UnderrunEvents,
					"underrun_ms", (endStats.SilenceFrames-startStats.SilenceFrames)*uint64(bus.FrameDuration/time.Millisecond),
					"underrun_limit_hits", endStats.UnderrunLimitHits-startStats.UnderrunLimitHits)
			}
		}
		observability.CallTraceCall(ctx, h.log, "play.end", call, fields...)
		h.dispatch(call.ID(), "call.play_end", map[string]any{
			"play_id":   playID,
			"reason":    reason,
			"played_ms": playedMS,
			"heard_ms":  heardMS,
			"wall_ms":   wallMS,
		})
		h.signalPlayWait(call.ID(), reason)
	}
	firstAudio := true
	var readyAt time.Time
	for chunk := range chunks {
		if !readyAt.IsZero() {
			w := time.Since(readyAt)
			ttsWaitTotal += w
			ttsWaitMax = max(ttsWaitMax, w)
		}
		select {
		case <-playCtx.Done():
			finish("interrupted")
			return
		default:
		}
		if call.IsEnded() {
			finish("ended")
			return
		}
		if firstAudio {
			firstAudio = false
			// lead_ms: how long the next sentence was synthesized ahead of
			// playback (prefetch lead). gap_ms: real inter-sentence silence
			// the caller hears (≈0 when prefetched, = TTS TTFB for a turn's
			// first sentence). first_audio_ms = lead_ms + gap_ms.
			now := time.Now()
			firstAudioAt = now
			cs.firstAudioNano.CompareAndSwap(0, now.UnixNano())
			observability.CallTraceCall(ctx, h.log, "tts.first_audio", call,
				"play_id", playID,
				"first_audio_ms", now.Sub(synthSubmit).Milliseconds(),
				"lead_ms", start.Sub(synthSubmit).Milliseconds(),
				"gap_ms", now.Sub(start).Milliseconds())
		}
		n, err := h.writeOutgoingFrames(playCtx, call, chunk.Samples)
		totalSamples += int64(n)
		if err != nil {
			if playCtx.Err() != nil {
				h.stopOutgoingPlayback(call.ID())
			} else {
				h.log.Debug("write outgoing failed during say", "err", err)
			}
			finish("interrupted")
			return
		}
		select {
		case <-playCtx.Done():
			h.stopOutgoingPlayback(call.ID())
			finish("interrupted")
			return
		default:
		}
		readyAt = time.Now()
	}
	// Source exhausted — stop holding the stream so the paced sender drains the
	// tail and goes idle (otherwise WaitPlaybackIdle would block on silence).
	setFeeding(false)
	if err := h.waitOutgoingPlayback(call, playCtx); err != nil {
		finish("interrupted")
		return
	}
	finish("completed")
	return
}

func heardPlaybackMs(playedMS int64, completed bool, firstAudioAt, now time.Time) int64 {
	if completed {
		return playedMS
	}
	if firstAudioAt.IsZero() {
		return 0
	}
	return min(playedMS, now.Sub(firstAudioAt).Milliseconds())
}

// padPlaybackTail keeps the outgoing stream running for _sayTailPadMs after the
// audio that just played. The far end's jitter buffer holds a chunk of audio it
// has not rendered yet; when the stream stops dead, that remainder is commonly
// dropped and the caller hears the closing character cut in half. Trailing
// silence gives the buffer time to play out.
//
// Not waited on: the frames are queued and the caller moves on. A barge-in
// clears them along with everything else, which is the desired behaviour.
func (h *Handlers) padPlaybackTail(ctx context.Context, callID string) {
	call, err := h.mgr.Get(callID)
	if err != nil || call.IsEnded() {
		return
	}
	if err := h.writePlaybackTail(ctx, call); err != nil {
		h.log.Debug("playback tail pad failed", "call_id", callID, "err", err)
	}
}

// _hangupTailPadMs is silence sent and played out before the agent hangs up.
// The turn tail (_sayTailPadMs) is only queued, and hangup drops queued audio;
// the far end also needs time to render its jitter buffer before the BYE, or
// the closing words of a goodbye are cut off.
const _hangupTailPadMs = 800

// _hangupDrainTimeout bounds the wait so a stuck transport cannot keep the
// call open after the agent asked to end it.
const _hangupDrainTimeout = 3 * time.Second

// drainBeforeHangup sends trailing silence and waits until it has left the
// pacer, so everything spoken before it has reached the caller.
func (h *Handlers) drainBeforeHangup(callID string) {
	call, err := h.mgr.Get(callID)
	if err != nil || call.IsEnded() {
		return
	}
	ctx, cancel := context.WithTimeout(call.Context(), _hangupDrainTimeout)
	defer cancel()
	silence := make([]int16, _hangupTailPadMs*bus.SampleRate/1000)
	if _, err := h.writeOutgoingFrames(ctx, call, silence); err != nil {
		return
	}
	_ = h.waitOutgoingPlayback(call, ctx)
}

// PreparedSay is an in-flight TTS synthesis whose audio streams in the
// background, ready to be rendered by PlayPrepared. It lets the kernel LLM
// turn synthesize the next sentence while the current one is still playing
// (prefetch), removing the inter-sentence synthesis gap.
type PreparedSay struct {
	text        string
	voice       string
	playID      string
	chunks      <-chan tts.PCMChunk
	tp          tts.Provider
	cancel      context.CancelFunc
	synthSubmit time.Time
	audible     chan struct{}
	synthesized chan struct{} // closed once the backend stream has been fully relayed
	playedMs    int64         // set by PlayPrepared: audio the caller heard before it ended
}

// PlayedMs is how much of the utterance the caller heard (same measure as
// call.play_end heard_ms). Zero before it is played.
func (p *PreparedSay) PlayedMs() int64 {
	if p == nil {
		return 0
	}
	return p.playedMs
}

// Close releases a prepared utterance that will not be played: it cancels the
// background synthesis and closes the provider. Safe on nil.
func (p *PreparedSay) Close() {
	if p == nil {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.tp != nil {
		_ = p.tp.Close()
	}
}

// PrepareSay starts TTS synthesis for text without claiming the play slot, so
// it can run while a previous utterance is still playing (prefetch). The
// returned PreparedSay must be either played via PlayPrepared or released via
// Close.
func (h *Handlers) PrepareSay(callID, text, voice string) (*PreparedSay, error) {
	return h.prepareSay(callID, text, voice, 0)
}

// prepareSay is PrepareSay with leadPadMs of leading silence (0 = none).
func (h *Handlers) prepareSay(callID, text, voice string, leadPadMs int) (*PreparedSay, error) {
	if text == "" {
		return nil, tts.ErrEmptyText
	}
	call, err := h.mgr.Get(callID)
	if err != nil {
		return nil, err
	}
	tp, err := h.factories.TTS()
	if err != nil {
		return nil, err
	}
	prebufferMs := 0
	if cs := h.stateFor(callID, nil); cs.performance != nil && cs.performance.TTSPrebufferMs != nil {
		prebufferMs = *cs.performance.TTSPrebufferMs
	}
	// Prefetch still dies with the call: hangup must not leave a synth/relay
	// goroutine running after the play slot is gone.
	synthCtx, cancel := context.WithCancel(call.Context())
	synthSubmit := time.Now()
	chunks, err := tp.Synthesize(synthCtx, text, tts.Options{Voice: voice, LeadPadMs: leadPadMs, PrebufferMs: prebufferMs})
	if err != nil {
		cancel()
		_ = tp.Close()
		return nil, err
	}
	audible := make(chan struct{})
	synthesized := make(chan struct{})
	relayed := make(chan tts.PCMChunk, 1)
	go func() {
		defer close(synthesized)
		relayAudible(synthCtx, chunks, relayed, audible)
	}()
	return &PreparedSay{
		text:        text,
		voice:       voice,
		playID:      newShortID(),
		chunks:      relayed,
		tp:          tp,
		cancel:      cancel,
		synthSubmit: synthSubmit,
		audible:     audible,
		synthesized: synthesized,
	}, nil
}

// relayAudible forwards synthesized chunks unchanged and closes audible as soon
// as the first one is in hand, so a caller can tell when the backend actually
// began returning audio for this utterance. audible also closes when synthesis
// ends without producing any, so a waiter is never stranded.
func relayAudible(ctx context.Context, in <-chan tts.PCMChunk, out chan<- tts.PCMChunk, audible chan struct{}) {
	defer close(out)
	signalled := false
	defer func() {
		if !signalled {
			close(audible)
		}
	}()
	for chunk := range in {
		if !signalled {
			signalled = true
			close(audible)
		}
		select {
		case out <- chunk:
		case <-ctx.Done():
			return
		}
	}
}

// WaitAudible blocks until this utterance's synthesis has produced its first
// audio, ctx is done, or timeout elapses. Callers use it to keep a later
// synthesis request from queueing in front of audio the caller is waiting on.
func (p *PreparedSay) WaitAudible(ctx context.Context, timeout time.Duration) {
	if p == nil || p.audible == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.audible:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// WaitSynthesized blocks until the backend has returned all of this
// utterance's audio (or failed), ctx is done, or timeout elapses. Callers use
// it to keep the next synthesis from competing with a stream still playing.
func (p *PreparedSay) WaitSynthesized(ctx context.Context, timeout time.Duration) {
	if p == nil || p.synthesized == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.synthesized:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// PlayPrepared claims the play slot and renders a previously prepared utterance,
// blocking until playback finishes or is interrupted (barge-in / call.stop).
// Returns ErrPlaybackInterrupted when cut short.
func (h *Handlers) PlayPrepared(ctx context.Context, callID string, ps *PreparedSay) error {
	if ps == nil {
		return nil
	}
	call, err := h.mgr.Get(callID)
	if err != nil {
		ps.Close()
		return err
	}
	cs := h.stateFor(callID, nil)

	waitCh := make(chan string, 1)
	h.registerPlayWait(callID, waitCh)
	defer h.unregisterPlayWait(callID, waitCh)

	playCtx, playCancel := context.WithCancel(call.Context())
	cs.playMu.Lock()
	if cs.playCancel != nil {
		cs.playCancel()
	}
	cs.playSeq++
	myPlaySeq := cs.playSeq
	cs.playCancel = playCancel
	cs.playMu.Unlock()

	// Stop the background synthesis once playback ends or is interrupted.
	defer ps.cancel()

	h.broadcast("call.assistant_text", call.ID(), map[string]any{
		"play_id": ps.playID,
		"text":    ps.text,
		"voice":   ps.voice,
	})
	cs.sayMu.Lock()
	cs.prevSayText = cs.lastSayText
	cs.lastSayText = ps.text
	cs.lastSayAt = time.Now()
	cs.sayMu.Unlock()

	observability.CallTraceCall(ctx, h.log, "rpc.say", call, "play_id", ps.playID, "text_len", len([]rune(ps.text)))
	observability.DialogTurn(ctx, h.log, call.ConversationID(), call.ID(), "assistant", ps.text, call.DialogAttrs()...)

	ps.playedMs = h.playChunks(ctx, call, cs, playCtx, playCancel, myPlaySeq, ps.playID, ps.synthSubmit, ps.chunks, ps.tp)

	select {
	case reason := <-waitCh:
		if reason == "interrupted" || reason == "ended" {
			return ErrPlaybackInterrupted
		}
	default:
	}
	return nil
}
