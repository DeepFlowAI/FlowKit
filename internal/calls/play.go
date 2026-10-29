package calls

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/audio"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// playHTTPClient fetches audio_url sources. A short timeout keeps a bad URL
// from stalling the RPC; the fetch happens before the play slot is claimed.
var playHTTPClient = &http.Client{Timeout: 10 * time.Second}

// maxAudioBytes caps how large a fetched or inlined audio file may be, so a
// misconfigured tone URL cannot exhaust memory.
const maxAudioBytes = 8 << 20 // 8 MiB

type playParams struct {
	CallID string `json:"call_id"`

	// Source — exactly one of Text / AudioURL / Audio must be set.
	Text     string `json:"text,omitempty"`
	AudioURL string `json:"audio_url,omitempty"`
	Audio    string `json:"audio,omitempty"` // base64 audio file bytes

	// Loop, when true, re-plays the prompt every IntervalMs until stopped.
	// The PCM is synthesized/decoded once and cached; each loop replays it.
	Loop       bool `json:"loop,omitempty"`
	IntervalMs int  `json:"interval_ms,omitempty"`

	Voice     string `json:"voice,omitempty"`
	LeadPadMs int    `json:"lead_pad_ms,omitempty"`
}

type playResult struct {
	PlayID string `json:"play_id"`
	Status string `json:"status"`
}

// handlePlay plays a prompt-tone to the call's downstream. With loop=false it
// plays once (transfer tone). With loop=true it caches the PCM and replays it
// every interval_ms until call.stop / barge-in / the leg ends / a newer play
// supersedes it. It shares the play slot (playSeq / playCancel) with call.say,
// so only one audio source is active per leg at a time.
func (h *Handlers) handlePlay(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p playParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if err := p.validateSource(); err != nil {
		return nil, rpc.ErrInvalidParams(err.Error())
	}

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.play"); rpcErr != nil {
		return nil, rpcErr
	}
	cs := h.stateFor(call.ID(), conn)

	// Resolve the source to PCM once, before claiming the play slot, so no DB
	// session / lock / playback is held during the (possibly slow) fetch/decode.
	pcm, rpcErr := h.resolvePlayPCM(ctx, &p)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if len(pcm) == 0 {
		return nil, rpc.ErrInvalidParams("source produced no audio")
	}

	playID := newShortID()

	// Claim the play slot: cancel any in-flight say/play and bump playSeq so a
	// newer play/say/stop supersedes this one. Child of call.Context so hangup
	// cancels playback without a separate Done watcher.
	playCtx, playCancel := context.WithCancel(call.Context())
	cs.playMu.Lock()
	if cs.playCancel != nil {
		cs.playCancel()
	}
	cs.playSeq++
	myPlaySeq := cs.playSeq
	cs.playCancel = playCancel
	cs.playMu.Unlock()

	interval := time.Duration(p.IntervalMs) * time.Millisecond
	go h.runPlay(ctx, call, cs, playCtx, playCancel, myPlaySeq, playID, pcm, p.Loop, interval)

	observability.CallTraceCall(ctx, h.log, "rpc.play", call,
		"play_id", playID, "loop", p.Loop, "interval_ms", p.IntervalMs, "samples", len(pcm))
	return playResult{PlayID: playID, Status: "playing"}, nil
}

// runPlay renders cached PCM to the call's outbound media. It owns the play
// slot teardown (clears cs.playCancel for myPlaySeq) and the cached PCM's
// lifetime. For loop=false it plays once; for loop=true it replays every
// interval, measured from the end of the previous playback, skipping (never
// stacking) a tick that arrives while a playback is still in flight.
func (h *Handlers) runPlay(ctx context.Context, call *session.Call, cs *callState,
	playCtx context.Context, playCancel context.CancelFunc, myPlaySeq uint64,
	playID string, pcm []int16, loop bool, interval time.Duration) {

	cs.playing.Store(true)
	cs.playStartedNano.Store(time.Now().UnixNano())
	start := time.Now()
	cs.firstAudioNano.CompareAndSwap(0, start.UnixNano())
	// Snapshot pacer stats at play start so finish() can attribute wire-level
	// anomalies (duck / pause / send-gap) to this play_id.
	startStats, statsOK := playbackStatsSnapshot(call)
	defer cs.playing.Store(false)
	defer cs.playStartedNano.Store(0)
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

	// playOnce writes the cached PCM out once. Returns false when interrupted
	// or the leg ended, so the loop can stop.
	playOnce := func(padTail bool) bool {
		n, err := h.writeOutgoingFrames(playCtx, call, pcm)
		totalSamples += int64(n)
		if n > 0 {
			cs.mediaPrimed.Store(true)
		}
		if err != nil {
			if playCtx.Err() != nil {
				h.stopOutgoingPlayback(call.ID())
			}
			return false
		}
		if padTail {
			if err := h.writePlaybackTail(playCtx, call); err != nil {
				if playCtx.Err() != nil {
					h.stopOutgoingPlayback(call.ID())
				}
				return false
			}
		}
		return h.waitOutgoingPlayback(call, playCtx) == nil
	}

	// played_ms is duration of PCM submitted to the outgoing queue, not bytes
	// confirmed on the wire. On interrupt, StopOutgoing drops queued-but-unsent
	// audio, so played_ms can overestimate what the far end heard.
	// The pacer stat deltas below record what actually went on the wire.
	finish := func(reason string) {
		playedMS := totalSamples * 1000 / int64(sampleRate)
		wallMS := time.Since(start).Milliseconds()
		fields := []any{
			"play_id", playID, "reason", reason,
			"played_ms", playedMS, "wall_ms", wallMS,
		}
		payload := map[string]any{
			"play_id":   playID,
			"reason":    reason,
			"played_ms": playedMS,
			"wall_ms":   wallMS,
		}
		if statsOK {
			if endStats, ok := playbackStatsSnapshot(call); ok {
				// max_send_gap_ms is a cumulative max and cannot be diffed; it
				// is the call-lifetime upper bound for gaps inside this play.
				deltas := map[string]uint64{
					"ducked_frames":     endStats.DuckedFrames - startStats.DuckedFrames,
					"pause_events":      endStats.PauseEvents - startStats.PauseEvents,
					"paused_ms":         endStats.PausedMs - startStats.PausedMs,
					"send_gap_overruns": endStats.SendGapOverruns - startStats.SendGapOverruns,
					"max_send_gap_ms":   endStats.MaxSendGapMs,
				}
				for k, v := range deltas {
					fields = append(fields, k, v)
					payload[k] = v
				}
			}
		}
		observability.CallTraceCall(ctx, h.log, "play.end", call, fields...)
		h.dispatch(call.ID(), "call.play_end", payload)
		h.signalPlayWait(call.ID(), reason)
	}

	if call.IsEnded() {
		finish("ended")
		return
	}

	if !playOnce(!loop) {
		if playCtx.Err() != nil {
			finish("interrupted")
		} else {
			finish("ended")
		}
		return
	}

	if !loop || interval <= 0 {
		finish("completed")
		return
	}

	// Loop: replay the cached PCM every interval, measured from the end of the
	// previous playback. A ticker whose tick arrives mid-playback is coalesced
	// (Go tickers drop, they do not stack), so we never overlap.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-playCtx.Done():
			finish("interrupted")
			return
		case <-call.Done():
			finish("ended")
			return
		case <-ticker.C:
			if call.IsEnded() {
				finish("ended")
				return
			}
			if !playOnce(false) {
				if playCtx.Err() != nil {
					finish("interrupted")
				} else {
					finish("ended")
				}
				return
			}
			// Restart the interval from the end of this playback so the gap
			// between tones is measured from silence, not from tone start.
			ticker.Reset(interval)
		}
	}
}

// validateSource enforces that exactly one source field is set.
func (p *playParams) validateSource() error {
	n := 0
	if p.Text != "" {
		n++
	}
	if p.AudioURL != "" {
		n++
	}
	if p.Audio != "" {
		n++
	}
	if n == 0 {
		return fmt.Errorf("missing source: one of text, audio_url, audio required")
	}
	if n > 1 {
		return fmt.Errorf("multiple sources: set only one of text, audio_url, audio")
	}
	return nil
}

// resolvePlayPCM turns the play source into 16 kHz mono PCM once. TTS text is
// synthesized to completion; audio_url is fetched then decoded; base64 audio is
// decoded in place.
func (h *Handlers) resolvePlayPCM(ctx context.Context, p *playParams) ([]int16, *rpc.Error) {
	switch {
	case p.Text != "":
		return h.synthesizePlayPCM(ctx, p.Text, p.Voice, p.LeadPadMs)
	case p.AudioURL != "":
		if err := h.checkOutboundURL(p.AudioURL); err != nil {
			return nil, rpc.ErrInvalidParams(err.Error())
		}
		data, err := fetchAudio(ctx, p.AudioURL)
		if err != nil {
			return nil, rpc.NewErrorf(rpc.CodeInvalidParams, "fetch audio_url: %v", err)
		}
		return decodePlayAudio(data)
	default: // p.Audio (base64)
		data, err := base64.StdEncoding.DecodeString(p.Audio)
		if err != nil {
			return nil, rpc.ErrInvalidParams("audio is not valid base64")
		}
		if len(data) > maxAudioBytes {
			return nil, rpc.ErrInvalidParams("audio exceeds size limit")
		}
		return decodePlayAudio(data)
	}
}

// synthesizePlayPCM runs one TTS synthesis to completion and returns the whole
// PCM buffer (so a loop can replay it without re-synthesizing).
func (h *Handlers) synthesizePlayPCM(ctx context.Context, text, voice string, leadPadMs int) ([]int16, *rpc.Error) {
	tp, err := h.factories.TTS()
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "%v", err)
	}
	defer func() { _ = tp.Close() }()
	chunks, err := tp.Synthesize(ctx, text, tts.Options{
		Voice: voice, LeadPadMs: leadPadMs,
	})
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "%v", err)
	}
	var pcm []int16
	for c := range chunks {
		pcm = append(pcm, c.Samples...)
	}
	return pcm, nil
}

func decodePlayAudio(data []byte) ([]int16, *rpc.Error) {
	pcm, err := audio.DecodeToPCM16k(data)
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeInvalidParams, "%v", err)
	}
	// Uploaded prompts are user assets with uncontrolled levels; a too-quiet
	// prompt gets chopped by the carrier's AMR transcoding VAD. Level only the
	// unhealthy ones; healthy prompts pass through byte-for-byte.
	return audio.LevelSpeechForPlayback(pcm), nil
}

// fetchAudio downloads an audio file with a timeout and a size cap. The read is
// bounded so an unbounded response cannot exhaust memory.
func fetchAudio(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := playHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAudioBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxAudioBytes {
		return nil, fmt.Errorf("audio exceeds size limit")
	}
	return data, nil
}

func (h *Handlers) registerPlayWait(callID string, ch chan string) {
	cs := h.stateFor(callID, nil)
	h.playWaitMu.Lock()
	cs.playWaits = append(cs.playWaits, ch)
	h.playWaitMu.Unlock()
}

func (h *Handlers) unregisterPlayWait(callID string, ch chan string) {
	cs := h.stateGet(callID)
	if cs == nil {
		return
	}
	h.playWaitMu.Lock()
	waiters := cs.playWaits
	out := waiters[:0]
	for _, w := range waiters {
		if w != ch {
			out = append(out, w)
		}
	}
	cs.playWaits = out
	h.playWaitMu.Unlock()
}

func (h *Handlers) signalPlayWait(callID, reason string) {
	cs := h.stateGet(callID)
	if cs == nil {
		return
	}
	h.playWaitMu.Lock()
	waiters := cs.playWaits
	h.playWaitMu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- reason:
		default:
		}
	}
}

// IsPlaying reports whether TTS is actively playing on the call.
func (h *Handlers) IsPlaying(callID string) bool {
	h.mu.Lock()
	cs := h.callState[callID]
	h.mu.Unlock()
	if cs == nil {
		return false
	}
	return cs.playing.Load()
}

// playBargeGrace is how long after RTP play starts before partial ASR may barge-in
// (reduces false cuts from TTS echo in the first syllable).
const playBargeGrace = 500 * time.Millisecond

// IsInterruptible reports whether user speech should trigger barge-in.
// True while TTS is synthesizing or RTP is playing (playCancel is held for both).
func (h *Handlers) IsInterruptible(callID string) bool {
	if h.IsPlaying(callID) {
		h.mu.Lock()
		cs := h.callState[callID]
		h.mu.Unlock()
		if cs == nil {
			return false
		}
		start := cs.playStartedNano.Load()
		if start != 0 && time.Since(time.Unix(0, start)) < playBargeGrace {
			return false
		}
		return true
	}
	h.mu.Lock()
	cs := h.callState[callID]
	h.mu.Unlock()
	if cs == nil {
		return false
	}
	cs.playMu.Lock()
	active := cs.playCancel != nil
	cs.playMu.Unlock()
	return active
}

func (h *Handlers) stopOutgoingPlayback(callID string) {
	call, err := h.mgr.Get(callID)
	if err != nil {
		return
	}
	mio := call.MediaIO()
	if mio == nil {
		return
	}
	mio.StopOutgoing()
}

// PausePlay pauses paced outbound audio without discarding it, so ResumePlay
// continues from the same position (adaptive barge-in candidate). Returns true
// only if something was playing and media is bound.
func (h *Handlers) PausePlay(callID string) bool {
	if !h.IsPlaying(callID) {
		return false
	}
	call, err := h.mgr.Get(callID)
	if err != nil {
		return false
	}
	mio := call.MediaIO()
	if mio == nil {
		return false
	}
	mio.PauseOutgoing()
	return true
}

// ResumePlay releases a PausePlay. No-op if not paused or media unbound.
func (h *Handlers) ResumePlay(callID string) {
	call, err := h.mgr.Get(callID)
	if err != nil {
		return
	}
	if mio := call.MediaIO(); mio != nil {
		mio.ResumeOutgoing()
	}
}

// DuckPlay attenuates paced outbound audio without changing playback timing.
// Returns true only while media is bound and actively playing.
func (h *Handlers) DuckPlay(callID string, gain float64) bool {
	if !h.IsPlaying(callID) {
		return false
	}
	call, err := h.mgr.Get(callID)
	if err != nil {
		return false
	}
	mio := call.MediaIO()
	if mio == nil {
		return false
	}
	mio.DuckOutgoing(gain)
	return true
}

// RestorePlay releases a DuckPlay. No-op if media unbound.
func (h *Handlers) RestorePlay(callID string) {
	call, err := h.mgr.Get(callID)
	if err != nil {
		return
	}
	if mio := call.MediaIO(); mio != nil {
		mio.RestoreOutgoing()
	}
}

func (h *Handlers) writeOutgoingFrames(ctx context.Context, call *session.Call, samples []int16) (int, error) {
	const frameSamples = bus.SamplesPerFrame
	written := 0
	for off := 0; off < len(samples); off += frameSamples {
		if ctx.Err() != nil {
			return written, ctx.Err()
		}
		end := off + frameSamples
		if end > len(samples) {
			end = len(samples)
		}
		if err := call.WriteOutgoingCtx(ctx, samples[off:end]); err != nil {
			return written, err
		}
		written += end - off
	}
	return written, nil
}

// writePlaybackTail keeps RTP flowing with silence after a finite prompt. The
// far end may still have speech buffered when the source PCM is exhausted;
// draining that buffer before play_end prevents a following hangup or media
// transition from clipping the final words. Tail samples are transport padding
// and are intentionally excluded from the prompt's played_ms.
func (h *Handlers) writePlaybackTail(ctx context.Context, call *session.Call) error {
	silence := make([]int16, _sayTailPadMs*bus.SampleRate/1000)
	_, err := h.writeOutgoingFrames(ctx, call, silence)
	return err
}

func (h *Handlers) waitOutgoingPlayback(call *session.Call, ctx context.Context) error {
	mio := call.MediaIO()
	if mio == nil {
		return nil
	}
	return mio.WaitPlaybackIdle(ctx)
}

// playbackStatsSnapshot returns the call's pacer counters when the bound
// transport exposes them (same probe as media.summary). Transports without
// PlaybackStats degrade to ok=false and no per-play stat fields.
func playbackStatsSnapshot(call *session.Call) (pacer.Stats, bool) {
	mio := call.MediaIO()
	if mio == nil {
		return pacer.Stats{}, false
	}
	if stats, ok := mio.(interface{ PlaybackStats() pacer.Stats }); ok {
		return stats.PlaybackStats(), true
	}
	return pacer.Stats{}, false
}
