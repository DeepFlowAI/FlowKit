// Package agentmedia exposes flowkit's optimized ASR/TTS providers as
// standalone, call-less JSON-RPC methods so non-telephony consumers (e.g. the
// VoiceSpeed agent console) can stream raw audio through the same engine that
// the SIP/WebRTC call layer uses — without the call/transport model.
//
// Wire protocol (all over the existing JSON-RPC WebSocket):
//
//	ASR  client→server : asr.start(req) → {session_id}
//	                     asr.audio(notification) {session_id, audio:base64(int16 LE 16k mono)}
//	                     asr.stop(req) {session_id} → {ok}
//	     server→client : asr.event(notification) {session_id, type:partial|final|error, text, seq}
//
//	TTS  client→server : tts.synthesize(req) {text, voice?, sample_rate?} → {synthesis_id}
//	                     tts.cancel(req) {synthesis_id} → {ok}
//	     server→client : tts.chunk(notification) {synthesis_id, audio:base64(int16 LE mono @ sample_rate), final}
//
// Sessions are keyed per RPC connection; dropping the connection tears down its
// providers.
package agentmedia

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/media/vad"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	"github.com/google/uuid"
)

// Handlers owns standalone ASR/TTS sessions, grouped by the RPC connection that
// created them so a dropped connection releases every provider it held.
type Handlers struct {
	asrFactory func() (asr.Provider, error)
	ttsFactory func() (tts.Provider, error)
	log        *slog.Logger

	mu      sync.Mutex
	asr     map[string]*asrSession // session_id → session
	tts     map[string]*ttsJob     // synthesis_id → job
	watched map[string]bool        // conn.ID() → close-watcher running
}

type asrSession struct {
	id       string
	connID   string
	provider asr.Provider
	// audio frames are funneled through one channel so a single goroutine feeds
	// the provider (preserving order — asr.audio notifications are dispatched
	// concurrently) and the VAD (which isn't safe under concurrent Feed).
	audio    chan []int16
	stopOnce sync.Once
}

func (s *asrSession) stop() {
	s.stopOnce.Do(func() {
		close(s.audio)        // ends pumpAudio
		_ = s.provider.Stop() // closes Events → pumpASR returns
	})
}

type ttsJob struct {
	id     string
	connID string
	cancel context.CancelFunc
}

// Register wires the asr.* / tts.* methods into reg. asrFactory/ttsFactory
// build a fresh provider per session (ASR is stateful per stream; TTS is
// request/response). A nil factory makes the corresponding methods report
// "unavailable" rather than panicking.
func Register(
	reg *rpc.Registry,
	asrFactory func() (asr.Provider, error),
	ttsFactory func() (tts.Provider, error),
	log *slog.Logger,
) *Handlers {
	if log == nil {
		log = slog.Default()
	}
	h := &Handlers{
		asrFactory: asrFactory,
		ttsFactory: ttsFactory,
		log:        log,
		asr:        make(map[string]*asrSession),
		tts:        make(map[string]*ttsJob),
		watched:    make(map[string]bool),
	}
	reg.Register("asr.start", h.handleASRStart)
	reg.RegisterNotification("asr.audio", h.handleASRAudio)
	reg.Register("asr.stop", h.handleASRStop)
	reg.Register("tts.synthesize", h.handleTTSSynthesize)
	reg.Register("tts.cancel", h.handleTTSCancel)
	return h
}

// ---- ASR ----

func (h *Handlers) handleASRStart(_ context.Context, conn *rpc.Conn, _ json.RawMessage) (any, *rpc.Error) {
	if h.asrFactory == nil {
		return nil, rpc.NewError(rpc.CodeASRUnavailable, "asr not configured")
	}
	provider, err := h.asrFactory()
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeASRUnavailable, "asr provider: %v", err)
	}
	if err := provider.Start(context.Background()); err != nil {
		return nil, rpc.NewErrorf(rpc.CodeASRUnavailable, "asr start: %v", err)
	}
	id := uuid.NewString()
	s := &asrSession{
		id:       id,
		connID:   conn.ID(),
		provider: provider,
		audio:    make(chan []int16, 128),
	}
	h.mu.Lock()
	h.asr[id] = s
	h.mu.Unlock()
	h.ensureWatcher(conn)
	go h.pumpAudio(s)
	go h.pumpASR(conn, s)
	return map[string]string{"session_id": id}, nil
}

// vadHangoverMs is how long the neural VAD must report "not speaking" after
// speech before we treat the utterance as finished and flush. VAD-gated, so it
// only counts silence *after* confirmed speech and resets if speech resumes —
// far tighter than a raw silence timer.
const vadHangoverMs = 360

// pumpAudio feeds frames to FunASR in order (single goroutine) and uses the
// neural VAD to detect end-of-utterance, flushing FunASR at that instant — the
// integrated-VAD behavior Aliyun NLS provides, but driven by the Silero VAD the
// operator already runs. The provider's own (long) silence timer stays as a
// fallback in case the VAD is unavailable.
func (h *Handlers) pumpAudio(s *asrSession) {
	var det *vad.Detector
	if vad.Enabled() {
		det = vad.NewDetector()
	}
	flusher, _ := s.provider.(asr.Flusher)
	var speaking bool
	var silenceMs, frameN, speechWinTotal int
	var peakRMS, lastTickRMS float64

	for samples := range s.audio {
		frameN++
		if err := s.provider.SendPCM(samples); err != nil {
			h.log.Debug("asr SendPCM failed", "session_id", s.id, "err", err)
			return
		}
		if frameN == 1 {
			// Format sanity: frame_samples lets us infer the client's framing;
			// if rms stays ~0 while the user speaks, audio isn't arriving / wrong
			// format; if rms is high but speech_windows stays 0, Silero isn't
			// flagging this mic audio as speech (the usual root cause).
			h.log.Info("asr stream start", "session_id", s.id,
				"frame_samples", len(samples), "vad", det != nil, "flusher", flusher != nil)
		}
		if det == nil || flusher == nil {
			continue // no neural VAD → rely on the provider's silence-timer endpoint
		}
		sw := det.Feed(samples)
		speechWinTotal += sw
		rms := pcmRMS(samples)
		if rms > peakRMS {
			peakRMS = rms
		}
		lastTickRMS = rms
		// Heartbeat every ~25 frames so a session's VAD behaviour is always in the
		// logs without per-frame spam.
		if frameN%25 == 0 {
			h.log.Info("asr vad tick", "session_id", s.id, "frame", frameN,
				"rms", int(rms), "speech_win", sw, "speaking", det.Speaking(),
				"silence_ms", silenceMs)
		}
		if det.Speaking() {
			if !speaking {
				h.log.Info("asr vad speech START", "session_id", s.id, "frame", frameN, "rms", int(rms))
			}
			speaking = true
			silenceMs = 0
			continue
		}
		if !speaking {
			continue // pre-speech / inter-utterance silence
		}
		silenceMs += len(samples) * 1000 / 16000
		if silenceMs >= vadHangoverMs {
			if err := flusher.Flush(); err != nil {
				h.log.Debug("asr flush failed", "session_id", s.id, "err", err)
			} else {
				h.log.Info("asr vad endpoint flush (speech END)", "session_id", s.id, "frame", frameN)
			}
			speaking = false
			silenceMs = 0
		}
	}
	h.log.Info("asr stream end", "session_id", s.id, "frames", frameN,
		"peak_rms", int(peakRMS), "last_rms", int(lastTickRMS), "speech_windows", speechWinTotal)
}

func pcmRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, v := range samples {
		f := float64(v)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(samples)))
}

func (h *Handlers) pumpASR(conn *rpc.Conn, s *asrSession) {
	for ev := range s.provider.Events() {
		var typ string
		switch ev.Type {
		case asr.EventPartial:
			// VAD-driven flushing means finals only follow real detected speech,
			// so the only filter needed is dropping obvious noise text (pure
			// punctuation / single fillers).
			if !asrgate.PlausibleUserSpeech(ev.Text) {
				continue
			}
			typ = "partial"
		case asr.EventFinal:
			// FunASR's punctuation model prepends a separator (，。？！) to a
			// segment that follows a flush, treating the stream as continuous.
			// Strip leading punctuation/space so each user turn reads cleanly.
			text := strings.TrimLeft(strings.TrimSpace(ev.Text), "，。？！、；：,.?!;:… 　")
			emit := asrgate.PlausibleUserSpeech(text)
			// Every final logged with whether we keep it — lets us see FunASR's
			// raw output (e.g. leading-punctuation fragments from a mid-utterance
			// flush) and correlate with the vad flush events above.
			h.log.Info("asr final", "session_id", s.id, "text", text, "emit", emit)
			if !emit {
				continue // empty flush re-arm or noise
			}
			ev.Text = text
			typ = "final"
		case asr.EventError:
			typ = "error"
		default:
			continue // VAD start/end are internal to the engine here
		}
		payload := map[string]any{
			"session_id": s.id,
			"type":       typ,
			"text":       ev.Text,
			"seq":        ev.Sequence,
		}
		if ev.Err != nil {
			payload["error"] = ev.Err.Error()
		}
		n, _ := rpc.NewNotification("asr.event", payload)
		if err := conn.SendNotification(n); err != nil {
			h.log.Debug("asr.event send failed", "session_id", s.id, "err", err)
		}
	}
}

func (h *Handlers) handleASRAudio(_ context.Context, _ *rpc.Conn, params json.RawMessage) {
	var p struct {
		SessionID string `json:"session_id"`
		Audio     string `json:"audio"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return
	}
	h.mu.Lock()
	s := h.asr[p.SessionID]
	h.mu.Unlock()
	if s == nil {
		return
	}
	raw, err := base64.StdEncoding.DecodeString(p.Audio)
	if err != nil || len(raw) < 2 {
		return
	}
	// Non-blocking: under severe backpressure drop the frame rather than stall
	// the WS reader or reorder audio.
	select {
	case s.audio <- bytesToInt16LE(raw):
	default:
		h.log.Debug("asr audio buffer full, dropping frame", "session_id", p.SessionID)
	}
}

func (h *Handlers) handleASRStop(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p struct {
		SessionID string `json:"session_id"`
	}
	_ = rpc.DecodeParams(params, &p)
	h.stopASR(p.SessionID)
	return map[string]bool{"ok": true}, nil
}

func (h *Handlers) stopASR(id string) {
	h.mu.Lock()
	s := h.asr[id]
	delete(h.asr, id)
	h.mu.Unlock()
	if s != nil {
		s.stop()
	}
}

// ---- TTS ----

func (h *Handlers) handleTTSSynthesize(_ context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p struct {
		Text       string `json:"text"`
		Voice      string `json:"voice"`
		SampleRate int    `json:"sample_rate"`
	}
	if e := rpc.DecodeParams(params, &p); e != nil {
		return nil, e
	}
	if p.Text == "" {
		return nil, rpc.NewError(rpc.CodeInvalidParams, "text required")
	}
	if h.ttsFactory == nil {
		return nil, rpc.NewError(rpc.CodeTTSUnavailable, "tts not configured")
	}
	provider, err := h.ttsFactory()
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "tts provider: %v", err)
	}
	synthCtx, cancel := context.WithCancel(context.Background())
	chunks, err := provider.Synthesize(synthCtx, p.Text, tts.Options{Voice: p.Voice, SampleRate: p.SampleRate})
	if err != nil {
		cancel()
		_ = provider.Close()
		return nil, rpc.NewErrorf(rpc.CodeTTSUnavailable, "tts synthesize: %v", err)
	}
	id := uuid.NewString()
	job := &ttsJob{id: id, connID: conn.ID(), cancel: cancel}
	h.mu.Lock()
	h.tts[id] = job
	h.mu.Unlock()
	h.ensureWatcher(conn)
	go h.pumpTTS(conn, job, provider, synthCtx, chunks)
	return map[string]string{"synthesis_id": id}, nil
}

func (h *Handlers) pumpTTS(conn *rpc.Conn, job *ttsJob, provider tts.Provider, ctx context.Context, chunks <-chan tts.PCMChunk) {
	defer func() {
		_ = provider.Close()
		job.cancel()
		h.mu.Lock()
		delete(h.tts, job.id)
		h.mu.Unlock()
	}()
	for chunk := range chunks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		payload := map[string]any{
			"synthesis_id": job.id,
			"audio":        base64.StdEncoding.EncodeToString(int16ToLEBytes(chunk.Samples)),
			"final":        chunk.Final,
		}
		n, _ := rpc.NewNotification("tts.chunk", payload)
		if err := sendWithBackpressure(conn, n, ctx); err != nil {
			h.log.Debug("tts.chunk send failed", "synthesis_id", job.id, "err", err)
			return
		}
	}
}

func (h *Handlers) handleTTSCancel(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p struct {
		SynthesisID string `json:"synthesis_id"`
	}
	_ = rpc.DecodeParams(params, &p)
	h.mu.Lock()
	job := h.tts[p.SynthesisID]
	h.mu.Unlock()
	if job != nil {
		job.cancel() // pumpTTS observes ctx.Done, drains/closes, deletes itself
	}
	return map[string]bool{"ok": true}, nil
}

// ---- connection lifecycle ----

// ensureWatcher spawns exactly one goroutine per connection that releases every
// ASR/TTS session the connection owns once it closes.
func (h *Handlers) ensureWatcher(conn *rpc.Conn) {
	h.mu.Lock()
	if h.watched[conn.ID()] {
		h.mu.Unlock()
		return
	}
	h.watched[conn.ID()] = true
	h.mu.Unlock()
	go func() {
		<-conn.CloseChan()
		h.cleanupConn(conn.ID())
	}()
}

func (h *Handlers) cleanupConn(connID string) {
	h.mu.Lock()
	var sessions []*asrSession
	for id, s := range h.asr {
		if s.connID == connID {
			sessions = append(sessions, s)
			delete(h.asr, id)
		}
	}
	var jobs []*ttsJob
	for id, j := range h.tts {
		if j.connID == connID {
			jobs = append(jobs, j)
			delete(h.tts, id)
		}
	}
	delete(h.watched, connID)
	h.mu.Unlock()
	for _, s := range sessions {
		s.stop()
	}
	for _, j := range jobs {
		j.cancel()
	}
}

// sendWithBackpressure retries a notification while the connection's send queue
// is full (audio bursts can transiently saturate it), bailing out if the
// synthesis is cancelled or the connection closes. Bounds the wait so a stuck
// peer can't pin the goroutine forever.
func sendWithBackpressure(conn *rpc.Conn, n *rpc.Notification, ctx context.Context) error {
	for attempts := 0; attempts < 1000; attempts++ {
		if err := conn.SendNotification(n); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-conn.CloseChan():
			return context.Canceled
		case <-time.After(5 * time.Millisecond):
		}
	}
	return context.DeadlineExceeded
}

func bytesToInt16LE(b []byte) []int16 {
	n := len(b) / 2
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		out[i] = int16(b[2*i]) | int16(b[2*i+1])<<8
	}
	return out
}

func int16ToLEBytes(s []int16) []byte {
	out := make([]byte, len(s)*2)
	for i, v := range s {
		out[2*i] = byte(v)
		out[2*i+1] = byte(v >> 8)
	}
	return out
}
