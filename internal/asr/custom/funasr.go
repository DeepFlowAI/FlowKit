// Package custom is a self-hosted ASR implementation of the asr.Provider
// interface, targeting a private FunASR WebSocket deployment.
//
// It speaks FunASR's official funasr_wss_server.py protocol:
//
//  1. client → ws(s)://host:port
//  2. client → JSON config: {mode, chunk_size, audio_fs, wav_format:"pcm", itn,
//     hotwords, is_speaking:true}
//  3. client → BINARY: 16 kHz mono int16 PCM frames
//  4. server → JSON: {"mode":"2pass-online", "text":...}  (partial)
//     server → JSON: {"mode":"2pass-offline", "text":...} (final per segment)
//  5. client → JSON: {"is_speaking": false}  (on Stop)
//
// "online" results map to asr.EventPartial, "offline" to asr.EventFinal.
package custom

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/gorilla/websocket"
)

// Config carries the FunASR-specific knobs the adapter needs.
type Config struct {
	// BaseURL is the FunASR WebSocket endpoint, e.g. "ws://127.0.0.1:10095".
	// Use wss:// for the server's default self-signed TLS (see InsecureSkipVerify).
	BaseURL string
	// Mode is "2pass" (default), "online", or "offline".
	Mode string
	// Hotwords is FunASR's hotword payload ("word weight" lines, or JSON).
	Hotwords string
	// ITN toggles inverse text normalization (e.g. "二零二六" → "2026").
	ITN bool
	// SampleRate of the PCM fed via SendPCM. Defaults to 16000.
	SampleRate int
	// ChunkSize is FunASR's streaming window [a,b,c]; b governs the window
	// (~b*60ms). Smaller b → faster partials / lower latency. Defaults to
	// [5,10,5] (~600ms); [8,8,4] (~480ms) is snappier.
	ChunkSize []int
	// InsecureSkipVerify accepts the FunASR server's self-signed wss cert.
	InsecureSkipVerify bool
	// EndpointSilenceMs, when > 0, forces an end-of-utterance flush after this
	// many ms of trailing silence following speech — instead of waiting for the
	// FunASR server's own (often slow) VAD timeout. This compresses the gap
	// between "user stops talking" and the final result (speech_end). The server
	// still does its full 2-pass decode on the buffered audio, so text/ITN are
	// unaffected; it just returns sooner. 0 disables (server VAD only).
	EndpointSilenceMs int
	// EndpointSilenceRMS is the per-frame RMS energy below which audio counts as
	// silence. Defaults to 350. Ignored when EndpointSilenceMs == 0.
	EndpointSilenceRMS int
}

const writeTimeout = 5 * time.Second

// Provider implements asr.Provider by streaming PCM to a FunASR WS session.
type Provider struct {
	cfg    Config
	conn   *websocket.Conn
	events chan asr.Event

	writeMu sync.Mutex

	startOnce  sync.Once
	startErr   error
	stopOnce   sync.Once
	eventsOnce sync.Once
	stopped    atomic.Bool

	seq atomic.Int32

	// forced-endpoint state; touched only from SendPCM under writeMu.
	hasVoiced bool
	silentMs  int
}

// New constructs a Provider. BaseURL is required; the session opens in Start.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("funasr asr: BaseURL required (ASR_BASE_URL)")
	}
	if cfg.Mode == "" {
		cfg.Mode = "2pass"
	}
	if cfg.SampleRate == 0 {
		cfg.SampleRate = 16000
	}
	if len(cfg.ChunkSize) != 3 {
		cfg.ChunkSize = []int{5, 10, 5}
	}
	if cfg.EndpointSilenceRMS == 0 {
		cfg.EndpointSilenceRMS = 350
	}
	return &Provider{
		cfg:    cfg,
		events: make(chan asr.Event, 32),
	}, nil
}

// Start implements asr.Provider. It dials the WebSocket and sends the FunASR
// config frame before returning.
func (p *Provider) Start(ctx context.Context) error {
	p.startOnce.Do(func() {
		dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
		if strings.HasPrefix(p.cfg.BaseURL, "wss") && p.cfg.InsecureSkipVerify {
			dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 — self-hosted FunASR self-signed cert
		}
		// Matpool / many FunASR WSS gateways require subprotocol "binary".
		hdr := http.Header{"Sec-WebSocket-Protocol": []string{"binary"}}
		conn, resp, err := dialer.DialContext(ctx, p.cfg.BaseURL, hdr)
		if err != nil {
			if resp != nil {
				p.startErr = fmt.Errorf("funasr asr: dial HTTP %d: %w", resp.StatusCode, err)
			} else {
				p.startErr = fmt.Errorf("funasr asr: dial: %w", err)
			}
			p.closeEvents()
			return
		}

		startMsg := map[string]any{
			"mode":           p.cfg.Mode,
			"chunk_size":     p.cfg.ChunkSize,
			"chunk_interval": 10,
			"wav_name":       "flowkit",
			"wav_format":     "pcm",
			"audio_fs":       p.cfg.SampleRate,
			"itn":            p.cfg.ITN,
			"is_speaking":    true,
		}
		if p.cfg.Hotwords != "" {
			startMsg["hotwords"] = p.cfg.Hotwords
		}
		_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err := conn.WriteJSON(startMsg); err != nil {
			_ = conn.Close()
			p.startErr = fmt.Errorf("funasr asr: send config: %w", err)
			p.closeEvents()
			return
		}
		_ = conn.SetWriteDeadline(time.Time{}) // clear; per-write deadlines follow

		p.conn = conn
		go p.readLoop()
	})
	return p.startErr
}

// writeLocked sets a write deadline then writes. Caller must hold writeMu.
// Without a deadline a half-open FunASR peer can block WriteMessage forever
// while writeMu is held, stalling SendPCM/Flush/Stop for the whole call.
func (p *Provider) writeLocked(messageType int, data []byte) error {
	_ = p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return p.conn.WriteMessage(messageType, data)
}

// writeJSONLocked marshals then writeLocked. Caller must hold writeMu.
func (p *Provider) writeJSONLocked(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.writeLocked(websocket.TextMessage, data)
}

// SendPCM implements asr.Provider. samples are 16 kHz mono int16.
func (p *Provider) SendPCM(samples []int16) error {
	if p.conn == nil {
		return asr.ErrNotStarted
	}
	if p.stopped.Load() {
		return asr.ErrStopped
	}
	frame := int16ToLEBytes(samples)
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.writeLocked(websocket.BinaryMessage, frame); err != nil {
		return err
	}
	if p.cfg.EndpointSilenceMs > 0 {
		p.maybeForceEndpointLocked(samples)
	}
	return nil
}

// SetEndpointSilence overrides the forced end-of-utterance silence timer for
// this session (per-call VoiceSpeed performance settings). ms/rms <= 0 leave
// the existing value unchanged.
func (p *Provider) SetEndpointSilence(ms, rms int) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if ms > 0 {
		p.cfg.EndpointSilenceMs = ms
	}
	if rms > 0 {
		p.cfg.EndpointSilenceRMS = rms
	}
}

// Flush forces FunASR to finalize the current utterance and re-arm for the
// next, implementing asr.Flusher. A VAD-driven consumer calls this the instant
// the speaker stops, giving clean sentence boundaries without the RMS timer.
// Safe to call between utterances (yields an empty final the consumer ignores).
func (p *Provider) Flush() error {
	if p.conn == nil {
		return asr.ErrNotStarted
	}
	if p.stopped.Load() {
		return asr.ErrStopped
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.writeJSONLocked(map[string]any{"is_speaking": false}); err != nil {
		return err
	}
	p.hasVoiced = false
	p.silentMs = 0
	return p.writeJSONLocked(map[string]any{"is_speaking": true})
}

// maybeForceEndpointLocked forces FunASR to finalize the current utterance once
// trailing silence exceeds EndpointSilenceMs, instead of waiting for the
// server's VAD timeout. It sends is_speaking:false (flush) then is_speaking:true
// (re-arm for the next utterance). Must hold writeMu. Errors are ignored — the
// server VAD stays as the fallback.
func (p *Provider) maybeForceEndpointLocked(samples []int16) {
	if frameRMS(samples) >= float64(p.cfg.EndpointSilenceRMS) {
		p.hasVoiced = true
		p.silentMs = 0
		return
	}
	if !p.hasVoiced {
		return // leading / inter-utterance silence
	}
	p.silentMs += len(samples) * 1000 / p.cfg.SampleRate
	if p.silentMs < p.cfg.EndpointSilenceMs {
		return
	}
	_ = p.writeJSONLocked(map[string]any{"is_speaking": false})
	_ = p.writeJSONLocked(map[string]any{"is_speaking": true})
	p.hasVoiced = false
	p.silentMs = 0
}

// frameRMS is the root-mean-square amplitude of a PCM frame.
func frameRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		f := float64(s)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(samples)))
}

// Events implements asr.Provider.
func (p *Provider) Events() <-chan asr.Event { return p.events }

// Stop implements asr.Provider. It signals end-of-speech then closes the conn.
// Close runs first so a blocked SendPCM write unblocks; the is_speaking:false
// frame is best-effort with a short deadline and must not hang teardown.
func (p *Provider) Stop() error {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		if p.conn == nil {
			p.closeEvents()
			return
		}
		// Unblock any in-flight WriteMessage held under writeMu.
		_ = p.conn.Close()
		p.writeMu.Lock()
		_ = p.writeJSONLocked(map[string]any{"is_speaking": false})
		p.writeMu.Unlock()
		// readLoop exits on the closed conn and closes the events channel.
	})
	return nil
}

// readLoop translates FunASR result frames into asr.Events until the socket
// closes, then closes the events channel.
func (p *Provider) readLoop() {
	defer p.closeEvents()
	for {
		_, data, err := p.conn.ReadMessage()
		if err != nil {
			if !p.stopped.Load() &&
				!websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Default().Warn("funasr asr: websocket read", "err", err)
				p.emit(asr.Event{Type: asr.EventError, Err: fmt.Errorf("funasr asr: %w", err)})
			}
			return
		}

		var msg struct {
			Mode    string `json:"mode"`
			Text    string `json:"text"`
			IsFinal bool   `json:"is_final"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue // ignore malformed frames
		}
		if msg.Text == "" {
			continue
		}
		switch {
		case strings.Contains(msg.Mode, "offline"):
			p.emit(asr.Event{Type: asr.EventFinal, Text: msg.Text})
		case strings.Contains(msg.Mode, "online"):
			p.emit(asr.Event{Type: asr.EventPartial, Text: msg.Text})
		}
	}
}

// emit stamps a sequence number + time. Partials block so barge-in is not
// dropped under load; other events use a non-blocking send.
func (p *Provider) emit(ev asr.Event) {
	ev.Sequence = int(p.seq.Add(1))
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	if ev.Type == asr.EventPartial {
		p.events <- ev
		return
	}
	select {
	case p.events <- ev:
	default:
	}
}

func (p *Provider) closeEvents() {
	p.eventsOnce.Do(func() { close(p.events) })
}

// int16ToLEBytes encodes signed 16-bit samples as little-endian bytes.
func int16ToLEBytes(samples []int16) []byte {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	return buf
}
