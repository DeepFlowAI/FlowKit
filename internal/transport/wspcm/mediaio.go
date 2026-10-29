// Package wspcm adapts a raw WebSocket carrying 16 kHz mono int16 PCM to the
// flowkit transport.MediaIO abstraction, so an RPC client (e.g. the VoiceSpeed
// backend bridging a browser) can feed a call's mic audio in and receive its TTS
// audio out while flowkit runs the FULL call core on the leg: FunASR + Silero
// VAD (+ hybrid gate), Smart-Turn turn detection, adaptive barge-in, full-pause
// and streaming TTS via converse. Only server-side AEC is absent — the browser
// does its own, and the link is clean wideband, so none is needed.
//
// Binary WS frames = little-endian int16 PCM (16 kHz mono), both directions. Text
// frames are reserved for control. Outgoing pacing and playback control come
// from the embedded pacer.Outgoing; this package only encodes frames onto the
// socket and runs the inbound pump.
//
// With WithSendAhead the leg sends each utterance up to that far ahead of real
// time, so the client's playout buffer rides out network jitter. The client then
// holds audio the server already sent, so barge-in has to reach it too: Stop,
// Pause, Resume, Duck and Restore are also sent as text frames
//
//	{"type":"playback","action":"flush"|"pause"|"resume"|"gain","gain":0.25}
//
// which the client applies to its own buffer.
package wspcm

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/gorilla/websocket"
)

const (
	sampleRate   = 16000
	frameSamples = sampleRate / 50 // 20 ms @ 16 kHz mono = 320
	writeTimeout = 5 * time.Second
)

// MediaIO is one WS-backed PCM leg. Embedding pacer.Outgoing satisfies
// OutgoingPCM plus the paced-playback half of transport.MediaIO (no DTMF).
type MediaIO struct {
	pacer.Outgoing

	ws        *websocket.Conn
	log       *slog.Logger
	writeMu   sync.Mutex    // gorilla/websocket allows one concurrent writer
	sendAhead time.Duration // > 0: client buffers and takes playback control frames

	incoming chan []int16

	closeOnce    sync.Once
	incomingOnce sync.Once
	closed       atomic.Bool
	done         chan struct{}
}

// Option configures New.
type Option func(*MediaIO)

// WithSendAhead enables send-ahead delivery (see the package doc). Only for
// clients that announced they apply playback control frames.
func WithSendAhead(d time.Duration) Option {
	return func(m *MediaIO) { m.sendAhead = d }
}

// New builds a MediaIO over an accepted WebSocket and starts its pumps.
// outBuf is the OutgoingPCM queue depth passed to the shared pacer.
func New(ws *websocket.Conn, inBuf, outBuf int, log *slog.Logger, opts ...Option) *MediaIO {
	m := &MediaIO{
		ws:       ws,
		log:      log,
		incoming: make(chan []int16, inBuf),
		done:     make(chan struct{}),
	}
	for _, o := range opts {
		o(m)
	}
	m.Outgoing.Bind(m, pacer.WithQueue(outBuf), pacer.WithSendAhead(m.sendAhead))
	go m.runIncomingPump()
	return m
}

type playbackControl struct {
	Type   string  `json:"type"`
	Action string  `json:"action"`
	Gain   float64 `json:"gain,omitempty"`
}

// sendControl writes one playback control frame. Ordered with the audio frames
// by writeMu, so the client sees it after every frame sent before it.
func (m *MediaIO) sendControl(action string, gain float64) {
	data, _ := json.Marshal(playbackControl{Type: "playback", Action: action, Gain: gain})
	if err := m.write(websocket.TextMessage, data); err != nil && m.log != nil {
		m.log.Debug("wspcm: playback control not sent", "action", action, "err", err)
	}
}

// StopOutgoing discards queued audio here and, with send-ahead, in the client.
// A duck lasts one utterance, as in the pacer, so the client's gain is reset
// too; otherwise every later reply would play at the ducked level.
func (m *MediaIO) StopOutgoing() {
	m.Outgoing.StopOutgoing()
	if m.sendAhead > 0 {
		m.sendControl("flush", 0)
		m.sendControl("gain", 1)
	}
}

// PauseOutgoing freezes playback here and, with send-ahead, in the client.
func (m *MediaIO) PauseOutgoing() {
	m.Outgoing.PauseOutgoing()
	if m.sendAhead > 0 {
		m.sendControl("pause", 0)
	}
}

// ResumeOutgoing releases a pause on both ends.
func (m *MediaIO) ResumeOutgoing() {
	if m.sendAhead > 0 {
		m.sendControl("resume", 0)
	}
	m.Outgoing.ResumeOutgoing()
}

// DuckOutgoing attenuates playback. With send-ahead the client applies the gain
// at its output, which also covers the audio it already holds; attenuating the
// frames here as well would apply the gain twice.
func (m *MediaIO) DuckOutgoing(gain float64) {
	if m.sendAhead > 0 {
		m.sendControl("gain", gain)
		return
	}
	m.Outgoing.DuckOutgoing(gain)
}

// RestoreOutgoing returns playback to unity gain.
func (m *MediaIO) RestoreOutgoing() {
	if m.sendAhead > 0 {
		m.sendControl("gain", 1)
		return
	}
	m.Outgoing.RestoreOutgoing()
}

func (m *MediaIO) write(typ int, data []byte) error {
	if m.closed.Load() || m.ws == nil {
		return errors.New("wspcm: closed")
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	_ = m.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	return m.ws.WriteMessage(typ, data)
}

// SendFrame implements pacer.Sink: encode one PCM frame as a WS binary message.
// Called only from the pacer send goroutine.
func (m *MediaIO) SendFrame(samples []int16) error {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	return m.write(websocket.BinaryMessage, buf)
}

// Kind implements transport.MediaIO.
func (m *MediaIO) Kind() transport.Kind { return transport.KindWSPCM }

// IncomingPCM implements transport.MediaIO — mic PCM decoded from the WS.
func (m *MediaIO) IncomingPCM() <-chan []int16 { return m.incoming }

// Done is closed when the leg is torn down (WS dropped or Close). Callers watch
// it to end the associated call.
func (m *MediaIO) Done() <-chan struct{} { return m.done }

// Close implements transport.MediaIO. Idempotent.
// Neither incoming nor the pacer OutgoingPCM channel is closed here: producers
// write via Call.WriteOutgoing, and the incoming pump may still be sending.
// Closing either under them races into a send-on-closed panic. Outgoing.Close
// stops the send loop without closing its channel; runIncomingPump closes
// incoming on exit.
func (m *MediaIO) Close() error {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		m.Outgoing.Close()
		if m.ws != nil {
			_ = m.ws.Close()
		}
	})
	return nil
}

func (m *MediaIO) closeIncoming() {
	m.incomingOnce.Do(func() { close(m.incoming) })
}

// runIncomingPump reads binary WS frames (LE int16 PCM), splits them into 20 ms
// frames, and pushes them onto IncomingPCM. Exits on WS error / Close.
func (m *MediaIO) runIncomingPump() {
	defer m.closeIncoming()
	var residual []int16
	for {
		if m.closed.Load() {
			return
		}
		typ, data, err := m.ws.ReadMessage()
		if err != nil {
			_ = m.Close() // WS dropped → tear down the leg (fires Done)
			return
		}
		if typ != websocket.BinaryMessage || len(data) < 2 {
			continue // text = control (reserved); skip odd/empty
		}
		n := len(data) / 2
		for i := 0; i < n; i++ {
			residual = append(residual, int16(binary.LittleEndian.Uint16(data[i*2:])))
		}
		for len(residual) >= frameSamples {
			frame := append([]int16(nil), residual[:frameSamples]...)
			residual = residual[frameSamples:]
			select {
			case <-m.done:
				return
			case m.incoming <- frame:
			default:
				// Slow consumer: drop to avoid unbounded growth.
			}
		}
	}
}
