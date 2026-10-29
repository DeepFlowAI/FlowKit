// Package webrtc adapts Pion v4 RTCPeerConnection to flowkit's
// transport.MediaIO. M2 task ② establishes the SDP / ICE lifecycle; the
// Opus ↔ PCM-16k bridge is layered on in task ③ (codec.go).
package webrtc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

// DefaultICEGatherMaxWait is the short embed window when NAT1To1IPs is set.
const DefaultICEGatherMaxWait = 300 * time.Millisecond

// Config controls per-Peer parameters.
type Config struct {
	// ICEServers is the standard ICE configuration. Empty means "no STUN /
	// TURN" — works only on loopback or fully open networks.
	ICEServers []string

	// NAT1To1IPs rewrites host ICE candidates for public reachability (Docker/NAT).
	NAT1To1IPs []string

	// Logger receives structured log records; nil uses slog.Default().
	Logger *slog.Logger

	// ICEGatherMaxWait bounds how long HandleOffer/CreateOffer block waiting
	// for ICE gathering before returning SDP. Zero = trickle mode (return
	// immediately; candidates follow via webrtc.ice). Tests may set a larger
	// value to embed all candidates in SDP without trickle exchange.
	ICEGatherMaxWait time.Duration

	// the MediaIO adapter. Zero falls back to 10 (≈200 ms at 20 ms frames).
	IncomingBuffer int
	OutgoingBuffer int
}

// Peer wraps a Pion RTCPeerConnection plus the inbound / outbound audio
// tracks. It owns one goroutine per direction (started in task ③).
//
// Concurrency: HandleOffer, AddICECandidate, OnLocalICECandidate, and Close
// may all be called from different goroutines. The internal state is
// protected by mu.
type Peer struct {
	cfg Config
	log *slog.Logger

	pc *webrtc.PeerConnection

	mu             sync.Mutex
	localTrack     *webrtc.TrackLocalStaticSample
	remoteTrack    *webrtc.TrackRemote
	onLocalICECB   func(candidate webrtc.ICECandidateInit)
	onConnectedCB  func()
	closeOnce      sync.Once
	closed         atomic.Bool
	mediaIO        *pcmMediaIO
	connectionDone chan struct{}
}

// New constructs a Peer with an audio-only sendrecv transceiver. Call
// HandleOffer or CreateOffer (future) to drive signalling.
func New(cfg Config) (*Peer, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.IncomingBuffer <= 0 {
		cfg.IncomingBuffer = 10
	}
	if cfg.OutgoingBuffer <= 0 {
		cfg.OutgoingBuffer = 10
	}

	pcCfg := webrtc.Configuration{}
	for _, s := range cfg.ICEServers {
		pcCfg.ICEServers = append(pcCfg.ICEServers, webrtc.ICEServer{URLs: []string{s}})
	}

	se := webrtc.SettingEngine{}
	if len(cfg.NAT1To1IPs) > 0 {
		se.SetNAT1To1IPs(cfg.NAT1To1IPs, webrtc.ICECandidateTypeHost)
	}

	// Pin WebRTC media to the same UDP range as SIP RTP (16384–32767) so a
	// single firewall / security-group rule covers both. Without this, pion
	// binds OS ephemeral ports (Linux 32768–60999) that fall outside the
	// opened range, so browser ICE checks never reach the kernel and the
	// agent leg has no audio.
	if err := se.SetEphemeralUDPPortRange(16384, 32767); err != nil {
		return nil, fmt.Errorf("webrtc: set udp port range: %w", err)
	}

	// Custom MediaEngine that registers ONLY PCMU. The default media engine
	// includes Opus, which the browser then picks for outbound audio — that
	// breaks the incoming pump because it decodes payloads as PCMU.
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000, Channels: 1},
		PayloadType:        0,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("webrtc: register PCMU: %w", err)
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, fmt.Errorf("webrtc: register interceptors: %w", err)
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(ir),
		webrtc.WithSettingEngine(se),
	)

	pc, err := api.NewPeerConnection(pcCfg)
	if err != nil {
		return nil, fmt.Errorf("webrtc: new peer connection: %w", err)
	}

	// Outgoing audio track (we send TTS audio to the browser).
	// G.711 PCMU 8 kHz mono.
	localTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000, Channels: 1},
		"audio", "flowkit",
	)
	if err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("webrtc: new local track: %w", err)
	}
	if _, err := pc.AddTrack(localTrack); err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("webrtc: add track: %w", err)
	}

	p := &Peer{
		cfg:            cfg,
		log:            cfg.Logger,
		pc:             pc,
		localTrack:     localTrack,
		connectionDone: make(chan struct{}),
	}
	p.mediaIO = newPCMMediaIO(p, localTrack, cfg.IncomingBuffer, cfg.OutgoingBuffer)
	p.wireCallbacks()

	return p, nil
}

// wireCallbacks installs Pion event handlers that fan out to user-supplied
// callbacks (OnLocalICECandidate / OnConnected) and into the MediaIO pumps.
func (p *Peer) wireCallbacks() {
	p.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return // ICE-gathering complete; some signalling layers pass null
		}
		p.mu.Lock()
		cb := p.onLocalICECB
		p.mu.Unlock()
		if cb != nil {
			cb(c.ToJSON())
		}
	})

	p.pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		p.log.Debug("peer connection state", "state", s.String())
		switch s {
		case webrtc.PeerConnectionStateConnected:
			p.mu.Lock()
			cb := p.onConnectedCB
			p.mu.Unlock()
			if cb != nil {
				cb()
			}
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateClosed,
			webrtc.PeerConnectionStateDisconnected:
			// Signal the MediaIO pumps to wind down (they may already be done).
			_ = p.Close()
		}
	})

	p.pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		p.mu.Lock()
		p.remoteTrack = t
		p.mu.Unlock()
		// Task ③ starts the Opus → PCM 16 k pump here.
		p.mediaIO.attachRemote(t)
	})
}

// OnLocalICECandidate registers a callback invoked for each ICE candidate
// produced locally (so the signalling layer can forward it to the peer).
func (p *Peer) OnLocalICECandidate(cb func(c webrtc.ICECandidateInit)) {
	p.mu.Lock()
	p.onLocalICECB = cb
	p.mu.Unlock()
}

// OnConnected registers a callback invoked once when the peer reaches the
// Connected state.
func (p *Peer) OnConnected(cb func()) {
	p.mu.Lock()
	p.onConnectedCB = cb
	p.mu.Unlock()
}

// HandleOffer processes an inbound SDP offer and returns the local answer.
// Use for browser-initiated calls (which M2 standardises on).
func (p *Peer) HandleOffer(_ context.Context, offerSDP string) (string, error) {
	if p.closed.Load() {
		return "", errors.New("webrtc: peer closed")
	}
	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}
	if err := p.pc.SetRemoteDescription(offer); err != nil {
		return "", fmt.Errorf("webrtc: set remote desc: %w", err)
	}
	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("webrtc: create answer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(p.pc)
	if err := p.pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("webrtc: set local desc: %w", err)
	}
	if p.cfg.ICEGatherMaxWait > 0 {
		select {
		case <-gatherComplete:
		case <-time.After(p.cfg.ICEGatherMaxWait):
			p.log.Warn("ICE gather timeout — returning partial SDP", "wait", p.cfg.ICEGatherMaxWait)
		}
	}
	return p.pc.LocalDescription().SDP, nil
}

// CreateOffer is the outbound-call side (M2 doesn't need this for the
// browser flow, but SIP-to-browser bridging in M3 will). Returns the local
// offer SDP that the caller forwards to the peer.
func (p *Peer) CreateOffer(_ context.Context) (string, error) {
	if p.closed.Load() {
		return "", errors.New("webrtc: peer closed")
	}
	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("webrtc: create offer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(p.pc)
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("webrtc: set local desc: %w", err)
	}
	if p.cfg.ICEGatherMaxWait > 0 {
		select {
		case <-gatherComplete:
		case <-time.After(p.cfg.ICEGatherMaxWait):
		}
	}
	return p.pc.LocalDescription().SDP, nil
}

// SetRemoteAnswer is the second half of the outbound-call flow.
func (p *Peer) SetRemoteAnswer(_ context.Context, answerSDP string) error {
	if p.closed.Load() {
		return errors.New("webrtc: peer closed")
	}
	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answerSDP}
	return p.pc.SetRemoteDescription(answer)
}

// AddICECandidate adds a remote ICE candidate. Should be called for each
// candidate the remote side trickled to us.
func (p *Peer) AddICECandidate(c webrtc.ICECandidateInit) error {
	if p.closed.Load() {
		return errors.New("webrtc: peer closed")
	}
	return p.pc.AddICECandidate(c)
}

// MediaIO returns the PCM 16 kHz mono adapter — the only place outside this
// package should touch audio.
func (p *Peer) MediaIO() transport.MediaIO { return p.mediaIO }

// LocalTrack exposes the outbound Pion track for testing and metrics.
func (p *Peer) LocalTrack() *webrtc.TrackLocalStaticSample {
	return p.localTrack
}

// Close releases the peer connection and its goroutines. Idempotent.
func (p *Peer) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		close(p.connectionDone)
		if p.mediaIO != nil {
			_ = p.mediaIO.Close()
		}
		if p.pc != nil {
			err = p.pc.Close()
		}
	})
	return err
}
