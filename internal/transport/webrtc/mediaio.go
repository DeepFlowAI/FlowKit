package webrtc

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/codec"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// pcmMediaIO is the transport.MediaIO adapter exposed by Peer.MediaIO().
// Embedding pacer.Outgoing satisfies OutgoingPCM plus the paced-playback
// half of transport.MediaIO (no DTMF). This type only encodes frames onto
// the local track and runs the inbound pump.
type pcmMediaIO struct {
	pacer.Outgoing

	peer        *Peer
	localTrack  *webrtc.TrackLocalStaticSample
	downsampler *codec.Downsampler16To8

	incoming chan []int16

	mu                  sync.Mutex
	remoteTrack         *webrtc.TrackRemote
	incomingPumpStarted bool

	closeOnce    sync.Once
	incomingOnce sync.Once
	closed       atomic.Bool
	done         chan struct{}
}

func newPCMMediaIO(p *Peer, localTrack *webrtc.TrackLocalStaticSample, inBuf, outBuf int) *pcmMediaIO {
	m := &pcmMediaIO{
		peer:        p,
		localTrack:  localTrack,
		downsampler: codec.NewDownsampler16To8(),
		incoming:    make(chan []int16, inBuf),
		done:        make(chan struct{}),
	}
	m.Outgoing.Bind(m, pacer.WithQueue(outBuf))
	return m
}

// SendFrame implements pacer.Sink: downsample, encode PCMU, write to the local
// track. Called only from the pacer send goroutine.
func (m *pcmMediaIO) SendFrame(samples []int16) error {
	if m.closed.Load() || m.localTrack == nil {
		return errors.New("webrtc: closed")
	}
	const frameDur = 20 * time.Millisecond
	pcm8 := m.downsampler.Process(samples)
	encoded := codec.EncodePCMU(pcm8)
	return m.localTrack.WriteSample(media.Sample{Data: encoded, Duration: frameDur})
}

// Kind implements transport.MediaIO.
func (m *pcmMediaIO) Kind() transport.Kind { return transport.KindWebRTC }

// IncomingPCM implements transport.MediaIO.
func (m *pcmMediaIO) IncomingPCM() <-chan []int16 { return m.incoming }

// Close implements transport.MediaIO. Signals shutdown via done. Neither
// IncomingPCM nor the pacer OutgoingPCM channel is closed under producers —
// that races into a send-on-closed panic. Outgoing.Close stops the send loop
// without closing its channel; runIncomingPump closes incoming on exit. If the
// incoming pump never started, Close closes incoming so range consumers unblock.
func (m *pcmMediaIO) Close() error {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		m.Outgoing.Close()
		m.mu.Lock()
		started := m.incomingPumpStarted
		m.mu.Unlock()
		if !started {
			m.closeIncoming()
		}
	})
	return nil
}

func (m *pcmMediaIO) closeIncoming() {
	m.incomingOnce.Do(func() { close(m.incoming) })
}

// attachRemote is called by Peer.OnTrack when the remote audio track arrives.
// Spawns a PCMU → PCM-16k pump goroutine that fills IncomingPCM until the
// track ends or the MediaIO is closed.
func (m *pcmMediaIO) attachRemote(t *webrtc.TrackRemote) {
	m.mu.Lock()
	m.remoteTrack = t
	already := m.incomingPumpStarted
	m.incomingPumpStarted = true
	m.mu.Unlock()
	if already {
		return
	}
	go m.runIncomingPump(t)
}

// runIncomingPump reads RTP packets from the remote track, decodes PCMU to
// PCM 8k, upsamples to PCM 16k, and pushes 20 ms frames onto IncomingPCM.
// Exits when the track ends or the MediaIO is closed.
func (m *pcmMediaIO) runIncomingPump(t *webrtc.TrackRemote) {
	defer m.closeIncoming()
	log := m.peer.log.With("pump", "incoming")
	for {
		if m.closed.Load() {
			return
		}
		pkt, _, err := t.ReadRTP()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug("ReadRTP ended", "err", err)
			}
			return
		}
		if len(pkt.Payload) == 0 {
			continue
		}
		pcm8 := codec.DecodePCMU(pkt.Payload)
		pcm16 := codec.Upsample8To16(pcm8)
		select {
		case <-m.done:
			return
		case m.incoming <- pcm16:
		default:
			// IncomingPCM buffer full — drop frame so we don't stall the
			// RTP reader. Subscribers can detect this via the AudioBus
			// drop counter once the frame reaches the per-call bus.
			log.Debug("incoming pcm buffer full — dropping frame")
		}
	}
}

// RemoteTrack exposes the remote track for testing / metrics. Returns nil
// until OnTrack has fired.
func (m *pcmMediaIO) RemoteTrack() *webrtc.TrackRemote {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remoteTrack
}
