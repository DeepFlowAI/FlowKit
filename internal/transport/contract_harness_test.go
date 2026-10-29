package transport_test

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/rtp"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	siptr "github.com/DeepFlowAI/flowkit/internal/transport/sip"
	webrtctr "github.com/DeepFlowAI/flowkit/internal/transport/webrtc"
	"github.com/DeepFlowAI/flowkit/internal/transport/wspcm"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const (
	frameSamples = 320 // 20 ms @ 16 kHz
	frameDur     = 20 * time.Millisecond
)

// timedFrame is one paced outgoing frame observed on the wire (or SentTap).
type timedFrame struct {
	at      time.Time
	samples []int16
}

// mediaHarness binds a live MediaIO under test to a wire-side observer.
type mediaHarness struct {
	name string
	mio  transport.MediaIO
	// outCap is the OutgoingPCM channel buffer size used by this harness.
	outCap int
	// remoteMio is the peer leg that can send into mio.IncomingPCM (pair harnesses).
	remoteMio transport.MediaIO
	// observe collects up to n frames that were actually sent, with arrival times.
	observe func(n int, timeout time.Duration) []timedFrame
	// drainDiscard consumes any leftover observed frames without asserting.
	drainDiscard func()
	cleanup      func()
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func pcmFrame(marker int16) []int16 {
	out := make([]int16, frameSamples)
	out[0] = marker
	for i := 1; i < len(out); i++ {
		out[i] = marker / 2
	}
	return out
}

func copyFrame(src []int16) []int16 {
	dst := make([]int16, len(src))
	copy(dst, src)
	return dst
}

// --- SIP ----------------------------------------------------------------

// newSIPHarnessBare builds a single MediaIO (no peer observer) for Close tests.
func newSIPHarnessBare(t *testing.T) *mediaHarness {
	t.Helper()
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	a, err := siptr.NewMediaIO(pcA, pcB.LocalAddr().String(), rtp.PayloadTypePCMU, 0, quietLogger(), time.Hour, "")
	require.NoError(t, err)
	return &mediaHarness{
		name:    "sip",
		mio:     a,
		outCap:  64, // sip.NewMediaIO outgoing buffer
		cleanup: func() { _ = a.Close(); _ = pcB.Close() },
	}
}

// newSIPHarness builds a UDP peer pair; observe via peer IncomingPCM.
func newSIPHarness(t *testing.T) *mediaHarness {
	t.Helper()
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	a, err := siptr.NewMediaIO(pcA, pcB.LocalAddr().String(), rtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	b, err := siptr.NewMediaIO(pcB, pcA.LocalAddr().String(), rtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)

	return &mediaHarness{
		name:      "sip",
		mio:       a,
		outCap:    64, // sip.NewMediaIO outgoing buffer
		remoteMio: b,
		observe: func(n int, timeout time.Duration) []timedFrame {
			return collectIncoming(b.IncomingPCM(), n, timeout)
		},
		drainDiscard: func() { drainChan(b.IncomingPCM(), 50*time.Millisecond) },
		cleanup: func() {
			_ = a.Close()
			_ = b.Close()
		},
	}
}

// --- WebRTC -------------------------------------------------------------

// newWebRTCHarnessBare builds an unconnected Peer MediaIO for Close tests that
// require IncomingPCM to close. The connected-peer Close defect is covered by
// TestContract_ConnectedWebRTCCloseDoesNotCloseIncomingPCM.
func newWebRTCHarnessBare(t *testing.T) *mediaHarness {
	t.Helper()
	peer, err := webrtctr.New(webrtctr.Config{
		Logger:         quietLogger(),
		OutgoingBuffer: 64,
	})
	require.NoError(t, err)
	return &mediaHarness{
		name:   "webrtc",
		mio:    peer.MediaIO(),
		outCap: 64, // webrtctr.Config.OutgoingBuffer (Peer default is 10)
		cleanup: func() {
			_ = peer.Close()
		},
	}
}

// newWebRTCHarness builds a connected peer pair; observe via remote IncomingPCM.
func newWebRTCHarness(t *testing.T) *mediaHarness {
	t.Helper()
	const outCap = 64
	peerA, err := webrtctr.New(webrtctr.Config{
		Logger:           quietLogger(),
		ICEGatherMaxWait: 5 * time.Second,
		OutgoingBuffer:   outCap,
	})
	require.NoError(t, err)
	peerB, err := webrtctr.New(webrtctr.Config{
		Logger:           quietLogger(),
		ICEGatherMaxWait: 5 * time.Second,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	peerA.OnConnected(func() { wg.Done() })
	peerB.OnConnected(func() { wg.Done() })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	offer, err := peerA.CreateOffer(ctx)
	require.NoError(t, err)
	answer, err := peerB.HandleOffer(ctx, offer)
	require.NoError(t, err)
	require.NoError(t, peerA.SetRemoteAnswer(ctx, answer))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = peerA.Close()
		_ = peerB.Close()
		t.Fatal("webrtc peers did not connect")
	}

	mio := peerA.MediaIO()
	return &mediaHarness{
		name:      "webrtc",
		mio:       mio,
		outCap:    outCap,
		remoteMio: peerB.MediaIO(),
		observe: func(n int, timeout time.Duration) []timedFrame {
			return collectIncoming(peerB.MediaIO().IncomingPCM(), n, timeout)
		},
		drainDiscard: func() { drainChan(peerB.MediaIO().IncomingPCM(), 50*time.Millisecond) },
		cleanup: func() {
			_ = peerA.Close()
			_ = peerB.Close()
		},
	}
}

// --- WS-PCM: local websocket; observe via client binary frames ----------

func newWSPCMHarness(t *testing.T) *mediaHarness {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	mioCh := make(chan *wspcm.MediaIO, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mio := wspcm.New(ws, 16, 64, quietLogger()) // outBuf=64
		mioCh <- mio
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	var mio *wspcm.MediaIO
	select {
	case mio = <-mioCh:
	case <-time.After(2 * time.Second):
		_ = client.Close()
		srv.Close()
		t.Fatal("wspcm server MediaIO not ready")
	}

	var mu sync.Mutex
	var observed []timedFrame
	var closed atomicBool
	obsDone := make(chan struct{})
	go func() {
		defer close(obsDone)
		for {
			_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
			typ, data, err := client.ReadMessage()
			if err != nil {
				return
			}
			if closed.get() {
				return
			}
			if typ != websocket.BinaryMessage || len(data) < 2 {
				continue
			}
			n := len(data) / 2
			samples := make([]int16, n)
			for i := 0; i < n; i++ {
				samples[i] = int16(binary.LittleEndian.Uint16(data[i*2:]))
			}
			// WS-PCM writes one 20 ms frame per paced tick.
			for len(samples) >= frameSamples {
				frame := copyFrame(samples[:frameSamples])
				samples = samples[frameSamples:]
				mu.Lock()
				observed = append(observed, timedFrame{at: time.Now(), samples: frame})
				mu.Unlock()
			}
		}
	}()

	return &mediaHarness{
		name:   "wspcm",
		mio:    mio,
		outCap: 64, // wspcm.New outBuf argument above
		observe: func(n int, timeout time.Duration) []timedFrame {
			deadline := time.Now().Add(timeout)
			for {
				mu.Lock()
				if len(observed) >= n {
					out := append([]timedFrame(nil), observed[:n]...)
					observed = observed[n:]
					mu.Unlock()
					return out
				}
				mu.Unlock()
				if time.Now().After(deadline) {
					mu.Lock()
					out := append([]timedFrame(nil), observed...)
					observed = nil
					mu.Unlock()
					return out
				}
				time.Sleep(2 * time.Millisecond)
			}
		},
		drainDiscard: func() {
			mu.Lock()
			observed = nil
			mu.Unlock()
		},
		cleanup: func() {
			closed.set(true)
			_ = mio.Close()
			_ = client.Close()
			srv.Close()
			<-obsDone
		},
	}
}

type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomicBool) set(v bool) { b.mu.Lock(); b.v = v; b.mu.Unlock() }
func (b *atomicBool) get() bool  { b.mu.Lock(); defer b.mu.Unlock(); return b.v }

func collectIncoming(ch <-chan []int16, n int, timeout time.Duration) []timedFrame {
	out := make([]timedFrame, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case frame, ok := <-ch:
			if !ok {
				return out
			}
			if len(frame) == 0 {
				continue
			}
			out = append(out, timedFrame{at: time.Now(), samples: copyFrame(frame)})
		case <-deadline:
			return out
		}
	}
	return out
}

func drainChan(ch <-chan []int16, d time.Duration) {
	deadline := time.After(d)
	for {
		select {
		case <-ch:
		case <-deadline:
			return
		}
	}
}

func writeFrames(t *testing.T, mio transport.MediaIO, frames [][]int16) {
	t.Helper()
	for i, f := range frames {
		select {
		case mio.OutgoingPCM() <- f:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout enqueueing frame %d", i)
		}
	}
}

// tryWriteFrames enqueues frames with a per-frame timeout. Returns how many
// were accepted before a send blocked past perFrameTimeout.
func tryWriteFrames(mio transport.MediaIO, frames [][]int16, perFrameTimeout time.Duration) int {
	for i, f := range frames {
		select {
		case mio.OutgoingPCM() <- f:
		case <-time.After(perFrameTimeout):
			return i
		}
	}
	return len(frames)
}

func span(frames []timedFrame) time.Duration {
	if len(frames) < 2 {
		return 0
	}
	return frames[len(frames)-1].at.Sub(frames[0].at)
}

func countSamples(frames []timedFrame) int {
	n := 0
	for _, f := range frames {
		n += len(f.samples)
	}
	return n
}

func waitPlaybackIdleIfSupported(t *testing.T, mio transport.MediaIO, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	require.NoError(t, mio.WaitPlaybackIdle(ctx))
}
