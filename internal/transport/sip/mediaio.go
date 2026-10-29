package sip

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/codec"
	flowrtp "github.com/DeepFlowAI/flowkit/internal/media/rtp"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	prtp "github.com/pion/rtp"
)

// SamplesPerFrame is 20 ms × 8 kHz = 160 G.711 bytes per RTP packet.
const samplesPerPCMUFrame = 160
const rtpTSIncrement = 160 // 8 kHz × 20 ms
const rtpFrameInterval = 20 * time.Millisecond

// MediaIO is the transport.MediaIO impl for the SIP transport. It owns:
//
//   - one UDP packet conn (the local RTP socket from UA.AllocRTPPort)
//   - a flowrtp.Session for outbound seq/ts management
//   - a JitterBuffer for inbound reorder tolerance
//   - an inbound pump that bridges RTP frames to IncomingPCM
//   - optional RFC 4733 DTMF receive/send when dtmfPayloadType != 0
//
// Embedding pacer.Outgoing satisfies OutgoingPCM plus the paced-playback
// half of transport.MediaIO; DTMF is implemented on this type. Encoding
// onto RTP and the inbound pump live here.
//
// Concurrency: IncomingPCM consumed by one goroutine; OutgoingPCM written
// by one goroutine. Close is idempotent and safe from any goroutine.
// Audio SendFrame (pacer goroutine) and SendDTMF (caller goroutine) may
// run concurrently; rtpWriteMu serializes Pack+WriteTo so wire order
// matches sequence assignment. remoteAddr is an atomic.Pointer so symmetric
// RTP latching from the receive goroutine is safe with senders.
type MediaIO struct {
	pacer.Outgoing

	log *slog.Logger

	conn net.PacketConn
	// remoteAddr is where outbound RTP is sent. It starts at the SDP c=/m=
	// address, then latches to the source of the first inbound RTP packet
	// (symmetric RTP), so a NAT'd peer that advertised a private address in SDP
	// still receives audio. Read by send paths, latched by the receive
	// goroutine — hence atomic.
	remoteAddr  atomic.Pointer[net.UDPAddr]
	latched     atomic.Bool
	configMu    sync.RWMutex
	payloadType uint8
	dtmfPT      uint8
	downsampler *codec.Downsampler16To8

	session *flowrtp.Session
	jitter  *flowrtp.JitterBuffer

	incoming chan []int16

	// rtpWriteMu covers session.Pack / PackTelephoneEvent + conn.WriteTo as
	// one critical section. Session serializes seq/ts assignment, but without
	// this lock two goroutines can Pack in order N,N+1 and WriteTo in reverse
	// — far-end jitter buffers then reorder/drop. SIP-only: DTMF send races
	// the pacer send goroutine on the same RTP stream.
	rtpWriteMu sync.Mutex

	dtmfMu      sync.Mutex
	dtmfHandler transport.DTMFHandler
	dtmfSendMu  sync.Mutex
	dtmfLastSeq uint16

	closeOnce sync.Once
	closed    atomic.Bool
	done      chan struct{}

	// idleTimeoutNS closes the incoming path when no RTP is received for this
	// long. Zero disables the check during provisional media. idleResetNS lets
	// the final answer start a fresh timer instead of inheriting ringing time.
	idleTimeoutNS atomic.Int64
	idleResetNS   atomic.Int64

	// ownMediaIP is our advertised public media IP (SIP_EXT_IP). Latch sources
	// equal to this are ignored — cloud EIP hairpin can deliver a local packet
	// that appears to come from the public IP; latching to it sends audio at
	// ourselves and the far end goes silent.
	ownMediaIP net.IP

	// foreignLogged keeps the unnegotiated-payload-type notice to one line
	// per call; the packets that trigger it usually arrive on a timer.
	foreignLogged atomic.Bool

	// foreignPackets counts every packet dropped for an unnegotiated payload
	// type, which the log line deliberately does not. One line says a foreign
	// sender exists; the count says whether it displaced a moment of speech or
	// a whole conversation.
	foreignPackets atomic.Uint64

	// idleTimedOut is set when the inbound RTP idle timer fires. Call hangup
	// needs this to tell "far-end media went silent" apart from a plain leg drop.
	idleTimedOut atomic.Bool
}

// NewMediaIO wraps an already-bound local UDP socket and the resolved remote
// address. payloadType is RTP PT 0 (PCMU) or 8 (PCMA). dtmfPayloadType is
// the RFC 4733 telephone-event PT negotiated in SDP (0 disables DTMF).
// ownMediaIP is the host's advertised public media IP (empty disables the
// self-latch guard).
func NewMediaIO(conn net.PacketConn, remote string, payloadType, dtmfPayloadType uint8, log *slog.Logger, idleTimeout time.Duration, ownMediaIP string) (*MediaIO, error) {
	return newMediaIO(conn, remote, payloadType, dtmfPayloadType, log, idleTimeout, ownMediaIP, true)
}

// NewMediaIOForEarlyMedia starts consuming the already-bound RTP socket
// without draining packets that may have arrived immediately after an 18x SDP.
func NewMediaIOForEarlyMedia(conn net.PacketConn, remote string, payloadType, dtmfPayloadType uint8, log *slog.Logger, idleTimeout time.Duration, ownMediaIP string) (*MediaIO, error) {
	return newMediaIO(conn, remote, payloadType, dtmfPayloadType, log, idleTimeout, ownMediaIP, false)
}

func newMediaIO(conn net.PacketConn, remote string, payloadType, dtmfPayloadType uint8, log *slog.Logger, idleTimeout time.Duration, ownMediaIP string, drainBuffered bool) (*MediaIO, error) {
	if payloadType != flowrtp.PayloadTypePCMU && payloadType != flowrtp.PayloadTypePCMA {
		return nil, errors.New("sip mediaio: unsupported payload type")
	}
	if conn == nil {
		return nil, errors.New("sip mediaio: nil conn")
	}
	addr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	if idleTimeout <= 0 {
		idleTimeout = rtpIdleTimeout()
	}

	m := &MediaIO{
		log:         log.With("component", "sip-mediaio", "remote", remote, "pt", payloadType, "dtmf_pt", dtmfPayloadType),
		conn:        conn,
		payloadType: payloadType,
		dtmfPT:      dtmfPayloadType,
		downsampler: codec.NewDownsampler16To8(),
		session:     flowrtp.NewSession(payloadType, rtpTSIncrement),
		jitter:      flowrtp.NewJitterBuffer(3),
		incoming:    make(chan []int16, 16),
		done:        make(chan struct{}),
		ownMediaIP:  net.ParseIP(ownMediaIP),
	}
	m.idleTimeoutNS.Store(int64(idleTimeout))
	m.idleResetNS.Store(time.Now().UnixNano())
	m.remoteAddr.Store(addr)
	m.Outgoing.Bind(m, pacer.WithQueue(64), pacer.WithIdleKeepalive(idleKeepalive()))
	if drainBuffered {
		if stale := drainStaleRTP(conn); stale > 0 {
			m.log.Info("discarded media that arrived before the call connected",
				"packets", stale, "approx_ms", stale*20)
		}
	}
	go m.runIncoming()
	return m, nil
}

// SetIdleTimeout changes the RTP idle policy and starts a fresh measurement
// window. A zero duration disables the timeout while the INVITE is unanswered.
func (m *MediaIO) SetIdleTimeout(timeout time.Duration) {
	if timeout < 0 {
		timeout = 0
	}
	m.idleResetNS.Store(time.Now().UnixNano())
	m.idleTimeoutNS.Store(int64(timeout))
}

// Reconfigure applies a later SDP answer to the same media owner. Provisional
// and final streams may change address, codec payload type, DTMF type, SSRC,
// sequence number, and timestamp; resetting the receive and send stream state
// prevents the final stream from being treated as stale early-media packets.
func (m *MediaIO) Reconfigure(remote string, payloadType, dtmfPayloadType uint8) error {
	if payloadType != flowrtp.PayloadTypePCMU && payloadType != flowrtp.PayloadTypePCMA {
		return errors.New("sip mediaio: unsupported payload type")
	}
	addr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		return err
	}
	if m.closed.Load() {
		return errors.New("sip mediaio: closed")
	}

	m.rtpWriteMu.Lock()
	m.configMu.Lock()
	oldRemote := m.remoteAddr.Load()
	m.remoteAddr.Store(addr)
	m.latched.Store(false)
	// Keep sending on the same RTP stream across the answer. A fresh SSRC with
	// a jumped sequence/timestamp makes some carrier gateways resynchronise:
	// they drop the first seconds of the new stream and then play the backlog
	// fast, so the caller hears only a rushed tail of the greeting.
	if payloadType != m.payloadType {
		m.session = flowrtp.NewSession(payloadType, rtpTSIncrement)
	}
	m.payloadType = payloadType
	m.dtmfPT = dtmfPayloadType
	m.jitter = flowrtp.NewJitterBuffer(3)
	m.foreignLogged.Store(false)
	m.configMu.Unlock()
	m.rtpWriteMu.Unlock()

	m.dtmfMu.Lock()
	m.dtmfLastSeq = 0
	m.dtmfMu.Unlock()
	m.log.Info("media reconfigured after provisional answer",
		"previous_remote", oldRemote,
		"remote", remote,
		"pt", payloadType,
		"dtmf_pt", dtmfPayloadType,
	)
	return nil
}

// drainStaleRTP discards whatever the kernel buffered on this socket while
// nobody was reading it.
//
// The socket is bound early, when the offer is built, but nothing reads it
// until the call is answered. A carrier that plays ringback sends media for
// that whole stretch — ten seconds of it is ordinary — and the kernel keeps
// every packet. The reader would then find hundreds of packets waiting and
// hand them downstream in one burst.
//
// That burst is not audio anyone should hear: it is the ringback, already
// minutes stale by playback standards. Worse, playback is paced at one frame
// per 20 ms, so a queue that arrives all at once never drains — whatever
// depth the burst creates becomes permanent added delay for the rest of the
// call, on top of the frames dropped when the buffers overflow.
func drainStaleRTP(conn net.PacketConn) int {
	// Bounded so a peer flooding the socket cannot hold up answering; at one
	// frame per 20 ms this is far more than any plausible ringback.
	const maxDrain = 4000
	buf := make([]byte, 1500)
	dropped := 0
	for dropped < maxDrain {
		// A deadline in the future rather than "now": an elapsed deadline
		// fails the read immediately even when packets are waiting, which
		// would drain nothing at all.
		if err := conn.SetReadDeadline(time.Now().Add(time.Millisecond)); err != nil {
			break
		}
		if _, _, err := conn.ReadFrom(buf); err != nil {
			break
		}
		dropped++
	}
	_ = conn.SetReadDeadline(time.Time{})
	return dropped
}

// SendFrame implements pacer.Sink: downsample, encode G.711, pack RTP, write
// UDP. Called only from the pacer send goroutine; may race SendDTMF.
func (m *MediaIO) SendFrame(samples []int16) error {
	if m.closed.Load() {
		return errors.New("sip mediaio: closed")
	}
	pcm8 := m.downsampler.Process(samples)
	if len(pcm8) < samplesPerPCMUFrame {
		padded := make([]int16, samplesPerPCMUFrame)
		copy(padded, pcm8)
		pcm8 = padded
	} else {
		pcm8 = pcm8[:samplesPerPCMUFrame]
	}
	m.rtpWriteMu.Lock()
	defer m.rtpWriteMu.Unlock()
	if m.closed.Load() {
		return errors.New("sip mediaio: closed")
	}
	m.configMu.RLock()
	payloadType := m.payloadType
	sess := m.session
	m.configMu.RUnlock()
	encoded := encodeG711(payloadType, pcm8)
	wire, err := sess.Pack(encoded)
	if err != nil {
		return err
	}
	_, err = m.conn.WriteTo(wire, m.remoteAddr.Load())
	return err
}

// Kind implements transport.MediaIO.
func (m *MediaIO) Kind() transport.Kind { return transport.KindSIP }

// IncomingPCM implements transport.MediaIO.
func (m *MediaIO) IncomingPCM() <-chan []int16 { return m.incoming }

// SetDTMFHandler implements transport.DTMFReceiver.
func (m *MediaIO) SetDTMFHandler(h transport.DTMFHandler) {
	m.dtmfMu.Lock()
	m.dtmfHandler = h
	m.dtmfMu.Unlock()
}

// SendDTMF implements transport.DTMFSender. Sends RFC 4733 events for each
// character in digits (0-9 * # A-D).
func (m *MediaIO) SendDTMF(digits string, durationMS int) error {
	m.configMu.RLock()
	dtmfPT := m.dtmfPT
	m.configMu.RUnlock()
	if dtmfPT == 0 {
		return errors.New("sip mediaio: dtmf not negotiated")
	}
	if durationMS <= 0 {
		durationMS = 160
	}
	m.dtmfSendMu.Lock()
	defer m.dtmfSendMu.Unlock()
	if m.closed.Load() {
		return errors.New("sip mediaio: closed")
	}
	for _, ch := range digits {
		event, err := flowrtp.DigitToEvent(string(ch))
		if err != nil {
			return err
		}
		if err := m.sendDTMFDigit(event, durationMS, dtmfPT); err != nil {
			return err
		}
	}
	return nil
}

func (m *MediaIO) sendDTMFDigit(event byte, durationMS int, dtmfPT uint8) error {
	const volume = 10
	totalDur := uint16(durationMS * 8) // 8000 Hz clock
	steps := []struct {
		end  bool
		dur  uint16
		wait time.Duration
	}{
		{false, 0, rtpFrameInterval},
		{false, totalDur / 2, rtpFrameInterval},
		{true, totalDur, rtpFrameInterval},
	}
	for _, step := range steps {
		if m.closed.Load() {
			return errors.New("sip mediaio: closed")
		}
		payload := flowrtp.BuildDTMFPayload(event, step.end, volume, step.dur)

		m.rtpWriteMu.Lock()
		m.configMu.RLock()
		sess := m.session
		m.configMu.RUnlock()
		wire, err := sess.PackTelephoneEvent(payload, dtmfPT, step.end)
		if err != nil {
			m.rtpWriteMu.Unlock()
			return err
		}
		_, err = m.conn.WriteTo(wire, m.remoteAddr.Load())
		m.rtpWriteMu.Unlock()
		if err != nil {
			return err
		}
		time.Sleep(step.wait)
	}
	return nil
}

// Close implements transport.MediaIO. Idempotent. Outgoing.Close stops the
// pacer send goroutine (required — goleak will fail if omitted). Neither
// IncomingPCM nor the pacer OutgoingPCM channel is closed under producers.
func (m *MediaIO) Close() error {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		m.Outgoing.Close()
		_ = m.conn.Close()
	})
	return nil
}

// runIncoming reads RTP packets from the UDP socket, depacketizes them,
// passes through the jitter buffer, decodes PCMU, upsamples 8k → 16k, and
// pushes each 320-sample frame to the IncomingPCM channel.
func (m *MediaIO) runIncoming() {
	defer func() {
		// Channel close races with Close — protect with closed flag.
		defer func() { _ = recover() }()
		close(m.incoming)
	}()

	buf := make([]byte, 1500) // typical MTU
	started := time.Now()
	var lastRTP time.Time
	for {
		if m.closed.Load() {
			return
		}
		_ = m.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, src, err := m.conn.ReadFrom(buf)
		if err != nil {
			if isTimeout(err) {
				idleTimeout := time.Duration(m.idleTimeoutNS.Load())
				if idleTimeout > 0 {
					ref := lastRTP
					resetAt := time.Unix(0, m.idleResetNS.Load())
					if ref.IsZero() || resetAt.After(ref) {
						ref = resetAt
					}
					if ref.IsZero() {
						ref = started
					}
					if time.Since(ref) >= idleTimeout {
						m.log.Info("rtp idle timeout", "idle", idleTimeout)
						m.idleTimedOut.Store(true)
						return
					}
				}
				continue
			}
			// Real error (or socket closed). Exit.
			return
		}
		pkt, err := flowrtp.Unpack(buf[:n])
		if err != nil {
			m.log.Debug("rtp unpack failed", "err", err)
			continue
		}
		// Anything whose payload type we did not negotiate is not this call's
		// audio, however well-formed it looks. RTCP reports in particular carry
		// the same version bits and unpack cleanly, and a stray one arriving
		// before the far end's first voice packet would otherwise latch our
		// send address onto whoever sent it — the peer then hears nothing for
		// the whole call. Such packets also must not reach the jitter buffer,
		// where they would be decoded as G.711 and injected as noise, nor count
		// as media for the idle timeout, which exists to notice silence.
		m.configMu.RLock()
		payloadType := m.payloadType
		dtmfPT := m.dtmfPT
		jitter := m.jitter
		if pkt.PayloadType != payloadType && (dtmfPT == 0 || pkt.PayloadType != dtmfPT) {
			m.configMu.RUnlock()
			m.noteForeignPacket(pkt.PayloadType, src, payloadType, dtmfPT)
			continue
		}
		lastRTP = time.Now()
		// Symmetric RTP (comedia): latch the send target to the source of the
		// first usable inbound RTP packet when it differs from the SDP address.
		// Reject sources that equal our own advertised media IP (EIP hairpin)
		// without marking latched, so a later real peer packet can still correct.
		m.maybeLatchRemote(src)
		if dtmfPT != 0 && pkt.PayloadType == dtmfPT {
			m.configMu.RUnlock()
			m.handleDTMFPacket(pkt)
			continue
		}
		// Push through jitter; deliver in-order frames
		for _, p := range jitter.Push(pkt) {
			if len(p.Payload) == 0 {
				continue
			}
			pcm8 := decodeG711(payloadType, p.Payload)
			pcm16 := codec.Upsample8To16(pcm8)
			select {
			case m.incoming <- pcm16:
			case <-m.done:
				return
			default:
				// Buffer full — drop frame for this call (slow consumer).
				m.log.Debug("incoming buffer full, dropping frame")
			}
		}
		m.configMu.RUnlock()
	}
}

// noteForeignPacket counts a packet dropped for an unrecognised payload type,
// and logs the first one. Only the first is logged, because the usual senders
// repeat on a timer and a per-packet line would bury the log; the running count
// reaches the call summary instead, where it can be compared against the
// call's length.
func (m *MediaIO) noteForeignPacket(pt uint8, src net.Addr, payloadType, dtmfPT uint8) {
	m.foreignPackets.Add(1)
	if m.foreignLogged.Swap(true) {
		return
	}
	m.log.Info("dropping packet with unnegotiated payload type",
		"pt", pt, "want_pt", payloadType, "dtmf_pt", dtmfPT, "src", src.String())
}

// ForeignPackets reports how many packets this call dropped for a payload type
// it never negotiated. Read by the call summary: a carrier that keeps sending
// its ringback codec after the answer silences the first seconds of speech,
// and this is what distinguishes that from a caller who simply said nothing.
func (m *MediaIO) ForeignPackets() uint64 { return m.foreignPackets.Load() }

// MediaEOFReason reports why the incoming media path ended, when the
// transport knows something more specific than "the channel closed".
func (m *MediaIO) MediaEOFReason() string {
	if m.idleTimedOut.Load() {
		return "media_timeout"
	}
	return ""
}

// maybeLatchRemote applies symmetric-RTP latching for one inbound packet source.
func (m *MediaIO) maybeLatchRemote(src net.Addr) {
	if m.latched.Load() {
		return
	}
	ua, ok := src.(*net.UDPAddr)
	if !ok || ua == nil || ua.IP == nil {
		return
	}
	prev := m.remoteAddr.Load()
	if prev == nil {
		return
	}
	if prev.IP.Equal(ua.IP) && prev.Port == ua.Port {
		m.latched.CompareAndSwap(false, true)
		return
	}
	if m.ownMediaIP != nil && ua.IP.Equal(m.ownMediaIP) {
		m.log.Info("rtp latch ignored (own media IP)",
			"sdp", prev.String(), "src", ua.String())
		return
	}
	if !m.latched.CompareAndSwap(false, true) {
		return
	}
	m.remoteAddr.Store(ua)
	m.log.Info("rtp latched to source", "sdp", prev.String(), "src", ua.String())
}

func (m *MediaIO) handleDTMFPacket(pkt *prtp.Packet) {
	digit, ok := flowrtp.ParseDTMFEnd(pkt.Payload)
	if !ok {
		return
	}
	m.dtmfMu.Lock()
	if pkt.SequenceNumber == m.dtmfLastSeq {
		m.dtmfMu.Unlock()
		return
	}
	m.dtmfLastSeq = pkt.SequenceNumber
	handler := m.dtmfHandler
	m.dtmfMu.Unlock()
	if handler != nil {
		handler(digit)
	}
}

func isTimeout(err error) bool {
	type timeoutError interface{ Timeout() bool }
	if te, ok := err.(timeoutError); ok && te.Timeout() {
		return true
	}
	return false
}

func decodeG711(payloadType uint8, payload []byte) []int16 {
	if payloadType == flowrtp.PayloadTypePCMA {
		return codec.DecodePCMA(payload)
	}
	return codec.DecodePCMU(payload)
}

func encodeG711(payloadType uint8, samples []int16) []byte {
	if payloadType == flowrtp.PayloadTypePCMA {
		return codec.EncodePCMA(samples)
	}
	return codec.EncodePCMU(samples)
}
