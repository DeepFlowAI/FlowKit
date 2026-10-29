package sip

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	flowrtp "github.com/DeepFlowAI/flowkit/internal/media/rtp"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	prtp "github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ transport.MediaIO = (*MediaIO)(nil)
var _ transport.DTMFReceiver = (*MediaIO)(nil)
var _ transport.DTMFSender = (*MediaIO)(nil)
var _ pacer.Sink = (*MediaIO)(nil)

// makePair sets up two MediaIOs aimed at each other on loopback.
func makePair(t *testing.T) (*MediaIO, *MediaIO, func()) {
	t.Helper()
	// These tests predate idle keepalive and assert on the audio frames they
	// enqueue; a default-on silence stream would get counted as that audio.
	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	addrA := pcA.LocalAddr().String()
	addrB := pcB.LocalAddr().String()

	a, err := NewMediaIO(pcA, addrB, flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	b, err := NewMediaIO(pcB, addrA, flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)

	cleanup := func() {
		_ = a.Close()
		_ = b.Close()
	}
	return a, b, cleanup
}

func TestMediaIO_NilConn_Errors(t *testing.T) {
	_, err := NewMediaIO(nil, "127.0.0.1:1234", flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.Error(t, err)
}

func TestMediaIO_BadRemote_Errors(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pc.Close()
	_, err = NewMediaIO(pc, "garbage", flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.Error(t, err)
}

func TestMediaIO_RTPIdleTimeoutClosesIncoming(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcA.Close()
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcB.Close()

	mio, err := NewMediaIO(pcA, pcB.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 200*time.Millisecond, "")
	require.NoError(t, err)
	defer mio.Close()

	select {
	case _, ok := <-mio.IncomingPCM():
		require.False(t, ok, "incoming channel should close after rtp idle timeout")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rtp idle timeout")
	}
	require.Equal(t, "media_timeout", mio.MediaEOFReason())
}

func TestMediaIO_CloseDoesNotReportMediaTimeout(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcA.Close()
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcB.Close()

	mio, err := NewMediaIO(pcA, pcB.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), time.Hour, "")
	require.NoError(t, err)
	require.NoError(t, mio.Close())

	select {
	case _, ok := <-mio.IncomingPCM():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("incoming channel should close after Close")
	}
	require.Equal(t, "", mio.MediaEOFReason())
}

func TestMediaIO_Loopback_OutgoingReachesPeerIncoming(t *testing.T) {
	a, b, cleanup := makePair(t)
	defer cleanup()

	// Generate a 20 ms PCM 16k frame (320 samples) of a 1 kHz sine
	samples := make([]int16, 320)
	for i := range samples {
		t := float64(i) / 16000.0
		samples[i] = int16(math.Sin(2*math.Pi*1000*t) * 8000)
	}

	// Send a few frames from A to B
	const N = 3
	for i := 0; i < N; i++ {
		select {
		case a.OutgoingPCM() <- samples:
		case <-time.After(time.Second):
			t.Fatal("could not enqueue outgoing")
		}
	}

	// Read on B's incoming
	received := 0
	deadline := time.After(3 * time.Second)
loop:
	for received < N {
		select {
		case got := <-b.IncomingPCM():
			require.Equal(t, 320, len(got), "incoming frame should be 320 samples (20ms@16kHz)")
			received++
		case <-deadline:
			break loop
		}
	}
	require.Equal(t, N, received, "B should receive every frame A sent")
}

func TestMediaIO_RTPResumesTalkspurtAfterSuppressedSilence(t *testing.T) {
	// This test drives SendFrame directly with wall-clock gaps between writes;
	// idle keepalive would fill those gaps and no talkspurt would ever resume.
	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	sender, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	receiver, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer receiver.Close()

	mio, err := NewMediaIO(sender, receiver.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), time.Hour, "")
	require.NoError(t, err)
	defer mio.Close()

	readPacket := func() *prtp.Packet {
		t.Helper()
		require.NoError(t, receiver.SetReadDeadline(time.Now().Add(time.Second)))
		buf := make([]byte, 1500)
		n, _, err := receiver.ReadFrom(buf)
		require.NoError(t, err)
		packet, err := flowrtp.Unpack(buf[:n])
		require.NoError(t, err)
		return packet
	}

	require.NoError(t, mio.SendFrame(make([]int16, 320)))
	first := readPacket()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, mio.SendFrame(make([]int16, 320)))
	resumed := readPacket()

	require.True(t, first.Marker)
	require.True(t, resumed.Marker, "the first RTP packet after suppressed silence must start a talkspurt")
	require.Equal(t, first.SequenceNumber+1, resumed.SequenceNumber)
	require.GreaterOrEqual(t, resumed.Timestamp-first.Timestamp, uint32(640),
		"the RTP timestamp must include the silent wall-clock interval")
}

// A NAT'd peer advertises a private (unreachable) address in its SDP, so the
// SDP-derived send target is a black hole. Symmetric RTP must latch the send
// target to the source of the peer's actual inbound packets so audio flows both
// ways.
func TestMediaIO_SymmetricRTPLatchesToSource(t *testing.T) {
	// The latch must be caused by B's enqueued audio below, not by B's idle
	// keepalive stream.
	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	// A is aimed at a private (unreachable, NAT'd-phone-style) address — the
	// black hole that symmetric RTP must correct — never B's real socket.
	a, err := NewMediaIO(pcA, "10.255.255.1:5004", flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	// B is aimed at A's real address and sends first.
	b, err := NewMediaIO(pcB, pcA.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()

	samples := make([]int16, 320)
	for i := range samples {
		samples[i] = int16(math.Sin(2*math.Pi*1000*float64(i)/16000.0) * 8000)
	}

	// B → A: A latches its send target to B's real source.
	go func() {
		for i := 0; i < 25; i++ {
			select {
			case b.OutgoingPCM() <- samples:
			case <-time.After(time.Second):
				return
			}
		}
	}()

	require.Eventually(t, func() bool {
		ra := a.remoteAddr.Load()
		return ra != nil && ra.String() == pcB.LocalAddr().String()
	}, 2*time.Second, 10*time.Millisecond, "A should latch send target to B's real source")

	// A → B must now reach B (it would have gone to the black hole before latch).
	go func() {
		for i := 0; i < 25; i++ {
			select {
			case a.OutgoingPCM() <- samples:
			case <-time.After(time.Second):
				return
			}
		}
	}()

	select {
	case got := <-b.IncomingPCM():
		require.Equal(t, 320, len(got))
	case <-time.After(3 * time.Second):
		t.Fatal("B never received audio from A after latch")
	}
}

// A hairpinned packet that appears to come from our own public media IP must
// not redirect the send target (otherwise desk-phone audio loops to ourselves).
func TestMediaIO_SymmetricRTPIgnoresOwnMediaIP(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcA.Close()

	sdp := "203.0.113.50:12232"
	own := "198.51.100.10"
	a, err := NewMediaIO(pcA, sdp, flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, own)
	require.NoError(t, err)
	defer a.Close()

	// Inject a packet sourced from our own public IP (EIP hairpin).
	a.maybeLatchRemote(&net.UDPAddr{IP: net.ParseIP(own), Port: 16386})
	require.False(t, a.latched.Load(), "own-media-IP source must not mark latched")
	require.Equal(t, sdp, a.remoteAddr.Load().String(), "send target must stay on the peer")

	// A later real peer packet may still latch (port change on same public host).
	peer := &net.UDPAddr{IP: net.ParseIP("203.0.113.50"), Port: 40000}
	a.maybeLatchRemote(peer)
	require.True(t, a.latched.Load())
	require.Equal(t, peer.String(), a.remoteAddr.Load().String())
}

// g711Packet builds one 20 ms PCMU packet whose payload is a single repeated
// byte, so a frame's origin can be told from the samples it decodes to.
func g711Packet(seq uint16, payload byte) []byte {
	pkt := make([]byte, 12+160)
	pkt[0] = 0x80 // version 2
	pkt[1] = 0    // PT 0 = PCMU
	binary.BigEndian.PutUint16(pkt[2:4], seq)
	binary.BigEndian.PutUint32(pkt[4:8], uint32(seq)*160)
	binary.BigEndian.PutUint32(pkt[8:12], 0x11223344)
	for i := 12; i < len(pkt); i++ {
		pkt[i] = payload
	}
	return pkt
}

// The socket is bound when the offer is built but unread until the call is
// answered, so a carrier playing ringback leaves hundreds of packets waiting
// in the kernel. Handing that backlog downstream in one burst permanently
// inflates the playback queue — paced at one frame per 20 ms, a queue that
// arrives at once never drains — which is heard as seconds of delay for the
// rest of the call, plus dropouts as the buffers overflow.
func TestMediaIO_DiscardsMediaBufferedBeforeTheCallConnected(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer peer.Close()

	// Ringback: 200 packets (4 s) arrive while nothing is reading the socket.
	const backlog = 200
	for i := 0; i < backlog; i++ {
		_, err := peer.WriteTo(g711Packet(uint16(i+1), 0xFF), pcA.LocalAddr())
		require.NoError(t, err)
	}
	// Let the kernel finish queueing them before the call is answered.
	time.Sleep(50 * time.Millisecond)

	a, err := NewMediaIO(pcA, peer.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()

	// The call connects and the far end starts talking.
	const live = 5
	for i := 0; i < live; i++ {
		_, err := peer.WriteTo(g711Packet(uint16(backlog+i+1), 0x00), pcA.LocalAddr())
		require.NoError(t, err)
	}

	var frames [][]int16
	deadline := time.After(500 * time.Millisecond)
collect:
	for {
		select {
		case f := <-a.IncomingPCM():
			frames = append(frames, f)
		case <-deadline:
			break collect
		}
	}

	require.NotEmpty(t, frames, "audio from after the answer must still arrive")
	require.LessOrEqual(t, len(frames), live,
		"got %d frames for %d live packets — the ringback backlog was delivered too",
		len(frames), live)
	loudest := 0
	for _, s := range frames[0] {
		if v := int(s); v > loudest {
			loudest = v
		} else if -v > loudest {
			loudest = -v
		}
	}
	require.Greater(t, loudest, 20000,
		"the first frame decodes near silence, so it is a stale ringback packet")
}

func TestMediaIO_ReconfigureAcceptsNewStreamAndResetsLatch(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	peerA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer peerA.Close()
	peerB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer peerB.Close()

	mio, err := NewMediaIOForEarlyMedia(
		pc,
		peerA.LocalAddr().String(),
		flowrtp.PayloadTypePCMU,
		0,
		quietLogger(),
		0,
		"",
	)
	require.NoError(t, err)
	defer mio.Close()

	_, err = peerA.WriteTo(g711Packet(400, 0x00), pc.LocalAddr())
	require.NoError(t, err)
	select {
	case <-mio.IncomingPCM():
	case <-time.After(time.Second):
		t.Fatal("initial RTP stream was not decoded")
	}
	require.True(t, mio.latched.Load())

	require.NoError(t, mio.Reconfigure(
		peerB.LocalAddr().String(),
		flowrtp.PayloadTypePCMA,
		96,
	))
	require.False(t, mio.latched.Load(), "final SDP must clear the early symmetric-RTP latch")

	pkt := g711Packet(1, 0x55)
	pkt[1] = flowrtp.PayloadTypePCMA
	binary.BigEndian.PutUint32(pkt[8:12], 0x55667788)
	_, err = peerB.WriteTo(pkt, pc.LocalAddr())
	require.NoError(t, err)
	select {
	case frame := <-mio.IncomingPCM():
		require.NotEmpty(t, frame)
	case <-time.After(time.Second):
		t.Fatal("RTP from the reconfigured final stream was not decoded")
	}
}

func TestMediaIO_ReconfigureKeepsOutgoingRTPStream(t *testing.T) {
	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	early, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer early.Close()
	final, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer final.Close()

	mio, err := NewMediaIOForEarlyMedia(pc, early.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer mio.Close()

	readPacket := func(conn net.PacketConn) *prtp.Packet {
		t.Helper()
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
		buf := make([]byte, 1500)
		n, _, err := conn.ReadFrom(buf)
		require.NoError(t, err)
		packet, err := flowrtp.Unpack(buf[:n])
		require.NoError(t, err)
		return packet
	}

	require.NoError(t, mio.SendFrame(make([]int16, 320)))
	before := readPacket(early)

	require.NoError(t, mio.Reconfigure(final.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0))
	require.NoError(t, mio.SendFrame(make([]int16, 320)))
	after := readPacket(final)

	require.Equal(t, before.SSRC, after.SSRC, "the answer must not start a new RTP stream")
	require.Equal(t, before.SequenceNumber+1, after.SequenceNumber)
	require.Greater(t, after.Timestamp-before.Timestamp, uint32(0))
	require.Less(t, after.Timestamp-before.Timestamp, uint32(8000), "the RTP timestamp must not jump")
}

func TestMediaIO_PreAnswerIdleTimeoutStartsWhenEnabled(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer peer.Close()

	mio, err := NewMediaIOForEarlyMedia(
		pc,
		peer.LocalAddr().String(),
		flowrtp.PayloadTypePCMU,
		0,
		quietLogger(),
		80*time.Millisecond,
		"",
	)
	require.NoError(t, err)
	defer mio.Close()
	mio.SetIdleTimeout(0)

	select {
	case _, ok := <-mio.IncomingPCM():
		if !ok {
			t.Fatal("pre-answer media closed while idle timeout was disabled")
		}
	case <-time.After(180 * time.Millisecond):
	}

	mio.SetIdleTimeout(80 * time.Millisecond)
	select {
	case _, ok := <-mio.IncomingPCM():
		require.False(t, ok, "answered idle timeout must close the incoming stream")
	case <-time.After(time.Second):
		t.Fatal("idle timeout did not start from the enable point")
	}
}

// rtcpSenderReport builds a minimal, well-formed RTCP sender report.
func rtcpSenderReport() []byte {
	sr := make([]byte, 28)
	sr[0] = 0x80 // version 2, no padding, zero report blocks
	sr[1] = 200  // PT 200 = sender report
	sr[3] = 6    // length in 32-bit words minus one
	return sr
}

// A peer sends its RTCP reports to the odd port above the RTP port it is
// talking to, which is the port the next call gets. Those reports carry the
// same version bits as RTP and unpack cleanly, so before this was handled one
// arriving ahead of the far end's first voice packet would latch this call's
// send target onto the reporting phone: the customer then heard nothing for
// the entire call while their own audio kept arriving normally.
func TestMediaIO_AnRTCPReportDoesNotHijackTheSendTarget(t *testing.T) {
	report := rtcpSenderReport()
	_, err := flowrtp.Unpack(report)
	require.NoError(t, err, "the report must parse as RTP, or this test proves nothing")

	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	stray, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer stray.Close()

	sdp := "203.0.113.50:12232"
	a, err := NewMediaIO(pcA, sdp, flowrtp.PayloadTypePCMU, 101, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()

	_, err = stray.WriteTo(report, pcA.LocalAddr())
	require.NoError(t, err)

	require.Never(t, func() bool { return a.latched.Load() },
		500*time.Millisecond, 10*time.Millisecond,
		"an RTCP report must not latch the send target")
	require.Equal(t, sdp, a.remoteAddr.Load().String(),
		"audio must still be sent to the peer we negotiated with")
}

// One log line says a foreign sender exists; only a count says whether it cost
// the call a moment or its opening seconds. Carriers have been seen holding
// their ringback codec for a second or two past the answer, which eats the
// caller's first words — indistinguishable, without this, from someone who
// answered and said nothing.
func TestMediaIO_CountsEveryPacketItDropsForAForeignPayloadType(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	stray, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer stray.Close()

	a, err := NewMediaIO(pcA, "203.0.113.50:12232", flowrtp.PayloadTypePCMU, 101, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()

	const sent = 5
	for i := 0; i < sent; i++ {
		_, err = stray.WriteTo(rtcpSenderReport(), pcA.LocalAddr())
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return a.ForeignPackets() == sent },
		time.Second, 10*time.Millisecond,
		"counted %d of %d dropped packets", a.ForeignPackets(), sent)
}

// The same packet must not reach the jitter buffer either: decoded as G.711 it
// would surface as a burst of noise, and fed to the recogniser as speech.
func TestMediaIO_AnRTCPReportIsNotDeliveredAsAudio(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	stray, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer stray.Close()

	a, err := NewMediaIO(pcA, "203.0.113.50:12232", flowrtp.PayloadTypePCMU, 101, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()

	_, err = stray.WriteTo(rtcpSenderReport(), pcA.LocalAddr())
	require.NoError(t, err)

	select {
	case got := <-a.IncomingPCM():
		t.Fatalf("an RTCP report was delivered as %d samples of audio", len(got))
	case <-time.After(500 * time.Millisecond):
	}
}

// A symmetric peer (its RTP source matches the SDP address) must not be
// redirected — latching only kicks in when the source differs from the SDP.
func TestMediaIO_SymmetricRTPNoLatchWhenSourceMatches(t *testing.T) {
	// Keepalive silence from B would latch A before the enqueued audio below
	// gets to prove the same thing.
	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	// A points at B's REAL address, so B's RTP source == the SDP address.
	sdp := pcB.LocalAddr().String()
	a, err := NewMediaIO(pcA, sdp, flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	b, err := NewMediaIO(pcB, pcA.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()

	samples := make([]int16, 320)
	go func() {
		for i := 0; i < 15; i++ {
			select {
			case b.OutgoingPCM() <- samples:
			case <-time.After(time.Second):
				return
			}
		}
	}()

	require.Eventually(t, func() bool { return a.latched.Load() }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, sdp, a.remoteAddr.Load().String(),
		"a symmetric peer (source == SDP) must not be redirected")
}

// While a feeder is active, a momentarily-empty queue (a TTS backend that fell
// behind real-time) must NOT be treated as end-of-playback: the paced sender
// rides it out with silence and only goes idle once feeding is cleared. This is
// what keeps a bursty/slow source from leaving audible gaps on the wire.
func TestMediaIO_HoldsStreamWhileFeeding(t *testing.T) {
	a, b, cleanup := makePair(t)
	defer cleanup()

	a.SetPlaybackFeeding(true)
	a.OutgoingPCM() <- make([]int16, 320)

	// Wait until a frame actually reaches B, so the sender is mid-playback.
	select {
	case <-b.IncomingPCM():
	case <-time.After(time.Second):
		t.Fatal("no frame reached peer")
	}

	// Queue is now empty but feeding is still true → must not go idle.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	require.Error(t, a.WaitPlaybackIdle(ctx),
		"playback must not go idle while a feeder is still active")

	// Clearing the feeder lets the sender drain the tail and go idle.
	a.SetPlaybackFeeding(false)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.NoError(t, a.WaitPlaybackIdle(ctx2),
		"playback should go idle once feeding stops")
}

func TestMediaIO_Close_Idempotent(t *testing.T) {
	a, _, cleanup := makePair(t)
	defer cleanup()
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
}

func TestMediaIO_FlushesPartialTailFrame(t *testing.T) {
	a, b, cleanup := makePair(t)
	defer cleanup()

	// 100 samples @ 16k — not a multiple of 320; tail must still be sent padded.
	samples := make([]int16, 100)
	for i := range samples {
		samples[i] = 500
	}

	a.OutgoingPCM() <- samples

	require.NoError(t, a.WaitPlaybackIdle(context.Background()))

	select {
	case got := <-b.IncomingPCM():
		require.Equal(t, 320, len(got))
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not receive tail frame")
	}
}

func TestMediaIO_StopOutgoing_ClearsQueuedAudio(t *testing.T) {
	a, _, cleanup := makePair(t)
	defer cleanup()

	samples := make([]int16, 320*50) // ~1s of audio
	for i := range samples {
		samples[i] = 1000
	}

	done := make(chan struct{})
	go func() {
		_ = a.WaitPlaybackIdle(context.Background())
		close(done)
	}()

	a.OutgoingPCM() <- samples
	time.Sleep(30 * time.Millisecond)
	a.StopOutgoing()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitPlaybackIdle did not return after StopOutgoing")
	}
}

func TestMediaIO_CloseStopsIncoming(t *testing.T) {
	a, _, cleanup := makePair(t)
	defer cleanup()

	require.NoError(t, a.Close())

	// IncomingPCM should close shortly after
	select {
	case _, ok := <-a.IncomingPCM():
		require.False(t, ok, "incoming should be closed after Close")
	case <-time.After(2 * time.Second):
		t.Fatal("IncomingPCM did not close within 2s of Close")
	}
}

func TestMediaIO_CloseStopsIdleOutgoingPump(t *testing.T) {
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcA.Close()
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pcB.Close()

	mio, err := NewMediaIO(pcA, pcB.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), time.Hour, "")
	require.NoError(t, err)

	require.NoError(t, mio.Close())

	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return !strings.Contains(string(buf[:n]), "internal/transport/pacer.(*Pacer).loop")
	}, time.Second, 10*time.Millisecond)
}

// Idle keepalive is on by default for SIP: from the moment the call connects,
// the far end sees a continuous RTP stream (G.711 silence) even before any
// audio is enqueued. SIP_IDLE_KEEPALIVE=0 opts out.
func TestMediaIO_IdleKeepaliveOnByDefault(t *testing.T) {
	t.Setenv("SIP_IDLE_KEEPALIVE", "1")
	pcA, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	pcB, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	a, err := NewMediaIO(pcA, pcB.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	b, err := NewMediaIO(pcB, pcA.LocalAddr().String(), flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()
	defer b.Close()

	// No OutgoingPCM writes: B must still receive 20 ms frames of silence.
	for i := 0; i < 2; i++ {
		select {
		case got := <-b.IncomingPCM():
			require.Equal(t, 320, len(got))
			require.Equal(t, int16(0), got[0])
		case <-time.After(2 * time.Second):
			t.Fatal("no keepalive frame reached the peer")
		}
	}
	require.Eventually(t, func() bool { return a.PlaybackStats().KeepaliveFrames >= 2 },
		time.Second, 5*time.Millisecond)
}
