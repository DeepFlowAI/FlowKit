package sip

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/recording"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// syncLogBuf is a mutex-guarded bytes.Buffer for slog capture across runOriginate.
type syncLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

type earlyRecordingCapture struct {
	mu     sync.Mutex
	events []map[string]any
}

func (c *earlyRecordingCapture) Notify(method, _ string, params map[string]any) {
	if method != "call.recording.completed" {
		return
	}
	copyParams := make(map[string]any, len(params))
	for key, value := range params {
		copyParams[key] = value
	}
	c.mu.Lock()
	c.events = append(c.events, copyParams)
	c.mu.Unlock()
}

func (c *earlyRecordingCapture) completed() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) == 0 {
		return nil
	}
	return c.events[len(c.events)-1]
}

func (b *syncLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureTestLogger() (*slog.Logger, *syncLogBuf) {
	buf := &syncLogBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func TestIsNormalOriginateOutcome(t *testing.T) {
	// Every reason endOriginate may receive: Info (normal) vs Warn (setup fail).
	cases := []struct {
		reason string
		normal bool
	}{
		{"busy", true},
		{"no_answer", true},
		{"local_cancel", true},
		// 4xx catch-all: may be a trunk fault, must stay visible at Warn.
		{"rejected", false},
		{"originate_failed", false},
		{"invite_failed", false},
		{"bad_answer_sdp", false},
		{"no_g711", false},
		{"ack_failed", false},
		{"mediaio_failed", false},
		{"bind_failed", false},
		{"", false},
		{"unknown_bucket", false},
	}
	for _, c := range cases {
		t.Run(c.reason, func(t *testing.T) {
			require.Equal(t, c.normal, isNormalOriginateOutcome(c.reason))
		})
	}
}

func TestSipResponseLogAttrsIncludesStatusAndRaw(t *testing.T) {
	resp := sip.NewResponse(404, "Not Found")
	resp.AppendHeader(sip.NewHeader("Warning", `399 trunk "no route"`))
	resp.AppendHeader(sip.NewHeader("Reason", `SIP;cause=404;text="Not Found"`))
	resp.AppendHeader(sip.NewHeader("Call-ID", "abc@carrier.example"))
	resp.SetSource("203.0.113.240:5060")

	attrs := sipResponseLogAttrs(resp)
	m := map[string]any{}
	for i := 0; i+1 < len(attrs); i += 2 {
		k, ok := attrs[i].(string)
		require.True(t, ok)
		m[k] = attrs[i+1]
	}
	require.Equal(t, "Not Found", m["sip_reason"])
	require.Equal(t, "203.0.113.240:5060", m["sip_source"])
	require.Equal(t, "SIP/2.0 404 Not Found", m["sip_start_line"])
	require.Equal(t, `399 trunk "no route"`, m["sip_hdr_warning"])
	require.Equal(t, `SIP;cause=404;text="Not Found"`, m["sip_hdr_reason"])
	require.Equal(t, "abc@carrier.example", m["sip_hdr_call_id"])
	raw, ok := m["sip_response"].(string)
	require.True(t, ok)
	require.Contains(t, raw, "SIP/2.0 404 Not Found")
	require.Contains(t, raw, "Warning:")
	require.Nil(t, sipResponseLogAttrs(nil))
}

func TestResolveOriginateMediaHost(t *testing.T) {
	cases := []struct {
		name       string
		sdp        string
		sipSource  string
		target     string
		wantHost   string
		wantReason string
	}{
		{"public_sdp", "203.0.113.10", "198.51.100.1", "203.0.113.99", "203.0.113.10", ""},
		{"direct_nat", "192.168.1.130", "198.51.100.54", "198.51.100.54", "198.51.100.54", "sip_source"},
		{"proxied_nat", "192.168.1.130", "127.0.0.1", "198.51.100.54", "198.51.100.54", "target_host"},
		{"proxied_no_target", "10.0.0.5", "127.0.0.1", "", "127.0.0.1", "sip_source_fallback"},
		{"hostname_sdp", "media.example.com", "127.0.0.1", "198.51.100.54", "media.example.com", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, reason := resolveOriginateMediaHost(c.sdp, c.sipSource, c.target)
			require.Equal(t, c.wantHost, got)
			require.Equal(t, c.wantReason, reason)
		})
	}
}

func TestResolveOriginateContact(t *testing.T) {
	target := sip.Uri{User: "ext", Host: "198.51.100.54", Port: 21881}
	t.Run("direct_uses_response_source", func(t *testing.T) {
		host, port, ok := resolveOriginateContact("192.168.1.130", 5060, "", "198.51.100.54:21881", target)
		require.True(t, ok)
		require.Equal(t, "198.51.100.54", host)
		require.Equal(t, 21881, port)
	})
	t.Run("proxied_private_uses_target", func(t *testing.T) {
		host, port, ok := resolveOriginateContact("192.168.1.130", 5060, "127.0.0.1:5070", "127.0.0.1:5070", target)
		require.True(t, ok)
		require.Equal(t, "198.51.100.54", host)
		require.Equal(t, 21881, port)
	})
	t.Run("proxied_public_no_rewrite", func(t *testing.T) {
		_, _, ok := resolveOriginateContact("198.51.100.54", 21881, "127.0.0.1:5070", "127.0.0.1:5070", target)
		require.False(t, ok)
	})
	t.Run("direct_already_matches_source", func(t *testing.T) {
		_, _, ok := resolveOriginateContact("198.51.100.54", 21881, "", "198.51.100.54:21881", target)
		require.False(t, ok)
	})
}

func TestParseTargetURI(t *testing.T) {
	cases := []struct {
		in       string
		wantUser string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{"sip:1001@trunk.example.com:5060", "1001", "trunk.example.com", 5060, false},
		{"sip:bob@10.0.0.5", "bob", "10.0.0.5", 5060, false},
		{"alice@example.com", "alice", "example.com", 5060, false},
		{"sip:host.only:5070", "", "host.only", 5070, false},
		{"host.only", "", "host.only", 5060, false},
		// Contact URI recorded from a REGISTER: the ;transport param must not be
		// parsed as part of the port.
		{"sip:353c4046ca714c80-10@198.51.100.132:29767;transport=udp", "353c4046ca714c80-10", "198.51.100.132", 29767, false},
		{"sip:bob@10.0.0.5;transport=udp", "bob", "10.0.0.5", 5060, false},
		{"sip:bob@host:5060;transport=udp?x=y", "bob", "host", 5060, false},
		{"sip:user@host:notaport", "", "", 0, true},
		{"", "", "", 0, true},
		{"sip:user@", "", "", 0, true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			u, err := parseTargetURI(c.in)
			if c.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantUser, u.User)
			require.Equal(t, c.wantHost, u.Host)
			require.Equal(t, c.wantPort, u.Port)
		})
	}
}

// startRingingUAS runs a minimal SIP UAS that answers every INVITE with 180
// Ringing and never sends 200 OK. Used to assert proxy/direct routing; 180
// also lets bestEffortCancel stop the far-end early-dialog after ForceCancel.
func startRingingUAS(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-ringing-uas"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)

	var inviteCount atomic.Int32
	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		inviteCount.Add(1)
		resp := sip.NewResponseFromRequest(req, 180, "Ringing", nil)
		resp.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: port}})
		_ = tx.Respond(resp)
		select {
		case <-tx.Done():
		case <-time.After(10 * time.Second):
		}
	})

	pc, err := net.ListenPacket("udp", listen)
	require.NoError(t, err)
	go func() { _ = srv.ServeUDP(pc) }()

	cleanup := func() {
		_ = srv.Close()
		_ = ua.Close()
		_ = pc.Close()
	}
	return listen, &inviteCount, cleanup
}

// startTestUAS runs a minimal SIP UAS that 200-OKs every INVITE with a
// PCMU answer pointing at a real (closed-after-test) RTP port. byeCount
// counts BYE arrivals so the test can assert BYE-on-hangup.
func startTestUAS(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-uas"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)

	rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

	var byeCount atomic.Int32

	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		answer := BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano())
		resp := sip.NewResponseFromRequest(req, 200, "OK", []byte(answer))
		resp.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
		resp.AppendHeader(sip.NewHeader("Content-Length", strconv.Itoa(len(answer))))
		resp.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: port}})
		_ = tx.Respond(resp)
	})
	srv.OnAck(func(_ *sip.Request, _ sip.ServerTransaction) {})
	srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
		byeCount.Add(1)
		resp := sip.NewResponseFromRequest(req, 200, "OK", nil)
		_ = tx.Respond(resp)
	})

	pc, err := net.ListenPacket("udp", listen)
	require.NoError(t, err)

	go func() { _ = srv.ServeUDP(pc) }()

	cleanup := func() {
		_ = srv.Close()
		_ = ua.Close()
		_ = pc.Close()
		_ = rtpPC.Close()
	}
	return listen, &byeCount, cleanup
}

func startEarlyMediaFailureUAS(t *testing.T) (string, func()) {
	t.Helper()
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-early-media-uas"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)
	rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		offer, parseErr := ParseSDP(string(req.Body()))
		if parseErr != nil {
			return
		}
		answer := BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano())
		progress := sip.NewResponseFromRequest(req, 183, "Session Progress", []byte(answer))
		progress.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
		progress.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: port}})
		_ = tx.Respond(progress)

		// Give the caller time to apply the provisional SDP, then play a short
		// carrier announcement before the final unavailable response.
		time.Sleep(80 * time.Millisecond)
		dst := &net.UDPAddr{IP: net.ParseIP(offer.ConnectIP), Port: offer.AudioPort}
		for i := 0; i < 8; i++ {
			_, _ = rtpPC.WriteTo(g711Packet(uint16(i+1), 0x00), dst)
			time.Sleep(20 * time.Millisecond)
		}
		final := sip.NewResponseFromRequest(req, 480, "Temporarily Unavailable", nil)
		final.AppendHeader(sip.NewHeader("Reason", "Q.850;cause=20;text=subscriber absent"))
		_ = tx.Respond(final)
	})

	pc, err := net.ListenPacket("udp", listen)
	require.NoError(t, err)
	go func() { _ = srv.ServeUDP(pc) }()

	return listen, func() {
		_ = srv.Close()
		_ = ua.Close()
		_ = pc.Close()
		_ = rtpPC.Close()
	}
}

func TestSIP_Outbound_RecordsEarlyMediaBeforeFinalFailure(t *testing.T) {
	uasAddr, uasCleanup := startEarlyMediaFailureUAS(t)
	defer uasCleanup()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	sigState.SetCaptureEarlyMedia(true)
	recordingCapture := &earlyRecordingCapture{}
	recordingManager, err := recording.NewManager(recording.Config{
		Enabled:           true,
		StartOn:           "answered",
		CaptureEarlyMedia: true,
		Encoding:          "pcm16",
		Layout:            recording.LayoutStereo,
		FlushInterval:     10 * time.Millisecond,
		Storage: recording.StorageConfig{
			Backend: "local",
			Local: recording.LocalConfig{
				Dir: t.TempDir(),
			},
			AliyunOSS: recording.AliyunOSSConfig{
				KeyTemplate: "recordings/{call_id}.wav",
			},
		},
	}, quietLogger())
	require.NoError(t, err)
	recordingManager.SetNotifier(recordingCapture)
	recordingManager.RegisterHooks(mgr)

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	sub := call.AudioBus().Subscribe()
	require.NotNil(t, sub)
	defer call.AudioBus().Unsubscribe(sub)

	require.NoError(t, sigState.StartOriginateCall(
		call,
		"sip:1001@"+uasAddr,
		"",
		"",
		3*time.Second,
		nil,
	))

	select {
	case frame := <-sub.Channel():
		require.NotEmpty(t, frame.Samples)
	case <-time.After(2 * time.Second):
		t.Fatal("carrier early media did not reach the call AudioBus")
	}
	select {
	case <-call.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("final 480 did not end the outbound call")
	}
	require.False(t, call.EverAnswered())
	require.Eventually(t, func() bool {
		return recordingCapture.completed() != nil && recordingManager.ActiveCount() == 0
	}, 2*time.Second, 20*time.Millisecond)
	completed := recordingCapture.completed()
	require.Equal(t, "early_media", completed["phase"])
	require.NotEmpty(t, completed["oss_url"])
	require.Greater(t, completed["duration_ms"].(int64), int64(0))
}

func TestSIP_Outbound_Originate_200OK_ACK(t *testing.T) {
	// Set up UAS first so we know the target address.
	uasAddr, byeCount, uasCleanup := startTestUAS(t)
	defer uasCleanup()

	// flowkit-side UA + Signal
	flowAddr, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	_ = flowAddr

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	call, err := sigState.Originate(ctx, "sip:1001@"+uasAddr)
	require.NoError(t, err)
	require.NotNil(t, call)
	require.Equal(t, session.DirectionOutbound, call.Direction())
	require.Equal(t, session.StateActive, call.State())
	require.Equal(t, 1, sigState.ActiveCalls())
	require.Equal(t, 1, mgr.Count())

	// Hangup → cleanup goroutine sends BYE
	require.NoError(t, call.Hangup("test_done"))
	require.Eventually(t, func() bool { return sigState.ActiveCalls() == 0 }, 3*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return byeCount.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"flowkit must send BYE on outbound-call hangup")
}

func TestSIP_Outbound_DestinationUsesOutboundProxyWhenSet(t *testing.T) {
	// Ringing UAS stands in for a front-end proxy (e.g. Kamailio). It must
	// send 180 so Hangup→CANCEL can finish; a silent UDP socket leaves
	// inviteCancel blocked until Timer B (~32s).
	proxyAddr, invites, proxyCleanup := startRingingUAS(t)
	defer proxyCleanup()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	sigState.SetOutboundProxy(proxyAddr)

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	t.Cleanup(func() { _ = mgr.Close(call.ID(), "test_done") })

	// 203.0.113.0/24 is TEST-NET-3 (RFC 5737): Request-URI stays there, so the
	// only way the INVITE reaches the proxy UAS is via SetOutboundProxy.
	err := sigState.StartOriginateCall(call, "sip:1001@203.0.113.1:5060", "", "", 2*time.Second, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return invites.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"expected INVITE to reach the outbound proxy")
	require.NoError(t, mgr.Close(call.ID(), "test_done"))
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_Outbound_DestinationUsesPhoneProxyWhenTrunkIDEmpty(t *testing.T) {
	trunkProxy, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer trunkProxy.Close()
	phoneAddr, phoneInvites, phoneCleanup := startRingingUAS(t)
	defer phoneCleanup()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	sigState.SetOutboundProxy(trunkProxy.LocalAddr().String())
	sigState.SetPhoneOutboundProxy(phoneAddr)

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	t.Cleanup(func() { _ = mgr.Close(call.ID(), "test_done") })

	// Empty trunk_id + full Contact URI = extension / direct dial → phone proxy.
	err = sigState.StartOriginateCall(call, "sip:1001@203.0.113.1:5060", "", "", 2*time.Second, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return phoneInvites.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"expected INVITE on phone outbound proxy")

	// Trunk proxy must stay quiet.
	buf := make([]byte, 2048)
	_ = trunkProxy.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err = trunkProxy.ReadFrom(buf)
	require.Error(t, err, "trunk proxy must not receive extension originate")

	require.NoError(t, mgr.Close(call.ID(), "test_done"))
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_Outbound_DestinationUsesTrunkProxyWhenTrunkIDSet(t *testing.T) {
	trunkAddr, trunkInvites, trunkCleanup := startRingingUAS(t)
	defer trunkCleanup()
	phoneProxy, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer phoneProxy.Close()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	sigState.SetOutboundProxy(trunkAddr)
	sigState.SetPhoneOutboundProxy(phoneProxy.LocalAddr().String())
	sigState.SetTrunkConfig(TrunkConfig{
		Server: "203.0.113.9",
		Port:   5060,
		DID:    "1000",
		User:   "1000",
	})
	sigState.SetOutboundResolver(func(id string) (TrunkConfig, bool) {
		if id == "trunk-a" {
			return TrunkConfig{Server: "203.0.113.9", Port: 5060, DID: "1000", User: "1000"}, true
		}
		return TrunkConfig{}, false
	})

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	t.Cleanup(func() { _ = mgr.Close(call.ID(), "test_done") })

	err = sigState.StartOriginateCall(call, "13800138000", "", "trunk-a", 2*time.Second, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return trunkInvites.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"expected INVITE on trunk outbound proxy")
	require.NoError(t, mgr.Close(call.ID(), "test_done"))
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_Outbound_DestinationFallsBackToTargetWhenProxyEmpty(t *testing.T) {
	// Ringing target (no outbound proxy): INVITE goes straight there; 180 lets
	// Hangup→CANCEL finish without waiting out Timer B.
	targetAddr, invites, targetCleanup := startRingingUAS(t)
	defer targetCleanup()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()
	// sigState.outboundProxy left empty (default).

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	t.Cleanup(func() { _ = mgr.Close(call.ID(), "test_done") })

	target := "sip:1001@" + targetAddr
	err := sigState.StartOriginateCall(call, target, "", "", 2*time.Second, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return invites.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"expected INVITE datagram to reach the target directly")
	require.NoError(t, mgr.Close(call.ID(), "test_done"))
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_Outbound_Originate_NoAnswerCloses(t *testing.T) {
	// Ringing-but-never-answers peer: Originate's wait ctx expires and Hangup
	// must tear the call down.
	uasAddr, _, uasCleanup := startRingingUAS(t)
	defer uasCleanup()

	_, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := sigState.Originate(ctx, "sip:nobody@"+uasAddr)
	require.Error(t, err, "originate with no answer should error")
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_Outbound_Originate_CancelReclaimsBeforeTimerB(t *testing.T) {
	// Silent peer (no provisional): ForceCancel exits WaitAnswer without
	// bestEffortCancel's CANCEL round-trip; hangup notify must be ≪1s.
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer silent.Close()

	log, logBuf := captureTestLogger()
	_, sigState, mgr, br, teardown := startTestUAWithLogger(t, log)
	defer teardown()

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)

	target := "sip:nobody@" + silent.LocalAddr().String()
	require.NoError(t, sigState.StartOriginateCall(call, target, "", "", 30*time.Second, nil))

	require.Eventually(t, func() bool {
		buf := make([]byte, 2048)
		_ = silent.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		_, _, err := silent.ReadFrom(buf)
		return err == nil
	}, time.Second, 10*time.Millisecond, "expected outbound INVITE to reach silent peer")

	started := time.Now()
	require.NoError(t, mgr.Close(call.ID(), "test_cancel"))
	require.Eventually(t, func() bool { return br.hasMethod("call.hangup") }, time.Second, 5*time.Millisecond,
		"runOriginate must exit WaitAnswer and emit hangup without Timer B")
	elapsed := time.Since(started)
	require.Less(t, elapsed, time.Second, "no-provisional cancel→hangup took %v, want <1s (Timer B is ~32s)", elapsed)
	t.Logf("no-provisional cancel→hangup notify in %v", elapsed)
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, time.Second, 10*time.Millisecond)

	logs := logBuf.String()
	require.Contains(t, logs, "originate skip CANCEL: no provisional response")
	require.Contains(t, logs, call.ID())
	require.NotContains(t, logs, "originate CANCEL failed")
	require.NotContains(t, logs, "originate CANCEL sent")
}

func TestSIP_Outbound_Originate_CancelWithProvisionalReclaims(t *testing.T) {
	// 180 then hangup: bestEffortCancel runs synchronously (≤ cancelAttemptTimeout).
	// sipgo UAS auto-200s CANCEL at the transaction layer, so this path should
	// finish well under the 500ms bound when CANCEL is delivered.
	uasAddr, _, uasCleanup := startRingingUAS(t)
	defer uasCleanup()

	log, logBuf := captureTestLogger()
	_, sigState, mgr, br, teardown := startTestUAWithLogger(t, log)
	defer teardown()

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	target := "sip:1001@" + uasAddr
	require.NoError(t, sigState.StartOriginateCall(call, target, "", "", 30*time.Second, nil))

	require.Eventually(t, func() bool { return br.hasMethod("call.ringing") }, 3*time.Second, 10*time.Millisecond)

	started := time.Now()
	require.NoError(t, mgr.Close(call.ID(), "test_cancel"))
	require.Eventually(t, func() bool { return br.hasMethod("call.hangup") }, time.Second, 5*time.Millisecond)
	elapsed := time.Since(started)
	require.Less(t, elapsed, time.Second, "provisional cancel→hangup took %v, want <1s", elapsed)
	t.Logf("provisional cancel→hangup notify (includes bestEffortCancel) in %v", elapsed)
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, time.Second, 10*time.Millisecond)

	logs := logBuf.String()
	require.Contains(t, logs, "originate CANCEL sent")
	require.Contains(t, logs, call.ID())
	require.Contains(t, logs, "sip_status=200")
}

func TestSIP_Outbound_Originate_CancelFailureLogsWarn(t *testing.T) {
	// 180 then peer socket gone: CANCEL gets no reply → Do hits cancelAttemptTimeout.
	// Warn must carry call_id, dest/target, err, and elapsed for postmortems.
	uasAddr, _, uasCleanup := startRingingUAS(t)

	log, logBuf := captureTestLogger()
	_, sigState, mgr, br, teardown := startTestUAWithLogger(t, log)
	defer teardown()

	call := mgr.Create(session.DirectionOutbound)
	require.NotNil(t, call)
	target := "sip:1001@" + uasAddr
	require.NoError(t, sigState.StartOriginateCall(call, target, "", "", 30*time.Second, nil))

	require.Eventually(t, func() bool { return br.hasMethod("call.ringing") }, 3*time.Second, 10*time.Millisecond)

	uasCleanup() // drop peer before hangup so CANCEL cannot complete

	started := time.Now()
	require.NoError(t, mgr.Close(call.ID(), "test_cancel"))
	require.Eventually(t, func() bool { return br.hasMethod("call.hangup") },
		cancelAttemptTimeout+time.Second, 10*time.Millisecond)
	elapsed := time.Since(started)
	t.Logf("provisional cancel→hangup with CANCEL failure in %v", elapsed)
	require.Less(t, elapsed, cancelAttemptTimeout+time.Second,
		"CANCEL failure path must stay bounded by cancelAttemptTimeout")

	logs := logBuf.String()
	require.Contains(t, logs, "level=WARN")
	require.Contains(t, logs, "originate CANCEL failed")
	require.Contains(t, logs, call.ID())
	require.Contains(t, logs, "err=")
	require.Contains(t, logs, "elapsed=")
	require.True(t, strings.Contains(logs, "target=") && strings.Contains(logs, "dest="),
		"expected target and dest in CANCEL failure log:\n%s", logs)
}
