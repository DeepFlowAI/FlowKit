package sip

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// startTestUA spawns a flowkit UA + Signal at 127.0.0.1:<port>. Returns the
// listen URI, signal, manager, and teardown.
func startTestUA(t *testing.T) (string, *Signal, *session.Manager, *captureBroadcaster, func()) {
	t.Helper()
	return startTestUAWithLogger(t, quietLogger())
}

// startTestUAWithLogger is startTestUA with a custom Signal logger (for log assertions).
func startTestUAWithLogger(t *testing.T, log *slog.Logger) (string, *Signal, *session.Manager, *captureBroadcaster, func()) {
	t.Helper()
	if log == nil {
		log = quietLogger()
	}
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := New(Config{
		Listen:    listen,
		ExtIP:     "127.0.0.1",
		UserAgent: "flowkit-test",
		Logger:    quietLogger(),
	})
	require.NoError(t, err)

	mgr := session.NewManager(quietLogger())
	br := &captureBroadcaster{}
	sigState := NewSignal(ua, mgr, br, log)

	// UA owner: this helper starts Serve and must Stop on teardown.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = ua.Start(ctx) }()
	require.Eventually(t, func() bool { return ua.LocalAddr() != "" }, time.Second, 10*time.Millisecond)

	teardown := func() {
		ids := mgr.IDs()
		for _, id := range ids {
			_ = mgr.Close(id, "test_cleanup")
		}
		if len(ids) > 0 {
			// Hangup is async (CANCEL/BYE). Wait for session maps to clear,
			// then a short BYE round-trip when the peer answers
			// (startByeContact). Skipping this sleep when no calls were live
			// keeps trivial UA tests fast.
			deadline := time.Now().Add(byeTimeout)
			for time.Now().Before(deadline) {
				if mgr.Count() == 0 && sigState.ActiveCalls() == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
		_ = ua.Stop()
	}
	// Always register Cleanup so forgotten teardown cannot leave Start blocked.
	t.Cleanup(teardown)
	return listen, sigState, mgr, br, teardown
}

// startByeContact listens for BYE (and ACK) on a real UDP Contact so inbound
// hangup can complete WriteBye without waiting out the 2s Bye timeout.
func startByeContact(t *testing.T) (host string, port int, cleanup func()) {
	t.Helper()
	port = freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-bye-contact"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)
	srv.OnAck(func(_ *sip.Request, _ sip.ServerTransaction) {})
	srv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
		_ = tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
	})

	pc, err := net.ListenPacket("udp", listen)
	require.NoError(t, err)
	go func() { _ = srv.ServeUDP(pc) }()

	cleanup = func() {
		_ = srv.Close()
		_ = ua.Close()
		_ = pc.Close()
	}
	// Caller must defer cleanup AFTER UA/call teardown so BYE can land while
	// this socket is still listening (t.Cleanup LIFO would close us too early
	// if registered after startTestUA).
	return "127.0.0.1", port, cleanup
}

// newTestClient sets up a separate sipgo UA + Client for sending INVITEs.
func newTestClient(t *testing.T) (*sipgo.Client, func()) {
	t.Helper()
	clientUA, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-client"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	c, err := sipgo.NewClient(clientUA)
	require.NoError(t, err)
	return c, func() { _ = c.Close() }
}

// pcmuOffer constructs an offer SDP with PCMU on the given client-side port.
func pcmuOffer(localIP string, port int) string {
	return BuildAnswerPCMU(localIP, port, 12345)
}

// sendInviteACK completes the UAC side of a 2xx INVITE (RespondSDP waits for ACK).
func sendInviteACK(client *sipgo.Client, invite *sip.Request, res *sip.Response, dest string) error {
	recipient := invite.Recipient
	if contact := res.Contact(); contact != nil {
		recipient = contact.Address
	}
	ack := sip.NewRequest(sip.ACK, recipient)
	ack.SetDestination(dest)
	ack.SetTransport(invite.Transport())
	if h := invite.From(); h != nil {
		ack.AppendHeader(sip.HeaderClone(h))
	}
	if h := res.To(); h != nil {
		ack.AppendHeader(sip.HeaderClone(h))
	}
	if h := invite.CallID(); h != nil {
		ack.AppendHeader(sip.HeaderClone(h))
	}
	if h := invite.CSeq(); h != nil {
		ack.AppendHeader(sip.HeaderClone(h))
	}
	if cseq := ack.CSeq(); cseq != nil {
		cseq.MethodName = sip.ACK
	}
	if h := invite.Contact(); h != nil {
		ack.AppendHeader(sip.HeaderClone(h))
	}
	return client.WriteRequest(ack)
}

func TestSIP_E2E_INVITE_to_200OK(t *testing.T) {
	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, br, teardown := startTestUA(t)
	defer teardown()

	// Allocate a fake "remote" RTP port — the test never reads/writes RTP,
	// it just provides a real address so MediaIO doesn't reject the remote.
	clientRTP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer clientRTP.Close()
	clientRTPPort := clientRTP.LocalAddr().(*net.UDPAddr).Port

	client, closeClient := newTestClient(t)
	defer closeClient()

	// Build INVITE
	targetURI := sip.Uri{User: "flowkit", Host: "127.0.0.1"}
	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI.Host = host
	targetURI.Port = port

	invite := sip.NewRequest(sip.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(pcmuOffer("127.0.0.1", clientRTPPort)))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: contactHost, Port: contactPort},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := client.TransactionRequest(ctx, invite)
	require.NoError(t, err)
	defer tx.Terminate()

	// Wait for final response (200 OK expected)
	var got *sip.Response
loop:
	for {
		select {
		case res, ok := <-tx.Responses():
			if !ok {
				t.Fatal("response channel closed before 200")
			}
			if res.StatusCode >= 200 {
				got = res
				break loop
			}
			// Provisional (1xx): keep waiting
		case <-ctx.Done():
			t.Fatal("timed out waiting for 200 OK")
		}
	}
	require.Equal(t, 200, int(got.StatusCode), "expected 200 OK, got %d %s", got.StatusCode, got.Reason)

	require.NoError(t, sendInviteACK(client, invite, got, listenAddr))

	// Parse the answer SDP and assert structure
	answer, err := ParseSDP(string(got.Body()))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", answer.ConnectIP)
	require.True(t, answer.HasPCMU())
	require.NotZero(t, answer.AudioPort)

	// Verify the Signal has 1 active call
	require.Equal(t, 1, sigState.ActiveCalls())
	require.Equal(t, 1, mgr.Count())

	// Broadcast should have fired
	require.Eventually(t, func() bool { return br.Count() >= 1 }, time.Second, 20*time.Millisecond,
		"call.incoming notification not broadcast")
	require.Equal(t, "call.incoming", br.First().Method)

	// Hang up via the session manager (simulates SDK agent calling call.hangup)
	calls := mgr.IDs()
	require.Len(t, calls, 1)
	require.NoError(t, mgr.Close(calls[0], "test_hangup"))

	// Active count should drop to 0
	require.Eventually(t, func() bool { return sigState.ActiveCalls() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSIP_E2E_InboundHangup_SendsBYE(t *testing.T) {
	listenAddr, _, mgr, _, teardown := startTestUA(t)
	defer teardown()

	clientRTP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer clientRTP.Close()
	clientRTPPort := clientRTP.LocalAddr().(*net.UDPAddr).Port

	clientUA, err := sipgo.NewUA(
		sipgo.WithUserAgent("test-client"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	require.NoError(t, err)
	defer clientUA.Close()

	clientSrv, err := sipgo.NewServer(clientUA)
	require.NoError(t, err)
	defer clientSrv.Close()

	var byeReceived atomic.Int32
	clientSrv.OnBye(func(req *sip.Request, tx sip.ServerTransaction) {
		byeReceived.Add(1)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
	})

	contactPort := freePort(t)
	contactListen := "127.0.0.1:" + strconv.Itoa(contactPort)
	contactPC, err := net.ListenPacket("udp", contactListen)
	require.NoError(t, err)
	defer contactPC.Close()
	go func() { _ = clientSrv.ServeUDP(contactPC) }()

	client, err := sipgo.NewClient(clientUA)
	require.NoError(t, err)
	defer client.Close()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	invite := sip.NewRequest(sip.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(pcmuOffer("127.0.0.1", clientRTPPort)))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: "127.0.0.1", Port: contactPort},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := client.TransactionRequest(ctx, invite)
	require.NoError(t, err)
	defer tx.Terminate()

	var got *sip.Response
loop:
	for {
		select {
		case res, ok := <-tx.Responses():
			if !ok {
				t.Fatal("response channel closed before 200")
			}
			if res.StatusCode >= 200 {
				got = res
				break loop
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for 200 OK")
		}
	}
	require.Equal(t, 200, int(got.StatusCode))
	require.NoError(t, sendInviteACK(client, invite, got, listenAddr))

	calls := mgr.IDs()
	require.Len(t, calls, 1)
	require.NoError(t, mgr.Close(calls[0], "test_hangup"))

	require.Eventually(t, func() bool { return byeReceived.Load() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"flowkit must send BYE on inbound-call hangup")
}

func TestSIP_E2E_INVITE_PCMA_to_200OK(t *testing.T) {
	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, br, teardown := startTestUA(t)
	defer teardown()

	clientRTP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer clientRTP.Close()
	clientRTPPort := clientRTP.LocalAddr().(*net.UDPAddr).Port

	client, closeClient := newTestClient(t)
	defer closeClient()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	invite := sip.NewRequest(sip.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(BuildAnswerG711("127.0.0.1", clientRTPPort, 12345, 8, 0)))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: contactHost, Port: contactPort},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := client.TransactionRequest(ctx, invite)
	require.NoError(t, err)
	defer tx.Terminate()

	var got *sip.Response
loop:
	for {
		select {
		case res, ok := <-tx.Responses():
			if !ok {
				t.Fatal("response channel closed before 200")
			}
			if res.StatusCode >= 200 {
				got = res
				break loop
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for 200 OK")
		}
	}
	require.Equal(t, 200, int(got.StatusCode))

	require.NoError(t, sendInviteACK(client, invite, got, listenAddr))

	answer, err := ParseSDP(string(got.Body()))
	require.NoError(t, err)
	require.True(t, answer.HasPCMA())
	pt, ok := answer.NegotiatedG711()
	require.True(t, ok)
	require.Equal(t, 8, pt)

	require.Equal(t, 1, sigState.ActiveCalls())
	require.Eventually(t, func() bool { return br.Count() >= 1 }, time.Second, 20*time.Millisecond)

	calls := mgr.IDs()
	require.Len(t, calls, 1)
	require.NoError(t, mgr.Close(calls[0], "test_hangup"))
}

func TestSIP_E2E_INVITE_NoPCMU_Rejected488(t *testing.T) {
	listenAddr, _, _, _, teardown := startTestUA(t)
	defer teardown()

	client, closeClient := newTestClient(t)
	defer closeClient()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	// Offer Opus only (PT 96, no PCMU)
	offer := "v=0\r\no=test 1 1 IN IP4 127.0.0.1\r\ns=t\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 5004 RTP/AVP 96\r\na=rtpmap:96 opus/48000/2\r\n"

	invite := sip.NewRequest(sip.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(offer))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "test", Host: "127.0.0.1", Port: 5070}})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tx, err := client.TransactionRequest(ctx, invite)
	require.NoError(t, err)
	defer tx.Terminate()

	for {
		select {
		case res, ok := <-tx.Responses():
			if !ok {
				t.Fatal("response channel closed without final response")
			}
			if res.StatusCode >= 200 {
				require.Equal(t, 488, int(res.StatusCode), "expected 488 for non-PCMU offer")
				return
			}
		case <-ctx.Done():
			t.Fatal("timed out")
		}
	}
}

func TestSIP_E2E_RTPIdleReleasesInboundSlot(t *testing.T) {
	t.Setenv("SIP_RTP_IDLE_TIMEOUT", "300ms")

	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()

	clientRTP, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer clientRTP.Close()
	clientRTPPort := clientRTP.LocalAddr().(*net.UDPAddr).Port

	client, closeClient := newTestClient(t)
	defer closeClient()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	invite := sip.NewRequest(sip.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(pcmuOffer("127.0.0.1", clientRTPPort)))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{User: "test", Host: contactHost, Port: contactPort},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := client.TransactionRequest(ctx, invite)
	require.NoError(t, err)
	defer tx.Terminate()

	var got *sip.Response
loop:
	for {
		select {
		case res, ok := <-tx.Responses():
			if !ok {
				t.Fatal("response channel closed before 200")
			}
			if res.StatusCode >= 200 {
				got = res
				break loop
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for 200 OK")
		}
	}
	require.Equal(t, 200, int(got.StatusCode))
	require.NoError(t, sendInviteACK(client, invite, got, listenAddr))
	require.Equal(t, 1, sigState.ActiveCalls())

	require.Eventually(t, func() bool {
		return sigState.ActiveCalls() == 0 && mgr.Count() == 0
	}, 3*time.Second, 50*time.Millisecond, "rtp idle timeout should end call and release inbound slot")

	// Second INVITE must be accepted — would get 486 if the slot stayed busy.
	invite2 := sip.NewRequest(sip.INVITE, targetURI)
	invite2.SetTransport("udp")
	invite2.SetDestination(listenAddr)
	invite2.SetBody([]byte(pcmuOffer("127.0.0.1", clientRTPPort)))
	invite2.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite2.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{User: "test2", Host: contactHost, Port: contactPort},
	})

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	tx2, err := client.TransactionRequest(ctx2, invite2)
	require.NoError(t, err)
	defer tx2.Terminate()

	for {
		select {
		case res, ok := <-tx2.Responses():
			if !ok {
				t.Fatal("second invite: response channel closed before 200")
			}
			if res.StatusCode >= 200 {
				require.Equal(t, 200, int(res.StatusCode), "second invite should not be busy")
				calls := mgr.IDs()
				for _, id := range calls {
					_ = mgr.Close(id, "test_cleanup")
				}
				return
			}
		case <-ctx2.Done():
			t.Fatal("timed out waiting for second 200 OK")
		}
	}
}
