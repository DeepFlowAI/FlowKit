// Package sipclient places inbound SIP calls against a Flowkit UA.
package sipclient

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/sip"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
)

// HoldUntil returns when the SIP dialog should end (hold duration).
type HoldUntil struct {
	Done   <-chan struct{}
	MaxDur time.Duration
}

// PlaceInbound sends INVITE, ACKs 200, optionally pumps silence RTP, then BYE.
func PlaceInbound(ctx context.Context, listenAddr string, targetURI sipmsg.Uri, idx int, hold HoldUntil) error {
	clientUA, err := sipgo.NewUA(
		sipgo.WithUserAgent("flowkit-capacity"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	if err != nil {
		return fmt.Errorf("session %d: NewUA: %w", idx, err)
	}
	defer func() { _ = clientUA.Close() }()

	client, err := sipgo.NewClient(clientUA)
	if err != nil {
		return fmt.Errorf("session %d: NewClient: %w", idx, err)
	}
	defer func() { _ = client.Close() }()

	rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("session %d: rtp listen: %w", idx, err)
	}
	defer func() { _ = rtpPC.Close() }()
	rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

	offer := sip.BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx))
	contact := sipmsg.ContactHeader{
		Address: sipmsg.Uri{User: "cap", Host: "127.0.0.1", Port: 6000 + idx},
	}
	dialogCli := sipgo.NewDialogClientCache(client, contact)

	invite := sipmsg.NewRequest(sipmsg.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(offer))
	invite.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&contact)
	setCapacityCallID(invite)

	sess, err := dialogCli.WriteInvite(ctx, invite)
	if err != nil {
		return fmt.Errorf("session %d: invite: %w", idx, err)
	}
	defer func() { _ = sess.Close() }()

	if err := sess.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		return fmt.Errorf("session %d: wait answer: %w", idx, err)
	}
	if sess.InviteResponse == nil || sess.InviteResponse.StatusCode != 200 {
		code := 0
		if sess.InviteResponse != nil {
			code = sess.InviteResponse.StatusCode
		}
		return fmt.Errorf("session %d: status %d", idx, code)
	}
	if err := sess.Ack(ctx); err != nil {
		return fmt.Errorf("session %d: ack: %w", idx, err)
	}

	go pumpSilence(rtpPC, hold.MaxDur+2*time.Second)

	select {
	case <-time.After(hold.MaxDur):
	case <-hold.Done:
	case <-ctx.Done():
	}

	if err := sess.Bye(ctx); err != nil {
		return fmt.Errorf("session %d: bye: %w", idx, err)
	}
	return nil
}

// WaitRemoteHangup keeps the dialog up until ctx done or remote BYE (best-effort).
func WaitRemoteHangup(ctx context.Context, listenAddr string, targetURI sipmsg.Uri, idx int) error {
	clientUA, err := sipgo.NewUA(
		sipgo.WithUserAgent("flowkit-capacity"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	if err != nil {
		return err
	}
	defer func() { _ = clientUA.Close() }()

	srv, err := sipgo.NewServer(clientUA)
	if err != nil {
		return err
	}
	defer func() { _ = srv.Close() }()

	byeCh := make(chan struct{}, 1)
	srv.OnBye(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		_ = tx.Respond(sipmsg.NewResponseFromRequest(req, 200, "OK", nil))
		select {
		case byeCh <- struct{}{}:
		default:
		}
	})

	contactPort := 7000 + idx
	contactPC, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", contactPort))
	if err != nil {
		return err
	}
	defer func() { _ = contactPC.Close() }()
	go func() { _ = srv.ServeUDP(contactPC) }()

	client, err := sipgo.NewClient(clientUA)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = rtpPC.Close() }()
	rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

	offer := sip.BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx))
	contact := sipmsg.ContactHeader{
		Address: sipmsg.Uri{User: "cap", Host: "127.0.0.1", Port: contactPort},
	}
	dialogCli := sipgo.NewDialogClientCache(client, contact)

	invite := sipmsg.NewRequest(sipmsg.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(offer))
	invite.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&contact)
	setCapacityCallID(invite)

	sess, err := dialogCli.WriteInvite(ctx, invite)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()

	if err := sess.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		return err
	}
	if sess.InviteResponse == nil || sess.InviteResponse.StatusCode != 200 {
		code := 0
		if sess.InviteResponse != nil {
			code = sess.InviteResponse.StatusCode
		}
		return fmt.Errorf("session %d: status %d", idx, code)
	}
	if err := sess.Ack(ctx); err != nil {
		return err
	}
	go pumpSilence(rtpPC, 10*time.Minute)

	select {
	case <-byeCh:
		return nil
	case <-ctx.Done():
		_ = sess.Bye(context.Background())
		return ctx.Err()
	}
}

func setCapacityCallID(invite *sipmsg.Request) {
	invite.RemoveHeader("Call-ID")
	callID := sipmsg.CallIDHeader(uuid.NewString())
	invite.AppendHeader(&callID)
}

func pumpSilence(pc net.PacketConn, d time.Duration) {
	buf := make([]byte, 172)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		_, _ = pc.WriteTo(buf, pc.LocalAddr())
		time.Sleep(20 * time.Millisecond)
	}
}
