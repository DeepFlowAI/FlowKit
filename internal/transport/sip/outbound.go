package sip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Ring time before an unanswered originate is cancelled as no_answer. Mobile
// callees often pick up after 30s; a shorter limit cancels calls the callee is
// already answering.
const defaultOriginateTimeout = 60 * time.Second

// Max raw SIP response bytes kept in failure logs (supplier triage).
const maxSIPResponseLogBytes = 4096

// OriginateOptions controls an outbound INVITE.
type OriginateOptions struct {
	URI      string
	CallerID string
	TrunkID  string
	Timeout  time.Duration
	Headers  map[string]string
	routing  TrunkConfig // resolved outbound profile; auth creds + host live here
}

// StartOriginateCall begins an async outbound INVITE. The call must already
// exist in session.Manager (DirectionOutbound). RPC returns immediately with
// status "originating"; call.ringing / call.answered / call.hangup follow.
//
// trunkID is optional: when non-empty, the wired OutboundResolver MUST find
// it (otherwise ErrTrunkNotFound / ErrTrunkNotReady); when empty, the
// resolver's default (system_outbound) is preferred, falling back to the
// static SetTrunkConfig.
func (s *Signal) StartOriginateCall(call *session.Call, uri, callerID, trunkID string, timeout time.Duration, headers map[string]string) error {
	if call == nil {
		return errors.New("originate: nil call")
	}
	if timeout <= 0 {
		timeout = defaultOriginateTimeout
	}
	safeHeaders, err := FilterOriginateHeaders(headers)
	if err != nil {
		return err
	}
	targetURI, fromUser, routing, err := s.resolveOriginate(uri, callerID, trunkID)
	if err != nil {
		return err
	}
	call.SetParties(fromUser, targetURI.User)
	if err := call.SetState(session.StateRinging); err != nil {
		return err
	}

	opts := OriginateOptions{
		URI:      uri,
		CallerID: fromUser,
		TrunkID:  trunkID,
		Timeout:  timeout,
		Headers:  safeHeaders,
		routing:  routing,
	}
	go s.runOriginate(call, targetURI, fromUser, opts)
	return nil
}

func (s *Signal) runOriginate(call *session.Call, targetURI sip.Uri, fromUser string, opts OriginateOptions) {
	// WaitAnswer's default cancel path (inviteCancel) blocks on Timer B/F when
	// no provisional arrives or CANCEL cannot complete. Cancel with
	// WaitAnswerForceCancelErr so sipgo returns immediately and Terminates the
	// INVITE client transaction; we best-effort CANCEL separately if needed.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	ctx, span := tracer.Start(ctx, "sip.originate")
	defer span.End()
	span.SetAttributes(attribute.String("sip.target", targetURI.String()))

	var endedLocally atomic.Bool
	var timedOut atomic.Bool
	forceAbort := func() {
		cancel(sipgo.WaitAnswerForceCancelErr)
	}
	go func() {
		select {
		case <-call.Done():
			endedLocally.Store(true)
			forceAbort()
		case <-ctx.Done():
		}
	}()
	timer := time.AfterFunc(opts.Timeout, func() {
		timedOut.Store(true)
		forceAbort()
	})
	defer timer.Stop()

	target := targetURI.String()
	s.log.Info("originate started", "call_id", call.ID(), "target", target, "from", fromUser)

	pc, localPort, offerSDP, err := s.allocOriginateOffer()
	if err != nil {
		s.endOriginate(ctx, call, span, "alloc_failed", nil)
		return
	}

	invite := sip.NewRequest(sip.INVITE, targetURI)
	// SetBody already adds the Content-Length header; appending it again emits a
	// duplicate Content-Length, which strict carrier SBCs may silently drop.
	invite.SetBody([]byte(offerSDP))
	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	invite.SetTransport("udp")
	// The Request-URI always targets the far end (trunk or phone Contact);
	// the transport destination is redirected to a next-hop proxy when one
	// is configured. Trunk originates use outboundProxy (e.g. Kamailio
	// :5060); direct / extension originates (empty trunk_id) prefer
	// phoneOutboundProxy (e.g. Kamailio :5070) so INVITEs keep NAT affinity
	// with desk-phone REGISTER pinholes.
	dest := net.JoinHostPort(targetURI.Host, strconv.Itoa(targetURI.Port))
	proxy := s.outboundProxy
	if opts.TrunkID == "" && s.phoneOutboundProxy != "" {
		proxy = s.phoneOutboundProxy
	}
	if proxy != "" {
		dest = proxy
	}
	invite.SetDestination(dest)

	from := &sip.FromHeader{
		Address: sip.Uri{User: fromUser, Host: s.ua.ExtIP(), Port: s.contactPort},
		Params:  sip.NewParams(),
	}
	from.Params.Add("tag", sip.GenerateTagN(8))
	invite.AppendHeader(from)

	for k, v := range opts.Headers {
		invite.AppendHeader(sip.NewHeader(k, v))
	}

	dua := &sipgo.DialogUA{
		Client: s.ua.Client(),
		ContactHDR: sip.ContactHeader{
			Address: sip.Uri{User: "flowkit", Host: s.ua.ExtIP(), Port: s.contactPort},
		},
	}

	dialog, err := dua.WriteInvite(ctx, invite)
	if err != nil {
		_ = pc.Close()
		s.log.Warn("originate invite write failed",
			"call_id", call.ID(),
			"target", target,
			"from", fromUser,
			"dest", dest,
			"trunk_id", opts.TrunkID,
			"err", err,
		)
		s.endOriginate(ctx, call, span, "invite_failed", nil)
		return
	}

	sipCallID := ""
	if hdr := invite.CallID(); hdr != nil {
		sipCallID = hdr.Value()
	}

	var ringingSent, earlyMediaSent bool
	var mio *MediaIO
	var provisionalSDP string
	waitErr := dialog.WaitAnswer(ctx, sipgo.AnswerOptions{
		Username: opts.routing.User,
		Password: opts.routing.Pass,
		OnResponse: func(r *sip.Response) error {
			if call.IsEnded() {
				return errors.New("call ended")
			}
			switch int(r.StatusCode) {
			case sip.StatusRinging:
				if !ringingSent {
					ringingSent = true
					s.notifyOriginate("call.ringing", call.ID(), nil)
				}
			case sip.StatusSessionInProgress:
				if !earlyMediaSent {
					earlyMediaSent = true
					s.notifyOriginate("call.early_media", call.ID(), nil)
				}
			}
			if s.captureEarlyMedia.Load() &&
				int(r.StatusCode) >= sip.StatusRinging &&
				int(r.StatusCode) < sip.StatusOK &&
				len(r.Body()) > 0 &&
				string(r.Body()) != provisionalSDP {
				answer, err := ParseSDP(string(r.Body()))
				if err != nil {
					s.log.Info("ignoring provisional response with unusable SDP",
						"call_id", call.ID(), "sip_status", r.StatusCode, "err", err)
					return nil
				}
				pt, ok := answer.NegotiatedG711()
				if !ok {
					s.log.Info("ignoring provisional response without supported G.711",
						"call_id", call.ID(), "sip_status", r.StatusCode)
					return nil
				}
				remoteRTP := s.resolveOriginateRTP(answer, r.Source(), targetURI)
				dtmfPT := NegotiatedDTMFPT(answer)
				if mio == nil {
					earlyMIO, err := NewMediaIOForEarlyMedia(
						pc, remoteRTP, uint8(pt), dtmfPT, s.log, 0, s.ua.ExtIP(),
					)
					if err != nil {
						s.log.Warn("provisional media setup failed",
							"call_id", call.ID(), "sip_status", r.StatusCode, "err", err)
						return nil
					}
					earlyMIO.SetIdleTimeout(0)
					if err := call.BindMediaIO(earlyMIO); err != nil {
						_ = earlyMIO.Close()
						s.log.Warn("provisional media bind failed",
							"call_id", call.ID(), "sip_status", r.StatusCode, "err", err)
						return nil
					}
					WireDTMF(earlyMIO, call, s.broadcaster)
					mio = earlyMIO
				} else if err := mio.Reconfigure(remoteRTP, uint8(pt), dtmfPT); err != nil {
					s.log.Warn("provisional media reconfigure failed",
						"call_id", call.ID(), "sip_status", r.StatusCode, "err", err)
					return nil
				}
				provisionalSDP = string(r.Body())
				if !earlyMediaSent {
					earlyMediaSent = true
					s.notifyOriginate("call.early_media", call.ID(), nil)
				}
			}
			return nil
		},
	})

	if waitErr != nil || call.IsEnded() {
		if mio == nil {
			_ = pc.Close()
		}
		// ForceCancel skips sipgo's CANCEL; send one while the peer may still
		// be early-dialog so far-end ringing stops without blocking us on Timer F.
		s.bestEffortCancel(dialog, call.ID(), target)
		_ = dialog.Close()
		reason := originateFailReason(waitErr, dialog.InviteResponse, endedLocally.Load())
		if timedOut.Load() && !endedLocally.Load() {
			reason = "no_answer"
		}
		if waitErr != nil && dialog.InviteResponse == nil {
			s.log.Warn("originate wait failed without SIP response",
				"call_id", call.ID(),
				"target", target,
				"from", fromUser,
				"dest", dest,
				"trunk_id", opts.TrunkID,
				"err", waitErr,
			)
		}
		s.endOriginate(ctx, call, span, reason, dialog.InviteResponse)
		return
	}

	resp := dialog.InviteResponse
	if resp == nil || int(resp.StatusCode) != 200 {
		if mio == nil {
			_ = pc.Close()
		}
		_ = dialog.Close()
		s.endOriginate(ctx, call, span, "originate_failed", resp)
		return
	}

	answer, err := ParseSDP(string(resp.Body()))
	if err != nil {
		_ = dialog.Bye(ctx)
		if mio == nil {
			_ = pc.Close()
		}
		_ = dialog.Close()
		s.endOriginate(ctx, call, span, "bad_answer_sdp", resp)
		return
	}
	if !answer.HasPCMU() && !answer.HasPCMA() {
		_ = dialog.Bye(ctx)
		if mio == nil {
			_ = pc.Close()
		}
		_ = dialog.Close()
		s.endOriginate(ctx, call, span, "no_g711", resp)
		return
	}
	pt, ok := answer.NegotiatedG711()
	if !ok {
		_ = dialog.Bye(ctx)
		if mio == nil {
			_ = pc.Close()
		}
		_ = dialog.Close()
		s.endOriginate(ctx, call, span, "no_g711", resp)
		return
	}

	// A NAT'd phone advertises a private address in its 200 OK Contact. The
	// dialog ACK (and later BYE) target that Contact, so both vanish into the
	// LAN: the phone never sees the ACK, keeps retransmitting 200 OK, treats the
	// call as unconfirmed, and sends no RTP — a dead call with no audio.
	//
	// Direct (no proxy): rewrite to the public host:port the 200 OK arrived from.
	// Proxied (Kamailio): resp.Source() is 127.0.0.1 — do not use that. When
	// Contact is still private, rewrite to the originate Request-URI host
	// (REGISTER Contact / public NAT mapping). Record-Route still keeps ACK
	// on the proxy hop; after loose_route the R-URI must be publicly reachable.
	if contact := resp.Contact(); contact != nil {
		if host, port, ok := resolveOriginateContact(contact.Address.Host, contact.Address.Port, proxy, resp.Source(), targetURI); ok {
			s.log.Info("originate: rewriting NAT contact for ACK/BYE",
				"contact", net.JoinHostPort(contact.Address.Host, strconv.Itoa(contact.Address.Port)),
				"rewrite", net.JoinHostPort(host, strconv.Itoa(port)),
				"proxy", proxy != "")
			contact.Address.Host = host
			contact.Address.Port = port
		}
	}

	if err := dialog.Ack(ctx); err != nil {
		if mio == nil {
			_ = pc.Close()
		}
		_ = dialog.Close()
		s.endOriginate(ctx, call, span, "ack_failed", resp)
		return
	}

	remoteRTP := s.resolveOriginateRTP(answer, resp.Source(), targetURI)
	dtmfPT := NegotiatedDTMFPT(answer)
	if mio == nil {
		mio, err = NewMediaIO(pc, remoteRTP, uint8(pt), dtmfPT, s.log, 0, s.ua.ExtIP())
		if err != nil {
			_ = pc.Close()
			_ = dialog.Bye(ctx)
			_ = dialog.Close()
			s.endOriginate(ctx, call, span, "mediaio_failed", resp)
			return
		}
		if err := call.BindMediaIO(mio); err != nil {
			_ = mio.Close()
			_ = dialog.Bye(ctx)
			_ = dialog.Close()
			s.endOriginate(ctx, call, span, "bind_failed", resp)
			return
		}
		WireDTMF(mio, call, s.broadcaster)
	} else {
		if err := mio.Reconfigure(remoteRTP, uint8(pt), dtmfPT); err != nil {
			_ = dialog.Bye(ctx)
			_ = dialog.Close()
			s.endOriginate(ctx, call, span, "mediaio_failed", resp)
			return
		}
		mio.SetIdleTimeout(rtpIdleTimeout())
	}
	_ = call.SetState(session.StateAnswered)
	_ = call.SetState(session.StateActive)

	if sipCallID != "" {
		s.mu.Lock()
		s.calls[sipCallID] = &callState{
			flowCall:  call,
			mediaIO:   mio,
			dialog:    dialog,
			g711PT:    pt,
			dtmfPT:    dtmfPT,
			localPort: localPort,
		}
		s.mu.Unlock()
	}

	go s.watchOutboundCall(sipCallID, call, dialog, mio)

	observability.CallTraceCall(ctx, s.log, "outbound.answered", call,
		"target", target,
		"transport", "sip",
	)
	s.notifyOriginate("call.answered", call.ID(), map[string]any{
		"direction": "outbound",
		"transport": "sip",
		"to":        targetURI.User,
		"from":      fromUser,
	})
	s.log.Info("outbound call established", "call_id", call.ID(), "target", target)
}

func (s *Signal) allocOriginateOffer() (net.PacketConn, int, string, error) {
	pc, err := s.ua.AllocRTPPort()
	if err != nil {
		return nil, 0, "", fmt.Errorf("originate: alloc rtp: %w", err)
	}
	localPort := pc.LocalAddr().(*net.UDPAddr).Port
	offerSDP := BuildAnswerPCMU(s.ua.ExtIP(), localPort, time.Now().UnixNano())
	return pc, localPort, offerSDP, nil
}

func (s *Signal) watchOutboundCall(sipCallID string, call *session.Call, dialog *sipgo.DialogClientSession, mio *MediaIO) {
	<-call.Done()

	// RTP idle timeout ends the Call without a peer SIP BYE. Mirror
	// remote_bye so controllers still receive call.hangup (keepalive
	// bridges otherwise suppress the peer-gone broadcast).
	s.broadcastMediaEOFHangup(call, mio)

	if sipCallID != "" {
		s.mu.Lock()
		delete(s.calls, sipCallID)
		s.mu.Unlock()
	}

	// call.Context is already cancelled; WithoutCancel keeps Values while the
	// short timeout still bounds an unresponsive peer.
	byeCtx, cancel := context.WithTimeout(context.WithoutCancel(call.Context()), byeTimeout)
	_ = dialog.Bye(byeCtx)
	cancel()
	_ = dialog.Close()
	_ = mio.Close()
}

// bestEffortCancel sends CANCEL after WaitAnswer ForceCancel when a provisional
// was seen. Bounded so a missing 200 to CANCEL cannot stall runOriginate.
// Failures are Warn: local call is already torn down while the peer may keep
// ringing — the only breadcrumb for "phone still ringing" complaints.
func (s *Signal) bestEffortCancel(dialog *sipgo.DialogClientSession, callID, target string) {
	if dialog == nil || dialog.InviteRequest == nil {
		return
	}
	invite := dialog.InviteRequest
	dest := invite.Destination()
	resp := dialog.InviteResponse
	if resp == nil || !resp.IsProvisional() {
		// RFC 3261: CANCEL only after a provisional. Peer INVITE tx runs to Timer B.
		inviteStatus := 0
		if resp != nil {
			inviteStatus = int(resp.StatusCode)
		}
		s.log.Info("originate skip CANCEL: no provisional response",
			"call_id", callID,
			"target", target,
			"dest", dest,
			"invite_status", inviteStatus,
		)
		return
	}
	cancelReq := sip.NewRequest(sip.CANCEL, invite.Recipient)
	if via := invite.Via(); via != nil {
		cancelReq.AppendHeader(sip.HeaderClone(via))
	}
	if h := invite.From(); h != nil {
		cancelReq.AppendHeader(sip.HeaderClone(h))
	}
	if h := invite.To(); h != nil {
		cancelReq.AppendHeader(sip.HeaderClone(h))
	}
	if h := invite.CallID(); h != nil {
		cancelReq.AppendHeader(sip.HeaderClone(h))
	}
	sip.CopyHeaders("Route", invite, cancelReq)
	cancelReq.SetTransport(invite.Transport())
	if dest != "" {
		cancelReq.SetDestination(dest)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelAttemptTimeout)
	defer cancel()
	started := time.Now()
	cancelResp, err := dialog.Do(ctx, cancelReq)
	elapsed := time.Since(started)
	attrs := []any{
		"call_id", callID,
		"target", target,
		"dest", dest,
		"elapsed", elapsed,
	}
	if err != nil {
		s.log.Warn("originate CANCEL failed", append(attrs, "err", err)...)
		return
	}
	status := 0
	reason := ""
	if cancelResp != nil {
		status = int(cancelResp.StatusCode)
		reason = cancelResp.Reason
	}
	attrs = append(attrs, "sip_status", status, "sip_reason", reason)
	if status != sip.StatusOK {
		s.log.Warn("originate CANCEL failed", attrs...)
		return
	}
	s.log.Info("originate CANCEL sent", attrs...)
}

func (s *Signal) endOriginate(ctx context.Context, call *session.Call, span trace.Span, reason string, resp *sip.Response) {
	if !call.IsEnded() {
		_ = s.mgr.Close(call.ID(), reason)
	}
	sipStatus := 0
	if resp != nil {
		sipStatus = int(resp.StatusCode)
	}
	traceAttrs := []any{"reason", reason, "sip_status", sipStatus}
	if resp != nil {
		traceAttrs = append(traceAttrs,
			"sip_reason", resp.Reason,
			"sip_source", resp.Source(),
		)
		if h := headerValue(resp, "Warning"); h != "" {
			traceAttrs = append(traceAttrs, "sip_warning", h)
		}
		if h := headerValue(resp, "Reason"); h != "" {
			traceAttrs = append(traceAttrs, "sip_reason_hdr", h)
		}
	}
	observability.CallTraceCall(ctx, s.log, "outbound.failed", call, traceAttrs...)

	params := map[string]any{"call_id": call.ID(), "reason": reason}
	if sipStatus > 0 {
		params["sip_status"] = sipStatus
	}
	if resp != nil {
		if resp.Reason != "" {
			params["sip_reason"] = resp.Reason
		}
		if h := headerValue(resp, "Warning"); h != "" {
			params["sip_warning"] = h
		}
		if h := headerValue(resp, "Reason"); h != "" {
			params["sip_reason_hdr"] = h
		}
		if h := headerValue(resp, "Call-ID"); h != "" {
			params["sip_call_id"] = h
		}
	}
	s.notifyOriginate("call.hangup", call.ID(), params)
	span.SetStatus(codes.Error, reason)

	logAttrs := []any{
		"call_id", call.ID(),
		"reason", reason,
		"sip_status", sipStatus,
		"component", "sip-signal",
	}
	logAttrs = append(logAttrs, sipResponseLogAttrs(resp)...)
	if isNormalOriginateOutcome(reason) {
		// Callee busy / no-answer / local cancel are expected dialer outcomes —
		// Info, not Warn (Warn is reserved for setup failures).
		s.log.Info("outbound call ended unanswered", logAttrs...)
	} else {
		s.log.Warn("originate failed", logAttrs...)
	}
}

// sipResponseLogAttrs extracts supplier-facing SIP response detail for logs
// (status line, key headers, truncated raw message). Safe with resp == nil.
func sipResponseLogAttrs(resp *sip.Response) []any {
	if resp == nil {
		return nil
	}
	attrs := []any{
		"sip_reason", resp.Reason,
		"sip_source", resp.Source(),
		"sip_start_line", resp.StartLine(),
	}
	for _, name := range []string{
		"Warning", "Reason", "Call-ID", "From", "To", "Contact",
		"Server", "User-Agent", "Via",
	} {
		if v := headerValue(resp, name); v != "" {
			key := "sip_hdr_" + strings.ToLower(strings.ReplaceAll(name, "-", "_"))
			attrs = append(attrs, key, v)
		}
	}
	raw := resp.String()
	if len(raw) > maxSIPResponseLogBytes {
		raw = raw[:maxSIPResponseLogBytes] + "...(truncated)"
	}
	attrs = append(attrs, "sip_response", raw)
	return attrs
}

func headerValue(resp *sip.Response, name string) string {
	if resp == nil {
		return ""
	}
	if h := resp.GetHeader(name); h != nil {
		return h.Value()
	}
	return ""
}

func originateFailReason(waitErr error, resp *sip.Response, localCancel bool) string {
	if localCancel {
		return "local_cancel"
	}
	if resp != nil {
		switch int(resp.StatusCode) {
		case sip.StatusBusyHere, 600:
			return "busy"
		case 487:
			return "local_cancel"
		case sip.StatusRequestTimeout, sip.StatusTemporarilyUnavailable, 410:
			return "no_answer"
		case 403, 404, 603:
			return "rejected"
		}
		if int(resp.StatusCode) >= 500 {
			return "originate_failed"
		}
		if int(resp.StatusCode) >= 400 {
			return "rejected"
		}
	}
	if waitErr != nil {
		if errors.Is(waitErr, context.DeadlineExceeded) {
			return "no_answer"
		}
		if errors.Is(waitErr, context.Canceled) {
			return "local_cancel"
		}
	}
	return "originate_failed"
}

// normalOriginateOutcomes are callee/local dispositions that end an outbound
// attempt without answer. Expected in dialer traffic; logged at Info.
//
// "rejected" is deliberately excluded: it is the catch-all bucket for every
// unmatched 4xx, so it also covers trunk-level faults (403 auth/caller-ID
// rejected, 407 proxy auth, 488 codec mismatch). Silencing it would hide a
// broken trunk, where every single call fails the same way.
var normalOriginateOutcomes = map[string]bool{
	"busy":         true,
	"no_answer":    true,
	"local_cancel": true,
}

func isNormalOriginateOutcome(reason string) bool {
	return normalOriginateOutcomes[reason]
}

func (s *Signal) notifyOriginate(method, callID string, extra map[string]any) {
	if s.broadcaster == nil {
		return
	}
	payload := map[string]any{"call_id": callID}
	for k, v := range extra {
		payload[k] = v
	}
	n, err := rpc.NewNotification(method, payload)
	if err != nil {
		return
	}
	s.broadcaster.BroadcastNotification(n)
}

// Originate sends INVITE and blocks until 200 OK. Used by Go-level tests.
func (s *Signal) Originate(ctx context.Context, target string) (*session.Call, error) {
	call := s.mgr.Create(session.DirectionOutbound)
	if call == nil {
		return nil, session.ErrShuttingDown
	}
	if err := s.StartOriginateCall(call, target, "", "", defaultOriginateTimeout, nil); err != nil {
		_ = s.mgr.Close(call.ID(), "originate_failed")
		return nil, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultOriginateTimeout)
	}
	for {
		if call.State() == session.StateActive {
			return call, nil
		}
		if call.IsEnded() {
			return nil, fmt.Errorf("originate: call ended before answer")
		}
		if time.Now().After(deadline) {
			_ = s.mgr.Close(call.ID(), "timeout")
			return nil, context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			_ = s.mgr.Close(call.ID(), "canceled")
			return nil, ctx.Err()
		case <-call.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// isPrivateSDPHost reports whether an SDP-advertised media host is one we can't
// reach directly — a NAT'd peer that put a private/loopback/unspecified/
// link-local address in its SDP. A hostname or empty string is trusted as-is.
func isPrivateSDPHost(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast()
}

// resolveOriginateRTP applies the same NAT fallback to provisional and final
// SDP so one MediaIO can span the whole outbound INVITE lifecycle.
func (s *Signal) resolveOriginateRTP(answer *SDP, responseSource string, target sip.Uri) string {
	sipSource := ""
	if h, _, err := net.SplitHostPort(responseSource); err == nil {
		sipSource = h
	}
	remoteHost, reason := resolveOriginateMediaHost(answer.ConnectIP, sipSource, target.Host)
	if reason != "" {
		s.log.Info("originate: SDP media host is private; using reachable host",
			"sdp_host", answer.ConnectIP,
			"sip_source", sipSource,
			"target_host", target.Host,
			"media_host", remoteHost,
			"reason", reason,
		)
	}
	return net.JoinHostPort(remoteHost, strconv.Itoa(answer.AudioPort))
}

// resolveOriginateContact picks a publicly reachable Contact host:port for
// ACK/BYE after a NAT'd phone answers. ok is false when no rewrite is needed.
func resolveOriginateContact(contactHost string, contactPort int, proxy, respSource string, target sip.Uri) (string, int, bool) {
	if contactHost == "" {
		return "", 0, false
	}
	if proxy == "" {
		host, portStr, err := net.SplitHostPort(respSource)
		if err != nil || host == "" {
			return "", 0, false
		}
		port, perr := strconv.Atoi(portStr)
		if perr != nil {
			return "", 0, false
		}
		if contactHost == host && contactPort == port {
			return "", 0, false
		}
		return host, port, true
	}
	if !isPrivateSDPHost(contactHost) {
		return "", 0, false
	}
	if target.Host == "" || isPrivateSDPHost(target.Host) {
		return "", 0, false
	}
	port := target.Port
	if port <= 0 {
		port = 5060
	}
	if contactHost == target.Host && contactPort == port {
		return "", 0, false
	}
	return target.Host, port, true
}

// resolveOriginateMediaHost picks the IP we should send RTP to when the remote
// SDP c= line is private. Returns (host, reason) where reason is empty when
// the SDP host is already usable.
func resolveOriginateMediaHost(sdpHost, sipSource, targetHost string) (string, string) {
	if !isPrivateSDPHost(sdpHost) {
		return sdpHost, ""
	}
	if sipSource != "" && !isPrivateSDPHost(sipSource) {
		return sipSource, "sip_source"
	}
	if targetHost != "" && !isPrivateSDPHost(targetHost) {
		return targetHost, "target_host"
	}
	if sipSource != "" {
		return sipSource, "sip_source_fallback"
	}
	return sdpHost, "sdp_unchanged"
}

// parseTargetURI accepts "sip:user@host:port" or "user@host" (defaults to
// sip: + port 5060).
func parseTargetURI(target string) (sip.Uri, error) {
	if target == "" {
		return sip.Uri{}, errors.New("sip: empty target")
	}
	body := target
	if len(body) >= 4 && body[:4] == "sip:" {
		body = body[4:]
	}
	userHost := body
	user := ""
	if at := indexOf(userHost, '@'); at >= 0 {
		user = userHost[:at]
		userHost = userHost[at+1:]
	}
	// Drop URI parameters (;transport=udp, ;lr…) and headers (?…). A Contact URI
	// recorded from a REGISTER carries ;transport, which must not be parsed as
	// part of the port (e.g. "host:29767;transport=udp").
	if semi := indexOf(userHost, ';'); semi >= 0 {
		userHost = userHost[:semi]
	}
	if q := indexOf(userHost, '?'); q >= 0 {
		userHost = userHost[:q]
	}
	host := userHost
	port := 5060
	if c := indexOf(userHost, ':'); c >= 0 {
		host = userHost[:c]
		p, err := strconv.Atoi(userHost[c+1:])
		if err != nil {
			return sip.Uri{}, fmt.Errorf("sip: bad port %q in %q", userHost[c+1:], target)
		}
		port = p
	}
	if host == "" {
		return sip.Uri{}, fmt.Errorf("sip: missing host in %q", target)
	}
	return sip.Uri{User: user, Host: host, Port: port}, nil
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
