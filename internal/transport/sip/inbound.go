package sip

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("flowkit/sip")

// Broadcaster is the subset of rpc.Server we need so the SIP layer can fan
// out call.incoming to every connected SDK agent.
type Broadcaster interface {
	BroadcastNotification(*rpc.Notification)
}

// Signal binds inbound INVITE / BYE to the session.Manager + Broadcaster
// pair. It maintains a SIP-Call-ID → session.Call map so BYEs and ACKs can
// be routed correctly.
type Signal struct {
	ua          *UA
	mgr         *session.Manager
	broadcaster Broadcaster
	dialogSrv   *sipgo.DialogServerCache
	log         *slog.Logger
	trunk       TrunkConfig
	contactPort int
	// outboundProxy, when set, is the transport-layer UDP destination for
	// trunk outbound INVITEs (e.g. Kamailio :5060). The Request-URI still
	// targets the trunk; empty sends directly to the trunk host:port.
	outboundProxy string
	// phoneOutboundProxy, when set, is used instead of outboundProxy for
	// direct / extension originates (empty trunk_id) so INVITEs leave via the
	// phone-facing proxy port (e.g. Kamailio :5070) and keep NAT affinity
	// with desk-phone REGISTER pinholes.
	phoneOutboundProxy string
	captureEarlyMedia  atomic.Bool

	mu                sync.Mutex
	calls             map[string]*callState // SIP Call-ID → state
	trunkResolver     TrunkResolver
	outboundResolver  OutboundResolverFn
	rejectUnknownPeer bool
	blockedPeers      map[string]struct{}
	toNormalizer      func(string) string

	onAccepted []func(callID, from string)
}

// SetCaptureEarlyMedia enables outbound provisional SDP/RTP consumption.
// It is tied to recording policy so deployments that do not retain pre-answer
// media preserve the existing signaling-only ringing behaviour.
func (s *Signal) SetCaptureEarlyMedia(enabled bool) {
	s.captureEarlyMedia.Store(enabled)
}

// SetToNormalizer installs an optional normalizer applied to the SIP To
// user-part before it is broadcast in `call.incoming`. Pass nil to disable.
func (s *Signal) SetToNormalizer(fn func(string) string) {
	s.mu.Lock()
	s.toNormalizer = fn
	s.mu.Unlock()
}

// SetOutboundProxy wires a next-hop SIP proxy (e.g. Kamailio) that trunk
// outbound INVITEs are sent to at the transport layer, without changing the
// Request-URI. Pass "" (the default) to send directly to the trunk.
func (s *Signal) SetOutboundProxy(addr string) {
	s.outboundProxy = addr
}

// SetPhoneOutboundProxy wires the proxy used for direct / extension
// originates (empty trunk_id). Pass "" to fall back to SetOutboundProxy /
// direct send.
func (s *Signal) SetPhoneOutboundProxy(addr string) {
	s.phoneOutboundProxy = addr
}

// SetOnAccepted registers callbacks invoked after a SIP call is ACK'd and
// call.incoming has been broadcast. Used by the kernel LLM auto-agent.
func (s *Signal) SetOnAccepted(fn func(callID, from string)) {
	s.mu.Lock()
	s.onAccepted = append(s.onAccepted, fn)
	s.mu.Unlock()
}

func (s *Signal) fireAccepted(callID, from string) {
	s.mu.Lock()
	cbs := make([]func(string, string), len(s.onAccepted))
	copy(cbs, s.onAccepted)
	s.mu.Unlock()
	for _, fn := range cbs {
		fn(callID, from)
	}
}

type callState struct {
	flowCall *session.Call
	mediaIO  *MediaIO

	// dialog is non-nil for both inbound and outbound calls so the cleanup
	// goroutine can send BYE when flowkit initiates hangup.
	dialog dialogCloser

	// media negotiation snapshot for SIP hold re-INVITE.
	g711PT    int
	dtmfPT    uint8
	localPort int

	// trunkID set on inbound INVITE when the source IP matched a Catalog
	// trunk; empty when no resolver is wired or no match (still accepted
	// unless Signal.rejectUnknownPeer is true).
	trunkID string
	// toUser carries the SIP To user-part as broadcast in call.incoming.
	toUser string
	// callerExtension is the extension that placed the call, set only when the
	// INVITE came from one of our registered phones and empty for a call off a
	// carrier trunk. It is what lets the layer above treat an agent dialling
	// from their handset as an outbound call rather than a customer calling in.
	callerExtension string
}

// dialogCloser is the slice of *sipgo.DialogClientSession we care about.
// It's an interface so inbound.go does not import sipgo.
type dialogCloser interface {
	Bye(ctx context.Context) error
	Close() error
}

// NewSignal wires INVITE / BYE / ACK handlers on the UA. Start the UA after
// construction.
func NewSignal(ua *UA, mgr *session.Manager, broadcaster Broadcaster, log *slog.Logger) *Signal {
	if log == nil {
		log = slog.Default()
	}
	sipPort := ua.cfg.ContactPort
	if sipPort == 0 {
		sipPort = contactPort(ua.cfg.Listen)
	}
	s := &Signal{
		ua:          ua,
		mgr:         mgr,
		broadcaster: broadcaster,
		dialogSrv: sipgo.NewDialogServerCache(ua.Client(), sip.ContactHeader{
			Address: sip.Uri{User: "flowkit", Host: ua.ExtIP(), Port: sipPort},
		}),
		log:         log.With("component", "sip-signal"),
		contactPort: sipPort,
		calls:       make(map[string]*callState),
	}
	ua.OnIncomingCall(s.handleInvite)
	ua.OnBye(s.handleBye)
	ua.OnAck(s.handleAck)
	ua.OnCancel(s.handleCancel)
	return s
}

func contactPort(listen string) int {
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return 5060
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return 5060
	}
	return port
}

// ActiveCalls returns the number of in-flight SIP calls.
func (s *Signal) ActiveCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// Shutdown hangs up every active SIP call.
func (s *Signal) Shutdown() {
	s.mu.Lock()
	calls := s.calls
	s.calls = make(map[string]*callState)
	s.mu.Unlock()
	for _, cs := range calls {
		_ = cs.flowCall.Hangup("server_stopping")
	}
}

func (s *Signal) atInboundCapacityLocked() bool {
	max := maxInboundCalls()
	return max > 0 && len(s.calls) >= max
}

// purgeEndedCalls removes SIP map entries whose session call already ended.
// This recovers from races where call.Done cleanup has not run yet.
func (s *Signal) purgeEndedCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sipCallID, cs := range s.calls {
		if !cs.flowCall.IsEnded() {
			continue
		}
		delete(s.calls, sipCallID)
		s.log.Warn("purged stale sip call entry", "sip_call_id", sipCallID, "call_id", cs.flowCall.ID())
	}
}

func (s *Signal) watchInboundCallLifetime(sipCallID string, call *session.Call, max time.Duration) {
	if max <= 0 {
		return
	}
	timer := time.NewTimer(max)
	defer timer.Stop()
	select {
	case <-call.Done():
		return
	case <-timer.C:
		if call.IsEnded() {
			return
		}
		s.log.Warn("inbound call max duration exceeded",
			"sip_call_id", sipCallID,
			"call_id", call.ID(),
			"max", max,
		)
		_ = s.mgr.Close(call.ID(), "max_duration")
	}
}

// --- handlers ---

func (s *Signal) handleInvite(inc IncomingCall) {
	req := inc.Request
	tx := inc.Transaction

	ctx, span := tracer.Start(context.Background(), "sip.invite_inbound")
	defer span.End()

	callIDHdr := req.CallID()
	if callIDHdr == nil {
		span.SetStatus(codes.Error, "missing_call_id")
		s.reject(tx, req, 400, "Missing Call-ID")
		return
	}
	sipCallID := callIDHdr.Value()
	span.SetAttributes(
		attribute.String("sip.call_id", sipCallID),
		attribute.String("sip.from", stripURI(req.From().Address.String())),
		attribute.String("sip.to", stripURI(req.To().Address.String())),
	)
	_ = ctx

	peerSource := effectivePeerSource(req)
	if peerHost := peerHostFromSource(peerSource); s.isBlockedPeer(peerHost) {
		s.log.Warn("inbound rejected: blocked peer",
			"peer", peerHost,
			"source", req.Source(),
			"peer_source", peerSource,
			"sip_call_id", sipCallID,
		)
		s.reject(tx, req, 403, "Forbidden")
		return
	}

	s.mu.Lock()
	if cs, exists := s.calls[sipCallID]; exists {
		s.mu.Unlock()
		// In-dialog re-INVITE (session timer refresh / remote hold). Must
		// answer 200 OK or peers with Session-Expires drop the call.
		if isInDialogInvite(req) {
			s.handleReInvite(cs, req, tx)
			return
		}
		s.log.Debug("duplicate invite ignored", "sip_call_id", sipCallID)
		return
	}
	s.mu.Unlock()

	trunkID, peerMatched := s.resolveInboundTrunk(peerSource)
	// One of our own phones dialling out is a known peer even though its
	// address matches no trunk in the catalog. Without this, locking the
	// kernel down to catalog trunks would reject every call agents place from
	// their handsets.
	callerExtension := s.resolveCallerExtension(req, peerSource)
	if callerExtension != "" {
		s.log.Info("inbound from registered extension",
			"extension", callerExtension,
			"sip_call_id", sipCallID,
			"peer_source", peerSource,
			"to", toUserPart(req),
		)
	}
	if !peerMatched && callerExtension == "" && s.rejectUnknownPeer {
		s.log.Warn("inbound rejected: unknown peer",
			"sip_call_id", sipCallID,
			"source", req.Source(),
			"peer_source", peerSource,
		)
		s.reject(tx, req, 403, "Forbidden")
		return
	}
	if trunkID != "" {
		span.SetAttributes(attribute.String("sip.trunk_id", trunkID))
	}

	dlg, err := s.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		s.log.Warn("sip dialog setup failed", "sip_call_id", sipCallID, "err", err)
		return
	}

	body := string(req.Body())
	sdpOffer, err := ParseSDP(body)
	if err != nil {
		s.log.Debug("bad sdp", "err", err, "sip_call_id", sipCallID)
		s.rejectDialog(dlg, tx, req, 400, "Bad SDP")
		return
	}
	pt, ok := sdpOffer.NegotiatedG711()
	if !ok {
		s.rejectDialog(dlg, tx, req, 488, "No G.711 codec")
		return
	}

	if !isPrimaryTrunkLeg(sipCallID, pt) {
		s.log.Debug("sip invite rejected", "code", 486, "reason", "Probe leg", "sip_call_id", sipCallID, "codec_pt", pt)
		s.rejectDialog(dlg, tx, req, 486, "Busy Here")
		return
	}

	s.purgeEndedCalls()

	s.mu.Lock()
	if _, exists := s.calls[sipCallID]; exists {
		s.mu.Unlock()
		s.log.Debug("duplicate invite ignored", "sip_call_id", sipCallID)
		s.rejectDialog(dlg, tx, req, 486, "Busy Here")
		return
	}
	if s.atInboundCapacityLocked() {
		s.mu.Unlock()
		s.log.Info("sip invite rejected", "code", 486, "reason", "At capacity", "sip_call_id", sipCallID)
		s.rejectDialog(dlg, tx, req, 486, "Busy Here")
		return
	}
	s.mu.Unlock()

	pc, err := s.ua.AllocRTPPort()
	if err != nil {
		s.log.Warn("rtp port alloc failed", "err", err, "sip_call_id", sipCallID)
		s.rejectDialog(dlg, tx, req, 500, "RTP exhausted")
		return
	}
	remoteRTP := net.JoinHostPort(sdpOffer.ConnectIP, strconv.Itoa(sdpOffer.AudioPort))
	mio, err := NewMediaIO(pc, remoteRTP, uint8(pt), NegotiatedDTMFPT(sdpOffer), s.log, 0, s.ua.ExtIP())
	if err != nil {
		_ = pc.Close()
		s.rejectDialog(dlg, tx, req, 500, "MediaIO setup failed")
		return
	}

	call := s.mgr.Create(session.DirectionInbound)
	if call == nil {
		_ = mio.Close()
		s.rejectDialog(dlg, tx, req, 503, "Shutting Down")
		return
	}
	call.SetParties(req.From().Address.String(), req.To().Address.String())
	if err := call.BindMediaIO(mio); err != nil {
		_ = mio.Close()
		_ = s.mgr.Close(call.ID(), "bind_failed")
		s.rejectDialog(dlg, tx, req, 500, "Bind failed")
		return
	}
	WireDTMF(mio, call, s.broadcaster)

	localPort := pc.LocalAddr().(*net.UDPAddr).Port

	s.mu.Lock()
	if _, exists := s.calls[sipCallID]; exists {
		s.mu.Unlock()
		_ = mio.Close()
		_ = s.mgr.Close(call.ID(), "duplicate_invite")
		s.rejectDialog(dlg, tx, req, 486, "Busy Here")
		return
	}
	if s.atInboundCapacityLocked() {
		s.mu.Unlock()
		_ = mio.Close()
		_ = s.mgr.Close(call.ID(), "at_capacity")
		s.rejectDialog(dlg, tx, req, 486, "Busy Here")
		return
	}
	s.calls[sipCallID] = &callState{
		flowCall: call, mediaIO: mio, dialog: dlg,
		g711PT: pt, dtmfPT: NegotiatedDTMFPT(sdpOffer), localPort: localPort,
		trunkID:         trunkID,
		toUser:          toUserPart(req),
		callerExtension: callerExtension,
	}
	s.mu.Unlock()

	_ = call.SetState(session.StateRinging)

	go s.watchInboundCallLifetime(sipCallID, call, inboundMaxDuration())

	go func(serverDlg *sipgo.DialogServerSession) {
		<-call.Done()
		// RTP idle timeout ends the Call without a peer SIP BYE. Mirror
		// remote_bye so controllers still receive call.hangup (keepalive
		// bridges otherwise suppress the peer-gone broadcast).
		s.broadcastMediaEOFHangup(call, mio)
		s.mu.Lock()
		delete(s.calls, sipCallID)
		s.mu.Unlock()

		if serverDlg != nil {
			// call.Context is already cancelled; WithoutCancel keeps Values.
			byeCtx, cancel := context.WithTimeout(context.WithoutCancel(call.Context()), byeTimeout)
			_ = serverDlg.Bye(byeCtx)
			cancel()
			_ = serverDlg.Close()
		}
		_ = mio.Close()
	}(dlg)

	if err := dlg.Respond(sip.StatusRinging, "Ringing", nil); err != nil {
		s.log.Warn("sip 180 failed", "err", err, "sip_call_id", sipCallID)
		_ = mio.Close()
		_ = s.mgr.Close(call.ID(), "ringing_failed")
		s.mu.Lock()
		delete(s.calls, sipCallID)
		s.mu.Unlock()
		return
	}

	answer := BuildAnswerG711(s.ua.ExtIP(), localPort, time.Now().UnixNano(), pt, sdpOffer.TelephoneEventPT())

	s.log.Info("sip answering", "call_id", call.ID(), "sip_call_id", sipCallID, "codec_pt", pt)

	s.completeAnswer(dlg, call, sipCallID, mio, answer)
}

func (s *Signal) completeAnswer(dlg *sipgo.DialogServerSession, call *session.Call, sipCallID string, mio *MediaIO, answer string) {
	if err := dlg.RespondSDP([]byte(answer)); err != nil {
		s.log.Warn("sip answer failed", "err", err, "sip_call_id", sipCallID, "call_id", call.ID())
		_ = mio.Close()
		_ = s.mgr.Close(call.ID(), "answer_failed")
		s.mu.Lock()
		delete(s.calls, sipCallID)
		s.mu.Unlock()
		return
	}

	_ = call.SetState(session.StateAnswered)
	_ = call.SetState(session.StateActive)
	observability.CallTraceCall(context.Background(), s.log, "inbound.accepted", call,
		"sip_call_id", sipCallID,
		"from", stripURI(call.From()),
		"transport", "sip",
	)

	// Notify agents only after ACK — avoids binding Explorer/SDK to parallel
	// trunk legs that never complete media setup.
	if s.broadcaster != nil {
		s.mu.Lock()
		cs := s.calls[sipCallID]
		var trunkID, toUser, callerExtension string
		if cs != nil {
			trunkID = cs.trunkID
			toUser = cs.toUser
			callerExtension = cs.callerExtension
		}
		normalizer := s.toNormalizer
		s.mu.Unlock()
		if normalizer != nil && toUser != "" {
			toUser = normalizer(toUser)
		}
		// Start the root call span (the telephone is the true entry point) so
		// the whole call shares one trace_id; propagate it downstream via the
		// traceparent field in call.incoming.
		spanCtx, span := observability.StartCallSpan(context.Background(), call.RootCallID())
		call.SetRootSpan(spanCtx, func() { span.End() })
		payload := map[string]any{
			"call_id":         call.ID(),
			"conversation_id": call.ConversationID(),
			"root_call_id":    call.RootCallID(),
			"phase":           call.Phase(),
			"direction":       "inbound",
			"transport":       "sip",
			"from":            stripURI(call.From()),
		}
		if tp := call.Traceparent(); tp != "" {
			payload["traceparent"] = tp
		}
		if toUser != "" {
			payload["to"] = toUser
		}
		if trunkID != "" {
			payload["trunk_id"] = trunkID
		}
		// Present only when one of our own phones placed the call. Its
		// presence is the signal that this is an agent dialling out, and
		// "to" is then the number they dialled rather than a DID of ours.
		if callerExtension != "" {
			payload["caller_extension"] = callerExtension
		}
		n, err := rpc.NewNotification("call.incoming", payload)
		if err == nil {
			s.broadcaster.BroadcastNotification(n)
		}
	}
	s.fireAccepted(call.ID(), call.From())
}

func (s *Signal) handleCancel(req *sip.Request) {
	callIDHdr := req.CallID()
	if callIDHdr == nil {
		return
	}
	sipCallID := callIDHdr.Value()
	s.mu.Lock()
	cs, ok := s.calls[sipCallID]
	s.mu.Unlock()
	if !ok {
		return
	}
	callID := cs.flowCall.ID()
	s.log.Info("sip cancel received", "sip_call_id", sipCallID, "call_id", callID)
	_ = s.mgr.Close(callID, "remote_cancel")
}

func (s *Signal) handleBye(req *sip.Request) {
	callIDHdr := req.CallID()
	if callIDHdr == nil {
		return
	}
	sipCallID := callIDHdr.Value()
	s.mu.Lock()
	cs, ok := s.calls[sipCallID]
	s.mu.Unlock()
	if !ok {
		return
	}
	// DialogServerSession tracks CSeq/state; ua.handleBye already sent 200 OK.
	if cs.dialog != nil {
		_ = cs.dialog.Close()
	}
	callID := cs.flowCall.ID()
	observability.CallTraceCall(context.Background(), s.log, "inbound.remote_bye", cs.flowCall,
		"sip_call_id", sipCallID)
	_ = s.mgr.Close(callID, "remote_bye")
	if s.broadcaster != nil {
		n, err := rpc.NewNotification("call.hangup", map[string]any{
			"call_id": callID,
			"reason":  "remote_bye",
		})
		if err == nil {
			s.broadcaster.BroadcastNotification(n)
		}
	}
}

// broadcastMediaEOFHangup notifies controllers when inbound media ended with a
// specific reason (today: RTP idle → media_timeout). No-op when the transport
// has nothing more specific than a plain channel close.
func (s *Signal) broadcastMediaEOFHangup(call *session.Call, mio *MediaIO) {
	if call == nil || mio == nil {
		return
	}
	reason := mio.MediaEOFReason()
	if reason == "" || s.broadcaster == nil {
		return
	}
	n, err := rpc.NewNotification("call.hangup", map[string]any{
		"call_id": call.ID(),
		"reason":  reason,
	})
	if err == nil {
		s.broadcaster.BroadcastNotification(n)
	}
}

func (s *Signal) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	if err := s.dialogSrv.ReadAck(req, tx); err != nil {
		s.log.Debug("sip ack ignored", "err", err)
	}

	callIDHdr := req.CallID()
	if callIDHdr == nil {
		return
	}
	sipCallID := callIDHdr.Value()
	s.mu.Lock()
	cs, ok := s.calls[sipCallID]
	s.mu.Unlock()
	if !ok {
		return
	}
	_ = cs.flowCall.SetState(session.StateActive)
}

// isInDialogInvite reports whether the INVITE carries a To-tag (RFC 3261
// in-dialog request). Initial INVITEs have an empty To-tag.
func isInDialogInvite(req *sip.Request) bool {
	to := req.To()
	if to == nil || to.Params == nil {
		return false
	}
	tag, ok := to.Params.Get("tag")
	return ok && tag != ""
}

// handleReInvite answers a session-timer / media-refresh re-INVITE for an
// established call. We keep the existing media ports and codec; remote RTP
// address updates are learned from subsequent packets.
//
// Response is built from the re-INVITE itself (correct CSeq / Via), not the
// original dialog InviteRequest — DialogServerSession.Respond would reuse the
// initial INVITE and break session-timer refreshes.
func (s *Signal) handleReInvite(cs *callState, req *sip.Request, tx sip.ServerTransaction) {
	sipCallID := ""
	if h := req.CallID(); h != nil {
		sipCallID = h.Value()
	}
	if cs == nil || cs.localPort == 0 {
		s.reject(tx, req, 481, "Call/Transaction Does Not Exist")
		return
	}

	sdp := BuildAnswerG711(
		s.ua.ExtIP(), cs.localPort, time.Now().UnixNano(), cs.g711PT, cs.dtmfPT,
	)
	resp := sip.NewResponseFromRequest(req, 200, "OK", []byte(sdp))
	resp.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	for _, h := range sessionExpiresResponseHeaders(req) {
		resp.AppendHeader(h)
	}
	if err := tx.Respond(resp); err != nil {
		s.log.Warn("sip re-invite respond failed", "sip_call_id", sipCallID, "err", err)
		return
	}
	s.log.Info("sip re-invite answered", "sip_call_id", sipCallID)
}

// sessionExpiresResponseHeaders echoes Session-Expires (RFC 4028) so the
// peer's session timer stays armed. We accept the offered value as UAS.
func sessionExpiresResponseHeaders(req *sip.Request) []sip.Header {
	se := req.GetHeader("Session-Expires")
	if se == nil {
		return nil
	}
	val := strings.TrimSpace(se.Value())
	if val == "" {
		return nil
	}
	// Prefer refresher=uas so we own the next refresh; peers that require
	// refresher=uac still accept a bare expires value.
	if !strings.Contains(strings.ToLower(val), "refresher=") {
		val = val + ";refresher=uas"
	}
	return []sip.Header{sip.NewHeader("Session-Expires", val)}
}

// reject sends a non-OK response and logs the reason.
func (s *Signal) reject(tx sip.ServerTransaction, req *sip.Request, code int, reason string) {
	sipCallID := ""
	if h := req.CallID(); h != nil {
		sipCallID = h.Value()
	}
	s.log.Info("sip invite rejected", "code", code, "reason", reason, "sip_call_id", sipCallID)
	resp := sip.NewResponseFromRequest(req, code, reason, nil)
	_ = tx.Respond(resp)
}

func (s *Signal) rejectDialog(dlg *sipgo.DialogServerSession, tx sip.ServerTransaction, req *sip.Request, code int, reason string) {
	sipCallID := ""
	if h := req.CallID(); h != nil {
		sipCallID = h.Value()
	}
	s.log.Info("sip invite rejected", "code", code, "reason", reason, "sip_call_id", sipCallID)
	_ = dlg.Respond(code, reason, nil)
}

// toUserPart returns the SIP To header user-part (the dialed DID for inbound
// INVITEs), or empty when the header / URI is malformed.
func toUserPart(req *sip.Request) string {
	to := req.To()
	if to == nil {
		return ""
	}
	return strings.TrimSpace(to.Address.User)
}

// stripURI returns the bare user@host portion (drop display name + tags).
func stripURI(s string) string {
	// Look for the first '<' / '>' angle-bracket form: "Display <sip:u@h>;tag=..."
	if open := strings.Index(s, "<"); open >= 0 {
		if close := strings.Index(s[open:], ">"); close > 0 {
			return s[open+1 : open+close]
		}
	}
	// Otherwise drop ;tag= suffix
	if semi := strings.Index(s, ";"); semi >= 0 {
		return strings.TrimSpace(s[:semi])
	}
	return fmt.Sprint(s)
}
