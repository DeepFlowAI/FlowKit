package webrtc

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	pwebrtc "github.com/pion/webrtc/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("flowkit/webrtc")

// PeerFactory constructs a fresh Peer for each incoming webrtc.offer.
// Implementations typically capture ICE-server config and a logger.
type PeerFactory func() (*Peer, error)

// Broadcaster is the subset of rpc.Server we need (BroadcastNotification).
// Injecting via interface keeps the webrtc package free of an rpc.Server
// type dependency for testing.
type Broadcaster interface {
	BroadcastNotification(*rpc.Notification)
}

// SignalState owns the registry of active Peers indexed by call_id. It
// wires JSON-RPC handlers for the two signalling methods:
// `webrtc.offer` and `webrtc.ice`.
type SignalState struct {
	factory     PeerFactory
	calls       *session.Manager
	broadcaster Broadcaster
	log         *slog.Logger

	mu    sync.Mutex
	peers map[string]*Peer // call_id → Peer
}

// RegisterSignalMethods constructs a SignalState and binds its handlers to
// the given Registry. broadcaster may be nil — call.incoming notifications
// will be silently skipped if so (useful for tests that exercise the
// signal handlers in isolation).
func RegisterSignalMethods(reg *rpc.Registry, factory PeerFactory, mgr *session.Manager, broadcaster Broadcaster, log *slog.Logger) *SignalState {
	if log == nil {
		log = slog.Default()
	}
	s := &SignalState{
		factory:     factory,
		calls:       mgr,
		broadcaster: broadcaster,
		log:         log,
		peers:       make(map[string]*Peer),
	}
	reg.Register("webrtc.offer", s.handleOffer)
	reg.Register("webrtc.ice", s.handleICE)
	return s
}

// PeerCount returns the current number of live peer connections.
func (s *SignalState) PeerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

// Shutdown closes every active peer and clears the map.
func (s *SignalState) Shutdown() {
	s.mu.Lock()
	peers := s.peers
	s.peers = make(map[string]*Peer)
	s.mu.Unlock()
	for _, p := range peers {
		_ = p.Close()
	}
}

// --- handlers ---

type offerParams struct {
	SDP string `json:"sdp"`
	// CallID is optional. If supplied, an existing Call with that id can be
	// reused (e.g. for a re-negotiation flow). M2 ignores it for simplicity.
	CallID string `json:"call_id,omitempty"`
	// Metadata is opaque routing context supplied by the caller (e.g. a browser
	// passing {agent_id, token}). flowkit does not interpret it — it is echoed
	// verbatim into the call.incoming broadcast so a registered agent can decide
	// which brain/turn endpoint to drive for this leg.
	Metadata map[string]any `json:"metadata,omitempty"`
}

type offerResult struct {
	CallID string `json:"call_id"`
	SDP    string `json:"sdp"`
}

func (s *SignalState) handleOffer(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	ctx, span := tracer.Start(ctx, "webrtc.offer")
	defer span.End()

	var p offerParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		span.SetStatus(codes.Error, "invalid_params")
		return nil, rpcErr
	}
	if p.SDP == "" {
		span.SetStatus(codes.Error, "empty_sdp")
		return nil, rpc.ErrInvalidParams("missing sdp")
	}
	span.SetAttributes(attribute.Int("sdp_len", len(p.SDP)))

	peer, err := s.factory()
	if err != nil {
		span.SetStatus(codes.Error, "factory_failed")
		return nil, rpc.NewErrorf(rpc.CodeWebRTCICEFailed, "factory: %v", err)
	}

	call := s.calls.Create(session.DirectionInbound)
	if call == nil {
		_ = peer.Close()
		return nil, rpc.NewError(rpc.CodeShuttingDown, "server is shutting down")
	}
	if err := call.BindMediaIO(peer.MediaIO()); err != nil {
		_ = peer.Close()
		_ = s.calls.Close(call.ID(), "bind_failed")
		return nil, rpc.NewErrorf(rpc.CodeWebRTCICEFailed, "bind: %v", err)
	}

	// Register trickle ICE **before** HandleOffer so candidates emitted
	// during answer gathering are forwarded to the browser immediately.
	if conn != nil {
		peer.OnLocalICECandidate(func(c pwebrtc.ICECandidateInit) {
			n, err := rpc.NewNotification("webrtc.ice", map[string]any{
				"call_id":   call.ID(),
				"candidate": c,
			})
			if err == nil {
				if err := conn.SendNotification(n); err != nil {
					s.log.Debug("webrtc.ice notify failed", "call_id", call.ID(), "err", err)
				}
			}
		})
	}

	answer, err := peer.HandleOffer(ctx, p.SDP)
	if err != nil {
		_ = peer.Close()
		_ = s.calls.Close(call.ID(), "offer_failed")
		return nil, rpc.NewErrorf(rpc.CodeWebRTCICEFailed, "handle offer: %v", err)
	}

	s.mu.Lock()
	s.peers[call.ID()] = peer
	s.mu.Unlock()

	// Auto-cleanup when the Call ends — either because we explicitly hang up
	// or because the peer disconnected.
	go func() {
		<-call.Done()
		s.mu.Lock()
		delete(s.peers, call.ID())
		s.mu.Unlock()
		_ = peer.Close()
	}()

	observability.CallTraceCall(ctx, s.log, "webrtc.created", call, "transport", "webrtc")

	// Start the root call span so the whole call shares one trace_id; propagate
	// it downstream via the traceparent field in call.incoming.
	spanCtx, span := observability.StartCallSpan(context.Background(), call.RootCallID())
	call.SetRootSpan(spanCtx, func() { span.End() })

	// Broadcast call.incoming so any registered Python agent can claim the call.
	if s.broadcaster != nil {
		payload := map[string]any{
			"call_id":         call.ID(),
			"conversation_id": call.ConversationID(),
			"root_call_id":    call.RootCallID(),
			"phase":           call.Phase(),
			"direction":       "inbound",
			"transport":       "webrtc",
		}
		if tp := call.Traceparent(); tp != "" {
			payload["traceparent"] = tp
		}
		if len(p.Metadata) > 0 {
			payload["metadata"] = p.Metadata
		}
		n, err := rpc.NewNotification("call.incoming", payload)
		if err == nil {
			s.broadcaster.BroadcastNotification(n)
		}
	}

	return offerResult{CallID: call.ID(), SDP: answer}, nil
}

type iceParams struct {
	CallID    string                   `json:"call_id"`
	Candidate pwebrtc.ICECandidateInit `json:"candidate"`
}

func (s *SignalState) handleICE(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p iceParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.CallID == "" {
		return nil, rpc.ErrInvalidParams("missing call_id")
	}

	s.mu.Lock()
	peer, ok := s.peers[p.CallID]
	s.mu.Unlock()
	if !ok {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if err := peer.AddICECandidate(p.Candidate); err != nil {
		return nil, rpc.NewErrorf(rpc.CodeWebRTCICEFailed, "add candidate: %v", err)
	}
	return map[string]bool{"ok": true}, nil
}
