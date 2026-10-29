package calls

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	flowsip "github.com/DeepFlowAI/flowkit/internal/transport/sip"
)

const defaultOriginateMaxConcurrent = 32

// How long a finished originate stays retryable under the same request_id.
const originateIdempotencyTTL = 15 * time.Minute

type originateRecord struct {
	callID         string
	conversationID string
}

// originateFlight is the single-flight state for one non-empty request_id.
// Leaders fill rec/err then close done; followers wait on done and replay.
type originateFlight struct {
	done chan struct{}
	rec  originateRecord
	err  *rpc.Error
}

var blockedOriginateDestinations = map[string]struct{}{
	"110": {}, "119": {}, "120": {}, "122": {}, "999": {}, "000": {},
}

// SIPOriginateController starts an outbound SIP leg for a pre-created Call.
//
// trunkID is optional: empty selects the default outbound trunk (system_outbound
// or legacy single-trunk config); non-empty must resolve via the catalog.
type SIPOriginateController interface {
	StartOriginateCall(call *session.Call, uri, callerID, trunkID string, timeout time.Duration, headers map[string]string) error
}

// RegisterOriginate wires call.originate when outbound SIP is enabled.
func (h *Handlers) RegisterOriginate(reg *rpc.Registry) {
	reg.Register("call.originate", h.handleOriginate)
	reg.Register("system.capacity", h.handleCapacity)
}

func (h *Handlers) handleCapacity(_ context.Context, _ *rpc.Conn, _ json.RawMessage) (any, *rpc.Error) {
	maxConcurrent := h.originateMaxConcurrent
	inFlight := int(atomic.LoadInt32(&h.originateInFlight))
	remaining := 0
	if maxConcurrent <= 0 {
		remaining = -1
	} else {
		remaining = max(0, maxConcurrent-inFlight)
	}
	calls := 0
	if h.mgr != nil {
		calls = h.mgr.Count()
	}
	return map[string]any{
		"calls":                    calls,
		"originate_in_flight":      inFlight,
		"originate_max_concurrent": maxConcurrent,
		"originate_remaining":      remaining,
	}, nil
}

// SetSIPOriginateController wires outbound originate (requires SIP transport).
func (h *Handlers) SetSIPOriginateController(c SIPOriginateController) {
	h.sipOriginate = c
}

// ExtensionResolver maps a registered extension (AOR) to its Contact SIP URI.
// The extension.Registry satisfies this. ok=false means the extension is not
// currently registered.
type ExtensionResolver interface {
	LookupContact(extension string) (uri string, ok bool)
}

// SetExtensionResolver wires originate-by-extension. When set, call.originate
// accepts {"extension": "1001"} and dials the registered phone. Pass nil (the
// default) to leave that branch unavailable.
func (h *Handlers) SetExtensionResolver(r ExtensionResolver) {
	h.extReg = r
}

// EnableOriginate allows call.originate RPC (off by default).
func (h *Handlers) EnableOriginate(maxConcurrent int) {
	h.originateEnabled = true
	if maxConcurrent <= 0 {
		maxConcurrent = defaultOriginateMaxConcurrent
	}
	h.originateMaxConcurrent = maxConcurrent
}

type originateParams struct {
	URI       string            `json:"uri"`
	Extension string            `json:"extension,omitempty"`
	CallerID  string            `json:"caller_id,omitempty"`
	TrunkID   string            `json:"trunk_id,omitempty"`
	TimeoutMS int               `json:"timeout_ms,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

func (h *Handlers) handleOriginate(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if !h.originateEnabled {
		return nil, rpc.ErrMethodNotFound("call.originate")
	}
	if h.sipOriginate == nil {
		return nil, rpc.NewErrorf(rpc.CodeInternalError, "sip originate not available")
	}
	var p originateParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	// Originate-by-extension: resolve the registered phone's Contact URI and
	// dial it directly (no trunk). Explicit uri always wins if both are given.
	if p.URI == "" && p.Extension != "" {
		if h.extReg == nil {
			return nil, rpc.NewErrorf(rpc.CodeInternalError, "extension originate not available")
		}
		uri, ok := h.extReg.LookupContact(p.Extension)
		if !ok {
			return nil, rpc.ErrInvalidParams("extension not registered")
		}
		p.URI = uri
		p.TrunkID = ""
	}
	if p.URI == "" {
		return nil, rpc.ErrInvalidParams("missing uri")
	}
	if rpcErr := validateOriginateDestination(p.URI); rpcErr != nil {
		return nil, rpcErr
	}
	safeHeaders, err := flowsip.FilterOriginateHeaders(p.Headers)
	if err != nil {
		return nil, rpc.NewErrorf(rpc.CodeInvalidParams, "%v", err)
	}

	flight, leader := h.beginOriginateFlight(p.RequestID)
	if !leader {
		<-flight.done
		if flight.err != nil {
			return nil, flight.err
		}
		return h.originateReplay(flight.rec), nil
	}

	failFlight := func(rpcErr *rpc.Error) (any, *rpc.Error) {
		h.failOriginateFlight(p.RequestID, flight, rpcErr)
		return nil, rpcErr
	}

	if !h.reserveOriginateSlot() {
		return failFlight(rpc.NewError(rpc.CodeRateLimited, "originate concurrent limit reached"))
	}

	call := h.mgr.Create(session.DirectionOutbound)
	if call == nil {
		h.releaseOriginateSlot()
		return failFlight(rpc.NewErrorf(rpc.CodeInternalError, "server shutting down"))
	}
	if p.RequestID != "" {
		call.SetRequestID(p.RequestID)
	}
	h.stateFor(call.ID(), conn)

	var timeout time.Duration // zero: transport default
	if p.TimeoutMS > 0 {
		timeout = time.Duration(p.TimeoutMS) * time.Millisecond
	}

	if err := h.sipOriginate.StartOriginateCall(call, p.URI, p.CallerID, p.TrunkID, timeout, safeHeaders); err != nil {
		h.releaseOriginateSlot()
		_ = h.mgr.Close(call.ID(), "originate_rejected")
		h.dropState(call.ID())
		return failFlight(mapOriginateErrorToRPC(err))
	}

	rec := originateRecord{callID: call.ID(), conversationID: call.ConversationID()}
	h.completeOriginateFlight(flight, rec)

	go func() {
		<-call.Done()
		h.releaseOriginateSlot()
		h.dropState(call.ID())
		h.expireOriginate(p.RequestID)
	}()

	observability.CallTraceCall(ctx, h.log, "rpc.originate", call, "uri", p.URI, "caller_id", p.CallerID, "request_id", p.RequestID)
	return h.originateReplay(rec), nil
}

func (h *Handlers) originateReplay(rec originateRecord) map[string]any {
	out := map[string]any{
		"call_id":         rec.callID,
		"conversation_id": rec.conversationID,
		"status":          "originating",
	}
	if call, err := h.mgr.Get(rec.callID); err == nil {
		if requestID := call.RequestID(); requestID != "" {
			out["request_id"] = requestID
		}
		out["conversation_id"] = call.ConversationID()
	}
	return out
}

// beginOriginateFlight claims request_id for single-flight admission.
// Empty request_id always leads without storing anything.
func (h *Handlers) beginOriginateFlight(requestID string) (*originateFlight, bool) {
	flight := &originateFlight{done: make(chan struct{})}
	if requestID == "" {
		return flight, true
	}
	actual, loaded := h.originateByRequest.LoadOrStore(requestID, flight)
	if loaded {
		existing, _ := actual.(*originateFlight)
		return existing, false
	}
	return flight, true
}

func (h *Handlers) completeOriginateFlight(flight *originateFlight, rec originateRecord) {
	flight.rec = rec
	close(flight.done)
}

func (h *Handlers) failOriginateFlight(requestID string, flight *originateFlight, rpcErr *rpc.Error) {
	if requestID != "" {
		h.originateByRequest.Delete(requestID)
	}
	flight.err = rpcErr
	close(flight.done)
}

func (h *Handlers) expireOriginate(requestID string) {
	if requestID == "" {
		return
	}
	time.AfterFunc(originateIdempotencyTTL, func() {
		h.originateByRequest.Delete(requestID)
	})
}

// reserveOriginateSlot atomically takes one concurrency slot before SIP start.
// Returns false when originateMaxConcurrent would be exceeded.
func (h *Handlers) reserveOriginateSlot() bool {
	if h.originateMaxConcurrent <= 0 {
		atomic.AddInt32(&h.originateInFlight, 1)
		return true
	}
	for {
		cur := atomic.LoadInt32(&h.originateInFlight)
		if cur >= int32(h.originateMaxConcurrent) {
			return false
		}
		if atomic.CompareAndSwapInt32(&h.originateInFlight, cur, cur+1) {
			return true
		}
	}
}

func (h *Handlers) releaseOriginateSlot() {
	atomic.AddInt32(&h.originateInFlight, -1)
}

// mapOriginateErrorToRPC translates SIP-layer originate failures into
// RPC error codes the CC can pattern-match on.
func mapOriginateErrorToRPC(err error) *rpc.Error {
	switch {
	case errors.Is(err, flowsip.ErrTrunkNotFound):
		return rpc.NewErrorf(rpc.CodeInvalidParams, "trunk_id not found")
	case errors.Is(err, flowsip.ErrTrunkNotReady):
		return rpc.NewErrorf(rpc.CodeInvalidParams, "trunk not ready for outbound")
	}
	return rpc.NewErrorf(rpc.CodeInvalidParams, "%v", err)
}

func validateOriginateDestination(uri string) *rpc.Error {
	raw := strings.TrimSpace(uri)
	if strings.HasPrefix(strings.ToLower(raw), "sip:") {
		raw = raw[4:]
	}
	if at := strings.IndexByte(raw, '@'); at >= 0 {
		raw = raw[:at]
	}
	digits := raw
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return nil
		}
	}
	if len(digits) <= 6 {
		if _, blocked := blockedOriginateDestinations[digits]; blocked {
			return rpc.NewError(rpc.CodeInvalidParams, "originate destination not allowed")
		}
	}
	return nil
}
