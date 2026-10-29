package calls

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/media/codec"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/gorilla/websocket"
)

// External-agent event methods (contract C). Provider-agnostic: any realtime
// agent that speaks the binary-PCM + JSON-control protocol can drive them.
const (
	evtExternalAgentConnected = "call.external_agent.connected"
	evtExternalAgentHandoff   = "call.external_agent.handoff"
	evtExternalAgentFailed    = "call.external_agent.failed"
	evtExternalAgentEnded     = "call.external_agent.ended"
)

const (
	defaultAudioInRate  = 16000 // PCM rate flowkit sends to the external agent
	defaultAudioOutRate = 24000 // PCM rate the external agent sends back
)

// externalAudioRingFrames bounds each direction's jitter buffer. 25 frames at
// 20 ms ≈ 500 ms; on overflow the oldest frame is dropped so latency does not
// accumulate. Control frames never use this buffer and are never dropped.
const externalAudioRingFrames = 25

// wsDialTimeout caps how long the WS connect may take before failing.
const wsDialTimeout = 8 * time.Second

// frameRing is a bounded FIFO with drop-oldest overflow semantics, used as a
// per-direction audio jitter buffer. It assumes a single producer (push) and a
// single consumer (pop); both are safe to call concurrently with each other.
type frameRing[T any] struct {
	mu      sync.Mutex
	buf     []T
	max     int
	sig     chan struct{} // pulsed on push to wake a blocked pop
	dropped uint64
}

func newFrameRing[T any](max int) *frameRing[T] {
	if max < 1 {
		max = 1
	}
	return &frameRing[T]{max: max, sig: make(chan struct{}, 1)}
}

// push appends v, dropping the oldest item when the ring is full. It never
// blocks. Returns true when an item was dropped to make room.
func (r *frameRing[T]) push(v T) bool {
	r.mu.Lock()
	dropped := false
	if len(r.buf) >= r.max {
		r.buf = r.buf[1:]
		r.dropped++
		dropped = true
	}
	r.buf = append(r.buf, v)
	r.mu.Unlock()
	select {
	case r.sig <- struct{}{}:
	default:
	}
	return dropped
}

// pop returns the oldest item, blocking until one is available or done closes.
func (r *frameRing[T]) pop(done <-chan struct{}) (T, bool) {
	var zero T
	for {
		r.mu.Lock()
		if len(r.buf) > 0 {
			v := r.buf[0]
			r.buf = r.buf[1:]
			r.mu.Unlock()
			return v, true
		}
		r.mu.Unlock()
		select {
		case <-r.sig:
		case <-done:
			return zero, false
		}
	}
}

func (r *frameRing[T]) droppedCount() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// externalAgentLeg owns one WebSocket session bridging a call's audio to an
// external realtime agent (e.g. VoiceSpeed). It is created per call.connect_
// external_agent and torn down on the first of: terminal control frame, WS
// error, call end, or call.stop/call.hangup for the call.
type externalAgentLeg struct {
	callID       string
	provider     string
	audioInRate  int
	audioOutRate int
	traceparent  string // bridge-span traceparent forwarded to events

	h    *Handlers
	call *session.Call
	ws   *websocket.Conn
	log  *slog.Logger

	sub     *bus.Subscriber
	outRing *frameRing[[]byte]  // call → agent (encoded PCM frames)
	inRing  *frameRing[[]int16] // agent → call (resampled samples)

	done     chan struct{}
	wg       sync.WaitGroup
	termOnce sync.Once
	endSpan  func()
}

type connectExternalAgentParams struct {
	CallID       string `json:"call_id"`
	Provider     string `json:"provider"`
	WSURL        string `json:"ws_url"`
	Token        string `json:"token"`
	AudioInRate  int    `json:"audio_in_rate"`
	AudioOutRate int    `json:"audio_out_rate"`
	Traceparent  string `json:"traceparent"`
	TraceCallID  string `json:"trace_call_id"`
}

func (h *Handlers) handleConnectExternalAgent(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p connectExternalAgentParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.CallID == "" || p.WSURL == "" {
		return nil, rpc.ErrInvalidParams("call_id and ws_url required")
	}
	if err := h.checkOutboundURL(p.WSURL); err != nil {
		return nil, rpc.ErrInvalidParams(err.Error())
	}
	if p.AudioInRate <= 0 {
		p.AudioInRate = defaultAudioInRate
	}
	if p.AudioOutRate <= 0 {
		p.AudioOutRate = defaultAudioOutRate
	}

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.connect_external_agent"); rpcErr != nil {
		return nil, rpcErr
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}

	if cs := h.stateGet(p.CallID); cs != nil {
		h.extMu.Lock()
		exists := cs.extAgent != nil
		h.extMu.Unlock()
		if exists {
			return nil, rpc.ErrCallStateInvalid("external agent already connected")
		}
	}

	// The external-agent leg is a child of the call's root span (§ trace). Fall
	// back to the propagated traceparent when the transport did not start one.
	parentCtx := call.RootSpanContext()
	if p.Traceparent != "" {
		parentCtx = observability.ContextFromTraceparent(parentCtx, p.Traceparent)
	}
	traceCallID := p.TraceCallID
	if traceCallID == "" {
		traceCallID = call.RootCallID()
	}
	spanCtx, span := observability.StartChildSpan(parentCtx, "voicespeed.bridge", traceCallID)
	downstreamTP := observability.TraceparentFromContext(spanCtx)

	dialURL, derr := buildExternalAgentURL(p.WSURL, p.Token, downstreamTP)
	if derr != nil {
		span.End()
		return nil, rpc.ErrInvalidParams(derr.Error())
	}

	dialer := &websocket.Dialer{HandshakeTimeout: wsDialTimeout}
	dialCtx, cancel := context.WithTimeout(ctx, wsDialTimeout)
	defer cancel()
	ws, _, wsErr := dialer.DialContext(dialCtx, dialURL, nil)
	if wsErr != nil {
		span.End()
		h.dispatchExternalEvent(p.CallID, evtExternalAgentFailed, map[string]any{
			"code":    rpc.CodeInternalError,
			"message": wsErr.Error(),
		}, downstreamTP)
		observability.CallTraceCall(spanCtx, h.log, "external_agent.failed", call,
			"provider", p.Provider, "message", wsErr.Error())
		return nil, rpc.NewErrorf(rpc.CodeInternalError, "external agent connect failed: %v", wsErr)
	}

	leg := &externalAgentLeg{
		callID:       p.CallID,
		provider:     p.Provider,
		audioInRate:  p.AudioInRate,
		audioOutRate: p.AudioOutRate,
		traceparent:  downstreamTP,
		h:            h,
		call:         call,
		ws:           ws,
		log:          h.log.With("call_id", p.CallID, "provider", p.Provider),
		outRing:      newFrameRing[[]byte](externalAudioRingFrames),
		inRing:       newFrameRing[[]int16](externalAudioRingFrames),
		done:         make(chan struct{}),
		endSpan:      func() { span.End() },
	}

	// Bind state so events flow back to the conn that issued the RPC.
	cs := h.stateFor(p.CallID, conn)

	h.extMu.Lock()
	if cs.extAgent != nil {
		h.extMu.Unlock()
		_ = ws.Close()
		span.End()
		return nil, rpc.ErrCallStateInvalid("external agent already connected")
	}
	cs.extAgent = leg
	h.extMu.Unlock()

	leg.sub = call.AudioBus().Subscribe()
	leg.wg.Add(4)
	go leg.pumpCallToWS()
	go leg.pumpWSWrite()
	go leg.pumpWSRead()
	go leg.pumpWSToCall()
	go leg.watchCall()

	h.dispatchExternalEvent(p.CallID, evtExternalAgentConnected, nil, downstreamTP)
	observability.CallTraceCall(spanCtx, h.log, "external_agent.connect", call,
		"provider", p.Provider, "audio_in_rate", p.AudioInRate, "audio_out_rate", p.AudioOutRate)

	return map[string]bool{"ok": true}, nil
}

// buildExternalAgentURL appends token + traceparent as query params on ws_url.
func buildExternalAgentURL(wsURL, token, traceparent string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if token != "" {
		q.Set("token", token)
	}
	if traceparent != "" {
		q.Set("traceparent", traceparent)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// pumpCallToWS subscribes the call audio, resamples to the agent input rate and
// enqueues encoded PCM frames for the writer. Never blocks the AudioBus.
func (l *externalAgentLeg) pumpCallToWS() {
	defer l.wg.Done()
	ch := l.sub.Channel()
	for {
		select {
		case <-l.done:
			return
		case frame, ok := <-ch:
			if !ok {
				return
			}
			samples := codec.ResampleLinear(frame.Samples, bus.SampleRate, l.audioInRate)
			l.outRing.push(int16ToBytes(samples))
		}
	}
}

// pumpWSWrite drains the outbound ring and writes binary PCM frames to the WS.
func (l *externalAgentLeg) pumpWSWrite() {
	defer l.wg.Done()
	for {
		data, ok := l.outRing.pop(l.done)
		if !ok {
			return
		}
		if err := l.ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
			l.finish(evtExternalAgentFailed, map[string]any{
				"code":    rpc.CodeInternalError,
				"message": "ws write: " + err.Error(),
			})
			return
		}
	}
}

// pumpWSRead reads WS frames: binary → call audio ring; text → control events.
func (l *externalAgentLeg) pumpWSRead() {
	defer l.wg.Done()
	for {
		mt, data, err := l.ws.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				l.finish(evtExternalAgentEnded, map[string]any{"reason": "remote_close"})
			} else {
				l.finish(evtExternalAgentFailed, map[string]any{
					"code":    rpc.CodeInternalError,
					"message": err.Error(),
				})
			}
			return
		}
		switch mt {
		case websocket.BinaryMessage:
			samples := codec.ResampleLinear(bytesToInt16(data), l.audioOutRate, bus.SampleRate)
			l.inRing.push(samples)
		case websocket.TextMessage:
			method, payload, terminal, ok := classifyControlFrame(data)
			if !ok {
				continue
			}
			if terminal {
				l.finish(method, payload)
				return
			}
			l.h.dispatchExternalEvent(l.callID, method, payload, l.traceparent)
		}
	}
}

// pumpWSToCall drains the inbound ring and writes resampled PCM to the call.
func (l *externalAgentLeg) pumpWSToCall() {
	defer l.wg.Done()
	// Child of call.Context so hangup unblocks WriteOutgoing; also cancelled
	// when the leg finishes without ending the call (local teardown).
	ctx, cancel := context.WithCancel(l.call.Context())
	defer cancel()
	go func() {
		<-l.done
		cancel()
	}()
	for {
		samples, ok := l.inRing.pop(l.done)
		if !ok {
			return
		}
		if _, err := l.h.writeOutgoingFrames(ctx, l.call, samples); err != nil {
			return
		}
	}
}

// watchCall tears the leg down (emitting ended) when the call terminates for a
// reason other than an explicit local teardown.
func (l *externalAgentLeg) watchCall() {
	select {
	case <-l.call.Done():
		l.finish(evtExternalAgentEnded, map[string]any{"reason": "call_ended"})
	case <-l.done:
	}
}

// finish runs the terminal path exactly once: signal pumps, close the WS,
// emit the terminal event (when method != ""), end the span and drop the leg.
func (l *externalAgentLeg) finish(method string, payload map[string]any) {
	l.termOnce.Do(func() {
		close(l.done)
		_ = l.ws.Close()
		if l.sub != nil {
			l.call.AudioBus().Unsubscribe(l.sub)
		}
		l.h.removeExternalAgent(l.callID, l)
		if method != "" {
			l.h.dispatchExternalEvent(l.callID, method, payload, l.traceparent)
			// finish may run after call.Context is cancelled; keep Values.
			observability.CallTrace(context.WithoutCancel(l.call.Context()), l.log, eventNameFor(method), l.callID,
				"provider", l.provider, "out_dropped", l.outRing.droppedCount(), "in_dropped", l.inRing.droppedCount())
		}
		if l.endSpan != nil {
			l.endSpan()
		}
	})
}

// dispatchExternalEvent routes an external-agent event to the call's owner conn
// and server-side subscribers, carrying traceparent so OpenDesk can continue
// the trace.
func (h *Handlers) dispatchExternalEvent(callID, method string, payload map[string]any, traceparent string) {
	if payload == nil {
		payload = map[string]any{}
	}
	if traceparent != "" {
		payload["traceparent"] = traceparent
	}
	h.dispatch(callID, method, payload)
}

// removeExternalAgent clears the leg from callState when it matches.
func (h *Handlers) removeExternalAgent(callID string, leg *externalAgentLeg) {
	cs := h.stateGet(callID)
	if cs == nil {
		return
	}
	h.extMu.Lock()
	if cs.extAgent == leg {
		cs.extAgent = nil
	}
	h.extMu.Unlock()
}

// teardownExternalAgent silently closes the external-agent leg for a call (no
// terminal event), used by explicit call.stop / call.hangup.
func (h *Handlers) teardownExternalAgent(callID string) {
	cs := h.stateGet(callID)
	if cs == nil {
		return
	}
	h.extMu.Lock()
	leg := cs.extAgent
	h.extMu.Unlock()
	if leg != nil {
		leg.finish("", nil)
	}
}

// classifyControlFrame maps a JSON control frame from the external agent to a
// flowkit event. terminal marks frames that end the leg; ok is false for
// frames that are not control events of interest (state/asr/llm progress),
// which are ignored.
func classifyControlFrame(data []byte) (method string, payload map[string]any, terminal, ok bool) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", nil, false, false
	}
	typ, _ := raw["type"].(string)
	switch typ {
	case "handoff":
		inner := map[string]any{}
		for k, v := range raw {
			if k == "type" {
				continue
			}
			inner[k] = v
		}
		return evtExternalAgentHandoff, map[string]any{"payload": inner}, false, true
	case "error":
		return evtExternalAgentFailed, map[string]any{
			"code":    raw["code"],
			"message": raw["message"],
		}, true, true
	case "ended", "close":
		return evtExternalAgentEnded, map[string]any{"reason": raw["reason"]}, true, true
	case "state":
		if s, _ := raw["state"].(string); s == "ended" {
			return evtExternalAgentEnded, map[string]any{"reason": "ended"}, true, true
		}
		return "", nil, false, false
	default:
		return "", nil, false, false
	}
}

// eventNameFor maps an event method to a short CallTrace event label.
func eventNameFor(method string) string {
	switch method {
	case evtExternalAgentHandoff:
		return "external_agent.handoff"
	case evtExternalAgentFailed:
		return "external_agent.failed"
	case evtExternalAgentEnded:
		return "external_agent.ended"
	default:
		return "external_agent.connect"
	}
}

func int16ToBytes(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(s))
	}
	return b
}

func bytesToInt16(b []byte) []int16 {
	n := len(b) / 2
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}
