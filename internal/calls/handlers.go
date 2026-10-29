// Package calls wires the call.* JSON-RPC methods (answer, hangup, say,
// listen, stop) into the rpc.Registry. Each handler looks up the target Call
// in session.Manager and acts on it; per-call ASR/TTS state is owned by this
// package so the handlers can route events back to the SDK as notifications.
package calls

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	"go.opentelemetry.io/otel"
)

// tracer is acquired lazily so when the OTel SDK is not configured (no
// OTLP endpoint) Start() returns a no-op span — zero allocations.
var tracer = otel.Tracer("flowkit/calls")

// ProviderFactories returns fresh per-call ASR and TTS Providers. Each call
// gets its own ASR session (because ASR is stateful per stream); TTS can be
// reused but a factory lets the wiring layer decide.
type ProviderFactories struct {
	ASR func() (asr.Provider, error)
	TTS func() (tts.Provider, error)
}

// Broadcaster is the subset of rpc.Server used to fan transcript events out
// to every connected client (so a browser UI can mirror what the agent
// hears and says).
type Broadcaster interface {
	BroadcastNotification(*rpc.Notification)
}

// CallEventFn receives server-side call notifications (ASR/TTS lifecycle).
type CallEventFn func(method string, params map[string]any)

type callEventSub struct {
	id int
	fn CallEventFn
}

// Handlers owns the per-call runtime state and the conn↔call mapping needed
// to forward events as notifications. All per-call fields live on callState;
// the locks below stay field-scoped (not one big lock) so play/bridge/ext
// paths do not serialize on each other.
type Handlers struct {
	mgr         *session.Manager
	factories   ProviderFactories
	log         *slog.Logger
	broadcaster Broadcaster // optional; nil = no fan-out

	mu        sync.Mutex
	callState map[string]*callState // call_id → state

	playWaitMu sync.Mutex // guards callState.playWaits

	// Bridge index stays on Handlers (not in callState): a pair is shared by
	// two calls, and hangup can race Close→Done→dropState with unbridgeCall.
	// Keeping these maps means teardown still finds the pair after dropState.
	bridgeMu    sync.Mutex
	bridgePeers map[string]string      // call_id → peer call_id
	bridgePairs map[string]*bridgePair // sorted key → pair

	// keepalive survivor sweep: a keepalive bridge leaves the surviving leg
	// in StateActive when its peer dies. Without a bound, a customer leg whose
	// browser agent never reconnects would leak forever — timers keyed by
	// survivor call_id close it after keepaliveSweep (0 disables the sweep).
	keepaliveMu     sync.Mutex
	keepaliveTimers map[string]*time.Timer
	keepaliveSweep  time.Duration

	outboundHosts []string // allowed hosts for RPC-supplied URLs; empty = any

	extMu      sync.Mutex // guards callState.extAgent
	converseMu sync.Mutex // guards callState.converseCancel

	// rooms is the optional multi-party mixing controller. nil = room.* RPC
	// methods return method-not-found (feature gated off by default).
	rooms RoomController

	// roomASR holds the live transcript of each leg currently in a room,
	// keyed by call id. See room_transcript.go.
	roomASRMu sync.Mutex
	roomASR   map[string]*roomTranscript

	sipHold      SIPHoldController // optional SIP re-INVITE hold
	sipOriginate SIPOriginateController
	extReg       ExtensionResolver // optional; resolves originate-by-extension
	bridgeObs    BridgeObserver    // optional bridge recording (recording.Manager)

	originateEnabled       bool
	originateMaxConcurrent int
	originateInFlight      int32
	// originateByRequest maps request_id → *originateFlight for single-flight
	// idempotency (in-progress waiters + replay after success).
	originateByRequest sync.Map

	// bridgeASREnabled turns on continuous dual-leg transcription during a
	// human↔human bridge (off by default; opt-in like recording).
	bridgeASREnabled bool
	// bridgeASRFactory builds the per-leg ASR provider for bridge transcription.
	// nil → reuse factories.ASR (the IVR call-path provider).
	bridgeASRFactory func() (asr.Provider, error)

	kernelExclusive atomic.Bool
}

// BridgeObserver is notified when two calls are bridged (e.g. stereo recording),
// and when a takeover swaps one of those legs out, so a recording can follow the
// new leg instead of ending with the old one.
type BridgeObserver interface {
	OnBridged(userLeg, agentLeg *session.Call)
	OnLegReplaced(oldCallID string, newCall *session.Call)
}

// SetBridgeObserver wires optional bridge-time recording.
func (h *Handlers) SetBridgeObserver(o BridgeObserver) {
	h.bridgeObs = o
}

// callState is the per-call runtime kept inside Handlers.
// Lock ownership (Handlers-level locks unless noted):
//   - h.mu: map membership, conn, events, asrProvider/asrSub/listening/mediaWatchStarts
//   - playMu / sayMu: play cancel + last-say (per-call)
//   - h.playWaitMu: playWaits
//   - h.extMu: extAgent
//   - h.converseMu: converseCancel
//
// Bridge peer/pair maps stay on Handlers (see Handlers.bridgePeers comment).
type callState struct {
	conn           *rpc.Conn
	asrProvider    asr.Provider
	asrSub         *bus.Subscriber
	listening      atomic.Bool
	listenDialog   *dialogControl
	adaptiveListen atomic.Bool

	// mediaWatchStarts counts call.Done watchers spawned for this callState.
	// call.stop clears ASR but keeps the map entry, so a later call.listen must
	// not start a second watcher (at most one for the life of the call).
	mediaWatchStarts int

	// playCancel cancels the in-flight call.say goroutine. A new say
	// supersedes any previous one; call.stop(target=play|all) trips it.
	playMu          sync.Mutex
	playCancel      context.CancelFunc
	playSeq         uint64
	playing         atomic.Bool
	playStartedNano atomic.Int64 // unix nano; barge-in grace after play starts
	mediaPrimed     atomic.Bool  // outbound prompt audio has already reached the media path
	firstAudioNano  atomic.Int64 // unix nano of the call's first outbound prompt audio; 0 = none yet

	// Last assistant TTS scripts (for echo rejection on custom FunASR).
	sayMu       sync.Mutex
	prevSayText string
	lastSayText string
	lastSayAt   time.Time

	// Optional per-call dialog-timing overrides from VoiceSpeed.
	performance *PerformanceOverrides

	events []callEventSub // server-side subscribers; under h.mu

	playWaits []chan string // waiters for play-done; under h.playWaitMu

	extAgent *externalAgentLeg // v1 audio bridge WS leg; under h.extMu

	converseCancel context.CancelFunc // v2 text-turn loop cancel; under h.converseMu
}

// Register wires the call.* methods into reg.
func Register(reg *rpc.Registry, mgr *session.Manager, factories ProviderFactories, log *slog.Logger) *Handlers {
	if log == nil {
		log = slog.Default()
	}
	h := &Handlers{
		mgr:             mgr,
		factories:       factories,
		log:             log,
		callState:       make(map[string]*callState),
		bridgePeers:     make(map[string]string),
		bridgePairs:     make(map[string]*bridgePair),
		keepaliveTimers: make(map[string]*time.Timer),
	}
	reg.Register("call.answer", h.handleAnswer)
	reg.Register("call.hangup", h.handleHangup)
	reg.Register("call.say", h.handleSay)
	reg.Register("call.play", h.handlePlay)
	reg.Register("call.listen", h.handleListen)
	reg.Register("call.stop", h.handleStop)
	reg.Register("call.bridge", h.handleBridge)
	reg.Register("call.unbridge", h.handleUnbridge)
	reg.Register("call.list", h.handleList)
	reg.Register("call.dtmf", h.handleDTMF)
	reg.Register("call.hold", h.handleHold)
	reg.Register("call.unhold", h.handleUnhold)
	reg.Register("call.mute", h.handleMute)
	reg.Register("call.unmute", h.handleUnmute)
	reg.Register("call.connect_external_agent", h.handleConnectExternalAgent)
	reg.Register("call.converse_external_agent", h.handleConverseExternalAgent)
	return h
}

// SetBroadcaster wires the broadcast channel used for transcript-mirror
// notifications. Safe to call any time before traffic starts.
func (h *Handlers) SetBroadcaster(b Broadcaster) {
	h.broadcaster = b
}

// broadcast sends a notification to every connected client (best effort —
// nil broadcaster is silently skipped).
func (h *Handlers) broadcast(method, callID string, extra map[string]any) {
	if h.broadcaster == nil {
		return
	}
	payload := map[string]any{"call_id": callID}
	for k, v := range extra {
		payload[k] = v
	}
	h.attachRequestID(callID, payload)
	n, err := rpc.NewNotification(method, payload)
	if err != nil {
		h.log.Debug("broadcast encode failed", "method", method, "err", err)
		return
	}
	h.broadcaster.BroadcastNotification(n)
}

// stateFor lazily allocates the per-call state and records the binding to
// the calling rpc.Conn so events can flow back as notifications.
// Sole create entry for callState.
func (h *Handlers) stateFor(callID string, conn *rpc.Conn) *callState {
	h.mu.Lock()
	defer h.mu.Unlock()
	cs, ok := h.callState[callID]
	if !ok {
		cs = &callState{conn: conn}
		h.callState[callID] = cs
	} else if cs.conn == nil && conn != nil {
		cs.conn = conn
	}
	return cs
}

// stateGet returns the per-call state without creating it.
func (h *Handlers) stateGet(callID string) *callState {
	h.mu.Lock()
	cs := h.callState[callID]
	h.mu.Unlock()
	return cs
}

// dropState is the sole destroy entry for callState: removes the map entry and
// stops ASR. Bridge maps stay on Handlers and are cleared by teardownBridge;
// ext/converse mid-call paths still clear their fields first.
func (h *Handlers) dropState(callID string) {
	h.mu.Lock()
	cs, ok := h.callState[callID]
	delete(h.callState, callID)
	var provider asr.Provider
	var listenDialog *dialogControl
	if ok {
		provider = cs.asrProvider
		cs.asrProvider = nil
		cs.asrSub = nil // bus closes the subscriber channel on hangup
		listenDialog = cs.listenDialog
		cs.listenDialog = nil
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	if provider != nil {
		_ = provider.Stop()
	}
	if listenDialog != nil {
		listenDialog.close()
	}

	// Wake play waiters before the struct becomes unreachable. broadcastCallHangup
	// calls dropState without stopCallMedia, and can race ahead of
	// watchCallMediaEnd's stopCallMedia→signalPlayWait path; once the map entry
	// is gone, signalPlayWait is a no-op and saySyncPadded would sit until
	// ctx.Done() (wrong error type for upstream ErrPlaybackInterrupted branches).
	// "interrupted" matches stopCallMedia / playChunks so waiters keep returning
	// ErrPlaybackInterrupted rather than a new reason string.
	h.playWaitMu.Lock()
	waiters := cs.playWaits
	cs.playWaits = nil
	h.playWaitMu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- "interrupted":
		default:
		}
	}
}

var nextCallEventSubID atomic.Int64

// dispatch fans a call event to server-side subscribers and the owning RPC conn.
func (h *Handlers) dispatch(callID, method string, payload map[string]any) {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["call_id"] = callID

	h.mu.Lock()
	cs := h.callState[callID]
	var subs []callEventSub
	var listenDialog *dialogControl
	if cs != nil {
		subs = append([]callEventSub(nil), cs.events...)
		listenDialog = cs.listenDialog
	}
	h.mu.Unlock()
	if listenDialog != nil {
		listenDialog.onEvent(method, payload)
	}
	for _, sub := range subs {
		sub.fn(method, payload)
	}
	h.notify(callID, method, payload)
}

// notify emits a server-initiated event to the conn that owns the call.
func (h *Handlers) notify(callID, method string, payload map[string]any) {
	h.mu.Lock()
	cs, ok := h.callState[callID]
	h.mu.Unlock()
	if !ok || cs == nil || cs.conn == nil {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["call_id"] = callID
	h.attachRequestID(callID, payload)
	n, err := rpc.NewNotification(method, payload)
	if err != nil {
		h.log.Warn("notify encode failed", "method", method, "err", err)
		return
	}
	if err := cs.conn.SendNotification(n); err != nil {
		h.log.Debug("notify send failed", "method", method, "err", err)
	}
}

func (h *Handlers) attachRequestID(callID string, payload map[string]any) {
	if h == nil || h.mgr == nil || payload == nil {
		return
	}
	call, err := h.mgr.Get(callID)
	if err != nil {
		return
	}
	if requestID := call.RequestID(); requestID != "" {
		payload["request_id"] = requestID
	}
}

// newShortID returns a short random hex string suitable for play_id /
// record_id. Implemented inline to avoid importing crypto/rand into every
// handler file.
func newShortID() string {
	// 4 bytes hex = 8 chars. crypto/rand for non-test paths.
	var b [4]byte
	_, _ = randRead(b[:])
	return hex(b[:])
}

// randRead is a small indirection that tests can override.
var randRead = func(b []byte) (int, error) { return cryptoRandRead(b) }

func cryptoRandRead(b []byte) (int, error) {
	// stdlib import kept here to make randRead overridable in tests.
	return cryptoRandReadImpl(b)
}
