package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
)

// EventBufferSize is the default capacity of the Call.events channel. A buffer
// keeps publishers from blocking when a subscriber is briefly slow.
const EventBufferSize = 64

var (
	// ErrInvalidTransition is returned by SetState when from→to is not allowed.
	ErrInvalidTransition = errors.New("session: invalid state transition")

	// ErrEnded is returned when an operation is attempted on a terminal call.
	ErrEnded = errors.New("session: call already ended")
)

// Call is the in-memory representation of a single conversation. It owns its
// own state-machine and a single event channel; transport layers translate
// between Call methods/events and the wire.
//
// Each Call has its own AudioBus that fans incoming PCM frames out to one or
// more consumers (typically ASR, optionally recording). Outgoing PCM (TTS)
// is written directly to the bound MediaIO without going through the bus.
//
// All public methods are safe for concurrent use.
type Call struct {
	id             string
	conversationID string
	rootCallID     string
	phase          string
	direction      Direction
	from           string // remote URI (caller for inbound, callee for outbound)
	to             string // local URI / number
	requestID      string // optional client idempotency key (call.originate)
	createdAt      time.Time
	log            *slog.Logger

	mu    sync.RWMutex
	state State

	events    chan Event
	ctx       context.Context    // cancelled exactly when the call ends
	cancel    context.CancelFunc // idempotent; invoked from closeEvents
	closeOnce sync.Once
	ended     atomic.Bool
	answered  atomic.Bool
	held      atomic.Bool
	muted     atomic.Bool
	eventsMu  sync.Mutex // serializes sends to c.events and channel close

	// Audio plumbing (M2). bus is created eagerly so consumers can subscribe
	// before media is bound; mediaIO is nil until BindMediaIO is called.
	audioBus *bus.Bus
	mediaMu  sync.Mutex
	mediaIO  transport.MediaIO
	pumpDone chan struct{} // nil until BindMediaIO; closed when pump exits

	// Root OTel span for the whole call (set by the transport that accepts the
	// call). rootSpanCtx carries the span so child legs can derive from it;
	// endRootSpan ends the span exactly once when the call closes.
	traceMu     sync.Mutex
	rootSpanCtx context.Context
	endRootSpan func()
	traceparent string // guaranteed-valid W3C traceparent, set by SetRootSpan

	// renderSinks receive outgoing (played) PCM at the transport's paced send
	// time. Multiple consumers (recording, AEC echo reference) subscribe by id
	// without contending for a single slot. Sinks must be non-blocking and copy
	// samples they retain.
	renderMu    sync.Mutex
	renderSinks map[string]func(samples []int16)
	// tapViaTransport is true when the bound MediaIO's SetSentTap is wired to
	// the fan-out (post-pacing). WriteOutgoing then skips its enqueue-time
	// fallback tap to avoid double-feeding and bursty queue-time timestamps.
	tapViaTransport atomic.Bool

	// captureProc, if set, transforms each incoming mic frame before it is
	// published to the AudioBus (e.g. AEC3 echo cancellation). nil = passthrough.
	captureProc atomic.Pointer[CaptureProcessor]

	// mediaTimeNs is the inbound sample clock (nanoseconds), advanced by
	// pumpIncoming from published frame lengths. Hold / dropped sub-frames do
	// not advance it. Exposed via MediaTime / call.trace media_ms.
	mediaTimeNs atomic.Int64
	// mediaWallStartNs / mediaWallLastNs are UnixNano anchors for wall_ms.
	// Start is aligned to the first frame's audio begin (arrival minus that
	// frame's duration) so it shares MediaTime's zero. Last is arrival of the
	// most recent frame. Time after last is reported as tail_idle_ms.
	mediaWallStartNs atomic.Int64
	mediaWallLastNs  atomic.Int64
	heldFrames       atomic.Uint64 // inbound frames skipped while held
	mutedFrames      atomic.Uint64 // inbound frames emptied while muted
	// noOutputFrames counts capture-processor inputs that produced no samples
	// (includes normal sub-frame buffering, not only discarded audio).
	noOutputFrames atomic.Uint64
}

// CaptureProcessor transforms one mic frame (16 kHz int16 mono) before it reaches
// the AudioBus. Returning an empty slice drops the frame (e.g. sub-frame buffering).
type CaptureProcessor interface {
	Process(frame []int16) []int16
}

// NewCall constructs a Call in StateIdle under context.Background().
// Prefer Manager.Create / CreateWithID in production so the call is tracked
// and its context inherits Values from the Manager root.
func NewCall(id string, direction Direction, log *slog.Logger) *Call {
	return newCall(context.Background(), id, direction, log)
}

// newCall constructs a Call whose lifecycle context is a child of parent.
// Manager passes context.WithoutCancel(root): Values inherit, but parent
// cancel does not close Call.Done. Shutdown force is applied by Manager.watch
// calling Hangup when the root cancels, keeping Done and IsEnded coupled.
func newCall(parent context.Context, id string, direction Direction, log *slog.Logger) *Call {
	if log == nil {
		log = slog.Default()
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	convID := NewConversationID()
	logger := log.With(
		"call_id", id,
		"conversation_id", convID,
		"root_call_id", id,
		"phase", PhaseLLM,
		"direction", direction.String(),
	)
	return &Call{
		id:             id,
		conversationID: convID,
		rootCallID:     id,
		phase:          PhaseLLM,
		direction:      direction,
		createdAt:      time.Now().UTC(),
		log:            logger,
		state:          StateIdle,
		events:         make(chan Event, EventBufferSize),
		ctx:            ctx,
		cancel:         cancel,
		audioBus:       bus.New(logger),
	}
}

// Context returns the per-call lifecycle context. It is cancelled exactly when
// the call reaches StateEnded (same instant as Done closes). Safe to derive
// child contexts for per-call goroutines; hangup cancels the whole tree.
//
// Distinct from RootSpanContext, which carries the OTel span only.
func (c *Call) Context() context.Context { return c.ctx }

// Done returns a channel that is closed when the call reaches StateEnded.
// It is c.Context().Done() — the same channel — so select{<-Done()} and
// Context cancellation cannot observe divergent states.
//
// Useful for managers / supervisors to learn about termination without
// consuming events from Subscribe (which is reserved for a single SDK
// consumer).
func (c *Call) Done() <-chan struct{} { return c.ctx.Done() }

// ID returns the call identifier.
func (c *Call) ID() string { return c.id }

// RequestID returns the client idempotency key, if the caller supplied one.
func (c *Call) RequestID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.requestID
}

// SetRequestID stores the client idempotency key. Empty values are ignored.
func (c *Call) SetRequestID(requestID string) {
	if requestID == "" {
		return
	}
	c.mu.Lock()
	c.requestID = requestID
	c.mu.Unlock()
}

// ConversationID returns the session-level id shared across bridged legs.
func (c *Call) ConversationID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conversationID
}

// RootCallID returns the primary user leg call_id for this conversation.
func (c *Call) RootCallID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rootCallID
}

// Phase returns the current conversation phase (llm or agent).
func (c *Call) Phase() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.phase
}

// SetPhase updates the conversation phase and logger fields.
func (c *Call) SetPhase(phase string) {
	c.mu.Lock()
	c.phase = phase
	c.log = c.log.With("phase", phase)
	c.mu.Unlock()
}

func (c *Call) applyConversation(conversationID, rootCallID, phase string) {
	c.mu.Lock()
	c.conversationID = conversationID
	c.rootCallID = rootCallID
	c.phase = phase
	c.log = c.log.With(
		"conversation_id", conversationID,
		"root_call_id", rootCallID,
		"phase", phase,
	)
	c.mu.Unlock()
}

// TraceAttrs returns slog key/value pairs for call.trace correlation.
func (c *Call) TraceAttrs() []any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return []any{
		"conversation_id", c.conversationID,
		"root_call_id", c.rootCallID,
		"phase", c.phase,
		"media_ms", c.MediaTime().Milliseconds(),
	}
}

// MediaTime returns the inbound sample-clock offset maintained by pumpIncoming.
// It advances only when frames are published to the AudioBus (not while held).
// Anyone who has observed a published frame is guaranteed to read a MediaTime
// no earlier than that frame's end (Store happens before Publish).
func (c *Call) MediaTime() time.Duration {
	return time.Duration(c.mediaTimeNs.Load())
}

// DialogAttrs returns slog key/value pairs for call.dialog logging.
func (c *Call) DialogAttrs() []any {
	return c.TraceAttrs()
}

// SetRootSpan records the root call span context and its end func. The end
// func is invoked once when the call closes. Calling more than once replaces
// the previous binding (ending the previous span first).
func (c *Call) SetRootSpan(ctx context.Context, end func()) {
	tp := observability.EnsureTraceparent(ctx)
	traceID := observability.TraceIDFromTraceparent(tp)

	c.traceMu.Lock()
	prev := c.endRootSpan
	c.rootSpanCtx = ctx
	c.endRootSpan = end
	c.traceparent = tp
	c.traceMu.Unlock()

	// Carry the trace_id on the call logger so flowkit logs correlate with the
	// cross-service trace even when traces are not exported.
	if traceID != "" {
		c.mu.Lock()
		c.log = c.log.With("trace_id", traceID)
		c.mu.Unlock()
	}

	if prev != nil {
		prev()
	}
}

// RootSpanContext returns the context carrying the call's root span, or a
// background context when none was set.
func (c *Call) RootSpanContext() context.Context {
	c.traceMu.Lock()
	defer c.traceMu.Unlock()
	if c.rootSpanCtx == nil {
		return context.Background()
	}
	return c.rootSpanCtx
}

// Traceparent returns the W3C traceparent for the call's root span. Once
// SetRootSpan has run this is always a non-empty, well-formed value (a random
// trace_id is minted when no OTLP backend is configured), so every call stays
// part of the cross-service trace. Returns "" only before SetRootSpan is called.
func (c *Call) Traceparent() string {
	c.traceMu.Lock()
	defer c.traceMu.Unlock()
	return c.traceparent
}

// Direction returns whether the call is inbound or outbound.
func (c *Call) Direction() Direction { return c.direction }

// From returns the remote URI / number recorded for this call.
func (c *Call) From() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.from
}

// To returns the local URI / number recorded for this call.
func (c *Call) To() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.to
}

// SetParties records the from/to URIs. Should be called before announcing the
// call to subscribers (i.e. just after NewCall).
func (c *Call) SetParties(from, to string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.from = from
	c.to = to
}

// CreatedAt returns the time NewCall was invoked (UTC).
func (c *Call) CreatedAt() time.Time { return c.createdAt }

// State returns the current state under a read lock.
func (c *Call) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// IsEnded reports whether the call has reached the terminal state.
func (c *Call) IsEnded() bool { return c.ended.Load() }

// Subscribe returns the event channel. The channel is closed when the call
// reaches StateEnded — subscribers should select on it accordingly.
//
// A Call has a single broadcast channel: every event goes to every subscriber
// because there is at most one consumer (the SDK transport layer) per Call.
func (c *Call) Subscribe() <-chan Event {
	return c.events
}

// SetState performs a state transition + emits an EventStateChange. If the
// transition is illegal, no change is made and ErrInvalidTransition is
// returned.
func (c *Call) SetState(next State) error {
	c.mu.Lock()
	from := c.state
	if from == StateEnded {
		c.mu.Unlock()
		return ErrEnded
	}
	if !canTransition(from, next) {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, from, next)
	}
	c.state = next
	if next == StateAnswered {
		c.answered.Store(true)
	}
	c.mu.Unlock()

	c.emit(NewStateChange(from, next))

	if next == StateEnded {
		c.closeEvents()
	}
	return nil
}

// EverAnswered reports whether the call reached the answered state before it
// ended. It remains true after hangup so artifact finalizers can distinguish
// a pre-answer announcement from a normal conversation recording.
func (c *Call) EverAnswered() bool { return c.answered.Load() }

// Emit pushes an arbitrary event to subscribers. Drops the event if the
// channel buffer is full (slow subscribers do not block the publisher).
func (c *Call) Emit(e Event) {
	c.emit(e)
}

func (c *Call) emit(e Event) {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	if c.ended.Load() {
		return
	}
	select {
	case c.events <- e:
	default:
		c.log.Warn("event dropped (slow subscriber)", "type", e.Type)
	}
}

// Hangup transitions the call to StateEnded (if not already), emits an
// EventHangup, and closes the event channel. Idempotent.
func (c *Call) Hangup(reason string) error {
	c.mu.Lock()
	from := c.state
	if from == StateEnded {
		c.mu.Unlock()
		return nil // idempotent
	}
	c.state = StateEnded
	// Capture the logger under the lock: SetTraceID may swap c.log concurrently
	// (e.g. a late inbound trace-set racing this hangup), so read it here rather
	// than after the unlock.
	logger := c.log
	c.mu.Unlock()

	durationMS := time.Since(c.createdAt).Milliseconds()
	observability.CallTraceCall(context.Background(), logger, "call.ended", c,
		"reason", reason,
		"duration_ms", durationMS,
		"direction", c.direction.String(),
	)
	c.emit(NewStateChange(from, StateEnded))
	c.emit(NewHangup(reason, durationMS))
	c.closeEvents()
	return nil
}

func (c *Call) closeEvents() {
	c.closeOnce.Do(func() {
		c.eventsMu.Lock()
		c.ended.Store(true)
		close(c.events)
		c.eventsMu.Unlock()
		// Cancel after ended=true so observers that wake on Done see IsEnded.
		// cancel and Done share one channel; do not close it separately.
		c.cancel()
		// Tear down media plumbing: close MediaIO (which closes its channels)
		// and close the audio bus so any subscribers (ASR) see EOF.
		c.mediaMu.Lock()
		mio := c.mediaIO
		c.mediaMu.Unlock()
		if mio != nil {
			_ = mio.Close()
		}
		// Once: Hangup and peer-EOF both reach here via closeOnce. Emitted after
		// MediaIO close so the pump has stopped accepting; do not wait on
		// pumpDone — Hangup("media_eof") runs inside pumpIncoming.
		c.emitMediaSummary()
		if c.audioBus != nil {
			c.audioBus.Close()
		}
		// End the root call span (if the transport started one).
		c.traceMu.Lock()
		end := c.endRootSpan
		c.endRootSpan = nil
		c.traceMu.Unlock()
		if end != nil {
			end()
		}
	})
}

func (c *Call) emitMediaSummary() {
	// media_ms may lag by up to ~1–2 frames: we do not wait for pumpDone here
	// (EOF Hangup runs inside pumpIncoming), so a final Publish/Store can still
	// be in flight. Do not chase tens-of-ms lost_ms as a hard fault.
	mediaMS := c.MediaTime().Milliseconds()
	now := time.Now().UnixNano()
	start := c.mediaWallStartNs.Load()
	last := c.mediaWallLastNs.Load()
	var wallMS, tailIdleMS int64
	if start > 0 && last > 0 {
		wallMS = (last - start) / int64(time.Millisecond)
		tailIdleMS = (now - last) / int64(time.Millisecond)
	}
	var busDropped uint64
	var busSubs int
	if c.audioBus != nil {
		busDropped = c.audioBus.DroppedTotal()
		busSubs = c.audioBus.SubscriberCount()
	}
	c.mu.RLock()
	logger := c.log
	c.mu.RUnlock()
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	transportKind := "none"
	if mio != nil {
		transportKind = string(mio.Kind())
	}
	fields := []any{
		"transport", transportKind,
		"media_ms", mediaMS,
		"wall_ms", wallMS,
		"lost_ms", wallMS - mediaMS,
		"tail_idle_ms", tailIdleMS,
		"held_frames", c.heldFrames.Load(),
		"muted_frames", c.mutedFrames.Load(),
		"no_output_frames", c.noOutputFrames.Load(),
		"bus_dropped", busDropped,
		"bus_subscribers", busSubs,
	}
	// Only some transports can receive audio they never agreed to; asking via
	// an optional method keeps a field that is meaningless for a browser leg
	// out of that leg's summary.
	if counter, ok := mio.(interface{ ForeignPackets() uint64 }); ok {
		fields = append(fields, "foreign_pt_packets", counter.ForeignPackets())
	}
	if stats, ok := mio.(interface{ PlaybackStats() pacer.Stats }); ok {
		snapshot := stats.PlaybackStats()
		fields = append(fields,
			"pacer_frames_sent", snapshot.FramesSent,
			"pacer_silence_frames", snapshot.SilenceFrames,
			"pacer_underrun_events", snapshot.UnderrunEvents,
			"pacer_keepalive_frames", snapshot.KeepaliveFrames,
			"pacer_dropped_frames", snapshot.FramesDropped,
			"pacer_slow_writes", snapshot.PaceResets,
			"pacer_underrun_limit_hits", snapshot.UnderrunLimitHits,
			"pacer_duck_events", snapshot.DuckEvents,
			"pacer_ducked_frames", snapshot.DuckedFrames,
			"pacer_pause_events", snapshot.PauseEvents,
			"pacer_paused_ms", snapshot.PausedMs,
			"pacer_send_gap_overruns", snapshot.SendGapOverruns,
			"pacer_max_send_gap_ms", snapshot.MaxSendGapMs,
		)
	}
	observability.CallTraceCall(c.RootSpanContext(), logger, "media.summary", c, fields...)
}

// AudioBus returns the per-call AudioBus. Subscribers (ASR, optional
// recording) call Subscribe() before BindMediaIO so they don't miss frames.
// The bus is closed when the call reaches StateEnded.
func (c *Call) AudioBus() *bus.Bus { return c.audioBus }

// BindMediaIO attaches a transport.MediaIO to this Call. From this point:
//   - frames arriving on io.IncomingPCM() are published to AudioBus()
//   - callers may write to io.OutgoingPCM() (also accessible via WriteOutgoing)
//   - when io closes (peer disconnect), Call auto-hangs-up with reason
//     "media_eof", or a more specific reason the transport reports (e.g.
//     "media_timeout" after SIP RTP idle)
//
// Calling BindMediaIO more than once is a no-op (logged).
func (c *Call) BindMediaIO(io transport.MediaIO) error {
	if io == nil {
		return errors.New("session: BindMediaIO got nil")
	}
	if c.IsEnded() {
		return ErrEnded
	}
	c.mediaMu.Lock()
	if c.mediaIO != nil {
		c.mediaMu.Unlock()
		c.log.Warn("BindMediaIO called twice — ignoring second binding")
		return nil
	}
	c.mediaIO = io
	c.pumpDone = make(chan struct{})
	c.mediaMu.Unlock()

	have, missing := probeMediaIOCaps(io)
	observability.CallTraceCall(c.RootSpanContext(), c.log, "media.bind", c,
		"transport", string(io.Kind()),
		"have", have,
		"missing", missing,
	)

	// If render sinks were registered before media bound, wire the fan-out to a
	// post-pacing transport now (usually sinks attach after binding, but be safe).
	c.renderMu.Lock()
	hasSinks := len(c.renderSinks) > 0
	c.renderMu.Unlock()
	if hasSinks {
		io.SetSentTap(c.dispatchRender)
		c.tapViaTransport.Store(true)
	}

	go c.pumpIncoming()
	return nil
}

// optionalMediaIOCaps lists capabilities that remain transport-specific.
// Paced-playback methods are required on MediaIO itself (shared pacer kernel);
// only DTMF still varies by transport. Keep in sync with internal/transport/io.go.
var optionalMediaIOCaps = []struct {
	name string
	has  func(transport.MediaIO) bool
}{
	{"DTMFReceiver", func(io transport.MediaIO) bool { _, ok := io.(transport.DTMFReceiver); return ok }},
	{"DTMFSender", func(io transport.MediaIO) bool { _, ok := io.(transport.DTMFSender); return ok }},
}

func probeMediaIOCaps(io transport.MediaIO) (have, missing string) {
	var haveList, missingList []string
	for _, item := range optionalMediaIOCaps {
		if item.has(io) {
			haveList = append(haveList, item.name)
		} else {
			missingList = append(missingList, item.name)
		}
	}
	return joinMediaCaps(haveList), joinMediaCaps(missingList)
}

// joinMediaCaps formats capability names for log/OTLP search (comma-separated).
// Empty lists become "none" so queries never match a blank field.
func joinMediaCaps(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ",")
}

// MediaIO returns the bound transport (nil if not yet bound).
func (c *Call) MediaIO() transport.MediaIO {
	c.mediaMu.Lock()
	defer c.mediaMu.Unlock()
	return c.mediaIO
}

// IsHeld reports whether the call is in local hold (media gated).
func (c *Call) IsHeld() bool { return c.held.Load() }

// SetHeld toggles local hold. When held, incoming frames are not published
// to AudioBus and WriteOutgoing drops frames. State must be Active→Held or
// Held→Active.
func (c *Call) SetHeld(held bool) error {
	if c.IsEnded() {
		return ErrEnded
	}
	cur := c.State()
	if held {
		if cur == StateAnswered {
			if err := c.SetState(StateActive); err != nil {
				return err
			}
			cur = StateActive
		}
		if cur != StateActive {
			return fmt.Errorf("%w: %s → held", ErrInvalidTransition, cur)
		}
		c.held.Store(true)
		return c.SetState(StateHeld)
	}
	if cur != StateHeld {
		return nil // idempotent
	}
	c.held.Store(false)
	return c.SetState(StateActive)
}

// IsMuted reports whether this party's own microphone is gated.
func (c *Call) IsMuted() bool { return c.muted.Load() }

// SetMuted gates only what this party sends. They stay in the conversation and
// keep hearing the other side, so this is not hold and carries no state change
// — a muted call is still Active or Bridged.
//
// Callers who can mute locally (a browser switching off its own track) should
// keep doing that; this exists for the ones who cannot, such as an agent on a
// SIP handset being muted from a web console.
func (c *Call) SetMuted(muted bool) error {
	if c.IsEnded() {
		return ErrEnded
	}
	c.muted.Store(muted)
	return nil
}

// AddRenderSink registers a non-blocking callback for outgoing (played) PCM under
// id; call RemoveRenderSink(id) to detach. Sinks must copy samples they retain.
// Frames arrive at the transport's paced send time once media is bound (correct
// echo timing); before bind, WriteOutgoing taps at enqueue time.
func (c *Call) AddRenderSink(id string, fn func(samples []int16)) {
	if fn == nil {
		return
	}
	c.renderMu.Lock()
	if c.renderSinks == nil {
		c.renderSinks = make(map[string]func(samples []int16))
	}
	c.renderSinks[id] = fn
	c.renderMu.Unlock()
	c.bindRenderTap()
}

// RemoveRenderSink detaches a previously added render sink.
func (c *Call) RemoveRenderSink(id string) {
	c.renderMu.Lock()
	delete(c.renderSinks, id)
	empty := len(c.renderSinks) == 0
	c.renderMu.Unlock()
	if !empty {
		return
	}
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	if mio != nil {
		mio.SetSentTap(nil)
	}
	c.tapViaTransport.Store(false)
}

// SetCaptureProcessor installs (or clears, with nil) the mic pre-processor run
// before frames are published to the AudioBus.
func (c *Call) SetCaptureProcessor(p CaptureProcessor) {
	if p == nil {
		c.captureProc.Store(nil)
		return
	}
	c.captureProc.Store(&p)
}

// bindRenderTap points the transport's paced send tap at the fan-out dispatcher.
// Until media is bound, WriteOutgoing uses its enqueue-time fallback.
func (c *Call) bindRenderTap() {
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	if mio == nil {
		c.tapViaTransport.Store(false)
		return
	}
	mio.SetSentTap(c.dispatchRender)
	c.tapViaTransport.Store(true)
}

// dispatchRender fans one played frame out to all render sinks. Sinks must be
// non-blocking; the mutex only guards the (tiny) sink map.
func (c *Call) dispatchRender(samples []int16) {
	c.renderMu.Lock()
	for _, fn := range c.renderSinks {
		fn(samples)
	}
	c.renderMu.Unlock()
}

// WriteOutgoing sends one frame of 16 kHz mono PCM towards the peer. Returns
// an error if no MediaIO is bound or the call has ended.
func (c *Call) WriteOutgoing(samples []int16) error {
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	if mio == nil {
		return errors.New("session: no MediaIO bound")
	}
	if c.IsEnded() {
		return ErrEnded
	}
	if c.held.Load() {
		return nil
	}
	if !c.tapViaTransport.Load() {
		c.dispatchRender(samples)
	}
	select {
	case mio.OutgoingPCM() <- samples:
		return nil
	case <-c.Done():
		return ErrEnded
	}
}

// WriteOutgoingCtx is WriteOutgoing that also aborts when ctx is cancelled.
// Used by paced playback so a barge-in (play context cancel) stops feeding
// frames immediately instead of blocking on a full outgoing buffer — otherwise
// the transport would keep draining already-queued audio after the stop.
func (c *Call) WriteOutgoingCtx(ctx context.Context, samples []int16) error {
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	if mio == nil {
		return errors.New("session: no MediaIO bound")
	}
	if c.IsEnded() {
		return ErrEnded
	}
	if c.held.Load() {
		return nil
	}
	if !c.tapViaTransport.Load() {
		c.dispatchRender(samples)
	}
	select {
	case mio.OutgoingPCM() <- samples:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.Done():
		return ErrEnded
	}
}

// pumpIncoming drains the bound MediaIO incoming channel into the AudioBus.
// Runs in a dedicated goroutine; exits when:
//   - MediaIO.IncomingPCM() is closed (peer end) → triggers auto-hangup
//   - the Call has already ended (closeEvents closed MediaIO first)
func (c *Call) pumpIncoming() {
	defer func() {
		if c.pumpDone != nil {
			close(c.pumpDone)
		}
	}()
	c.mediaMu.Lock()
	mio := c.mediaIO
	c.mediaMu.Unlock()
	if mio == nil {
		return
	}

	const sampleRate = bus.SampleRate
	var ts time.Duration
	// Reused silence for muted frames. Never written to, so publishing it by
	// reference is safe even though subscribers see the same backing array.
	var silence []int16
	source := "transport"
	for samples := range mio.IncomingPCM() {
		now := time.Now().UnixNano()
		frameDur := time.Duration(len(samples)) * time.Second / time.Duration(sampleRate)
		// Anchor wall zero at the first frame's audio begin, not its arrival.
		c.mediaWallStartNs.CompareAndSwap(0, now-int64(frameDur))
		c.mediaWallLastNs.Store(now)
		if c.ended.Load() {
			return
		}
		if c.held.Load() {
			c.heldFrames.Add(1)
			continue
		}
		// Pre-process (e.g. AEC3 echo cancellation) before anything downstream
		// (VAD/ASR/recording) sees the frame. An empty result means the processor
		// buffered a sub-frame; skip publishing until it emits samples.
		if p := c.captureProc.Load(); p != nil {
			samples = (*p).Process(samples)
			if len(samples) == 0 {
				c.noOutputFrames.Add(1)
				continue
			}
		}
		if c.muted.Load() {
			// Empty the frame rather than drop it, which is what separates
			// this from hold: the call is still running, so the media clock
			// has to keep advancing and the recording has to stay aligned
			// with the other leg. Emptying here — after the capture processor
			// — also leaves echo cancellation converged on the real
			// microphone signal, so lifting the mute takes effect at once.
			if len(silence) < len(samples) {
				silence = make([]int16, len(samples))
			}
			samples = silence[:len(samples)]
			c.mutedFrames.Add(1)
		}
		frameTS := ts
		ts += time.Duration(len(samples)) * time.Second / time.Duration(sampleRate)
		c.mediaTimeNs.Store(int64(ts))
		c.audioBus.Publish(bus.Frame{
			Samples:   samples,
			Timestamp: frameTS,
			Source:    source,
		})
	}
	// Channel closed by transport → peer disconnected. Hang up if not already.
	if !c.ended.Load() {
		reason := "media_eof"
		c.mediaMu.Lock()
		mio := c.mediaIO
		c.mediaMu.Unlock()
		if reporter, ok := mio.(interface{ MediaEOFReason() string }); ok {
			if specific := reporter.MediaEOFReason(); specific != "" {
				reason = specific
			}
		}
		_ = c.Hangup(reason)
	}
}
