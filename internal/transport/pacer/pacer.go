// Package pacer is the shared 20 ms send-pacing kernel for telephony MediaIO
// transports (SIP, WebRTC, WS-PCM).
//
// Each transport previously owned its own paced-sender loop; those loops
// diverged on trailing-frame padding, pause draining, tick phase, stop, and
// under-run hold. This package owns those decisions once so transports only
// implement Sink — encode one PCM frame and put it on the wire.
//
// Embed Outgoing in a transport MediaIO to expose the transport-named playback
// methods (PauseOutgoing, WaitPlaybackIdle, …) without per-transport stubs.
// See Outgoing for the two-step Bind construction and Close obligation.
//
// Sink.SendFrame is called exclusively from the pacer's single send goroutine;
// implementations need not synchronize among themselves.
//
// Concurrency:
//   - OutgoingPCM may be written from any goroutine (typically one producer).
//     Close never closes that channel, so a racing write after Close cannot
//     panic.
//   - Pause, Resume, Stop, Duck, Restore, SetFeeding, SetSentTap, WaitIdle,
//     and Close are safe from any goroutine.
//   - Stats accessors are safe from any goroutine.
package pacer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// SampleRate is the PCM sample rate the kernel paces at.
	SampleRate = 16000
	// FrameSamples is one 20 ms mono frame at SampleRate.
	FrameSamples = SampleRate / 50 // 320
	// FrameDuration is the nominal send interval.
	FrameDuration = 20 * time.Millisecond
	// minimumPaceSpacing prevents deadline correction from compressing packets
	// into a burst that a carrier jitter buffer may render as clipped speech.
	minimumPaceSpacing = FrameDuration * 3 / 4

	// sendGapOverrunThreshold flags consecutive in-burst send completions
	// farther apart than 2×FrameDuration: a hole that large starves the far-end
	// (carrier) jitter buffer and renders as a stutter on the wire.
	sendGapOverrunThreshold = 2 * FrameDuration

	// MaxUnderrunHoldFrames caps consecutive silence frames inserted while
	// SetFeeding(true) and the queue is empty (250 × 20 ms = 5 s). Without a
	// cap, a stuck TTS/LLM or a caller that never clears feeding would emit
	// silence forever: WaitIdle never returns and the call appears healthy in
	// logs. Hitting the cap ends the burst (idle) so the upper layer can
	// proceed; UnderrunLimitHits counts how often this safety valve fired.
	MaxUnderrunHoldFrames = 250

	defaultQueue = 64
)

// ErrClosed is returned by WaitIdle after Close.
var ErrClosed = errors.New("pacer: closed")

// Sink is the transport-specific half of paced send: encode one PCM frame and
// put it on the wire. It is invoked only from the pacer send goroutine.
type Sink interface {
	SendFrame(samples []int16) error
}

// Stats are cumulative counters for call-end diagnostics. They are never
// logged inside this package.
type Stats struct {
	FramesSent        uint64 // frames delivered to Sink (audio + underrun silence + idle keepalive)
	SilenceFrames     uint64 // underrun hold frames (subset of FramesSent)
	UnderrunEvents    uint64 // times the queue ran dry mid-burst and silence was held
	KeepaliveFrames   uint64 // idle keepalive silence frames (subset of FramesSent)
	FramesDropped     uint64 // whole frames discarded by Stop/Close
	PaceResets        uint64 // times the absolute pace clock was safely realigned
	UnderrunLimitHits uint64 // bursts ended because underrun silence hit the hold cap
	DuckEvents        uint64 // times Duck with gain<1 took effect (barge-in attenuation)
	DuckedFrames      uint64 // frames sent while gain<1 (audible dip, no gap on the wire)
	PauseEvents       uint64 // !paused→paused transitions (idempotent repeats not counted)
	PausedMs          uint64 // wall-clock ms spent paused (settled on Resume/Stop/Close)
	SendGapOverruns   uint64 // in-burst send-completion gaps over sendGapOverrunThreshold
	MaxSendGapMs      uint64 // largest such gap, ms
}

// Pacer paces 16 kHz mono PCM onto a Sink at FrameDuration.
type Pacer struct {
	sink     Sink
	outgoing chan []int16
	cmds     chan command

	underrunHoldLimit int           // consecutive silence frames; default MaxUnderrunHoldFrames
	idleKeepalive     bool          // pace silence while idle instead of blocking (WithIdleKeepalive)
	sendAhead         time.Duration // audio sent ahead of real time (WithSendAhead); 0 = strict real time

	closed atomic.Bool
	done   chan struct{}
	once   sync.Once

	mu          sync.Mutex
	idle        bool
	idleWaiters []chan struct{}

	framesSent        atomic.Uint64
	silenceFrames     atomic.Uint64
	underrunEvents    atomic.Uint64
	keepaliveFrames   atomic.Uint64
	framesDropped     atomic.Uint64
	paceResets        atomic.Uint64
	underrunLimitHits atomic.Uint64
	duckEvents        atomic.Uint64
	duckedFrames      atomic.Uint64
	pauseEvents       atomic.Uint64
	pausedMs          atomic.Uint64
	sendGapOverruns   atomic.Uint64
	maxSendGapMs      atomic.Uint64
}

type cmdKind int

const (
	cmdPause cmdKind = iota
	cmdResume
	cmdStop
	cmdDuck
	cmdRestore
	cmdSetFeeding
	cmdSetTap
	cmdSyncIdle // WaitIdle probe: drain OutgoingPCM then report idle
	cmdClose
)

type command struct {
	kind       cmdKind
	gain       float64
	feeding    bool
	tap        func([]int16)
	ack        chan struct{} // closed when the send goroutine has applied the cmd
	idleReply  chan<- bool   // cmdSyncIdle: true if idle after drain
	idleWaiter chan struct{} // cmdSyncIdle: registered under mu when not idle
}

// Option configures New.
type Option func(*config)

type config struct {
	queue             int
	underrunHoldLimit int
	idleKeepalive     bool
	sendAhead         time.Duration
}

// WithQueue sets the OutgoingPCM buffer depth (default 64).
func WithQueue(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.queue = n
		}
	}
}

// WithUnderrunHoldLimit overrides MaxUnderrunHoldFrames for this pacer.
// Intended for tests so the 5 s production cap need not be waited out.
func WithUnderrunHoldLimit(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.underrunHoldLimit = n
		}
	}
}

// WithIdleKeepalive keeps the media stream alive while idle: when the queue is
// empty and no burst is feeding, the send loop paces all-zero PCM frames at
// FrameDuration (encoded by the Sink as G.711 silence) instead of blocking.
// Carrier SBCs and phone jitter buffers treat a multi-second send hole as a
// VAD/DTX state change, so audio after the hole renders as a stutter; an
// unbroken stream avoids that. Pause still stops sending entirely, and
// WaitIdle semantics are unchanged — idle stays a queue state, not a wire
// state. Default off.
func WithIdleKeepalive(enabled bool) Option {
	return func(c *config) {
		c.idleKeepalive = enabled
	}
}

// WithSendAhead lets each burst run up to d ahead of real time, for a peer
// that buffers and plays out on its own clock (a browser over WebSocket). The
// peer's buffer then absorbs network and scheduling jitter that would otherwise
// be heard as a stutter. Real-time peers (RTP into a carrier or phone jitter
// buffer) must leave this at 0: a burst overflows their buffer.
//
// The peer holds up to d of audio the pacer already considers sent, so it must
// also apply Stop / Pause / Duck itself (the transport forwards them); the
// pacer alone can only act on audio it has not sent yet.
func WithSendAhead(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.sendAhead = d
		}
	}
}

// New starts a paced sender bound to sink. The send goroutine runs until Close.
func New(sink Sink, opts ...Option) *Pacer {
	cfg := config{queue: defaultQueue, underrunHoldLimit: MaxUnderrunHoldFrames}
	for _, o := range opts {
		o(&cfg)
	}
	p := &Pacer{
		sink:              sink,
		outgoing:          make(chan []int16, cfg.queue),
		cmds:              make(chan command, 16),
		underrunHoldLimit: cfg.underrunHoldLimit,
		idleKeepalive:     cfg.idleKeepalive,
		sendAhead:         cfg.sendAhead,
		done:              make(chan struct{}),
		idle:              true,
	}
	go p.loop()
	return p
}

// OutgoingPCM is the enqueue channel for PCM of any length. Owned by the
// pacer; callers must not close it.
func (p *Pacer) OutgoingPCM() chan<- []int16 { return p.outgoing }

// Pause freezes sending without dropping queued or enqueued PCM. While paused
// the send goroutine keeps draining OutgoingPCM into an internal buffer so
// producers are not blocked. Idempotent. Blocks until the send goroutine has
// observed the pause (or the pacer is closed).
func (p *Pacer) Pause() { p.sendCmdSync(command{kind: cmdPause}) }

// Resume continues after Pause from the same buffer position. Idempotent.
func (p *Pacer) Resume() { p.sendCmdSync(command{kind: cmdResume}) }

// Stop discards all unsent audio (queue + internal buffer), restores send
// gain to unity (so a prior Duck does not attenuate the next utterance),
// enters idle so WaitIdle returns, and leaves the pacer ready for new writes.
// Idempotent with respect to an already-idle pacer. The frame currently
// inside Sink.SendFrame is not interrupted; discard applies to everything
// still buffered after that call returns.
func (p *Pacer) Stop() { p.sendCmdSync(command{kind: cmdStop}) }

// Duck temporarily attenuates frames at send time without stopping playback.
// gain is clamped to (0, 1]; non-positive or >1 restores unity.
func (p *Pacer) Duck(gain float64) {
	p.sendCmdSync(command{kind: cmdDuck, gain: gain})
}

// Restore returns send gain to unity.
func (p *Pacer) Restore() { p.sendCmdSync(command{kind: cmdRestore}) }

// SetSentTap registers a callback invoked at the moment a frame is handed to
// Sink (after ducking). The callback must not block. If it retains samples it
// must copy them. Pass nil to clear.
func (p *Pacer) SetSentTap(fn func([]int16)) {
	p.sendCmdSync(command{kind: cmdSetTap, tap: fn})
}

// SetFeeding brackets a bursty source (e.g. streaming TTS). While true, an
// empty queue during an active burst emits paced silence instead of going
// idle, up to MaxUnderrunHoldFrames consecutive silence frames; beyond that
// the burst ends (idle) even if feeding remains true. When false, draining
// to empty (after any padded tail) enters idle.
//
// Feeding takes effect only after the first PCM chunk has started a burst —
// matching SIP MediaIO: SetPlaybackFeeding alone does not start a silence
// stream while the sender is still blocked waiting for the first chunk.
// Silence hold applies to mid-burst underruns, not to the pre-playback idle
// state.
func (p *Pacer) SetFeeding(feeding bool) {
	p.sendCmdSync(command{kind: cmdSetFeeding, feeding: feeding})
}

// WaitIdle blocks until there is no queued or in-flight outgoing audio (or
// Stop forced idle), the pacer is closed, or ctx is done.
//
// Idle is decided on the send goroutine after a sync command drains
// OutgoingPCM into residual. The same critical section registers the caller's
// waiter when not idle, so there is no probe→register gap and no retry loop.
//
// If ctx is canceled after the waiter was registered, this returns ctx.Err()
// and leaves the waiter channel in idleWaiters until the next signalIdle
// closes it. That is safe: the channel is never reused or received from again.
func (p *Pacer) WaitIdle(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	reply := make(chan bool, 1)
	waiter := make(chan struct{})
	cmd := command{kind: cmdSyncIdle, idleReply: reply, idleWaiter: waiter}
	select {
	case p.cmds <- cmd:
	case <-p.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case idle := <-reply:
		if idle {
			return nil
		}
	case <-p.done:
		return ErrClosed
	case <-ctx.Done():
		// Sync may still register waiter; it will be closed by signalIdle later.
		return ctx.Err()
	}
	select {
	case <-waiter:
		if p.closed.Load() {
			return ErrClosed
		}
		return nil
	case <-p.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the send goroutine. Idempotent. OutgoingPCM stays open so a
// racing write cannot panic; further writes are drained and discarded.
func (p *Pacer) Close() {
	p.once.Do(func() {
		p.closed.Store(true)
		ack := make(chan struct{})
		select {
		case p.cmds <- command{kind: cmdClose, ack: ack}:
			select {
			case <-ack:
			case <-time.After(2 * time.Second):
				// Send goroutine stuck in Sink; still close done so waiters unblock.
			}
		default:
			// cmds full — force shutdown signal.
		}
		close(p.done)
		p.signalIdle()
	})
}

// Snapshot returns cumulative counters.
func (p *Pacer) Snapshot() Stats {
	return Stats{
		FramesSent:        p.framesSent.Load(),
		SilenceFrames:     p.silenceFrames.Load(),
		UnderrunEvents:    p.underrunEvents.Load(),
		KeepaliveFrames:   p.keepaliveFrames.Load(),
		FramesDropped:     p.framesDropped.Load(),
		PaceResets:        p.paceResets.Load(),
		UnderrunLimitHits: p.underrunLimitHits.Load(),
		DuckEvents:        p.duckEvents.Load(),
		DuckedFrames:      p.duckedFrames.Load(),
		PauseEvents:       p.pauseEvents.Load(),
		PausedMs:          p.pausedMs.Load(),
		SendGapOverruns:   p.sendGapOverruns.Load(),
		MaxSendGapMs:      p.maxSendGapMs.Load(),
	}
}

func (p *Pacer) sendCmdSync(c command) {
	if p.closed.Load() {
		return
	}
	ack := make(chan struct{})
	c.ack = ack
	select {
	case p.cmds <- c:
	case <-p.done:
		return
	}
	select {
	case <-ack:
	case <-p.done:
	}
}

func (p *Pacer) markActive() {
	p.mu.Lock()
	p.idle = false
	p.mu.Unlock()
}

func (p *Pacer) signalIdle() {
	p.mu.Lock()
	p.idle = true
	waiters := p.idleWaiters
	p.idleWaiters = nil
	p.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// acceptChunk appends PCM from OutgoingPCM and marks playback active.
func (p *Pacer) acceptChunk(st *loopState, chunk []int16) {
	if p.closed.Load() {
		return
	}
	p.markActive()
	st.residual = append(st.residual, chunk...)
}

type loopState struct {
	paused      bool
	pausedSince time.Time // set on the !paused→paused transition
	feeding     bool
	gain        float64 // linear; 1 = unity
	tap         func([]int16)
	residual    []int16
	lastSentAt  time.Time // last frame handed to Sink (burst or keepalive)
	// carryAhead is how far ahead of real time the burst was when it paused.
	// The peer pauses with that audio still buffered, so the resumed burst
	// starts from it instead of sending another full lead on top.
	carryAhead time.Duration
	// peerUntil is when the peer finishes playing what a burst sent ahead
	// before going idle. An utterance that follows straight away continues
	// from there, so back-to-back utterances do not each add a full lead.
	peerUntil time.Time
}

func (p *Pacer) loop() {
	st := loopState{gain: 1}
	silence := make([]int16, FrameSamples)

	for {
		if p.closed.Load() && len(st.residual) == 0 {
			p.discardBuffered(&st)
			p.signalIdle()
			return
		}

		// Idle / paused with nothing to send: wait for audio or control.
		if len(st.residual) == 0 {
			if !p.waitForWork(&st, silence) {
				return
			}
			if len(st.residual) == 0 {
				continue
			}
		}

		if st.paused {
			if !p.pauseDrain(&st) {
				return
			}
			continue
		}

		if !p.playBurst(&st, silence) {
			return
		}
	}
}

// waitForWork blocks until residual has samples, a relevant cmd arrives, or close.
// Returns false when the loop should exit.
//
// With idle keepalive on (and not paused) the wait is paced instead of
// blocking: one silence frame goes to the Sink every FrameDuration so the RTP
// stream never goes quiet between utterances. The cadence continues the
// preceding burst's (st.lastSentAt), and audio arriving mid-wait is handed to
// playBurst at the next frame boundary, so the wire stream — and with it RTP
// sequence numbers and timestamps — stays contiguous across
// audio→silence→audio. Pause is an intentional stop and freezes keepalive
// too; cmds and new audio are still handled within one frame period.
func (p *Pacer) waitForWork(st *loopState, silence []int16) bool {
	keepalive := p.idleKeepalive && !st.paused
	var nextTick time.Time
	if keepalive {
		nextTick = firstKeepaliveTick(st)
	}
	for {
		var timer *time.Timer
		var tick <-chan time.Time
		if keepalive {
			d := time.Until(nextTick)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
			tick = timer.C
		}
		select {
		case <-p.done:
			stopTimer(timer)
			p.discardBuffered(st)
			p.signalIdle()
			return false
		case c := <-p.cmds:
			stopTimer(timer)
			if p.applyCmd(st, c) {
				p.discardBuffered(st)
				p.signalIdle()
				return false
			}
			// Stop while idle: already idle.
			if c.kind == cmdStop {
				p.signalIdle()
			}
			// cmdSyncIdle (and any future drain) may have filled residual.
			if len(st.residual) > 0 {
				return true
			}
			keepalive = p.idleKeepalive && !st.paused
			if keepalive {
				nextTick = firstKeepaliveTick(st)
			}
		case chunk := <-p.outgoing:
			stopTimer(timer)
			p.acceptChunk(st, chunk)
			if !keepalive {
				return true
			}
			// Keepalive: deliver at the next frame boundary (the tick case
			// returns once residual is non-empty) so the cadence never breaks.
		case <-tick:
			if len(st.residual) > 0 {
				return true
			}
			if !p.sendKeepalive(st, silence) {
				// Transport failure: stop ticking and fall back to the plain
				// blocking wait; Close tears the goroutine down.
				keepalive = false
				continue
			}
			nextTick = nextTick.Add(FrameDuration)
			if !nextTick.After(st.lastSentAt) {
				nextTick = st.lastSentAt.Add(FrameDuration)
			}
		}
	}
}

// firstKeepaliveTick schedules the first keepalive frame on the previous
// send's absolute cadence while that cadence is still ahead of us; otherwise
// (cold start, or a pause/stop gap already longer than a frame) it sends
// immediately.
func firstKeepaliveTick(st *loopState) time.Time {
	if !st.lastSentAt.IsZero() {
		if t := st.lastSentAt.Add(FrameDuration); t.After(time.Now()) {
			return t
		}
	}
	return time.Now()
}

// sendKeepalive hands one all-zero PCM frame to the Sink — the exact voice
// path, so encoded silence rides the same RTP stream with continuous sequence
// numbers and timestamps. Reports false when the Sink failed.
func (p *Pacer) sendKeepalive(st *loopState, silence []int16) bool {
	out := applyGain(silence, st.gain)
	if st.tap != nil {
		st.tap(out)
	}
	if err := p.sink.SendFrame(out); err != nil {
		return false
	}
	st.lastSentAt = time.Now()
	p.framesSent.Add(1)
	p.keepaliveFrames.Add(1)
	return true
}

// stopTimer releases a pending wait timer, draining a late fire so the timer
// is not left to fire into an abandoned channel.
func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// pauseDrain keeps accepting OutgoingPCM until Resume, Stop, or Close.
func (p *Pacer) pauseDrain(st *loopState) bool {
	for st.paused {
		select {
		case <-p.done:
			p.discardBuffered(st)
			p.signalIdle()
			return false
		case c := <-p.cmds:
			if p.applyCmd(st, c) {
				p.discardBuffered(st)
				p.signalIdle()
				return false
			}
			if c.kind == cmdStop {
				p.signalIdle()
				return true
			}
		case chunk := <-p.outgoing:
			p.acceptChunk(st, chunk)
		}
	}
	return true
}

// playBurst sends frames with send-then-wait phase until idle, pause, stop, or close.
// Returns false when the loop should exit (closed).
func (p *Pacer) playBurst(st *loopState, silence []int16) bool {
	var deadline time.Time
	var underrunHeld int
	var underrunCapped bool
	// Send-gap measurement is per burst entry: pause/idle return out of
	// playBurst, so their wire holes (already covered by PausedMs) never land
	// in SendGapOverruns.
	var prevSentAt time.Time

	for {
		if stop, closed := p.pollCmds(st); closed {
			return false
		} else if stop {
			p.signalIdle()
			return true
		} else if st.paused {
			p.carryForPause(st, deadline)
			return true
		}
		p.drainOutgoing(st)

		// Ahead of real time with the source momentarily dry: the peer is
		// still playing what was sent, so wait for more audio instead of
		// queueing silence in front of it. Silence starts only once the
		// peer's buffer is down to a frame.
		if p.sendAhead > 0 && st.feeding && len(st.residual) < FrameSamples && !deadline.IsZero() {
			if hold := deadline.Add(-FrameDuration); time.Now().Before(hold) {
				if stop, closed := p.waitDeadline(st, hold, true); closed {
					return false
				} else if stop {
					p.signalIdle()
					return true
				}
				continue
			}
		}

		frame, isSilence, done := p.nextFrame(st, silence, &underrunHeld, &underrunCapped)
		if done || frame == nil {
			if p.sendAhead > 0 && !deadline.IsZero() {
				st.peerUntil = deadline
			}
			p.signalIdle()
			return true
		}

		out := applyGain(frame, st.gain)
		if st.tap != nil {
			st.tap(out)
		}
		writeStartedAt := time.Now()
		if err := p.sink.SendFrame(out); err != nil {
			// Transport failure: go idle; Close tears the goroutine down.
			p.signalIdle()
			return true
		}
		sentAt := time.Now()
		p.framesSent.Add(1)
		if isSilence {
			p.silenceFrames.Add(1)
		}
		if st.gain < 1 {
			p.duckedFrames.Add(1)
		}
		if !prevSentAt.IsZero() {
			if gap := sentAt.Sub(prevSentAt); gap > sendGapOverrunThreshold {
				p.sendGapOverruns.Add(1)
				// Only the send goroutine writes maxSendGapMs; Load+Store suffices.
				if gapMs := uint64(gap.Milliseconds()); gapMs > p.maxSendGapMs.Load() {
					p.maxSendGapMs.Store(gapMs)
				}
			}
		}
		prevSentAt = sentAt
		st.lastSentAt = sentAt

		// Keep an absolute media cadence so ordinary scheduler jitter does not
		// accumulate across a phrase. If catching up would leave less than a safe
		// packet interval, realign from the actual completed write instead of
		// emitting a burst into the carrier jitter buffer.
		if deadline.IsZero() {
			carry := st.carryAhead
			if left := st.peerUntil.Sub(sentAt); left > carry {
				carry = min(left, p.sendAhead)
			}
			deadline = sentAt.Add(FrameDuration + carry)
			st.carryAhead = 0
			st.peerUntil = time.Time{}
			if sentAt.Sub(writeStartedAt) >= FrameDuration {
				p.paceResets.Add(1)
			}
		} else {
			deadline = deadline.Add(FrameDuration)
		}
		// deadline is where the media clock stands after this frame; wake is
		// when the next frame goes out.
		wake := deadline
		if p.sendAhead > 0 {
			// Fallen behind real time (slow writes): restart the clock here
			// and rebuild the lead, rather than declaring audio the peer
			// never had time to play.
			if deadline.Before(sentAt) {
				p.paceResets.Add(1)
				deadline = sentAt.Add(FrameDuration)
				wake = deadline
			}
			// Underrun silence goes out in real time so it never sits in the
			// peer's buffer ahead of the audio that follows it.
			if !isSilence {
				wake = deadline.Add(-p.sendAhead)
			}
		} else if deadline.Sub(sentAt) < minimumPaceSpacing {
			p.paceResets.Add(1)
			deadline = sentAt.Add(FrameDuration)
			wake = deadline
		}
		if stop, closed := p.waitDeadline(st, wake, false); closed {
			return false
		} else if stop {
			p.signalIdle()
			return true
		} else if st.paused {
			p.carryForPause(st, deadline)
			return true
		}
	}
}

// nextFrame returns the next PCM frame to send.
// done=true means enter idle (no more audio, or underrun hold cap reached).
func (p *Pacer) nextFrame(st *loopState, silence []int16, underrunHeld *int, underrunCapped *bool) (frame []int16, isSilence bool, done bool) {
	if len(st.residual) >= FrameSamples {
		*underrunHeld = 0
		*underrunCapped = false
		frame = append([]int16(nil), st.residual[:FrameSamples]...)
		st.residual = st.residual[FrameSamples:]
		return frame, false, false
	}
	if st.feeding && !*underrunCapped {
		if *underrunHeld >= p.underrunHoldLimit {
			// Cap reached: count once, stop inserting silence, flush any
			// partial residual, then idle so WaitIdle can return.
			*underrunCapped = true
			p.underrunLimitHits.Add(1)
		} else {
			if *underrunHeld == 0 {
				p.underrunEvents.Add(1)
			}
			*underrunHeld++
			return silence, true, false
		}
	}
	if len(st.residual) > 0 {
		// Pad trailing partial frame with zeros — never drop the tail.
		padded := make([]int16, FrameSamples)
		copy(padded, st.residual)
		st.residual = st.residual[:0]
		return padded, false, false
	}
	return nil, false, true
}

// carryForPause records how much sent audio the peer still holds when a burst
// leaves for a pause (see loopState.carryAhead). Only send-ahead peers have any.
func (p *Pacer) carryForPause(st *loopState, deadline time.Time) {
	if p.sendAhead <= 0 || deadline.IsZero() {
		return
	}
	st.carryAhead = min(max(time.Until(deadline), 0), p.sendAhead)
}

// waitDeadline waits for deadline while applying commands and accepting audio.
// With untilAudio it also returns as soon as a full frame is buffered or any
// command was applied, so the caller re-evaluates what to send.
func (p *Pacer) waitDeadline(st *loopState, deadline time.Time, untilAudio bool) (stop, closed bool) {
	if stop, closed := p.pollCmds(st); stop || closed || st.paused {
		return stop, closed
	}
	for {
		d := time.Until(deadline)
		if d <= 0 {
			return p.pollCmds(st)
		}
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
			return p.pollCmds(st)
		case <-p.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.discardBuffered(st)
			p.signalIdle()
			return false, true
		case c := <-p.cmds:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if quit, stop := p.handleCmd(st, c); quit {
				p.discardBuffered(st)
				p.signalIdle()
				return false, true
			} else if stop || st.paused || untilAudio {
				return stop, false
			}
			// Other cmds (duck/feed/tap): continue waiting for the same deadline.
		case chunk := <-p.outgoing:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.acceptChunk(st, chunk)
			if untilAudio && len(st.residual) >= FrameSamples {
				return false, false
			}
			// Keep waiting for the same deadline so pacing does not drift.
		}
	}
}

// pollCmds non-blockingly applies queued control commands.
func (p *Pacer) pollCmds(st *loopState) (stop, closed bool) {
	for {
		select {
		case <-p.done:
			p.discardBuffered(st)
			p.signalIdle()
			return false, true
		case c := <-p.cmds:
			if quit, stop := p.handleCmd(st, c); quit {
				p.discardBuffered(st)
				p.signalIdle()
				return false, true
			} else if stop {
				return true, false
			}
		default:
			return false, false
		}
	}
}

// handleCmd applies c. quit=true means Close; stop=true means Stop.
func (p *Pacer) handleCmd(st *loopState, c command) (quit, stop bool) {
	quit = p.applyCmd(st, c)
	if quit {
		return true, false
	}
	return false, c.kind == cmdStop
}

// applyCmd mutates loop state. Returns true if the send loop must exit (Close).
func (p *Pacer) applyCmd(st *loopState, c command) (quit bool) {
	defer func() {
		if c.ack != nil {
			close(c.ack)
		}
	}()
	switch c.kind {
	case cmdPause:
		if !st.paused {
			st.paused = true
			st.pausedSince = time.Now()
			p.pauseEvents.Add(1)
		}
	case cmdResume:
		p.settlePause(st)
		st.paused = false
	case cmdStop:
		p.settlePause(st)
		p.discardBuffered(st)
		st.paused = false
		st.carryAhead = 0 // the peer flushes its buffer along with ours
		st.peerUntil = time.Time{}
		st.gain = 1       // Duck is per-utterance; next audio must start at unity
	case cmdDuck:
		g := c.gain
		if g <= 0 || g > 1 {
			g = 1
		}
		st.gain = g
		if g < 1 {
			p.duckEvents.Add(1)
		}
	case cmdRestore:
		st.gain = 1
	case cmdSetFeeding:
		st.feeding = c.feeding
	case cmdSetTap:
		st.tap = c.tap
	case cmdSyncIdle:
		// Drain first: cmds and OutgoingPCM are separate channels, so select
		// order alone cannot guarantee the probe observes a just-enqueued chunk.
		p.drainOutgoing(st)
		p.mu.Lock()
		isIdle := p.idle && len(st.residual) == 0
		if !isIdle && c.idleWaiter != nil {
			p.idleWaiters = append(p.idleWaiters, c.idleWaiter)
		}
		p.mu.Unlock()
		if c.idleReply != nil {
			c.idleReply <- isIdle
		}
	case cmdClose:
		p.settlePause(st)
		p.discardBuffered(st)
		return true
	}
	return false
}

// settlePause closes an open pause interval, adding its wall-clock duration to
// PausedMs. Called on every transition out of paused (Resume/Stop/Close) so
// the final segment is neither lost nor double-counted.
func (p *Pacer) settlePause(st *loopState) {
	if st.paused && !st.pausedSince.IsZero() {
		p.pausedMs.Add(uint64(time.Since(st.pausedSince).Milliseconds()))
		st.pausedSince = time.Time{}
	}
}

func (p *Pacer) drainOutgoing(st *loopState) {
	for {
		select {
		case chunk := <-p.outgoing:
			p.acceptChunk(st, chunk)
		default:
			return
		}
	}
}

// discardBuffered drops residual and queued OutgoingPCM, counting dropped
// frames from the total sample count once (ceil), so short chunks are not
// each rounded up independently.
func (p *Pacer) discardBuffered(st *loopState) {
	total := len(st.residual)
	st.residual = st.residual[:0]
	for {
		select {
		case chunk := <-p.outgoing:
			total += len(chunk)
		default:
			if total > 0 {
				frames := (total + FrameSamples - 1) / FrameSamples
				p.framesDropped.Add(uint64(frames))
			}
			return
		}
	}
}

func applyGain(frame []int16, gain float64) []int16 {
	if gain >= 1 {
		// Still copy so Sink/tap cannot mutate the residual-backed slice.
		out := make([]int16, len(frame))
		copy(out, frame)
		return out
	}
	out := make([]int16, len(frame))
	q15 := int32(gain*32768 + 0.5)
	for i, s := range frame {
		out[i] = int16((int32(s) * q15) >> 15)
	}
	return out
}
