package calls

import (
	"log/slog"
	"strings"
	"sync"
	"time"
)

// adaptiveBarge escalates barge-in actions with the amount of evidence that the
// user really wants to interrupt, instead of reacting audibly to raw VAD onsets:
//
//	idle → watching    VAD onset: observe only — by default nothing audible
//	                   happens, so noise and short acknowledgements never make
//	                   the bot stutter (onsetAction can opt back into duck/pause)
//	watching → ducked  enough real content accumulated (candidateChars): lower
//	                   the playback gain, keep talking quietly
//	ducked → yielded   sustained real content (confirmChars): pause playback
//	                   losslessly and keep the in-flight reply, so a false
//	                   interrupt can resume from the exact same position
//	any → confirmed    explicit stop cue, or a real speech_end final: stop
//	                   playback and cancel the reply
//
// A backchannel ("嗯/对/好的"), an upstream-rejected utterance (onSpeechDiscard),
// or a timeout unwinds whatever audible action was taken: restore for a duck,
// resume for a pause. Rules only, no model. All callbacks run off the lock. One
// instance per call; ASR events arrive serially on the agent's subscribe
// goroutine while timers fire on their own goroutines — hence the mutex.
type adaptiveBarge struct {
	log    *slog.Logger
	callID string
	cfg    bargeCfg

	duck    func() bool // attenuate playback; true if something was playing
	restore func()      // restore playback to unity gain
	pause   func() bool // pause playback losslessly; true if something was playing
	resume  func()      // release a pause (continues from the same position)
	confirm func()      // stop playback + cancel in-flight LLM/turn

	mu       sync.Mutex
	state    int
	audible  int  // which audible action is currently applied
	observed bool // this utterance already ducked once and timed out; do not re-duck
	acc      strings.Builder
	timer    *time.Timer
	timerGen uint64
}

type bargeCfg struct {
	candidateChars int           // accumulated content runes → duck (candidate)
	confirmChars   int           // accumulated content runes → yield (lossless pause)
	maxPause       time.Duration // safety: unwind a duck never resolved by speech_end
	onsetGrace     time.Duration // watching with no real content → back to idle
	falseInterrupt time.Duration // yielded with no speech_end → resume playback
	onsetAction    string        // "none" (default) | "duck" | "pause" at VAD onset
	resumeOnFalse  bool          // resume after a false interrupt (false: stay stopped)
}

const (
	bargeIdle = iota
	bargeWatching
	bargeDucked
	bargeYielded
	bargeConfirmed
)

const (
	audibleNone = iota
	audibleDucked
	audiblePaused
)

func newAdaptiveBarge(callID string, cfg bargeCfg, log *slog.Logger,
	duck func() bool, restore func(), pause func() bool, resume, confirm func()) *adaptiveBarge {
	return &adaptiveBarge{
		log:     log,
		callID:  callID,
		cfg:     cfg,
		duck:    duck,
		restore: restore,
		pause:   pause,
		resume:  resume,
		confirm: confirm,
	}
}

// onSpeechStart enters watching on a VAD onset. By default this is inaudible:
// playback keeps going while we wait for ASR evidence, so noise and short
// acknowledgements never interrupt the bot. onsetAction restores the old
// duck/pause-on-onset behaviour for deployments that want it.
func (b *adaptiveBarge) onSpeechStart() {
	b.mu.Lock()
	if b.state != bargeIdle {
		b.mu.Unlock()
		return
	}
	b.state = bargeWatching
	b.armTimerLocked(b.cfg.onsetGrace, b.onWatchTimeout)
	b.mu.Unlock()

	switch b.cfg.onsetAction {
	case "duck":
		if b.duck() {
			b.setAudible(audibleDucked)
		}
	case "pause":
		if b.pause() {
			b.setAudible(audiblePaused)
		} else if b.duck() {
			b.setAudible(audibleDucked)
		}
	}
	b.log.Debug("barge watching", "call_id", b.callID)
}

// onPartial feeds one accepted ASR partial (incremental text) while the bot is
// playing, and escalates duck → pause → confirm as content accumulates.
func (b *adaptiveBarge) onPartial(text string) {
	b.mu.Lock()
	if b.state == bargeConfirmed {
		b.mu.Unlock()
		return
	}
	b.acc.WriteString(text)
	total := b.acc.String()
	runes := bargeContentRunes(total)
	st := b.state

	switch {
	case containsStopWord(total):
		// Explicit stop cue: zero-latency confirm from any state.
		b.state = bargeConfirmed
		b.audible = audibleNone
		b.stopTimerLocked()
		b.mu.Unlock()
		b.log.Info("barge confirmed", "call_id", b.callID, "reason", "stop_word", "text", total)
		b.confirm()

	case isBackchannelOnly(total):
		// Pure acknowledgement/filler: never an interruption. Unwind any
		// audible onset action; keep accumulating in case real speech follows.
		if b.audible == audibleNone {
			b.mu.Unlock()
			return
		}
		aud := b.audible
		b.audible = audibleNone
		if st == bargeDucked || st == bargeYielded {
			b.state = bargeWatching
			b.armTimerLocked(b.cfg.onsetGrace, b.onWatchTimeout)
		}
		b.mu.Unlock()
		b.unwind(aud, "backchannel", total)

	case runes >= b.cfg.confirmChars && st != bargeYielded:
		b.state = bargeYielded
		b.stopTimerLocked()
		b.mu.Unlock()
		b.enterYield(total)

	case runes >= b.cfg.candidateChars && (st == bargeIdle || st == bargeWatching) && !b.observed:
		if b.audible != audibleNone {
			// The onset action already lowered or paused playback; only the
			// state escalates.
			b.state = bargeDucked
			b.armTimerLocked(b.cfg.maxPause, b.onDuckTimeout)
			b.mu.Unlock()
			b.log.Info("barge ducked", "call_id", b.callID, "text", total)
			return
		}
		b.mu.Unlock()
		if !b.duck() {
			return // nothing playing / duck unsupported: keep watching
		}
		b.mu.Lock()
		if b.state == bargeConfirmed || b.state == bargeYielded {
			b.mu.Unlock()
			b.restore()
			return
		}
		b.state = bargeDucked
		b.audible = audibleDucked
		b.armTimerLocked(b.cfg.maxPause, b.onDuckTimeout)
		b.mu.Unlock()
		b.log.Info("barge ducked", "call_id", b.callID, "text", total)

	default:
		b.mu.Unlock()
	}
}

// enterYield pauses playback losslessly (state already set to yielded). The
// in-flight reply is NOT cancelled: if the interrupt turns out to be false, the
// falseInterrupt timer resumes from the same position.
func (b *adaptiveBarge) enterYield(total string) {
	b.mu.Lock()
	prior := b.audible
	b.mu.Unlock()

	aud := prior
	if prior != audiblePaused {
		if b.pause() {
			aud = audiblePaused
		} else if prior == audibleNone && b.duck() {
			// Pause unsupported: duck is the closest lossless fallback.
			aud = audibleDucked
		}
	}
	if aud == audibleNone {
		// Nothing is playing (or the transport supports neither pause nor
		// duck): fall back to a plain confirm so barge-in still works.
		b.mu.Lock()
		if b.state != bargeYielded {
			b.mu.Unlock()
			return
		}
		b.state = bargeConfirmed
		b.stopTimerLocked()
		b.mu.Unlock()
		b.log.Info("barge confirmed", "call_id", b.callID, "reason", "no_pause_fallback", "text", total)
		b.confirm()
		return
	}
	b.mu.Lock()
	if b.state != bargeYielded {
		b.mu.Unlock()
		return
	}
	b.audible = aud
	b.armTimerLocked(b.cfg.falseInterrupt, b.onYieldTimeout)
	b.mu.Unlock()
	b.log.Info("barge yielded (paused)", "call_id", b.callID, "text", total)
}

// onSpeechEnd resolves the utterance. Returns true if the caller should start an
// LLM turn for final. A real final confirms from any active state; a backchannel
// or filler final unwinds whatever audible action was taken. Backchannels are
// only filtered for utterances that began while the bot was talking: with
// nothing playing, "可以" / "好的" is the caller's answer and starts a turn.
func (b *adaptiveBarge) onSpeechEnd(final string) bool {
	b.mu.Lock()
	st := b.state
	aud := b.audible
	b.stopTimerLocked()
	b.state = bargeIdle
	b.audible = audibleNone
	b.observed = false
	b.acc.Reset()
	b.mu.Unlock()

	real := acceptUserTurn(final) && !isBackchannelOnly(final)
	if st == bargeConfirmed {
		return real // already stopped; start a turn only if there is real content
	}
	if st == bargeWatching || st == bargeDucked || st == bargeYielded {
		if real {
			b.log.Info("barge confirmed", "call_id", b.callID, "reason", "speech_end", "text", final)
			b.confirm()
			return true
		}
		b.unwind(aud, "backchannel", final)
		return false
	}
	return acceptUserTurn(final)
}

// onSpeechDiscard handles an utterance the upstream ASR gate rejected as
// noise / low energy / echo. That is a confirmed false interrupt: unwind any
// audible action immediately instead of waiting for a safety timeout.
func (b *adaptiveBarge) onSpeechDiscard() {
	b.mu.Lock()
	st := b.state
	aud := b.audible
	b.stopTimerLocked()
	b.state = bargeIdle
	b.audible = audibleNone
	b.observed = false
	b.acc.Reset()
	b.mu.Unlock()

	if st == bargeConfirmed {
		return // playback already stopped; nothing to unwind
	}
	b.unwind(aud, "discard", "")
}

// close stops the timer and unwinds any active duck/pause.
func (b *adaptiveBarge) close() {
	b.mu.Lock()
	b.stopTimerLocked()
	aud := b.audible
	b.audible = audibleNone
	b.state = bargeIdle
	b.mu.Unlock()
	switch aud {
	case audibleDucked:
		b.restore()
	case audiblePaused:
		b.resume()
	}
}

// unwind releases an audible action: restore for a duck, resume for a pause.
// No-op when nothing audible was applied.
func (b *adaptiveBarge) unwind(aud int, reason, text string) {
	switch aud {
	case audibleDucked:
		b.log.Info("barge restore ("+reason+")", "call_id", b.callID, "text", text)
		b.restore()
	case audiblePaused:
		b.log.Info("barge resume ("+reason+")", "call_id", b.callID, "text", text)
		b.resume()
	}
}

// setAudible records which audible action the onset applied.
func (b *adaptiveBarge) setAudible(aud int) {
	b.mu.Lock()
	b.audible = aud
	b.mu.Unlock()
}

// onWatchTimeout unwinds any speculative onset action, but keeps tracking the
// acoustic utterance until ASR resolves it with speech_end or speech_discard.
// The first partial can arrive after onsetGrace on real carrier paths; dropping
// the watching state here would make that utterance impossible to interrupt.
func (b *adaptiveBarge) onWatchTimeout(gen uint64) {
	b.mu.Lock()
	if gen != b.timerGen || b.state != bargeWatching {
		b.mu.Unlock()
		return
	}
	aud := b.audible
	b.audible = audibleNone
	b.timer = nil
	b.mu.Unlock()
	b.unwind(aud, "watch timeout", "")
}

// onDuckTimeout unwinds a duck that ASR never resolved. The utterance stays
// observed so repeated VAD edges from the same choppy speech do not duck again,
// while speech_end can still confirm if the final turns out to be real.
func (b *adaptiveBarge) onDuckTimeout(gen uint64) {
	b.mu.Lock()
	if gen != b.timerGen || b.state != bargeDucked {
		b.mu.Unlock()
		return
	}
	aud := b.audible
	b.state = bargeWatching
	b.audible = audibleNone
	b.observed = true
	b.armTimerLocked(b.cfg.maxPause, b.onWatchTimeout)
	b.mu.Unlock()
	b.unwind(aud, "timeout safety", "")
}

// onYieldTimeout resolves a yield that speech_end never confirmed: a false
// interrupt. Default: resume playback from where it paused. With resumeOnFalse
// disabled the pause is treated as a confirmed interruption instead.
func (b *adaptiveBarge) onYieldTimeout(gen uint64) {
	b.mu.Lock()
	if gen != b.timerGen || b.state != bargeYielded {
		b.mu.Unlock()
		return
	}
	aud := b.audible
	b.audible = audibleNone
	b.observed = false
	b.acc.Reset()
	b.timer = nil
	if !b.cfg.resumeOnFalse {
		b.state = bargeConfirmed
		b.mu.Unlock()
		b.log.Info("barge confirmed", "call_id", b.callID, "reason", "false_interrupt_no_resume")
		b.confirm()
		return
	}
	b.state = bargeIdle
	b.mu.Unlock()
	b.unwind(aud, "false interrupt", "")
}

// armTimerLocked replaces the state timer. The generation guard makes a timer
// that already fired but has not run yet a no-op after any state transition.
func (b *adaptiveBarge) armTimerLocked(d time.Duration, fn func(gen uint64)) {
	b.stopTimerLocked()
	b.timerGen++
	gen := b.timerGen
	b.timer = time.AfterFunc(d, func() { fn(gen) })
}

func (b *adaptiveBarge) stopTimerLocked() {
	b.timerGen++
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}
