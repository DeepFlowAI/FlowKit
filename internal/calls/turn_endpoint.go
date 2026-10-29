package calls

import (
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/DeepFlowAI/flowkit/internal/media/turn"
)

// turnEndpointer refines FunASR's fixed trailing-silence endpoint. When the
// caller pauses, the shape of the transcript so far decides how long that pause
// has to run before the turn is released:
//
//   - finished   (a whole short answer like "好的", anything ending in 。？！ or
//     吗/呢/吧) → release after a short bar, so an ordinary exchange answers
//     immediately.
//   - unfinished (ends on "然后", a comma, or mid-number) → never release early;
//     FunASR's long fallback (TURN_MAX_SILENCE_MS) decides, so a caller
//     collecting their words is not cut off.
//   - neutral    → ask the Smart Turn v3 model, which can release earlier than
//     the neutral bar when it is confident the turn is over.
//
// The text rules carry the common cases on their own; the model is there for the
// shapes they cannot judge. That split matters because the model's Chinese
// accuracy is mediocre — as the only gate it made short turns wait out the
// fallback.
//
// It is created only when turn.Enabled(). Feed runs on the ASR pump goroutine and
// stays cheap (RMS + ring-buffer append); model inference runs on a private
// worker goroutine off the media hot path. Because Flush() also resets FunASR's
// internal silence timer, the early release and the long fallback never double-fire.
type turnEndpointer struct {
	flush       func() error
	log         *slog.Logger
	callID      string
	rmsThr      float64
	triggerMs   int
	reAskMs     int
	maxMs       int
	minVoicedMs int
	threshold   float32

	// Feed-goroutine-only state:
	buf       []int16
	hasVoiced bool
	voicedMs  int
	silentMs  int
	nextAskMs int

	// The transcript so far, written by the ASR event pump and read by Feed.
	partialMu sync.Mutex
	partial   []rune

	resetReq   atomic.Bool
	gen        atomic.Uint64
	flushedGen atomic.Uint64
	req        chan turnRequest
	warned     atomic.Bool
}

type turnRequest struct {
	gen     uint64
	samples []int16
}

const (
	turnBufMaxSamples = 16000 * 8          // Smart Turn looks at the last 8 s
	turnPreRoll       = 16000 * 200 / 1000 // 200 ms kept before speech onset
	// Transcript kept per turn. Only the tail and "is this a short answer?"
	// matter to the text rules, and trimming keeps a rambling turn bounded.
	turnPartialMaxRunes = 32
)

func newTurnEndpointer(callID string, flush func() error, log *slog.Logger) *turnEndpointer {
	e := &turnEndpointer{
		flush:       flush,
		log:         log,
		callID:      callID,
		rmsThr:      float64(envIntDefault("ASR_ENDPOINT_SILENCE_RMS", 350)),
		triggerMs:   envIntDefault("TURN_TRIGGER_SILENCE_MS", 250),
		reAskMs:     envIntDefault("TURN_REASK_SILENCE_MS", 300),
		maxMs:       envIntDefault("TURN_MAX_SILENCE_MS", 1500),
		minVoicedMs: envIntDefault("TURN_MIN_VOICED_MS", 400),
		threshold:   float32(envFloatDefault("TURN_COMPLETE_THRESHOLD", 0.7)),
		req:         make(chan turnRequest, 1),
	}
	e.gen.Store(1)
	e.nextAskMs = e.triggerMs
	go e.worker()
	return e
}

// Feed observes one uplink PCM frame (16 kHz mono). Cheap; never blocks on
// inference. The buffer holds only the current utterance (a short pre-roll +
// speech + trailing pause) so Smart Turn sees a VAD-segment-like window, not the
// whole call — feeding it the greeting/echo history wrecks its predictions.
func (e *turnEndpointer) Feed(samples []int16) {
	if e.resetReq.Swap(false) {
		e.reset()
	}
	frameMs := len(samples) * 1000 / 16000

	if turnFrameRMS(samples) >= e.rmsThr {
		if !e.hasVoiced {
			// Utterance onset: drop the pre-speech history, keep a short pre-roll.
			e.trimTo(turnPreRoll)
			e.hasVoiced = true
			e.voicedMs = 0
			e.nextAskMs = e.triggerMs
		}
		e.silentMs = 0
		e.voicedMs += frameMs
		e.append(samples)
		return
	}

	// Silence.
	e.append(samples)
	if !e.hasVoiced {
		e.trimTo(turnPreRoll) // keep the buffer tiny until speech starts
		return
	}
	e.silentMs += frameMs
	// Guard against cutting off a fragment: never early-release until the turn
	// carries enough real speech. Set below a whole short answer ("好的", ~400 ms)
	// but above the single syllable that makes the model fire a spurious
	// high-confidence "complete".
	if e.voicedMs < e.minVoicedMs {
		return
	}
	shape := classifyTurnShape(e.partialText())
	target := pauseTargetMs(shape)
	if target == 0 {
		return // mid-thought: leave the decision to FunASR's long fallback
	}
	if e.silentMs >= target {
		e.release(e.gen.Load(), "text", "shape", shape.String(),
			"silent_ms", e.silentMs, "voiced_ms", e.voicedMs)
		return
	}
	if shape == shapeFinished {
		return // still waiting out the short bar; the model has nothing to add
	}
	// Only ask between the first-pause trigger and the fallback horizon: past
	// maxMs, FunASR's own timer flushes, so more asks are pointless.
	if e.silentMs < e.nextAskMs || e.silentMs > e.maxMs {
		return
	}
	e.nextAskMs += e.reAskMs // re-ask as the pause grows, until speech resumes
	req := turnRequest{
		gen:     e.gen.Load(),
		samples: append([]int16(nil), e.buf...),
	}
	select {
	case e.req <- req: // hand off to the worker
	default: // worker still busy with the previous window; skip this ask
	}
}

// ObservePartial records an ASR partial for the turn in progress, which is what
// the text rules read. FunASR emits fragments ("如" then "何挂" then "号") rather
// than a growing transcript, so they are appended.
func (e *turnEndpointer) ObservePartial(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	e.partialMu.Lock()
	e.partial = append(e.partial, []rune(text)...)
	if n := len(e.partial); n > turnPartialMaxRunes {
		e.partial = append(e.partial[:0], e.partial[n-turnPartialMaxRunes:]...)
	}
	e.partialMu.Unlock()
}

func (e *turnEndpointer) partialText() string {
	e.partialMu.Lock()
	defer e.partialMu.Unlock()
	return string(e.partial)
}

// Reset marks the current utterance finished (called on every ASR final). The
// generation change immediately invalidates in-flight/queued predictions; the
// next Feed also clears the acoustic buffer.
func (e *turnEndpointer) Reset() {
	e.gen.Add(1)
	e.resetReq.Store(true)
	e.partialMu.Lock()
	e.partial = e.partial[:0]
	e.partialMu.Unlock()
}

// release flushes the ASR segment for turn gen, at most once. Both the text
// rules (Feed goroutine) and the model (worker goroutine) can get there first,
// and either may be holding a generation the caller has since finished.
func (e *turnEndpointer) release(gen uint64, path string, attrs ...any) {
	if gen != e.gen.Load() {
		return
	}
	prev := e.flushedGen.Load()
	if prev == gen || !e.flushedGen.CompareAndSwap(prev, gen) {
		return
	}
	if err := e.flush(); err != nil {
		e.flushedGen.CompareAndSwap(gen, prev) // let a later attempt retry
		e.log.Info("turn flush failed", "call_id", e.callID, "err", err)
		return
	}
	e.log.Info("turn early release",
		append([]any{"call_id", e.callID, "path", path}, attrs...)...)
}

// Close stops the worker. Call once, after Feed will no longer be invoked.
func (e *turnEndpointer) Close() { close(e.req) }

func (e *turnEndpointer) reset() {
	e.buf = e.buf[:0]
	e.hasVoiced = false
	e.voicedMs = 0
	e.silentMs = 0
	e.nextAskMs = e.triggerMs
}

func (e *turnEndpointer) append(s []int16) {
	e.buf = append(e.buf, s...)
	e.trimTo(turnBufMaxSamples)
}

func (e *turnEndpointer) trimTo(maxLen int) {
	if len(e.buf) > maxLen {
		n := copy(e.buf, e.buf[len(e.buf)-maxLen:])
		e.buf = e.buf[:n]
	}
}

func (e *turnEndpointer) worker() {
	for req := range e.req {
		if req.gen != e.gen.Load() || req.gen == e.flushedGen.Load() {
			continue
		}
		prob, err := turn.Predict(req.samples)
		if err != nil {
			if !e.warned.Swap(true) {
				e.log.Warn("turn predict failed", "call_id", e.callID, "err", err)
			}
			continue
		}
		// A final may have arrived while inference was running. Never flush a
		// completed/changed utterance, and never flush one generation twice.
		if req.gen != e.gen.Load() || req.gen == e.flushedGen.Load() {
			continue
		}
		audioMs := len(req.samples) * 1000 / 16000
		complete := prob > e.threshold
		e.log.Info("turn.predict", "call_id", e.callID,
			"prob", prob, "complete", complete, "audio_ms", audioMs)
		if complete {
			e.release(req.gen, "model", "prob", prob, "audio_ms", audioMs)
		}
	}
}

func turnFrameRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		f := float64(s)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(samples)))
}

func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envPosInt(key string) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func envFloatDefault(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
