package spk

import (
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

// Result is the outcome of checking one utterance against the voices on the
// call.
type Result struct {
	// OK is false only for a confident mismatch: everything else — no model, no
	// enrollment yet, audio too short, inference error — accepts the utterance.
	OK bool
	// Enrolled is true on the utterance that became the reference.
	Enrolled bool
	// Compared is true when the utterance resembled a known voice closely enough
	// to join it. Similarity is reported either way — when false it says how
	// close the nearest known voice came before a new one was opened.
	Compared   bool
	Similarity float32
	Threshold  float32
	// SpeakerID identifies the voice this utterance was attributed to, counting
	// from 1 in the order the voices were first heard. Stable for the length of
	// the call, so a log can be read as a conversation between known parties.
	SpeakerID int
	// Speakers is how many voices are currently being tracked, which stops at the
	// configured cap: on a crowded street the least recently heard one is
	// recycled, so this is not a running total of everyone who ever spoke.
	Speakers int
	// Switched is true on the utterance where the call changed hands, i.e. this
	// SpeakerID is the caller from here on. SwitchReason distinguishes a genuine
	// handover from recovering a bad enrollment.
	Switched     bool
	SwitchReason string
}

// Gate is one call's speaker state. Check may be called from any goroutine.
type Gate struct {
	minEnroll int // samples

	mu sync.Mutex
	tr *tracker
}

// NewGate returns a per-call gate, or nil if speaker verification is not
// initialised (callers treat nil as "accept everything"). Thresholds come from
// the process dialogpolicy (SIP_LLM_SPK_*).
func NewGate() *Gate {
	if !Enabled() {
		return nil
	}
	p := dialogpolicy.Get()
	return &Gate{
		minEnroll: p.SpkMinEnrollMs * fbSampleRate / 1000,
		tr: newTracker(
			p.SpkThreshold,
			p.SpkMaxSpeakers,
			p.SpkHandoverTurns,
			p.SpkHandoverLevelRatio,
			p.SpkReenrollAfter,
		),
	}
}

// Check attributes the voiced audio behind an utterance to one of the voices on
// the call and reports whether that voice is the caller's, enrolling on the
// first utterance solid enough to serve as a reference. It never rejects when it
// cannot tell.
func (g *Gate) Check(samples []int16) Result {
	if g == nil || !Enabled() {
		return Result{OK: true}
	}
	// Too short to identify anyone, and no reference to compare against yet.
	if len(samples) < g.minEnroll && !g.enrolled() {
		return Result{OK: true}
	}
	// Embedding runs outside the lock: it is ~50 ms of inference, and holding the
	// call's speaker state across it would serialise nothing useful.
	emb, err := Embed(samples)
	if err != nil {
		return Result{OK: true}
	}
	level := speechRMS(samples)

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tr.observe(emb, level)
}

// enrolled reports whether a caller reference exists yet.
func (g *Gate) enrolled() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tr.primary >= 0
}
