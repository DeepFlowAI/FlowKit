package textproc

import (
	"context"
	"log/slog"
	"math"
	"unicode"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// Processor turns LLM text into what a TTS model should read: symbol cleanup,
// lexicon substitutions for Model, then digit-by-digit reading of identifiers.
type Processor struct {
	Lexicon *Lexicon // may be nil
	Model   string   // model name matched against Entry.Models
}

// Process returns the text to synthesize.
func (p *Processor) Process(text string) string {
	text = Sanitize(text)
	text = p.Lexicon.Apply(text, p.Model)
	return ReadDigits(text)
}

// Guard bounds obviously broken audio. Checks run on the PCM stream only (no
// model, no ASR), so they cost a few additions per sample.
type Guard struct {
	// MaxSilenceMs shortens any pause between speech segments to this length
	// (a model stalling mid-sentence). Silence before the first speech, such
	// as a deliberate lead pad, is untouched. 0 disables.
	MaxSilenceMs int
	// BaseMs + PerCharMs × (spoken characters) is the longest an utterance may
	// run; audio past it is cut (a model babbling or looping). 0 disables.
	BaseMs, PerCharMs int
}

// guardRMS is the 10 ms window RMS (int16 scale) below which audio counts as
// silence; it matches the lead-trim threshold used for CosyVoice output.
const guardRMS = 350.0

// defaultSampleRate is the media bus rate providers emit unless
// Options.SampleRate asks otherwise.
const defaultSampleRate = 16000

// Wrap returns a tts.Provider that runs proc on the text and g on the audio
// around inner. It works with any provider.
func Wrap(inner tts.Provider, proc *Processor, g Guard) tts.Provider {
	return &wrapped{inner: inner, proc: proc, guard: g}
}

type wrapped struct {
	inner tts.Provider
	proc  *Processor
	guard Guard
}

func (w *wrapped) Close() error { return w.inner.Close() }

func (w *wrapped) Synthesize(ctx context.Context, text string, opts tts.Options) (<-chan tts.PCMChunk, error) {
	text = w.proc.Process(text)
	if text == "" {
		return nil, tts.ErrEmptyText
	}
	ictx, cancel := context.WithCancel(ctx)
	in, err := w.inner.Synthesize(ictx, text, opts)
	if err != nil {
		cancel()
		return nil, err
	}
	rate := opts.SampleRate
	if rate <= 0 {
		rate = defaultSampleRate
	}
	out := make(chan tts.PCMChunk, 8)
	go func() {
		defer close(out)
		defer cancel()
		w.pump(ictx, ctx, text, opts, rate, in, out)
	}()
	return out, nil
}

// pump forwards chunks while applying the guard. An attempt that produced no
// samples at all (backend error, empty generation) is retried once.
func (w *wrapped) pump(ictx, ctx context.Context, text string, opts tts.Options, rate int, in <-chan tts.PCMChunk, out chan<- tts.PCMChunk) {
	win := rate / 100
	maxSilence := w.guard.MaxSilenceMs * rate / 1000
	maxTotal := 0
	if w.guard.PerCharMs > 0 {
		maxTotal = (w.guard.BaseMs + w.guard.PerCharMs*spokenChars(text)) * rate / 1000
	}
	var (
		total, emitted, silentRun int
		speech                    bool
		retried                   bool
		dropped                   int
	)
	send := func(c tts.PCMChunk) bool {
		select {
		case out <- c:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		c, ok := <-in
		if !ok || (c.Final && total+len(c.Samples) == 0) {
			if total == 0 && !retried && ctx.Err() == nil {
				retried = true
				slog.Default().Warn("tts guard: empty synthesis, retrying once", "chars", spokenChars(text))
				next, err := w.inner.Synthesize(ictx, text, opts)
				if err == nil {
					drain(in)
					in = next
					continue
				}
			}
			if ok {
				send(c)
			}
			return
		}
		total += len(c.Samples)
		s := c.Samples
		if maxSilence > 0 {
			s, speech, silentRun = capSilence(s, win, maxSilence, speech, silentRun, &dropped)
		}
		if maxTotal > 0 && emitted+len(s) > maxTotal {
			s = s[:maxTotal-emitted]
			slog.Default().Warn("tts guard: utterance over length, cut",
				"limit_ms", maxTotal*1000/rate, "chars", spokenChars(text))
			send(tts.PCMChunk{Samples: s, Final: true})
			drain(in)
			return
		}
		emitted += len(s)
		if !send(tts.PCMChunk{Samples: s, Final: c.Final}) {
			return
		}
		if c.Final {
			if dropped > 0 {
				slog.Default().Info("tts guard: long pause shortened", "dropped_ms", dropped*1000/rate)
			}
			return
		}
	}
}

// capSilence drops 10 ms windows once a pause after speech has lasted limit
// samples. State (speech seen, current silent run) carries across chunks.
func capSilence(s []int16, win, limit int, speech bool, silentRun int, dropped *int) ([]int16, bool, int) {
	out := s[:0:0]
	for i := 0; i < len(s); i += win {
		w := s[i:min(i+win, len(s))]
		if rms(w) > guardRMS {
			speech, silentRun = true, 0
			out = append(out, w...)
			continue
		}
		if !speech {
			out = append(out, w...)
			continue
		}
		silentRun += len(w)
		if silentRun <= limit {
			out = append(out, w...)
		} else {
			*dropped += len(w)
		}
	}
	return out, speech, silentRun
}

func rms(s []int16) float64 {
	var sum float64
	for _, v := range s {
		f := float64(v)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(s)))
}

// spokenChars counts letters and digits (each Han character counts once).
func spokenChars(text string) int {
	n := 0
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			n++
		}
	}
	return n
}

// drain consumes a channel in the background so a provider goroutine blocked
// on send can exit after its context is cancelled.
func drain(c <-chan tts.PCMChunk) {
	go func() {
		for range c {
		}
	}()
}
