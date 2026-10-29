package llm

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
)

// IsTurnActive reports whether an LLM turn is in progress for the call.
func (h *ChatHandlers) IsTurnActive(callID string) bool {
	key := "call:" + callID
	h.mu.Lock()
	defer h.mu.Unlock()
	sess := h.sessions[key]
	return sess != nil && sess.cancel != nil
}

// Streaming split thresholds in runes. A turn's first chunk ends at the first
// punctuation past streamFirstSplitMaxLen so playback starts while the model is
// still writing; later chunks wait longer so sentences are not chopped into
// unnatural fragments. The first chunk is not allowed to be a bare "您好，":
// CosyVoice2 often stretches the last character of such a tiny chunk.
const (
	streamSplitMaxLen      = 24
	streamFirstSplitMaxLen = 6
)

// PreparedSpeech is an in-flight TTS synthesis ready to be rendered. Close
// releases it without playing (cancels synthesis, frees the provider).
type PreparedSpeech interface {
	Close()
}

// firstAudibleWait caps how long the sentence after the turn's opening one is
// held back waiting for that opening sentence to produce audio. Only a stalled
// backend should ever reach it.
const firstAudibleWait = 3 * time.Second

// audibleWaiter is implemented by PreparedSpeech handles that can report when
// their backend started returning audio.
type audibleWaiter interface {
	WaitAudible(ctx context.Context, timeout time.Duration)
}

// openingSynthWait caps how long the next sentence waits for the opening
// sentence to finish synthesizing. Past it, prefetch starts anyway.
const openingSynthWait = 5 * time.Second

// synthWaiter is implemented by PreparedSpeech handles that can report when
// their backend finished returning audio.
type synthWaiter interface {
	WaitSynthesized(ctx context.Context, timeout time.Duration)
}

// turnTailPadder is implemented by sinks that can keep the audio stream running
// for a moment after a turn's last sentence.
type turnTailPadder interface {
	PadTurnTail()
}

// padTurnTail runs after the last sentence of a turn has played. Sentences
// within a turn butt up against each other, so the stream only actually falls
// quiet here — which is the one place the far end can clip the closing
// syllable. Skipped on barge-in, where going quiet is the point.
func padTurnTail(ctx context.Context, sink SpeechSink) {
	if ctx.Err() != nil {
		return
	}
	if p, ok := sink.(turnTailPadder); ok {
		p.PadTurnTail()
	}
}

// prepareSentences synthesizes sentences from sentenceCh into preparedCh, one
// ahead of playback, and closes preparedCh when the source is drained.
//
// The turn's opening sentence gets the backend to itself. TTS backends
// commonly synthesize one request at a time, so issuing the next request
// before the opening sentence has produced audio only queues work in front of
// the words the caller is currently waiting for — the opening sentence's
// time-to-first-audio roughly doubles. The same contention later starves the
// opening sentence's own stream: it underruns mid-word while the backend
// starts the following requests. So the next request waits until the opening
// sentence has finished synthesizing; the audio already queued for playback
// by then covers the next sentence's time-to-first-audio.
func prepareSentences(ctx context.Context, sentenceCh <-chan string,
	preparedCh chan<- PreparedSpeech, sink SpeechSink, recordErr func(error)) {
	defer close(preparedCh)
	opening := true
	for sentence := range sentenceCh {
		if ctx.Err() != nil {
			return
		}
		ps, err := sink.Prepare(sentence)
		if err != nil {
			recordErr(err)
			return
		}
		if ps == nil {
			continue
		}
		select {
		case preparedCh <- ps:
		case <-ctx.Done():
			ps.Close()
			return
		}
		if opening {
			opening = false
			if w, ok := ps.(audibleWaiter); ok {
				w.WaitAudible(ctx, firstAudibleWait)
			}
			if w, ok := ps.(synthWaiter); ok {
				w.WaitSynthesized(ctx, openingSynthWait)
			}
		}
	}
}

// SpeechSink synthesizes (Prepare) and renders (Play) assistant sentences.
// RunCallTurn calls Prepare for the next sentence while Play of the current one
// is still in progress, so the next sentence's TTS is synthesized ahead of time
// (prefetch) and there is no inter-sentence synthesis gap.
type SpeechSink interface {
	// Prepare starts synthesizing text; the returned handle's audio streams in
	// the background. May run while a previous Play is still in progress.
	Prepare(text string) (PreparedSpeech, error)
	// Play renders a prepared handle, blocking until playback ends or is
	// interrupted (barge-in). Returns an error on interruption.
	Play(PreparedSpeech) error
}

// speakable sentence. Used by the kernel SIP auto-agent (no RPC client).
func (h *ChatHandlers) RunCallTurn(ctx context.Context, callID, userText string, sink SpeechSink) error {
	if h.client == nil {
		return nil
	}
	userText = strings.TrimSpace(userText)
	if userText == "" {
		return nil
	}

	key := "call:" + callID
	meta := RequestMeta{CallID: callID}
	if h.mgr != nil {
		if c, err := h.mgr.Get(callID); err == nil {
			meta = RequestMetaFromCall(c)
		}
	}
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.mu.Lock()
	sess := h.sessionLocked(key)
	if sess.cancel != nil {
		sess.cancel()
	}
	sess.cancel = cancel
	sess.hist = append(sess.hist, Message{Role: "user", Content: userText})
	history := h.withSystem(sess.hist)
	h.mu.Unlock()

	observability.DialogTurn(ctx, h.log, meta.ConversationID, meta.CallID, "user", userText,
		"root_call_id", meta.RootCallID, "phase", meta.Phase)

	var opErr error
	var opErrMu sync.Mutex
	// playedOK becomes true once the reply's first sentence has begun playing.
	// After that, a later sentence's TTS hiccup must NOT abort the whole turn
	// or surface an error to the caller — the user already heard a reply, so we
	// truncate gracefully instead of playing the "system busy" fallback over a
	// reply that was already partly spoken.
	var playedOK atomic.Bool
	recordErr := func(err error) {
		if err == nil {
			return
		}
		if playedOK.Load() {
			if turnCtx.Err() == nil {
				h.log.Debug("llm call turn: trailing sentence dropped after TTS error",
					"call_id", callID, "err", err)
			}
			return
		}
		opErrMu.Lock()
		if opErr == nil {
			opErr = err
		}
		opErrMu.Unlock()
		if turnCtx.Err() == nil {
			h.log.Debug("llm call turn speech failed", "call_id", callID, "err", err)
		}
		cancel()
	}

	// Two-stage pipeline: sentences are synthesized ahead (Prepare) into
	// preparedCh while a single Play renders them sequentially. The depth-1
	// buffer means the next sentence's TTS is synthesized during the current
	// sentence's playback, eliminating the inter-sentence synthesis gap.
	sentenceCh := make(chan string, 8)
	preparedCh := make(chan PreparedSpeech, 1)

	var prepWg sync.WaitGroup
	prepWg.Add(1)
	go func() {
		defer prepWg.Done()
		prepareSentences(turnCtx, sentenceCh, preparedCh, sink, recordErr)
	}()

	var playWg sync.WaitGroup
	playWg.Add(1)
	go func() {
		defer playWg.Done()
		for ps := range preparedCh {
			if turnCtx.Err() != nil {
				ps.Close()
				continue
			}
			// The reply's first audible sentence has reached playback: from here
			// on, trailing-sentence TTS errors truncate quietly (see recordErr).
			playedOK.Store(true)
			if err := sink.Play(ps); err != nil {
				recordErr(err)
			}
		}
	}()

	splitter := NewSentenceSplitterFastFirst(streamSplitMaxLen, streamFirstSplitMaxLen, func(sentence string) {
		if turnCtx.Err() != nil {
			return
		}
		select {
		case sentenceCh <- sentence:
		case <-turnCtx.Done():
		}
	})

	tokenCh := make(chan string, 32)
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.client.StreamChat(turnCtx, meta, history, tokenCh)
	}()

	var full strings.Builder
	for delta := range tokenCh {
		if turnCtx.Err() != nil {
			break
		}
		full.WriteString(delta)
		splitter.Feed(delta)
	}

	splitter.Flush()
	// The LLM produced no content (e.g. a backend whose knowledge base returns an
	// empty reply for unmatched/chit-chat queries). Speak a fallback so the call
	// never goes silent. Only when not interrupted, so a barge-in stays silent.
	if h.emptyFallback != "" && strings.TrimSpace(full.String()) == "" && turnCtx.Err() == nil {
		full.WriteString(h.emptyFallback)
		h.log.Info("llm call turn: empty reply, speaking fallback", "call_id", callID)
		select {
		case sentenceCh <- h.emptyFallback:
		case <-turnCtx.Done():
		}
	}
	close(sentenceCh)
	prepWg.Wait()
	playWg.Wait()
	padTurnTail(turnCtx, sink)

	// Persist whatever the assistant produced this turn — including a partial
	// reply from a barged-in / interrupted turn — so the model keeps dialog
	// context and does not re-greet on every turn. Runs on all exit paths
	// (the early returns below only carry the error back to the caller).
	if reply := strings.TrimSpace(full.String()); reply != "" {
		h.mu.Lock()
		if sess := h.sessions[key]; sess != nil {
			sess.hist = append(sess.hist, Message{Role: "assistant", Content: reply})
			if len(sess.hist) > maxHistoryMessages {
				sess.hist = append([]Message(nil), sess.hist[len(sess.hist)-maxHistoryMessages:]...)
			}
		}
		h.mu.Unlock()
		observability.DialogTurn(ctx, h.log, meta.ConversationID, meta.CallID, "assistant", reply,
			"root_call_id", meta.RootCallID, "phase", meta.Phase)
	}

	opErrMu.Lock()
	oe := opErr
	opErrMu.Unlock()
	if oe != nil && turnCtx.Err() == nil {
		return oe
	}
	if err := <-errCh; err != nil {
		if turnCtx.Err() != nil {
			return turnCtx.Err()
		}
		return err
	}
	if turnCtx.Err() != nil {
		return turnCtx.Err()
	}
	return nil
}

// StreamSentences renders assistant text arriving on `tokens` through sink with
// sentence-level prefetch: the next sentence is synthesized (Prepare) while the
// current one is rendered (Play), so there is no inter-sentence synthesis gap. It
// returns the full concatenated text and the first TTS error that occurred before
// any audio played (errors on later sentences truncate quietly). Blocks until
// tokens is closed and all audio has played, or ctx is cancelled (barge-in).
//
// Shared by the external-brain converse agent (SSE deltas). RunCallTurn has an
// equivalent inline pipeline over LLM tokens; the two should be unified later.
func StreamSentences(ctx context.Context, tokens <-chan string, sink SpeechSink, log *slog.Logger) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var opErr error
	var opErrMu sync.Mutex
	var playedOK atomic.Bool
	recordErr := func(err error) {
		if err == nil {
			return
		}
		if playedOK.Load() {
			if ctx.Err() == nil {
				log.Debug("stream sentences: trailing sentence dropped after TTS error", "err", err)
			}
			return
		}
		opErrMu.Lock()
		if opErr == nil {
			opErr = err
		}
		opErrMu.Unlock()
		if ctx.Err() == nil {
			log.Debug("stream sentences: speech failed", "err", err)
		}
		cancel()
	}

	sentenceCh := make(chan string, 8)
	preparedCh := make(chan PreparedSpeech, 1)

	var prepWg sync.WaitGroup
	prepWg.Add(1)
	go func() {
		defer prepWg.Done()
		prepareSentences(ctx, sentenceCh, preparedCh, sink, recordErr)
	}()

	var playWg sync.WaitGroup
	playWg.Add(1)
	go func() {
		defer playWg.Done()
		for ps := range preparedCh {
			if ctx.Err() != nil {
				ps.Close()
				continue
			}
			playedOK.Store(true)
			if err := sink.Play(ps); err != nil {
				recordErr(err)
			}
		}
	}()

	splitter := NewSentenceSplitterFastFirst(streamSplitMaxLen, streamFirstSplitMaxLen, func(sentence string) {
		if ctx.Err() != nil {
			return
		}
		select {
		case sentenceCh <- sentence:
		case <-ctx.Done():
		}
	})

	var full strings.Builder
	for delta := range tokens {
		if ctx.Err() != nil {
			break
		}
		full.WriteString(delta)
		splitter.Feed(delta)
	}
	splitter.Flush()
	close(sentenceCh)
	prepWg.Wait()
	playWg.Wait()
	padTurnTail(ctx, sink)

	opErrMu.Lock()
	oe := opErr
	opErrMu.Unlock()
	return full.String(), oe
}

// CancelCall aborts an in-flight llm.chat / RunCallTurn for the given call.
func (h *ChatHandlers) CancelCall(callID string) {
	key := "call:" + callID
	h.mu.Lock()
	defer h.mu.Unlock()
	if sess := h.sessions[key]; sess != nil && sess.cancel != nil {
		sess.cancel()
		sess.cancel = nil
	}
}
