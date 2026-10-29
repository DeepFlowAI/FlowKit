package calls

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/llm"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// inboundDedupWindow is how long an identical ASR final is treated as a FunASR
// duplicate and skipped rather than starting another LLM turn.
const inboundDedupWindow = 3 * time.Second

type InboundLLMConfig struct {
	Greeting string

	// BargeInMinChars: speech_partial rune count to trigger barge-in (0 = any non-empty).
	BargeInMinChars int
	// BargeInOnStart: also barge-in on call.speech_start (ASR VAD); prone to false triggers.
	BargeInOnStart bool

	// Dialog holds the adaptive barge-in / turn-taking knobs. Shared with the
	// external-brain converse agent via the dialogControl unit so the logic is
	// implemented once (see dialog_control.go).
	Dialog dialogConfig

	// GreetingLeadPadMs prepends this much leading silence to the initial
	// greeting so its opening words survive the SIP answer→media cut-through gap
	// (during which the caller cannot yet hear us). The silence also re-primes
	// the receiver's jitter buffer. 0 = TTS default (no extra pad).
	GreetingLeadPadMs int

	// SessionInit, when non-nil, fires a session_start callback on each inbound
	// call (before the first LLM turn). Built from LLM_SESSION_INIT_URL; nil = off.
	SessionInit *llm.SessionInitializer
}

// LoadInboundLLMConfig builds the kernel auto-LLM agent config from the process
// dialogpolicy. greeting overrides SIP_LLM_GREETING when non-empty (main passes
// the env value explicitly for the startup log path).
func LoadInboundLLMConfig(greeting string) InboundLLMConfig {
	p := dialogpolicy.Get()
	cfg := InboundLLMConfig{
		Greeting:          greeting,
		BargeInMinChars:   p.BargeInMinChars,
		BargeInOnStart:    p.BargeInOnStart,
		GreetingLeadPadMs: p.GreetingLeadPadMs,
		Dialog:            loadDialogConfig(),
	}
	if cfg.Greeting == "" {
		cfg.Greeting = p.Greeting
	}
	cfg.SessionInit = llm.NewSessionInitializerFromEnv()
	return cfg
}

// InboundLLMAgent runs answer → listen → LLM → TTS without a browser client.
type InboundLLMAgent struct {
	calls *Handlers
	llm   *llm.ChatHandlers
	mgr   *session.Manager
	cfg   InboundLLMConfig
	log   *slog.Logger

	mu      sync.Mutex
	running map[string]struct{}
}

// NewInboundLLMAgent constructs an agent; llm and calls must be non-nil.
func NewInboundLLMAgent(calls *Handlers, chat *llm.ChatHandlers, mgr *session.Manager, cfg InboundLLMConfig, log *slog.Logger) *InboundLLMAgent {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Greeting == "" {
		cfg.Greeting = "您好，请问有什么可以帮您？"
	}
	if cfg.BargeInMinChars < 0 {
		cfg.BargeInMinChars = 0
	}
	return &InboundLLMAgent{
		calls:   calls,
		llm:     chat,
		mgr:     mgr,
		cfg:     cfg,
		log:     log,
		running: make(map[string]struct{}),
	}
}

// HandleIncoming starts the auto LLM session for one SIP call.
func (a *InboundLLMAgent) HandleIncoming(callID, from string) {
	a.mu.Lock()
	if _, ok := a.running[callID]; ok {
		a.mu.Unlock()
		return
	}
	a.running[callID] = struct{}{}
	a.mu.Unlock()

	a.log.Info("sip inbound llm agent starting", "call_id", callID, "from", from)

	// Optional session_start init (e.g. DeepFlow OpenAPI pre-generation). Uses the
	// SIP call id as instanceId so the later LLM extendData.instanceId matches.
	// Nil-safe; fire-and-forget so the HTTP round-trip never delays answering.
	// WithoutCancel: a fast hangup must not abort the init HTTP call mid-flight.
	if a.cfg.SessionInit != nil {
		initCtx := context.Background()
		to := ""
		if c, err := a.mgr.Get(callID); err == nil {
			to = c.To()
			initCtx = context.WithoutCancel(c.Context())
		}
		go a.cfg.SessionInit.Init(initCtx, callID, from, to)
	}

	go a.run(callID)
}

func (a *InboundLLMAgent) run(callID string) {
	defer func() {
		a.mu.Lock()
		delete(a.running, callID)
		a.mu.Unlock()
	}()

	call, err := a.mgr.Get(callID)
	if err != nil {
		return
	}

	// Child of call.Context: hangup cancels the agent; cancel() on early return
	// only ends this agent subtree, not the call itself.
	ctx, cancel := context.WithCancel(call.Context())
	defer cancel()

	var turnMu sync.Mutex
	var turnCancel context.CancelFunc
	var turnGen uint64
	var lastBarge time.Time
	var bargeMu sync.Mutex
	var partialMu sync.Mutex
	var partialCount int
	// Dedup identical ASR finals: FunASR 2pass re-emits the same offline final
	// several times in quick succession. Each speech_end starts (and cancels the
	// prior) a turn, so duplicates fire the LLM 2–3× — harmless for a stateless
	// model but breaks a stateful per-sid backend (it sees the question repeated
	// + connections dropped). A user genuinely repeating verbatim within the
	// window is implausible, so skipping is safe.
	var lastText string
	var lastTextAt time.Time

	cancelTurn := func() {
		turnMu.Lock()
		if turnCancel != nil {
			turnCancel()
			turnCancel = nil
		}
		turnMu.Unlock()
	}

	// Adaptive barge-in via the shared dialogControl (nil when disabled). The
	// confirm action cancels the in-flight LLM turn; dialogControl stops playback.
	dc := a.calls.newDialogControl(callID, a.cfg.Dialog, func() {
		cancelTurn()
		a.llm.CancelCall(callID)
	}, a.log)
	defer dc.close()

	unsub := a.calls.SubscribeCall(callID, func(method string, params map[string]any) {
		switch method {
		case "call.speech_start":
			// Adaptive barge: pause on VAD onset while classifying the utterance.
			if dc != nil {
				dc.onSpeechStart()
				return
			}
			if a.cfg.BargeInOnStart && a.calls.IsInterruptible(callID) {
				a.maybeBargeIn(callID, cancelTurn, &bargeMu, &lastBarge)
			}
		case "call.speech_partial":
			text, _ := params["text"].(string)
			// Adaptive path: feed the classifier, which pauses (candidate) or
			// confirms. Only while playing — during the LLM-only phase there is
			// nothing to yield and echo/noise partials must not cut TTS. The
			// dispatch layer already gated for speech energy; the classifier does
			// its own stop-word/backchannel/length filtering, so short fragments
			// (single-char stop-word increments) are intentionally forwarded here.
			if dc != nil {
				dc.onPartial(text)
				return
			}
			if !asrgate.PlausibleUserSpeech(text) {
				return
			}
			// Legacy hard barge-in: count accepted partials in this utterance.
			// FunASR emits short incremental partials (1–3 chars) where no single
			// one reaches BargeInMinChars; accumulate so sustained speech still
			// barges, while still requiring enough real speech to ignore blips.
			partialMu.Lock()
			partialCount++
			n := partialCount
			partialMu.Unlock()
			if !a.shouldBargePartial(text) && !a.enoughBargePartials(n) {
				return
			}
			if a.calls.IsInterruptible(callID) {
				a.maybeBargeIn(callID, cancelTurn, &bargeMu, &lastBarge)
			}
		case "call.speech_discard":
			// The ASR gate rejected the utterance (noise/low energy/echo): a
			// confirmed false trigger. Unwind any adaptive duck/pause and reset
			// the legacy partial accumulator, same as a speech_end would.
			partialMu.Lock()
			partialCount = 0
			partialMu.Unlock()
			if dc != nil {
				dc.onSpeechDiscard()
			}
		case "call.speech_end":
			partialMu.Lock()
			partialCount = 0
			partialMu.Unlock()
			text, _ := params["text"].(string)
			text = strings.TrimSpace(text)
			if dc != nil {
				// Resolve the paused candidate: resume (keep the reply) on a
				// backchannel/filler final, or confirm and start a new turn on
				// real content. onSpeechEnd returns whether to proceed.
				if !dc.onSpeechEnd(text) {
					a.log.Debug("sip inbound llm no turn (adaptive barge resume)", "call_id", callID, "text", text)
					return
				}
			} else if !acceptUserTurn(text) {
				a.log.Debug("sip inbound llm ignore short/filler asr", "call_id", callID, "text", text)
				return
			}
			turnMu.Lock()
			dup := text == lastText && time.Since(lastTextAt) < inboundDedupWindow
			lastText = text
			lastTextAt = time.Now()
			turnMu.Unlock()
			if dup {
				a.log.Info("sip inbound llm dedup duplicate asr final", "call_id", callID, "text", text)
				return
			}
			a.log.Debug("sip inbound llm user text", "call_id", callID, "text", text)
			cancelTurn()
			// Stop the previous turn's in-flight playback immediately. cancelTurn
			// only cancels the turn context (stops the LLM); the audio runs on a
			// separate play context, so without this the prior reply keeps playing
			// until the new reply's first audio claims the play slot. Short finals
			// like "再见"/"拜拜" never trigger a partial barge-in (their partials are
			// single chars, filtered as noise), so this is their only stop path.
			_ = a.calls.StopPlay(callID)
			turnCtx, cancel := context.WithCancel(ctx)
			turnMu.Lock()
			turnGen++
			gen := turnGen
			turnCancel = cancel
			turnMu.Unlock()
			go func(userText string, gen uint64) {
				// Clean up this turn's context, but only if no newer turn has
				// replaced it. A long previous turn can still be tearing down
				// (canceled TTS prefetch) after the next speech_end has already
				// started a new turn; a blind cancelTurn() here would cancel that
				// newer turn and silently drop its reply.
				defer func() {
					turnMu.Lock()
					if turnGen == gen && turnCancel != nil {
						turnCancel()
						turnCancel = nil
					}
					turnMu.Unlock()
				}()
				err := a.llm.RunCallTurn(turnCtx, callID, userText, a.calls.SpeechSink(turnCtx, callID, ""))
				if err != nil && turnCtx.Err() == nil && ctx.Err() == nil {
					a.log.Warn("sip inbound llm turn failed", "call_id", callID, "err", err)
					_ = a.calls.SaySync(ctx, callID, "抱歉，系统暂时繁忙，请稍后再试。", "")
				}
			}(text, gen)
		}
	})
	defer unsub()

	if _, err := a.calls.AnswerCall(ctx, callID); err != nil {
		a.log.Warn("sip inbound llm answer failed", "call_id", callID, "err", err)
		return
	}
	if _, err := a.calls.ListenCall(ctx, callID); err != nil {
		a.log.Warn("sip inbound llm listen failed", "call_id", callID, "err", err)
		return
	}
	if a.cfg.Greeting != "" {
		// Prepend leading silence so the greeting's opening words survive the
		// SIP answer→media cut-through gap (the caller cannot hear us for the
		// first several hundred ms after 200 OK). The silence is dropped by the
		// carrier during the gap and re-primes the jitter buffer once media is up.
		if err := a.calls.saySyncPadded(ctx, callID, a.cfg.Greeting, "", a.cfg.GreetingLeadPadMs); err != nil && ctx.Err() == nil && err != ErrPlaybackInterrupted {
			a.log.Warn("sip inbound llm greeting failed", "call_id", callID, "err", err)
		}
	}

	<-ctx.Done()
	cancelTurn()
	a.log.Info("sip inbound llm agent ended", "call_id", callID)
}

func (a *InboundLLMAgent) shouldBargePartial(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	if a.cfg.BargeInMinChars <= 0 {
		return true
	}
	return len([]rune(text)) >= a.cfg.BargeInMinChars
}

// enoughBargePartials reports whether the number of accepted partials in the
// current utterance is enough to barge. Handles ASR providers (FunASR) whose
// partials are short increments where no single one reaches BargeInMinChars;
// it mirrors the char threshold (0 = any).
func (a *InboundLLMAgent) enoughBargePartials(n int) bool {
	if a.cfg.BargeInMinChars <= 0 {
		return true
	}
	return n >= a.cfg.BargeInMinChars
}

// acceptUserTurn filters noise/echo finals before starting an LLM turn.
func acceptUserTurn(text string) bool {
	return asrgate.PlausibleUserSpeech(text)
}

const bargeInCooldown = 400 * time.Millisecond

func (a *InboundLLMAgent) maybeBargeIn(callID string, cancelTurn func(), mu *sync.Mutex, last *time.Time) {
	mu.Lock()
	if time.Since(*last) < bargeInCooldown {
		mu.Unlock()
		return
	}
	*last = time.Now()
	mu.Unlock()
	a.log.Info("sip inbound llm barge-in", "call_id", callID)
	cancelTurn()
	a.llm.CancelCall(callID)
	_ = a.calls.StopPlay(callID)
}

// AnswerCall is the programmatic equivalent of call.answer.
func (h *Handlers) AnswerCall(ctx context.Context, callID string) (any, error) {
	raw, _ := json.Marshal(callIDParams{CallID: callID})
	res, rpcErr := h.handleAnswer(ctx, nil, raw)
	return res, rpcErrAsError(rpcErr)
}

// ListenCall is the programmatic equivalent of call.listen.
func (h *Handlers) ListenCall(ctx context.Context, callID string) (any, error) {
	raw, _ := json.Marshal(listenParams{CallID: callID})
	res, rpcErr := h.handleListen(ctx, nil, raw)
	return res, rpcErrAsError(rpcErr)
}

// SaySync runs call.say and blocks until paced playback finishes.
func (h *Handlers) SaySync(ctx context.Context, callID, text, voice string) error {
	return h.saySyncPadded(ctx, callID, text, voice, 0)
}

// saySyncPadded is SaySync with a per-utterance leading-silence pad (ms). A
// large pad on the initial greeting keeps its opening words from being clipped
// in the SIP answer→media cut-through gap. leadPadMs<=0 uses the TTS default.
func (h *Handlers) saySyncPadded(ctx context.Context, callID, text, voice string, leadPadMs int) error {
	waitCh := make(chan string, 1)
	h.registerPlayWait(callID, waitCh)
	defer h.unregisterPlayWait(callID, waitCh)

	raw, _ := json.Marshal(sayParams{CallID: callID, Text: text, Voice: voice, LeadPadMs: leadPadMs})
	if _, rpcErr := h.handleSay(ctx, nil, raw); rpcErr != nil {
		return rpcErrAsError(rpcErr)
	}
	select {
	case reason := <-waitCh:
		if reason == "interrupted" {
			return ErrPlaybackInterrupted
		}
		return nil
	case <-ctx.Done():
		_ = h.StopPlay(callID)
		return ctx.Err()
	}
}

// SpeechSink returns an llm.SpeechSink for the kernel LLM turn that prefetches
// the next sentence's TTS (Prepare) while the current one is playing (Play),
// removing the inter-sentence synthesis gap.
func (h *Handlers) SpeechSink(ctx context.Context, callID, voice string) llm.SpeechSink {
	return &ttsSink{h: h, ctx: ctx, callID: callID, voice: voice}
}

// openingSpeechSink is SpeechSink with leadPadMs of leading silence on the first
// sentence only, so a model-generated opening survives the answer→media gap the
// same way a fixed greeting does.
func (h *Handlers) openingSpeechSink(ctx context.Context, callID, voice string, leadPadMs int) llm.SpeechSink {
	return &ttsSink{h: h, ctx: ctx, callID: callID, voice: voice, leadPadMs: leadPadMs}
}

type ttsSink struct {
	h      *Handlers
	ctx    context.Context
	callID string
	voice  string
	// leadPadMs applies to the next Prepare only. Prepare runs on a single
	// goroutine (llm.StreamSentences), so no locking.
	leadPadMs int
}

func (s *ttsSink) Prepare(text string) (llm.PreparedSpeech, error) {
	leadPad := s.leadPadMs
	s.leadPadMs = 0
	ps, err := s.h.prepareSay(s.callID, text, s.voice, leadPad)
	if err != nil {
		return nil, err
	}
	return ps, nil
}

func (s *ttsSink) Play(p llm.PreparedSpeech) error {
	ps, ok := p.(*PreparedSay)
	if !ok || ps == nil {
		return nil
	}
	return s.h.PlayPrepared(s.ctx, s.callID, ps)
}

// PadTurnTail sends a short run of silence once the turn's last sentence has
// played. Prompts spoken through call.say carry the same trailing silence in
// the synthesized audio itself (_sayTailPadMs); a turn cannot do that, because
// its sentences are synthesized one at a time with no way to know which is
// last, and padding every one of them would open a gap mid-reply.
func (s *ttsSink) PadTurnTail() {
	s.h.padPlaybackTail(s.ctx, s.callID)
}

// StopPlay cancels in-flight TTS and clears queued outbound audio.
// Uses WithoutCancel when the call still exists: callers often invoke this
// from a path that already observed ctx.Done() (saySyncPadded after hangup).
func (h *Handlers) StopPlay(callID string) error {
	raw, _ := json.Marshal(stopParams{CallID: callID, Target: "play"})
	ctx := context.Background()
	if call, err := h.mgr.Get(callID); err == nil {
		ctx = context.WithoutCancel(call.Context())
	}
	_, rpcErr := h.handleStop(ctx, nil, raw)
	return rpcErrAsError(rpcErr)
}

// SubscribeCall registers a server-side listener for call events on one call.
func (h *Handlers) SubscribeCall(callID string, fn CallEventFn) func() {
	id := int(nextCallEventSubID.Add(1))
	cs := h.stateFor(callID, nil)
	h.mu.Lock()
	cs.events = append(cs.events, callEventSub{id: id, fn: fn})
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		cur := h.callState[callID]
		if cur == nil {
			h.mu.Unlock()
			return
		}
		subs := cur.events
		out := subs[:0]
		for _, sub := range subs {
			if sub.id != id {
				out = append(out, sub)
			}
		}
		cur.events = out
		h.mu.Unlock()
	}
}

func rpcErrAsError(rpcErr *rpc.Error) error {
	if rpcErr == nil {
		return nil
	}
	return rpcErr
}
