package calls

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/llm"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// call.converse_external_agent (v2 flowkit-中枢架构): unlike call.connect_external_
// agent (v1), flowkit owns the whole call leg — ASR (VAD/endpoint/barge-in) and
// TTS — and treats the external agent (VoiceSpeed) as a stateless text brain.
// For each user utterance it transcribes, flowkit POSTs the text to the agent's
// turn URL and gets back {reply, commands, voice_spk_id}: it speaks the reply
// with the returned voice, and on a transfer_human / hang_up command emits the
// same call.external_agent.handoff / .ended events as v1. No audio leaves flowkit.

// converseBargeMinPartials is how many accepted speech_partial events in one
// utterance trigger the legacy hard barge-in on the external-agent path.
//
// Deliberately NOT SIP_LLM_BARGE_IN_MIN_CHARS / dialogpolicy.BargeInMinChars:
// that knob is a character-count threshold (and the kernel auto-LLM path also
// reuses the same number as a partial-count bar — an existing semantic mix-up
// we are not expanding here). Converse only counts partial events, so wiring it
// to MIN_CHARS would change barge latency whenever that env differs from 2
// (e.g. .env.v100 sets 4). Keep the historical fixed bar until a dedicated
// partial-count config is designed.
const converseBargeMinPartials = 2

// converseHTTPClient is shared; per-turn deadlines come from the turn context.
var converseHTTPClient = &http.Client{}

type converseExternalAgentParams struct {
	CallID      string                `json:"call_id"`
	Provider    string                `json:"provider"`
	TurnURL     string                `json:"turn_url"`     // full POST URL of the agent turn endpoint
	Token       string                `json:"token"`        // Bearer token for the turn endpoint
	Greeting    string                `json:"greeting"`     // fixed opening line spoken on entry
	Opening     string                `json:"opening"`      // "model": agent generates the first line on entry
	VoiceSpkID  string                `json:"voice_spk_id"` // initial TTS voice (turns may override)
	Traceparent string                `json:"traceparent"`
	TraceCallID string                `json:"trace_call_id"`
	Performance *PerformanceOverrides `json:"performance,omitempty"`
}

const (
	// converseOpeningModel asks the agent for a first line before the caller
	// speaks. A non-empty greeting takes precedence.
	converseOpeningModel = "model"
	// converseTriggerOpening marks the turn request that carries no user text
	// because the call has just connected.
	converseTriggerOpening = "opening"
)

type converseTurnRequest struct {
	SessionID   string `json:"session_id"`
	Text        string `json:"text"`
	Trigger     string `json:"trigger,omitempty"`
	Traceparent string `json:"traceparent,omitempty"`
	TurnID      string `json:"turn_id,omitempty"` // echoed back in barge-in reports
}

type converseTurnCommand struct {
	Verb   string            `json:"verb"`
	Params map[string]string `json:"params"`
}

type converseTurnResponse struct {
	Commands   []converseTurnCommand `json:"commands"`
	VoiceSpkID string                `json:"voice_spk_id"`
}

// converseAgent runs the per-call ASR↔turn↔TTS loop with barge-in.
type converseAgent struct {
	h           *Handlers
	callID      string
	provider    string
	turnURL     string
	token       string
	greeting    string
	opening     string
	traceparent string
	log         *slog.Logger
	performance *PerformanceOverrides

	mu    sync.Mutex
	voice string // current TTS voice, updated from each turn response

	loopCtx    context.Context
	loopCancel context.CancelFunc
	stopOnce   sync.Once
}

func (h *Handlers) handleConverseExternalAgent(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p converseExternalAgentParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.CallID == "" || p.TurnURL == "" {
		return nil, rpc.ErrInvalidParams("call_id and turn_url required")
	}
	if err := h.checkOutboundURL(p.TurnURL); err != nil {
		return nil, rpc.ErrInvalidParams(err.Error())
	}

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.converse_external_agent"); rpcErr != nil {
		return nil, rpcErr
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	if err := h.startConverse(call, conn, p); err != nil {
		return nil, rpc.ErrCallStateInvalid(err.Error())
	}
	return map[string]bool{"ok": true}, nil
}

// startConverse builds and runs a converse agent for an already-bound call. It is
// shared by call.converse_external_agent (RPC-driven, over a SIP/WebRTC leg) and
// the WS-PCM media leg (voicespeed backend bridge). conn is the RPC connection
// external-agent events flow back to; pass nil for the WS-PCM leg (no separate
// control conn). Returns an error if a converse agent already runs for the call.
func (h *Handlers) startConverse(call *session.Call, conn *rpc.Conn, p converseExternalAgentParams) error {
	callID := call.ID()

	// Bind state so events flow back to the conn that issued the RPC (if any).
	cs := h.stateFor(callID, conn)

	h.converseMu.Lock()
	if cs.converseCancel != nil {
		h.converseMu.Unlock()
		return fmt.Errorf("converse agent already running")
	}
	h.converseMu.Unlock()

	// Child of the call's root span; fall back to the propagated traceparent when
	// the transport did not start one. The same traceparent rides each turn HTTP
	// call and each emitted event so the whole conversation is one trace.
	parentCtx := call.RootSpanContext()
	if p.Traceparent != "" {
		parentCtx = observability.ContextFromTraceparent(parentCtx, p.Traceparent)
	}
	traceCallID := p.TraceCallID
	if traceCallID == "" {
		traceCallID = call.RootCallID()
	}
	spanCtx, span := observability.StartChildSpan(parentCtx, "voicespeed.converse", traceCallID)
	downstreamTP := observability.TraceparentFromContext(spanCtx)

	if !p.Performance.empty() {
		cs.performance = p.Performance
	}

	// Child of call.Context: hangup ends the loop; loopCancel still stops the
	// agent alone on handoff without ending the call.
	loopCtx, loopCancel := context.WithCancel(call.Context())
	a := &converseAgent{
		h:           h,
		callID:      callID,
		provider:    p.Provider,
		turnURL:     p.TurnURL,
		token:       p.Token,
		greeting:    p.Greeting,
		opening:     p.Opening,
		traceparent: downstreamTP,
		voice:       p.VoiceSpkID,
		log:         h.log.With("call_id", callID, "provider", p.Provider),
		loopCtx:     loopCtx,
		loopCancel:  loopCancel,
		performance: p.Performance,
	}

	h.converseMu.Lock()
	cs.converseCancel = loopCancel
	h.converseMu.Unlock()

	go a.run(loopCtx, func() { span.End() })

	h.dispatchExternalEvent(callID, evtExternalAgentConnected, nil, downstreamTP)
	observability.CallTraceCall(spanCtx, h.log, "external_agent.connect", call,
		"provider", p.Provider, "mode", "converse")
	return nil
}

func (a *converseAgent) run(loopCtx context.Context, endSpan func()) {
	defer endSpan()
	defer func() {
		if cs := a.h.stateGet(a.callID); cs != nil {
			a.h.converseMu.Lock()
			cs.converseCancel = nil
			a.h.converseMu.Unlock()
		}
		a.log.Info("converse agent ended")
	}()

	if _, err := a.h.mgr.Get(a.callID); err != nil {
		return
	}

	var turnMu sync.Mutex
	var turnCancel context.CancelFunc
	var turnGen uint64
	var lastText string
	var lastTextAt time.Time
	var lastBarge time.Time
	var bargeMu sync.Mutex
	var partialMu sync.Mutex
	var partialCount int
	// Audio the caller heard of the latest playback; read when the fixed greeting is cut off.
	var lastPlayedMs atomic.Int64

	cancelTurn := func() {
		turnMu.Lock()
		if turnCancel != nil {
			turnCancel()
			turnCancel = nil
		}
		turnMu.Unlock()
	}
	// startTurn runs fn as the one in-flight turn, so cancelTurn (a new user
	// utterance or a barge-in) can stop it.
	startTurn := func(fn func(ctx context.Context)) {
		turnCtx, cancel := context.WithCancel(loopCtx)
		turnMu.Lock()
		turnGen++
		gen := turnGen
		turnCancel = cancel
		turnMu.Unlock()
		go func() {
			defer func() {
				turnMu.Lock()
				if turnGen == gen && turnCancel != nil {
					turnCancel()
					turnCancel = nil
				}
				turnMu.Unlock()
			}()
			fn(turnCtx)
		}()
	}
	// Adaptive barge-in via the shared dialogControl (same implementation as the
	// kernel auto-LLM agent). nil when SIP_LLM_ADAPTIVE_BARGE_IN is off → fall back
	// to the legacy hard barge-in below. confirm cancels the in-flight /turn.
	dc := a.h.newDialogControl(a.callID, mergeDialogConfig(loadDialogConfig(), a.performance), cancelTurn, a.log)
	defer dc.close()
	bargeIn := func() {
		bargeMu.Lock()
		if time.Since(lastBarge) < bargeInCooldown {
			bargeMu.Unlock()
			return
		}
		lastBarge = time.Now()
		bargeMu.Unlock()
		a.log.Info("converse barge-in")
		cancelTurn()
		_ = a.h.StopPlay(a.callID)
	}

	unsub := a.h.SubscribeCall(a.callID, func(method string, params map[string]any) {
		switch method {
		case "call.play_end":
			ms, _ := params["heard_ms"].(int64)
			lastPlayedMs.Store(ms)
		case "call.speech_start":
			// Adaptive path only: pause playback on VAD onset while classifying.
			if dc != nil {
				dc.onSpeechStart()
			}
		case "call.speech_partial":
			text, _ := params["text"].(string)
			// Adaptive path: the classifier pauses (candidate) or confirms; the
			// dispatch layer already gated for speech energy. Legacy path: count
			// accepted partials and hard-barge once enough accumulate.
			if dc != nil {
				dc.onPartial(text)
				return
			}
			if !asrgate.PlausibleUserSpeech(text) {
				return
			}
			// FunASR emits short incremental partials; accumulate so sustained
			// speech barges while brief blips don't. Only barge while RTP plays.
			partialMu.Lock()
			partialCount++
			n := partialCount
			partialMu.Unlock()
			if n < converseBargeMinPartials {
				return
			}
			if a.h.IsInterruptible(a.callID) {
				bargeIn()
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
			voicedMs, _ := params["voiced_ms"].(int)
			// Absent when speaker verification is off, which is not a mismatch.
			voiceMatch, ok := params["voice_match"].(bool)
			if !ok {
				voiceMatch = true
			}
			// Drop ASR leading punctuation ("。/，/？" FunASR prepends) before it
			// reaches the model — the call-path cleanup v1's direct ASR skipped.
			text = asrgate.TrimLeadingPunct(strings.TrimSpace(text))
			// Adaptive path resolves the paused candidate (resume on backchannel,
			// confirm+stop on real content); legacy path just filters fillers.
			if dc != nil {
				if !dc.onSpeechEnd(text) {
					return
				}
			} else if !acceptUserTurn(text) {
				return
			}
			turnMu.Lock()
			dup := text == lastText && time.Since(lastTextAt) < inboundDedupWindow
			lastText = text
			lastTextAt = time.Now()
			turnMu.Unlock()
			if dup {
				a.log.Info("converse dedup duplicate asr final", "text", text)
				return
			}
			cancelTurn()
			// Stop the prior reply's in-flight playback immediately (see inbound_llm).
			_ = a.h.StopPlay(a.callID)
			startTurn(func(ctx context.Context) {
				a.doTurn(ctx, text, voicedMs, voiceMatch)
			})
		}
	})
	defer unsub()

	// flowkit owns ASR in v2: start listening (idempotent if already on).
	if _, err := a.h.ListenCall(loopCtx, a.callID); err != nil {
		a.log.Warn("converse listen failed (continuing; asr may already be active)", "err", err)
	}
	if a.greeting != "" {
		leadPad := 0
		if a.performance != nil && a.performance.GreetingLeadPadMs != nil {
			leadPad = *a.performance.GreetingLeadPadMs
		}
		var err error
		if leadPad > 0 {
			err = a.h.saySyncPadded(loopCtx, a.callID, a.greeting, a.currentVoice(), leadPad)
		} else {
			err = a.h.SaySync(loopCtx, a.callID, a.greeting, a.currentVoice())
		}
		if err == ErrPlaybackInterrupted {
			played := []playedSpeech{{text: a.greeting, playedMs: lastPlayedMs.Load()}}
			a.reportBargeIn("", heardText(played, int64(leadPad)))
		} else if err != nil && loopCtx.Err() == nil {
			a.log.Warn("converse greeting failed", "err", err)
		}
	} else if a.opening == converseOpeningModel {
		startTurn(a.doOpening)
	}

	<-loopCtx.Done()
	cancelTurn()
}

// doTurn runs one round: stream the user text to the agent's SSE /turn endpoint,
// speak the reply incrementally (sentence prefetch, no inter-sentence gap), then
// act on any command. Commands run only after the reply is spoken, and never when
// the turn was interrupted (barge-in), so a barged-in user is not transferred.
// voicedMs is how much real speech backed the utterance and voiceMatch whether
// it came from the enrolled caller; irreversible commands need both.
func (a *converseAgent) doTurn(ctx context.Context, userText string, voicedMs int, voiceMatch bool) {
	a.log.Debug("converse user text", "text", userText)

	turnID := newShortID()
	sink := &playTracker{SpeechSink: a.h.SpeechSink(ctx, a.callID, a.currentVoice())}
	tokenCh := make(chan string, 32)
	var resp converseTurnResponse
	var streamErr error
	go func() {
		defer close(tokenCh)
		streamErr = a.streamTurn(ctx, userText, "", turnID, tokenCh, &resp)
	}()

	// Renders delta text through TTS as it streams; returns the full spoken text.
	full, _ := llm.StreamSentences(ctx, tokenCh, sink, a.log)

	if ctx.Err() != nil {
		a.reportBargeIn(turnID, heardText(sink.played, 0))
		return // barge-in / call end cancelled this turn
	}
	if streamErr != nil && strings.TrimSpace(full) == "" {
		a.log.Warn("converse turn stream failed", "err", streamErr)
		_ = a.h.SaySync(ctx, a.callID, "抱歉，系统暂时繁忙，请稍后再试。", a.currentVoice())
		return
	}
	a.setVoice(resp.VoiceSpkID) // applies to subsequent turns

	if ctx.Err() != nil {
		return
	}

	for _, cmd := range resp.Commands {
		if cmd.Verb == "transfer_human" || cmd.Verb == "hang_up" {
			if !voiceMatch {
				a.log.Info("converse command skipped (unrecognised voice)", "call_id", a.callID,
					"verb", cmd.Verb)
				continue
			}
			if min := commandMinVoicedMs(); voicedMs > 0 && voicedMs < min {
				a.log.Info("converse command skipped (weak evidence)", "call_id", a.callID,
					"verb", cmd.Verb, "voiced_ms", voicedMs, "min_voiced_ms", min)
				continue
			}
		}
		switch cmd.Verb {
		case "transfer_human":
			payload := map[string]any{}
			if g := strings.TrimSpace(cmd.Params["group"]); g != "" {
				payload["suggested_target"] = g
			}
			a.h.dispatchExternalEvent(a.callID, evtExternalAgentHandoff,
				map[string]any{"payload": payload}, a.traceparent)
			traceCtx := a.callTraceCtx()
			observability.CallTrace(traceCtx, a.log, "external_agent.handoff", a.callID,
				"provider", a.provider, "suggested_target", payload["suggested_target"])
			a.stop() // stop the loop; the call stays up for OpenDesk to bridge to a human
			return
		case "hang_up":
			// Before the ended event: the call controller hangs up on receiving it.
			a.h.drainBeforeHangup(a.callID)
			a.h.dispatchExternalEvent(a.callID, evtExternalAgentEnded,
				map[string]any{"reason": "hangup"}, a.traceparent)
			// Call is still live here — HangupCall is what ends it. Use the
			// call context for span correlation (not WithoutCancel).
			traceCtx := a.callTraceCtx()
			observability.CallTrace(traceCtx, a.log, "external_agent.ended", a.callID,
				"provider", a.provider, "reason", "hangup")
			a.stop()
			_ = a.h.HangupCall(traceCtx, a.callID, hangupReasonBot)
			return
		default:
			// transfer_agent / update_field: parsed and forwarded but not executed
			// in v2 (OpenDesk consumes them later).
			a.log.Info("converse command not executed in v2", "verb", cmd.Verb, "params", cmd.Params)
		}
	}
}

// doOpening lets the agent speak first right after the call connects. The
// request carries the opening trigger instead of user text, and the first
// sentence gets the greeting lead pad. Unlike doTurn, a failure stays silent
// (no fallback line) and commands are ignored: nothing the caller said backs
// them.
func (a *converseAgent) doOpening(ctx context.Context) {
	leadPad := 0
	if a.performance != nil && a.performance.GreetingLeadPadMs != nil {
		leadPad = *a.performance.GreetingLeadPadMs
	}
	turnID := newShortID()
	sink := &playTracker{
		SpeechSink: a.h.openingSpeechSink(ctx, a.callID, a.currentVoice(), leadPad),
		leadPadMs:  int64(leadPad),
	}
	tokenCh := make(chan string, 32)
	var resp converseTurnResponse
	var streamErr error
	go func() {
		defer close(tokenCh)
		streamErr = a.streamTurn(ctx, "", converseTriggerOpening, turnID, tokenCh, &resp)
	}()

	full, _ := llm.StreamSentences(ctx, tokenCh, sink, a.log)

	if ctx.Err() != nil {
		a.reportBargeIn(turnID, heardText(sink.played, sink.leadPadMs))
		return // the caller spoke first / barged in / call ended
	}
	if streamErr != nil && strings.TrimSpace(full) == "" {
		a.log.Warn("converse opening failed", "err", streamErr)
		return
	}
	a.setVoice(resp.VoiceSpkID)
}

// streamTurn POSTs the user text (or, for the opening, just the trigger) to the
// agent's SSE /turn endpoint and forwards spoken `delta` text to tokenCh; the
// final `done` event's commands + voice id are written to out. Returns on stream
// end, an `error` event, or ctx cancellation.
func (a *converseAgent) streamTurn(ctx context.Context, userText, trigger, turnID string, tokenCh chan<- string, out *converseTurnResponse) error {
	reqBody, _ := json.Marshal(converseTurnRequest{
		SessionID:   a.callID,
		Text:        userText,
		Trigger:     trigger,
		Traceparent: a.traceparent,
		TurnID:      turnID,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.turnURL, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	start := time.Now()
	resp, err := converseHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("turn stream http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	sawDelta := false
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(line[len("data:"):])
		if data == "" {
			continue
		}
		var ev struct {
			Type       string                `json:"type"`
			Text       string                `json:"text"`
			Commands   []converseTurnCommand `json:"commands"`
			VoiceSpkID string                `json:"voice_spk_id"`
			Message    string                `json:"message"`
		}
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			continue // ignore malformed / comment lines
		}
		switch ev.Type {
		case "delta":
			if ev.Text != "" {
				if !sawDelta {
					sawDelta = true
					// How long the external brain took to produce any speakable
					// text. Splitting the wait before playback into "brain" and
					// "our own buffering + TTS" needs this on the flowkit side.
					observability.CallTrace(ctx, a.log, "external_agent.first_delta", a.callID,
						"provider", a.provider, "ttft_ms", time.Since(start).Milliseconds())
				}
				select {
				case tokenCh <- ev.Text:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case "done":
			out.Commands = ev.Commands
			out.VoiceSpkID = ev.VoiceSpkID
		case "error":
			return fmt.Errorf("turn stream error: %s", ev.Message)
		}
	}
	return sc.Err()
}

func (a *converseAgent) stop() {
	a.stopOnce.Do(func() {
		if a.loopCancel != nil {
			a.loopCancel()
		}
	})
}

// callTraceCtx returns the live call context for logging / hangup. Falls back
// to Background only when the call is already gone from the manager.
func (a *converseAgent) callTraceCtx() context.Context {
	if c, err := a.h.mgr.Get(a.callID); err == nil {
		return c.Context()
	}
	return context.Background()
}

func (a *converseAgent) currentVoice() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.voice
}

func (a *converseAgent) setVoice(v string) {
	if v == "" {
		return
	}
	a.mu.Lock()
	a.voice = v
	a.mu.Unlock()
}

// hangupReasonBot is the call.ended reason when the agent itself hangs up, so
// call records can tell it apart from the caller hanging up.
const hangupReasonBot = "bot_hangup"

// HangupCall is the programmatic equivalent of call.hangup.
func (h *Handlers) HangupCall(ctx context.Context, callID, reason string) error {
	raw, _ := json.Marshal(callIDParams{CallID: callID, Reason: reason})
	_, rpcErr := h.handleHangup(ctx, nil, raw)
	return rpcErrAsError(rpcErr)
}
