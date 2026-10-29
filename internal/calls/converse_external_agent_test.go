package calls

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

// streamTurn must POST {session_id,text,traceparent} with a Bearer token + an SSE
// Accept header, forward `delta` text to the token channel, and capture the final
// `done` event's commands + voice — the streaming contract with VoiceSpeed.
func TestConverseStreamTurn_RequestAndParse(t *testing.T) {
	var gotBody converseTurnRequest
	var gotAuth, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"data: {\"type\":\"delta\",\"text\":\"好的\"}\n\n" +
				"data: {\"type\":\"delta\",\"text\":\"再见\"}\n\n" +
				"data: {\"type\":\"done\",\"commands\":[{\"verb\":\"hang_up\",\"params\":{}}],\"voice_spk_id\":\"siyue\"}\n\n"))
	}))
	defer srv.Close()

	a := &converseAgent{callID: "call-9", turnURL: srv.URL, token: "sk-1", traceparent: "tp-1"}
	tokenCh := make(chan string, 8)
	var out converseTurnResponse
	var streamErr error
	done := make(chan struct{})
	go func() {
		streamErr = a.streamTurn(context.Background(), "再见", "", "turn-1", tokenCh, &out)
		close(tokenCh)
		close(done)
	}()
	var deltas []string
	for tok := range tokenCh {
		deltas = append(deltas, tok)
	}
	<-done
	require.NoError(t, streamErr)

	require.Equal(t, "call-9", gotBody.SessionID)
	require.Equal(t, "再见", gotBody.Text)
	require.Empty(t, gotBody.Trigger)
	require.Equal(t, "tp-1", gotBody.Traceparent)
	require.Equal(t, "turn-1", gotBody.TurnID)
	require.Equal(t, "Bearer sk-1", gotAuth)
	require.Equal(t, "text/event-stream", gotAccept)

	require.Equal(t, []string{"好的", "再见"}, deltas)
	require.Equal(t, "siyue", out.VoiceSpkID)
	require.Len(t, out.Commands, 1)
	require.Equal(t, "hang_up", out.Commands[0].Verb)
}

// openingServer answers the opening turn with body and records the request.
func openingServer(t *testing.T, status int, body string, got *converseTurnRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(got)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The opening asks for a first line with the trigger and no user text, and pads
// only its first sentence with the greeting lead silence.
func TestConverseDoOpening_TriggerAndLeadPad(t *testing.T) {
	capture := &synthCapture{}
	_, mgr, h := newParityStack(t, capture)
	call := mgr.Create(session.DirectionInbound)
	_ = bindCollectingIO(t, call)

	var got converseTurnRequest
	srv := openingServer(t, http.StatusOK,
		"data: {\"type\":\"delta\",\"text\":\"您好，这里是客服中心。\"}\n\n"+
			"data: {\"type\":\"delta\",\"text\":\"请问有什么可以帮您？\"}\n\n"+
			"data: {\"type\":\"done\",\"commands\":[]}\n\n", &got)

	pad := 700
	a := &converseAgent{h: h, callID: call.ID(), turnURL: srv.URL, log: quietLogger(),
		performance: &PerformanceOverrides{GreetingLeadPadMs: &pad}}
	a.doOpening(context.Background())

	require.Equal(t, converseTriggerOpening, got.Trigger)
	require.Empty(t, got.Text)
	calls := capture.all()
	require.GreaterOrEqual(t, len(calls), 2)
	require.Equal(t, pad, calls[0].opts.LeadPadMs)
	for _, c := range calls[1:] {
		require.Zero(t, c.opts.LeadPadMs, "only the first sentence is padded")
	}
}

// A failed opening stays silent: no "system busy" fallback like doTurn plays.
func TestConverseDoOpening_FailureIsSilent(t *testing.T) {
	capture := &synthCapture{}
	_, mgr, h := newParityStack(t, capture)
	call := mgr.Create(session.DirectionInbound)
	_ = bindCollectingIO(t, call)

	var got converseTurnRequest
	srv := openingServer(t, http.StatusInternalServerError, "boom", &got)

	a := &converseAgent{h: h, callID: call.ID(), turnURL: srv.URL, log: quietLogger()}
	a.doOpening(context.Background())

	require.Empty(t, capture.all())
}

// Commands in the opening reply are not acted on: the caller has said nothing.
func TestConverseDoOpening_IgnoresCommands(t *testing.T) {
	_, _, h := newTestStack(t)
	var got converseTurnRequest
	srv := openingServer(t, http.StatusOK,
		"data: {\"type\":\"done\",\"commands\":[{\"verb\":\"hang_up\",\"params\":{}}]}\n\n", &got)

	callID := "call-open"
	ended := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(callID, func(method string, params map[string]any) {
		if method == evtExternalAgentEnded {
			ended <- params
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: callID, turnURL: srv.URL, log: quietLogger()}
	a.doOpening(context.Background())

	select {
	case <-ended:
		t.Fatal("an opening must not hang up the call")
	case <-time.After(300 * time.Millisecond):
	}
}

// A transfer_human command must emit call.external_agent.handoff carrying the
// group as suggested_target (the payload OpenDesk routes to the 转人工 outlet).
func TestConverseDoTurn_TransferHumanEmitsHandoff(t *testing.T) {
	_, _, h := newTestStack(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Empty reply (no delta) so the turn skips TTS and exercises only commands.
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"done\",\"commands\":[{\"verb\":\"transfer_human\",\"params\":{\"group\":\"complaint\"}}]}\n\n"))
	}))
	defer srv.Close()

	callID := "call-h"
	got := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(callID, func(method string, params map[string]any) {
		if method == evtExternalAgentHandoff {
			got <- params
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: callID, turnURL: srv.URL, log: quietLogger()}
	a.doTurn(context.Background(), "我要投诉", 900, true)

	select {
	case p := <-got:
		inner, _ := p["payload"].(map[string]any)
		require.Equal(t, "complaint", inner["suggested_target"])
	case <-time.After(2 * time.Second):
		t.Fatal("expected a handoff event")
	}
}

// A transfer asked for by a barely-voiced utterance is noise, not a request:
// the reply is still spoken but the handoff must not fire.
func TestConverseDoTurn_WeakEvidenceSkipsTransfer(t *testing.T) {
	_, _, h := newTestStack(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"done\",\"commands\":[{\"verb\":\"transfer_human\",\"params\":{}}]}\n\n"))
	}))
	defer srv.Close()

	callID := "call-weak"
	got := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(callID, func(method string, params map[string]any) {
		if method == evtExternalAgentHandoff {
			got <- params
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: callID, turnURL: srv.URL, log: quietLogger()}
	a.doTurn(context.Background(), "给我吗", 180, true)

	select {
	case <-got:
		t.Fatal("weak-evidence turn must not hand off to a human")
	case <-time.After(300 * time.Millisecond):
	}
}

// A voice the gate could not place is still answered — the caller's own score
// drops far enough on a degraded uplink to be indistinguishable from a
// stranger's, and making them repeat themselves is the worse failure. What it
// does forfeit is the handoff, which cannot be taken back.
func TestConverseDoTurn_UnrecognisedVoiceSkipsTransfer(t *testing.T) {
	_, _, h := newTestStack(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"done\",\"commands\":[{\"verb\":\"transfer_human\",\"params\":{}}]}\n\n"))
	}))
	defer srv.Close()

	callID := "call-unknown-voice"
	got := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(callID, func(method string, params map[string]any) {
		if method == evtExternalAgentHandoff {
			got <- params
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: callID, turnURL: srv.URL, log: quietLogger()}
	a.doTurn(context.Background(), "转人工", 900, false)

	select {
	case <-got:
		t.Fatal("an unrecognised voice must not hand off to a human")
	case <-time.After(300 * time.Millisecond):
	}
}

// A hang_up command must emit call.external_agent.ended with reason "hangup".
func TestConverseDoTurn_HangUpEmitsEnded(t *testing.T) {
	_, _, h := newTestStack(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"done\",\"commands\":[{\"verb\":\"hang_up\",\"params\":{}}]}\n\n"))
	}))
	defer srv.Close()

	callID := "call-e"
	got := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(callID, func(method string, params map[string]any) {
		if method == evtExternalAgentEnded {
			got <- params
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: callID, turnURL: srv.URL, log: quietLogger()}
	a.doTurn(context.Background(), "没别的事了，再见", 900, true)

	select {
	case p := <-got:
		require.Equal(t, "hangup", p["reason"])
	case <-time.After(2 * time.Second):
		t.Fatal("expected an ended event")
	}
}

// idleWaitIO records whether the player waited for the outgoing queue to drain.
type idleWaitIO struct {
	*fakeio.FakeIO
	waited atomic.Bool
}

func (w *idleWaitIO) WaitPlaybackIdle(ctx context.Context) error {
	w.waited.Store(true)
	return w.FakeIO.WaitPlaybackIdle(ctx)
}

// The goodbye's tail must be played out before ended fires, because the call
// controller hangs up as soon as it sees that event.
func TestConverseDoTurn_HangUpDrainsBeforeEnded(t *testing.T) {
	_, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := &idleWaitIO{FakeIO: fakeio.New(256)}
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"done\",\"commands\":[{\"verb\":\"hang_up\",\"params\":{}}]}\n\n"))
	}))
	defer srv.Close()

	drainedAtEnded := make(chan bool, 1)
	unsub := h.SubscribeCall(call.ID(), func(method string, _ map[string]any) {
		if method == evtExternalAgentEnded {
			drainedAtEnded <- io.waited.Load()
		}
	})
	defer unsub()

	a := &converseAgent{h: h, callID: call.ID(), turnURL: srv.URL, log: quietLogger()}
	a.doTurn(context.Background(), "没别的事了，再见", 900, true)

	select {
	case drained := <-drainedAtEnded:
		require.True(t, drained, "ended fired before the outgoing audio drained")
	case <-time.After(2 * time.Second):
		t.Fatal("expected an ended event")
	}
	tail := _hangupTailPadMs * bus.SampleRate / 1000
	require.Eventually(t, func() bool { return totalOutgoing(io.FakeIO) >= tail }, time.Second, 10*time.Millisecond)
}

// Converse legacy barge-in counts accepted partial *events*, not characters.
// SIP_LLM_BARGE_IN_MIN_CHARS must not change that bar — deployments that set it
// to 4 (e.g. .env.v100) would otherwise feel a slower interrupt on the external
// agent path after dialogpolicy centralisation.
func TestConverseLegacyBargeMinPartialsIgnoresBargeInMinChars(t *testing.T) {
	require.Equal(t, 2, converseBargeMinPartials)

	t.Setenv("SIP_LLM_BARGE_IN_MIN_CHARS", "4")
	p, err := dialogpolicy.LoadFromEnv()
	require.NoError(t, err)
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)

	require.Equal(t, 4, dialogpolicy.Get().BargeInMinChars,
		"policy should pick up the env so this test is actually stressing the split")
	require.Equal(t, 2, converseBargeMinPartials,
		"converse legacy path must keep the historical fixed partial-count bar")
}
