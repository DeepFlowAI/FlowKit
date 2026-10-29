package calls

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestClassifyControlFrame(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		method   string
		terminal bool
		ok       bool
	}{
		{"handoff", `{"type":"handoff","reason":"billing","urgency":"high"}`, evtExternalAgentHandoff, false, true},
		{"error", `{"type":"error","code":42,"message":"boom"}`, evtExternalAgentFailed, true, true},
		{"ended", `{"type":"ended","reason":"done"}`, evtExternalAgentEnded, true, true},
		{"close", `{"type":"close"}`, evtExternalAgentEnded, true, true},
		{"state ended", `{"type":"state","state":"ended"}`, evtExternalAgentEnded, true, true},
		{"state listening", `{"type":"state","state":"listening"}`, "", false, false},
		{"asr partial", `{"type":"asr_partial","text":"hi"}`, "", false, false},
		{"garbage", `not json`, "", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			method, payload, terminal, ok := classifyControlFrame([]byte(c.in))
			require.Equal(t, c.ok, ok)
			require.Equal(t, c.terminal, terminal)
			require.Equal(t, c.method, method)
			if c.method == evtExternalAgentHandoff {
				inner, _ := payload["payload"].(map[string]any)
				require.Equal(t, "billing", inner["reason"])
				require.Equal(t, "high", inner["urgency"])
				_, hasType := inner["type"]
				require.False(t, hasType, "type must be stripped from handoff payload")
			}
		})
	}
}

func TestFrameRing_DropsOldestOnOverflow(t *testing.T) {
	r := newFrameRing[int](3)
	require.False(t, r.push(1))
	require.False(t, r.push(2))
	require.False(t, r.push(3))
	require.True(t, r.push(4), "4th push into a size-3 ring must drop the oldest")
	require.Equal(t, uint64(1), r.droppedCount())

	done := make(chan struct{})
	// Oldest (1) was dropped; remaining order is 2,3,4.
	got := []int{}
	for i := 0; i < 3; i++ {
		v, ok := r.pop(done)
		require.True(t, ok)
		got = append(got, v)
	}
	require.Equal(t, []int{2, 3, 4}, got)
}

func TestFrameRing_PopUnblocksOnDone(t *testing.T) {
	r := newFrameRing[int](2)
	done := make(chan struct{})
	popped := make(chan bool, 1)
	go func() {
		_, ok := r.pop(done)
		popped <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	close(done)
	select {
	case ok := <-popped:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("pop did not unblock on done")
	}
}

// eventCollector captures dispatched call events for assertions.
type eventCollector struct {
	mu     sync.Mutex
	events []collectedEvent
}

type collectedEvent struct {
	method  string
	payload map[string]any
}

func (e *eventCollector) record(method string, payload map[string]any) {
	// dispatch hands the same payload map to subscribers and then to notify
	// (which writes call_id); copy the top level so this collector can read it
	// from the test goroutine without racing that write.
	cp := make(map[string]any, len(payload))
	for k, v := range payload {
		cp[k] = v
	}
	e.mu.Lock()
	e.events = append(e.events, collectedEvent{method: method, payload: cp})
	e.mu.Unlock()
}

func (e *eventCollector) methods() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.events))
	for i, ev := range e.events {
		out[i] = ev.method
	}
	return out
}

func (e *eventCollector) has(method string) bool {
	for _, m := range e.methods() {
		if m == method {
			return true
		}
	}
	return false
}

// fakeExternalAgentServer is an in-process WS endpoint that stands in for a
// VoiceSpeed realtime session.
func fakeExternalAgentServer(t *testing.T, onConn func(*websocket.Conn)) (string, func()) {
	t.Helper()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		onConn(ws)
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, srv.Close
}

func TestConnectExternalAgent_BinaryAudioAndHandoff(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.connect_external_agent")

	// Server: send one binary PCM frame (downstream rate) then a handoff frame.
	serverReady := make(chan struct{})
	wsURL, closeSrv := fakeExternalAgentServer(t, func(ws *websocket.Conn) {
		close(serverReady)
		// 24k PCM frame → flowkit resamples to 16k and writes to the call.
		pcm := make([]int16, 480)
		for i := range pcm {
			pcm[i] = int16(i)
		}
		_ = ws.WriteMessage(websocket.BinaryMessage, int16ToBytes(pcm))
		_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"handoff","reason":"need_human"}`))
		// Keep the connection open so the leg stays alive.
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer closeSrv()

	io := fakeio.New(16)
	go io.CollectOutgoing()
	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, io)

	ec := &eventCollector{}
	unsub := h.SubscribeCall(call.ID(), ec.record)
	defer unsub()

	raw, rpcErr := driveRPC(t, reg, "call.connect_external_agent", map[string]any{
		"call_id":        call.ID(),
		"provider":       "voicespeed",
		"ws_url":         wsURL,
		"token":          "tok",
		"audio_in_rate":  16000,
		"audio_out_rate": 24000,
		"traceparent":    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"trace_call_id":  call.ID(),
	})
	require.Nil(t, rpcErr)
	var res map[string]any
	require.NoError(t, json.Unmarshal(raw, &res))
	require.Equal(t, true, res["ok"])

	<-serverReady

	// connected + handoff events arrive.
	require.Eventually(t, func() bool {
		return ec.has(evtExternalAgentConnected) && ec.has(evtExternalAgentHandoff)
	}, 2*time.Second, 20*time.Millisecond)

	// Downstream audio reached the call's outbound media.
	require.Eventually(t, func() bool {
		return len(io.Outgoing()) > 0
	}, 2*time.Second, 20*time.Millisecond)

	// handoff payload carries the agent fields and a traceparent.
	ec.mu.Lock()
	var handoff map[string]any
	for _, ev := range ec.events {
		if ev.method == evtExternalAgentHandoff {
			handoff = ev.payload
		}
	}
	ec.mu.Unlock()
	require.NotNil(t, handoff)
	require.NotEmpty(t, handoff["traceparent"])
	inner, _ := handoff["payload"].(map[string]any)
	require.Equal(t, "need_human", inner["reason"])
}

func TestConnectExternalAgent_RemoteCloseEmitsEnded(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.connect_external_agent")

	wsURL, closeSrv := fakeExternalAgentServer(t, func(ws *websocket.Conn) {
		_ = ws.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
		_ = ws.Close()
	})
	defer closeSrv()

	io := fakeio.New(8)
	go io.CollectOutgoing()
	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, io)

	ec := &eventCollector{}
	unsub := h.SubscribeCall(call.ID(), ec.record)
	defer unsub()

	_, rpcErr := driveRPC(t, reg, "call.connect_external_agent", map[string]any{
		"call_id":  call.ID(),
		"provider": "voicespeed",
		"ws_url":   wsURL,
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		return ec.has(evtExternalAgentEnded)
	}, 2*time.Second, 20*time.Millisecond)

	// Leg removed from the registry after termination.
	require.Eventually(t, func() bool {
		cs := h.stateGet(call.ID())
		if cs == nil {
			return true
		}
		h.extMu.Lock()
		ok := cs.extAgent != nil
		h.extMu.Unlock()
		return !ok
	}, 2*time.Second, 20*time.Millisecond)
}

func TestConnectExternalAgent_HangupTearsDownSilently(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.connect_external_agent")
	reg.AllowPreHello("call.hangup")

	wsURL, closeSrv := fakeExternalAgentServer(t, func(ws *websocket.Conn) {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer closeSrv()

	io := fakeio.New(8)
	go io.CollectOutgoing()
	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, io)
	id := call.ID()

	ec := &eventCollector{}
	unsub := h.SubscribeCall(id, ec.record)
	defer unsub()

	_, rpcErr := driveRPC(t, reg, "call.connect_external_agent", map[string]any{
		"call_id": id, "provider": "voicespeed", "ws_url": wsURL,
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return ec.has(evtExternalAgentConnected) }, 2*time.Second, 20*time.Millisecond)

	_, rpcErr = driveRPC(t, reg, "call.hangup", map[string]string{"call_id": id})
	require.Nil(t, rpcErr)

	// Leg gone, and the explicit hangup did not emit a terminal failed/ended.
	require.Eventually(t, func() bool {
		cs := h.stateGet(id)
		if cs == nil {
			return true
		}
		h.extMu.Lock()
		ok := cs.extAgent != nil
		h.extMu.Unlock()
		return !ok
	}, 2*time.Second, 20*time.Millisecond)
	require.False(t, ec.has(evtExternalAgentFailed))
}

func TestConnectExternalAgent_DialFailureEmitsFailed(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.connect_external_agent")

	io := fakeio.New(8)
	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, io)

	ec := &eventCollector{}
	unsub := h.SubscribeCall(call.ID(), ec.record)
	defer unsub()

	_, rpcErr := driveRPC(t, reg, "call.connect_external_agent", map[string]any{
		"call_id":  call.ID(),
		"provider": "voicespeed",
		"ws_url":   "ws://127.0.0.1:1/nope",
	})
	require.NotNil(t, rpcErr)
	require.True(t, ec.has(evtExternalAgentFailed))
}
