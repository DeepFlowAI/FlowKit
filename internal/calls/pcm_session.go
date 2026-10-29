package calls

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/wspcm"
)

const (
	pcmSessionInBuf  = 32
	pcmSessionOutBuf = 64
	maxPCMSendAhead  = time.Second
)

var pcmUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true },
}

// pcmSessionStart is the first (JSON) frame on /media/pcm: the converse routing so
// flowkit runs the full core on the leg and calls the given turn endpoint (brain).
type pcmSessionStart struct {
	Provider    string                `json:"provider"`
	TurnURL     string                `json:"turn_url"`
	Token       string                `json:"token"`
	Greeting    string                `json:"greeting"`
	Opening     string                `json:"opening"`
	VoiceSpkID  string                `json:"voice_spk_id"`
	Traceparent string                `json:"traceparent"`
	Performance *PerformanceOverrides `json:"performance,omitempty"`
	// SendAheadMs > 0 announces a client with its own playout buffer that
	// applies playback control frames; TTS is then sent that far ahead of
	// real time (see wspcm.WithSendAhead). Capped at maxPCMSendAhead.
	SendAheadMs int `json:"send_ahead_ms,omitempty"`
}

// PCMSessionHandler serves /media/pcm: a WebSocket carrying 16 kHz mono int16 PCM
// both directions. The client (VoiceSpeed backend bridging a browser) sends one
// JSON start frame, then streams binary PCM; flowkit creates a call on the leg and
// runs converse, so the leg exercises the REAL FlowKit core (ASR / VAD / adaptive
// barge / turn detection / full-pause / streaming TTS). TTS PCM streams back as
// binary frames. Register with rpc.Server.RegisterHTTP("/media/pcm", ...).
func (h *Handlers) PCMSessionHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := pcmUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ws.SetReadLimit(1 << 20) // 1 MiB max frame
		_, data, err := ws.ReadMessage()
		if err != nil {
			_ = ws.Close()
			return
		}
		var start pcmSessionStart
		if err := json.Unmarshal(data, &start); err != nil || start.TurnURL == "" {
			_ = ws.WriteMessage(websocket.TextMessage,
				[]byte(`{"error":"first frame must be JSON with turn_url"}`))
			_ = ws.Close()
			return
		}
		if err := h.checkOutboundURL(start.TurnURL); err != nil {
			msg, _ := json.Marshal(map[string]string{"error": err.Error()})
			_ = ws.WriteMessage(websocket.TextMessage, msg)
			_ = ws.Close()
			return
		}

		call := h.mgr.Create(session.DirectionInbound)
		if call == nil {
			_ = ws.Close()
			return
		}
		sendAhead := min(time.Duration(max(start.SendAheadMs, 0))*time.Millisecond, maxPCMSendAhead)
		mio := wspcm.New(ws, pcmSessionInBuf, pcmSessionOutBuf, h.log, wspcm.WithSendAhead(sendAhead))
		if err := call.BindMediaIO(mio); err != nil {
			_ = mio.Close()
			_ = h.mgr.Close(call.ID(), "bind_failed")
			return
		}
		if err := call.SetState(session.StateAnswered); err != nil {
			_ = mio.Close()
			_ = h.mgr.Close(call.ID(), "answer_failed")
			return
		}
		if err := call.SetState(session.StateActive); err != nil {
			_ = mio.Close()
			_ = h.mgr.Close(call.ID(), "activate_failed")
			return
		}

		spanCtx, span := observability.StartCallSpan(call.Context(), call.RootCallID())
		call.SetRootSpan(spanCtx, func() { span.End() })
		observability.CallTraceCall(spanCtx, h.log, "pcm.session", call, "transport", "wspcm",
			"send_ahead_ms", sendAhead.Milliseconds())

		// Ack the call id so the bridge can correlate, then run converse.
		_ = ws.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"ready","call_id":"`+call.ID()+`"}`))

		provider := start.Provider
		if provider == "" {
			provider = "voicespeed-bridge"
		}
		if err := h.startConverse(call, nil, converseExternalAgentParams{
			Provider:    provider,
			TurnURL:     start.TurnURL,
			Token:       start.Token,
			Greeting:    start.Greeting,
			Opening:     start.Opening,
			VoiceSpkID:  start.VoiceSpkID,
			Traceparent: start.Traceparent,
			Performance: start.Performance,
		}); err != nil {
			h.log.Warn("pcm session: start converse failed", "call_id", call.ID(), "err", err)
			_ = mio.Close()
			_ = h.mgr.Close(call.ID(), "converse_failed")
			return
		}

		// Hold the request open until the leg (WS) or the call ends, then tear down.
		select {
		case <-mio.Done():
		case <-call.Done():
		}
		_ = mio.Close()
		_ = h.mgr.Close(call.ID(), "pcm_session_ended")
	})
}
