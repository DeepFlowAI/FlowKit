package calls

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

func TestPCMSessionHandlerMarksCallActive(t *testing.T) {
	_, mgr, handlers := newTestStack(t)
	server := httptest.NewServer(handlers.PCMSessionHandler())
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.WriteJSON(map[string]string{
		"provider": "test",
		"turn_url": "http://127.0.0.1:1/turn",
	}))

	_, payload, err := conn.ReadMessage()
	require.NoError(t, err)
	var ack struct {
		CallID string `json:"call_id"`
	}
	require.NoError(t, json.Unmarshal(payload, &ack))
	require.NotEmpty(t, ack.CallID)

	call, err := mgr.Get(ack.CallID)
	require.NoError(t, err)
	require.Equal(t, session.StateActive, call.State())
}
