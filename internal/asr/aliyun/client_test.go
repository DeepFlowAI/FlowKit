package aliyun

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestStartASRClient_WaitsForTranscriptionStarted(t *testing.T) {
	startedSent := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		header, _ := start["header"].(map[string]any)
		if header["name"] != "StartTranscription" {
			return
		}

		if err := conn.WriteJSON(map[string]any{
			"header": map[string]any{"name": "TranscriptionStarted", "status": 20000000},
		}); err != nil {
			return
		}
		close(startedSent)

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	client, err := startASRClient(context.Background(), conn, "appkey", DefaultASRConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	select {
	case <-startedSent:
	case <-time.After(time.Second):
		t.Fatal("server did not send TranscriptionStarted")
	}
}

func TestASRClient_SendPCM_AfterStarted(t *testing.T) {
	ready := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		if err := conn.WriteJSON(map[string]any{
			"header": map[string]any{"name": "TranscriptionStarted", "status": 20000000},
		}); err != nil {
			return
		}
		close(ready)

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	client, err := startASRClient(context.Background(), conn, "appkey", DefaultASRConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("server not ready for PCM")
	}

	require.NoError(t, client.SendPCM([]byte{0, 0, 1, 0}))
}

func TestStartASRClient_TimesOutWithoutTranscriptionStarted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	client, err := startASRClient(ctx, conn, "appkey", DefaultASRConfig())
	require.Error(t, err)
	require.Nil(t, client)
	require.ErrorContains(t, err, "TranscriptionStarted")
}

func TestStartASRClient_FailsOnTaskFailedBeforeReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"header": map[string]any{
				"name":           "TaskFailed",
				"status":         40000002,
				"status_message": "Invalid binary message while server state is 'ROUTING'",
			},
		})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	client, err := startASRClient(context.Background(), conn, "appkey", DefaultASRConfig())
	require.Error(t, err)
	require.Nil(t, client)
	require.ErrorContains(t, err, "ROUTING")
}

func TestASRClient_StopAndWait_IncludesAppKey(t *testing.T) {
	stopCh := make(chan map[string]any, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"header": map[string]any{"name": "TranscriptionStarted", "status": 20000000},
		})

		var stop map[string]any
		if err := conn.ReadJSON(&stop); err != nil {
			return
		}
		stopCh <- stop
		_ = conn.WriteJSON(map[string]any{
			"header": map[string]any{"name": "TranscriptionCompleted", "status": 20000000},
		})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	client, err := startASRClient(context.Background(), conn, "test-appkey", DefaultASRConfig())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, client.StopAndWait(ctx))

	select {
	case stop := <-stopCh:
		header, _ := stop["header"].(map[string]any)
		require.Equal(t, "test-appkey", header["appkey"])
		require.Equal(t, "StopTranscription", header["name"])
	case <-time.After(time.Second):
		t.Fatal("server did not receive StopTranscription")
	}
}

func TestTranslate_TaskFailed(t *testing.T) {
	ev := translate(ASREvent{Type: "task_failed", Text: "boom"})
	require.Equal(t, asr.EventError, ev.Type)
	require.ErrorContains(t, ev.Err, "boom")
}

func TestIsNLSIdleTimeout(t *testing.T) {
	require.True(t, isNLSIdleTimeout(40000004, "", `{"header":{"status_text":"Gateway:IDLE_TIMEOUT:Websocket session is idle"}}`))
	require.False(t, isNLSIdleTimeout(40000002, "ROUTING", ""))
}

func TestASRClient_IdleTimeoutClosesCleanly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		var start map[string]any
		if err := conn.ReadJSON(&start); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"header": map[string]any{"name": "TranscriptionStarted", "status": 20000000},
		})
		_ = conn.WriteJSON(map[string]any{
			"header": map[string]any{
				"name":       "TaskFailed",
				"status":     40000004,
				"status_text": "Gateway:IDLE_TIMEOUT:Websocket session is idle for too long time, the last directive is 'StartTranscription'!",
			},
		})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)

	client, err := startASRClient(context.Background(), conn, "appkey", DefaultASRConfig())
	require.NoError(t, err)

	_, ok := <-client.Events()
	require.True(t, ok)
	_, ok = <-client.Events()
	require.False(t, ok, "events channel should close without surfacing task_failed")
	require.NoError(t, client.getCloseErr())
}
