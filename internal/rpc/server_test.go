package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/config"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// quietLogger returns a slog.Logger that discards records, keeping test output clean.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// startTestServer launches a Server bound to :0 with a handler that echoes
// requests back. It returns the ws URL, the *Server, and a teardown function.
func startTestServer(t *testing.T, h Handler) (wsURL string, srv *Server, teardown func()) {
	t.Helper()

	cfg := config.RPCConfig{
		Listen:       "127.0.0.1:0",
		HelloTimeout: 0, // disable for most tests
	}
	srv = New(cfg, h, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Start(ctx)
		close(done)
	}()

	// Wait until listeners are bound — happens-before via channel close.
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server failed to bind within 2s")
	}
	wsURL = "ws://" + srv.Addr() + "/ws"
	require.NotEmpty(t, wsURL)

	teardown = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("server did not stop within 3s")
		}
	}
	return wsURL, srv, teardown
}

func TestServer_StartsAndStops(t *testing.T) {
	cfg := config.RPCConfig{Listen: "127.0.0.1:0"}
	srv := New(cfg, HandlerFunc{}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Start(ctx) }()

	<-srv.Ready()
	require.NotEmpty(t, srv.Addr())

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server stop timed out")
	}
}

func TestServer_RejectsNoListenerConfig(t *testing.T) {
	srv := New(config.RPCConfig{}, HandlerFunc{}, quietLogger())
	err := srv.Start(context.Background())
	require.Error(t, err)
}

func TestServer_WS_RoundTrip(t *testing.T) {
	wsURL, _, teardown := startTestServer(t, HandlerFunc{
		OnRequest: func(_ context.Context, _ *Conn, r *Request) *Response {
			require.Equal(t, "echo", r.Method)
			resp, _ := NewResponse(r.ID, map[string]string{"echoed": string(r.Params)})
			return resp
		},
	})
	defer teardown()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer c.Close()

	req, err := NewRequest("echo", "hello", "r1")
	require.NoError(t, err)
	frame, err := json.Marshal(req)
	require.NoError(t, err)
	require.NoError(t, c.WriteMessage(websocket.TextMessage, frame))

	_, data, err := c.ReadMessage()
	require.NoError(t, err)

	in, err := Decode(data)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.Equal(t, `"r1"`, string(in.Response.ID))
}

func TestServer_WS_BadJSON_ReturnsParseError(t *testing.T) {
	wsURL, _, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("not json")))

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := c.ReadMessage()
	require.NoError(t, err)

	in, err := Decode(data)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.NotNil(t, in.Response.Error)
	require.Equal(t, CodeParseError, in.Response.Error.Code)
}

func TestServer_WS_Notification_NoResponse(t *testing.T) {
	got := make(chan string, 1)
	wsURL, _, teardown := startTestServer(t, HandlerFunc{
		OnNotification: func(_ context.Context, _ *Conn, n *Notification) {
			got <- n.Method
		},
	})
	defer teardown()

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer c.Close()

	req, _ := NewRequest("event.ping", nil, nil) // nil id = notification
	frame, _ := json.Marshal(req)
	require.NoError(t, c.WriteMessage(websocket.TextMessage, frame))

	select {
	case m := <-got:
		require.Equal(t, "event.ping", m)
	case <-time.After(2 * time.Second):
		t.Fatal("handler never called for notification")
	}
}

func TestServer_HelloTimeout_ClosesConnection(t *testing.T) {
	cfg := config.RPCConfig{
		Listen:       "127.0.0.1:0",
		HelloTimeout: 200 * time.Millisecond,
	}
	srv := New(cfg, HandlerFunc{}, quietLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Start(ctx) }()
	<-srv.Ready()

	c, _, err := websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws", nil)
	require.NoError(t, err)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = c.ReadMessage()
	require.Error(t, err, "connection should be closed by server after hello timeout")
}

func TestServer_ConcurrentRequests(t *testing.T) {
	var counter atomic.Int32
	wsURL, _, teardown := startTestServer(t, HandlerFunc{
		OnRequest: func(_ context.Context, _ *Conn, r *Request) *Response {
			counter.Add(1)
			resp, _ := NewResponse(r.ID, map[string]int{"n": int(counter.Load())})
			return resp
		},
	})
	defer teardown()

	const clients = 10
	const perClient = 20
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if !assertNoErr(t, err) {
				return
			}
			defer c.Close()
			for j := 0; j < perClient; j++ {
				id := fmt.Sprintf("c%d-r%d", i, j)
				req, _ := NewRequest("add", nil, id)
				frame, _ := json.Marshal(req)
				if !assertNoErr(t, c.WriteMessage(websocket.TextMessage, frame)) {
					return
				}
				_, _, err = c.ReadMessage()
				if !assertNoErr(t, err) {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int32(clients*perClient), counter.Load())
}

// Test Unix Socket transport
func TestServer_UnixSocket_RoundTrip(t *testing.T) {
	// macOS limits unix socket paths to 104 chars; t.TempDir() is too long. Use /tmp.
	sockPath := filepath.Join("/tmp", fmt.Sprintf("fk-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(sockPath) })
	cfg := config.RPCConfig{Unix: sockPath}

	srv := New(cfg, HandlerFunc{
		OnRequest: func(_ context.Context, _ *Conn, r *Request) *Response {
			resp, _ := NewResponse(r.ID, map[string]string{"pong": "yes"})
			return resp
		},
	}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Start(ctx) }()
	<-srv.Ready()

	c, err := net.Dial("unix", sockPath)
	require.NoError(t, err)
	defer c.Close()

	req, _ := NewRequest("ping", nil, "u1")
	frame, _ := json.Marshal(req)
	_, err = c.Write(append(frame, '\n'))
	require.NoError(t, err)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(c)
	line, err := reader.ReadBytes('\n')
	require.NoError(t, err)

	in, err := Decode(line)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.JSONEq(t, `{"pong":"yes"}`, string(in.Response.Result))
}

func TestServer_BroadcastNotification(t *testing.T) {
	wsURL, srv, teardown := startTestServer(t, HandlerFunc{})
	defer teardown()

	const clients = 3
	conns := make([]*websocket.Conn, 0, clients)
	for i := 0; i < clients; i++ {
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.NoError(t, err)
		conns = append(conns, c)
		defer c.Close()
	}

	require.Eventually(t, func() bool { return srv.ConnCount() == clients }, time.Second, 5*time.Millisecond)

	n, _ := NewNotification("system.health", map[string]int{"calls": 0})
	srv.BroadcastNotification(n)

	for _, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, data, err := c.ReadMessage()
		require.NoError(t, err)
		in, err := Decode(data)
		require.NoError(t, err)
		require.Equal(t, KindNotification, in.Kind)
		require.Equal(t, "system.health", in.Notification.Method)
	}
}

// --- helpers ---

func assertNoErr(t *testing.T, err error) bool {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return false
	}
	return true
}
