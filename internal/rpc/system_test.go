package rpc

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSystemHello_MarksConnReady(t *testing.T) {
	reg := NewRegistry(quietLogger())
	RegisterSystemMethods(reg, ServerInfo{
		Name:    "flowkit",
		Version: "0.0.1",
		NodeID:  "flowkit-test",
	}, nil)

	require.True(t, reg.HasMethod("system.hello"))
	require.True(t, reg.HasMethod("system.shutdown"))

	conn := &Conn{}
	require.False(t, conn.IsHelloOK())

	params, _ := json.Marshal(HelloParams{
		SDK:          "flowkit-sdk-py",
		Version:      "0.1.0",
		Capabilities: []string{"events"},
	})
	req := &Request{JSONRPC: Version, Method: "system.hello", Params: params, ID: json.RawMessage(`"h1"`)}
	resp := reg.ProcessRequest(context.Background(), conn, req)

	require.Nil(t, resp.Error)
	require.True(t, conn.IsHelloOK())

	var got HelloResult
	require.NoError(t, json.Unmarshal(resp.Result, &got))
	require.Equal(t, "flowkit", got.Server)
	require.Equal(t, "0.0.1", got.Version)
	require.Equal(t, "flowkit-test", got.NodeID)
	require.WithinDuration(t, time.Now().UTC(), got.Time, 5*time.Second)
}

func TestSystemHello_InvalidParams(t *testing.T) {
	reg := NewRegistry(quietLogger())
	RegisterSystemMethods(reg, ServerInfo{Name: "flowkit"}, nil)

	conn := &Conn{}
	req := &Request{JSONRPC: Version, Method: "system.hello", Params: json.RawMessage(`not json`), ID: json.RawMessage(`"x"`)}
	resp := reg.ProcessRequest(context.Background(), conn, req)
	require.NotNil(t, resp.Error)
	require.Equal(t, CodeInvalidParams, resp.Error.Code)
	require.False(t, conn.IsHelloOK(), "failed hello must not flip the flag")
}

func TestSystemShutdown_CallsHook(t *testing.T) {
	var called atomic.Bool
	reg := NewRegistry(quietLogger())
	RegisterSystemMethods(reg, ServerInfo{Name: "flowkit"}, func() { called.Store(true) })

	conn := &Conn{}
	conn.MarkHelloOK()
	req := &Request{JSONRPC: Version, Method: "system.shutdown", ID: json.RawMessage(`"s1"`)}
	resp := reg.ProcessRequest(context.Background(), conn, req)

	require.Nil(t, resp.Error)
	var got ShutdownResult
	require.NoError(t, json.Unmarshal(resp.Result, &got))
	require.True(t, got.OK)

	// hook runs in a goroutine; give it a moment
	require.Eventually(t, called.Load, time.Second, 5*time.Millisecond)
}

func TestSystemShutdown_NilHook_IsOK(t *testing.T) {
	reg := NewRegistry(quietLogger())
	RegisterSystemMethods(reg, ServerInfo{Name: "flowkit"}, nil)
	conn := &Conn{}
	conn.MarkHelloOK()
	req := &Request{JSONRPC: Version, Method: "system.shutdown", ID: json.RawMessage(`"s2"`)}
	resp := reg.ProcessRequest(context.Background(), conn, req)
	require.Nil(t, resp.Error)
}

func TestSystemShutdown_RejectedBeforeHello(t *testing.T) {
	var called atomic.Bool
	reg := NewRegistry(quietLogger())
	RegisterSystemMethods(reg, ServerInfo{Name: "flowkit"}, func() { called.Store(true) })
	req := &Request{JSONRPC: Version, Method: "system.shutdown", ID: json.RawMessage(`"s3"`)}
	resp := reg.ProcessRequest(context.Background(), &Conn{}, req)
	require.NotNil(t, resp.Error)
	require.Equal(t, CodeNotReady, resp.Error.Code)
	require.False(t, called.Load())
}

func TestHealthBroadcaster_TicksAndStops(t *testing.T) {
	// Need a Server to broadcast through. Start a real one.
	wsURL, srv, teardown := startTestServer(t, NewRegistry(quietLogger()))
	defer teardown()
	_ = wsURL // unused; broadcaster only needs srv

	var ticks atomic.Int32
	hb := NewHealthBroadcaster(srv, 30*time.Millisecond, func() HealthParams {
		ticks.Add(1)
		return HealthParams{Calls: 0, UptimeS: 1}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hb.Run(ctx); close(done) }()

	// Wait for at least 3 ticks
	require.Eventually(t, func() bool { return ticks.Load() >= 3 }, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcaster did not stop")
	}
}
