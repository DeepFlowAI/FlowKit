package rpc

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry_Register_AndCall(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.AllowPreHello("test.echo")
	reg.Register("test.echo", func(_ context.Context, _ *Conn, params json.RawMessage) (any, *Error) {
		// Return the params unchanged (as a parsed object) so the caller can
		// assert structure rather than rely on escape-sequence semantics.
		var obj map[string]any
		if rpcErr := DecodeParams(params, &obj); rpcErr != nil {
			return nil, rpcErr
		}
		return obj, nil
	})

	require.True(t, reg.HasMethod("test.echo"))

	req, _ := NewRequest("test.echo", map[string]string{"x": "y"}, "r1")
	resp := reg.ProcessRequest(context.Background(), &Conn{}, req)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	require.JSONEq(t, `{"x":"y"}`, string(resp.Result))
}

func TestRegistry_MethodNotFound(t *testing.T) {
	reg := NewRegistry(quietLogger())
	req, _ := NewRequest("never.registered", nil, "r1")
	resp := reg.ProcessRequest(context.Background(), &Conn{}, req)
	require.NotNil(t, resp.Error)
	require.Equal(t, CodeMethodNotFound, resp.Error.Code)
}

func TestRegistry_HelloGate(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.Register("call.answer", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) {
		return map[string]string{"ok": "yes"}, nil
	})

	// Conn that has NOT marked hello OK
	conn := &Conn{}
	require.False(t, conn.IsHelloOK())

	req, _ := NewRequest("call.answer", nil, "r1")
	resp := reg.ProcessRequest(context.Background(), conn, req)
	require.NotNil(t, resp.Error)
	require.Equal(t, CodeNotReady, resp.Error.Code, "non-allow-listed method must be gated by hello")

	// After hello, call should succeed
	conn.MarkHelloOK()
	resp = reg.ProcessRequest(context.Background(), conn, req)
	require.Nil(t, resp.Error)
}

func TestRegistry_PreHelloAllowedMethods(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.Register("system.hello", func(_ context.Context, c *Conn, _ json.RawMessage) (any, *Error) {
		c.MarkHelloOK()
		return map[string]string{"ack": "y"}, nil
	})

	conn := &Conn{}
	req, _ := NewRequest("system.hello", nil, "r1")
	resp := reg.ProcessRequest(context.Background(), conn, req)
	require.Nil(t, resp.Error, "system.hello must work pre-hello")
	require.True(t, conn.IsHelloOK())
}

func TestRegistry_HandlerReturnsError(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.AllowPreHello("test.fail")
	reg.Register("test.fail", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) {
		return nil, ErrInvalidParams("missing field x")
	})

	req, _ := NewRequest("test.fail", nil, "r1")
	resp := reg.ProcessRequest(context.Background(), &Conn{}, req)
	require.Equal(t, CodeInvalidParams, resp.Error.Code)
	require.Contains(t, resp.Error.Message, "missing field x")
}

func TestRegistry_NilResult_BecomesEmptyObject(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.AllowPreHello("test.void")
	reg.Register("test.void", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) {
		return nil, nil
	})
	req, _ := NewRequest("test.void", nil, "r1")
	resp := reg.ProcessRequest(context.Background(), &Conn{}, req)
	require.Nil(t, resp.Error)
	require.JSONEq(t, `{}`, string(resp.Result))
}

func TestRegistry_Notification(t *testing.T) {
	reg := NewRegistry(quietLogger())
	got := make(chan string, 1)
	reg.RegisterNotification("test.fire", func(_ context.Context, _ *Conn, _ json.RawMessage) {
		got <- "fired"
	})

	n, _ := NewNotification("test.fire", nil)
	reg.ProcessNotification(context.Background(), &Conn{}, n)
	require.Equal(t, "fired", <-got)
}

func TestRegistry_UnknownNotification_IsSilentlyDropped(t *testing.T) {
	reg := NewRegistry(quietLogger())
	n, _ := NewNotification("unknown.event", nil)
	// Should not panic
	reg.ProcessNotification(context.Background(), &Conn{}, n)
}

func TestRegistry_ConcurrentDispatch(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.AllowPreHello("test.add")
	var counter atomic.Int64
	reg.Register("test.add", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) {
		counter.Add(1)
		return nil, nil
	})

	const N = 200
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := NewRequest("test.add", nil, "x")
			reg.ProcessRequest(context.Background(), &Conn{}, req)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(N), counter.Load())
}

func TestDecodeParams_HappyPath(t *testing.T) {
	type p struct {
		X int `json:"x"`
	}
	var out p
	require.Nil(t, DecodeParams(json.RawMessage(`{"x":42}`), &out))
	require.Equal(t, 42, out.X)
}

func TestDecodeParams_EmptyTreatedAsObject(t *testing.T) {
	type p struct {
		X int `json:"x"`
	}
	var out p
	require.Nil(t, DecodeParams(nil, &out))
	require.Equal(t, 0, out.X)
}

func TestDecodeParams_InvalidJSON(t *testing.T) {
	var out struct{}
	rpcErr := DecodeParams(json.RawMessage(`not json`), &out)
	require.NotNil(t, rpcErr)
	require.Equal(t, CodeInvalidParams, rpcErr.Code)
}

func TestRegistry_RegisterIgnoresEmpty(t *testing.T) {
	reg := NewRegistry(quietLogger())
	reg.Register("", func(_ context.Context, _ *Conn, _ json.RawMessage) (any, *Error) { return nil, nil })
	reg.Register("noop", nil)
	require.False(t, reg.HasMethod(""))
	require.False(t, reg.HasMethod("noop"))
}
