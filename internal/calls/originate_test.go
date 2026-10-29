package calls

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	flowsip "github.com/DeepFlowAI/flowkit/internal/transport/sip"
	"github.com/stretchr/testify/require"
)

type stubSIPOriginate struct {
	lastURI      string
	lastCallerID string
	lastTrunkID  string
	lastCall     *session.Call
}

func (s *stubSIPOriginate) StartOriginateCall(call *session.Call, uri, callerID, trunkID string, _ time.Duration, _ map[string]string) error {
	s.lastCall = call
	s.lastURI = uri
	s.lastCallerID = callerID
	s.lastTrunkID = trunkID
	return nil
}

func registerOriginate(t *testing.T, reg *rpc.Registry, h *Handlers) {
	t.Helper()
	h.EnableOriginate(0)
	h.RegisterOriginate(reg)
	reg.AllowPreHello("system.capacity")
}

func TestHandleOriginate_Disabled(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{"uri": "13800138000"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeMethodNotFound, rpcErr.Code)
}

func TestHandleOriginate_ReturnsOriginating(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	stub := &stubSIPOriginate{}
	h.SetSIPOriginateController(stub)
	registerOriginate(t, reg, h)

	result, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":        "13800138000",
		"caller_id":  "123",
		"timeout_ms": 30000,
	})
	require.Nil(t, rpcErr)
	require.Contains(t, string(result), `"status":"originating"`)
	require.Contains(t, string(result), `"call_id"`)
	require.Equal(t, "13800138000", stub.lastURI)
	require.Equal(t, "123", stub.lastCallerID)
	require.Equal(t, session.DirectionOutbound, stub.lastCall.Direction())
	require.Equal(t, 1, mgr.Count())
}

func TestHandleOriginate_PassesTrunkID(t *testing.T) {
	reg, _, h := newTestStack(t)
	stub := &stubSIPOriginate{}
	h.SetSIPOriginateController(stub)
	registerOriginate(t, reg, h)

	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":      "13800138000",
		"trunk_id": "trunk_sh_main",
	})
	require.Nil(t, rpcErr)
	require.Equal(t, "trunk_sh_main", stub.lastTrunkID)
}

func TestHandleOriginate_MapsTrunkErrorsToRPC(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(trunkErrFailer{err: flowsip.ErrTrunkNotFound})
	registerOriginate(t, reg, h)

	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":      "13800138000",
		"trunk_id": "nope",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
	require.Contains(t, rpcErr.Message, "trunk_id not found")
}

type trunkErrFailer struct{ err error }

func (f trunkErrFailer) StartOriginateCall(_ *session.Call, _, _, _ string, _ time.Duration, _ map[string]string) error {
	return f.err
}

func TestHandleOriginate_NoSIP(t *testing.T) {
	reg, _, h := newTestStack(t)
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{"uri": "13800138000"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInternalError, rpcErr.Code)
}

func TestHandleOriginate_MissingURI(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestHandleOriginate_RejectBlockedDestination(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{"uri": "110"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestHandleOriginate_RejectForbiddenHeader(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":     "13800138000",
		"headers": map[string]string{"From": "evil@example.com"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestHandleOriginate_RejectCreate(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	mgr.BeginShutdown()
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{"uri": "13800138000"})
	require.NotNil(t, rpcErr)
}

func TestHandleOriginate_IdempotentRequestID(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	stub := &stubSIPOriginate{}
	h.SetSIPOriginateController(stub)
	registerOriginate(t, reg, h)

	first, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":        "13800138000",
		"request_id": "ao-attempt-1",
	})
	require.Nil(t, rpcErr)
	require.Contains(t, string(first), `"request_id":"ao-attempt-1"`)
	require.Equal(t, 1, mgr.Count())

	second, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":        "13800138000",
		"request_id": "ao-attempt-1",
	})
	require.Nil(t, rpcErr)
	require.Equal(t, string(first), string(second))
	require.Equal(t, 1, mgr.Count())
}

func TestHandleList_IncludesRequestID(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
		"uri":        "13800138000",
		"request_id": "ao-list-1",
	})
	require.Nil(t, rpcErr)

	listed, rpcErr := driveRPC(t, reg, "call.list", map[string]any{})
	require.Nil(t, rpcErr)
	require.Contains(t, string(listed), `"request_id":"ao-list-1"`)
}

func TestHandleCapacity_ReportsOriginateRemaining(t *testing.T) {
	reg, _, h := newTestStack(t)
	h.SetSIPOriginateController(&stubSIPOriginate{})
	registerOriginate(t, reg, h)
	h.EnableOriginate(4)

	result, rpcErr := driveRPC(t, reg, "system.capacity", map[string]any{})
	require.Nil(t, rpcErr)
	require.Contains(t, string(result), `"originate_max_concurrent":4`)
	require.Contains(t, string(result), `"originate_remaining":4`)
}

func TestHandleOriginate_StartError(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	h.SetSIPOriginateController(originateFailer{})
	registerOriginate(t, reg, h)
	_, rpcErr := driveRPC(t, reg, "call.originate", map[string]string{"uri": "13800138000"})
	require.NotNil(t, rpcErr)
	require.Eventually(t, func() bool { return mgr.Count() == 0 }, time.Second, 10*time.Millisecond)
}

type originateFailer struct{}

func (originateFailer) StartOriginateCall(_ *session.Call, _, _, _ string, _ time.Duration, _ map[string]string) error {
	return errors.New("trunk not configured")
}

// gatingSIPOriginate blocks inside StartOriginateCall until release is closed.
// entered is signaled once per Start so tests can wait for admission.
type gatingSIPOriginate struct {
	started atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newGatingSIPOriginate() *gatingSIPOriginate {
	return &gatingSIPOriginate{
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
}

func (g *gatingSIPOriginate) StartOriginateCall(call *session.Call, _, _, _ string, _ time.Duration, _ map[string]string) error {
	g.started.Add(1)
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	_ = call
	return nil
}

func TestHandleOriginate_ConcurrentDuplicateRequestID_SingleFlight(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	gate := newGatingSIPOriginate()
	h.SetSIPOriginateController(gate)
	registerOriginate(t, reg, h)

	const n = 16
	var startBarrier sync.WaitGroup
	startBarrier.Add(n)
	results := make([]json.RawMessage, n)
	errs := make([]*rpc.Error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			startBarrier.Done()
			startBarrier.Wait()
			results[i], errs[i] = driveRPC(t, reg, "call.originate", map[string]any{
				"uri":        "13800138000",
				"request_id": "dup-concurrent-1",
			})
		}(i)
	}

	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first StartOriginateCall")
	}
	require.Equal(t, int32(1), gate.started.Load())
	close(gate.release)
	wg.Wait()

	require.Equal(t, int32(1), gate.started.Load())
	require.Equal(t, 1, mgr.Count())
	var callID string
	for i := 0; i < n; i++ {
		require.Nil(t, errs[i], "goroutine %d: %v", i, errs[i])
		var got map[string]any
		require.NoError(t, json.Unmarshal(results[i], &got))
		id, _ := got["call_id"].(string)
		require.NotEmpty(t, id)
		if callID == "" {
			callID = id
		} else {
			require.Equal(t, callID, id)
		}
		require.Equal(t, "dup-concurrent-1", got["request_id"])
	}
}

func TestHandleOriginate_ConcurrentMaxConcurrency_ReservesAtomically(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	gate := newGatingSIPOriginate()
	h.SetSIPOriginateController(gate)
	registerOriginate(t, reg, h)
	h.EnableOriginate(2)

	const n = 12
	var startBarrier sync.WaitGroup
	startBarrier.Add(n)
	var success, limited atomic.Int32
	callIDs := make(chan string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			startBarrier.Done()
			startBarrier.Wait()
			result, rpcErr := driveRPC(t, reg, "call.originate", map[string]any{
				"uri": "13800138000",
			})
			if rpcErr != nil {
				require.Equal(t, rpc.CodeRateLimited, rpcErr.Code)
				limited.Add(1)
				return
			}
			success.Add(1)
			var got map[string]any
			require.NoError(t, json.Unmarshal(result, &got))
			callIDs <- got["call_id"].(string)
		}()
	}

	require.Eventually(t, func() bool {
		return gate.started.Load() == 2 && limited.Load() == int32(n-2)
	}, 2*time.Second, 5*time.Millisecond)
	require.LessOrEqual(t, int(atomic.LoadInt32(&h.originateInFlight)), 2)
	require.Equal(t, int32(2), gate.started.Load())
	require.Equal(t, int32(n-2), limited.Load())
	require.Equal(t, 2, mgr.Count())

	close(gate.release)
	wg.Wait()
	require.Equal(t, int32(2), success.Load())
	require.Equal(t, int32(2), gate.started.Load())
	close(callIDs)
	ids := map[string]struct{}{}
	for id := range callIDs {
		ids[id] = struct{}{}
	}
	require.Len(t, ids, 2)
}
