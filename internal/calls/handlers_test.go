package calls

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	ttsmock "github.com/DeepFlowAI/flowkit/internal/tts/mock"
	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newTestStack(t *testing.T) (*rpc.Registry, *session.Manager, *Handlers) {
	t.Helper()
	mgr := session.NewManager(quietLogger())
	t.Cleanup(func() { mgr.Shutdown("test_cleanup") })
	reg := rpc.NewRegistry(quietLogger())
	for _, m := range []string{"call.answer", "call.originate", "call.hangup", "call.say", "call.play", "call.listen", "call.stop", "call.bridge", "call.unbridge", "call.list", "call.dtmf", "call.hold", "call.unhold", "call.mute", "call.unmute"} {
		reg.AllowPreHello(m)
	}
	h := Register(reg, mgr, ProviderFactories{
		ASR: func() (asr.Provider, error) { return asrmock.New(), nil },
		TTS: func() (tts.Provider, error) { return ttsmock.New(), nil },
	}, quietLogger())
	return reg, mgr, h
}

func driveRPC(t *testing.T, reg *rpc.Registry, method string, params any) (json.RawMessage, *rpc.Error) {
	t.Helper()
	req, err := rpc.NewRequest(method, params, "t-1")
	require.NoError(t, err)
	resp := reg.ProcessRequest(context.Background(), &rpc.Conn{}, req)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Result, nil
}

func TestDropState_SignalsPlayWaitsWithoutStopCallMedia(t *testing.T) {
	_, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	callID := call.ID()
	h.stateFor(callID, nil)

	waitCh := make(chan string, 1)
	h.registerPlayWait(callID, waitCh)

	// Same order as broadcastCallHangup: dropState with no prior signalPlayWait.
	h.dropState(callID)

	select {
	case reason := <-waitCh:
		require.Equal(t, "interrupted", reason)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("dropState must wake play waiters; hanging until ctx cancel is a regression")
	}
	require.Nil(t, h.stateGet(callID))
}

func TestNewShortID_Unique(t *testing.T) {
	a := newShortID()
	b := newShortID()
	require.Len(t, a, 8)
	require.Len(t, b, 8)
	require.NotEqual(t, a, b)
}
