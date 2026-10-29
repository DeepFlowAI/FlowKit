package calls

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

type dtmfFakeIO struct {
	*fakeio.FakeIO
	sent string
}

func (d *dtmfFakeIO) SendDTMF(digits string, _ int) error {
	d.sent = digits
	return nil
}

func TestHandleDTMF_SendsViaMediaIO(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := &dtmfFakeIO{FakeIO: fakeio.New(4)}
	require.NoError(t, call.BindMediaIO(io))

	_, rpcErr := driveRPC(t, reg, "call.dtmf", map[string]any{
		"call_id": call.ID(),
		"digits":  "12#",
	})
	require.Nil(t, rpcErr)
	require.Equal(t, "12#", io.sent)
}

func TestHandleDTMF_UnsupportedTransport(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.BindMediaIO(fakeio.New(4)))

	_, rpcErr := driveRPC(t, reg, "call.dtmf", map[string]any{
		"call_id": call.ID(),
		"digits":  "1",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallStateInvalid, rpcErr.Code)
}

var _ transport.DTMFSender = (*dtmfFakeIO)(nil)
