package sip

import (
	"net"
	"testing"
	"time"

	flowrtp "github.com/DeepFlowAI/flowkit/internal/media/rtp"
	"github.com/stretchr/testify/require"
)

func TestMediaIO_ReceivesDTMFEndPacket(t *testing.T) {
	pcA, err := listenUDP()
	require.NoError(t, err)
	pcB, err := listenUDP()
	require.NoError(t, err)
	defer pcA.Close()
	defer pcB.Close()

	const dtmfPT = uint8(96)
	a, err := NewMediaIO(pcA, pcB.LocalAddr().String(), flowrtp.PayloadTypePCMU, dtmfPT, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()

	got := make(chan string, 1)
	a.SetDTMFHandler(func(digit string) {
		got <- digit
	})

	sess := flowrtp.NewSession(flowrtp.PayloadTypePCMU, 160)
	payload := flowrtp.BuildDTMFPayload(5, true, 10, 640)
	wire, err := sess.PackTelephoneEvent(payload, dtmfPT, true)
	require.NoError(t, err)
	_, err = pcB.WriteTo(wire, pcA.LocalAddr())
	require.NoError(t, err)

	select {
	case digit := <-got:
		require.Equal(t, "5", digit)
	case <-time.After(2 * time.Second):
		t.Fatal("dtmf handler not invoked")
	}
}

func TestMediaIO_SendDTMF_RequiresNegotiation(t *testing.T) {
	pcA, err := listenUDP()
	require.NoError(t, err)
	defer pcA.Close()
	a, err := NewMediaIO(pcA, "127.0.0.1:9", flowrtp.PayloadTypePCMU, 0, quietLogger(), 0, "")
	require.NoError(t, err)
	defer a.Close()
	require.Error(t, a.SendDTMF("1", 160))
}

func listenUDP() (*net.UDPConn, error) {
	return net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
}
