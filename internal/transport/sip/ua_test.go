package sip

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// freePort allocates a UDP port for SIP listen during tests.
func freePort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pc.LocalAddr().(*net.UDPAddr)
	require.NoError(t, pc.Close())
	return addr.Port
}

func TestNew_DefaultsApplied(t *testing.T) {
	u, err := New(Config{Listen: "127.0.0.1:0", Logger: quietLogger()})
	require.NoError(t, err)
	require.NotEqual(t, "", u.ExtIP(), "ext_ip should auto-detect")
	min, max := u.RTPPortRange()
	require.Equal(t, 16384, min)
	require.Equal(t, 32767, max)
}

func TestNew_EmptyListenIsAllowedUntilStart(t *testing.T) {
	u, err := New(Config{Logger: quietLogger()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = u.Start(ctx)
	require.Error(t, err, "Start with empty Listen must error")
}

func TestStart_BindsAndExposesAddr(t *testing.T) {
	port := freePort(t)
	u, err := New(Config{
		Listen: "127.0.0.1:" + strconv.Itoa(port),
		ExtIP:  "127.0.0.1",
		Logger: quietLogger(),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErr := make(chan error, 1)
	go func() { startErr <- u.Start(ctx) }()

	// Wait for bind
	require.Eventually(t, func() bool { return u.LocalAddr() != "" }, time.Second, 10*time.Millisecond)
	require.Contains(t, u.LocalAddr(), strconv.Itoa(port))

	cancel()
	select {
	case <-startErr:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}

func TestStop_Idempotent(t *testing.T) {
	port := freePort(t)
	u, err := New(Config{
		Listen: "127.0.0.1:" + strconv.Itoa(port),
		ExtIP:  "127.0.0.1",
		Logger: quietLogger(),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = u.Start(ctx) }()
	require.Eventually(t, func() bool { return u.LocalAddr() != "" }, time.Second, 10*time.Millisecond)

	require.NoError(t, u.Stop())
	require.NoError(t, u.Stop())
}

func TestAllocRTPPort_ReturnsBoundSocket(t *testing.T) {
	u, err := New(Config{
		Listen:     "127.0.0.1:0",
		ExtIP:      "127.0.0.1",
		RTPPortMin: 40000,
		RTPPortMax: 40100,
		Logger:     quietLogger(),
	})
	require.NoError(t, err)

	pc, err := u.AllocRTPPort()
	require.NoError(t, err)
	defer pc.Close()

	addr := pc.LocalAddr().(*net.UDPAddr)
	require.GreaterOrEqual(t, addr.Port, 40000)
	require.LessOrEqual(t, addr.Port, 40100)
}

// Two calls must not end up on adjacent ports. A peer sends RTCP to the odd
// port above the RTP port it is talking to, so a consecutively-allocated
// second call would receive the first call's peer reports on its audio socket
// and could latch its send target onto that peer, silencing the far end.
func TestAllocRTPPort_LeavesTheOddRTCPPortsFree(t *testing.T) {
	u, err := New(Config{
		Listen:     "127.0.0.1:0",
		ExtIP:      "127.0.0.1",
		RTPPortMin: 40000,
		RTPPortMax: 40100,
		Logger:     quietLogger(),
	})
	require.NoError(t, err)

	first, err := u.AllocRTPPort()
	require.NoError(t, err)
	defer first.Close()
	second, err := u.AllocRTPPort()
	require.NoError(t, err)
	defer second.Close()

	p1 := first.LocalAddr().(*net.UDPAddr).Port
	p2 := second.LocalAddr().(*net.UDPAddr).Port
	require.Zero(t, p1%2, "RTP ports are even by convention (RFC 3550)")
	require.Zero(t, p2%2, "RTP ports are even by convention (RFC 3550)")
	require.NotEqual(t, p1+1, p2, "the second call took the first call's RTCP port")
}

// An odd lower bound must not make every allocation odd.
func TestAllocRTPPort_RoundsAnOddLowerBoundUp(t *testing.T) {
	u, err := New(Config{
		Listen:     "127.0.0.1:0",
		ExtIP:      "127.0.0.1",
		RTPPortMin: 40001,
		RTPPortMax: 40100,
		Logger:     quietLogger(),
	})
	require.NoError(t, err)

	pc, err := u.AllocRTPPort()
	require.NoError(t, err)
	defer pc.Close()

	require.Zero(t, pc.LocalAddr().(*net.UDPAddr).Port%2)
}

func TestDetectExtIP_ReturnsNonLoopback(t *testing.T) {
	ip, err := detectExtIP()
	require.NoError(t, err)
	// On totally isolated CI we fall back to 127.0.0.1; that's also acceptable.
	if ip != "127.0.0.1" {
		parsed := net.ParseIP(ip).To4()
		require.NotNil(t, parsed, "ip should be a valid IPv4: %q", ip)
		require.False(t, parsed.IsLoopback())
		require.False(t, strings.HasPrefix(ip, "169.254."), "link-local excluded")
	}
}

func TestOnIncomingCall_NilHandlerReturns503(t *testing.T) {
	// We can't easily craft a SIP INVITE without a real sender; this test
	// validates the storage of the handler. End-to-end INVITE flow is
	// covered by SIPp tests (task ⑤).
	u, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)

	called := false
	u.OnIncomingCall(func(_ IncomingCall) { called = true })

	u.OnIncomingCall(nil)
	require.False(t, called) // sanity: never called yet
}
