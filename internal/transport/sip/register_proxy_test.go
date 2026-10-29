package sip

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/extension"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// TestRegister_BindsToEffectivePeerSourceBehindProxy simulates Kamailio:
// REGISTER arrives from loopback with X-Src-Addr = the phone's real public
// source. The registrar must bind to that address, not 127.0.0.1.
func TestRegister_BindsToEffectivePeerSourceBehindProxy(t *testing.T) {
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := New(Config{
		Listen:    listen,
		ExtIP:     "127.0.0.1",
		UserAgent: "flowkit-test",
		Logger:    quietLogger(),
	})
	require.NoError(t, err)
	reg := extension.NewRegistry(time.Now)
	ua.SetExtensionRegistry(reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ua.Start(ctx) }()
	require.Eventually(t, func() bool { return ua.LocalAddr() != "" }, time.Second, 10*time.Millisecond)
	defer func() { _ = ua.Stop() }()

	req := newRegisterRequest("1001", port)
	req.SetSource("127.0.0.1:5070")
	req.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "1001", Host: "192.168.1.50", Port: 5060}})
	req.AppendHeader(sip.NewHeader(headerSrcAddr, "203.0.113.50:40000"))

	ua.handleRegister(req, &stubServerTx{})

	contact, ok := reg.Lookup("1001")
	require.True(t, ok, "extension must be registered")
	require.Equal(t, "203.0.113.50", contact.Host)
	require.Equal(t, 40000, contact.Port)
}

func TestRegister_IgnoresSpoofedSrcAddrFromUntrusted(t *testing.T) {
	port := freePort(t)
	listen := "127.0.0.1:" + strconv.Itoa(port)

	ua, err := New(Config{
		Listen:    listen,
		ExtIP:     "127.0.0.1",
		UserAgent: "flowkit-test",
		Logger:    quietLogger(),
	})
	require.NoError(t, err)
	reg := extension.NewRegistry(time.Now)
	ua.SetExtensionRegistry(reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ua.Start(ctx) }()
	require.Eventually(t, func() bool { return ua.LocalAddr() != "" }, time.Second, 10*time.Millisecond)
	defer func() { _ = ua.Stop() }()

	req := newRegisterRequest("1002", port)
	req.SetSource("192.0.2.43:5060")
	req.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "1002", Host: "10.0.0.9", Port: 5060}})
	req.AppendHeader(sip.NewHeader(headerSrcAddr, "203.0.113.99:40000"))

	ua.handleRegister(req, &stubServerTx{})

	contact, ok := reg.Lookup("1002")
	require.True(t, ok)
	require.Equal(t, "192.0.2.43", contact.Host)
	require.Equal(t, 5060, contact.Port)
}

func TestRegister_LogsSuccessfulRefreshAtDebug(t *testing.T) {
	req := newRegisterRequest("1003", 5060)
	req.SetSource("203.0.113.50:40000")

	var infoLogs bytes.Buffer
	infoLogger := slog.New(slog.NewTextHandler(&infoLogs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	infoUA, err := New(Config{Listen: "127.0.0.1:0", Logger: infoLogger})
	require.NoError(t, err)
	infoUA.handleRegister(req, &stubServerTx{})
	require.NotContains(t, infoLogs.String(), "sip register")

	var debugLogs bytes.Buffer
	debugLogger := slog.New(slog.NewTextHandler(&debugLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	debugUA, err := New(Config{Listen: "127.0.0.1:0", Logger: debugLogger})
	require.NoError(t, err)
	debugUA.handleRegister(req, &stubServerTx{})
	require.True(t, strings.Contains(debugLogs.String(), "sip register"))
}

func newRegisterRequest(user string, listenPort int) *sip.Request {
	req := sip.NewRequest(sip.REGISTER, sip.Uri{User: user, Host: "127.0.0.1", Port: listenPort})
	req.SetTransport("udp")
	fromParams := sip.NewParams()
	fromParams.Add("tag", "regtag")
	req.AppendHeader(&sip.FromHeader{
		Address: sip.Uri{User: user, Host: "127.0.0.1"},
		Params:  fromParams,
	})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: user, Host: "127.0.0.1"}})
	req.AppendHeader(sip.NewHeader("Call-ID", "reg-"+user))
	req.AppendHeader(&sip.CSeqHeader{SeqNo: 1, MethodName: sip.REGISTER})
	req.AppendHeader(sip.NewHeader("Expires", "120"))
	maxFwd := sip.MaxForwardsHeader(70)
	req.AppendHeader(&maxFwd)
	return req
}

// stubServerTx is enough for handleRegister (only Respond is used).
type stubServerTx struct{}

func (s *stubServerTx) Respond(res *sip.Response) error { return nil }
func (s *stubServerTx) Acks() <-chan *sip.Request       { return nil }
func (s *stubServerTx) OnCancel(f sip.FnTxCancel) bool  { return false }
func (s *stubServerTx) Terminate()                      {}
func (s *stubServerTx) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (s *stubServerTx) Err() error { return nil }
func (s *stubServerTx) OnTerminate(f sip.FnTxTerminate) bool {
	return false
}
