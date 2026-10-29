package sip

import (
	"strconv"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/extension"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// callerExtensionFixture builds a Signal whose registrar holds one phone,
// registered from registeredHost.
func callerExtensionFixture(t *testing.T, aor, registeredHost string) *Signal {
	t.Helper()
	ua, err := New(Config{
		Listen:    "127.0.0.1:" + strconv.Itoa(freePort(t)),
		ExtIP:     "127.0.0.1",
		UserAgent: "flowkit-test",
		Logger:    quietLogger(),
	})
	require.NoError(t, err)
	reg := extension.NewRegistry(time.Now)
	reg.Register(aor, sip.Uri{User: aor, Host: registeredHost, Port: 40000}, time.Minute)
	ua.SetExtensionRegistry(reg)
	return NewSignal(ua, session.NewManager(quietLogger()), nil, quietLogger())
}

func inviteFrom(user string) *sip.Request {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "13800138000", Host: "127.0.0.1"})
	req.SetTransport("udp")
	params := sip.NewParams()
	params.Add("tag", "fromtag")
	req.AppendHeader(&sip.FromHeader{
		Address: sip.Uri{User: user, Host: "192.168.31.250", Port: 5070},
		Params:  params,
	})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "13800138000", Host: "127.0.0.1"}})
	return req
}

// An agent dialling from their desk phone must be recognised, so the layer
// above can place the call they dialled instead of routing them as a customer.
func TestResolveCallerExtension_RecognisesRegisteredPhone(t *testing.T) {
	s := callerExtensionFixture(t, "9999", "203.0.113.50")

	require.Equal(t, "9999", s.resolveCallerExtension(inviteFrom("9999"), "203.0.113.50:31509"))
}

// Handsets register and call through different NAT port mappings, so the port
// carries no information — insisting it match would reject real phones.
func TestResolveCallerExtension_ToleratesADifferentNATPort(t *testing.T) {
	s := callerExtensionFixture(t, "9999", "203.0.113.50")

	require.Equal(t, "9999", s.resolveCallerExtension(inviteFrom("9999"), "203.0.113.50:55123"))
}

// From is a claim anyone can write. Honouring it unchecked would let a stranger
// place calls billed to someone else's extension, so the call must arrive from
// the address that extension registered from.
func TestResolveCallerExtension_RejectsAForgedFrom(t *testing.T) {
	s := callerExtensionFixture(t, "9999", "203.0.113.50")

	require.Empty(t, s.resolveCallerExtension(inviteFrom("9999"), "192.0.2.43:5060"))
}

// A number nobody registered is just an ordinary inbound call.
func TestResolveCallerExtension_IgnoresUnregisteredCaller(t *testing.T) {
	s := callerExtensionFixture(t, "9999", "203.0.113.50")

	require.Empty(t, s.resolveCallerExtension(inviteFrom("1234"), "203.0.113.50:31509"))
}

// Deployments without extensions must behave exactly as before.
func TestResolveCallerExtension_NoRegistryMeansNoExtension(t *testing.T) {
	ua, err := New(Config{
		Listen:    "127.0.0.1:" + strconv.Itoa(freePort(t)),
		ExtIP:     "127.0.0.1",
		UserAgent: "flowkit-test",
		Logger:    quietLogger(),
	})
	require.NoError(t, err)
	s := NewSignal(ua, session.NewManager(quietLogger()), nil, quietLogger())

	require.Empty(t, s.resolveCallerExtension(inviteFrom("9999"), "203.0.113.50:31509"))
}
