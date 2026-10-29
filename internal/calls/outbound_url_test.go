package calls

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckOutboundURL_EmptyAllowlistAllowsAll(t *testing.T) {
	h := &Handlers{}
	require.NoError(t, h.checkOutboundURL("http://169.254.169.254/latest/meta-data"))
}

func TestCheckOutboundURL_Allowlist(t *testing.T) {
	h := &Handlers{}
	h.SetOutboundURLAllowedHosts([]string{" Agent.Example.com ", ".cdn.example.net", ""})

	allowed := []string{
		"https://agent.example.com/api/v1/turn/stream",
		"wss://AGENT.example.com:8443/realtime",
		"https://a.cdn.example.net/prompt.wav",
		"https://x.y.cdn.example.net/prompt.wav",
	}
	for _, u := range allowed {
		require.NoError(t, h.checkOutboundURL(u), u)
	}

	denied := []string{
		"http://169.254.169.254/latest/meta-data",
		"http://127.0.0.1:5002/turn",
		"https://evil-agent.example.com/turn",
		"https://agent.example.com.evil.test/turn",
		"https://cdn.example.net/prompt.wav", // ".cdn.example.net" is subdomains only
		"not a url",
		"",
	}
	for _, u := range denied {
		require.Error(t, h.checkOutboundURL(u), u)
	}
}
