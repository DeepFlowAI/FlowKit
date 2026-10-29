package observability

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLLMRequest_DisabledByDefault(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetLLMRequestLogEnabled(false)

	h := http.Header{}
	h.Set("Authorization", "Bearer secret-key")
	LLMRequest(context.Background(), l, "POST", "https://api.example/v1/chat/completions", h, []byte(`{"model":"m"}`))
	require.Empty(t, buf.String())
}

func TestLLMRequest_WhenEnabled(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetLLMRequestLogEnabled(true)
	t.Cleanup(func() { SetLLMRequestLogEnabled(false) })

	h := http.Header{}
	h.Set("Authorization", "Bearer secret-key")
	h.Set("X-SID", "call-1")
	body := []byte(`{"model":"deepseek-chat","stream":true}`)
	LLMRequest(context.Background(), l, "POST", "https://api.example/v1/chat/completions", h, body,
		"conversation_id", "CIG-1")

	out := buf.String()
	require.Contains(t, out, "llm.request")
	require.Contains(t, out, "deepseek-chat")
	require.Contains(t, out, "call-1")
	require.Contains(t, out, "Bearer [REDACTED]")
	require.NotContains(t, out, "secret-key")
}

func TestRedactHTTPHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer abc")
	h.Set("Content-Type", "application/json")
	out := RedactHTTPHeaders(h)
	require.Equal(t, "Bearer [REDACTED]", out["Authorization"])
	require.Equal(t, "application/json", out["Content-Type"])
}
