package observability

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

type stubCall struct {
	id string
}

func (s stubCall) ID() string           { return s.id }
func (s stubCall) TraceAttrs() []any    { return nil }

func TestASRStreamTrace_DisabledByDefault(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetASRStreamLogEnabled(false)

	ASRStreamTrace(context.Background(), l, "asr.speech_partial", stubCall{id: "c1"}, "text", "你好")
	require.Empty(t, buf.String())
}

func TestASRStreamTrace_WhenEnabled(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetASRStreamLogEnabled(true)
	t.Cleanup(func() { SetASRStreamLogEnabled(false) })

	ASRStreamTrace(context.Background(), l, "asr.speech_partial", stubCall{id: "c1"}, "text", "床前")
	out := buf.String()
	for _, want := range []string{"call.trace", "asr.speech_partial", "c1", "床前"} {
		require.Contains(t, out, want)
	}
}

func TestInitASRStreamLogFromEnv(t *testing.T) {
	t.Setenv("LOG_ASR_STREAM", "true")
	InitASRStreamLogFromEnv()
	require.True(t, ASRStreamLogEnabled())
	SetASRStreamLogEnabled(false)
}
