package observability

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDialogTurn_DisabledByDefault(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetDialogLogEnabled(false)

	DialogTurn(context.Background(), l, "CIG-1", "call-1", "user", "hello")
	require.Empty(t, buf.String())
}

func TestDialogTurn_WhenEnabled(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	SetDialogLogEnabled(true)
	t.Cleanup(func() { SetDialogLogEnabled(false) })

	DialogTurn(context.Background(), l, "CIG-1", "call-1", "user", "你好")
	out := buf.String()
	for _, want := range []string{"call.dialog", "CIG-1", "call-1", "user", "你好"} {
		require.Contains(t, out, want)
	}
}

func TestInitDialogLogFromEnv(t *testing.T) {
	t.Setenv("LOG_CALL_DIALOG", "true")
	InitDialogLogFromEnv()
	require.True(t, DialogLogEnabled())
	SetDialogLogEnabled(false)
}
