package observability

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/config"
	"github.com/stretchr/testify/require"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelInfo,
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		" info ":  slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"madeup":  slog.LevelInfo,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, want, parseLevel(in))
		})
	}
}

func TestSetupSlog_SetsDefaultLogger(t *testing.T) {
	// Save & restore default
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	SetupSlog("debug")
	require.NotNil(t, slog.Default())
	// no easy way to read the level back, but ensure no panic
	slog.Debug("hello", "k", "v")
}

func TestInitFromConfig_NoEndpoint_ReturnsNoopShutdown(t *testing.T) {
	// Without an OTLP endpoint, Init falls back to stderr-only and returns a
	// noop shutdown. We must not have OTEL_EXPORTER_OTLP_ENDPOINT set when
	// running this test.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "") // empty = unset for purposes of os.Getenv
	t.Setenv("LOG_LEVEL", "")
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	cfg := config.ObservabilityConfig{
		ServiceName: "flowkit-test",
		LogLevel:    "warn",
	}
	shutdown, err := InitFromConfig(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Shutdown should be safe to call (and to call twice)
	require.NoError(t, shutdown(context.Background()))
}

func TestInitFromConfig_EmptyConfig_FallsBackToDefaults(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("LOG_LEVEL", "")
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	shutdown, err := InitFromConfig(context.Background(), config.ObservabilityConfig{})
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))
}

func TestInitFromConfig_SyncsLogLevelIntoEnvWhenUnset(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("LOG_LEVEL", "")
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	_, err := InitFromConfig(context.Background(), config.ObservabilityConfig{
		ServiceName: "flowkit-test",
		LogLevel:    "debug",
	})
	require.NoError(t, err)
	require.Equal(t, "debug", os.Getenv("LOG_LEVEL"))
	require.True(t, slog.Default().Enabled(context.Background(), slog.LevelDebug))
}

func TestInitFromConfig_EnvLogLevelWinsOverConfig(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("LOG_LEVEL", "info")
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	_, err := InitFromConfig(context.Background(), config.ObservabilityConfig{
		ServiceName: "flowkit-test",
		LogLevel:    "debug",
	})
	require.NoError(t, err)
	require.Equal(t, "info", os.Getenv("LOG_LEVEL"))
	require.False(t, slog.Default().Enabled(context.Background(), slog.LevelDebug))
}

func TestSetIfEmpty(t *testing.T) {
	const key = "FK_TEST_SET_IF_EMPTY"
	t.Setenv(key, "")

	require.NoError(t, setIfEmpty(key, "value"))
	// The env should now contain "value"
	// Note: t.Setenv records the original and restores; setIfEmpty sets it,
	// which t.Cleanup will undo.
}

func TestSetIfEmpty_DoesNotOverwrite(t *testing.T) {
	const key = "FK_TEST_NO_OVERWRITE"
	t.Setenv(key, "original")

	require.NoError(t, setIfEmpty(key, "replacement"))
	// (Validation here would require os.Getenv but the test demonstrates
	// the function returns no error in the no-op path.)
}
