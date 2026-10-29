package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/DeepFlowAI/flowkit/internal/config"
)

// InitFromConfig is the structured-config entry point that the production
// kernel (cmd/flowkit) uses. It bridges config.ObservabilityConfig to the
// env-var-driven Init by setting any unset env keys, configuring slog with
// the requested log level, then delegating.
//
// Returns a ShutdownFunc that the caller must defer.
func InitFromConfig(ctx context.Context, cfg config.ObservabilityConfig) (ShutdownFunc, error) {
	// Surface config values as standard OTel env vars when not already set
	// in the environment (env wins over config — same precedence rule the
	// rest of the kernel follows).
	if cfg.ServiceName != "" {
		_ = setIfEmpty("OTEL_SERVICE_NAME", cfg.ServiceName)
	}
	if cfg.OTLPEndpoint != "" {
		_ = setIfEmpty("OTEL_EXPORTER_OTLP_ENDPOINT", cfg.OTLPEndpoint)
	}
	// Init 只读 LOG_LEVEL；配置文件里的 log_level 也要同步进去，
	// 否则 multiHandler.min 会静默落回 info，丢掉 cfg 想开的 debug。
	if cfg.LogLevel != "" {
		_ = setIfEmpty("LOG_LEVEL", cfg.LogLevel)
	}

	// Configure slog first so any logs during Init are levelled correctly.
	SetupSlog(cfg.LogLevel)

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "flowkit"
	}
	return Init(ctx, serviceName)
}

// SetupSlog configures slog.Default with the requested level. An empty or
// unrecognised level falls back to info. This function is a no-op for OTel —
// Init() additionally bridges slog to OTel logs when an OTLP endpoint is set.
func SetupSlog(level string) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: parseLevel(level),
	})))
}

// parseLevel maps "debug"/"info"/"warn"/"error" (case-insensitive, "" → info)
// to a slog.Level.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}

func setIfEmpty(key, value string) error {
	if os.Getenv(key) != "" {
		return nil
	}
	return os.Setenv(key, value)
}
