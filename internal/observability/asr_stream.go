package observability

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
)

// InitASRStreamLogFromEnv reads LOG_ASR_STREAM (true/1/yes/on, case-insensitive).
// When enabled, pumpASREvents logs every ASR partial and VAD boundary at INFO
// (body=call.trace, event=asr.speech_partial|asr.speech_start).
func InitASRStreamLogFromEnv() {
	SetASRStreamLogEnabled(parseTruthy(os.Getenv("LOG_ASR_STREAM")))
}

var asrStreamLogEnabled atomic.Bool

// SetASRStreamLogEnabled toggles streaming ASR trace logs.
func SetASRStreamLogEnabled(v bool) {
	asrStreamLogEnabled.Store(v)
}

// ASRStreamLogEnabled reports whether streaming ASR logging is on.
func ASRStreamLogEnabled() bool {
	return asrStreamLogEnabled.Load()
}

// ASRStreamTrace logs one streaming ASR event when LOG_ASR_STREAM is enabled.
func ASRStreamTrace(ctx context.Context, l *slog.Logger, event string, c interface {
	ID() string
	TraceAttrs() []any
}, attrs ...any) {
	if !ASRStreamLogEnabled() || c == nil {
		return
	}
	CallTraceCall(ctx, l, event, c, attrs...)
}
