package observability

import (
	"context"
	"log/slog"
)

// CallTraceMessage is the slog/OTel log body for lifecycle records.
// Filter in any OTLP backend: body="call.trace" AND attributes.call_id="<uuid>".
const CallTraceMessage = "call.trace"

// CallTrace emits a structured call lifecycle log.
//
// All records flow through slog.Default() (or the injected logger), which
// observability.Init bridges to OTLP logs when OTEL_EXPORTER_OTLP_ENDPOINT
// is set. The same record is also written to stderr for local debugging.
//
// Pass a request context when available so OTel can correlate logs with
// active trace spans (trace_id / span_id on the log record).
func CallTrace(ctx context.Context, l *slog.Logger, event, callID string, attrs ...any) {
	if l == nil {
		l = slog.Default()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	args := make([]any, 0, 2+len(attrs))
	args = append(args, "event", event, "call_id", callID)
	args = append(args, attrs...)
	l.InfoContext(ctx, CallTraceMessage, args...)
}

// CallTraceCall is like CallTrace but includes conversation_id, root_call_id,
// and phase from the Call.
func CallTraceCall(ctx context.Context, l *slog.Logger, event string, c interface {
	ID() string
	TraceAttrs() []any
}, attrs ...any) {
	if c == nil {
		CallTrace(ctx, l, event, "", attrs...)
		return
	}
	merged := append(c.TraceAttrs(), attrs...)
	CallTrace(ctx, l, event, c.ID(), merged...)
}
