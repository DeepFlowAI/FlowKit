package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// callTracer is the tracer used for call-lifecycle spans. When the OTel SDK is
// not configured it returns no-op spans (zero allocations).
var callTracer = otel.Tracer(ScopeName)

// callPropagator implements W3C trace-context. We hold an explicit instance so
// inject/extract works regardless of the process-global propagator setting.
var callPropagator = propagation.TraceContext{}

// StartCallSpan starts the root span for a call ("call") with the business key
// call.id = callID. The returned context carries the span; the returned span
// must be ended when the call terminates (see session.Call.SetRootSpan). When
// no OTLP endpoint is configured this is a no-op span.
func StartCallSpan(ctx context.Context, callID string) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := callTracer.Start(ctx, "call")
	span.SetAttributes(attribute.String("call.id", callID))
	return ctx, span
}

// StartChildSpan starts a child span of ctx with the given name and the
// business key call.id = callID. Used for sub-legs such as voicespeed.bridge.
func StartChildSpan(ctx context.Context, name, callID string) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := callTracer.Start(ctx, name)
	span.SetAttributes(attribute.String("call.id", callID))
	return ctx, span
}

// TraceparentFromContext serializes the active span context in ctx to a W3C
// traceparent header value, or "" when ctx carries no recording span.
func TraceparentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	carrier := propagation.MapCarrier{}
	callPropagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// EnsureTraceparent returns a non-empty, well-formed W3C traceparent for the
// span in ctx. When ctx carries a valid (e.g. exported) span context its real
// traceparent is used. Otherwise — no OTLP exporter, a no-op/non-recording span
// — it generates a random trace_id + span_id so every call still has a stable
// trace_id and stays part of the cross-service trace.
func EnsureTraceparent(ctx context.Context) string {
	if tp := TraceparentFromContext(ctx); tp != "" {
		return tp
	}
	var traceID [16]byte
	var spanID [8]byte
	_, _ = rand.Read(traceID[:])
	_, _ = rand.Read(spanID[:])
	return "00-" + hex.EncodeToString(traceID[:]) + "-" + hex.EncodeToString(spanID[:]) + "-01"
}

// TraceIDFromTraceparent extracts the 32-hex trace-id from a W3C traceparent
// ("00-<trace-id>-<span-id>-<flags>"), or "" when malformed.
func TraceIDFromTraceparent(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// ContextFromTraceparent returns a context with the remote span context
// restored from a W3C traceparent header value. Spans started from the result
// continue the propagated trace. An empty/invalid traceparent yields ctx
// unchanged.
func ContextFromTraceparent(ctx context.Context, traceparent string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceparent == "" {
		return ctx
	}
	return callPropagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
