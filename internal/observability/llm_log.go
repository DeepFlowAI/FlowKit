package observability

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
)

// LLMRequestLogMessage is the slog/OTel body for outbound LLM HTTP requests.
// Filter: body="llm.request" AND attributes.conversation_id="CIG-...".
const LLMRequestLogMessage = "llm.request"

var llmRequestLogEnabled atomic.Bool

// SetLLMRequestLogEnabled toggles llm.request HTTP debug logging.
func SetLLMRequestLogEnabled(v bool) {
	llmRequestLogEnabled.Store(v)
}

// LLMRequestLogEnabled reports whether LLM request logging is on.
func LLMRequestLogEnabled() bool {
	return llmRequestLogEnabled.Load()
}

// InitLLMRequestLogFromEnv reads LOG_LLM_REQUEST (true/1/yes/on, case-insensitive).
func InitLLMRequestLogFromEnv() {
	SetLLMRequestLogEnabled(parseTruthy(os.Getenv("LOG_LLM_REQUEST")))
}

// LLMRequest logs the outbound LLM HTTP call when LOG_LLM_REQUEST is enabled.
// Authorization and other credential headers are redacted.
func LLMRequest(ctx context.Context, l *slog.Logger, method, url string, headers http.Header, body []byte, attrs ...any) {
	if !LLMRequestLogEnabled() {
		return
	}
	if l == nil {
		l = slog.Default()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	args := make([]any, 0, 10+len(attrs))
	args = append(args,
		"method", method,
		"url", url,
		"headers", RedactHTTPHeaders(headers),
		"body", string(body),
	)
	args = append(args, attrs...)
	l.InfoContext(ctx, LLMRequestLogMessage, args...)
}

// RedactHTTPHeaders returns a copy of headers with credential values masked.
func RedactHTTPHeaders(h http.Header) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) == 0 {
			continue
		}
		val := vs[0]
		if isSensitiveHeader(k) {
			val = redactCredential(val)
		}
		out[k] = val
	}
	return out
}

func isSensitiveHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "authorization", "proxy-authorization", "x-api-key", "api-key":
		return true
	default:
		return false
	}
}

func redactCredential(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(v), "bearer ") {
		return "Bearer [REDACTED]"
	}
	return "[REDACTED]"
}
