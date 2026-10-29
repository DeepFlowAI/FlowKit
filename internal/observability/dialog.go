package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

// DialogLogMessage is the slog/OTel body for per-turn transcript records.
// Filter: body="call.dialog" AND attributes.conversation_id="CIG-...".
const DialogLogMessage = "call.dialog"

var dialogLogEnabled atomic.Bool

// SetDialogLogEnabled toggles call.dialog transcript logging at runtime.
func SetDialogLogEnabled(v bool) {
	dialogLogEnabled.Store(v)
}

// DialogLogEnabled reports whether transcript logging is on.
func DialogLogEnabled() bool {
	return dialogLogEnabled.Load()
}

// InitDialogLogFromEnv reads LOG_CALL_DIALOG (true/1/yes/on, case-insensitive).
func InitDialogLogFromEnv() {
	SetDialogLogEnabled(parseTruthy(os.Getenv("LOG_CALL_DIALOG")))
}

func parseTruthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// DialogTurn logs one user or assistant utterance when dialog logging is enabled.
func DialogTurn(ctx context.Context, l *slog.Logger, conversationID, callID, role, text string, attrs ...any) {
	if !DialogLogEnabled() || strings.TrimSpace(text) == "" {
		return
	}
	if l == nil {
		l = slog.Default()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	args := make([]any, 0, 8+len(attrs))
	args = append(args,
		"conversation_id", conversationID,
		"call_id", callID,
		"role", role,
		"text", text,
	)
	args = append(args, attrs...)
	l.InfoContext(ctx, DialogLogMessage, args...)
}
