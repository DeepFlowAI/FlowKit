package observability

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
)

func TestCallTrace_UsesInfoContext(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	l := slog.New(h)

	CallTrace(context.Background(), l, "call.created", "abc-123", "direction", "inbound")

	out := buf.String()
	for _, want := range []string{
		`call.trace`,
		`call.created`,
		`abc-123`,
		`inbound`,
	} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Fatalf("log output missing %q: %s", want, out)
		}
	}
}
