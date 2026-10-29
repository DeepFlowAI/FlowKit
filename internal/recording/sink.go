package recording

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// ObjectMeta identifies one recording object in storage.
type ObjectMeta struct {
	CallID         string
	ConversationID string
	Date           string // YYYY-MM-DD UTC
}

// StorageSink uploads a stereo WAV blob incrementally (Strategy per backend).
// Implementations must not block the audio path for long — batch in Append.
type StorageSink interface {
	// Open creates the object and writes the initial WAV header bytes.
	Open(ctx context.Context, meta ObjectMeta, wavHeader []byte) error
	// Append adds PCM payload (stereo interleaved) after Open.
	Append(ctx context.Context, pcm []byte) error
	// Close flushes tail bytes, optionally patches the WAV header, returns a URL or path.
	Close(ctx context.Context, headerPatch []byte, dataSize uint32) (url string, err error)
}

// SinkFactory builds StorageSink instances (Abstract Factory).
type SinkFactory interface {
	NewSink(ctx context.Context, meta ObjectMeta) (StorageSink, error)
}

// NewSinkFactory selects a factory from storage backend configuration.
func NewSinkFactory(cfg Config, log *slog.Logger) (SinkFactory, error) {
	switch strings.ToLower(cfg.Storage.Backend) {
	case "local":
		return newLocalSinkFactory(cfg.Storage.Local, cfg.Storage.AliyunOSS.KeyTemplate, log)
	case "aliyun_oss":
		return newOSSSinkFactory(cfg.Storage.AliyunOSS, log)
	default:
		return nil, fmt.Errorf("recording: unknown storage backend %q", cfg.Storage.Backend)
	}
}

// RenderKey substitutes {date},{conversation_id},{call_id} in the template.
func RenderKey(tpl string, meta ObjectMeta) string {
	r := strings.NewReplacer(
		"{date}", sanitizeKeyPart(meta.Date),
		"{conversation_id}", sanitizeKeyPart(meta.ConversationID),
		"{call_id}", sanitizeKeyPart(meta.CallID),
	)
	return r.Replace(tpl)
}

func sanitizeKeyPart(s string) string {
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func objectMeta(callID, conversationID string, created time.Time) ObjectMeta {
	return ObjectMeta{
		CallID:         callID,
		ConversationID: conversationID,
		Date:           created.UTC().Format("2006-01-02"),
	}
}
