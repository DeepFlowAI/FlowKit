package recording

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

const ossAppendRetries = 3

type ossSinkFactory struct {
	cfg    AliyunOSSConfig
	client *oss.Client
	bucket *oss.Bucket
	log    *slog.Logger
}

func newOSSSinkFactory(cfg AliyunOSSConfig, log *slog.Logger) (SinkFactory, error) {
	if log == nil {
		log = slog.Default()
	}
	client, err := oss.New(cfg.Endpoint, cfg.AccessKey, cfg.AccessSecret,
		oss.Timeout(10, 120))
	if err != nil {
		return nil, fmt.Errorf("recording oss: client: %w", err)
	}
	bucket, err := client.Bucket(cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("recording oss: bucket: %w", err)
	}
	return &ossSinkFactory{cfg: cfg, client: client, bucket: bucket, log: log}, nil
}

func (f *ossSinkFactory) NewSink(_ context.Context, meta ObjectMeta) (StorageSink, error) {
	key := RenderKey(f.cfg.KeyTemplate, meta)
	return &ossSink{
		bucket: f.bucket,
		key:    key,
		cfg:    f.cfg,
		log:    f.log,
	}, nil
}

type ossSink struct {
	bucket   *oss.Bucket
	key      string
	cfg      AliyunOSSConfig
	log      *slog.Logger
	mu       sync.Mutex
	position int64
	opened   bool
}

func (s *ossSink) appendOpts() []oss.Option {
	opts := []oss.Option{oss.ContentType("audio/wav")}
	if s.cfg.SSE {
		opts = append(opts, oss.ServerSideEncryption("AES256"))
	}
	return opts
}

func (s *ossSink) Open(ctx context.Context, _ ObjectMeta, wavHeader []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	next, err := s.bucket.AppendObject(s.key, bytes.NewReader(wavHeader), 0, s.appendOpts()...)
	if err != nil {
		return fmt.Errorf("recording oss: append header: %w", err)
	}
	s.position = next
	s.opened = true
	return nil
}

func (s *ossSink) Append(ctx context.Context, pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		return fmt.Errorf("recording oss: not open")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return s.appendLocked(ctx, pcm)
}

func (s *ossSink) appendLocked(ctx context.Context, pcm []byte) error {
	var lastErr error
	backoff := 10 * time.Millisecond
	for attempt := 0; attempt < ossAppendRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 10
			if err := s.syncPosition(ctx); err != nil && s.log != nil {
				s.log.Warn("recording oss: sync position failed", "err", err)
			}
		}
		next, err := s.bucket.AppendObject(s.key, bytes.NewReader(pcm), s.position, s.appendOpts()...)
		if err == nil {
			s.position = next
			return nil
		}
		lastErr = err
		if !isOSSPositionMismatch(err) {
			break
		}
		if err := s.syncPosition(ctx); err != nil {
			lastErr = err
			break
		}
	}
	return fmt.Errorf("recording oss: append: %w", lastErr)
}

func isOSSPositionMismatch(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "PositionNotEqualToLength") ||
		strings.Contains(msg, "409") ||
		strings.Contains(msg, "position")
}

func (s *ossSink) syncPosition(ctx context.Context) error {
	meta, err := s.bucket.GetObjectDetailedMeta(s.key)
	if err != nil {
		// Fallback: Content-Length from GetObjectMeta.
		headers, err2 := s.bucket.GetObjectMeta(s.key)
		if err2 != nil {
			return err
		}
		cl := headers.Get("Content-Length")
		if cl == "" {
			return err
		}
		n, err3 := strconv.ParseInt(cl, 10, 64)
		if err3 != nil {
			return err3
		}
		s.position = n
		return nil
	}
	if pos := meta.Get("x-oss-next-append-position"); pos != "" {
		n, err := strconv.ParseInt(pos, 10, 64)
		if err == nil {
			s.position = n
			return nil
		}
	}
	if cl := meta.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err == nil {
			s.position = n
			return nil
		}
	}
	return fmt.Errorf("recording oss: no append position in meta")
}

func (s *ossSink) Close(ctx context.Context, headerPatch []byte, dataSize uint32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	url := s.objectURL()
	if !s.opened {
		return url, nil
	}
	s.opened = false

	if dataSize == 0 {
		if err := s.bucket.DeleteObject(s.key); err != nil && s.log != nil {
			s.log.Debug("recording oss: delete empty object", "key", s.key, "err", err)
		}
		return "", nil
	}
	if len(headerPatch) >= wavHeaderSize {
		if err := s.patchWAVHeader(ctx, headerPatch); err != nil {
			if s.log != nil {
				s.log.Error("recording oss: wav header patch failed",
					"key", s.key, "data_size", dataSize, "err", err)
			}
			return url, err
		}
		if s.log != nil {
			s.log.Debug("recording oss: wav header patched",
				"key", s.key, "data_size", dataSize)
		}
	}
	return url, nil
}

// patchWAVHeader downloads the appendable object, rewrites the 44-byte RIFF
// header, and replaces the object via PutObject (players require correct sizes).
func (s *ossSink) patchWAVHeader(ctx context.Context, patch []byte) error {
	_, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	rc, err := s.bucket.GetObject(s.key)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if len(body) < wavHeaderSize {
		return fmt.Errorf("object too short: %d bytes", len(body))
	}
	copy(body[:wavHeaderSize], patch[:wavHeaderSize])
	if err := s.bucket.PutObject(s.key, bytes.NewReader(body), s.appendOpts()...); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	return nil
}

func (s *ossSink) objectURL() string {
	ep := strings.TrimSuffix(s.cfg.Endpoint, "/")
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	return fmt.Sprintf("https://%s.%s/%s", s.cfg.Bucket, ep, s.key)
}
