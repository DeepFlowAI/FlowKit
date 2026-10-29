package recording

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type localSinkFactory struct {
	cfg         LocalConfig
	keyTemplate string
	log         *slog.Logger
}

func newLocalSinkFactory(cfg LocalConfig, keyTemplate string, log *slog.Logger) (SinkFactory, error) {
	if log == nil {
		log = slog.Default()
	}
	for _, dir := range []string{cfg.Dir, cfg.OrphanDir} {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("recording local: mkdir %s: %w", dir, err)
		}
	}
	if keyTemplate == "" {
		keyTemplate = "recordings/{date}/{conversation_id}/{call_id}.wav"
	}
	return &localSinkFactory{cfg: cfg, keyTemplate: keyTemplate, log: log}, nil
}

func (f *localSinkFactory) NewSink(_ context.Context, meta ObjectMeta) (StorageSink, error) {
	key := RenderKey(f.keyTemplate, meta)
	path := filepath.Join(f.cfg.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	return &localSink{path: path, log: f.log}, nil
}

type localSink struct {
	path string
	log  *slog.Logger
	mu   sync.Mutex
	f    *os.File
}

func (s *localSink) Open(_ context.Context, _ ObjectMeta, wavHeader []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(wavHeader); err != nil {
		_ = f.Close()
		return err
	}
	s.f = f
	return nil
}

func (s *localSink) Append(_ context.Context, pcm []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return fmt.Errorf("recording local: not open")
	}
	_, err := s.f.Write(pcm)
	return err
}

func (s *localSink) Close(_ context.Context, headerPatch []byte, _ uint32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return s.path, nil
	}
	if len(headerPatch) >= wavHeaderSize {
		if _, err := s.f.WriteAt(headerPatch, 0); err != nil {
			s.log.Warn("recording: local wav header patch failed", "path", s.path, "err", err)
		}
	}
	path := s.path
	if err := s.f.Close(); err != nil {
		return path, err
	}
	s.f = nil
	return "file://" + path, nil
}
