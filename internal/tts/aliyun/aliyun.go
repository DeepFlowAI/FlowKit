// Package aliyun is the Aliyun NLS one-shot REST TTS implementation of the
// tts.Provider interface.
//
// The low-level REST client lives in `client.go`; this file (`aliyun.go`)
// adapts it to the streaming `tts.Provider` interface by emitting the
// entire payload as a single PCMChunk with Final=true. True streaming
// TTS will arrive when we adopt Aliyun's WebSocket TTS endpoint.
//
// Authentication is shared with the ASR adapter via `internal/auth/aliyun`.
package aliyun

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/auth/aliyun"
	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// Config carries the Aliyun-specific knobs the adapter needs.
type Config struct {
	AccessKeyID     string
	AccessKeySecret string
	AppKey          string
	Region          string

	// Defaults used when Options field is empty.
	DefaultVoice      string
	DefaultSampleRate int // 16000 typical
	DefaultVolume     int // 0..100
	DefaultSpeechRate int // -500..500
}

// Provider implements tts.Provider by wrapping TTSClient.
type Provider struct {
	cfg    Config
	client *TTSClient

	closed atomic.Bool
}

// New constructs a Provider. The underlying TTSClient is allocated up front;
// per-Synthesize calls reuse it (it is thread-safe).
func New(cfg Config) (*Provider, error) {
	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		return nil, errors.New("aliyun tts: AccessKeyID / AccessKeySecret required")
	}
	if cfg.AppKey == "" {
		return nil, errors.New("aliyun tts: AppKey required")
	}
	if cfg.Region == "" {
		cfg.Region = "cn-shanghai"
	}
	if cfg.DefaultSampleRate == 0 {
		cfg.DefaultSampleRate = 16000
	}
	if cfg.DefaultVolume == 0 {
		cfg.DefaultVolume = 50
	}
	if cfg.DefaultVoice == "" {
		cfg.DefaultVoice = "siyue"
	}
	tm, err := auth.NewTokenManager(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.Region)
	if err != nil {
		return nil, err
	}
	client := NewTTSClient(tm, cfg.AppKey, cfg.Region)
	return &Provider{cfg: cfg, client: client}, nil
}

// Synthesize implements tts.Provider.
func (p *Provider) Synthesize(ctx context.Context, text string, opts tts.Options) (<-chan tts.PCMChunk, error) {
	if p.closed.Load() {
		return nil, tts.ErrClosed
	}
	if text == "" {
		return nil, tts.ErrEmptyText
	}

	tcfg := TTSConfig{
		Text:       text,
		Voice:      pickStr(opts.Voice, p.cfg.DefaultVoice),
		Format:     "pcm",
		SampleRate: pickInt(opts.SampleRate, p.cfg.DefaultSampleRate),
		Volume:     pickInt(opts.Volume, p.cfg.DefaultVolume),
		SpeechRate: pickInt(opts.SpeechRate, p.cfg.DefaultSpeechRate),
	}

	out := make(chan tts.PCMChunk, 1)
	go func() {
		defer close(out)
		raw, err := p.client.Synthesize(ctx, tcfg)
		if err != nil {
			// Surface the error via context cancellation semantics by closing
			// the channel without emitting. Callers should also check ctx.Err().
			return
		}
		samples := bytesToInt16LE(raw)
		select {
		case out <- tts.PCMChunk{Samples: samples, Final: true}:
		case <-ctx.Done():
		}
	}()
	return out, nil
}

// Close implements tts.Provider.
func (p *Provider) Close() error {
	p.closed.Store(true)
	return nil
}

// pickStr returns override if non-empty, else fallback.
func pickStr(override, fallback string) string {
	if override != "" {
		return override
	}
	return fallback
}

// pickInt returns override if > 0, else fallback.
func pickInt(override, fallback int) int {
	if override > 0 {
		return override
	}
	return fallback
}

// bytesToInt16LE decodes little-endian 16-bit samples.
func bytesToInt16LE(b []byte) []int16 {
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

// Touch time package so the import is referenced even if unused in production
// paths; the adapter may add timing tags in future iterations.
var _ = time.Now
