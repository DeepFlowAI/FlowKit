// Package aliyun is the Aliyun NLS streaming ASR implementation of the
// asr.Provider interface.
//
// The low-level WebSocket protocol client lives in `client.go` within this
// package; this file (`aliyun.go`) is the adapter that exposes it as an
// `asr.Provider`. Authentication (TokenManager) is shared with the TTS
// adapter via `internal/auth/aliyun`.
package aliyun

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/auth/aliyun"
	"github.com/DeepFlowAI/flowkit/internal/asr"
)

// Config wraps the Aliyun-specific knobs the adapter needs. AccessKey* +
// AppKey are required; Region defaults to "cn-shanghai".
type Config struct {
	AccessKeyID     string
	AccessKeySecret string
	AppKey          string
	Region          string

	// Optional overrides; zero values pick DefaultASRConfig().
	SampleRate               int
	EnableIntermediateResult bool
	EnablePunctuation        bool
	EnableITN                bool
	OverrideDefaults         bool // set to apply the above fields

	// MaxSentenceSilenceMs sets NLS 尾点静音阈值 (ms); 0 keeps NLS 默认 (800ms).
	// Applied independently of OverrideDefaults.
	MaxSentenceSilenceMs int
}

// Provider implements asr.Provider by wrapping ASRClient.
type Provider struct {
	cfg Config

	tm     *auth.TokenManager
	client *ASRClient
	events chan asr.Event

	startOnce sync.Once
	startErr  error
	stopOnce  sync.Once
	stopped   atomic.Bool

	seq atomic.Int32
}

// New constructs a Provider. It validates required fields but does not yet
// open a session — Start does that.
func New(cfg Config) (*Provider, error) {
	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		return nil, errors.New("aliyun asr: AccessKeyID / AccessKeySecret required")
	}
	if cfg.AppKey == "" {
		return nil, errors.New("aliyun asr: AppKey required")
	}
	if cfg.Region == "" {
		cfg.Region = "cn-shanghai"
	}
	tm, err := auth.NewTokenManager(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.Region)
	if err != nil {
		return nil, err
	}
	return &Provider{
		cfg:    cfg,
		tm:     tm,
		events: make(chan asr.Event, 32),
	}, nil
}

// Start implements asr.Provider.
func (p *Provider) Start(ctx context.Context) error {
	p.startOnce.Do(func() {
		ac := DefaultASRConfig()
		if p.cfg.OverrideDefaults {
			if p.cfg.SampleRate > 0 {
				ac.SampleRate = p.cfg.SampleRate
			}
			ac.EnableIntermediateResult = p.cfg.EnableIntermediateResult
			ac.EnablePunctuation = p.cfg.EnablePunctuation
			ac.EnableITN = p.cfg.EnableITN
		}
		if p.cfg.MaxSentenceSilenceMs > 0 {
			ac.MaxSentenceSilenceMs = p.cfg.MaxSentenceSilenceMs
		}
		c, err := NewASRClient(ctx, p.tm, p.cfg.AppKey, p.cfg.Region, ac)
		if err != nil {
			p.startErr = err
			close(p.events)
			return
		}
		p.client = c
		go p.bridge()
	})
	return p.startErr
}

// bridge consumes legacy ASR events, translates them, and pushes to the
// new-shape channel. Exits when the legacy channel closes.
func (p *Provider) bridge() {
	defer close(p.events)
	for raw := range p.client.Events() {
		ev := translate(raw)
		ev.Sequence = int(p.seq.Add(1))
		if ev.At.IsZero() {
			ev.At = time.Now().UTC()
		}
		select {
		case p.events <- ev:
		default:
			// buffer full — drop (slow consumer)
		}
	}
}

// SendPCM implements asr.Provider. samples are little-endian int16 which we
// flatten to []byte for the legacy client.
func (p *Provider) SendPCM(samples []int16) error {
	if p.client == nil {
		return asr.ErrNotStarted
	}
	if p.stopped.Load() {
		return asr.ErrStopped
	}
	return p.client.SendPCM(int16ToLEBytes(samples))
}

// Events implements asr.Provider.
func (p *Provider) Events() <-chan asr.Event { return p.events }

// Stop implements asr.Provider.
func (p *Provider) Stop() error {
	var err error
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		if p.client != nil {
			// StopAndWait drains the recogniser politely; fall back to Close on
			// timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = p.client.StopAndWait(ctx)
			if err != nil {
				_ = p.client.Close()
			}
		} else {
			// Start was never called or it failed — ensure events channel is
			// closed for symmetry.
			select {
			case <-p.events:
			default:
				close(p.events)
			}
		}
	})
	return err
}

// translate maps a legacy ASREvent to the new asr.Event shape.
func translate(raw ASREvent) asr.Event {
	switch raw.Type {
	case "sentence_begin":
		return asr.Event{Type: asr.EventVADStart, At: time.Now().UTC()}
	case "result_changed":
		return asr.Event{Type: asr.EventPartial, Text: raw.Text, At: time.Now().UTC()}
	case "sentence_end":
		return asr.Event{Type: asr.EventFinal, Text: raw.Text, At: time.Now().UTC()}
	case "completed":
		return asr.Event{Type: asr.EventVADEnd, At: time.Now().UTC()}
	case "started":
		// Map "session started" to nothing semantic; clients receive nothing
		// useful from this. Drop by returning Sequence=0 / Type=EventVADStart
		// and let the bridge filter? Cleanest: just emit nothing. We'll
		// emit it as an unknown VADStart so consumers don't lose count.
		return asr.Event{Type: asr.EventVADStart, At: time.Now().UTC()}
	case "task_failed":
		return asr.Event{
			Type: asr.EventError,
			Err:  errors.New("aliyun asr: " + raw.Text),
			At:   time.Now().UTC(),
		}
	}
	// Unknown event: surface as an error so the user knows something happened
	return asr.Event{
		Type: asr.EventError,
		Err:  errors.New("aliyun asr: unknown event type: " + raw.Type),
		At:   time.Now().UTC(),
	}
}

// int16ToLEBytes encodes signed 16-bit samples as little-endian bytes.
func int16ToLEBytes(samples []int16) []byte {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	return buf
}
