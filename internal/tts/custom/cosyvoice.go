// Package custom is a self-hosted TTS implementation of the tts.Provider
// interface, targeting a private CosyVoice FastAPI deployment.
//
// It speaks CosyVoice's official runtime/python/fastapi/server.py protocol:
// a form POST to /inference_sft (tts_text + spk_id) whose response body is a
// stream of raw little-endian int16 PCM at the model's native sample rate
// (CosyVoice2 = 24000 Hz, CosyVoice v1 = 22050 Hz). The body is resampled to
// 16 kHz (bus.SampleRate) and emitted as a single Final PCMChunk, mirroring
// the one-shot shape of the aliyun adapter.
package custom

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// targetSampleRate is the rate the internal media bus runs at
// (matches internal/media/bus.SampleRate). CosyVoice output is resampled to it.
const targetSampleRate = 16000

// streamEmitSrcSamples is how much native-rate PCM to buffer before emitting a
// playable chunk (~0.067s at 24 kHz). Kept small so the first audio frame goes
// out shortly after CosyVoice's first bytes arrive, instead of waiting to fill
// a larger buffer (cuts first-sentence latency). Generation outruns playback,
// so the small initial buffer does not underrun.
const streamEmitSrcSamples = 1600

// Config carries the CosyVoice-specific knobs the adapter needs.
type Config struct {
	// BaseURL is the CosyVoice FastAPI root, e.g. "http://127.0.0.1:50000".
	BaseURL string
	// DefaultVoice maps to CosyVoice spk_id (e.g. "中文女") when Options.Voice
	// is empty.
	DefaultVoice string
	// SourceSampleRate is the model's native output rate: 24000 for CosyVoice2,
	// 22050 for CosyVoice v1. Defaults to 24000.
	SourceSampleRate int
	// Endpoint overrides the inference path. Defaults to "/inference_sft".
	Endpoint string
	// HTTPClient is optional; a client with the timeouts below is used when nil.
	HTTPClient *http.Client
	// Timeout bounds the whole request (headers + body). The body read is
	// throttled by playback, so this must exceed the longest sentence's audio
	// duration. Defaults to 60s.
	Timeout time.Duration
	// ResponseHeaderTimeout bounds how long to wait for the server's response
	// headers after the request is sent — i.e. the model's generation phase for
	// a generate-then-send server. Catches a stalled server fast without cutting
	// the playback-throttled body read. Defaults to 15s.
	ResponseHeaderTimeout time.Duration
	// MaxAttempts is the total number of synthesis attempts (1 = no retry).
	// A transient connection error or 5xx is retried; caller cancellation
	// (barge-in / call end) and 4xx are not. Defaults to 2.
	MaxAttempts int
	// TrimLeadingSilence drops the near-silent lead-in CosyVoice/zero-shot output
	// often prepends (0.4–0.9s) before the first phoneme, so the AI's audible
	// onset is not delayed. Capped (see leadTrimMaxSrcSamp) so a genuinely
	// slow-starting clip is never over-trimmed.
	TrimLeadingSilence bool
	// PrebufferMs, when > 0, is how much audio to accumulate before the first
	// emit. Later emits use the normal small streaming chunk. A larger value
	// cushions a bursty backend
	// whose chunks arrive in spurts (e.g. V100 with ~350ms inter-chunk gaps) so
	// the paced playout does not underrun/stutter. 0 keeps the small default
	// (~66ms) for lowest first-audio latency. Trades first-audio latency for
	// smoothness.
	PrebufferMs int
	// LeadPadMs, when > 0, prepends this much silence before the first phoneme
	// (and fades the phoneme in over leadFadeMs). Telephony stops RTP between
	// sentences; a model with no leading silence (e.g. V100 CosyVoice) lets the
	// receiver's jitter buffer re-prime on the first phoneme and clip it. The
	// silent pad re-primes on silence; the fade-in avoids the 0→full click.
	LeadPadMs int
	// TrimSustainMs is the lookahead used to confirm real speech vs a leading
	// artifact (see speechStartIndex). A clean voice with a pure-silence lead
	// (e.g. "default") can set this small (~60ms) to emit sooner. 0 → 200ms.
	TrimSustainMs int
	// MaxVowelHoldMs drops later frames of a near-stationary voiced plateau
	// (CosyVoice2 zero-shot stretching one character for seconds). 0 → 700ms.
	// Later phonemes still play: a jump in energy or zero-crossing rate resets.
	MaxVowelHoldMs int
}

// leadFadeMs is the fade-in applied to the speech onset after a silence pad, so
// the silence→speech transition has no click. Native-rate.
const leadFadeMs = 12

// fadeInPlace ramps the first n samples of s linearly from 0 to full gain.
func fadeInPlace(s []int16, n int) {
	if n > len(s) {
		n = len(s)
	}
	for i := 0; i < n; i++ {
		s[i] = int16(float64(s[i]) * float64(i) / float64(n))
	}
}

// Leading-silence trim knobs (operate on native-rate samples, before resample).
const (
	leadTrimRMS        = 350.0 // window RMS above which a window counts as speech
	leadTrimWin        = 240   // 10ms @ 24kHz scan window
	// leadTrimMaxSrcSamp caps how much lead-in we buffer before giving up the
	// artifact skip. CosyVoice zero-shot's leading blip + gap before real speech
	// is variable and can run ~1s+, so this must be generous (~2.5s @ 24kHz) or a
	// long leading gap pushes past the cap and the "哦" blip is kept.
	leadTrimMaxSrcSamp = 60000
)

// windowRMS is the RMS of s[off:off+win].
func windowRMS(s []int16, off, win int) float64 {
	var sum float64
	for j := off; j < off+win; j++ {
		v := float64(s[j])
		sum += v * v
	}
	return math.Sqrt(sum / float64(win))
}

// leadingSilence returns the count of leading near-silent samples in s: the
// start of the first `win`-window whose RMS exceeds rms, less one window of
// soft-attack headroom. Returns -1 when no speech window is found in s yet.
func leadingSilence(s []int16, rms float64, win int) int {
	for i := 0; i+win <= len(s); i += win {
		if windowRMS(s, i, win) > rms {
			start := i - win
			if start < 0 {
				start = 0
			}
			return start
		}
	}
	return -1
}

// speechStartIndex returns where sustained (real) speech begins, skipping the
// short leading artifact CosyVoice zero-shot emits before the text — a brief
// "哦" of variable loudness and length (≈40–100ms), always followed by a gap.
// An onset counts as real speech only when ≥70% of the next sustainWin windows
// are voiced; the artifact's short burst fails that test regardless of how loud
// it is. One window of soft-attack headroom is kept before the onset. A larger
// sustainWin rejects longer artifacts but waits longer before emitting; a clean
// voice (e.g. default, pure-silence lead) can use a small sustainWin for speed.
// Returns -1 when undecided (caller accumulates more); with final=true it
// decides on whatever lookahead is available (short final clips).
func speechStartIndex(s []int16, rms float64, win, sustainWin int, final bool) int {
	for i := 0; i+win <= len(s); i += win {
		if windowRMS(s, i, win) <= rms {
			continue
		}
		voiced, avail := 0, 0
		for k := 0; k < sustainWin; k++ {
			o := i + k*win
			if o+win > len(s) {
				break
			}
			avail++
			if windowRMS(s, o, win) > rms {
				voiced++
			}
		}
		if avail < sustainWin && !final {
			return -1 // not enough lookahead yet — wait for more samples
		}
		if avail == 0 {
			continue
		}
		if voiced*10 >= avail*7 {
			start := i - win // keep one window of soft attack
			if start < 0 {
				start = 0
			}
			return start
		}
		// not sustained → leading artifact, keep scanning
	}
	return -1
}

// Provider implements tts.Provider by calling a CosyVoice FastAPI endpoint.
type Provider struct {
	cfg    Config
	client *http.Client
	closed atomic.Bool
}

// New constructs a Provider. BaseURL is required.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("cosyvoice tts: BaseURL required (TTS_BASE_URL)")
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.SourceSampleRate == 0 {
		cfg.SourceSampleRate = 24000
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "/inference_sft"
	}
	if cfg.DefaultVoice == "" {
		cfg.DefaultVoice = "中文女"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	if cfg.ResponseHeaderTimeout <= 0 {
		cfg.ResponseHeaderTimeout = 15 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 2
	}
	if cfg.MaxVowelHoldMs == 0 {
		cfg.MaxVowelHoldMs = defaultMaxVowelHoldMs
	}
	if cfg.MaxVowelHoldMs < 0 {
		cfg.MaxVowelHoldMs = 0
	}
	client := cfg.HTTPClient
	if client == nil {
		client = NewHTTPClient(cfg.Timeout, cfg.ResponseHeaderTimeout)
	}
	return &Provider{cfg: cfg, client: client}, nil
}

// NewHTTPClient builds the HTTP client CosyVoice synthesis uses. Share ONE
// instance across all per-utterance providers (pass it via Config.HTTPClient)
// so its keep-alive connection pool is reused — each sentence otherwise pays a
// fresh TCP+TLS handshake (~85ms). The header timeout bounds a stalled
// generation phase without cutting the playback-throttled body read.
func NewHTTPClient(timeout, headerTimeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if headerTimeout <= 0 {
		headerTimeout = 15 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          32,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: headerTimeout,
			ExpectContinueTimeout: time.Second,
		},
	}
}

// Synthesize implements tts.Provider. Text cleanup happens upstream in
// textproc.Wrap, so text arrives here ready to send.
func (p *Provider) Synthesize(ctx context.Context, text string, opts tts.Options) (<-chan tts.PCMChunk, error) {
	if p.closed.Load() {
		return nil, tts.ErrClosed
	}
	if text == "" {
		return nil, tts.ErrEmptyText
	}

	spk := opts.Voice
	if spk == "" {
		spk = p.cfg.DefaultVoice
	}
	form := url.Values{}
	form.Set("tts_text", text)
	form.Set("spk_id", spk)

	start := time.Now()
	resp, err := p.do(ctx, form.Encode())
	if err != nil {
		return nil, err
	}

	// Output rate: callers (e.g. the standalone agent path) may request the
	// model's native rate to avoid the downsample; the call/media path leaves
	// SampleRate=0 and keeps the bus rate (targetSampleRate).
	outRate := targetSampleRate
	if opts.SampleRate > 0 {
		outRate = opts.SampleRate
	}

	// Per-synthesis lead-pad override (e.g. a long pad for the initial greeting
	// so its opening words survive the SIP answer→media cut-through gap).
	leadPadMs := p.cfg.LeadPadMs
	if opts.LeadPadMs > 0 {
		leadPadMs = opts.LeadPadMs
	}
	tailPadMs := opts.TailPadMs
	prebufferMs := p.cfg.PrebufferMs
	if opts.PrebufferMs > 0 {
		prebufferMs = opts.PrebufferMs
	}

	out := make(chan tts.PCMChunk, 8)
	go func() {
		defer close(out)
		defer func() { _ = resp.Body.Close() }()
		if err := p.streamBody(ctx, resp.Body, out, outRate, leadPadMs, tailPadMs, prebufferMs, start); err != nil {
			slog.Default().Warn("cosyvoice tts: read body", "err", err)
		}
	}()
	return out, nil
}

// do issues the synthesis POST, retrying transient failures (connection error,
// header-timeout stall, or 5xx) up to cfg.MaxAttempts. Caller cancellation
// (barge-in / call end) and 4xx responses are not retried. It returns a 200
// response whose body the caller owns (must read and Close).
func (p *Provider) do(ctx context.Context, body string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= p.cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			p.cfg.BaseURL+p.cfg.Endpoint, strings.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("cosyvoice tts: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := p.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err() // caller canceled — do not retry
			}
			lastErr = fmt.Errorf("cosyvoice tts: request: %w", err)
			slog.Default().Warn("cosyvoice tts: request failed",
				"attempt", attempt, "max", p.cfg.MaxAttempts, "err", err)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		_ = resp.Body.Close()
		lastErr = fmt.Errorf("cosyvoice tts: http %d", resp.StatusCode)
		if resp.StatusCode < http.StatusInternalServerError {
			return nil, lastErr // 4xx — retry won't help
		}
		slog.Default().Warn("cosyvoice tts: server error",
			"attempt", attempt, "max", p.cfg.MaxAttempts, "status", resp.StatusCode)
	}
	return nil, lastErr
}

// start is when the synthesis request was submitted; it is only used to report
// how the wait before the first playable chunk splits between the backend and
// this provider's own lead-trim + prebuffer.
func (p *Provider) streamBody(ctx context.Context, body io.Reader, out chan<- tts.PCMChunk, outRate int, leadPadMs int, tailPadMs int, prebufferMs int, start time.Time) error {
	readBuf := make([]byte, 4096)
	var byteCarry []byte
	var pending []int16
	// While trimming, samples accumulate in `lead` and nothing is emitted until
	// speech onset is located (or the cap is hit). trimDone short-circuits the
	// whole mechanism when disabled.
	var lead []int16
	trimDone := !p.cfg.TrimLeadingSilence
	// The larger prebuffer applies only to startup. Once playback can begin, emit
	// small chunks so audio already delivered by a bursty backend is not held
	// back until another full prebuffer arrives.
	emitN := streamEmitSrcSamples
	if prebufferMs > 0 {
		emitN = prebufferMs * p.cfg.SourceSampleRate / 1000
	}
	leadPadN := leadPadMs * p.cfg.SourceSampleRate / 1000
	fadeN := leadFadeMs * p.cfg.SourceSampleRate / 1000
	sustainMs := p.cfg.TrimSustainMs
	if sustainMs <= 0 {
		sustainMs = 200
	}
	sustainWin := sustainMs * p.cfg.SourceSampleRate / 1000 / leadTrimWin
	if sustainWin < 1 {
		sustainWin = 1
	}
	gate := newVowelHoldGate(p.cfg.SourceSampleRate, p.cfg.MaxVowelHoldMs)
	filter := func(s []int16) []int16 {
		if gate == nil {
			return s
		}
		return gate.Filter(s)
	}

	// startSpeech prepends the silence pad and fades the onset in (no click),
	// then appends the speech to pending.
	startSpeech := func(speech []int16) {
		if leadPadN > 0 {
			fadeInPlace(speech, fadeN)
			pending = append(pending, make([]int16, leadPadN)...)
		}
		pending = append(pending, speech...)
	}

	// admit routes freshly decoded samples either through the lead-in trimmer or
	// straight into the playable `pending` buffer once trimming is finished.
	admit := func(s []int16) {
		if trimDone {
			pending = append(pending, filter(s)...)
			return
		}
		lead = append(lead, s...)
		if idx := speechStartIndex(lead, leadTrimRMS, leadTrimWin, sustainWin, false); idx >= 0 {
			slog.Default().Info("cosyvoice tts: lead-trim",
				"trimmed_ms", idx*1000/p.cfg.SourceSampleRate,
				"fade_ms", leadFadeMs, "pad_ms", leadPadMs,
				"rms_threshold", leadTrimRMS)
			startSpeech(filter(lead[idx:]))
			lead = nil
			trimDone = true
		} else if len(lead) >= leadTrimMaxSrcSamp {
			// no sustained speech within cap — best-effort trim leading silence
			if drop := leadingSilence(lead, leadTrimRMS, leadTrimWin); drop >= 0 {
				startSpeech(filter(lead[drop:]))
			} else {
				pending = append(pending, filter(lead)...)
			}
			lead = nil
			trimDone = true
		}
	}

	var firstByteAt time.Time
	firstChunkLogged := false

	emit := func(samples []int16, final bool) error {
		if len(samples) == 0 && !final {
			return nil
		}
		resampled := resampleLinear(samples, p.cfg.SourceSampleRate, outRate)
		if len(resampled) == 0 && !final {
			return nil
		}
		if !firstChunkLogged {
			firstChunkLogged = true
			var firstByteMs int64
			if !firstByteAt.IsZero() {
				firstByteMs = firstByteAt.Sub(start).Milliseconds()
			}
			slog.Default().Info("cosyvoice tts: first-chunk",
				"first_byte_ms", firstByteMs,
				"first_chunk_ms", time.Since(start).Milliseconds(),
				"prebuffer_ms", prebufferMs)
		}
		select {
		case out <- tts.PCMChunk{Samples: resampled, Final: final}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		n, err := body.Read(readBuf)
		if n > 0 {
			if firstByteAt.IsZero() {
				firstByteAt = time.Now()
			}
			byteCarry = append(byteCarry, readBuf[:n]...)
			even := len(byteCarry) - len(byteCarry)%2
			if even > 0 {
				admit(bytesToInt16LE(byteCarry[:even]))
				byteCarry = byteCarry[even:]
			}
			for len(pending) >= emitN {
				if err := emit(pending[:emitN], false); err != nil {
					return err
				}
				pending = pending[emitN:]
				emitN = streamEmitSrcSamples
			}
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	if len(byteCarry) >= 2 {
		admit(bytesToInt16LE(byteCarry))
	}
	// Short clip whose onset never crossed the streaming threshold: trim the
	// held lead now (or keep it whole if it is silent end-to-end).
	if !trimDone && len(lead) > 0 {
		idx := speechStartIndex(lead, leadTrimRMS, leadTrimWin, sustainWin, true)
		if idx < 0 {
			idx = leadingSilence(lead, leadTrimRMS, leadTrimWin) // undecided → best-effort first onset
		}
		if idx >= 0 {
			startSpeech(filter(lead[idx:]))
		} else {
			pending = append(pending, filter(lead)...)
		}
	}
	if gate != nil {
		pending = append(pending, gate.Flush()...)
		if dropped := gate.DroppedMs(); dropped > 0 {
			slog.Default().Info("cosyvoice tts: vowel-hold truncated",
				"dropped_ms", dropped, "max_hold_ms", p.cfg.MaxVowelHoldMs)
		}
	}
	// Trailing silence: keep the far end's jitter buffer fed so the final
	// syllable plays out before the stream stops, instead of being clipped.
	if tailPadMs > 0 {
		pending = append(pending, make([]int16, tailPadMs*p.cfg.SourceSampleRate/1000)...)
	}
	return emit(pending, true)
}

// Close implements tts.Provider.
func (p *Provider) Close() error {
	p.closed.Store(true)
	return nil
}

// bytesToInt16LE decodes little-endian 16-bit samples. A trailing odd byte
// (incomplete sample) is dropped.
func bytesToInt16LE(b []byte) []int16 {
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

// resampleLinear converts in from srcRate to dstRate using linear
// interpolation. For voice this is adequate (see internal/media/codec).
func resampleLinear(in []int16, srcRate, dstRate int) []int16 {
	if len(in) == 0 || srcRate == dstRate || srcRate <= 0 || dstRate <= 0 {
		return in
	}
	outLen := int(int64(len(in)) * int64(dstRate) / int64(srcRate))
	if outLen <= 0 {
		return nil
	}
	out := make([]int16, outLen)
	ratio := float64(srcRate) / float64(dstRate)
	for i := range out {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)
		s0 := int32(in[idx])
		s1 := s0
		if idx+1 < len(in) {
			s1 = int32(in[idx+1])
		}
		out[i] = int16(s0 + int32(frac*float64(s1-s0)))
	}
	return out
}
