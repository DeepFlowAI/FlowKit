package calls

import (
	"log/slog"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/aec"
	"github.com/DeepFlowAI/flowkit/internal/media/denoise"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport"
)

// EnableAEC installs a per-call hook that attaches a WebRTC AEC3 echo canceller
// to SIP legs. The canceller removes played TTS echo from the mic uplink before
// VAD/ASR/recording see it, using the paced SetSentTap render reference.
//
// WebRTC and WS-PCM legs are skipped on purpose: the browser (or the VoiceSpeed
// bridge feeding WS-PCM) already applies its own AEC, so a second server-side
// pass would double-cancel and degrade uplink quality. That decision is keyed
// off transport.Kind — not off optional-capability probes.
//
// No-op if the binary was built without the `aec` tag (aec.Available()==false).
func EnableAEC(mgr *session.Manager, opt aec.Options, log *slog.Logger) {
	if mgr == nil {
		return
	}
	if !aec.Available() {
		log.Warn("AEC_ENABLED set but binary built without -tags aec; echo cancellation disabled")
		return
	}
	mgr.AddCreateHook(func(c *session.Call) {
		go attachAEC(c, opt, log)
	})
}

// attachAEC waits for media to bind, skips legs that already cancel echo
// peer-side, then wires the canceller as the call's capture processor + render
// sink for the call's lifetime.
func attachAEC(c *session.Call, opt aec.Options, log *slog.Logger) {
	mio := waitMediaBound(c)
	if mio == nil {
		return
	}
	switch mio.Kind() {
	case transport.KindWebRTC, transport.KindWSPCM:
		// Browser / WS-PCM peer already applies AEC. A second server pass would
		// be double-cancellation and hurt uplink quality — not a missing-capability
		// fallback.
		return
	case transport.KindFake:
		// Test doubles have no acoustic path; do not attach.
		return
	}

	can := aec.New(opt)
	if can == nil {
		log.Warn("aec: canceller init failed", "call_id", c.ID())
		return
	}
	pipe := &aecPipeline{
		canceller:  can,
		diagnostic: newAECDiagnostic(os.Getenv("AEC_DIAGNOSTIC_DIR"), c.ID(), log),
		nearEnd:    &nearEndGuard{},
		// Echo cancellation must see the linear echo path, so the neural enhancer
		// runs after it, never before.
		denoiser: denoise.New(),
		log:      log,
		callID:   c.ID(),
	}
	c.SetCaptureProcessor(pipe)
	c.AddRenderSink("aec", pipe.ProcessRender)
	log.Info("aec attached", "call_id", c.ID(), "transport", mio.Kind(),
		"ns", opt.NSEnabled, "ns_level", opt.NSLevel, "agc", opt.AGCEnabled,
		"stream_delay_ms", opt.StreamDelayMS, "denoise", pipe.denoiser != nil)

	<-c.Done()
	c.RemoveRenderSink("aec")
	c.SetCaptureProcessor(nil)
	pipe.Close()
	can.Close()
}

// aecPipeline adapts the canceller to capture/render call hooks and optionally
// records raw-vs-processed diagnostics without changing the media path. When the
// neural enhancer is available it is the second stage, so what reaches the
// AudioBus (ASR, VAD, recording) is echo-cancelled and denoised.
type aecPipeline struct {
	canceller  *aec.Canceller
	diagnostic *aecDiagnostic
	denoiser   *denoise.Denoiser
	nearEnd    *nearEndGuard

	log    *slog.Logger
	callID string
	// Uplink frames that carried speech-level energy but left the pipeline below
	// the gate, counted only while nothing was playing. Suppressing a frame under
	// the bot's own voice is the echo canceller doing its job; suppressing one in
	// silence means the caller's speech never reached ASR, which otherwise looks
	// exactly like the caller saying nothing at all.
	// The two stages are attributed separately: the same missing audio calls for
	// retuning the canceller in one case and the enhancer in the other.
	lastRenderNS atomic.Int64
	voicedFrames atomic.Int64
	eatenByAEC   atomic.Int64
	eatenByNR    atomic.Int64
	// Consecutive speech-level frames seen by the capture goroutine, which is
	// the only writer.
	voicedRun int
}

const (
	// Uplink RMS that counts as speech. Mirrors the ASR gate's default: below it
	// a frame would not be voiced for the meter either, so losing it costs
	// nothing.
	pipelineSpeechRMS = 800
	// How far below the gate a processed frame has to land to count as eaten.
	pipelineEatenRMS = pipelineSpeechRMS / 4
	// Echo persists in the uplink for a while after the last rendered frame, so
	// only frames this long after playback stopped are attributed to the caller.
	pipelineEchoTail = 300 * time.Millisecond
	// Speech-level frames in a row before the enhancer's output is judged. It
	// returns audio a hop (16ms) behind its input, so at an onset the input is
	// already loud while the output is still the silence from before — charging
	// that to the enhancer overstates it by roughly half. Three 10ms frames
	// clear the delay; a genuine swallowed utterance lasts far longer.
	pipelineNRSettleFrames = 3
)

func (p *aecPipeline) Process(frame []int16) []int16 {
	raw := append([]int16(nil), frame...)
	cancelled := p.canceller.ProcessCapture(frame)
	processed := p.nearEnd.preserveNearEnd(raw, cancelled, time.Now())
	if p.denoiser != nil {
		// The enhancer buffers, so keep the canceller's output for the tally —
		// it is the only place the two stages can still be told apart.
		cancelled = append([]int16(nil), cancelled...)
		processed = p.denoiser.Process(processed)
	}
	p.diagnostic.WriteCapture(raw, processed)
	p.accountSuppression(raw, cancelled, processed)
	return processed
}

// accountSuppression tallies caller speech the pipeline removed while the line
// was otherwise silent, charging it to whichever stage dropped it.
func (p *aecPipeline) accountSuppression(raw, cancelled, processed []int16) {
	last := p.lastRenderNS.Load()
	if frameRMS(raw) < pipelineSpeechRMS ||
		(last != 0 && time.Since(time.Unix(0, last)) < pipelineEchoTail) {
		p.voicedRun = 0
		return
	}
	p.voicedRun++
	p.voicedFrames.Add(1)
	switch {
	case frameRMS(cancelled) < pipelineEatenRMS:
		// Cancellation is in step with its input, so one frame is enough.
		p.eatenByAEC.Add(1)
	case frameRMS(processed) < pipelineEatenRMS && p.voicedRun >= pipelineNRSettleFrames:
		p.eatenByNR.Add(1)
	}
}

func (p *aecPipeline) ProcessRender(frame []int16) {
	p.nearEnd.observeRender(frame, time.Now())
	p.diagnostic.WriteRender(frame)
	if frameRMS(frame) >= pipelineSpeechRMS {
		p.lastRenderNS.Store(time.Now().UnixNano())
	}
	p.canceller.ProcessRender(frame)
}

func (p *aecPipeline) Close() {
	p.diagnostic.Close()
	p.denoiser.Close()
	byAEC, byNR := p.eatenByAEC.Load(), p.eatenByNR.Load()
	if byAEC+byNR > 0 {
		p.log.Info("aec pipeline suppressed uplink speech", "call_id", p.callID,
			"voiced_frames", p.voicedFrames.Load(),
			"eaten_ms", (byAEC+byNR)*20,
			"aec_ms", byAEC*20, "denoise_ms", byNR*20)
	}
}

// frameRMS is the root-mean-square level of one PCM frame.
func frameRMS(frame []int16) float64 {
	if len(frame) == 0 {
		return 0
	}
	var sum float64
	for _, s := range frame {
		sum += float64(s) * float64(s)
	}
	return math.Sqrt(sum / float64(len(frame)))
}

// waitMediaBound polls until the call's MediaIO is bound (it binds around answer,
// which can be seconds after create on a long ring) or the call ends.
func waitMediaBound(c *session.Call) transport.MediaIO {
	for {
		if mio := c.MediaIO(); mio != nil {
			return mio
		}
		select {
		case <-c.Done():
			return nil
		case <-time.After(20 * time.Millisecond):
		}
	}
}
