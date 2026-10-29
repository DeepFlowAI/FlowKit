package recording

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

const uploaderQueueDepth = 8

// localChDepth buffers outgoing (agent) frames between the non-blocking tap and
// pumpLocal. The remote leg rides a bus subscriber; the local leg only has this
// channel, so it needs enough headroom to ride out brief pump stalls (e.g. an
// OSS upload flush) without the tap dropping agent audio. ~5s at 20ms frames.
const localChDepth = 256

// Recorder captures one Call as WAV (stereo or mono_mix per Config.Layout) and uploads via StorageSink.
type Recorder struct {
	cfg     Config
	factory SinkFactory
	call    *session.Call
	log     *slog.Logger

	lifecycleMu sync.Mutex
	sinkOpenMu  sync.Mutex
	beginOnce   sync.Once
	stopOnce    sync.Once
	beginErr    atomic.Value // error
	sinkOpenErr error
	sinkTried   bool
	lazyOpen    bool

	beginAt      time.Time
	clockStart   time.Time
	beginMediaAt time.Duration
	beginLocalAt time.Duration
	remoteBase   time.Duration
	remoteClock  localTimeline
	localClock   localTimeline

	encoder        *StereoEncoder
	sinkPtr        atomic.Pointer[StorageSink]
	meta           ObjectMeta
	pcmBytes       atomic.Int64
	partial        atomic.Bool
	dropped        atomic.Uint64
	stopped        atomic.Bool
	suppressNotify atomic.Bool // bridge handoff: close file but do not push completed event

	uploadCh chan []byte
	done     chan struct{}
	uploadWG sync.WaitGroup
	pumpsWG  sync.WaitGroup

	remoteSub *bus.Subscriber
	localCh   chan localFrame
	hooks     *Hooks
	url       atomic.Value // string
}

type localFrame struct {
	ts      time.Duration
	samples []int16
}

// NewRecorder constructs a recorder for call (not started until Begin).
func NewRecorder(cfg Config, factory SinkFactory, call *session.Call, log *slog.Logger) *Recorder {
	if log == nil {
		log = slog.Default()
	}
	return &Recorder{
		cfg:      cfg,
		factory:  factory,
		call:     call,
		log:      log.With("call_id", call.ID(), "conversation_id", call.ConversationID()),
		encoder:  NewStereoEncoder(),
		uploadCh: make(chan []byte, uploaderQueueDepth),
		done:     make(chan struct{}),
		localCh:  make(chan localFrame, localChDepth),
	}
}

func (r *Recorder) getSink() StorageSink {
	p := r.sinkPtr.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (r *Recorder) setSink(s StorageSink) {
	if s == nil {
		r.sinkPtr.Store(nil)
		return
	}
	r.sinkPtr.Store(&s)
}

// Attach blocks until the call ends (waits for start state, records, then stops).
func (r *Recorder) Attach(ctx context.Context) {
	r.waitAndRun(ctx)
}

func (r *Recorder) waitAndRun(ctx context.Context) {
	defer r.Stop()

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for !r.call.IsEnded() {
		if r.shouldStart() {
			_ = r.Begin(ctx)
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-r.call.Done():
			return
		case <-tick.C:
		}
	}

	done := r.call.Done()
	if r.cfg.MaxDuration > 0 {
		timer := time.NewTimer(r.cfg.MaxDuration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-done:
		case <-timer.C:
			r.partial.Store(true)
			r.log.Info("recording: max_duration reached", "limit", r.cfg.MaxDuration)
		}
		return
	}
	select {
	case <-ctx.Done():
	case <-done:
	}
}

func (r *Recorder) shouldStart() bool {
	st := r.call.State()
	if r.cfg.CaptureEarlyMedia && r.call.Direction() == session.DirectionOutbound && st == session.StateRinging {
		return true
	}
	switch r.cfg.StartOn {
	case "ringing":
		return st == session.StateRinging || st == session.StateAnswered ||
			st == session.StateActive || st == session.StateBridged
	default:
		return st == session.StateAnswered || st == session.StateActive || st == session.StateBridged
	}
}

// Begin subscribes to media and starts bus/tap pumps. Normal recordings open
// storage immediately. Outbound early-media capture opens it lazily on the
// first real frame so signaling-only failures never create empty artifacts.
func (r *Recorder) Begin(ctx context.Context) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	var retErr error
	r.beginOnce.Do(func() {
		if r.stopped.Load() {
			retErr = errRecordingStopped
			r.beginErr.Store(retErr)
			return
		}
		r.meta = objectMeta(r.call.ID(), r.call.ConversationID(), r.call.CreatedAt())

		// Subscribe before opening storage. Opening a sink can take tens to
		// hundreds of milliseconds; if the call ends in that window the bus
		// is already closed and Subscribe returns nil. Creating the object
		// first would leave an empty recording and a false completed event.
		r.remoteSub = r.call.AudioBus().Subscribe()
		if r.remoteSub == nil {
			retErr = errRecordingNoBus
			r.beginErr.Store(retErr)
			r.log.Info("recording: call ended before recording could start",
				"state", r.call.State(),
			)
			return
		}

		// Anchor the timeline at capture start, not after sink.Open returns,
		// so frames buffered during Open stay aligned to zero.
		r.clockStart = time.Now()
		r.remoteBase = r.call.MediaTime()
		r.lazyOpen = r.cfg.CaptureEarlyMedia &&
			r.call.Direction() == session.DirectionOutbound &&
			!r.call.EverAnswered()
		if !r.lazyOpen {
			if err := r.openSink(ctx, r.remoteBase, 0); err != nil {
				retErr = err
				r.beginErr.Store(err)
				r.call.AudioBus().Unsubscribe(r.remoteSub)
				r.remoteSub = nil
				return
			}
		}

		r.pumpsWG.Add(2)
		remoteSub := r.remoteSub
		go func() {
			defer r.pumpsWG.Done()
			r.pumpRemote(remoteSub)
		}()
		go func() {
			defer r.pumpsWG.Done()
			r.pumpLocal()
		}()

		r.call.AddRenderSink("recording", r.tapOutgoing)
	})
	if retErr != nil {
		return retErr
	}
	if v := r.beginErr.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
}

func (r *Recorder) openSink(ctx context.Context, mediaAt, localAt time.Duration) error {
	r.sinkOpenMu.Lock()
	defer r.sinkOpenMu.Unlock()
	if r.getSink() != nil {
		return nil
	}
	if r.sinkTried {
		return r.sinkOpenErr
	}
	r.sinkTried = true
	if r.stopped.Load() {
		r.sinkOpenErr = errRecordingStopped
		return r.sinkOpenErr
	}

	r.beginAt = time.Now().UTC()
	r.beginMediaAt = mediaAt
	r.beginLocalAt = localAt
	sink, err := r.factory.NewSink(ctx, r.meta)
	if err == nil {
		err = sink.Open(ctx, r.meta, BuildWAVHeaderPlaceholderForLayout(r.cfg.Layout))
	}
	if err != nil {
		r.sinkOpenErr = err
		r.log.Error("recording: sink open failed", "err", err)
		return err
	}
	r.setSink(sink)

	r.uploadWG.Add(1)
	go func() {
		defer r.uploadWG.Done()
		r.runUploader()
	}()

	r.log.Info("recording started",
		"backend", r.cfg.Storage.Backend,
		"layout", r.cfg.Layout,
		"object", RenderKey(r.cfg.Storage.AliyunOSS.KeyTemplate, r.meta),
		"begin_unix_ms", r.beginAt.UnixMilli(),
		"media_ms", mediaAt.Milliseconds(),
	)
	if r.hooks != nil && r.hooks.OnStarted != nil {
		r.hooks.OnStarted(RecordingStarted{
			StorageKey: RenderKey(r.cfg.Storage.AliyunOSS.KeyTemplate, r.meta),
			StartedAt:  r.beginAt,
		})
	}
	return nil
}

// Status returns RPC-friendly recording state.
func (r *Recorder) Status() map[string]any {
	url, _ := r.url.Load().(string)
	return map[string]any{
		"active":  r.getSink() != nil && !r.stopped.Load(),
		"url":     url,
		"partial": r.partial.Load(),
		"mode":    "call",
	}
}

func (r *Recorder) tapOutgoing(samples []int16) {
	if r.stopped.Load() {
		return
	}
	ts, ok := r.localClock.timestamp(time.Since(r.clockStart), len(samples))
	if !ok {
		r.dropped.Add(1)
		r.partial.Store(true)
		return
	}
	cp := append([]int16(nil), samples...)
	select {
	case r.localCh <- localFrame{ts: ts, samples: cp}:
	case <-r.done:
	default:
		r.dropped.Add(1)
		r.partial.Store(true)
	}
}

func (r *Recorder) pumpRemote(sub *bus.Subscriber) {
	if sub == nil {
		return
	}
	defer r.call.AudioBus().Unsubscribe(sub)

	for {
		select {
		case <-r.done:
			return
		case frame, ok := <-sub.Channel():
			if !ok {
				return
			}
			if r.stopped.Load() {
				continue
			}
			wall := time.Since(r.clockStart)
			remoteTS, ok := r.remoteClock.timestamp(wall, len(frame.Samples))
			if !ok {
				r.dropped.Add(1)
				r.partial.Store(true)
				continue
			}
			remoteAt := r.remoteBase + remoteTS
			if r.getSink() == nil {
				if err := r.openSink(context.Background(), remoteAt, wall); err != nil {
					continue
				}
			}
			if sub.Dropped() > 0 {
				r.partial.Store(true)
			}
			ts := remoteAt - r.beginMediaAt
			if ts < 0 {
				ts = 0
			}
			cp := append([]int16(nil), frame.Samples...)
			applyGainClamp(cp, r.cfg.RemoteGain)
			pcm := r.encoder.WriteRemote(ts, cp)
			if r.encoder.HadIssues() {
				r.partial.Store(true)
			}
			r.enqueuePCM(pcm)
		}
	}
}

func (r *Recorder) pumpLocal() {
	for {
		select {
		case <-r.done:
			return
		case lf, ok := <-r.localCh:
			if !ok {
				return
			}
			if r.stopped.Load() {
				continue
			}
			if r.getSink() == nil {
				if r.lazyOpen && !r.call.EverAnswered() {
					continue
				}
				if err := r.openSink(context.Background(), r.remoteBase+lf.ts, lf.ts); err != nil {
					continue
				}
			}
			ts := lf.ts - r.beginLocalAt
			if ts < 0 {
				ts = 0
			}
			pcm := r.encoder.WriteLocal(ts, lf.samples)
			if r.encoder.HadIssues() {
				r.partial.Store(true)
			}
			r.enqueuePCM(pcm)
		}
	}
}

// applyGainClamp scales samples in place by gain, clamping to int16 range.
// A gain of 1.0 (or non-positive) is a no-op.
func applyGainClamp(samples []int16, gain float64) {
	if gain == 1.0 || gain <= 0 {
		return
	}
	for i, v := range samples {
		x := float64(v) * gain
		if x > 32767 {
			x = 32767
		} else if x < -32768 {
			x = -32768
		}
		samples[i] = int16(x)
	}
}

func (r *Recorder) enqueuePCM(pcm []byte) {
	pcm = encodePCMForLayout(r.cfg.Layout, pcm)
	if len(pcm) == 0 {
		return
	}
	r.pcmBytes.Add(int64(len(pcm)))
	select {
	case r.uploadCh <- pcm:
	case <-r.done:
	default:
		if r.cfg.DropOnOverrun {
			r.dropped.Add(1)
			r.partial.Store(true)
		} else {
			select {
			case r.uploadCh <- pcm:
			case <-r.done:
			}
		}
	}
}

func (r *Recorder) runUploader() {
	sink := r.getSink()
	appendFn := func(ctx context.Context, b []byte) error {
		if sink == nil {
			return nil
		}
		return sink.Append(ctx, b)
	}
	runUploader(r.done, r.cfg.FlushInterval, r.uploadCh, appendFn, r.log)
}

func (r *Recorder) closeSinkUnlocked(ctx context.Context) {
	sink := r.getSink()
	if sink == nil {
		return
	}
	tail := encodePCMForLayout(r.cfg.Layout, r.encoder.Flush())
	if len(tail) > 0 {
		r.pcmBytes.Add(int64(len(tail)))
		_ = sink.Append(ctx, tail)
	}
	dataSize := uint32(r.pcmBytes.Load())
	patch := PatchWAVHeaderForLayout(r.cfg.Layout, dataSize)
	url, err := sink.Close(ctx, patch, dataSize)
	if err != nil {
		r.log.Warn("recording: close failed", "err", err)
	}
	r.url.Store(url)
	headerPatched := dataSize > 0
	partial := r.partial.Load() || r.encoder.HadIssues()
	if dataSize == 0 {
		r.log.Warn("recording completed with no audio data",
			"call_id", r.call.ID(),
			"state", r.call.State(),
		)
	} else {
		lateRemote, lateLocal := r.encoder.LateFrames()
		remoteResyncs, remoteMaxJump := r.remoteClock.resyncStats()
		localResyncs, localMaxJump := r.localClock.resyncStats()
		r.log.Info("recording completed",
			"url", url,
			"bytes", dataSize,
			"partial", partial,
			"header_patched", headerPatched,
			"dropped", r.dropped.Load(),
			"late_remote_frames", lateRemote,
			"late_local_frames", lateLocal,
			"remote_resyncs", remoteResyncs,
			"remote_max_resync_ms", remoteMaxJump.Milliseconds(),
			"local_resyncs", localResyncs,
			"local_max_resync_ms", localMaxJump.Milliseconds(),
		)
	}
	if dataSize > 0 && !r.suppressNotify.Load() && r.hooks != nil && r.hooks.OnCompleted != nil {
		endedAt := time.Now().UTC()
		r.hooks.OnCompleted(RecordingCompleted{
			URL:           url,
			StorageKey:    RenderKey(r.cfg.Storage.AliyunOSS.KeyTemplate, r.meta),
			StartedAt:     r.beginAt,
			EndedAt:       endedAt,
			DurationMS:    durationMSForLayout(r.cfg.Layout, dataSize),
			Partial:       partial,
			HeaderPatched: headerPatched,
		})
	}
	r.setSink(nil)
}

// StopForBridgeHandoff stops the per-leg recorder when switching to bridge mode.
// The WAV is still finalized on OSS, but no call.recording.completed is sent
// (the bridge recorder emits one event for all leg IDs).
func (r *Recorder) StopForBridgeHandoff() {
	r.suppressNotify.Store(true)
	r.Stop()
}

// Stop finalizes the recording (idempotent).
func (r *Recorder) Stop() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	r.stopOnce.Do(func() {
		r.stopped.Store(true)
		r.call.RemoveRenderSink("recording")

		if r.remoteSub != nil {
			r.call.AudioBus().Unsubscribe(r.remoteSub)
			r.remoteSub = nil
		}

		close(r.done)
		r.pumpsWG.Wait()
		r.uploadWG.Wait()

		r.closeSinkUnlocked(context.Background())
	})
}
