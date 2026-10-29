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

// BridgeRecorder captures bridged legs (encoder: L=user incoming, R=agent incoming;
// mono_mix writes the average to a single-channel WAV).
type BridgeRecorder struct {
	cfg     Config
	factory SinkFactory
	userLeg *session.Call
	// agentLeg is swapped on takeover, and is read outside lifecycleMu by the
	// manager when it looks a recorder up by leg id.
	agentLeg atomic.Pointer[session.Call]
	log      *slog.Logger

	lifecycleMu sync.Mutex
	beginOnce   sync.Once
	stopOnce    sync.Once
	beginAt     time.Time

	encoder  *StereoEncoder
	sinkPtr  atomic.Pointer[StorageSink]
	meta     ObjectMeta
	pcmBytes atomic.Int64
	partial  atomic.Bool
	dropped  atomic.Uint64
	stopped  atomic.Bool
	url      atomic.Value

	uploadCh chan []byte
	done     chan struct{}
	uploadWG sync.WaitGroup
	pumpsWG  sync.WaitGroup

	userSub  *bus.Subscriber
	agentSub *bus.Subscriber
	hooks    *Hooks
}

// NewBridgeRecorder builds a conversation-level bridge recording.
func NewBridgeRecorder(cfg Config, factory SinkFactory, userLeg, agentLeg *session.Call, log *slog.Logger) *BridgeRecorder {
	if log == nil {
		log = slog.Default()
	}
	root := userLeg.RootCallID()
	br := &BridgeRecorder{
		cfg:     cfg,
		factory: factory,
		userLeg: userLeg,
		log: log.With(
			"call_id", root+"_bridge",
			"conversation_id", userLeg.ConversationID(),
			"user_leg", userLeg.ID(),
			"agent_leg", agentLeg.ID(),
		),
		encoder:  NewStereoEncoder(),
		uploadCh: make(chan []byte, uploaderQueueDepth),
		done:     make(chan struct{}),
	}
	br.agentLeg.Store(agentLeg)
	return br
}

// AgentLeg returns the leg currently feeding the local channel. It changes when
// a supervisor takes the agent's place.
func (br *BridgeRecorder) AgentLeg() *session.Call {
	return br.agentLeg.Load()
}

func (br *BridgeRecorder) getSink() StorageSink {
	p := br.sinkPtr.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (br *BridgeRecorder) setSink(s StorageSink) {
	if s == nil {
		br.sinkPtr.Store(nil)
		return
	}
	br.sinkPtr.Store(&s)
}

// Run records until both legs end or max_duration elapses.
func (br *BridgeRecorder) Run(ctx context.Context) {
	defer br.Stop()
	br.Begin(ctx)
	done := br.waitBothDone(ctx)
	if br.cfg.MaxDuration > 0 {
		timer := time.NewTimer(br.cfg.MaxDuration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-done:
		case <-timer.C:
			br.partial.Store(true)
			br.log.Info("bridge recording: max_duration reached", "limit", br.cfg.MaxDuration)
		}
		return
	}
	<-done
}

func (br *BridgeRecorder) waitBothDone(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		userDone := br.userLeg.Done()
		agentDone := br.AgentLeg().Done()
		select {
		case <-userDone:
			select {
			case <-agentDone:
			case <-ctx.Done():
			}
		case <-agentDone:
			select {
			case <-userDone:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()
	return ch
}

// Begin opens storage and starts both bus pumps.
func (br *BridgeRecorder) Begin(ctx context.Context) {
	br.lifecycleMu.Lock()
	defer br.lifecycleMu.Unlock()

	br.beginOnce.Do(func() {
		if br.stopped.Load() {
			return
		}
		root := br.userLeg.RootCallID()
		br.meta = objectMeta(root+"_bridge", br.userLeg.ConversationID(), br.userLeg.CreatedAt())

		// Subscribe both legs before opening storage. Opening a sink can take
		// tens to hundreds of milliseconds; if either call ends in that window
		// its bus is already closed. Creating the object first would leave an
		// empty recording and a false completed event.
		//
		// The pumps below take their subscriber as an argument rather than
		// reading the field: the fields are rewritten under lifecycleMu by Stop
		// and by a takeover, which a running pump must not race with.
		agentLeg := br.AgentLeg()
		userSub := br.userLeg.AudioBus().Subscribe()
		agentSub := agentLeg.AudioBus().Subscribe()
		if userSub == nil || agentSub == nil {
			br.log.Info("bridge recording: call ended before recording could start",
				"user_state", br.userLeg.State(),
				"agent_state", agentLeg.State(),
			)
			if userSub != nil {
				br.userLeg.AudioBus().Unsubscribe(userSub)
			}
			if agentSub != nil {
				agentLeg.AudioBus().Unsubscribe(agentSub)
			}
			return
		}

		// Anchor the timeline at capture start, not after sink.Open returns.
		br.beginAt = time.Now().UTC()

		sink, err := br.factory.NewSink(ctx, br.meta)
		if err != nil {
			br.log.Error("bridge recording: sink failed", "err", err)
			br.userLeg.AudioBus().Unsubscribe(userSub)
			agentLeg.AudioBus().Unsubscribe(agentSub)
			return
		}
		if err := sink.Open(ctx, br.meta, BuildWAVHeaderPlaceholderForLayout(br.cfg.Layout)); err != nil {
			br.log.Error("bridge recording: open failed", "err", err)
			br.userLeg.AudioBus().Unsubscribe(userSub)
			agentLeg.AudioBus().Unsubscribe(agentSub)
			return
		}
		br.setSink(sink)
		br.userSub = userSub
		br.agentSub = agentSub

		br.uploadWG.Add(1)
		go func() {
			defer br.uploadWG.Done()
			br.runUploader()
		}()

		br.pumpsWG.Add(2)
		go func() {
			defer br.pumpsWG.Done()
			br.pumpLeg(br.userLeg, userSub, true)
		}()
		go func() {
			defer br.pumpsWG.Done()
			br.pumpLeg(agentLeg, agentSub, false)
		}()

		br.log.Info("bridge recording started",
			"layout", br.cfg.Layout,
			"object", RenderKey(br.cfg.Storage.AliyunOSS.KeyTemplate, br.meta),
			"begin_unix_ms", br.beginAt.UnixMilli(),
			"media_ms", br.userLeg.MediaTime().Milliseconds(),
		)
		if br.hooks != nil && br.hooks.OnStarted != nil {
			br.hooks.OnStarted(RecordingStarted{
				StorageKey: RenderKey(br.cfg.Storage.AliyunOSS.KeyTemplate, br.meta),
				StartedAt:  br.beginAt,
			})
		}
	})
}

// ReplaceAgentLeg re-points the local channel at newCall after a takeover.
//
// A takeover swaps the agent leg for a supervisor's without interrupting the
// customer, so the recording has to follow. Left alone, the old leg's bus
// closes and the local channel goes silent for the rest of the call — and
// because a recorder is looked up by its agent leg id, the old leg's hangup
// would stop the recording entirely while the call is still up.
//
// The encoder keeps its original time base, so the new leg's audio lands where
// it belongs on the timeline and the file stays aligned with the customer.
func (br *BridgeRecorder) ReplaceAgentLeg(newCall *session.Call) {
	if newCall == nil {
		return
	}
	br.lifecycleMu.Lock()
	defer br.lifecycleMu.Unlock()
	if br.stopped.Load() {
		return
	}

	old := br.agentLeg.Swap(newCall)
	// Closes the subscriber channel too, which is what ends the old pump.
	if old != nil && br.agentSub != nil {
		old.AudioBus().Unsubscribe(br.agentSub)
	}
	br.agentSub = nil
	if br.getSink() == nil {
		return // Begin never opened storage; nothing to pump into.
	}

	sub := newCall.AudioBus().Subscribe()
	if sub == nil {
		br.log.Error("bridge recording: subscribe to taking-over leg failed", "new_leg", newCall.ID())
		br.partial.Store(true)
		return
	}
	br.agentSub = sub
	br.pumpsWG.Add(1)
	go func() {
		defer br.pumpsWG.Done()
		br.pumpLeg(newCall, sub, false)
	}()
	br.log.Info("bridge recording: agent leg replaced", "new_leg", newCall.ID())
}

// Status returns RPC-friendly bridge recording state.
func (br *BridgeRecorder) Status() map[string]any {
	return map[string]any{
		"active":  br.getSink() != nil && !br.stopped.Load(),
		"url":     br.URL(),
		"partial": br.partial.Load(),
		"mode":    "bridge",
	}
}

func (br *BridgeRecorder) pumpLeg(call *session.Call, sub *bus.Subscriber, left bool) {
	if sub == nil {
		return
	}
	defer call.AudioBus().Unsubscribe(sub)

	for {
		select {
		case <-br.done:
			return
		case frame, ok := <-sub.Channel():
			if !ok {
				return
			}
			if br.stopped.Load() || br.getSink() == nil {
				continue
			}
			if sub.Dropped() > 0 {
				br.partial.Store(true)
			}
			ts := time.Since(br.beginAt)
			cp := append([]int16(nil), frame.Samples...)
			var pcm []byte
			if left {
				pcm = br.encoder.WriteRemote(ts, cp)
			} else {
				pcm = br.encoder.WriteLocal(ts, cp)
			}
			if br.encoder.HadIssues() {
				br.partial.Store(true)
			}
			br.enqueuePCM(pcm)
		}
	}
}

func (br *BridgeRecorder) enqueuePCM(pcm []byte) {
	pcm = encodePCMForLayout(br.cfg.Layout, pcm)
	if len(pcm) == 0 {
		return
	}
	br.pcmBytes.Add(int64(len(pcm)))
	select {
	case br.uploadCh <- pcm:
	case <-br.done:
	default:
		if br.cfg.DropOnOverrun {
			br.dropped.Add(1)
			br.partial.Store(true)
		} else {
			select {
			case br.uploadCh <- pcm:
			case <-br.done:
			}
		}
	}
}

func (br *BridgeRecorder) runUploader() {
	sink := br.getSink()
	appendFn := func(ctx context.Context, b []byte) error {
		if sink == nil {
			return nil
		}
		return sink.Append(ctx, b)
	}
	runUploader(br.done, br.cfg.FlushInterval, br.uploadCh, appendFn, br.log)
}

func (br *BridgeRecorder) closeSinkUnlocked(ctx context.Context) {
	sink := br.getSink()
	if sink == nil {
		return
	}
	tail := encodePCMForLayout(br.cfg.Layout, br.encoder.Flush())
	if len(tail) > 0 {
		br.pcmBytes.Add(int64(len(tail)))
		_ = sink.Append(ctx, tail)
	}
	dataSize := uint32(br.pcmBytes.Load())
	patch := PatchWAVHeaderForLayout(br.cfg.Layout, dataSize)
	url, err := sink.Close(ctx, patch, dataSize)
	if err != nil {
		br.log.Warn("bridge recording: close failed", "err", err)
	}
	br.url.Store(url)
	headerPatched := dataSize > 0
	partial := br.partial.Load() || br.encoder.HadIssues()
	if dataSize == 0 {
		br.log.Warn("bridge recording completed with no audio data")
	} else {
		lateUser, lateAgent := br.encoder.LateFrames()
		br.log.Info("bridge recording completed",
			"url", url,
			"bytes", dataSize,
			"partial", partial,
			"late_user_frames", lateUser,
			"late_agent_frames", lateAgent,
		)
	}
	if br.hooks != nil && br.hooks.OnCompleted != nil {
		endedAt := time.Now().UTC()
		br.hooks.OnCompleted(RecordingCompleted{
			URL:           url,
			StorageKey:    RenderKey(br.cfg.Storage.AliyunOSS.KeyTemplate, br.meta),
			StartedAt:     br.beginAt,
			EndedAt:       endedAt,
			DurationMS:    durationMSForLayout(br.cfg.Layout, dataSize),
			Partial:       partial,
			HeaderPatched: headerPatched,
		})
	}
	br.setSink(nil)
}

// Stop finalizes the bridge recording.
func (br *BridgeRecorder) Stop() {
	br.lifecycleMu.Lock()
	defer br.lifecycleMu.Unlock()

	br.stopOnce.Do(func() {
		br.stopped.Store(true)

		if br.userSub != nil {
			br.userLeg.AudioBus().Unsubscribe(br.userSub)
			br.userSub = nil
		}
		if br.agentSub != nil {
			br.AgentLeg().AudioBus().Unsubscribe(br.agentSub)
			br.agentSub = nil
		}

		close(br.done)
		br.pumpsWG.Wait()
		br.uploadWG.Wait()

		br.closeSinkUnlocked(context.Background())
	})
}

func (br *BridgeRecorder) URL() string {
	if v := br.url.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
