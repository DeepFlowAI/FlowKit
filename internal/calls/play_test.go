package calls

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

// totalOutgoing sums all samples the fakeio has collected so far.
func totalOutgoing(io *fakeio.FakeIO) int {
	total := 0
	for _, f := range io.Outgoing() {
		total += len(f)
	}
	return total
}

// sinePCM builds a 440 Hz sine at 16 kHz whose per-frame RMS is
// amplitude/sqrt(2).
func sinePCM(amplitude, samples int) []int16 {
	out := make([]int16, samples)
	for i := range out {
		out[i] = int16(float64(amplitude) * math.Sin(2*math.Pi*440*float64(i)/bus.SampleRate))
	}
	return out
}

// pcmRMS returns the root-mean-square level of the samples.
func pcmRMS(samples []int16) float64 {
	var sum float64
	for _, sample := range samples {
		v := float64(sample)
		sum += v * v
	}
	return math.Sqrt(sum / float64(len(samples)))
}

// buildMonoWAV constructs a minimal 16 kHz mono PCM16 WAV of the given samples.
func buildMonoWAV(t *testing.T, samples []int16) []byte {
	t.Helper()
	var buf bytes.Buffer
	dataBytes := len(samples) * 2
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+dataBytes))
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(bus.SampleRate))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(bus.SampleRate*2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(2))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16))
	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dataBytes))
	for _, s := range samples {
		_ = binary.Write(&buf, binary.LittleEndian, s)
	}
	return buf.Bytes()
}

func TestHandlePlay_SingleTTS_WritesPCM(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(256)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	raw, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "你好",
	})
	require.Nil(t, rpcErr)
	var res playResult
	require.NoError(t, json.Unmarshal(raw, &res))
	require.NotEmpty(t, res.PlayID)
	require.Equal(t, "playing", res.Status)

	// 2 chars × 800 samples/char plus the shared transport tail, played once.
	wantSamples := 1600 + _sayTailPadMs*bus.SampleRate/1000
	require.Eventually(t, func() bool { return totalOutgoing(io) == wantSamples }, 2*time.Second, 10*time.Millisecond)

	// Playback goroutine exits (single, non-looping).
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_WAVSource_DecodesAndPlays(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(256)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	// A healthy prompt (RMS ~3500, inside the leveling pass-through band) must
	// play byte-for-byte unchanged.
	pcm := sinePCM(5000, 3200)
	wav := buildMonoWAV(t, pcm)
	playEnd := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(call.ID(), func(method string, params map[string]any) {
		if method == "call.play_end" {
			copied := make(map[string]any, len(params))
			for key, value := range params {
				copied[key] = value
			}
			playEnd <- copied
		}
	})
	defer unsub()

	raw, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(),
		"audio":   base64.StdEncoding.EncodeToString(wav),
	})
	require.Nil(t, rpcErr)
	var res playResult
	require.NoError(t, json.Unmarshal(raw, &res))

	wantTail := _sayTailPadMs * bus.SampleRate / 1000
	require.Eventually(t, func() bool { return totalOutgoing(io) == len(pcm)+wantTail }, 2*time.Second, 10*time.Millisecond)
	var played []int16
	for _, frame := range io.Outgoing() {
		played = append(played, frame...)
	}
	require.Equal(t, pcm, played[:len(pcm)], "uploaded prompts must preserve their original waveform")
	require.Equal(t, wantTail, countTrailingZeros(played))
	select {
	case end := <-playEnd:
		require.Equal(t, "completed", end["reason"])
		require.Equal(t, int64(len(pcm)*1000/bus.SampleRate), end["played_ms"],
			"transport drain padding must not inflate the source duration")
		require.NotContains(t, end, "ducked_frames",
			"transports without PlaybackStats must not carry pacer stat fields")
	case <-time.After(time.Second):
		t.Fatal("call.play_end timeout")
	}
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_QuietWAVSource_IsLeveledUp(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(256)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	// RMS ~800, like the too-quiet uploaded closing prompt that the carrier's
	// AMR transcoding VAD chops up; decodePlayAudio must level it up.
	pcm := sinePCM(1131, 3200)
	wav := buildMonoWAV(t, pcm)

	raw, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(),
		"audio":   base64.StdEncoding.EncodeToString(wav),
	})
	require.Nil(t, rpcErr)
	var res playResult
	require.NoError(t, json.Unmarshal(raw, &res))

	wantTail := _sayTailPadMs * bus.SampleRate / 1000
	require.Eventually(t, func() bool { return totalOutgoing(io) == len(pcm)+wantTail }, 2*time.Second, 10*time.Millisecond)
	var played []int16
	for _, frame := range io.Outgoing() {
		played = append(played, frame...)
	}
	inRMS := pcmRMS(pcm)
	outRMS := pcmRMS(played[:len(pcm)])
	require.Greater(t, outRMS, inRMS*2, "quiet prompt must be leveled up: in=%f out=%f", inRMS, outRMS)
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_Loop_ReplaysThenStops(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(4096)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	// 1-char text = 800 samples per replay; small interval → several replays.
	raw, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "好", "loop": true, "interval_ms": 20,
	})
	require.Nil(t, rpcErr)
	var res playResult
	require.NoError(t, json.Unmarshal(raw, &res))

	// Wait until the prompt has been replayed at least 3 times (>= 2400 samples).
	require.Eventually(t, func() bool { return totalOutgoing(io) >= 3*800 }, 3*time.Second, 10*time.Millisecond,
		"loop should replay the prompt multiple times")

	// call.stop must halt the loop and exit the goroutine.
	_, rpcErr = driveRPC(t, reg, "call.stop", map[string]any{"call_id": call.ID(), "target": "play"})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond)

	// After stop, no further replays are written.
	settled := totalOutgoing(io)
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, settled, totalOutgoing(io), "no replays after stop")
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_Loop_ReplaysDoNotStack(t *testing.T) {
	// The loop is a single sequential goroutine that plays the cached PCM,
	// then resets the interval timer — so replays never overlap. Each replay
	// writes exactly the cached PCM (800 samples for a 1-char prompt), so the
	// total written is always a whole multiple of the replay size, never a
	// partial-overlap artifact.
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(4096)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "好", "loop": true, "interval_ms": 15,
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return totalOutgoing(io) >= 4*800 }, 3*time.Second, 10*time.Millisecond)

	_, rpcErr = driveRPC(t, reg, "call.stop", map[string]any{"call_id": call.ID(), "target": "play"})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond)

	require.Equal(t, 0, totalOutgoing(io)%800, "each replay writes exactly one whole prompt; no partial overlap")
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_Loop_ExitsOnCallEnd(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(4096)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	before := runtime.NumGoroutine()
	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "好", "loop": true, "interval_ms": 20,
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return totalOutgoing(io) >= 800 }, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, call.Hangup("remote_bye"))
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, time.Second, 10*time.Millisecond,
		"loop goroutine must exit when the leg ends")

	// Goroutine count returns to baseline (no leak).
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+2 }, 2*time.Second, 20*time.Millisecond)
}

func TestHandlePlay_NewPlaySupersedesLoop(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(4096)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "好", "loop": true, "interval_ms": 20,
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return totalOutgoing(io) >= 800 }, 2*time.Second, 10*time.Millisecond)

	// A second play supersedes the loop; only one playback source stays active.
	_, rpcErr = driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "你",
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_SayStopsLoop(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(4096)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "好", "loop": true, "interval_ms": 20,
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return totalOutgoing(io) >= 800 }, 2*time.Second, 10*time.Millisecond)

	// call.say shares the play slot, so it cancels the loop.
	_, rpcErr = driveRPC(t, reg, "call.say", map[string]string{"call_id": call.ID(), "text": "在"})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool { return !h.IsPlaying(call.ID()) }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_NoSource_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{"call_id": call.ID()})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestHandlePlay_MultipleSources_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "hi", "audio_url": "http://x/y.wav",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestHandlePlay_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{"call_id": "ghost", "text": "hi"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

// statsIO wraps FakeIO with a pacer.Stats source so play.end stat deltas can
// be asserted. The first PlaybackStats call (play-start snapshot) returns
// zero; later calls (finish snapshot) return the fixture.
type statsIO struct {
	*fakeio.FakeIO
	mu    sync.Mutex
	calls int
	stats pacer.Stats
}

func (s *statsIO) PlaybackStats() pacer.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return pacer.Stats{}
	}
	return s.stats
}

func TestHandlePlay_PlayEndIncludesPacerStatDeltas(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	io := &statsIO{
		FakeIO: fakeio.New(256),
		stats: pacer.Stats{
			DuckedFrames:    7,
			PauseEvents:     2,
			PausedMs:        120,
			SendGapOverruns: 3,
			MaxSendGapMs:    55,
		},
	}
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	playEnd := make(chan map[string]any, 1)
	unsub := h.SubscribeCall(call.ID(), func(method string, params map[string]any) {
		if method == "call.play_end" {
			copied := make(map[string]any, len(params))
			for key, value := range params {
				copied[key] = value
			}
			playEnd <- copied
		}
	})
	defer unsub()

	raw, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(), "text": "你好",
	})
	require.Nil(t, rpcErr)
	var res playResult
	require.NoError(t, json.Unmarshal(raw, &res))

	select {
	case end := <-playEnd:
		require.Equal(t, "completed", end["reason"])
		require.Equal(t, uint64(7), end["ducked_frames"])
		require.Equal(t, uint64(2), end["pause_events"])
		require.Equal(t, uint64(120), end["paused_ms"])
		require.Equal(t, uint64(3), end["send_gap_overruns"])
		require.Equal(t, uint64(55), end["max_send_gap_ms"],
			"cumulative max, reported as-is (not diffable)")
	case <-time.After(2 * time.Second):
		t.Fatal("call.play_end timeout")
	}
	require.NoError(t, call.Hangup("done"))
}

func TestHandlePlay_UnsupportedAudio_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	_, rpcErr := driveRPC(t, reg, "call.play", map[string]any{
		"call_id": call.ID(),
		"audio":   base64.StdEncoding.EncodeToString([]byte("ID3\x03\x00\x00\x00\x00\x00\x00")),
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}
