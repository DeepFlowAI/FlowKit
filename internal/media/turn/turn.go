// Package turn is a semantic end-of-turn detector wrapping the open-source
// Smart Turn v3 model (pipecat-ai/smart-turn-v3). Given the recent user audio it
// predicts whether the speaker has actually finished their turn ("complete") or
// merely paused mid-thought ("incomplete"), so endpointing can respond fast when
// the user is done and keep listening when they are not — instead of a fixed
// silence timeout.
//
// The model is a Whisper-tiny encoder + linear head (~8 MB int8 ONNX). It takes
// an 80×800 Whisper log-mel spectrogram of the last 8 s of 16 kHz audio and
// returns a single completion probability (sigmoid). The ONNX model is embedded;
// onnxruntime is loaded at runtime (dlopen) from the same shared lib as the VAD.
// A small pool of stateless sessions is shared across all calls; inference is
// run off the media hot path (once per candidate endpoint), ~30–100 ms on CPU.
package turn

import (
	_ "embed"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	ort "github.com/yalue/onnxruntime_go"
)

//go:embed smart-turn-v3.2-cpu.onnx
var modelData []byte

var (
	initMu sync.Mutex
	ready  atomic.Bool
	pool   chan *ort.DynamicAdvancedSession
)

// Init loads onnxruntime + the Smart Turn model into a pool of poolSize sessions.
// ortLib is the path to libonnxruntime.so (shared with the VAD). Safe to call
// once; no-op if already initialised. onnxruntime's environment must already be
// initialised by an earlier vad.Init or this call.
func Init(ortLib string, poolSize int) error {
	initMu.Lock()
	defer initMu.Unlock()
	if ready.Load() {
		return nil
	}
	if ortLib == "" {
		return fmt.Errorf("turn: onnxruntime lib path empty")
	}
	if poolSize < 1 {
		poolSize = 1
	}
	// Share onnxruntime with the VAD: only initialise the environment if some
	// other component (e.g. vad.Init) has not already done so.
	ort.SetSharedLibraryPath(ortLib)
	if !ort.IsInitialized() {
		if err := ort.InitializeEnvironment(); err != nil {
			return fmt.Errorf("turn: init env: %w", err)
		}
	}
	path, cleanup, err := writeTempModel(modelData)
	if err != nil {
		return err
	}
	defer cleanup()

	p := make(chan *ort.DynamicAdvancedSession, poolSize)
	for i := 0; i < poolSize; i++ {
		s, err := ort.NewDynamicAdvancedSession(path,
			[]string{"input_features"},
			[]string{"logits"}, nil)
		if err != nil {
			return fmt.Errorf("turn: new session: %w", err)
		}
		p <- s
	}
	pool = p
	ready.Store(true)
	return nil
}

// Enabled reports whether the detector is initialised and usable.
func Enabled() bool { return ready.Load() }

// Predict returns the completion probability for the given 16 kHz mono samples
// (the recent utterance audio). prob > 0.5 means the model thinks the turn is
// complete. Callers may pass more than 8 s; only the last 8 s are used.
func Predict(samples []int16) (float32, error) {
	if !ready.Load() {
		return 0, fmt.Errorf("turn: not ready")
	}
	audio := make([]float32, len(samples))
	for i, s := range samples {
		audio[i] = float32(s) / 32768.0
	}
	return predictFloat(audio)
}

func predictFloat(audio []float32) (float32, error) {
	feats := logMelFeatures(audio) // [nMels*nFrames]

	inT, err := ort.NewTensor(ort.NewShape(1, nMels, nFrames), feats)
	if err != nil {
		return 0, err
	}
	defer func() { _ = inT.Destroy() }()
	outT, err := ort.NewEmptyTensor[float32](ort.NewShape(1, 1))
	if err != nil {
		return 0, err
	}
	defer func() { _ = outT.Destroy() }()

	s := <-pool
	defer func() { pool <- s }()
	if err := s.Run([]ort.Value{inT}, []ort.Value{outT}); err != nil {
		return 0, err
	}
	return outT.GetData()[0], nil
}

// SelfTest runs one inference on 1 s of a synthetic 200 Hz tone to verify the
// model loads and the input/output contract holds. It returns the completion
// probability; an error means the ONNX shapes are wrong.
func SelfTest() (float32, error) {
	if !ready.Load() {
		return 0, fmt.Errorf("turn: not ready")
	}
	tone := make([]int16, melSampleRate) // 1 s
	for i := range tone {
		tone[i] = int16(8000 * math.Sin(2*math.Pi*200*float64(i)/float64(melSampleRate)))
	}
	return Predict(tone)
}

// writeTempModel writes the embedded ONNX bytes to a temp file (onnxruntime
// needs a filesystem path) and returns the path plus a cleanup func.
func writeTempModel(data []byte) (string, func(), error) {
	f, err := os.CreateTemp("", "smart_turn_*.onnx")
	if err != nil {
		return "", func() {}, fmt.Errorf("turn: temp model: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", func() {}, fmt.Errorf("turn: write model: %w", err)
	}
	_ = f.Close()
	return f.Name(), func() { _ = os.Remove(f.Name()) }, nil
}
