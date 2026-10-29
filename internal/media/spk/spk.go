// Package spk tells the caller's voice from everyone else's. On a street or in
// an open office the mic picks up bystanders talking, and a VAD-less ASR turns
// them into fluent sentences that look exactly like the caller's own turns:
// energy, duration and denoising cannot separate them, because they really are
// human speech. Who is speaking is the only dimension that can.
//
// It embeds each finalised utterance with CAM++ (a 192-dimensional speaker
// vector), enrolls the first solid utterance as the caller, and scores the rest
// against it by cosine similarity. Measured on a street call, the caller's own
// utterances score around 0.6 against the enrollment while bystanders sit near
// 0.2, so a threshold in between separates them with a wide margin.
package spk

import (
	_ "embed"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	ort "github.com/yalue/onnxruntime_go"
)

//go:embed campplus.onnx
var modelData []byte

// EmbeddingDim is the width of a CAM++ speaker vector.
const EmbeddingDim = 192

var (
	initMu sync.Mutex
	ready  atomic.Bool
	pool   chan *ort.DynamicAdvancedSession
)

// Init loads onnxruntime + the CAM++ model into a pool of poolSize sessions.
// ortLib is the path to libonnxruntime.so (shared with the other models). Safe
// to call once; no-op if already initialised.
func Init(ortLib string, poolSize int) error {
	initMu.Lock()
	defer initMu.Unlock()
	if ready.Load() {
		return nil
	}
	if ortLib == "" {
		return fmt.Errorf("spk: onnxruntime lib path empty")
	}
	if poolSize < 1 {
		poolSize = 1
	}
	// The VAD normally initialises the process-wide onnxruntime environment
	// first; joining the existing one is the common path, not an error.
	if !ort.IsInitialized() {
		ort.SetSharedLibraryPath(ortLib)
		if err := ort.InitializeEnvironment(); err != nil {
			return fmt.Errorf("spk: init env: %w", err)
		}
	}
	f, err := os.CreateTemp("", "campplus_*.onnx")
	if err != nil {
		return fmt.Errorf("spk: temp model: %w", err)
	}
	if _, err := f.Write(modelData); err != nil {
		_ = f.Close()
		return fmt.Errorf("spk: write model: %w", err)
	}
	_ = f.Close()

	p := make(chan *ort.DynamicAdvancedSession, poolSize)
	for i := 0; i < poolSize; i++ {
		s, err := ort.NewDynamicAdvancedSession(f.Name(),
			[]string{"input"}, []string{"output"}, nil)
		if err != nil {
			return fmt.Errorf("spk: new session: %w", err)
		}
		p <- s
	}
	pool = p
	ready.Store(true)
	return nil
}

// Enabled reports whether speaker embedding is initialised and usable.
func Enabled() bool { return ready.Load() }

// Embed turns 16 kHz mono speech into a unit-length speaker vector. The audio
// should be the voiced part of one utterance; anything under a window (25 ms)
// has no features to embed.
func Embed(samples []int16) ([]float32, error) {
	if !ready.Load() {
		return nil, fmt.Errorf("spk: not initialised")
	}
	feat := fbankFeatures(samples)
	if feat == nil {
		return nil, fmt.Errorf("spk: audio too short (%d samples)", len(samples))
	}
	frames := int64(len(feat) / fbMels)
	in, err := ort.NewTensor(ort.NewShape(1, frames, fbMels), feat)
	if err != nil {
		return nil, fmt.Errorf("spk: input tensor: %w", err)
	}
	defer func() { _ = in.Destroy() }()
	out, err := ort.NewEmptyTensor[float32](ort.NewShape(1, EmbeddingDim))
	if err != nil {
		return nil, fmt.Errorf("spk: output tensor: %w", err)
	}
	defer func() { _ = out.Destroy() }()

	sess := <-pool
	err = sess.Run([]ort.Value{in}, []ort.Value{out})
	pool <- sess
	if err != nil {
		return nil, fmt.Errorf("spk: run: %w", err)
	}
	emb := make([]float32, EmbeddingDim)
	copy(emb, out.GetData())
	normalize(emb)
	return emb, nil
}

// normalize scales a vector to unit length, so a dot product is the cosine
// similarity.
func normalize(v []float32) {
	var sum float32
	for _, x := range v {
		sum += x * x
	}
	if sum <= 0 {
		return
	}
	inv := float32(1 / math.Sqrt(float64(sum)))
	for i := range v {
		v[i] *= inv
	}
}

// similarity is the cosine similarity of two unit-length vectors.
func similarity(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot
}
