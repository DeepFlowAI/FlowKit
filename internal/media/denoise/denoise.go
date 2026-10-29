// Package denoise is a streaming speech enhancer (GTCRN) that removes
// environmental noise — traffic, wind, fans — from the mic uplink before VAD and
// ASR see it. On a street the raw uplink carries enough noise for a VAD-less ASR
// to hallucinate whole sentences out of it, which then reach the dialog as user
// turns; cleaning the audio at the entrance is what stops that.
//
// It does not remove other people's speech. Nothing in the open-source
// enhancement family does: separating a target talker from competing talkers
// needs speaker enrollment, which is a different mechanism.
//
// The model runs frame by frame in the STFT domain (512-point window, 256-sample
// hop at 16 kHz) with its recurrent state carried in three caches, so each call
// keeps its own Denoiser while the ONNX sessions are pooled and shared. Added
// latency is one hop (16 ms).
package denoise

import (
	_ "embed"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	ort "github.com/yalue/onnxruntime_go"
)

//go:embed gtcrn.onnx
var modelData []byte

// Cache shapes are fixed by the exported graph.
var (
	convCacheShape  = ort.NewShape(2, 1, 16, 16, 33)
	traCacheShape   = ort.NewShape(2, 3, 1, 1, 16)
	interCacheShape = ort.NewShape(2, 1, 33, 16)
	specShape       = ort.NewShape(1, bins, 1, 2)
)

var (
	initMu sync.Mutex
	ready  atomic.Bool
	pool   chan *ort.DynamicAdvancedSession
	window [nfft]float32 // periodic sqrt-Hann, used for analysis and synthesis
	mixWet = defaultMixWet
)

// How much of the enhanced signal to keep, the rest being the untouched input.
// The model is built for low-SNR audio and suppresses hard, which on a real
// call takes a quarter of the speech-level frames down to silence along with
// the noise; a caller whose words vanish is worse off than one who is merely
// noisy. Holding back a share of the original bounds that loss at about -10dB —
// still audible to VAD, ASR and the speaker embedding — and costs the same
// -10dB off the noise suppression.
const defaultMixWet float32 = 0.7

func init() {
	for i := range window {
		h := 0.5 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(nfft))
		window[i] = float32(math.Sqrt(h))
	}
}

// Init loads onnxruntime + the GTCRN model into a pool of poolSize sessions.
// ortLib is the path to libonnxruntime.so (the same library the VAD uses). Safe
// to call once; no-op if already initialised. Wet/dry mix comes from
// dialogpolicy (SIP_LLM_DENOISE_MIX); 1 is the model on its own.
func Init(ortLib string, poolSize int) error {
	initMu.Lock()
	defer initMu.Unlock()
	if ready.Load() {
		return nil
	}
	mixWet = float32(dialogpolicy.Get().DenoiseMix)
	if ortLib == "" {
		return fmt.Errorf("denoise: onnxruntime lib path empty")
	}
	if poolSize < 1 {
		poolSize = 1
	}
	// The VAD normally initialises the process-wide onnxruntime environment
	// first; joining the existing one is the common path, not an error.
	if !ort.IsInitialized() {
		ort.SetSharedLibraryPath(ortLib)
		if err := ort.InitializeEnvironment(); err != nil {
			return fmt.Errorf("denoise: init env: %w", err)
		}
	}
	f, err := os.CreateTemp("", "gtcrn_*.onnx")
	if err != nil {
		return fmt.Errorf("denoise: temp model: %w", err)
	}
	if _, err := f.Write(modelData); err != nil {
		_ = f.Close()
		return fmt.Errorf("denoise: write model: %w", err)
	}
	_ = f.Close()

	p := make(chan *ort.DynamicAdvancedSession, poolSize)
	for i := 0; i < poolSize; i++ {
		s, err := ort.NewDynamicAdvancedSession(f.Name(),
			[]string{"mix", "conv_cache", "tra_cache", "inter_cache"},
			[]string{"enh", "conv_cache_out", "tra_cache_out", "inter_cache_out"}, nil)
		if err != nil {
			return fmt.Errorf("denoise: new session: %w", err)
		}
		p <- s
	}
	pool = p
	ready.Store(true)
	return nil
}

// Enabled reports whether the enhancer is initialised and usable.
func Enabled() bool { return ready.Load() }

// Denoiser is one call's streaming state. Process must be called from a single
// goroutine (the audio pump).
type Denoiser struct {
	fft *fft512
	mix float32 // share of the enhanced signal in the output

	analysis [nfft]float32 // sliding window of input samples
	overlap  [nfft]float32 // synthesis accumulator
	spec     [bins * 2]float32
	frame    [nfft]float32

	pending []int16 // input not yet forming a full hop
	out     []int16 // enhanced samples ready to hand back

	convCache, traCache, interCache *ort.Tensor[float32]
	convOut, traOut, interOut       *ort.Tensor[float32]
	specIn, specOut                 *ort.Tensor[float32]
	err                             error
}

// New allocates a per-call enhancer, or nil if the model is not initialised.
// Close must be called when the call ends.
func New() *Denoiser {
	if !ready.Load() {
		return nil
	}
	d := &Denoiser{fft: newFFT512(), mix: mixWet}
	// One hop of silence up front: the first hop of output only exists after a
	// full window has been seen, and priming here keeps Process returning as many
	// samples as it was given instead of a short first frame.
	d.out = make([]int16, 0, 2*nfft)
	d.out = append(d.out, make([]int16, hop)...)

	var err error
	alloc := func(shape ort.Shape) *ort.Tensor[float32] {
		if err != nil {
			return nil
		}
		var t *ort.Tensor[float32]
		t, err = ort.NewEmptyTensor[float32](shape)
		return t
	}
	d.convCache, d.convOut = alloc(convCacheShape), alloc(convCacheShape)
	d.traCache, d.traOut = alloc(traCacheShape), alloc(traCacheShape)
	d.interCache, d.interOut = alloc(interCacheShape), alloc(interCacheShape)
	d.specIn, d.specOut = alloc(specShape), alloc(specShape)
	if err != nil {
		d.Close()
		return nil
	}
	return d
}

// Process enhances a frame of 16 kHz mono samples and returns the same number of
// enhanced samples, delayed by one hop. On any inference error it degrades to
// passthrough rather than dropping audio.
func (d *Denoiser) Process(in []int16) []int16 {
	if d == nil || d.err != nil {
		return in
	}
	d.pending = append(d.pending, in...)
	for len(d.pending) >= hop {
		if err := d.step(d.pending[:hop]); err != nil {
			d.err = err
			return in
		}
		d.pending = d.pending[hop:]
	}
	if len(d.out) < len(in) {
		return in // still filling the first window
	}
	res := make([]int16, len(in))
	copy(res, d.out[:len(in)])
	d.out = d.out[:copy(d.out, d.out[len(in):])]
	return res
}

// step slides in one hop, runs the model on the resulting window and appends the
// hop of enhanced output that overlap-add has completed.
func (d *Denoiser) step(samples []int16) error {
	copy(d.analysis[:], d.analysis[hop:])
	for i, s := range samples {
		d.analysis[nfft-hop+i] = float32(s) / 32768
	}
	for i := range d.frame {
		d.frame[i] = d.analysis[i] * window[i]
	}
	d.fft.forward(d.frame[:], d.spec[:])

	if err := d.infer(); err != nil {
		return err
	}

	d.fft.inverse(d.spec[:], d.frame[:])
	// Analysis and synthesis both apply sqrt-Hann, whose squares sum to 1 at 50%
	// overlap, so overlap-add needs no normalisation.
	for i := range d.frame {
		d.overlap[i] += d.frame[i] * window[i]
	}
	// The hop being emitted is the window's leading hop, so analysis[0:hop] is
	// the untouched input for exactly these samples — mixing here keeps dry and
	// wet in phase, which mixing outside the enhancer could not.
	for i := 0; i < hop; i++ {
		s := d.mix*d.overlap[i] + (1-d.mix)*d.analysis[i]
		v := math.Round(float64(s) * 32768)
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		d.out = append(d.out, int16(v))
	}
	copy(d.overlap[:], d.overlap[hop:])
	for i := nfft - hop; i < nfft; i++ {
		d.overlap[i] = 0
	}
	return nil
}

// infer runs one frame through a pooled session, replacing d.spec with the
// enhanced spectrum and rotating the recurrent caches.
func (d *Denoiser) infer() error {
	copy(d.specIn.GetData(), d.spec[:])
	sess := <-pool
	err := sess.Run(
		[]ort.Value{d.specIn, d.convCache, d.traCache, d.interCache},
		[]ort.Value{d.specOut, d.convOut, d.traOut, d.interOut})
	pool <- sess
	if err != nil {
		return fmt.Errorf("denoise: run: %w", err)
	}
	copy(d.spec[:], d.specOut.GetData())
	d.convCache, d.convOut = d.convOut, d.convCache
	d.traCache, d.traOut = d.traOut, d.traCache
	d.interCache, d.interOut = d.interOut, d.interCache
	return nil
}

// Close releases the call's tensors.
func (d *Denoiser) Close() {
	if d == nil {
		return
	}
	for _, t := range []*ort.Tensor[float32]{
		d.convCache, d.traCache, d.interCache,
		d.convOut, d.traOut, d.interOut, d.specIn, d.specOut,
	} {
		if t != nil {
			_ = t.Destroy()
		}
	}
}
