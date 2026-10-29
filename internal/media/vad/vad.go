// Package vad is a streaming Silero VAD (neural speech-vs-noise detector) used
// to gate barge-in and ASR-final acceptance: real human voice triggers/records,
// noise does not. The Silero ONNX model is embedded; onnxruntime is loaded at
// runtime (dlopen) from VAD_ONNXRUNTIME_LIB. A small pool of stateless sessions
// is shared across all calls; per-call LSTM state lives in each Detector, so
// concurrency only costs a few KB + ~1ms CPU per 32ms window per active call.
package vad

import (
	_ "embed"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	ort "github.com/yalue/onnxruntime_go"
)

//go:embed silero_vad.onnx
var modelData []byte

const (
	// WindowSamples is Silero v5's fixed window at 16 kHz (32 ms).
	WindowSamples = 512
	// contextSize is the 64 trailing samples of the previous window that Silero
	// v5 prepends to each window; the model input is contextSize+WindowSamples
	// (576). Feeding only 512 makes the model score speech near zero.
	contextSize = 64
	sampleRate  = 16000
	stateLen    = 2 * 1 * 128 // LSTM state [2,1,128]
)

var (
	initMu sync.Mutex
	ready  atomic.Bool
	pool   chan *ort.DynamicAdvancedSession
)

// Init loads onnxruntime + the Silero model into a pool of poolSize sessions.
// ortLib is the path to libonnxruntime.so. Safe to call once; no-op if already
// initialised or if ortLib is empty (VAD then stays disabled — callers fall back).
func Init(ortLib string, poolSize int) error {
	initMu.Lock()
	defer initMu.Unlock()
	if ready.Load() {
		return nil
	}
	if ortLib == "" {
		return fmt.Errorf("vad: onnxruntime lib path empty")
	}
	if poolSize < 1 {
		poolSize = 1
	}
	// The onnxruntime environment is process-wide and shared with the other
	// models we run, so whoever gets here first sets it up and the rest join in.
	if !ort.IsInitialized() {
		ort.SetSharedLibraryPath(ortLib)
		if err := ort.InitializeEnvironment(); err != nil {
			return fmt.Errorf("vad: init env: %w", err)
		}
	}
	f, err := os.CreateTemp("", "silero_vad_*.onnx")
	if err != nil {
		return fmt.Errorf("vad: temp model: %w", err)
	}
	if _, err := f.Write(modelData); err != nil {
		_ = f.Close()
		return fmt.Errorf("vad: write model: %w", err)
	}
	_ = f.Close()

	p := make(chan *ort.DynamicAdvancedSession, poolSize)
	for i := 0; i < poolSize; i++ {
		s, err := ort.NewDynamicAdvancedSession(f.Name(),
			[]string{"input", "state", "sr"},
			[]string{"output", "stateN"}, nil)
		if err != nil {
			return fmt.Errorf("vad: new session: %w", err)
		}
		p <- s
	}
	pool = p
	ready.Store(true)
	return nil
}

// Enabled reports whether the VAD is initialised and usable.
func Enabled() bool { return ready.Load() }

// Detector is a per-call streaming detector. Feed must be called from a single
// goroutine; Speaking may be read concurrently.
type Detector struct {
	state    []float32
	context  []float32 // 64 trailing samples of the previous window
	buf      []int16
	speaking atomic.Bool

	speechRun, silenceRun int
	onThr, offThr         float32
	onWin, offWin         int
}

// Detector sensitivity, tunable from env (read once). Defaults: ~96 ms
// sustained speech to flip on (fast barge-in), ~400 ms silence hangover to flip
// off. Lower onThr/onWin catches soft double-talk onsets sooner (helps barge-in
// when AEC attenuates the near-end voice) at the cost of more noise flips.
//   - VAD_ON_THRESHOLD  (float, default 0.5)
//   - VAD_OFF_THRESHOLD (float, default 0.35)
//   - VAD_ON_WINDOWS    (int windows @32ms, default 3)
//   - VAD_OFF_WINDOWS   (int windows @32ms, default 12)
var (
	cfgOnThr  = envFloat("VAD_ON_THRESHOLD", 0.5)
	cfgOffThr = envFloat("VAD_OFF_THRESHOLD", 0.35)
	cfgOnWin  = envInt("VAD_ON_WINDOWS", 3)
	cfgOffWin = envInt("VAD_OFF_WINDOWS", 12)
)

func envFloat(key string, def float32) float32 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 32); err == nil && f > 0 {
			return float32(f)
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// NewDetector returns a detector using the env-tuned sensitivity.
func NewDetector() *Detector {
	return &Detector{
		state:   make([]float32, stateLen),
		context: make([]float32, contextSize),
		onThr:   cfgOnThr,
		offThr:  cfgOffThr,
		onWin:   cfgOnWin,
		offWin:  cfgOffWin,
	}
}

// Feed runs VAD over full 512-sample windows in the 16 kHz int16 samples and
// updates the speaking state. Returns how many windows were classified speech.
func (d *Detector) Feed(samples []int16) (speechWindows int) {
	if !ready.Load() || d == nil {
		return 0
	}
	d.buf = append(d.buf, samples...)
	for len(d.buf) >= WindowSamples {
		win := d.buf[:WindowSamples]
		prob, err := d.run(win)
		d.buf = d.buf[WindowSamples:]
		if err != nil {
			logRunErr(err)
			continue
		}
		if debugProb {
			logProbDebug(winRMS(win), prob)
		}
		switch {
		case prob >= d.onThr:
			d.speechRun++
			d.silenceRun = 0
			if d.speechRun >= d.onWin {
				d.speaking.Store(true)
			}
			speechWindows++
		case prob < d.offThr:
			d.silenceRun++
			d.speechRun = 0
			if d.silenceRun >= d.offWin {
				d.speaking.Store(false)
			}
		}
	}
	return speechWindows
}

// Speaking reports whether human voice is currently active (with hangover).
func (d *Detector) Speaking() bool {
	return d != nil && d.speaking.Load()
}

func (d *Detector) run(win []int16) (float32, error) {
	s := <-pool
	defer func() { pool <- s }()

	// Silero v5 input = 64-sample context (previous window's tail) + 512 new.
	in := make([]float32, contextSize+WindowSamples)
	copy(in[:contextSize], d.context)
	for i, v := range win {
		in[contextSize+i] = float32(v) / 32768.0
	}
	// Carry the last 64 normalised samples as the next window's context.
	copy(d.context, in[contextSize+WindowSamples-contextSize:])
	inT, err := ort.NewTensor(ort.NewShape(1, contextSize+WindowSamples), in)
	if err != nil {
		return 0, err
	}
	defer func() { _ = inT.Destroy() }()
	stT, err := ort.NewTensor(ort.NewShape(2, 1, 128), d.state)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stT.Destroy() }()
	srT, err := ort.NewTensor(ort.NewShape(1), []int64{sampleRate})
	if err != nil {
		return 0, err
	}
	defer func() { _ = srT.Destroy() }()
	outT, err := ort.NewEmptyTensor[float32](ort.NewShape(1, 1))
	if err != nil {
		return 0, err
	}
	defer func() { _ = outT.Destroy() }()
	stNT, err := ort.NewEmptyTensor[float32](ort.NewShape(2, 1, 128))
	if err != nil {
		return 0, err
	}
	defer func() { _ = stNT.Destroy() }()

	if err := s.Run([]ort.Value{inT, stT, srT}, []ort.Value{outT, stNT}); err != nil {
		return 0, err
	}
	copy(d.state, stNT.GetData())
	return outT.GetData()[0], nil
}

var runErrCount atomic.Int64

func logRunErr(err error) {
	if n := runErrCount.Add(1); n <= 5 {
		slog.Default().Warn("vad inference error", "err", err)
	}
}

// debugProb (env VAD_DEBUG) logs (rms, prob) for audible windows to diagnose
// why real speech may score low.
var (
	debugProb  = os.Getenv("VAD_DEBUG") == "1" || os.Getenv("VAD_DEBUG") == "true"
	debugProbN atomic.Int64
)

func winRMS(win []int16) float64 {
	var sum float64
	for _, s := range win {
		sum += float64(s) * float64(s)
	}
	return math.Sqrt(sum / float64(len(win)))
}

func logProbDebug(rms float64, prob float32) {
	// Log audible windows the VAD did NOT clearly mark as speech (prob below the
	// on-threshold). These are the "why didn't barge-in trigger" cases — audible
	// near-end voice that the gate suppressed (e.g. AEC-attenuated double-talk).
	if rms < 150 || prob >= cfgOnThr {
		return
	}
	if n := debugProbN.Add(1); n <= 400 {
		slog.Default().Info("vad.prob.miss", "rms", int(rms), "prob", prob)
	}
}

// SelfTest runs one inference on a synthetic 300 Hz tone to verify the model
// loads and infers. Returns the speech probability (a tone gives a non-trivial
// value; an error means the input/output contract is wrong).
func SelfTest() (float32, error) {
	if !ready.Load() {
		return 0, fmt.Errorf("vad not ready")
	}
	d := NewDetector()
	win := make([]int16, WindowSamples)
	for i := range win {
		win[i] = int16(8000 * math.Sin(2*math.Pi*300*float64(i)/sampleRate))
	}
	return d.run(win)
}
