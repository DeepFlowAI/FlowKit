package calls

import (
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// aecDiagnostic writes opt-in, per-call WAV probes:
//   - capture.wav: stereo, left=raw SIP uplink, right=post-AEC uplink
//   - render.wav: mono, the paced TTS reference supplied to AEC
//
// It is disabled unless AEC_DIAGNOSTIC_DIR is set. Files are intentionally
// local and short-lived diagnostic artifacts, not part of normal recording.
type aecDiagnostic struct {
	mu sync.Mutex

	capture *wavProbe
	render  *wavProbe
	log     *slog.Logger
}

func newAECDiagnostic(dir, callID string, log *slog.Logger) *aecDiagnostic {
	if dir == "" {
		return nil
	}
	callDir := filepath.Join(dir, callID)
	if err := os.MkdirAll(callDir, 0o750); err != nil {
		log.Warn("aec diagnostic mkdir failed", "call_id", callID, "err", err)
		return nil
	}
	capture, err := newWAVProbe(filepath.Join(callDir, "capture.wav"), 2)
	if err != nil {
		log.Warn("aec diagnostic capture open failed", "call_id", callID, "err", err)
		return nil
	}
	render, err := newWAVProbe(filepath.Join(callDir, "render.wav"), 1)
	if err != nil {
		_ = capture.Close()
		log.Warn("aec diagnostic render open failed", "call_id", callID, "err", err)
		return nil
	}
	log.Info("aec diagnostic enabled", "call_id", callID, "dir", callDir)
	return &aecDiagnostic{capture: capture, render: render, log: log}
}

func (d *aecDiagnostic) WriteCapture(raw, processed []int16) {
	if d == nil {
		return
	}
	n := min(len(raw), len(processed))
	interleaved := make([]int16, n*2)
	for i := 0; i < n; i++ {
		interleaved[i*2] = raw[i]
		interleaved[i*2+1] = processed[i]
	}
	d.mu.Lock()
	_ = d.capture.Write(interleaved)
	d.mu.Unlock()
}

func (d *aecDiagnostic) WriteRender(samples []int16) {
	if d == nil {
		return
	}
	d.mu.Lock()
	_ = d.render.Write(samples)
	d.mu.Unlock()
}

func (d *aecDiagnostic) Close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	captureErr := d.capture.Close()
	renderErr := d.render.Close()
	d.mu.Unlock()
	if captureErr != nil || renderErr != nil {
		d.log.Warn("aec diagnostic close failed", "capture_err", captureErr, "render_err", renderErr)
	}
}

type wavProbe struct {
	file     *os.File
	channels uint16
	bytes    uint32
	closed   bool
}

func newWAVProbe(path string, channels uint16) (*wavProbe, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	p := &wavProbe{file: file, channels: channels}
	if _, err := file.Write(makeWAVHeader(channels, 0)); err != nil {
		_ = file.Close()
		return nil, err
	}
	return p, nil
}

func (p *wavProbe) Write(samples []int16) error {
	if p.closed {
		return nil
	}
	data := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(data[i*2:], uint16(sample))
	}
	n, err := p.file.Write(data)
	p.bytes += uint32(n)
	return err
}

func (p *wavProbe) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	if _, err := p.file.Seek(0, 0); err != nil {
		_ = p.file.Close()
		return err
	}
	if _, err := p.file.Write(makeWAVHeader(p.channels, p.bytes)); err != nil {
		_ = p.file.Close()
		return err
	}
	return p.file.Close()
}

func makeWAVHeader(channels uint16, dataBytes uint32) []byte {
	const (
		sampleRate    = uint32(16000)
		bitsPerSample = uint16(16)
	)
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 36+dataBytes)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], channels)
	binary.LittleEndian.PutUint32(header[24:28], sampleRate)
	byteRate := sampleRate * uint32(channels) * uint32(bitsPerSample/8)
	binary.LittleEndian.PutUint32(header[28:32], byteRate)
	binary.LittleEndian.PutUint16(header[32:34], channels*(bitsPerSample/8))
	binary.LittleEndian.PutUint16(header[34:36], bitsPerSample)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataBytes)
	return header
}
