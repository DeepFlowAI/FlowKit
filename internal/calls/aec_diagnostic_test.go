package calls

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestAECDiagnosticWritesRawProcessedAndRenderWAV(t *testing.T) {
	dir := t.TempDir()
	d := newAECDiagnostic(dir, "call-1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if d == nil {
		t.Fatal("newAECDiagnostic returned nil")
	}
	d.WriteCapture([]int16{1, 2}, []int16{3, 4})
	d.WriteRender([]int16{5, 6})
	d.Close()

	capture := readProbe(t, filepath.Join(dir, "call-1", "capture.wav"))
	if got := binary.LittleEndian.Uint16(capture[22:24]); got != 2 {
		t.Fatalf("capture channels = %d, want 2", got)
	}
	if got := binary.LittleEndian.Uint32(capture[40:44]); got != 8 {
		t.Fatalf("capture data bytes = %d, want 8", got)
	}
	wantCapture := []int16{1, 3, 2, 4}
	for i, want := range wantCapture {
		if got := int16(binary.LittleEndian.Uint16(capture[44+i*2:])); got != want {
			t.Fatalf("capture sample[%d] = %d, want %d", i, got, want)
		}
	}

	render := readProbe(t, filepath.Join(dir, "call-1", "render.wav"))
	if got := binary.LittleEndian.Uint16(render[22:24]); got != 1 {
		t.Fatalf("render channels = %d, want 1", got)
	}
}

func readProbe(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
