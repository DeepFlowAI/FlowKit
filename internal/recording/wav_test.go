package recording

import (
	"encoding/binary"
	"testing"
)

func TestBuildWAVHeader_Stereo16k(t *testing.T) {
	h := BuildWAVHeader(1000)
	if len(h) != wavHeaderSize {
		t.Fatalf("header size = %d", len(h))
	}
	if string(h[0:4]) != "RIFF" {
		t.Fatalf("RIFF magic")
	}
	if binary.LittleEndian.Uint32(h[40:44]) != 1000 {
		t.Fatalf("data chunk size")
	}
	if binary.LittleEndian.Uint16(h[22:24]) != 2 {
		t.Fatalf("channels")
	}
	if binary.LittleEndian.Uint32(h[24:28]) != 16000 {
		t.Fatalf("sample rate")
	}
}

func TestBuildWAVHeader_Mono16k(t *testing.T) {
	h := BuildWAVHeaderChannels(1, 500)
	if binary.LittleEndian.Uint16(h[22:24]) != 1 {
		t.Fatalf("channels")
	}
	if binary.LittleEndian.Uint32(h[28:32]) != 32000 {
		t.Fatalf("byte rate")
	}
}
