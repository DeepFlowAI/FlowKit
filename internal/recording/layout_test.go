package recording

import (
	"encoding/binary"
	"testing"
)

func TestParseLayout_DefaultStereo(t *testing.T) {
	got, err := ParseLayout("")
	if err != nil || got != LayoutStereo {
		t.Fatalf("got %q err=%v", got, err)
	}
}

func TestParseLayout_Invalid(t *testing.T) {
	if _, err := ParseLayout("streo"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseLayout_MonoMix(t *testing.T) {
	for _, in := range []string{"mono_mix", "MONO_MIX"} {
		got, err := ParseLayout(in)
		if err != nil || got != LayoutMonoMix {
			t.Fatalf("%q => %q err=%v", in, got, err)
		}
	}
}

func TestMixStereoPCMToMono(t *testing.T) {
	stereo := make([]byte, 8) // 2 frames
	binary.LittleEndian.PutUint16(stereo[0:2], 1000)  // L
	binary.LittleEndian.PutUint16(stereo[2:4], 3000)  // R
	binary.LittleEndian.PutUint16(stereo[4:6], 2000)
	binary.LittleEndian.PutUint16(stereo[6:8], 4000)

	mono := mixStereoPCMToMono(stereo)
	if len(mono) != 4 {
		t.Fatalf("len=%d", len(mono))
	}
	if int16(binary.LittleEndian.Uint16(mono[0:2])) != 2000 {
		t.Fatalf("frame0 = %d", int16(binary.LittleEndian.Uint16(mono[0:2])))
	}
	if int16(binary.LittleEndian.Uint16(mono[2:4])) != 3000 {
		t.Fatalf("frame1 = %d", int16(binary.LittleEndian.Uint16(mono[2:4])))
	}
}

func TestEncodePCMForLayout_StereoPassthrough(t *testing.T) {
	in := []byte{1, 2, 3, 4}
	out := encodePCMForLayout(LayoutStereo, in)
	if len(out) != 4 || out[0] != 1 {
		t.Fatal("expected passthrough")
	}
}
