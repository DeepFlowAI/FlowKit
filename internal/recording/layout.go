package recording

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// Layout selects how PCM is written into the WAV object.
type Layout string

const (
	// LayoutStereo keeps dual-channel WAV (L=remote/user, R=local/agent).
	LayoutStereo Layout = "stereo"
	// LayoutMonoMix averages L and R into a single channel for playback in both ears.
	LayoutMonoMix Layout = "mono_mix"
)

const wavStereoFrameBytes = 4 // 16-bit LE interleaved L+R per sample frame

// ParseLayout parses a layout string. Empty means stereo.
func ParseLayout(s string) (Layout, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return LayoutStereo, nil
	case string(LayoutStereo):
		return LayoutStereo, nil
	case string(LayoutMonoMix):
		return LayoutMonoMix, nil
	default:
		return "", fmt.Errorf("recording: layout %q must be stereo or mono_mix", s)
	}
}

func channelsForLayout(layout Layout) int {
	if layout == LayoutMonoMix {
		return 1
	}
	return 2
}

// encodePCMForLayout converts encoder output (always stereo interleaved) to the
// on-disk layout. Stereo passes through; mono_mix averages L/R per sample.
func encodePCMForLayout(layout Layout, stereoPCM []byte) []byte {
	if len(stereoPCM) == 0 || layout != LayoutMonoMix {
		return stereoPCM
	}
	return mixStereoPCMToMono(stereoPCM)
}

// mixStereoPCMToMono averages L/R; when one side is silent the active side is ~6 dB lower.
func mixStereoPCMToMono(stereo []byte) []byte {
	if len(stereo)%wavStereoFrameBytes != 0 {
		stereo = stereo[:len(stereo)/wavStereoFrameBytes*wavStereoFrameBytes]
	}
	n := len(stereo) / wavStereoFrameBytes
	out := make([]byte, n*2)
	for i := 0; i < n; i++ {
		off := i * wavStereoFrameBytes
		l := int32(int16(binary.LittleEndian.Uint16(stereo[off : off+2])))
		r := int32(int16(binary.LittleEndian.Uint16(stereo[off+2 : off+4])))
		s := int16((l + r) / 2)
		binary.LittleEndian.PutUint16(out[i*2:i*2+2], uint16(s))
	}
	return out
}
