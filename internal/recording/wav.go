package recording

import (
	"encoding/binary"
)

const (
	wavHeaderSize    = 44
	wavSampleRate    = 16000
	wavBitsPerSample = 16
)

// wavChannels is the encoder's internal stereo interleave width (L+R).
const wavChannels = 2

// BuildWAVHeader returns a 44-byte PCM WAV header for stereo 16 kHz 16-bit LE.
func BuildWAVHeader(dataSize uint32) []byte {
	return BuildWAVHeaderChannels(2, dataSize)
}

// BuildWAVHeaderChannels builds a WAV header for the given channel count (1 or 2).
func BuildWAVHeaderChannels(channels int, dataSize uint32) []byte {
	if channels < 1 {
		channels = 1
	}
	if channels > 2 {
		channels = 2
	}
	byteRate := uint32(wavSampleRate * channels * wavBitsPerSample / 8)
	blockAlign := uint16(channels * wavBitsPerSample / 8)
	riffSize := uint32(36) + dataSize

	h := make([]byte, wavHeaderSize)
	copy(h[0:4], "RIFF")
	binary.LittleEndian.PutUint32(h[4:8], riffSize)
	copy(h[8:12], "WAVE")
	copy(h[12:16], "fmt ")
	binary.LittleEndian.PutUint32(h[16:20], 16)
	binary.LittleEndian.PutUint16(h[20:22], 1)
	binary.LittleEndian.PutUint16(h[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(h[24:28], wavSampleRate)
	binary.LittleEndian.PutUint32(h[28:32], byteRate)
	binary.LittleEndian.PutUint16(h[32:34], blockAlign)
	binary.LittleEndian.PutUint16(h[34:36], wavBitsPerSample)
	copy(h[36:40], "data")
	binary.LittleEndian.PutUint32(h[40:44], dataSize)
	return h
}

// BuildWAVHeaderPlaceholderForLayout writes a header with zero-length data for layout.
func BuildWAVHeaderPlaceholderForLayout(layout Layout) []byte {
	return BuildWAVHeaderChannels(channelsForLayout(layout), 0)
}

// PatchWAVHeaderForLayout rewrites RIFF/data sizes for the final object.
func PatchWAVHeaderForLayout(layout Layout, dataSize uint32) []byte {
	return BuildWAVHeaderChannels(channelsForLayout(layout), dataSize)
}

// durationMSForLayout converts a written PCM payload into playable milliseconds.
// Reporting how long a recorder stayed open would overstate files that dropped
// frames or were cut short, and consumers use the duration to render a playback
// timeline, so it has to describe the audio actually stored.
func durationMSForLayout(layout Layout, dataSize uint32) int64 {
	bytesPerMS := wavSampleRate * channelsForLayout(layout) * wavBitsPerSample / 8 / 1000
	if bytesPerMS <= 0 {
		return 0
	}
	return int64(dataSize) / int64(bytesPerMS)
}
