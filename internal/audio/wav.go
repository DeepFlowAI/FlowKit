// Package audio decodes container audio formats into the 16 kHz mono int16 PCM
// that Flowkit's media pipeline uses (matching internal/media/bus.SampleRate).
// It is used by call.play to turn a prompt-tone file into playable PCM once,
// before playback begins.
package audio

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/media/codec"
)

// ErrUnsupportedFormat is returned for audio in a container the decoder cannot
// handle (an unrecognized format, or a WAV variant that is not integer PCM16).
var ErrUnsupportedFormat = errors.New("audio: unsupported format")

// DecodeToPCM16k inspects the leading bytes of data and decodes it to 16 kHz
// mono int16 PCM. Both PCM WAV and MP3 are supported; anything else returns
// ErrUnsupportedFormat.
func DecodeToPCM16k(data []byte) ([]int16, error) {
	switch {
	case len(data) >= 12 && string(data[0:4]) == "RIFF" && string(data[8:12]) == "WAVE":
		return decodeWAV(data)
	case len(data) >= 3 && string(data[0:3]) == "ID3":
		return decodeMP3(data)
	case len(data) >= 2 && data[0] == 0xFF && (data[1]&0xE0) == 0xE0:
		return decodeMP3(data)
	default:
		return nil, fmt.Errorf("%w: unrecognized container", ErrUnsupportedFormat)
	}
}

// decodeWAV parses a PCM WAV file: it reads the fmt chunk for the source format
// and the data chunk for samples, down-mixes stereo to mono, and resamples to
// 16 kHz. Only integer PCM (format tag 1) at 16 bits per sample is supported.
func decodeWAV(data []byte) ([]int16, error) {
	if len(data) < 12 {
		return nil, errors.New("audio: wav too short")
	}
	// Skip the 12-byte RIFF/WAVE header, then walk the chunk list.
	var (
		audioFmt   uint16
		numChannel uint16
		sampleRate uint32
		bitsPerSmp uint16
		haveFmt    bool
		pcmBytes   []byte
	)
	off := 12
	for off+8 <= len(data) {
		id := string(data[off : off+4])
		size := int(binary.LittleEndian.Uint32(data[off+4 : off+8]))
		body := off + 8
		if size < 0 || body+size > len(data) {
			// Clamp to what is actually present (some encoders write a
			// streaming-style data size of 0 or an over-long value).
			size = len(data) - body
		}
		switch id {
		case "fmt ":
			if size < 16 {
				return nil, errors.New("audio: wav fmt chunk too small")
			}
			audioFmt = binary.LittleEndian.Uint16(data[body : body+2])
			numChannel = binary.LittleEndian.Uint16(data[body+2 : body+4])
			sampleRate = binary.LittleEndian.Uint32(data[body+4 : body+8])
			bitsPerSmp = binary.LittleEndian.Uint16(data[body+14 : body+16])
			haveFmt = true
		case "data":
			pcmBytes = data[body : body+size]
		}
		// Chunks are word-aligned: an odd size is followed by a pad byte.
		off = body + size + (size & 1)
	}
	if !haveFmt {
		return nil, errors.New("audio: wav missing fmt chunk")
	}
	if audioFmt != 1 {
		return nil, fmt.Errorf("%w: wav format tag %d (only PCM16 supported)", ErrUnsupportedFormat, audioFmt)
	}
	if bitsPerSmp != 16 {
		return nil, fmt.Errorf("%w: wav %d-bit (only 16-bit supported)", ErrUnsupportedFormat, bitsPerSmp)
	}
	if numChannel == 0 {
		return nil, errors.New("audio: wav zero channels")
	}
	if len(pcmBytes) == 0 {
		return nil, errors.New("audio: wav empty data chunk")
	}

	// Bytes → int16 (little-endian), down-mixing interleaved channels to mono.
	ch := int(numChannel)
	frames := len(pcmBytes) / (2 * ch)
	mono := make([]int16, frames)
	for i := 0; i < frames; i++ {
		var sum int32
		for c := 0; c < ch; c++ {
			b := (i*ch + c) * 2
			sum += int32(int16(binary.LittleEndian.Uint16(pcmBytes[b : b+2])))
		}
		mono[i] = int16(sum / int32(ch))
	}

	return codec.ResampleLinear(mono, int(sampleRate), bus.SampleRate), nil
}
