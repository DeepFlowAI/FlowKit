package audio

// This file decodes MP3 (ID3-tagged or raw MPEG frames) into the 16 kHz mono
// int16 PCM the media pipeline uses. Decoding is pure Go (no cgo) via go-mp3;
// call.play uses it to turn an MP3 prompt file into playable PCM once.

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/hajimehoshi/go-mp3"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/media/codec"
)

// decodeMP3 decodes an MP3 byte stream to 16 kHz mono int16 PCM. go-mp3 always
// emits 16-bit little-endian stereo (interleaved) PCM at the source sample
// rate, so we down-mix to mono and resample to bus.SampleRate.
func decodeMP3(data []byte) ([]int16, error) {
	if len(data) == 0 {
		return nil, errors.New("audio: mp3 empty data")
	}

	d, err := mp3.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("audio: mp3 decode: %w", err)
	}
	srcRate := d.SampleRate()
	if srcRate <= 0 {
		return nil, fmt.Errorf("audio: mp3 invalid sample rate %d", srcRate)
	}

	pcmBytes, err := io.ReadAll(d)
	if err != nil {
		return nil, fmt.Errorf("audio: mp3 read: %w", err)
	}
	if len(pcmBytes) < 4 {
		return nil, errors.New("audio: mp3 produced no samples")
	}

	// Bytes → int16 (little-endian), down-mixing interleaved L/R to mono by
	// averaging (matching decodeWAV's down-mix).
	frames := len(pcmBytes) / 4
	mono := make([]int16, frames)
	for i := 0; i < frames; i++ {
		b := i * 4
		l := int32(int16(uint16(pcmBytes[b]) | uint16(pcmBytes[b+1])<<8))
		r := int32(int16(uint16(pcmBytes[b+2]) | uint16(pcmBytes[b+3])<<8))
		mono[i] = int16((l + r) / 2)
	}

	return codec.ResampleLinear(mono, srcRate, bus.SampleRate), nil
}
