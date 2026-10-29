// Package codec converts audio between wire-level formats (G.711, eventually
// Opus) and Flowkit's internal PCM 16 kHz mono representation.
//
// G.711 μ-law (PCMU) and A-law (PCMA) are 8-bit, 8 kHz mono codecs.
// Encoding/decoding is a fixed table lookup — no licence issues, no cgo.
package codec

// μ-law / A-law decode tables, computed at package init. Each maps the 8-bit
// codeword to a signed 16-bit linear PCM sample.
var (
	muLawDecodeTable [256]int16
	aLawDecodeTable  [256]int16
)

func init() {
	for i := 0; i < 256; i++ {
		muLawDecodeTable[i] = muLawDecodeSample(byte(i))
		aLawDecodeTable[i] = aLawDecodeSample(byte(i))
	}
}

// ---- μ-law (PCMU) ----

// EncodePCMU converts a slice of 16-bit linear PCM samples to μ-law bytes.
// One input sample produces exactly one output byte.
func EncodePCMU(samples []int16) []byte {
	out := make([]byte, len(samples))
	for i, s := range samples {
		out[i] = encodeMuLawSample(s)
	}
	return out
}

// DecodePCMU converts μ-law bytes to 16-bit linear PCM samples. One input
// byte produces exactly one output sample.
func DecodePCMU(b []byte) []int16 {
	out := make([]int16, len(b))
	for i, c := range b {
		out[i] = muLawDecodeTable[c]
	}
	return out
}

// encodeMuLawSample compresses one 16-bit linear sample to one μ-law byte.
// Algorithm per ITU-T G.711 §3. Operates in int32 internally so that the
// most-negative input (-32768) does not overflow on negation.
func encodeMuLawSample(s int16) byte {
	const bias = 0x84
	const clip = 32635

	sign := byte(0)
	v := int32(s)
	if v < 0 {
		v = -v
		sign = 0x80
	}
	if v > clip {
		v = clip
	}
	v += bias
	exponent := byte(7)
	for mask := int32(0x4000); v&mask == 0 && exponent > 0; mask >>= 1 {
		exponent--
	}
	mantissa := byte((v >> (exponent + 3)) & 0x0F)
	return ^(sign | (exponent << 4) | mantissa)
}

// muLawDecodeSample expands one μ-law byte to one 16-bit linear sample.
func muLawDecodeSample(b byte) int16 {
	const bias = 0x84
	b = ^b
	sign := b & 0x80
	exponent := (b >> 4) & 0x07
	mantissa := b & 0x0F
	sample := int16(((int(mantissa) << 3) + bias) << exponent)
	sample -= bias
	if sign != 0 {
		sample = -sample
	}
	return sample
}

// ---- A-law (PCMA) — included for completeness; SIP often offers both ----

// EncodePCMA converts PCM to A-law.
func EncodePCMA(samples []int16) []byte {
	out := make([]byte, len(samples))
	for i, s := range samples {
		out[i] = encodeALawSample(s)
	}
	return out
}

// DecodePCMA converts A-law to PCM.
func DecodePCMA(b []byte) []int16 {
	out := make([]int16, len(b))
	for i, c := range b {
		out[i] = aLawDecodeTable[c]
	}
	return out
}

func encodeALawSample(s int16) byte {
	sign := byte(0)
	v := int32(s)
	if v < 0 {
		v = ^v // one's complement, per the standard
		sign = 0x80
	}
	if v > 32635 {
		v = 32635
	}
	var enc byte
	if v >= 256 {
		exponent := byte(7)
		for mask := int32(0x4000); v&mask == 0 && exponent > 1; mask >>= 1 {
			exponent--
		}
		mantissa := byte((v >> (exponent + 3)) & 0x0F)
		enc = (exponent << 4) | mantissa
	} else {
		enc = byte(v>>4) & 0x0F
	}
	return (sign | enc) ^ 0x55
}

func aLawDecodeSample(b byte) int16 {
	b ^= 0x55
	sign := b & 0x80
	exponent := (b >> 4) & 0x07
	mantissa := b & 0x0F
	var sample int32
	if exponent != 0 {
		sample = int32(mantissa)<<4 + 8 + (1 << 8)
		sample <<= (exponent - 1)
	} else {
		sample = int32(mantissa)<<4 + 8
	}
	if sign != 0 {
		sample = -sample
	}
	return int16(sample)
}
