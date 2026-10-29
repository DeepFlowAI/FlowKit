package denoise

import "math"

const (
	nfft = 512
	hop  = 256
	bins = nfft/2 + 1 // 257 one-sided bins
)

// fft512 is a fixed-size iterative radix-2 complex FFT. The model runs one
// 512-point transform per 16 ms hop, which is negligible next to the inference
// itself, so the real input is simply carried as a complex signal instead of
// being packed into a half-length transform.
type fft512 struct {
	re, im [nfft]float64
	cos    [nfft / 2]float64
	sin    [nfft / 2]float64
	rev    [nfft]int
}

func newFFT512() *fft512 {
	f := &fft512{}
	for i := range f.cos {
		f.cos[i] = math.Cos(2 * math.Pi * float64(i) / float64(nfft))
		f.sin[i] = math.Sin(2 * math.Pi * float64(i) / float64(nfft))
	}
	shift := 0
	for n := nfft; n > 1; n >>= 1 {
		shift++
	}
	for i := range f.rev {
		r, v := 0, i
		for b := 0; b < shift; b++ {
			r = r<<1 | v&1
			v >>= 1
		}
		f.rev[i] = r
	}
	return f
}

// run transforms re/im in place. inverse conjugates the twiddles and scales by
// 1/nfft.
func (f *fft512) run(inverse bool) {
	for i, j := range f.rev {
		if i < j {
			f.re[i], f.re[j] = f.re[j], f.re[i]
			f.im[i], f.im[j] = f.im[j], f.im[i]
		}
	}
	for size := 2; size <= nfft; size <<= 1 {
		step := nfft / size
		for start := 0; start < nfft; start += size {
			for k := 0; k < size/2; k++ {
				c, s := f.cos[k*step], -f.sin[k*step]
				if inverse {
					s = -s
				}
				i, j := start+k, start+k+size/2
				tr := f.re[j]*c - f.im[j]*s
				ti := f.re[j]*s + f.im[j]*c
				f.re[j] = f.re[i] - tr
				f.im[j] = f.im[i] - ti
				f.re[i] += tr
				f.im[i] += ti
			}
		}
	}
	if inverse {
		for i := range f.re {
			f.re[i] /= float64(nfft)
			f.im[i] /= float64(nfft)
		}
	}
}

// forward transforms 512 real samples into the 257 one-sided bins, written to
// dst as interleaved re/im pairs — the layout the model takes.
func (f *fft512) forward(in []float32, dst []float32) {
	for i := 0; i < nfft; i++ {
		f.re[i] = float64(in[i])
		f.im[i] = 0
	}
	f.run(false)
	for k := 0; k < bins; k++ {
		dst[2*k] = float32(f.re[k])
		dst[2*k+1] = float32(f.im[k])
	}
}

// inverse transforms 257 interleaved bins back into 512 real samples, filling
// the redundant upper half by conjugate symmetry.
func (f *fft512) inverse(src []float32, out []float32) {
	for k := 0; k < bins; k++ {
		f.re[k] = float64(src[2*k])
		f.im[k] = float64(src[2*k+1])
	}
	for k := bins; k < nfft; k++ {
		m := nfft - k
		f.re[k] = f.re[m]
		f.im[k] = -f.im[m]
	}
	f.run(true)
	for i := 0; i < nfft; i++ {
		out[i] = float32(f.re[i])
	}
}
