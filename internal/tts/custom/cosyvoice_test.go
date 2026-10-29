package custom

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// fakeCosyVoice returns a server that echoes back nSamples of int16 PCM at the
// given source rate, recording the form it received.
func fakeCosyVoice(t *testing.T, nSamples int) (*httptest.Server, *string, *string) {
	t.Helper()
	var gotText, gotSpk string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/inference_sft" {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		_ = r.ParseForm()
		gotText = r.FormValue("tts_text")
		gotSpk = r.FormValue("spk_id")
		buf := make([]byte, nSamples*2)
		for i := 0; i < nSamples; i++ {
			binary.LittleEndian.PutUint16(buf[i*2:], uint16(int16(i)))
		}
		_, _ = w.Write(buf)
	}))
	return srv, &gotText, &gotSpk
}

func drain(ch <-chan tts.PCMChunk) []int16 {
	var out []int16
	for c := range ch {
		out = append(out, c.Samples...)
	}
	return out
}

func TestSynthesize_StreamsChunksBeforeEOF(t *testing.T) {
	var gotChunks int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Two HTTP body chunks: first fills one emit window, second completes.
		chunk := make([]byte, streamEmitSrcSamples*2)
		for i := 0; i < streamEmitSrcSamples; i++ {
			binary.LittleEndian.PutUint16(chunk[i*2:], uint16(int16(i)))
		}
		_, _ = w.Write(chunk)
		_, _ = w.Write(chunk)
	}))
	defer srv.Close()

	p, err := New(Config{BaseURL: srv.URL, SourceSampleRate: 24000})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := p.Synthesize(context.Background(), "流式", tts.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for c := range ch {
		if len(c.Samples) > 0 {
			gotChunks++
		}
	}
	if gotChunks < 2 {
		t.Fatalf("chunks with samples = %d, want >= 2 (streaming)", gotChunks)
	}
}

func TestSynthesize_PrebufferOnlyDelaysFirstChunk(t *testing.T) {
	const sourceRate = 24000
	const initialSamples = sourceRate * 3 / 4
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pcm := make([]byte, initialSamples*2)
		for i := 0; i < initialSamples; i++ {
			binary.LittleEndian.PutUint16(pcm[i*2:], uint16(int16(i)))
		}
		_, _ = w.Write(pcm)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}))
	defer srv.Close()

	p, err := New(Config{
		BaseURL:          srv.URL,
		SourceSampleRate: sourceRate,
		PrebufferMs:      400,
		MaxVowelHoldMs:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := p.Synthesize(context.Background(), "预缓冲", tts.Options{})
	if err != nil {
		t.Fatal(err)
	}

	// Once the 400 ms startup threshold is met, all already-arrived audio must
	// keep flowing in small chunks. Holding it until another 400 ms arrives
	// discards the jitter cushion and makes paced telephony playback underrun.
	wantBeforeBackendResumes := targetSampleRate * 3 / 5
	got := 0
	deadline := time.After(500 * time.Millisecond)
	for got < wantBeforeBackendResumes {
		select {
		case chunk := <-chunks:
			got += len(chunk.Samples)
		case <-deadline:
			close(release)
			for range chunks {
			}
			t.Fatalf("samples emitted before backend resumed = %d, want >= %d", got, wantBeforeBackendResumes)
		}
	}
	close(release)
	for range chunks {
	}
}

func TestSynthesize_ResamplesTo16k(t *testing.T) {
	srv, gotText, gotSpk := fakeCosyVoice(t, 2400) // 0.1s @ 24000
	defer srv.Close()

	p, err := New(Config{BaseURL: srv.URL, SourceSampleRate: 24000, DefaultVoice: "中文女"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := p.Synthesize(context.Background(), "你好", tts.Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(ch)

	// 2400 samples @24k → ~1600 @16k. Allow a couple samples of slack: the
	// stream is resampled per emitted chunk, so chunk boundaries lose sub-sample
	// fractions to rounding (negligible: <1ms). The check verifies the 2/3 ratio,
	// not an exact count.
	if len(got) < 1597 || len(got) > 1600 {
		t.Fatalf("resampled length = %d, want ~1600", len(got))
	}
	if *gotText != "你好" {
		t.Errorf("tts_text = %q, want 你好", *gotText)
	}
	if *gotSpk != "中文女" {
		t.Errorf("spk_id = %q, want 中文女", *gotSpk)
	}
}

func TestSynthesize_VoiceOverride(t *testing.T) {
	srv, _, gotSpk := fakeCosyVoice(t, 16)
	defer srv.Close()

	p, _ := New(Config{BaseURL: srv.URL})
	ch, err := p.Synthesize(context.Background(), "hi", tts.Options{Voice: "英文男"})
	if err != nil {
		t.Fatal(err)
	}
	drain(ch)
	if *gotSpk != "英文男" {
		t.Errorf("spk_id = %q, want 英文男 (Options.Voice override)", *gotSpk)
	}
}

func TestSynthesize_SameRateNoResample(t *testing.T) {
	srv, _, _ := fakeCosyVoice(t, 320)
	defer srv.Close()

	p, _ := New(Config{BaseURL: srv.URL, SourceSampleRate: 16000})
	ch, _ := p.Synthesize(context.Background(), "hi", tts.Options{})
	if got := drain(ch); len(got) != 320 {
		t.Fatalf("length = %d, want 320 (no resample at 16k)", len(got))
	}
}

func TestSynthesize_Errors(t *testing.T) {
	p, _ := New(Config{BaseURL: "http://127.0.0.1:1"})
	if _, err := p.Synthesize(context.Background(), "", tts.Options{}); err != tts.ErrEmptyText {
		t.Errorf("empty text err = %v, want ErrEmptyText", err)
	}
	_ = p.Close()
	if _, err := p.Synthesize(context.Background(), "hi", tts.Options{}); err != tts.ErrClosed {
		t.Errorf("closed err = %v, want ErrClosed", err)
	}
}

func TestSynthesize_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	p, _ := New(Config{BaseURL: srv.URL})
	if _, err := p.Synthesize(context.Background(), "hi", tts.Options{}); err == nil {
		t.Error("expected error on HTTP 500")
	}
}

func TestSynthesize_TruncatesHeldVowel(t *testing.T) {
	const sr = 24000
	pcm := makeTone(3000, sr, 200, 6000)
	buf := make([]byte, len(pcm)*2)
	for i, v := range pcm {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(v))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(buf)
	}))
	defer srv.Close()

	p, err := New(Config{
		BaseURL:            srv.URL,
		SourceSampleRate:   sr,
		TrimLeadingSilence: false,
		MaxVowelHoldMs:     700,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(mustSynth(t, p, "您好，请问有什么可以帮您？"))
	gotMs := len(got) * 1000 / targetSampleRate
	if gotMs > 1200 {
		t.Fatalf("played_ms=%d after resample, want <= 1200 (3s hold cut to ~700ms)", gotMs)
	}
	if gotMs < 400 {
		t.Fatalf("played_ms=%d, hold gate over-trimmed", gotMs)
	}
}

func mustSynth(t *testing.T, p *Provider, text string) <-chan tts.PCMChunk {
	t.Helper()
	ch, err := p.Synthesize(context.Background(), text, tts.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestNew_RequiresBaseURL(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("expected error when BaseURL empty")
	}
}

func TestResampleLinear_Downsample(t *testing.T) {
	in := make([]int16, 441)
	got := resampleLinear(in, 22050, 16000)
	if want := 441 * 16000 / 22050; len(got) != want {
		t.Fatalf("len = %d, want %d", len(got), want)
	}
}
