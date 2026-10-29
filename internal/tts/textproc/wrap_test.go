package textproc

import (
	"context"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// fakeProvider replays scripted attempts: each Synthesize call emits the next
// attempt's chunks and records the text it received.
type fakeProvider struct {
	attempts [][]tts.PCMChunk
	texts    []string
}

func (f *fakeProvider) Synthesize(ctx context.Context, text string, _ tts.Options) (<-chan tts.PCMChunk, error) {
	f.texts = append(f.texts, text)
	chunks := f.attempts[len(f.texts)-1]
	out := make(chan tts.PCMChunk)
	go func() {
		defer close(out)
		for _, c := range chunks {
			select {
			case out <- c:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (f *fakeProvider) Close() error { return nil }

// pcm builds ms milliseconds of 16 kHz audio: a loud square wave or silence.
func pcm(ms int, loud bool) []int16 {
	s := make([]int16, ms*16)
	if loud {
		for i := range s {
			s[i] = 3000
			if i%2 == 1 {
				s[i] = -3000
			}
		}
	}
	return s
}

func cat(parts ...[]int16) []int16 {
	var out []int16
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func collect(t *testing.T, p tts.Provider, text string) (samples int, finals int) {
	t.Helper()
	ch, err := p.Synthesize(context.Background(), text, tts.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for c := range ch {
		samples += len(c.Samples)
		if c.Final {
			finals++
		}
	}
	return samples, finals
}

func TestWrapProcessesText(t *testing.T) {
	f := &fakeProvider{attempts: [][]tts.PCMChunk{{{Samples: pcm(100, true), Final: true}}}}
	w := Wrap(f, &Processor{Model: "m"}, Guard{})
	collect(t, w, "**工号**1001")
	if f.texts[0] != "工号幺零零幺" {
		t.Errorf("provider got %q", f.texts[0])
	}
	if _, err := w.Synthesize(context.Background(), "**", tts.Options{}); err != tts.ErrEmptyText {
		t.Errorf("markdown-only text: err = %v, want ErrEmptyText", err)
	}
}

func TestWrapShortensLongPause(t *testing.T) {
	audio := cat(pcm(300, false), pcm(200, true), pcm(2000, false), pcm(200, true))
	f := &fakeProvider{attempts: [][]tts.PCMChunk{{
		{Samples: audio[:len(audio)/2]}, {Samples: audio[len(audio)/2:], Final: true},
	}}}
	got, finals := collect(t, Wrap(f, &Processor{}, Guard{MaxSilenceMs: 500}), "测试")
	want := (300 + 200 + 500 + 200) * 16 // lead silence kept, inner pause capped at 500 ms
	if got != want || finals != 1 {
		t.Errorf("samples = %d (want %d), finals = %d", got, want, finals)
	}
}

func TestWrapCutsOverLength(t *testing.T) {
	f := &fakeProvider{attempts: [][]tts.PCMChunk{{
		{Samples: pcm(1000, true)}, {Samples: pcm(1000, true)}, {Samples: pcm(1000, true), Final: true},
	}}}
	// 2 chars → limit 1000 + 2×300 = 1600 ms
	got, finals := collect(t, Wrap(f, &Processor{}, Guard{BaseMs: 1000, PerCharMs: 300}), "你好")
	if got != 1600*16 || finals != 1 {
		t.Errorf("samples = %d (want %d), finals = %d", got, 1600*16, finals)
	}
}

func TestWrapRetriesEmptySynthesis(t *testing.T) {
	f := &fakeProvider{attempts: [][]tts.PCMChunk{
		{{Final: true}}, // backend returned nothing
		{{Samples: pcm(100, true), Final: true}},
	}}
	got, finals := collect(t, Wrap(f, &Processor{}, Guard{}), "你好")
	if got != 100*16 || finals != 1 || len(f.texts) != 2 {
		t.Errorf("samples = %d, finals = %d, attempts = %d", got, finals, len(f.texts))
	}
}

func TestWrapEmptyAfterRetryEndsCleanly(t *testing.T) {
	f := &fakeProvider{attempts: [][]tts.PCMChunk{{}, {}}}
	got, _ := collect(t, Wrap(f, &Processor{}, Guard{}), "你好")
	if got != 0 || len(f.texts) != 2 {
		t.Errorf("samples = %d, attempts = %d", got, len(f.texts))
	}
}
