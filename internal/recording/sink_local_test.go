package recording

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

func TestLocalSink_WriteAndPatchHeader(t *testing.T) {
	dir := t.TempDir()
	factory, err := newLocalSinkFactory(LocalConfig{Dir: dir}, "test/{call_id}.wav", nil)
	if err != nil {
		t.Fatal(err)
	}
	meta := ObjectMeta{CallID: "c1", ConversationID: "conv1", Date: "2026-05-27"}
	sink, err := factory.NewSink(context.Background(), meta)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	hdr := BuildWAVHeaderPlaceholderForLayout(LayoutStereo)
	if err := sink.Open(ctx, meta, hdr); err != nil {
		t.Fatal(err)
	}
	pcm := make([]byte, bus.SamplesPerFrame*4)
	if err := sink.Append(ctx, pcm); err != nil {
		t.Fatal(err)
	}
	url, err := sink.Close(ctx, PatchWAVHeaderForLayout(LayoutStereo, uint32(len(pcm))), uint32(len(pcm)))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test", "c1.wav")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file missing: %v url=%s", err, url)
	}
}
