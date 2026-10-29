package recording

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

func countNonDirFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info != nil && !info.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return n
}

// When the call has already ended, Begin must not open storage or fire hooks.
// Opening the sink first races with short unanswered legs and leaves an empty
// object plus a false recording.completed event.
func TestRecorder_Begin_SkipsStorageWhenBusClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := takeoverTestConfig(dir)
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	call := session.NewCall("c1", session.DirectionOutbound, nil)
	_ = call.Hangup("customer_no_answer")

	var started, completed atomic.Int32
	rec := NewRecorder(cfg, factory, call, nil)
	rec.hooks = &Hooks{
		OnStarted:   func(RecordingStarted) { started.Add(1) },
		OnCompleted: func(RecordingCompleted) { completed.Add(1) },
	}

	err = rec.Begin(context.Background())
	if !errors.Is(err, errRecordingNoBus) {
		t.Fatalf("Begin() = %v, want errRecordingNoBus", err)
	}
	if n := countNonDirFiles(t, dir); n != 0 {
		t.Fatalf("created %d storage files under %s, want 0", n, dir)
	}
	if started.Load() != 0 || completed.Load() != 0 {
		t.Fatalf("hooks fired: started=%d completed=%d, want 0/0",
			started.Load(), completed.Load())
	}
}

func TestBridgeRecorder_Begin_SkipsStorageWhenBusClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := takeoverTestConfig(dir)
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	user := session.NewCall("u1", session.DirectionInbound, nil)
	agent := session.NewCall("a1", session.DirectionOutbound, nil)
	_ = user.Hangup("customer_no_answer")
	_ = agent.Hangup("peer_gone")

	var started, completed atomic.Int32
	br := NewBridgeRecorder(cfg, factory, user, agent, nil)
	br.hooks = &Hooks{
		OnStarted:   func(RecordingStarted) { started.Add(1) },
		OnCompleted: func(RecordingCompleted) { completed.Add(1) },
	}

	br.Begin(context.Background())

	if n := countNonDirFiles(t, dir); n != 0 {
		t.Fatalf("created %d storage files under %s, want 0", n, dir)
	}
	if started.Load() != 0 || completed.Load() != 0 {
		t.Fatalf("hooks fired: started=%d completed=%d, want 0/0",
			started.Load(), completed.Load())
	}
}
