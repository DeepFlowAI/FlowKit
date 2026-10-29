package recording

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeAppender struct {
	mu      sync.Mutex
	got     []byte
	delay   time.Duration
	failFor int
}

func (f *fakeAppender) append(_ context.Context, b []byte) error {
	time.Sleep(f.delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFor > 0 {
		f.failFor--
		return errors.New("transient")
	}
	f.got = append(f.got, b...)
	return nil
}

// A slow storage append must not stop the queue from draining: producers
// enqueue without blocking and drop on a full queue, exactly like the recorder.
func TestRunUploader_SlowAppendDoesNotDropChunks(t *testing.T) {
	uploadCh := make(chan []byte, uploaderQueueDepth)
	done := make(chan struct{})
	app := &fakeAppender{delay: 150 * time.Millisecond}
	finished := make(chan struct{})
	go func() {
		runUploader(done, 10*time.Millisecond, uploadCh, app.append, nil)
		close(finished)
	}()

	var want []byte
	dropped := 0
	for i := 0; i < 50; i++ {
		chunk := []byte{byte(i)}
		want = append(want, chunk...)
		select {
		case uploadCh <- chunk:
		default:
			dropped++
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(done)
	<-finished

	if dropped != 0 {
		t.Fatalf("dropped %d chunks while an append was in flight", dropped)
	}
	if !bytes.Equal(app.got, want) {
		t.Fatalf("uploaded %v, want %v", app.got, want)
	}
}

func TestRunUploader_RetriesFailedAppendInOrder(t *testing.T) {
	uploadCh := make(chan []byte, uploaderQueueDepth)
	done := make(chan struct{})
	app := &fakeAppender{failFor: 2}
	finished := make(chan struct{})
	go func() {
		runUploader(done, 10*time.Millisecond, uploadCh, app.append, nil)
		close(finished)
	}()

	want := []byte("abcdef")
	for _, c := range want {
		uploadCh <- []byte{c}
		time.Sleep(15 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(done)
	<-finished

	if !bytes.Equal(app.got, want) {
		t.Fatalf("uploaded %q, want %q", app.got, want)
	}
}
