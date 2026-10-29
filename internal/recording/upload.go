package recording

import (
	"context"
	"log/slog"
	"time"
)

// runUploader drains uploadCh and appends to sink until done is closed.
//
// Appends run in the background, one at a time so the object grows in order,
// while this loop keeps draining uploadCh into pending. uploadCh is shallow;
// a storage round trip longer than its depth would otherwise make producers
// drop audio and cut holes into the recording. A failed append is put back in
// front of pending so a later tick retries it.
func runUploader(
	done <-chan struct{},
	interval time.Duration,
	uploadCh <-chan []byte,
	appendFn func(context.Context, []byte) error,
	log *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var pending, inflightBuf []byte
	var inflight chan error // nil while no append is running
	appendOnce := func(b []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		return appendFn(ctx, b)
	}
	start := func() {
		if inflight != nil || len(pending) == 0 {
			return
		}
		inflightBuf, pending = pending, nil
		ch := make(chan error, 1)
		inflight = ch
		go func(b []byte) { ch <- appendOnce(b) }(inflightBuf)
	}
	finish := func(err error) {
		if err != nil {
			if log != nil {
				log.Warn("recording: append failed", "err", err, "pending_bytes", len(inflightBuf))
			}
			pending = append(inflightBuf, pending...)
		}
		inflight, inflightBuf = nil, nil
	}

	closed := false
	for !closed {
		select {
		case <-done:
			closed = true
		case chunk := <-uploadCh:
			pending = append(pending, chunk...)
		case <-ticker.C:
			start()
		case err := <-inflight:
			finish(err)
		}
	}
	if inflight != nil {
		finish(<-inflight)
	}
	for {
		select {
		case chunk := <-uploadCh:
			pending = append(pending, chunk...)
		default:
			if len(pending) > 0 {
				if err := appendOnce(pending); err != nil && log != nil {
					log.Warn("recording: append failed", "err", err, "pending_bytes", len(pending))
				}
			}
			return
		}
	}
}
