package extension

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
)

// mockSink collects emitted events for assertion.
type mockSink struct {
	mu     sync.Mutex
	events []event
}

type event struct {
	method  string
	payload map[string]any
}

func (m *mockSink) Emit(method string, payload map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event{method: method, payload: payload})
}

func (m *mockSink) snapshot() []event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]event(nil), m.events...)
}

func contactURI(user, host string, port int) sip.Uri {
	return sip.Uri{Scheme: "sip", User: user, Host: host, Port: port}
}

func newTestRegistry() (*Registry, *mockSink, *time.Time) {
	clock := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	nowPtr := &clock
	r := NewRegistry(func() time.Time { return *nowPtr })
	sink := &mockSink{}
	r.SetEventSink(sink)
	return r, sink, nowPtr
}

func TestRegister_FirstTimeEmitsRegistered(t *testing.T) {
	r, sink, _ := newTestRegistry()
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)

	evs := sink.snapshot()
	if len(evs) != 1 || evs[0].method != EventExtensionRegistered {
		t.Fatalf("want 1 registered event, got %+v", evs)
	}
	if evs[0].payload["extension"] != "1001" {
		t.Errorf("extension payload: %v", evs[0].payload["extension"])
	}
}

func TestRegister_RefreshSameContactNoEvent(t *testing.T) {
	r, sink, now := newTestRegistry()
	c := contactURI("1001", "10.0.0.5", 5060)
	r.Register("1001", c, time.Hour)
	*now = now.Add(time.Minute)
	r.Register("1001", c, time.Hour)

	evs := sink.snapshot()
	if len(evs) != 1 {
		t.Fatalf("refresh with same contact should not re-emit; got %+v", evs)
	}
}

func TestRegister_ChangedContactEmitsReregister(t *testing.T) {
	r, sink, now := newTestRegistry()
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)
	*now = now.Add(time.Minute)
	r.Register("1001", contactURI("1001", "10.0.0.9", 5062), time.Hour)

	evs := sink.snapshot()
	if len(evs) != 3 {
		t.Fatalf("want registered+unregistered+registered, got %+v", evs)
	}
	if evs[1].method != EventExtensionUnregistered || evs[1].payload["reason"] != ReasonReregister {
		t.Errorf("second event: %+v", evs[1])
	}
	if evs[2].method != EventExtensionRegistered {
		t.Errorf("third event: %+v", evs[2])
	}
}

func TestLookup_ExpiredFails(t *testing.T) {
	r, _, now := newTestRegistry()
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)

	if _, ok := r.Lookup("1001"); !ok {
		t.Fatal("expected live binding before expiry")
	}
	*now = now.Add(2 * time.Hour)
	if _, ok := r.Lookup("1001"); ok {
		t.Fatal("expected expired binding to be absent from Lookup")
	}
}

func TestReRegisterAfterExpiryEmitsRegistered(t *testing.T) {
	r, sink, now := newTestRegistry()
	c := contactURI("1001", "10.0.0.5", 5060)
	r.Register("1001", c, time.Hour)
	*now = now.Add(2 * time.Hour) // let it expire
	r.Register("1001", c, time.Hour)

	evs := sink.snapshot()
	if len(evs) != 2 {
		t.Fatalf("want two registered events across expiry, got %+v", evs)
	}
	if evs[1].method != EventExtensionRegistered {
		t.Errorf("second event should be registered: %+v", evs[1])
	}
}

func TestDeregister_RemovesAndEmits(t *testing.T) {
	r, sink, _ := newTestRegistry()
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)
	r.Deregister("1001", ReasonRemoved)

	if _, ok := r.Lookup("1001"); ok {
		t.Fatal("binding should be gone after Deregister")
	}
	evs := sink.snapshot()
	if len(evs) != 2 || evs[1].method != EventExtensionUnregistered || evs[1].payload["reason"] != ReasonRemoved {
		t.Fatalf("want registered+unregistered(removed), got %+v", evs)
	}
}

func TestDeregister_UnknownNoEvent(t *testing.T) {
	r, sink, _ := newTestRegistry()
	r.Deregister("9999", ReasonRemoved)
	if evs := sink.snapshot(); len(evs) != 0 {
		t.Fatalf("deregistering unknown aor should emit nothing, got %+v", evs)
	}
}

func TestList_OnlyLiveBindingsSorted(t *testing.T) {
	r, _, now := newTestRegistry()
	r.Register("1002", contactURI("1002", "10.0.0.6", 5060), time.Hour)
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)
	r.Register("1003", contactURI("1003", "10.0.0.7", 5060), time.Minute) // expires sooner

	*now = now.Add(30 * time.Minute) // 1003 expired, 1001/1002 live
	got := r.List()
	if len(got) != 2 {
		t.Fatalf("want 2 live bindings, got %d: %+v", len(got), got)
	}
	if got[0].Extension != "1001" || got[1].Extension != "1002" {
		t.Errorf("List not sorted by extension: %+v", got)
	}
}

func TestSweeper_RemovesExpiredEmitsUnregistered(t *testing.T) {
	r, sink, now := newTestRegistry()
	r.Register("1001", contactURI("1001", "10.0.0.5", 5060), time.Hour)
	*now = now.Add(2 * time.Hour)

	r.sweepExpired()

	if _, ok := r.Lookup("1001"); ok {
		t.Fatal("sweeper should have removed expired binding")
	}
	evs := sink.snapshot()
	last := evs[len(evs)-1]
	if last.method != EventExtensionUnregistered || last.payload["reason"] != ReasonExpired {
		t.Fatalf("want unregistered(expired), got %+v", last)
	}
}

func TestRunSweeper_ExitsOnContextCancel(t *testing.T) {
	r, _, _ := newTestRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.RunSweeper(ctx, 5*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSweeper did not exit after context cancel")
	}
}

func TestRestore_ExtendsShortLeaseAndEmitsRegistered(t *testing.T) {
	clock := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	nowPtr := &clock
	clockFn := func() time.Time { return *nowPtr }

	src := NewRegistry(clockFn)
	src.Register("1001", contactURI("1001", "198.51.100.54", 21881), 40*time.Second)
	dump := src.List()
	if len(dump) != 1 {
		t.Fatalf("want 1 dumped binding, got %+v", dump)
	}

	*nowPtr = clock.Add(10 * time.Second) // 30s remain — below migrateMinLease
	dst := NewRegistry(clockFn)
	dstSink := &mockSink{}
	dst.SetEventSink(dstSink)

	result := dst.Restore(dump)
	if result.Loaded != 1 || result.Skipped != 0 {
		t.Fatalf("want loaded=1 skipped=0, got %+v", result)
	}
	got, ok := dst.Lookup("1001")
	if !ok {
		t.Fatal("restored binding missing")
	}
	if got.Host != "198.51.100.54" || got.Port != 21881 {
		t.Fatalf("contact mismatch: %+v", got)
	}
	views := dst.List()
	wantExp := nowPtr.Add(migrateMinLease)
	if len(views) != 1 || !views[0].ExpiresAt.Equal(wantExp) {
		t.Fatalf("want ExpiresAt bumped to %v, got %+v", wantExp, views)
	}
	evs := dstSink.snapshot()
	if len(evs) != 1 || evs[0].method != EventExtensionRegistered {
		t.Fatalf("want 1 registered event, got %+v", evs)
	}
}

func TestRestore_SkipsExpiredAndBadContact(t *testing.T) {
	r, sink, now := newTestRegistry()
	result := r.Restore([]BindingView{
		{
			Extension: "gone",
			Contact:   "sip:gone@1.2.3.4:5060",
			ExpiresAt: now.Add(-time.Second),
		},
		{
			Extension: "bad",
			Contact:   "not-a-uri",
			ExpiresAt: now.Add(time.Hour),
		},
		{
			Extension: "ok",
			Contact:   "sip:ok@5.6.7.8:5070",
			ExpiresAt: now.Add(time.Hour),
			LastSeen:  now.Add(-time.Minute),
		},
	})
	if result.Loaded != 1 || result.Skipped != 2 {
		t.Fatalf("want loaded=1 skipped=2, got %+v", result)
	}
	if _, ok := r.Lookup("ok"); !ok {
		t.Fatal("expected ok binding")
	}
	evs := sink.snapshot()
	if len(evs) != 1 || evs[0].method != EventExtensionRegistered {
		t.Fatalf("want registered for loaded binding, got %+v", evs)
	}
}
