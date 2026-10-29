package llm

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// scriptProvider streams a fixed set of deltas then closes tokenCh.
type scriptProvider struct{ deltas []string }

func (scriptProvider) Name() string  { return "fake" }
func (scriptProvider) Model() string { return "fake" }
func (p scriptProvider) StreamChat(ctx context.Context, _ RequestMeta, _ []Message, tokenCh chan<- string) error {
	defer close(tokenCh)
	for _, d := range p.deltas {
		select {
		case tokenCh <- d:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type fakePrepared struct{ text string }

func (fakePrepared) Close() {}

// recordingSink records the order of Prepare/Play events and simulates a slow
// playback so prefetch (Prepare of the next sentence during Play of the current
// one) is observable.
type recordingSink struct {
	mu      sync.Mutex
	events  []string
	playDur time.Duration
}

func (s *recordingSink) record(ev string) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}

func (s *recordingSink) Prepare(text string) (PreparedSpeech, error) {
	s.record("prepare:" + text)
	return fakePrepared{text: text}, nil
}

func (s *recordingSink) Play(p PreparedSpeech) error {
	pp := p.(fakePrepared)
	s.record("play-start:" + pp.text)
	time.Sleep(s.playDur)
	s.record("play-end:" + pp.text)
	return nil
}

func (s *recordingSink) PadTurnTail() { s.record("pad-tail") }

func (s *recordingSink) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// bargingSink cancels the turn from inside Play, standing in for a caller who
// interrupts the reply.
type bargingSink struct {
	recordingSink
	cancel context.CancelFunc
}

func (s *bargingSink) Play(PreparedSpeech) error {
	s.record("play-interrupted")
	s.cancel()
	return context.Canceled
}

// gatedPrepared reports itself audible only once release is closed, standing in
// for a TTS backend that has not yet returned its first audio.
type gatedPrepared struct {
	text    string
	release chan struct{}
}

func (gatedPrepared) Close() {}

func (p gatedPrepared) WaitAudible(ctx context.Context, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.release:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// gatingSink hands out gatedPrepared handles whose audible signal the test
// controls, and records the order of Prepare calls.
type gatingSink struct {
	mu      sync.Mutex
	events  []string
	release chan struct{}
}

func (s *gatingSink) Prepare(text string) (PreparedSpeech, error) {
	s.mu.Lock()
	s.events = append(s.events, "prepare:"+text)
	s.mu.Unlock()
	return gatedPrepared{text: text, release: s.release}, nil
}

func (s *gatingSink) Play(PreparedSpeech) error { return nil }

func (s *gatingSink) prepared() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func TestPrepareSentencesHoldsBackUntilOpeningIsAudible(t *testing.T) {
	sink := &gatingSink{release: make(chan struct{})}
	sentenceCh := make(chan string, 3)
	for _, s := range []string{"甲。", "乙。", "丙。"} {
		sentenceCh <- s
	}
	close(sentenceCh)
	preparedCh := make(chan PreparedSpeech, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		prepareSentences(context.Background(), sentenceCh, preparedCh, sink, func(error) {})
	}()
	// Drain so the depth-1 buffer never becomes the reason work stalls.
	go func() {
		for range preparedCh {
		}
	}()

	// While the opening sentence is still silent, no later synthesis may start:
	// the backend serialises, so a second request would delay the first.
	time.Sleep(50 * time.Millisecond)
	if got := sink.prepared(); len(got) != 1 || got[0] != "prepare:甲。" {
		t.Fatalf("expected only the opening sentence prepared, got %v", got)
	}

	close(sink.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prepareSentences did not finish after the opening sentence became audible")
	}
	if got := sink.prepared(); len(got) != 3 {
		t.Fatalf("expected all three sentences prepared, got %v", got)
	}
}

// synthGatedPrepared is audible at once but reports its synthesis finished
// only once synthDone is closed.
type synthGatedPrepared struct {
	gatedPrepared
	synthDone chan struct{}
}

func (p synthGatedPrepared) WaitSynthesized(ctx context.Context, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.synthDone:
	case <-timer.C:
	case <-ctx.Done():
	}
}

type synthGatingSink struct {
	gatingSink
	synthDone chan struct{}
}

func (s *synthGatingSink) Prepare(text string) (PreparedSpeech, error) {
	s.mu.Lock()
	s.events = append(s.events, "prepare:"+text)
	s.mu.Unlock()
	return synthGatedPrepared{gatedPrepared: gatedPrepared{text: text, release: s.release}, synthDone: s.synthDone}, nil
}

func TestPrepareSentencesHoldsBackUntilOpeningIsSynthesized(t *testing.T) {
	// Call c0a83e13: the next two sentences started synthesizing as soon as the
	// opening one was audible, and its stream underran mid-word ("动").
	release := make(chan struct{})
	close(release)
	sink := &synthGatingSink{gatingSink: gatingSink{release: release}, synthDone: make(chan struct{})}
	sentenceCh := make(chan string, 3)
	for _, s := range []string{"甲。", "乙。", "丙。"} {
		sentenceCh <- s
	}
	close(sentenceCh)
	preparedCh := make(chan PreparedSpeech, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		prepareSentences(context.Background(), sentenceCh, preparedCh, sink, func(error) {})
	}()
	go func() {
		for range preparedCh {
		}
	}()

	time.Sleep(50 * time.Millisecond)
	if got := sink.prepared(); len(got) != 1 {
		t.Fatalf("expected only the opening sentence prepared while it synthesizes, got %v", got)
	}

	close(sink.synthDone)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("prepareSentences did not finish after the opening sentence was synthesized")
	}
	if got := sink.prepared(); len(got) != 3 {
		t.Fatalf("expected all three sentences prepared, got %v", got)
	}
}

func TestRunCallTurnPrefetchesNextSentence(t *testing.T) {
	h := &ChatHandlers{
		client:   scriptProvider{deltas: []string{"甲。乙。丙。"}},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		sessions: make(map[string]*chatSession),
	}
	sink := &recordingSink{playDur: 50 * time.Millisecond}

	if err := h.RunCallTurn(context.Background(), "c1", "你好", sink); err != nil {
		t.Fatalf("RunCallTurn: %v", err)
	}

	idx := map[string]int{}
	for i, e := range sink.events {
		idx[e] = i
	}
	for _, e := range []string{"prepare:乙。", "play-end:甲。", "play-start:甲。", "play-start:乙。", "play-start:丙。"} {
		if _, ok := idx[e]; !ok {
			t.Fatalf("missing event %q; got %v", e, sink.events)
		}
	}

	// The next sentence is synthesized before the current one finishes playing.
	if idx["prepare:乙。"] > idx["play-end:甲。"] {
		t.Fatalf("expected prepare of 乙。 before play-end of 甲。; events=%v", sink.events)
	}
	// Playback stays strictly sequential.
	if idx["play-start:甲。"] >= idx["play-start:乙。"] || idx["play-start:乙。"] >= idx["play-start:丙。"] {
		t.Fatalf("expected sequential playback; events=%v", sink.events)
	}
}

func TestRunCallTurnPadsTheTailOnceTheWholeReplyHasPlayed(t *testing.T) {
	h := &ChatHandlers{
		client:   scriptProvider{deltas: []string{"甲。乙。丙。"}},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		sessions: make(map[string]*chatSession),
	}
	sink := &recordingSink{}

	if err := h.RunCallTurn(context.Background(), "c1", "你好", sink); err != nil {
		t.Fatalf("RunCallTurn: %v", err)
	}

	events := sink.recorded()
	pads := 0
	for _, e := range events {
		if e == "pad-tail" {
			pads++
		}
	}
	// One pad for the turn, not one per sentence: padding between sentences
	// would drop silence into the middle of a reply.
	if pads != 1 {
		t.Fatalf("expected exactly one tail pad, got %d; events=%v", pads, events)
	}
	if events[len(events)-1] != "pad-tail" {
		t.Fatalf("expected the pad after the last sentence, got %v", events)
	}
}

func TestRunCallTurnSkipsTheTailPadWhenBargedIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &ChatHandlers{
		client:   scriptProvider{deltas: []string{"甲。乙。丙。"}},
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		sessions: make(map[string]*chatSession),
	}
	sink := &bargingSink{cancel: cancel}

	_ = h.RunCallTurn(ctx, "c1", "你好", sink)

	// The caller wants silence, so trailing silence is beside the point — and
	// the stop path has already cleared the queue it would be written to.
	for _, e := range sink.recorded() {
		if e == "pad-tail" {
			t.Fatalf("expected no tail pad after a barge-in; events=%v", sink.recorded())
		}
	}
}

func TestStreamSentencesPadsTheTailOnceTheWholeReplyHasPlayed(t *testing.T) {
	// StreamSentences is the converse-agent twin of RunCallTurn's pipeline;
	// both must pad once per turn, never per sentence.
	sink := &recordingSink{}
	tokens := make(chan string, 1)
	tokens <- "甲。乙。丙。"
	close(tokens)

	if _, err := StreamSentences(context.Background(), tokens, sink, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("StreamSentences: %v", err)
	}

	events := sink.recorded()
	pads := 0
	for _, e := range events {
		if e == "pad-tail" {
			pads++
		}
	}
	if pads != 1 {
		t.Fatalf("expected exactly one tail pad, got %d; events=%v", pads, events)
	}
	if events[len(events)-1] != "pad-tail" {
		t.Fatalf("expected the pad after the last sentence, got %v", events)
	}
}

func TestStreamSentencesSkipsTheTailPadWhenBargedIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &bargingSink{cancel: cancel}

	tokens := make(chan string, 1)
	tokens <- "甲。乙。丙。"
	close(tokens)

	_, _ = StreamSentences(ctx, tokens, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, e := range sink.recorded() {
		if e == "pad-tail" {
			t.Fatalf("expected no tail pad after a barge-in; events=%v", sink.recorded())
		}
	}
}
