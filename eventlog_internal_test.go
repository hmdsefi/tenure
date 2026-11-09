// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// logSink is the delivery side of an event log under test: a JSON log handler whose records it
// keeps, and an OnEvent that records its calls. Records and calls are stamped from one counter,
// so their order is exact. A gate can hold OnEvent calls.
type logSink struct {
	stamp atomic.Uint64

	mu      sync.Mutex
	records []sinkRecord
	calls   []sinkCall
	in      int           // OnEvent calls in progress
	most    int           // the most OnEvent calls in progress at once
	gate    chan struct{} // when not nil, OnEvent calls with an index >= from wait for it
	from    int
	wgate   chan struct{} // when not nil, Write waits for it
}

// sinkRecord is one log record: its JSON object without the time, and its stamp.
type sinkRecord struct {
	line  string
	stamp uint64
}

// sinkCall is one OnEvent call: its event and the stamps of its start and end.
type sinkCall struct {
	e          Event
	start, end uint64
}

// Write keeps one record: the JSON handler writes each with one Write.
func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	wgate := s.wgate
	s.mu.Unlock()
	if wgate != nil {
		<-wgate
	}
	st := s.stamp.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, sinkRecord{line: strings.TrimSuffix(string(p), "\n"), stamp: st})
	return len(p), nil
}

// logger returns a logger of every level that writes to s, with no time.
func (s *logSink) logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(s, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
}

// onEvent is the OnEvent of the log under test.
func (s *logSink) onEvent(e Event) {
	start := s.stamp.Add(1)
	s.mu.Lock()
	s.in++
	s.most = max(s.most, s.in)
	i := len(s.calls)
	s.calls = append(s.calls, sinkCall{e: e, start: start})
	gate := s.gate
	if i < s.from {
		gate = nil
	}
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	end := s.stamp.Add(1)
	s.mu.Lock()
	s.in--
	s.calls[i].end = end
	s.mu.Unlock()
}

// hold makes the OnEvent calls from index from on (0-based) wait until release is called.
func (s *logSink) hold(from int) (release func()) {
	gate := make(chan struct{})
	s.mu.Lock()
	s.gate, s.from = gate, from
	s.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

// snapshot returns copies of the records and the calls.
func (s *logSink) snapshot() ([]sinkRecord, []sinkCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sinkRecord(nil), s.records...), append([]sinkCall(nil), s.calls...)
}

// newTestLog returns an event log of member "a" delivering to s, started with holder "a/1". Its
// delivery is stopped at cleanup.
func newTestLog(t *testing.T, s *logSink) *eventLog {
	t.Helper()
	l := newEventLog("a", s.logger(), s.onEvent)
	l.start("a/1")
	t.Cleanup(l.stop)
	return l
}

// stopLog stops l's delivery and fails t unless, once every goroutine of the bubble is blocked,
// no delivery goroutine is left.
func stopLog(t *testing.T, l *eventLog) {
	t.Helper()
	l.stop()
	synctest.Wait()
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	if c := strings.Count(string(buf[:n]), "tenure.(*eventLog).run("); c != 0 {
		t.Errorf("%d delivery goroutine(s) left after stop, want 0", c)
	}
}

// eventKinds are the kinds the tests here emit, in turn.
var eventKinds = []EventKind{EventNodeDraining, EventShardAcquired, EventViewChanged, EventNodeClosed}

// testEvent returns the i-th event the tests here emit: its kind from eventKinds and its fields
// from i.
func testEvent(i int) Event {
	return Event{Kind: eventKinds[i%len(eventKinds)], Space: "s", Shard: i % 2, Slot: -1, Term: 7, Count: i, Reason: "drained"}
}

// TestEventLogOrder checks that 10 000 events emitted by 8 goroutines reach OnEvent with Seq 1 to
// 10 000 in order, one call at a time, each right after its log record and after the previous
// call has returned.
func TestEventLogOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const writers, each = 8, 1250
		var s logSink
		l := newTestLog(t, &s)
		var wg sync.WaitGroup
		for w := range writers {
			wg.Go(func() {
				for i := range each {
					l.emit(testEvent(w*each + i))
					if i%100 == 0 {
						time.Sleep(time.Millisecond) // let the others and the delivery run
					}
				}
			})
		}
		wg.Wait()
		l.waitDelivered(context.Background(), writers*each)

		records, calls := s.snapshot()
		if len(calls) != writers*each || len(records) != writers*each {
			t.Fatalf("got %d calls and %d records, want %d of each", len(calls), len(records), writers*each)
		}
		if s.most != 1 {
			t.Errorf("at most %d OnEvent calls at once, want 1", s.most)
		}
		for i, c := range calls {
			if want := uint64(i + 1); c.e.Seq != want {
				t.Fatalf("call %d has Seq %d, want %d", i, c.e.Seq, want)
			}
			if r := records[i]; r.stamp >= c.start || c.start >= c.end {
				t.Fatalf("call %d (Seq %d): record at %d, call from %d to %d; want the record first", i, c.e.Seq, r.stamp, c.start, c.end)
			}
			if i > 0 && calls[i-1].end > records[i].stamp {
				t.Fatalf("record %d at %d, before call %d returned at %d", i, records[i].stamp, i-1, calls[i-1].end)
			}
			var rec struct{ Msg string }
			if err := json.Unmarshal([]byte(records[i].line), &rec); err != nil || rec.Msg != kinds[c.e.Kind].msg {
				t.Fatalf("record %d = %s (%v), want the message %q", i, records[i].line, err, kinds[c.e.Kind].msg)
			}
		}
		stopLog(t, l)
	})
}

// TestEventLogNeverWaits checks that emit returns at once while OnEvent is blocked, queueing
// every event, and that the queued events are delivered in order once it returns.
func TestEventLogNeverWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var s logSink
		release := s.hold(0)
		l := newTestLog(t, &s)
		l.emit(testEvent(0))
		synctest.Wait() // OnEvent is now blocked on the first event

		t0 := time.Now()
		const n = 5000
		for i := 1; i < n; i++ {
			if got := l.emit(testEvent(i)); got != uint64(i+1) {
				t.Fatalf("emit #%d returned Seq %d, want %d", i+1, got, i+1)
			}
		}
		if d := time.Since(t0); d != 0 {
			t.Errorf("emitting took %v, want 0", d)
		}
		if _, calls := s.snapshot(); len(calls) != 1 {
			t.Errorf("%d OnEvent calls while the first is blocked, want 1", len(calls))
		}
		if r := l.recent(); len(r) != ringSize || r[ringSize-1].Seq != n {
			t.Errorf("recent() ends at Seq %d with %d events, want %d events ending at %d", r[len(r)-1].Seq, len(r), ringSize, n)
		}

		release()
		l.waitDelivered(context.Background(), n)
		_, calls := s.snapshot()
		if len(calls) != n {
			t.Fatalf("%d OnEvent calls, want %d", len(calls), n)
		}
		for i, c := range calls {
			if c.e.Seq != uint64(i+1) {
				t.Fatalf("call %d has Seq %d, want %d", i, c.e.Seq, i+1)
			}
		}
		stopLog(t, l)
	})
}

// TestEventLogRing checks RecentEvents' ring: oldest first, at most the newest 256, a copy, and
// with the events stop dropped from the queue; emit after stop does nothing.
func TestEventLogRing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var s logSink
		release := s.hold(10)
		defer release()
		l := newTestLog(t, &s)
		if r := l.recent(); r == nil || len(r) != 0 {
			t.Errorf("recent() of a new log = %#v, want an empty slice", r)
		}
		for i := range 100 {
			l.emit(testEvent(i))
		}
		r := l.recent()
		if len(r) != 100 || r[0].Seq != 1 || r[99].Seq != 100 {
			t.Fatalf("recent() after 100 events has %d events from Seq %d, want 100 from 1", len(r), r[0].Seq)
		}

		for i := 100; i < 300; i++ {
			l.emit(testEvent(i))
		}
		synctest.Wait() // OnEvent is blocked on the 11th event; 289 are queued
		r = l.recent()
		if len(r) != ringSize {
			t.Fatalf("recent() after 300 events has %d events, want %d", len(r), ringSize)
		}
		for i, e := range r {
			if want := uint64(300 - ringSize + 1 + i); e.Seq != want || e.Count != int(want-1) {
				t.Fatalf("recent()[%d] = Seq %d Count %d, want Seq %d Count %d", i, e.Seq, e.Count, want, want-1)
			}
		}

		r[0].Seq, r[0].Space = 0, "changed"
		if again := l.recent(); again[0].Seq != 300-ringSize+1 || again[0].Space != "s" {
			t.Errorf("recent()[0] after changing an earlier result = %+v, want it unchanged", again[0])
		}

		l.stop() // returns while OnEvent is blocked: stop does not wait for the call
		if seq := l.emit(testEvent(300)); seq != 0 {
			t.Errorf("emit after stop returned Seq %d, want 0", seq)
		}
		r = l.recent()
		if len(r) != ringSize || r[ringSize-1].Seq != 300 {
			t.Errorf("recent() after stop ends at Seq %d, want 300: the dropped events stay", r[len(r)-1].Seq)
		}
		release()
		stopLog(t, l)
		if _, calls := s.snapshot(); len(calls) != 11 {
			t.Errorf("%d OnEvent calls, want 11: stop drops the queued events", len(calls))
		}

		// A stop while the delivery goroutine writes a record waits for the record, and the
		// record's OnEvent call starts before stop returns; the queued event is dropped.
		var s2 logSink
		wgate := make(chan struct{})
		s2.wgate = wgate
		l2 := newTestLog(t, &s2)
		l2.emit(testEvent(0))
		l2.emit(testEvent(1))
		synctest.Wait() // the goroutine writes the first record
		stopped := make(chan struct{})
		go func() {
			l2.stop()
			close(stopped)
		}()
		synctest.Wait()
		select {
		case <-stopped:
			t.Error("stop returned while a record was being written")
		default:
		}
		s2.mu.Lock()
		s2.wgate = nil
		s2.mu.Unlock()
		close(wgate)
		<-stopped
		synctest.Wait()
		if records, calls := s2.snapshot(); len(records) != 1 || len(calls) != 1 || calls[0].e.Seq != 1 {
			t.Errorf("after stop: %d records and calls %v, want one of each for Seq 1", len(records), calls)
		}
		stopLog(t, l2)
	})
}

// TestEventLogHolder checks that a log record carries the member and the holder the node had
// when the event was emitted, not the one it has when the record is written.
func TestEventLogHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var s logSink
		release := s.hold(0)
		l := newTestLog(t, &s)
		l.emit(Event{Kind: EventNodeDraining, Shard: -1, Slot: -1})
		synctest.Wait()
		l.emit(Event{Kind: EventSessionOpened, Shard: -1, Slot: -1})
		l.setHolder("a/2")
		l.emit(Event{Kind: EventSessionRestarted, Shard: -1, Slot: -1, Holder: "a/1"})
		l.emit(Event{Kind: EventSessionOpened, Shard: -1, Slot: -1})
		release()
		l.waitDelivered(context.Background(), 4)

		records, _ := s.snapshot()
		want := []string{
			`{"level":"INFO","msg":"tenure: node draining","member":"a","holder":"a/1"}`,
			`{"level":"INFO","msg":"tenure: session opened","member":"a","holder":"a/1"}`,
			`{"level":"INFO","msg":"tenure: session restarted","member":"a","holder":"a/2","previous":"a/1"}`,
			`{"level":"INFO","msg":"tenure: session opened","member":"a","holder":"a/2"}`,
		}
		if len(records) != len(want) {
			t.Fatalf("%d records, want %d", len(records), len(want))
		}
		for i, r := range records {
			if r.line != want[i] {
				t.Errorf("record %d = %s, want %s", i, r.line, want[i])
			}
		}
		stopLog(t, l)
	})
}

// TestEventLogFields checks what emit sets: Seq, and Time as the wall-clock time in UTC without a
// monotonic reading; the other fields are kept. It also checks a log record's attributes and that
// a nil logger and a nil OnEvent are fine.
func TestEventLogFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var s logSink
		l := newTestLog(t, &s)
		time.Sleep(90 * time.Minute)
		boom := errors.New("boom")
		in := Event{Seq: 99, Time: time.Now(), Kind: EventShardAcquireFailed, Space: "s", Shard: 1, Slot: -1,
			Holder: "b/2", Duration: 2 * time.Second, Err: boom}
		l.emit(in)
		l.waitDelivered(context.Background(), 1)

		records, calls := s.snapshot()
		got := calls[0].e
		if got.Seq != 1 {
			t.Errorf("Seq = %d, want 1", got.Seq)
		}
		if got.Time.Location() != time.UTC || !got.Time.Equal(time.Now()) {
			t.Errorf("Time = %v, want %v in UTC", got.Time, time.Now().UTC())
		}
		if strings.Contains(got.Time.String(), "m=") || got.Time != got.Time.Round(0) {
			t.Errorf("Time = %s has a monotonic reading, want none", got.Time)
		}
		want := in
		want.Seq, want.Time = got.Seq, got.Time
		if got != want {
			t.Errorf("delivered %+v, want %+v", got, want)
		}
		if r := l.recent(); len(r) != 1 || r[0] != got {
			t.Errorf("recent() = %+v, want [%+v]", r, got)
		}
		wantRec := `{"level":"DEBUG","msg":"tenure: shard acquire failed","member":"a","holder":"a/1","space":"s",` +
			`"shard":1,"holder":"b/2","retry_after":2000000000,"error":"boom"}`
		if len(records) != 1 || records[0].line != wantRec {
			t.Errorf("records = %v, want [%s]", records, wantRec)
		}

		stopLog(t, l)

		var buf bytes.Buffer
		for _, nl := range []*eventLog{newEventLog("a", nil, nil), newEventLog("a", slog.New(slog.NewTextHandler(&buf, nil)), nil)} {
			nl.start("a/1")
			seq := nl.emit(Event{Kind: EventNodeDraining, Shard: -1, Slot: -1})
			nl.waitDelivered(context.Background(), seq)
			stopLog(t, nl)
		}
		if !strings.Contains(buf.String(), "tenure: node draining") {
			t.Errorf("text log = %q, want a node draining record", buf.String())
		}
	})
}
