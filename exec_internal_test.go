// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// scriptCall is one call the scripted store received.
type scriptCall struct {
	method string
	holder string
	space  string
	shard  int
	term   core.Term
	slot   int
	timing core.Timing

	ctx context.Context
	at  time.Time // when the call arrived

	reply    chan scriptReply
	returned bool // the call has returned to the executor
}

// scriptReply is what a scripted call returns.
type scriptReply struct {
	term    core.Term
	holding Holding
	slot    int
	err     error
}

// scriptStore is a Store and SlotStore whose calls block until the test answers them, or until
// their context ends, unless auto answers them first.
type scriptStore struct {
	mu    sync.Mutex
	calls []*scriptCall

	// auto, when set, may answer a call at once; it runs on the lane.
	auto func(c *scriptCall) (scriptReply, bool)
}

func (s *scriptStore) serve(ctx context.Context, c *scriptCall) scriptReply {
	c.ctx, c.at, c.reply = ctx, time.Now(), make(chan scriptReply, 1)
	s.mu.Lock()
	s.calls = append(s.calls, c)
	auto := s.auto
	s.mu.Unlock()
	var r scriptReply
	answered := false
	if auto != nil {
		r, answered = auto(c)
	}
	if !answered {
		select {
		case r = <-c.reply:
		case <-ctx.Done():
			r = scriptReply{slot: -1, err: ctx.Err()}
		}
	}
	s.mu.Lock()
	c.returned = true
	s.mu.Unlock()
	return r
}

// open returns the calls that have not returned, in arrival order.
func (s *scriptStore) open() []*scriptCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cs []*scriptCall
	for _, c := range s.calls {
		if !c.returned {
			cs = append(cs, c)
		}
	}
	return cs
}

// all returns every call received, in arrival order.
func (s *scriptStore) all() []*scriptCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*scriptCall(nil), s.calls...)
}

func (s *scriptStore) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	return s.serve(ctx, &scriptCall{method: "KeepAlive", holder: holder, timing: timing}).err
}

func (s *scriptStore) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	r := s.serve(ctx, &scriptCall{method: "Acquire", holder: holder, space: space, shard: shard})
	return r.term, r.err
}

func (s *scriptStore) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	return s.serve(ctx, &scriptCall{method: "Release", holder: holder, space: space, shard: shard,
		term: term}).err
}

func (s *scriptStore) Confirm(ctx context.Context, holder, space string, shard int,
	term core.Term) (Holding, error) {
	r := s.serve(ctx, &scriptCall{method: "Confirm", holder: holder, space: space, shard: shard,
		term: term})
	return r.holding, r.err
}

func (s *scriptStore) CloseSession(ctx context.Context, holder string) error {
	return s.serve(ctx, &scriptCall{method: "CloseSession", holder: holder}).err
}

func (s *scriptStore) Holdings(context.Context, string) ([]Holding, error) { return nil, nil }

func (s *scriptStore) ClaimSlot(ctx context.Context, holder string) (int, error) {
	r := s.serve(ctx, &scriptCall{method: "ClaimSlot", holder: holder})
	return r.slot, r.err
}

func (s *scriptStore) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	return s.serve(ctx, &scriptCall{method: "ReleaseSlot", holder: holder, slot: slot}).err
}

func (s *scriptStore) Slots(context.Context) (int, []string, error) { return 0, nil, nil }

func (s *scriptStore) SetSlots(context.Context, int) error { return nil }

// execHarness is an executor over a scripted store, built inside a synctest bubble.
type execHarness struct {
	t      *testing.T
	st     *scriptStore
	e      *executor
	cancel context.CancelFunc
	epoch  time.Time

	mu     sync.Mutex
	failed []callResult
	hold   chan struct{} // when set, failed waits until it is closed
}

const execHolder = "a/1111"

func newExecHarness(t *testing.T) *execHarness {
	t.Helper()
	h := &execHarness{t: t, st: &scriptStore{}, epoch: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.e = newExecutor(ctx, h.st, core.DefaultTiming, h.epoch, func(r callResult) {
		h.mu.Lock()
		h.failed = append(h.failed, r)
		hold := h.hold
		h.mu.Unlock()
		if hold != nil {
			<-hold
		}
	})
	t.Cleanup(h.close)
	return h
}

// close shuts the executor down and checks that no lane goroutine is left.
func (h *execHarness) close() {
	h.cancel()
	h.e.wait()
	if n := laneGoroutines(); n != 0 {
		h.t.Errorf("%d lane goroutines after wait", n)
	}
}

// holdFailed makes failed wait until hold is closed; nil stops that.
func (h *execHarness) holdFailed(hold chan struct{}) {
	h.mu.Lock()
	h.hold = hold
	h.mu.Unlock()
}

func (h *execHarness) failures() []callResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]callResult(nil), h.failed...)
}

// do queues act for generation 1 and fails the test unless the executor's answer is queued.
func (h *execHarness) do(act core.Action, queued bool) {
	h.t.Helper()
	if got := h.e.do(1, execHolder, act); got != queued {
		h.t.Fatalf("do(%T%+v) = %v, want %v", act, act, got, queued)
	}
}

// answer answers the first open call whose method or label is name, and waits for the bubble
// to settle.
func (h *execHarness) answer(name string, r scriptReply) *scriptCall {
	h.t.Helper()
	synctest.Wait()
	for _, c := range h.st.open() {
		if c.method == name || label(c) == name {
			c.reply <- r
			synctest.Wait()
			return c
		}
	}
	h.t.Fatalf("no open %s call; open: %s", name, describe(h.st.open()))
	return nil
}

func describe(cs []*scriptCall) string {
	var b strings.Builder
	for _, c := range cs {
		fmt.Fprintf(&b, "%s(%s/%d t%d slot %d) ", c.method, c.space, c.shard, c.term, c.slot)
	}
	return b.String()
}

// label names a call by its method and, for a shard call, its shard.
func label(c *scriptCall) string {
	if c.space == "" {
		return c.method
	}
	return fmt.Sprintf("%s(%s/%d)", c.method, c.space, c.shard)
}

// methods labels calls in the order given.
func methods(cs []*scriptCall) string {
	var ms []string
	for _, c := range cs {
		ms = append(ms, label(c))
	}
	return strings.Join(ms, " ")
}

// sortedMethods labels calls in sorted order, for calls on different lanes, whose arrival order
// is not fixed.
func sortedMethods(cs []*scriptCall) string {
	var ms []string
	for _, c := range cs {
		ms = append(ms, label(c))
	}
	slices.Sort(ms)
	return strings.Join(ms, " ")
}

// laneGoroutines counts the executor's lane goroutines.
func laneGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "tenure.(*executor).run(")
}

func TestExecLaneOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		h.do(core.Acquire{Space: "s", Shard: 0}, true)
		h.do(core.Release{Space: "s", Shard: 0, Term: 5}, true)
		h.do(core.Confirm{Space: "s", Shard: 0, Term: 5}, true)
		h.do(core.Acquire{Space: "s", Shard: 1}, true)
		h.do(core.Acquire{Space: "t", Shard: 0}, true)
		synctest.Wait()

		// One call per lane at a time; the lanes run side by side.
		if got, want := sortedMethods(h.st.open()), "Acquire(s/0) Acquire(s/1) Acquire(t/0)"; got != want {
			t.Fatalf("open calls %q, want %q", got, want)
		}
		h.answer("Acquire(s/0)", scriptReply{term: 5})
		if got, want := sortedMethods(h.st.open()), "Acquire(s/1) Acquire(t/0) Release(s/0)"; got != want {
			t.Fatalf("open calls %q, want %q", got, want)
		}
		time.Sleep(time.Second)
		h.answer("Release", scriptReply{})
		if got, want := sortedMethods(h.st.open()), "Acquire(s/1) Acquire(t/0) Confirm(s/0)"; got != want {
			t.Fatalf("open calls %q, want %q", got, want)
		}
		h.answer("Confirm", scriptReply{holding: Holding{Space: "s", Shard: 0, Holder: execHolder, Term: 5}})
		h.answer("Acquire(s/1)", scriptReply{term: 1})
		h.answer("Acquire(t/0)", scriptReply{term: 1})

		rs := h.e.take()
		var got []string
		for i, r := range rs {
			got = append(got, fmt.Sprintf("%T%+v", r.act, r.act))
			if r.start >= r.end {
				t.Errorf("result %d: start %d not before end %d", i, r.start, r.end)
			}
			if i > 0 && rs[i-1].end >= r.end {
				t.Errorf("result %d: end %d not after %d", i, r.end, rs[i-1].end)
			}
		}
		want := []string{
			"core.Acquire{Space:s Shard:0}",
			"core.Release{Space:s Shard:0 Term:5}",
			"core.Confirm{Space:s Shard:0 Term:5}",
			"core.Acquire{Space:s Shard:1}",
			"core.Acquire{Space:t Shard:0}",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("results in order %v, want %v", got, want)
		}
		// On one lane each call starts after the previous one ended.
		if rs[0].end >= rs[1].start || rs[1].end >= rs[2].start {
			t.Errorf("lane s/0 stamps overlap: %+v", rs[:3])
		}
		if rs := h.e.take(); rs != nil {
			t.Errorf("second take = %v, want nil", rs)
		}
	})
}

func TestExecPermits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		var mu sync.Mutex
		inFlight, peak := 0, 0
		enter := func() {
			mu.Lock()
			inFlight++
			peak = max(peak, inFlight)
			mu.Unlock()
		}
		leave := func() {
			mu.Lock()
			inFlight--
			mu.Unlock()
		}
		h.st.auto = func(c *scriptCall) (scriptReply, bool) {
			if c.method == "KeepAlive" {
				return scriptReply{}, false // answered by the test
			}
			enter()
			defer leave()
			time.Sleep(time.Second)
			return scriptReply{term: 1, slot: 0}, true
		}

		t0 := time.Now()
		for i := range 40 {
			h.do(core.Acquire{Space: "s", Shard: i}, true)
		}
		h.do(core.ClaimSlot{}, true)
		h.do(core.ReleaseSlot{Slot: 3}, true)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				if !h.e.withPermit(func() {
					enter()
					defer leave()
					time.Sleep(time.Second)
				}) {
					t.Error("withPermit returned false")
				}
			})
		}
		synctest.Wait()
		mu.Lock()
		if inFlight != 16 {
			t.Errorf("%d non-session calls in flight, want %d", inFlight, 16)
		}
		mu.Unlock()

		// A renewal never waits for a permit.
		h.do(core.KeepAlive{}, true)
		synctest.Wait()
		open := h.st.open()
		var ka *scriptCall
		for _, c := range open {
			if c.method == "KeepAlive" {
				ka = c
			}
		}
		if ka == nil || !ka.at.Equal(t0) {
			t.Fatalf("KeepAlive did not start at once with every permit taken: %s", describe(open))
		}
		ka.reply <- scriptReply{}

		time.Sleep(10 * time.Second)
		wg.Wait()
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		if peak != 16 {
			t.Errorf("peak %d non-session calls in flight, want %d", peak, 16)
		}
		if n := len(h.e.take()); n != 43 {
			t.Errorf("%d results, want 43", n)
		}
	})

	// A failed call gives its permit back before failed runs for it.
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		h.st.auto = func(c *scriptCall) (scriptReply, bool) {
			return scriptReply{err: errFlaky}, c.method == "Release"
		}
		hold := make(chan struct{})
		h.holdFailed(hold)
		for i := range execPermits {
			h.do(core.Release{Space: "s", Shard: i, Term: 1}, true)
		}
		synctest.Wait() // every Release has failed, and failed is running for it
		h.do(core.Acquire{Space: "s", Shard: 99}, true)
		synctest.Wait()
		open := methods(h.st.open())
		close(hold)
		h.holdFailed(nil)
		if want := "Acquire(s/99)"; open != want {
			t.Errorf("open calls %q while failed runs for %d Releases, want %q", open, execPermits, want)
		}
		if n := len(h.failures()); n != execPermits {
			t.Errorf("failed called %d times, want %d", n, execPermits)
		}
	})
}

func TestExecKeepAliveQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		h.do(core.KeepAlive{}, true) // in flight
		synctest.Wait()
		h.do(core.KeepAlive{}, true)  // queued
		h.do(core.KeepAlive{}, false) // a second queued one is dropped
		h.do(core.KeepAlive{}, false)
		h.answer("KeepAlive", scriptReply{})
		h.do(core.KeepAlive{}, true) // the queued one is in flight now
		h.do(core.KeepAlive{}, false)
		h.answer("KeepAlive", scriptReply{})
		h.answer("KeepAlive", scriptReply{})
		synctest.Wait()
		if n := len(h.st.all()); n != 3 {
			t.Errorf("%d KeepAlive calls, want 3", n)
		}
		if n := len(h.e.take()); n != 3 {
			t.Errorf("%d results, want 3", n)
		}

		// A queued CloseSession does not count as a queued KeepAlive.
		h.do(core.KeepAlive{}, true)
		synctest.Wait()
		h.do(core.CloseSession{}, true)
		h.do(core.KeepAlive{}, true)
		h.do(core.KeepAlive{}, false)
		h.answer("KeepAlive", scriptReply{})
		h.answer("CloseSession", scriptReply{})
		h.answer("KeepAlive", scriptReply{})
		if got, want := methods(h.st.all()[3:]), "KeepAlive CloseSession KeepAlive"; got != want {
			t.Errorf("calls %q, want %q", got, want)
		}
	})
}

func TestExecDropIdentical(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		cases := []struct{ first, other core.Action }{
			{core.Release{Space: "s", Shard: 0, Term: 5}, core.Release{Space: "s", Shard: 0, Term: 6}},
			{core.ReleaseSlot{Slot: 1}, core.ReleaseSlot{Slot: 2}},
			{core.ClaimSlot{}, core.ReleaseSlot{Slot: 1}},
			{core.CloseSession{}, core.CloseSession{}},
			{core.CloseSession{}, core.KeepAlive{}},
		}
		for _, tc := range cases {
			before := len(h.st.all())
			h.do(tc.first, true) // in flight
			synctest.Wait()
			h.do(tc.first, false) // equal to the one in flight
			if tc.other == tc.first {
				// Nothing else is queued: the only call is the one in flight.
				h.answer(label(h.st.open()[0]), scriptReply{})
				if n := len(h.st.all()) - before; n != 1 {
					t.Errorf("%T: %d calls, want 1", tc.first, n)
				}
				h.e.take()
				continue
			}
			h.do(tc.other, true)  // queued
			h.do(tc.other, false) // equal to the one queued
			h.do(tc.first, false) // still equal to the one in flight
			for range 2 {
				synctest.Wait()
				open := h.st.open()
				if len(open) != 1 {
					t.Fatalf("%T: open calls %s, want one", tc.first, describe(open))
				}
				open[0].reply <- scriptReply{slot: 1}
			}
			synctest.Wait()
			if got, want := len(h.st.all())-before, 2; got != want {
				t.Errorf("%T then %T: %d calls, want %d: %s", tc.first, tc.other, got, want,
					methods(h.st.all()[before:]))
			}
			h.e.take()
		}

		// Acquire and Confirm are never dropped.
		before := len(h.st.all())
		for _, act := range []core.Action{
			core.Acquire{Space: "s", Shard: 1}, core.Acquire{Space: "s", Shard: 1},
			core.Confirm{Space: "s", Shard: 1, Term: 2}, core.Confirm{Space: "s", Shard: 1, Term: 2},
			core.Confirm{Space: "s", Shard: 1, Term: 2},
		} {
			h.do(act, true)
		}
		for range 5 {
			synctest.Wait()
			h.st.open()[0].reply <- scriptReply{term: 2}
		}
		synctest.Wait()
		if got, want := methods(h.st.all()[before:]),
			"Acquire(s/1) Acquire(s/1) Confirm(s/1) Confirm(s/1) Confirm(s/1)"; got != want {
			t.Errorf("calls %q, want %q", got, want)
		}

		// Once a call has returned, an equal one runs again.
		before = len(h.st.all())
		h.do(core.Release{Space: "s", Shard: 0, Term: 5}, true)
		h.answer("Release", scriptReply{})
		h.do(core.Release{Space: "s", Shard: 0, Term: 5}, true)
		h.answer("Release", scriptReply{})
		if n := len(h.st.all()) - before; n != 2 {
			t.Errorf("%d Release calls after the first returned, want 2", n)
		}

		// A call whose result is out is no longer in flight, also while failed still runs for it
		// on the lane.
		hold := make(chan struct{})
		h.holdFailed(hold)
		before = len(h.st.all())
		h.do(core.Release{Space: "s", Shard: 0, Term: 5}, true)
		h.answer("Release", scriptReply{err: errFlaky})
		queued := h.e.do(1, execHolder, core.Release{Space: "s", Shard: 0, Term: 5})
		close(hold)
		h.holdFailed(nil)
		if !queued {
			t.Fatal("Release dropped while failed ran for an equal one that had returned")
		}
		h.answer("Release", scriptReply{})
		if n := len(h.st.all()) - before; n != 2 {
			t.Errorf("%d Release calls with failed running for the first, want 2", n)
		}
	})
}

func TestExecContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		retry := core.DefaultTiming.RetryPeriod
		time.Sleep(5 * time.Second)
		t0 := time.Now()
		h.do(core.Acquire{Space: "s", Shard: 0}, true)
		h.do(core.Release{Space: "s", Shard: 0, Term: 3}, true)
		synctest.Wait()
		time.Sleep(1500 * time.Millisecond)
		acq := h.answer("Acquire", scriptReply{term: 3})
		rel := h.st.open()[0]
		for _, c := range []struct {
			c     *scriptCall
			start time.Time
		}{{acq, t0}, {rel, t0.Add(1500 * time.Millisecond)}} {
			if !c.c.at.Equal(c.start) {
				t.Errorf("%s started at %v, want %v", c.c.method, c.c.at.Sub(h.epoch), c.start.Sub(h.epoch))
			}
			if d, ok := c.c.ctx.Deadline(); !ok || !d.Equal(c.start.Add(retry)) {
				t.Errorf("%s deadline %v, want start + %v", c.c.method, d.Sub(c.start), retry)
			}
		}
		// The Release is never answered: its context ends at its deadline.
		time.Sleep(retry)
		synctest.Wait()
		if !errors.Is(rel.ctx.Err(), context.DeadlineExceeded) {
			t.Errorf("Release ctx err %v, want deadline exceeded", rel.ctx.Err())
		}

		// KeepAlive: SentAt is read right before the call, with the matching wall time, and the
		// session context carries the Timing and the same RetryPeriod bound.
		h.do(core.KeepAlive{}, true)
		synctest.Wait()
		ka := h.st.open()[0]
		time.Sleep(retry)
		synctest.Wait()
		rs := h.e.take()
		r := rs[len(rs)-1]
		sentAt := ka.at.Sub(h.epoch)
		if ka.timing != core.DefaultTiming {
			t.Errorf("KeepAlive timing %+v, want %+v", ka.timing, core.DefaultTiming)
		}
		if d, _ := ka.ctx.Deadline(); !d.Equal(ka.at.Add(retry)) {
			t.Errorf("KeepAlive deadline %v after start, want %v", d.Sub(ka.at), retry)
		}
		if !r.sentWall.Equal(h.epoch.Add(sentAt)) || !r.sentWall.Equal(ka.at) {
			t.Errorf("sentWall %v, want epoch + %v", r.sentWall.Sub(h.epoch), sentAt)
		}
		if want := []core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}; !reflect.DeepEqual(r.inputs, want) {
			t.Errorf("timed-out KeepAlive inputs %+v, want %+v", r.inputs, want)
		}

		h.do(core.KeepAlive{}, true)
		ka = h.answer("KeepAlive", scriptReply{})
		r = h.e.take()[0]
		if want := []core.Input{core.SessionRenewed{SentAt: ka.at.Sub(h.epoch)}}; !reflect.DeepEqual(r.inputs, want) {
			t.Errorf("KeepAlive inputs %+v, want %+v", r.inputs, want)
		}

		// Every call context derives from the internal one.
		h.do(core.Confirm{Space: "s", Shard: 0, Term: 3}, true)
		synctest.Wait()
		cf := h.st.open()[0]
		h.cancel()
		synctest.Wait()
		if !errors.Is(cf.ctx.Err(), context.Canceled) {
			t.Errorf("Confirm ctx err %v after the internal context ended, want canceled", cf.ctx.Err())
		}
	})
}

var errFlaky = errors.New("flaky")

func TestExecResults(t *testing.T) {
	lost := core.SessionFailed{Cause: core.CauseLost}
	wrappedLost := fmt.Errorf("store: %w", ErrSessionLost)
	held := Holding{Space: "s", Shard: 3, Holder: "b/2222", Term: 9, Version: 4}
	rows := []struct {
		name    string
		act     core.Action
		reply   scriptReply
		block   bool // never answered: the call times out
		inputs  []core.Input
		holding Holding
		failed  bool
	}{
		{"KeepAlive/nil", core.KeepAlive{}, scriptReply{}, false,
			[]core.Input{core.SessionRenewed{SentAt: 3 * time.Second}}, Holding{}, false},
		{"KeepAlive/lost", core.KeepAlive{}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{lost}, Holding{}, false},
		{"KeepAlive/wrapped-lost", core.KeepAlive{}, scriptReply{err: fmt.Errorf("x: %w", ErrSessionLost)}, false,
			[]core.Input{lost}, Holding{}, false},
		{"KeepAlive/other", core.KeepAlive{}, scriptReply{err: errFlaky}, false,
			[]core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}, Holding{}, false},
		{"KeepAlive/timeout", core.KeepAlive{}, scriptReply{}, true,
			[]core.Input{core.SessionFailed{Cause: core.CauseUnreachable}}, Holding{}, false},

		{"CloseSession/nil", core.CloseSession{}, scriptReply{}, false,
			[]core.Input{core.SessionClosed{}}, Holding{}, false},
		{"CloseSession/lost", core.CloseSession{}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{core.SessionClosed{}}, Holding{}, true},
		{"CloseSession/wrapped-lost", core.CloseSession{}, scriptReply{err: wrappedLost}, false,
			[]core.Input{core.SessionClosed{}}, Holding{}, true},
		{"CloseSession/other", core.CloseSession{}, scriptReply{err: errFlaky}, false,
			nil, Holding{}, true},

		{"Acquire/term", core.Acquire{Space: "s", Shard: 3}, scriptReply{term: 7}, false,
			[]core.Input{core.Acquired{Space: "s", Shard: 3, Term: 7}}, Holding{}, false},
		{"Acquire/held", core.Acquire{Space: "s", Shard: 3},
			scriptReply{err: &HeldError{Holder: "b/2222", RetryAfter: 2 * time.Second}}, false,
			[]core.Input{core.AcquireFailed{Space: "s", Shard: 3, Holder: "b/2222", RetryAfter: 2 * time.Second}},
			Holding{}, false},
		{"Acquire/wrapped-held", core.Acquire{Space: "s", Shard: 3},
			scriptReply{err: fmt.Errorf("store: %w", &HeldError{Holder: "b/2222", RetryAfter: time.Second})}, false,
			[]core.Input{core.AcquireFailed{Space: "s", Shard: 3, Holder: "b/2222", RetryAfter: time.Second}},
			Holding{}, false},
		{"Acquire/lost", core.Acquire{Space: "s", Shard: 3}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{core.Acquired{Space: "s", Shard: 3}, lost}, Holding{}, false},
		{"Acquire/wrapped-lost", core.Acquire{Space: "s", Shard: 3}, scriptReply{err: wrappedLost}, false,
			[]core.Input{core.Acquired{Space: "s", Shard: 3}, lost}, Holding{}, false},
		{"Acquire/other", core.Acquire{Space: "s", Shard: 3}, scriptReply{err: errFlaky}, false,
			[]core.Input{core.Acquired{Space: "s", Shard: 3}}, Holding{}, false},
		{"Acquire/timeout", core.Acquire{Space: "s", Shard: 3}, scriptReply{}, true,
			[]core.Input{core.Acquired{Space: "s", Shard: 3}}, Holding{}, false},

		{"Release/nil", core.Release{Space: "s", Shard: 3, Term: 7}, scriptReply{}, false,
			[]core.Input{core.Released{Space: "s", Shard: 3, Term: 7}}, Holding{}, false},
		{"Release/lost", core.Release{Space: "s", Shard: 3, Term: 7}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"Release/wrapped-lost", core.Release{Space: "s", Shard: 3, Term: 7}, scriptReply{err: wrappedLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"Release/other", core.Release{Space: "s", Shard: 3, Term: 7}, scriptReply{err: errFlaky}, false,
			nil, Holding{}, true},

		{"Confirm/nil", core.Confirm{Space: "s", Shard: 3, Term: 7}, scriptReply{holding: held}, false,
			[]core.Input{core.HolderChanged{Space: "s", Shard: 3, Holder: "b/2222", Term: 9}}, held, false},
		{"Confirm/absent", core.Confirm{Space: "s", Shard: 3, Term: 7},
			scriptReply{holding: Holding{Space: "s", Shard: 3}}, false,
			[]core.Input{core.HolderChanged{Space: "s", Shard: 3}}, Holding{Space: "s", Shard: 3}, false},
		{"Confirm/lost", core.Confirm{Space: "s", Shard: 3, Term: 7}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"Confirm/wrapped-lost", core.Confirm{Space: "s", Shard: 3, Term: 7}, scriptReply{err: wrappedLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"Confirm/other", core.Confirm{Space: "s", Shard: 3, Term: 7}, scriptReply{err: errFlaky}, false,
			nil, Holding{}, true},

		{"ClaimSlot/slot", core.ClaimSlot{}, scriptReply{slot: 2}, false,
			[]core.Input{core.SlotClaimed{Slot: 2}}, Holding{}, false},
		{"ClaimSlot/lost", core.ClaimSlot{}, scriptReply{slot: -1, err: ErrSessionLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"ClaimSlot/wrapped-lost", core.ClaimSlot{}, scriptReply{slot: -1, err: wrappedLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"ClaimSlot/noslot", core.ClaimSlot{}, scriptReply{slot: -1, err: ErrNoSlot}, false,
			nil, Holding{}, true},
		{"ClaimSlot/other", core.ClaimSlot{}, scriptReply{slot: -1, err: errFlaky}, false,
			nil, Holding{}, true},

		{"ReleaseSlot/nil", core.ReleaseSlot{Slot: 1}, scriptReply{}, false,
			[]core.Input{core.SlotReleased{Slot: 1}}, Holding{}, false},
		{"ReleaseSlot/lost", core.ReleaseSlot{Slot: 1}, scriptReply{err: ErrSessionLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"ReleaseSlot/wrapped-lost", core.ReleaseSlot{Slot: 1}, scriptReply{err: wrappedLost}, false,
			[]core.Input{lost}, Holding{}, true},
		{"ReleaseSlot/other", core.ReleaseSlot{Slot: 1}, scriptReply{err: errFlaky}, false,
			nil, Holding{}, true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newExecHarness(t)
				h.st.auto = func(*scriptCall) (scriptReply, bool) { return row.reply, !row.block }
				time.Sleep(3 * time.Second)
				if !h.e.do(7, execHolder, row.act) {
					t.Fatal("do dropped the call")
				}
				synctest.Wait()
				if row.block {
					time.Sleep(core.DefaultTiming.RetryPeriod)
					synctest.Wait()
				}
				select {
				case <-h.e.ready():
				default:
					t.Fatal("ready not signalled")
				}
				rs := h.e.take()
				if len(rs) != 1 {
					t.Fatalf("%d results, want 1", len(rs))
				}
				r := rs[0]
				if r.gen != 7 || r.act != row.act {
					t.Errorf("gen %d act %+v, want 7 %+v", r.gen, r.act, row.act)
				}
				if !reflect.DeepEqual(r.inputs, row.inputs) {
					t.Errorf("inputs %+v, want %+v", r.inputs, row.inputs)
				}
				if r.holding != row.holding {
					t.Errorf("holding %+v, want %+v", r.holding, row.holding)
				}
				wantErr := row.reply.err
				if row.block {
					wantErr = context.DeadlineExceeded
				}
				if !errors.Is(r.err, wantErr) || (wantErr == nil) != (r.err == nil) {
					t.Errorf("err %v, want %v", r.err, wantErr)
				}
				if c := h.st.all()[0]; c.holder != execHolder {
					t.Errorf("store saw holder %q, want %q", c.holder, execHolder)
				}
				fs := h.failures()
				if row.failed != (len(fs) == 1) || len(fs) > 1 {
					t.Fatalf("failed called %d times, want it called: %v", len(fs), row.failed)
				}
				if row.failed && (fs[0].err != r.err || fs[0].end != r.end) {
					t.Errorf("failed got %+v, want the result %+v", fs[0], r)
				}
			})
		})
	}
}

func TestExecDiscards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		e := h.e
		e.do(1, "a/1", core.Confirm{Space: "s", Shard: 0, Term: 1}) // in flight
		e.do(1, "a/1", core.Release{Space: "s", Shard: 0, Term: 1}) // queued behind it
		e.do(1, "a/1", core.Acquire{Space: "s", Shard: 1})          // answered, not taken
		e.do(1, "a/1", core.Release{Space: "s", Shard: 2, Term: 1}) // in flight
		e.do(1, "a/1", core.KeepAlive{})                            // in flight
		e.do(1, "a/1", core.KeepAlive{})                            // queued
		synctest.Wait()
		h.answer("Acquire", scriptReply{term: 1})

		e.dropOlder(2)
		if rs := e.take(); len(rs) != 0 {
			t.Fatalf("take after dropOlder = %+v, want none", rs)
		}
		if e.do(1, "a/1", core.Acquire{Space: "s", Shard: 3}) {
			t.Error("do queued a call of an older generation")
		}
		// An equal call of the new generation is not dropped by the old one in flight.
		if !e.do(2, "a/2", core.Release{Space: "s", Shard: 2, Term: 1}) {
			t.Error("Release of generation 2 dropped")
		}
		e.do(2, "a/2", core.KeepAlive{})

		h.answer("Confirm", scriptReply{err: errFlaky})
		h.answer("Release", scriptReply{}) // s/2, generation 1
		h.answer("KeepAlive", scriptReply{})
		synctest.Wait()
		if got, want := sortedMethods(h.st.open()), "KeepAlive Release(s/2)"; got != want {
			t.Fatalf("open calls %q, want %q: the dropped Release of s/0 ran", got, want)
		}
		h.answer("Release", scriptReply{}) // s/2, generation 2
		h.answer("KeepAlive", scriptReply{})
		synctest.Wait()

		if got, want := sortedMethods(h.st.all()),
			"Acquire(s/1) Confirm(s/0) KeepAlive KeepAlive Release(s/2) Release(s/2)"; got != want {
			t.Errorf("calls %q, want %q", got, want)
		}
		var got []string
		for _, r := range e.take() {
			got = append(got, fmt.Sprintf("%d %T", r.gen, r.act))
		}
		if want := []string{"2 core.Release", "2 core.KeepAlive"}; !reflect.DeepEqual(got, want) {
			t.Errorf("results %v, want %v", got, want)
		}
		for _, c := range h.st.all()[4:] {
			if c.holder != "a/2" {
				t.Errorf("%s with holder %q, want a/2", c.method, c.holder)
			}
		}
		// A discarded result's failed store call is still reported.
		if fs := h.failures(); len(fs) != 1 || fs[0].gen != 1 || fs[0].err != errFlaky {
			t.Errorf("failures %+v, want the generation 1 Confirm", fs)
		}

		// A call taken from its lane but still waiting for a permit never starts once its
		// generation is dropped.
		release := make(chan struct{})
		for range 16 {
			go h.e.withPermit(func() { <-release })
		}
		synctest.Wait()
		e.do(2, "a/2", core.Acquire{Space: "s", Shard: 9})
		synctest.Wait()
		e.dropOlder(3)
		close(release)
		synctest.Wait()
		if n := len(h.st.all()); n != 6 {
			t.Errorf("%d store calls, want 6: %s", n, methods(h.st.all()[6:]))
		}
		if rs := e.take(); len(rs) != 0 {
			t.Errorf("results %+v, want none", rs)
		}
	})
}

func TestExecIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		if n := laneGoroutines(); n != 0 {
			t.Fatalf("%d lane goroutines before any call", n)
		}
		h.do(core.KeepAlive{}, true)
		h.do(core.ClaimSlot{}, true)
		h.do(core.Acquire{Space: "s", Shard: 0}, true)
		h.do(core.Release{Space: "s", Shard: 0, Term: 1}, true)
		synctest.Wait()
		if n := laneGoroutines(); n != 3 {
			t.Errorf("%d lane goroutines with three busy lanes, want 3", n)
		}
		h.answer("KeepAlive", scriptReply{})
		if n := laneGoroutines(); n != 2 {
			t.Errorf("%d lane goroutines after the session lane emptied, want 2", n)
		}
		h.answer("ClaimSlot", scriptReply{slot: 0})
		h.answer("Acquire", scriptReply{term: 1})
		h.answer("Release", scriptReply{})
		if n := laneGoroutines(); n != 0 {
			t.Errorf("%d lane goroutines while idle", n)
		}
		h.e.mu.Lock()
		if n := len(h.e.lanes); n != 0 {
			t.Errorf("%d lanes kept while idle", n)
		}
		h.e.mu.Unlock()

		// A lane comes back for the next call and goes again.
		time.Sleep(time.Hour)
		h.do(core.Acquire{Space: "s", Shard: 0}, true)
		h.answer("Acquire", scriptReply{term: 2})
		if n := laneGoroutines(); n != 0 {
			t.Errorf("%d lane goroutines while idle again", n)
		}
	})
}

func TestExecNoRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		h.st.auto = func(c *scriptCall) (scriptReply, bool) {
			if c.space == "block" {
				return scriptReply{}, false // times out
			}
			return scriptReply{slot: -1, err: errFlaky}, true
		}
		acts := []core.Action{
			core.KeepAlive{},
			core.Acquire{Space: "s", Shard: 0},
			core.Release{Space: "s", Shard: 1, Term: 1},
			core.Confirm{Space: "s", Shard: 2, Term: 1},
			core.ClaimSlot{},
			core.Acquire{Space: "block", Shard: 0},
			core.Confirm{Space: "block", Shard: 1, Term: 1},
		}
		for _, act := range acts {
			h.do(act, true)
		}
		synctest.Wait()
		h.do(core.CloseSession{}, true)
		h.do(core.ReleaseSlot{Slot: 0}, true)
		time.Sleep(10 * core.DefaultTiming.RetryPeriod)
		synctest.Wait()

		if n := len(h.st.all()); n != len(acts)+2 {
			t.Errorf("%d store calls for %d actions: %s", n, len(acts)+2, methods(h.st.all()))
		}
		if n := len(h.e.take()); n != len(acts)+2 {
			t.Errorf("%d results, want %d", n, len(acts)+2)
		}
		var failed []string
		for _, r := range h.failures() {
			failed = append(failed, fmt.Sprintf("%T", r.act))
		}
		want := []string{"core.Release", "core.Confirm", "core.ClaimSlot", "core.CloseSession",
			"core.ReleaseSlot", "core.Confirm"}
		slices.Sort(failed)
		slices.Sort(want)
		if !reflect.DeepEqual(failed, want) {
			t.Errorf("failed called for %v, want %v", failed, want)
		}
	})
}

func TestExecWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newExecHarness(t)
		for i := range 20 {
			h.do(core.Acquire{Space: "s", Shard: i}, true)
		}
		h.do(core.Release{Space: "s", Shard: 0, Term: 1}, true)
		synctest.Wait()
		permitted := make(chan bool, 1)
		go func() { permitted <- h.e.withPermit(func() { t.Error("withPermit ran f after the end") }) }()
		synctest.Wait()

		h.cancel()
		h.e.wait()
		if <-permitted {
			t.Error("withPermit returned true")
		}
		// The 16 calls in flight return with the context's error; the others never start.
		if n := len(h.st.all()); n != 16 {
			t.Errorf("%d store calls, want %d", n, 16)
		}
		if n := len(h.e.take()); n != 16 {
			t.Errorf("%d results, want %d", n, 16)
		}
		if h.e.do(1, execHolder, core.KeepAlive{}) {
			t.Error("do queued a call after wait")
		}
		if n := laneGoroutines(); n != 0 {
			t.Errorf("%d lane goroutines after wait", n)
		}
	})
}

func TestExecPanics(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		store Store
		act   core.Action
		want  string
	}{
		{&scriptStore{}, core.StartRun{}, "tenure: not a store action core.StartRun"},
		{&scriptStore{}, core.Wake{}, "tenure: not a store action core.Wake"},
		{struct{ Store }{&scriptStore{}}, core.ClaimSlot{}, "tenure: core.ClaimSlot needs a SlotStore"},
	} {
		e := newExecutor(ctx, tc.store, core.DefaultTiming, time.Now(), nil)
		func() {
			defer func() {
				if got := fmt.Sprint(recover()); got != tc.want {
					t.Errorf("panic %q, want %q", got, tc.want)
				}
			}()
			e.do(1, execHolder, tc.act)
		}()
	}
}
