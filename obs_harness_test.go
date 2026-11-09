// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// OBS §10's harness for the event tests. Every test runs inside a testing/synctest bubble with one
// memstore.New(nil) and one static list, which lists no member unless the test names some (with
// the data each node joins with, so that their Join does not change the view, whose Version stays
// 1); each node joins it through a Share of its own (the group's recMembership, sm) and gets its
// own recStore over the memstore (fs),
// wrapped in faulty, which injects errors per method. Each node has core.DefaultTiming, space "s"
// with 2 unfenced shards served by a runLog of its own, a holder drawn from a fixed Rand
// (obsHolderA for "a", obsHolderB for "b"), a recording OnEvent (eventRec) and a Logger that
// captures each record as JSON without its time (logCapture). Each node ends with Close and the
// test with the baseline check.

// The holders of members a and b in their first incarnation: the member ID, "/" and 16 bytes of
// 0x11 or 0x22 in lowercase hex.
const (
	obsHolderA = "a/11111111111111111111111111111111"
	obsHolderB = "b/22222222222222222222222222222222"
)

// obsSpace is the name of the harness's space.
const obsSpace = "s"

// obsHarness holds what every node of a test shares: the memstore and the group.
type obsHarness struct {
	t      *testing.T
	ms     *memstore.Store
	g      *group
	listed map[string]bool // the IDs the group lists from the start
}

// newObsHarness returns a harness over a new memstore and a group that lists the members listed,
// none by default, each as the harness's Node gives its Self.
func newObsHarness(t *testing.T, listed ...string) *obsHarness {
	t.Helper()
	members := make([]core.Member, len(listed))
	h := &obsHarness{t: t, ms: memstore.New(nil), listed: make(map[string]bool, len(listed))}
	for i, id := range listed {
		members[i] = obsSelf(id)
		h.listed[id] = true
	}
	h.g = newGroup(t, members...)
	return h
}

// obsSelf returns the Self of the harness's member id.
func obsSelf(id string) tenure.Member {
	return tenure.Member{ID: id, Addr: id + ":1"}
}

// obsNode is one node of the harness with its doubles. Node is nil until Start has succeeded.
type obsNode struct {
	*tenure.Node
	t      *testing.T
	ID     string
	Holder string // the holder of the first incarnation
	M      *recMembership
	FS     *recStore // the store's log and script
	Store  *faulty   // what the node gets as its Store
	Runs   *runLog
	Events *eventRec
	Log    *logCapture
	Cfg    tenure.Config // the Config Start gets; a test may change it before Start
}

// Node returns the doubles and the Config of member id ("a", "b", "c", …), not started. Each
// option changes the Config.
func (h *obsHarness) Node(id string, opts ...func(*tenure.Config)) *obsNode {
	h.t.Helper()
	fs := newRecStore(h.ms)
	n := &obsNode{
		t:      h.t,
		ID:     id,
		Holder: id + "/" + strings.Repeat(fmt.Sprintf("%02x", obsRandBase(id)), 16),
		M:      h.membership(id),
		FS:     fs,
		Store:  newFaulty(fs),
		Runs:   &runLog{},
		Events: &eventRec{},
		Log:    &logCapture{},
	}
	n.Cfg = tenure.Config{
		Self:       obsSelf(id),
		Membership: n.M,
		Store:      n.Store,
		Spaces:     []tenure.Space{{Name: obsSpace, Shards: 2, Run: n.Runs.Run}},
		Timing:     core.DefaultTiming,
		Rand:       &obsRand{base: obsRandBase(id)},
		OnEvent:    n.Events.OnEvent,
		Logger:     n.Log.Logger(),
	}
	for _, o := range opts {
		o(&n.Cfg)
	}
	return n
}

// membership returns the recMembership of member id: over a Share of the list, as a listed member
// or one the list does not hold yet.
func (h *obsHarness) membership(id string) *recMembership {
	if h.listed[id] {
		return h.g.Node(id)
	}
	return h.g.Absent(id)
}

// Start starts member id as Node does and fails t unless Start succeeds.
func (h *obsHarness) Start(id string, opts ...func(*tenure.Config)) *obsNode {
	h.t.Helper()
	n := h.Node(id, opts...)
	if err := n.Start(context.Background()); err != nil {
		h.t.Fatalf("Start(%s) = %v, want nil", id, err)
	}
	return n
}

// Start starts the node with its Config and ctx, and returns Start's error.
func (n *obsNode) Start(ctx context.Context) error {
	node, err := tenure.Start(ctx, n.Cfg)
	if err == nil {
		n.Node = node
	}
	return err
}

// MustClose closes the node with a background context and fails t unless Close returns nil.
func (n *obsNode) MustClose() {
	n.t.Helper()
	if err := n.Close(context.Background()); err != nil {
		n.t.Fatalf("Close(%s) = %v, want nil", n.ID, err)
	}
}

// obsRandBase returns the byte of id's first incarnation: 0x11 for "a", 0x22 for "b", and so on
// by the first letter.
func obsRandBase(id string) byte {
	return (id[0] - 'a' + 1) * 0x11
}

// obsRand is a node's fixed Rand: the k-th incarnation it draws (from 0) is 16 bytes of base+k,
// so a new incarnation after a loss has a new holder.
type obsRand struct {
	mu   sync.Mutex
	base byte // the byte of the current incarnation
	read int  // the bytes of it read so far
}

func (r *obsRand) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range p {
		if r.read == 16 {
			r.base, r.read = r.base+1, 0
		}
		p[i] = r.base
		r.read++
	}
	return len(p), nil
}

// faulty is a recStore, as a tenure.Watcher over the memstore, whose calls of a method can be made
// to fail with an injected error. It scripts the recStore with Answer, so a test that injects
// errors into a method does not also Answer it. The calls stay in the recStore's log, with their
// errors.
type faulty struct {
	*watchingStore

	mu     sync.Mutex
	faults map[string]*injection
}

// injection is the injected error of one method.
type injection struct {
	err  error
	left int // the calls still to fail; < 0: every call
}

// newFaulty returns fs as a faulty store that injects no error yet.
func newFaulty(fs *recStore) *faulty {
	return &faulty{watchingStore: fs.Watching(), faults: make(map[string]*injection)}
}

// Fail makes the next times calls of method fail with err, without reaching the memstore, and
// every later call when times is 0 or less, until Heal.
func (f *faulty) Fail(method string, err error, times int) {
	f.mu.Lock()
	if times <= 0 {
		times = -1
	}
	f.faults[method] = &injection{err: err, left: times}
	f.mu.Unlock()
	f.Answer(method, func(ctx context.Context, args []any) ([]any, error) {
		if err := f.take(method); err != nil {
			return nil, err
		}
		return f.Forward(ctx, method, args)
	})
}

// Heal ends the errors of method: its later calls reach the memstore.
func (f *faulty) Heal(method string) {
	f.mu.Lock()
	delete(f.faults, method)
	f.mu.Unlock()
	f.Answer(method, nil)
}

// take returns the error the next call of method gets, nil when it reaches the memstore.
func (f *faulty) take(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ft := f.faults[method]
	if ft == nil || ft.left == 0 {
		return nil
	}
	if ft.left > 0 {
		ft.left--
	}
	return ft.err
}

// eventCall is one call of an eventRec's OnEvent.
type eventCall struct {
	Event       tenure.Event
	Start, End  time.Time // bubble times; End is zero while the call runs
	Seq, EndSeq uint64    // stamps from seq at the start and the return
	Done        bool
}

// eventRec is the harness's OnEvent: it records every call and how many ran at once, and can run
// a hook in each call or hold calls until released. The methods are safe for concurrent use.
type eventRec struct {
	mu    sync.Mutex
	calls []*eventCall
	in    int // calls in progress
	most  int // the most calls in progress at once
	hook  func(i int, e tenure.Event)
	from  int           // with gate, calls from the from-th on (from 1) wait for it
	gate  chan struct{} // nil: no call waits
}

// OnEvent is a Config.OnEvent that records the call, then waits for a BlockFrom's release, then
// runs the hook.
func (r *eventRec) OnEvent(e tenure.Event) {
	c := &eventCall{Event: e, Start: time.Now(), Seq: seq()}
	r.mu.Lock()
	r.calls = append(r.calls, c)
	i := len(r.calls)
	r.in++
	r.most = max(r.most, r.in)
	gate, hook := r.gate, r.hook
	if i < r.from {
		gate = nil
	}
	r.mu.Unlock()

	if gate != nil {
		<-gate
	}
	if hook != nil {
		hook(i, e)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.in--
	c.End, c.EndSeq, c.Done = time.Now(), seq(), true
}

// Hook makes f run in every later call, with the call's number (from 1) and its event; nil ends
// it. f runs on the node's delivery goroutine and may block it.
func (r *eventRec) Hook(f func(i int, e tenure.Event)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hook = f
}

// BlockFrom makes the n-th call (from 1) and every later one wait until release is called.
// release may be called more than once.
func (r *eventRec) BlockFrom(n int) (release func()) {
	gate := make(chan struct{})
	r.mu.Lock()
	r.from, r.gate = n, gate
	r.mu.Unlock()
	return sync.OnceFunc(func() {
		close(gate)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.gate == gate {
			r.gate = nil
		}
	})
}

// Calls returns a copy of the recorded calls, in call order.
func (r *eventRec) Calls() []eventCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]eventCall, len(r.calls))
	for i, c := range r.calls {
		out[i] = *c
	}
	return out
}

// Events returns the event of each recorded call, in call order.
func (r *eventRec) Events() []tenure.Event {
	calls := r.Calls()
	out := make([]tenure.Event, len(calls))
	for i, c := range calls {
		out[i] = c.Event
	}
	return out
}

// Most returns the most calls that ran at once.
func (r *eventRec) Most() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.most
}

// logRecord is one record a logCapture took: its JSON object without the time and without the
// final newline, and its stamp from seq.
type logRecord struct {
	JSON string
	Seq  uint64
}

// logCapture is the harness's log handler: a slog.JSONHandler of every level whose records it
// keeps, with the time dropped, so that a test can compare exact records. The methods are safe
// for concurrent use.
type logCapture struct {
	mu   sync.Mutex
	recs []logRecord
}

// Logger returns a logger that writes to c.
func (c *logCapture) Logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(c, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
}

// Write keeps one record: the JSON handler writes each record with one call.
func (c *logCapture) Write(p []byte) (int, error) {
	r := logRecord{JSON: strings.TrimSuffix(string(p), "\n"), Seq: seq()}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r)
	return len(p), nil
}

// Records returns a copy of the records, in order.
func (c *logCapture) Records() []logRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.recs)
}

// Lines returns the JSON of each record, in order.
func (c *logCapture) Lines() []string {
	recs := c.Records()
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.JSON
	}
	return out
}

// wantDelivery fails t unless the node's OnEvent got its events with Seq 1, 2, … in order, one
// call at a time, each after the previous call returned, and its log has one record per call,
// written after the previous call returned and before its own call started.
func wantDelivery(t *testing.T, n *obsNode) {
	t.Helper()
	calls, recs := n.Events.Calls(), n.Log.Records()
	if m := n.Events.Most(); m > 1 {
		t.Errorf("%s: %d OnEvent calls ran at once, want 1", n.ID, m)
	}
	if len(recs) != len(calls) {
		t.Errorf("%s: %d log records for %d OnEvent calls, want as many", n.ID, len(recs), len(calls))
	}
	for i, c := range calls {
		if want := uint64(i + 1); c.Event.Seq != want {
			t.Errorf("%s: OnEvent call %d got Seq %d, want %d", n.ID, i+1, c.Event.Seq, want)
		}
		if i > 0 && (!calls[i-1].Done || calls[i-1].EndSeq > c.Seq) {
			t.Errorf("%s: OnEvent call %d started before call %d returned", n.ID, i+1, i)
		}
		if i < len(recs) {
			if recs[i].Seq > c.Seq {
				t.Errorf("%s: record %d written after its OnEvent call started", n.ID, i+1)
			}
			if i > 0 && recs[i].Seq < calls[i-1].EndSeq {
				t.Errorf("%s: record %d written before OnEvent call %d returned", n.ID, i+1, i)
			}
		}
	}
}

// ofKinds returns the events of the listed kinds, in order.
func ofKinds(events []tenure.Event, kinds ...tenure.EventKind) []tenure.Event {
	var out []tenure.Event
	for _, e := range events {
		if slices.Contains(kinds, e.Kind) {
			out = append(out, e)
		}
	}
	return out
}

// bare returns e without Seq and Time, for comparing the other fields.
func bare(e tenure.Event) tenure.Event {
	e.Seq, e.Time = 0, time.Time{}
	return e
}

// bareAll returns bare of each event.
func bareAll(events []tenure.Event) []tenure.Event {
	out := make([]tenure.Event, len(events))
	for i, e := range events {
		out[i] = bare(e)
	}
	return out
}

// nodeKinds are the node lifecycle kinds.
var nodeKinds = []tenure.EventKind{tenure.EventNodeDraining, tenure.EventNodeDrained, tenure.EventNodeClosed}
