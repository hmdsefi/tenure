// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/static"
)

// The doubles of the node-level tests (RUN §10). A test runs a node against a recStore (fs) over
// one memstore shared by every node, and a recMembership (sm) from a group over one static list.
// Both record every call and can be scripted per method name. Everything here runs inside a
// testing/synctest bubble: times are bubble times.

// callSeq stamps the start and the end of every recorded call, across all doubles.
var callSeq atomic.Uint64

// seq returns the next stamp of the counter that orders recorded calls. A test can stamp its own
// observations, such as a step hook, to order them against calls made at the same bubble instant.
func seq() uint64 { return callSeq.Add(1) }

// call is one recorded call of a double.
type call struct {
	Method string
	Args   []any // the arguments after ctx, in parameter order

	Deadline    time.Time // ctx's deadline, if HasDeadline
	HasDeadline bool

	Start, End  time.Time // bubble times; End is zero while the call is in flight
	Seq, EndSeq uint64    // stamps from seq at the start and the end
	Done        bool      // the call has returned

	Result []any // the results before the error, in order
	Err    error
}

// String formats c for failure messages, such as `Acquire(n1/1, orders, 3) = [5], <nil>`.
func (c call) String() string {
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = fmt.Sprint(a)
	}
	s := c.Method + "(" + strings.Join(args, ", ") + ")"
	if !c.Done {
		return s + " in flight"
	}
	return fmt.Sprintf("%s = %v, %v", s, c.Result, c.Err)
}

// callsInOrder merges logs of several doubles into one, ordered by start stamp.
func callsInOrder(logs ...[]call) []call {
	all := slices.Concat(logs...)
	slices.SortFunc(all, func(a, b call) int { return cmp.Compare(a.Seq, b.Seq) })
	return all
}

// callMethods returns the method of each call, in order.
func callMethods(calls []call) []string {
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = c.Method
	}
	return out
}

// answerFunc replaces a call: it gets the call's ctx and arguments and returns its results
// before the error, and the error.
type answerFunc func(ctx context.Context, args []any) ([]any, error)

// callRule is the script of one method.
type callRule struct {
	fail   error
	gate   chan struct{}
	delay  time.Duration
	answer answerFunc
}

// script records the calls of a double and scripts them per method name. A call follows the
// script in place when it starts: it waits for a Block's release, then sleeps a Delay, both
// ignoring ctx; then it returns the Fail error, or else the Answer's results, or else forwards.
// The methods are safe for concurrent use.
type script struct {
	mu    sync.Mutex
	log   []*call
	rules map[string]callRule
}

// Fail makes every later call of method return err without forwarding; nil ends it.
func (s *script) Fail(method string, err error) {
	s.edit(method, func(r *callRule) { r.fail = err })
}

// Block holds every later call of method until release is called, past ctx's deadline. release
// may be called more than once.
func (s *script) Block(method string) (release func()) {
	gate := make(chan struct{})
	s.edit(method, func(r *callRule) { r.gate = gate })
	var once sync.Once
	return func() {
		once.Do(func() {
			close(gate)
			s.edit(method, func(r *callRule) {
				if r.gate == gate {
					r.gate = nil
				}
			})
		})
	}
}

// Delay makes every later call of method sleep d before it goes on, past ctx's deadline; 0 ends
// it.
func (s *script) Delay(method string, d time.Duration) {
	s.edit(method, func(r *callRule) { r.delay = d })
}

// Answer makes f replace every later call of method; nil ends it. f's results must have the
// method's result types, for example core.Term for Acquire and <-chan core.View for Watch.
func (s *script) Answer(method string, f answerFunc) {
	s.edit(method, func(r *callRule) { r.answer = f })
}

// Calls returns a copy of the recorded calls of the given methods, or of every method when none
// is given, in start order.
func (s *script) Calls(methods ...string) []call {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []call
	for _, c := range s.log {
		if len(methods) == 0 || slices.Contains(methods, c.Method) {
			out = append(out, *c)
		}
	}
	return out
}

// edit changes the rule of method under the lock.
func (s *script) edit(method string, f func(r *callRule)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rules == nil {
		s.rules = make(map[string]callRule)
	}
	r := s.rules[method]
	f(&r)
	s.rules[method] = r
}

// do records one call of method and runs it by the script, with forward as the real call.
func (s *script) do(ctx context.Context, method string, args []any, forward func(context.Context) ([]any, error)) ([]any, error) {
	c := &call{Method: method, Args: snapshot(args), Start: time.Now()}
	c.Deadline, c.HasDeadline = ctx.Deadline()
	s.mu.Lock()
	r := s.rules[method]
	c.Seq = seq()
	s.log = append(s.log, c)
	s.mu.Unlock()

	if r.gate != nil {
		<-r.gate
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	var res []any
	var err error
	switch {
	case r.fail != nil:
		err = r.fail
	case r.answer != nil:
		res, err = r.answer(ctx, args)
	default:
		res, err = forward(ctx)
	}

	s.mu.Lock()
	c.End, c.EndSeq, c.Done, c.Result, c.Err = time.Now(), seq(), true, snapshot(res), err
	s.mu.Unlock()
	return res, err
}

// snapshot returns a copy of vs for the log, with its own copies of slices and Meta maps, so the
// log shares no memory with what the caller passed or got, which the caller may change later.
func snapshot(vs []any) []any {
	if vs == nil {
		return nil
	}
	out := make([]any, len(vs))
	for i, v := range vs {
		switch v := v.(type) {
		case []string:
			out[i] = slices.Clone(v)
		case []tenure.Holding:
			out[i] = slices.Clone(v)
		case core.Member:
			v.Meta = maps.Clone(v.Meta)
			out[i] = v
		default:
			out[i] = v
		}
	}
	return out
}

// argAt returns args[i] as a T and panics when it is not one.
func argAt[T any](args []any, i int) T {
	v, ok := args[i].(T)
	if !ok {
		panic(fmt.Sprintf("argument %d is %T, want %T", i, args[i], v))
	}
	return v
}

// resultAt returns res[i] as a T: the zero T when res has no such entry or it is nil, and a panic
// when it is of another type.
func resultAt[T any](res []any, i int) T {
	var zero T
	if i >= len(res) || res[i] == nil {
		return zero
	}
	v, ok := res[i].(T)
	if !ok {
		panic(fmt.Sprintf("result %d is %T, want %T", i, res[i], zero))
	}
	return v
}

// recStore is RUN §10's fs: a tenure.Store and tenure.SlotStore that records every call and
// forwards it to a memstore. Its variants share its log and script.
type recStore struct {
	script
	ms *memstore.Store

	injectOnce sync.Once
	injected   *injectedStore // what Injected returns
}

var (
	_ tenure.Store     = (*recStore)(nil)
	_ tenure.SlotStore = (*recStore)(nil)
	_ tenure.Watcher   = (*watchingStore)(nil)
	_ tenure.SlotStore = (*watchingStore)(nil)
	_ tenure.Watcher   = (*injectedStore)(nil)
	_ tenure.SlotStore = (*injectedStore)(nil)
	_ tenure.SlotStore = (*checkingStore)(nil)
	_ tenure.Store     = (*storeOnly)(nil)
)

// newRecStore returns a recStore over ms.
func newRecStore(ms *memstore.Store) *recStore {
	return &recStore{ms: ms}
}

// Forward makes the call of method with args on the memstore, without recording it. An Answer
// can use it to pass a call on.
func (s *recStore) Forward(ctx context.Context, method string, args []any) ([]any, error) {
	switch method {
	case "KeepAlive":
		return nil, s.ms.KeepAlive(ctx, argAt[string](args, 0), argAt[core.Timing](args, 1))
	case "Acquire":
		term, err := s.ms.Acquire(ctx, argAt[string](args, 0), argAt[string](args, 1), argAt[int](args, 2))
		return []any{term}, err
	case "Release":
		return nil, s.ms.Release(ctx, argAt[string](args, 0), argAt[string](args, 1), argAt[int](args, 2),
			argAt[core.Term](args, 3))
	case "Confirm":
		h, err := s.ms.Confirm(ctx, argAt[string](args, 0), argAt[string](args, 1), argAt[int](args, 2),
			argAt[core.Term](args, 3))
		return []any{h}, err
	case "CloseSession":
		return nil, s.ms.CloseSession(ctx, argAt[string](args, 0))
	case "Holdings":
		hs, err := s.ms.Holdings(ctx, argAt[string](args, 0))
		return []any{hs}, err
	case "ClaimSlot":
		slot, err := s.ms.ClaimSlot(ctx, argAt[string](args, 0))
		return []any{slot}, err
	case "ReleaseSlot":
		return nil, s.ms.ReleaseSlot(ctx, argAt[string](args, 0), argAt[int](args, 1))
	case "Slots":
		count, holders, err := s.ms.Slots(ctx)
		return []any{count, holders}, err
	case "SetSlots":
		return nil, s.ms.SetSlots(ctx, argAt[int](args, 0))
	case "WatchHolders":
		ch, err := s.ms.WatchHolders(ctx)
		return []any{ch}, err
	}
	panic("recStore: Forward of unknown method " + method)
}

// call records and runs one call that forwards to the memstore.
func (s *recStore) call(ctx context.Context, method string, args ...any) ([]any, error) {
	return s.do(ctx, method, args, func(ctx context.Context) ([]any, error) {
		return s.Forward(ctx, method, args)
	})
}

func (s *recStore) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	_, err := s.call(ctx, "KeepAlive", holder, timing)
	return err
}

func (s *recStore) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	res, err := s.call(ctx, "Acquire", holder, space, shard)
	return resultAt[core.Term](res, 0), err
}

func (s *recStore) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	_, err := s.call(ctx, "Release", holder, space, shard, term)
	return err
}

func (s *recStore) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	res, err := s.call(ctx, "Confirm", holder, space, shard, term)
	return resultAt[tenure.Holding](res, 0), err
}

func (s *recStore) CloseSession(ctx context.Context, holder string) error {
	_, err := s.call(ctx, "CloseSession", holder)
	return err
}

func (s *recStore) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	res, err := s.call(ctx, "Holdings", space)
	return resultAt[[]tenure.Holding](res, 0), err
}

func (s *recStore) ClaimSlot(ctx context.Context, holder string) (int, error) {
	res, err := s.call(ctx, "ClaimSlot", holder)
	return resultAt[int](res, 0), err
}

func (s *recStore) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	_, err := s.call(ctx, "ReleaseSlot", holder, slot)
	return err
}

func (s *recStore) Slots(ctx context.Context) (count int, holders []string, err error) {
	res, err := s.call(ctx, "Slots")
	return resultAt[int](res, 0), resultAt[[]string](res, 1), err
}

func (s *recStore) SetSlots(ctx context.Context, count int) error {
	_, err := s.call(ctx, "SetSlots", count)
	return err
}

// watchingStore is a recStore that is also a tenure.Watcher forwarding to the memstore.
type watchingStore struct{ *recStore }

// Watching returns s as a tenure.Watcher whose WatchHolders forwards to the memstore.
func (s *recStore) Watching() *watchingStore { return &watchingStore{s} }

func (s *watchingStore) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	res, err := s.call(ctx, "WatchHolders")
	return resultAt[<-chan tenure.Holding](res, 0), err
}

// injectedStore is a recStore that is also a tenure.Watcher whose channels receive only the
// values the test injects.
type injectedStore struct {
	*recStore
	mu      sync.Mutex
	watches []*holdingWatch // open channels, in creation order
}

// holdingWatch is one channel of an injectedStore.
type holdingWatch struct {
	ch   chan tenure.Holding
	stop func() bool // unregisters the context.AfterFunc that closes ch
}

// Injected returns s as a tenure.Watcher whose WatchHolders channels receive only what Inject
// sends, and close at CloseWatch or when their ctx is done. Every call returns the same value, so
// Inject and CloseWatch reach the channels of the node that was given an earlier result.
func (s *recStore) Injected() *injectedStore {
	s.injectOnce.Do(func() { s.injected = &injectedStore{recStore: s} })
	return s.injected
}

func (s *injectedStore) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	res, err := s.do(ctx, "WatchHolders", nil, s.open)
	return resultAt[<-chan tenure.Holding](res, 0), err
}

// open makes a new channel with memstore's buffer of 1024.
func (s *injectedStore) open(ctx context.Context) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w := &holdingWatch{ch: make(chan tenure.Holding, 1024)}
	s.watches = append(s.watches, w)
	w.stop = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closeLocked(w)
	})
	return []any{(<-chan tenure.Holding)(w.ch)}, nil
}

// Inject sends h to every open channel without waiting; a channel whose buffer is full is
// closed instead, as memstore does.
func (s *injectedStore) Inject(h tenure.Holding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range slices.Clone(s.watches) {
		select {
		case w.ch <- h:
		default:
			s.closeLocked(w)
		}
	}
}

// CloseWatch closes every open channel, as a store that cannot deliver every write does.
func (s *injectedStore) CloseWatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range slices.Clone(s.watches) {
		s.closeLocked(w)
	}
}

// closeLocked closes w unless that has happened already. The caller holds s.mu.
func (s *injectedStore) closeLocked(w *holdingWatch) {
	i := slices.Index(s.watches, w)
	if i < 0 {
		return
	}
	s.watches = slices.Delete(s.watches, i, i+1)
	close(w.ch)
	w.stop()
}

// checkingStore is a recStore with CheckNames and CheckTiming methods.
type checkingStore struct {
	*recStore
	names, timing error
}

// Checking returns s with CheckNames and CheckTiming methods that record their calls and return
// names and timing.
func (s *recStore) Checking(names, timing error) *checkingStore {
	return &checkingStore{recStore: s, names: names, timing: timing}
}

func (s *checkingStore) CheckNames(spaces []string, member string) error {
	_, err := s.do(context.Background(), "CheckNames", []any{spaces, member}, answerErr(s.names))
	return err
}

func (s *checkingStore) CheckTiming(t core.Timing) error {
	_, err := s.do(context.Background(), "CheckTiming", []any{t}, answerErr(s.timing))
	return err
}

// answerErr returns a forward function that returns err.
func answerErr(err error) func(context.Context) ([]any, error) {
	return func(context.Context) ([]any, error) { return nil, err }
}

// storeOnly is a recStore seen only as a tenure.Store: no SlotStore, no Watcher.
type storeOnly struct{ s *recStore }

// StoreOnly returns s as a plain tenure.Store, hiding its SlotStore methods. Calls are recorded in
// s's log.
func (s *recStore) StoreOnly() tenure.Store { return storeOnly{s} }

func (o storeOnly) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	return o.s.KeepAlive(ctx, holder, timing)
}

func (o storeOnly) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	return o.s.Acquire(ctx, holder, space, shard)
}

func (o storeOnly) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	return o.s.Release(ctx, holder, space, shard, term)
}

func (o storeOnly) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	return o.s.Confirm(ctx, holder, space, shard, term)
}

func (o storeOnly) CloseSession(ctx context.Context, holder string) error {
	return o.s.CloseSession(ctx, holder)
}

func (o storeOnly) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	return o.s.Holdings(ctx, space)
}

// group is one static list that every node of a test shares. Every change to the list goes
// through the group's helpers or its recMemberships, and each of them then moves the new view
// into the channels of every recMembership of the group: as with static itself, a change reaches
// every channel inside the call that makes it.
type group struct {
	t       testing.TB
	list    *static.Membership
	listed  map[string]bool               // IDs newGroup listed
	helpers map[string]*static.Membership // handles joined by Join, SetDraining or Leave, by ID

	mu    sync.Mutex
	nodes []*recMembership // every recMembership that Node and Absent returned
}

// newGroup returns a group over one static.New list of members, failing t on error.
func newGroup(t testing.TB, members ...core.Member) *group {
	t.Helper()
	list, err := static.New(members)
	if err != nil {
		t.Fatalf("static.New(%v) = %v, want nil", members, err)
	}
	g := &group{t: t, list: list, listed: make(map[string]bool), helpers: make(map[string]*static.Membership)}
	for _, m := range members {
		g.listed[m.ID] = true
	}
	return g
}

// Node returns RUN §10's sm for the listed member id: a recMembership over a new Share of the
// list.
func (g *group) Node(id string) *recMembership {
	g.t.Helper()
	if !g.listed[id] {
		g.t.Fatalf("group.Node(%q): not listed; use Absent", id)
	}
	return g.membership(id)
}

// Absent returns a recMembership, over a new Share of the list, for a member id the list does
// not hold: it appears in the views only once it joins.
func (g *group) Absent(id string) *recMembership {
	g.t.Helper()
	if g.listed[id] {
		g.t.Fatalf("group.Absent(%q): listed; use Node", id)
	}
	return g.membership(id)
}

// membership returns a new recMembership for id over a new Share of the list.
func (g *group) membership(id string) *recMembership {
	m := &recMembership{t: g.t, id: id, g: g, h: g.list.Share()}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nodes = append(g.nodes, m)
	return m
}

// flush moves what static has put in its channels since the last flush, the latest view and the
// close, into the channels of every recMembership of the group. Every call that may change the
// list calls it before it returns.
func (g *group) flush() {
	g.mu.Lock()
	nodes := slices.Clone(g.nodes)
	g.mu.Unlock()
	for _, m := range nodes {
		m.flush()
	}
}

// View returns the group's current view.
func (g *group) View() core.View {
	g.t.Helper()
	h := g.list.Share()
	ch, err := h.Watch(context.Background())
	if err != nil {
		g.t.Fatalf("group.View: Watch() = %v, want nil", err)
	}
	v := <-ch
	if err := h.Leave(context.Background()); err != nil {
		g.t.Fatalf("group.View: Leave() = %v, want nil", err)
	}
	return v
}

// Join joins m through a helper handle of its own, without recording: a member that is not a node
// of the test enters the views, or a listed one changes its data. It fails t when a node or an
// earlier Join or SetDraining has joined m.ID.
func (g *group) Join(m core.Member) {
	g.t.Helper()
	if g.helpers[m.ID] != nil {
		g.t.Fatalf("group.Join(%q): already joined by the group", m.ID)
	}
	h := g.list.Share()
	err := h.Join(context.Background(), m)
	g.flush()
	if err != nil {
		g.t.Fatalf("group.Join(%q) = %v, want nil", m.ID, err)
	}
	g.helpers[m.ID] = h
}

// Leave removes member id from the views, without recording. The member is one that Join or
// SetDraining joined, or one in the view that no node has joined.
func (g *group) Leave(id string) {
	g.t.Helper()
	h := g.helper(id)
	err := h.Leave(context.Background())
	g.flush()
	if err != nil {
		g.t.Fatalf("group.Leave(%q) = %v, want nil", id, err)
	}
	delete(g.helpers, id)
}

// SetDraining sets Draining of member id, without recording. The member is one that Join joined,
// or one in the view that no node has joined; that one stays joined by a helper handle.
func (g *group) SetDraining(id string, draining bool) {
	g.t.Helper()
	err := g.helper(id).SetDraining(context.Background(), draining)
	g.flush()
	if err != nil {
		g.t.Fatalf("group.SetDraining(%q, %t) = %v, want nil", id, draining, err)
	}
}

// helper returns the helper handle of id, joining one with id's current data when there is none.
func (g *group) helper(id string) *static.Membership {
	g.t.Helper()
	if h := g.helpers[id]; h != nil {
		return h
	}
	members := g.View().Members
	i := slices.IndexFunc(members, func(m core.Member) bool { return m.ID == id })
	if i < 0 {
		g.t.Fatalf("group: member %q is not in the view", id)
	}
	g.Join(members[i])
	return g.helpers[id]
}

// recMembership is RUN §10's sm: a tenure.Membership that records every call and forwards it to
// its static handle. Each channel Watch returns is a copy of a static channel that the group fills
// inside every call that changes the list, so a view reaches it as it reaches a static channel:
// inside the call that makes it. Pause and CloseWatch act on the copy only. A recMembership starts
// no goroutine.
type recMembership struct {
	script
	t  testing.TB
	id string
	g  *group
	h  *static.Membership

	mu      sync.Mutex
	paused  bool
	watches []*viewWatch // every channel Watch returned, in order
}

var _ tenure.Membership = (*recMembership)(nil)

// viewWatch is one channel of a recMembership. Its fields are guarded by the recMembership's mu.
type viewWatch struct {
	in      <-chan core.View // the static channel
	out     chan core.View   // buffer 1; a newer view replaces one not yet received
	held    core.View        // the latest view held back by Pause
	holding bool
	closed  bool
	stop    func() bool // unregisters the context.AfterFunc that closes out
}

func (m *recMembership) Join(ctx context.Context, self core.Member) error {
	_, err := m.do(ctx, "Join", []any{self}, func(ctx context.Context) ([]any, error) {
		err := m.h.Join(ctx, self)
		m.g.flush()
		return nil, err
	})
	return err
}

func (m *recMembership) Watch(ctx context.Context) (<-chan core.View, error) {
	res, err := m.do(ctx, "Watch", nil, m.watch)
	return resultAt[<-chan core.View](res, 0), err
}

func (m *recMembership) SetDraining(ctx context.Context, draining bool) error {
	_, err := m.do(ctx, "SetDraining", []any{draining}, func(ctx context.Context) ([]any, error) {
		err := m.h.SetDraining(ctx, draining)
		m.g.flush()
		return nil, err
	})
	return err
}

// Leave forwards to the static handle, which delivers the view without the member and closes
// every channel of the handle; both reach the copies before Leave returns.
func (m *recMembership) Leave(ctx context.Context) error {
	_, err := m.do(ctx, "Leave", nil, func(ctx context.Context) ([]any, error) {
		err := m.h.Leave(ctx)
		m.g.flush()
		return nil, err
	})
	return err
}

// watch opens a static channel and returns its copy, which holds the current view (unless
// paused) and is closed when ctx is done.
func (m *recMembership) watch(ctx context.Context) ([]any, error) {
	in, err := m.h.Watch(ctx)
	if err != nil {
		return nil, err
	}
	w := &viewWatch{in: in, out: make(chan core.View, 1)}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watches = append(m.watches, w)
	m.pullLocked(w) // static holds the current view at once
	w.stop = context.AfterFunc(ctx, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.pullLocked(w)
		m.closeLocked(w)
	})
	return []any{(<-chan core.View)(w.out)}, nil
}

// flush moves what static has put in each open channel of m into its copy.
func (m *recMembership) flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.watches {
		m.pullLocked(w)
	}
}

// pullLocked moves what static has put in w's static channel into w: the latest view, and the
// close. The caller holds m.mu, so views reach w in the order static made them.
func (m *recMembership) pullLocked(w *viewWatch) {
	for !w.closed {
		select {
		case v, ok := <-w.in:
			if !ok {
				m.closeLocked(w)
				return
			}
			m.deliverLocked(w, v)
		default:
			return
		}
	}
}

// deliverLocked sends v to w, replacing a view not yet received, or holds it while paused. The
// caller holds m.mu.
func (m *recMembership) deliverLocked(w *viewWatch, v core.View) {
	switch {
	case w.closed:
	case m.paused:
		w.held, w.holding = v, true
	default:
		sendLatest(w.out, v)
	}
}

// sendLatest puts v in ch, which has buffer 1 and only one sender, replacing a value not yet
// received.
func sendLatest(ch chan core.View, v core.View) {
	select {
	case <-ch:
	default:
	}
	ch <- v
}

// closeLocked closes w unless it is closed already. A view not yet received stays in it. The
// caller holds m.mu.
func (m *recMembership) closeLocked(w *viewWatch) {
	if !w.closed {
		w.closed, w.holding = true, false
		close(w.out)
		if w.stop != nil {
			w.stop()
		}
	}
}

// Pause holds every later view back from every channel until Resume. A view made before Pause
// has been delivered already.
func (m *recMembership) Pause() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.watches {
		m.pullLocked(w)
	}
	m.paused = true
}

// Resume ends Pause and delivers to each open channel only the latest view held back, if any.
func (m *recMembership) Resume() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.watches {
		m.pullLocked(w) // still paused: held back
	}
	m.paused = false
	for _, w := range m.watches {
		if w.holding && !w.closed {
			sendLatest(w.out, w.held)
		}
		w.holding = false
	}
}

// CloseWatch closes the i-th channel Watch returned (from 0), as a membership whose watch broke
// does; a view not yet received stays in it. The static channel behind it stays open until its
// ctx is done or Leave, but nothing more reaches the closed copy.
func (m *recMembership) CloseWatch(i int) {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if i < 0 || i >= len(m.watches) {
		m.t.Errorf("recMembership(%q).CloseWatch(%d): Watch returned %d channels", m.id, i, len(m.watches))
		return
	}
	m.pullLocked(m.watches[i])
	m.closeLocked(m.watches[i])
}

// SetDrainingQuiet sets Draining of the joined member without recording a call or following the
// script, as an operator who marks the member draining behind the node's back.
func (m *recMembership) SetDrainingQuiet(draining bool) {
	m.t.Helper()
	err := m.h.SetDraining(context.Background(), draining)
	m.g.flush()
	if err != nil {
		m.t.Errorf("recMembership(%q).SetDrainingQuiet(%t) = %v, want nil", m.id, draining, err)
	}
}

// checkingMembership is a recMembership with CheckNames and CheckTiming methods.
type checkingMembership struct {
	*recMembership
	names, timing error
}

// Checking returns m with CheckNames and CheckTiming methods that record their calls and return
// names and timing.
func (m *recMembership) Checking(names, timing error) *checkingMembership {
	return &checkingMembership{recMembership: m, names: names, timing: timing}
}

func (m *checkingMembership) CheckNames(spaces []string, member string) error {
	_, err := m.do(context.Background(), "CheckNames", []any{spaces, member}, answerErr(m.names))
	return err
}

func (m *checkingMembership) CheckTiming(t core.Timing) error {
	_, err := m.do(context.Background(), "CheckTiming", []any{t}, answerErr(m.timing))
	return err
}

// outsideHolder is the holder of the outside session.
const outsideHolder = "x/1"

// outside is RUN §10's outside holder: session x/1 on a store, renewed every RetryPeriod, that a
// test uses to take and give back shards behind the nodes' backs.
type outside struct {
	t      testing.TB
	st     tenure.Store
	timing core.Timing

	mu    sync.Mutex
	terms map[string]core.Term // by "space/shard"

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// newOutside opens session x/1 on st with timing and renews it every RetryPeriod until Stop,
// which t's cleanup also calls. st is normally the shared memstore, so the calls are not recorded.
// Its goroutine counts in a baseline check: a test stops it before the check.
func newOutside(t testing.TB, st tenure.Store, timing core.Timing) *outside {
	t.Helper()
	if err := st.KeepAlive(context.Background(), outsideHolder, timing); err != nil {
		t.Fatalf("outside: KeepAlive(%s) = %v, want nil", outsideHolder, err)
	}
	o := &outside{
		t: t, st: st, timing: timing,
		terms: make(map[string]core.Term),
		stop:  make(chan struct{}), done: make(chan struct{}),
	}
	go o.renew()
	t.Cleanup(o.Stop)
	return o
}

// renew renews the session every RetryPeriod until Stop.
func (o *outside) renew() {
	defer close(o.done)
	tick := time.NewTicker(o.timing.RetryPeriod)
	defer tick.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-tick.C:
			_ = o.st.KeepAlive(context.Background(), outsideHolder, o.timing)
		}
	}
}

// Acquire acquires the shard for x/1 and keeps the term for Release.
func (o *outside) Acquire(space string, shard int) (core.Term, error) {
	term, err := o.st.Acquire(context.Background(), outsideHolder, space, shard)
	if err == nil {
		o.mu.Lock()
		o.terms[space+"/"+strconv.Itoa(shard)] = term
		o.mu.Unlock()
	}
	return term, err
}

// Release releases the shard at the term Acquire got for it.
func (o *outside) Release(space string, shard int) error {
	key := space + "/" + strconv.Itoa(shard)
	o.mu.Lock()
	term, ok := o.terms[key]
	delete(o.terms, key)
	o.mu.Unlock()
	if !ok {
		return fmt.Errorf("outside: %s not acquired", key)
	}
	return o.st.Release(context.Background(), outsideHolder, space, shard, term)
}

// Stop ends the renewals and waits for the goroutine to end; the session then expires
// LeaseDuration after its last renewal. Later calls do nothing.
func (o *outside) Stop() {
	o.stopOnce.Do(func() {
		close(o.stop)
		<-o.done
	})
}

// bal returns the balanced owner of each shard of space over the member (or slot) IDs, from
// core.Balanced: the shards of each ID, ascending. An ID with no shard has no entry.
func bal(space string, shards int, ids ...string) map[string][]int {
	out := make(map[string][]int)
	for shard, i := range core.Balanced(space, shards, ids) {
		if i >= 0 {
			out[ids[i]] = append(out[ids[i]], shard)
		}
	}
	return out
}

// goroutineHeader matches the first line of a goroutine in runtime.Stack's output.
var goroutineHeader = regexp.MustCompile(`^goroutine (\d+) \[([^\]]*)\]`)

// bubbleMark matches the bubble a goroutine's state names.
var bubbleMark = regexp.MustCompile(`synctest bubble \d+`)

// baseline notes the goroutines of the caller's synctest bubble. check calls synctest.Wait and
// fails t, listing their stacks, when goroutines of the bubble exist that were not there at the
// start.
func baseline(t testing.TB) (check func()) {
	t.Helper()
	bubble := bubbleMark.FindString(stacks(false))
	if bubble == "" {
		t.Fatal("baseline: not inside a synctest bubble")
	}
	start := goroutines(bubble)
	return func() {
		t.Helper()
		synctest.Wait()
		now := goroutines(bubble)
		var ids []int
		for id := range now {
			if _, ok := start[id]; !ok {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return
		}
		slices.Sort(ids)
		leaked := make([]string, len(ids))
		for i, id := range ids {
			leaked[i] = now[id]
		}
		t.Errorf("baseline: %d goroutine(s) not there at the start:\n\n%s", len(ids), strings.Join(leaked, "\n\n"))
	}
}

// goroutines returns the stack of every goroutine of bubble, by goroutine ID.
func goroutines(bubble string) map[int]string {
	out := make(map[int]string)
	for _, g := range strings.Split(stacks(true), "\n\n") {
		g = strings.TrimSpace(g)
		m := goroutineHeader.FindStringSubmatch(g)
		if m == nil || bubbleMark.FindString(m[2]) != bubble {
			continue
		}
		id, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		out[id] = g
	}
	return out
}

// stacks returns runtime.Stack's output for the caller's goroutine, or for all of them.
func stacks(all bool) string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, all)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// orders returns RUN §10's space orders: 8 shards without fencing, served by run.
func orders(run func(context.Context, *tenure.Shard) error) tenure.Space {
	return tenure.Space{Name: "orders", Shards: 8, Run: run}
}

// runCall is one call of a runLog's Run.
type runCall struct {
	Shard *tenure.Shard // the Shard Run got

	Deadline    time.Time // ctx's deadline, if HasDeadline
	HasDeadline bool

	Start, End  time.Time // bubble times; End is zero while Run runs
	Seq, EndSeq uint64    // stamps from seq at the start and the return
	Done        bool      // Run has returned

	// CtxDone is when ctx was done, zero while it is not. CheckAtDone is what the Shard's Check
	// returned at that instant.
	CtxDone     time.Time
	CheckAtDone error

	Cause error // context.Cause(ctx) when Run returned, nil when ctx was not done then
	Err   error // what Run returned
}

// String formats c for failure messages, such as `Run(orders/3 term 5) = boom (cause <nil>)`.
func (c runCall) String() string {
	s := fmt.Sprintf("Run(%s/%d term %d)", c.Shard.Space, c.Shard.ID, c.Shard.Term)
	if !c.Done {
		return s + " running"
	}
	return fmt.Sprintf("%s = %v (cause %v)", s, c.Err, c.Cause)
}

// runScript replaces the body of a runLog's Run: it gets Run's arguments and the number of
// earlier calls for the same shard of the same space.
type runScript func(ctx context.Context, sh *tenure.Shard, earlier int) error

// runLog is RUN §10's rec: its Run records every call (its start, ctx's deadline, when ctx is
// done, the cause and the return) and, unless a script replaces its body, blocks until ctx is
// done and returns the cause. The methods are safe for concurrent use.
type runLog struct {
	mu     sync.Mutex
	calls  []*runCall
	script runScript
}

// Script makes f the body of every later call; nil restores blocking until ctx is done.
func (l *runLog) Script(f runScript) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.script = f
}

// Run is a Space.Run that records the call and runs the script.
func (l *runLog) Run(ctx context.Context, sh *tenure.Shard) error {
	c := &runCall{Shard: sh, Start: time.Now()}
	c.Deadline, c.HasDeadline = ctx.Deadline()
	l.mu.Lock()
	c.Seq = seq()
	earlier := 0
	for _, o := range l.calls {
		if o.Shard.Space == sh.Space && o.Shard.ID == sh.ID {
			earlier++
		}
	}
	l.calls = append(l.calls, c)
	f := l.script
	l.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
		at, check := time.Now(), sh.Check()
		l.mu.Lock()
		defer l.mu.Unlock()
		c.CtxDone, c.CheckAtDone = at, check
	})
	var err error
	if f != nil {
		err = f(ctx, sh, earlier)
	} else {
		<-ctx.Done()
		err = context.Cause(ctx)
	}
	stop()

	l.mu.Lock()
	defer l.mu.Unlock()
	c.End, c.EndSeq, c.Done, c.Err = time.Now(), seq(), true, err
	if ctx.Err() != nil {
		c.Cause = context.Cause(ctx)
	}
	return err
}

// Calls returns a copy of the recorded calls in start order.
func (l *runLog) Calls() []runCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]runCall, len(l.calls))
	for i, c := range l.calls {
		out[i] = *c
	}
	return out
}

// Of returns a copy of the recorded calls for shard id of space, in start order.
func (l *runLog) Of(space string, id int) []runCall {
	var out []runCall
	for _, c := range l.Calls() {
		if c.Shard.Space == space && c.Shard.ID == id {
			out = append(out, c)
		}
	}
	return out
}
