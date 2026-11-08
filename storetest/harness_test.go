// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
	"github.com/hmdsefi/tenure/storetest"
)

// These tests check how Run treats fixtures unlike memstore's own: stores that hide interfaces,
// real time, a peer that lags, missing capabilities. They run the suite over test-only wrappers
// of memstore: callRecorder logs every call, hider exposes only tenure.Store, and lagStore with
// lagPeer make a peer that lags behind Store.

// Skip messages of STO-830.
const (
	skipAdvance      = "storetest: fixture has no Advance"
	skipSetWall      = "storetest: fixture has no SetWall"
	skipDeleteRecord = "storetest: fixture has no DeleteRecord"
	skipSlots        = "storetest: store does not implement tenure.SlotStore"
	skipWatch        = "storetest: store does not implement tenure.Watcher"
)

// checkOutcomes checks that the child started every case under parent, in order, that the cases
// in skips skipped logging exactly their message, and that every other case passed.
func checkOutcomes(t *testing.T, r childRun, parent string, skips map[string]string) {
	t.Helper()
	if got := r.under(parent); !slices.Equal(got, caseNames) {
		t.Errorf("%s started %q, want %q", parent, got, caseNames)
	}
	for _, name := range caseNames {
		full := parent + "/" + name
		msg, skip := skips[name]
		switch got := r.results[full]; {
		case skip && got != "SKIP":
			t.Errorf("%s: %s, want SKIP", full, got)
		case skip:
			if logs, want := msgs(r.logs[full]), []string{msg}; !slices.Equal(logs, want) {
				t.Errorf("%s logs %q, want %q", full, logs, want)
			}
		case got != "PASS":
			t.Errorf("%s: %s, want PASS", full, got)
		}
	}
}

// caseName returns the name of the case t runs, the last two elements of its test name.
func caseName(t *testing.T) string {
	parts := strings.Split(t.Name(), "/")
	return strings.Join(parts[max(len(parts)-2, 0):], "/")
}

// hider exposes only the tenure.Store methods of the store it wraps.
type hider struct{ tenure.Store }

// TestChildHiddenInterfaces runs the suite on the virtual fixture with Store, then the peer,
// hidden behind a hider. It is child-only; TestATSTO26HiddenInterfaces checks its output.
func TestChildHiddenInterfaces(t *testing.T) {
	skipUnlessChild(t)
	t.Run("store", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			f := newVirtual(t)
			f.Store = hider{f.Store}
			return f
		})
	})
	t.Run("peer", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			f := newVirtual(t)
			peer := f.Peer
			f.Peer = func() tenure.Store { return hider{peer()} }
			return f
		})
	})
}

// TestATSTO26HiddenInterfaces: AT-STO-26 (steps written from the text of issue #42). When Store
// or the peer implements only tenure.Store, the cases that need neither tenure.Watcher nor
// tenure.SlotStore pass, the Slots/* cases skip with "storetest: store does not implement
// tenure.SlotStore" and the Watch/* cases with "storetest: store does not implement
// tenure.Watcher".
func TestATSTO26HiddenInterfaces(t *testing.T) {
	r := runChild(t, "^TestChildHiddenInterfaces$")
	if r.failed {
		t.Fatalf("child TestChildHiddenInterfaces failed:\n%s", r.out)
	}
	skips := make(map[string]string)
	for _, name := range caseNames {
		switch {
		case strings.HasPrefix(name, "Slots/"):
			skips[name] = skipSlots
		case strings.HasPrefix(name, "Watch/"):
			skips[name] = skipWatch
		}
	}
	for _, hidden := range []string{"store", "peer"} {
		checkOutcomes(t, r, "TestChildHiddenInterfaces/"+hidden, skips)
	}
}

// TestATSTO33RealTimeSkips: AT-STO-33 (steps written from the text of issue #42). On the
// real-time memstore fixture, which has neither Advance nor SetWall, exactly four cases skip:
// Session/ExpiryBoundary with "storetest: fixture has no Advance", and Term/WallAhead,
// Term/WallBehind and Term/LostRecordWallBehind with "storetest: fixture has no SetWall". Every
// other case passes.
func TestATSTO33RealTimeSkips(t *testing.T) {
	r := runChild(t, "^TestConformance$/^realtime$")
	if r.failed {
		t.Fatalf("child TestConformance/realtime failed:\n%s", r.out)
	}
	checkOutcomes(t, r, "TestConformance/realtime", map[string]string{
		"Session/ExpiryBoundary":    skipAdvance,
		"Term/WallAhead":            skipSetWall,
		"Term/WallBehind":           skipSetWall,
		"Term/LostRecordWallBehind": skipSetWall,
	})
}

// TestChildMissingCapabilities runs the suite on the virtual fixture without SetWall,
// DeleteRecord and Peer. It is child-only; TestATSTO37MissingCapabilities checks its output.
func TestChildMissingCapabilities(t *testing.T) {
	skipUnlessChild(t)
	storetest.Run(t, func(t *testing.T) storetest.Fixture {
		f := newVirtual(t)
		f.SetWall, f.DeleteRecord, f.Peer = nil, nil, nil
		return f
	})
}

// TestATSTO37MissingCapabilities: AT-STO-37 (steps written from the text of issue #42). With
// SetWall, DeleteRecord and Peer nil, the cases that need SetWall skip with "storetest: fixture
// has no SetWall", those that need DeleteRecord and not SetWall with "storetest: fixture has no
// DeleteRecord", and every other case passes. No case needs Peer, so none skips for it.
func TestATSTO37MissingCapabilities(t *testing.T) {
	r := runChild(t, "^TestChildMissingCapabilities$")
	if r.failed {
		t.Fatalf("child TestChildMissingCapabilities failed:\n%s", r.out)
	}
	checkOutcomes(t, r, "TestChildMissingCapabilities", map[string]string{
		"Term/WallAhead":            skipSetWall,
		"Term/WallBehind":           skipSetWall,
		"Term/LostRecord":           skipDeleteRecord,
		"Term/LostRecordWallBehind": skipSetWall,
		"Watch/Delete":              skipDeleteRecord,
	})
}

// argKind is what an argument of a logged call names.
type argKind uint8

const (
	argHolder argKind = iota
	argSpace
	argShard
	argSlot
	argCount
	argOther // a term or a Timing, which names nothing
)

// callArg is one argument of a logged call: a name, in s or n by its kind, or another value.
type callArg struct {
	kind  argKind
	s     string
	n     int
	other any
}

func holderArg(h string) callArg { return callArg{kind: argHolder, s: h} }
func spaceArg(s string) callArg  { return callArg{kind: argSpace, s: s} }
func shardArg(n int) callArg     { return callArg{kind: argShard, n: n} }
func slotArg(n int) callArg      { return callArg{kind: argSlot, n: n} }
func countArg(n int) callArg     { return callArg{kind: argCount, n: n} }
func otherArg(v any) callArg     { return callArg{kind: argOther, other: v} }

func (a callArg) String() string {
	switch a.kind {
	case argHolder, argSpace:
		return strconv.Quote(a.s)
	case argShard, argSlot, argCount:
		return strconv.Itoa(a.n)
	default:
		return fmt.Sprint(a.other)
	}
}

// call is one call a fixture received.
type call struct {
	fix    int    // the fixture, numbered from 1 in newStore order
	method string // a store method, "Peer" or "DeleteRecord"
	args   []callArg
}

func (c call) String() string {
	args := make([]string, len(c.args))
	for i, a := range c.args {
		args[i] = a.String()
	}
	return c.method + "(" + strings.Join(args, ", ") + ")"
}

// callLog holds the calls the fixtures of one Run received, in the order they arrived.
type callLog struct {
	mu    sync.Mutex
	calls []call
	cases []string // the case name of fixture i+1
}

func (l *callLog) add(fix int, method string, args ...callArg) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call{fix, method, args})
}

// newStore returns a virtual-time fixture whose Store and Peer are callRecorders of one memstore,
// with the fixture's number. Peer and DeleteRecord log their calls too.
func (l *callLog) newStore(t *testing.T) (storetest.Fixture, int) {
	l.mu.Lock()
	l.cases = append(l.cases, caseName(t))
	fix := len(l.cases)
	l.mu.Unlock()

	clock := newTestClock()
	mem := memstore.New(clock)
	rec := &callRecorder{mem: mem, log: l, fix: fix}
	return storetest.Fixture{
		Store: rec,
		Peer: func() tenure.Store {
			l.add(fix, "Peer")
			return rec
		},
		Timing:  core.DefaultTiming,
		Advance: clock.advance,
		SetWall: clock.setWall,
		DeleteRecord: func(space string, shard int) {
			l.add(fix, "DeleteRecord", spaceArg(space), shardArg(shard))
			mem.DeleteRecord(space, shard)
		},
	}, fix
}

// callRecorder is a tenure.Store, tenure.SlotStore and tenure.Watcher that logs each call and
// forwards it to a memstore.
type callRecorder struct {
	mem *memstore.Store
	log *callLog
	fix int
}

func (r *callRecorder) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	r.log.add(r.fix, "KeepAlive", holderArg(holder), otherArg(timing))
	return r.mem.KeepAlive(ctx, holder, timing)
}

func (r *callRecorder) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	r.log.add(r.fix, "Acquire", holderArg(holder), spaceArg(space), shardArg(shard))
	return r.mem.Acquire(ctx, holder, space, shard)
}

func (r *callRecorder) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	r.log.add(r.fix, "Release", holderArg(holder), spaceArg(space), shardArg(shard), otherArg(term))
	return r.mem.Release(ctx, holder, space, shard, term)
}

func (r *callRecorder) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	r.log.add(r.fix, "Confirm", holderArg(holder), spaceArg(space), shardArg(shard), otherArg(term))
	return r.mem.Confirm(ctx, holder, space, shard, term)
}

func (r *callRecorder) CloseSession(ctx context.Context, holder string) error {
	r.log.add(r.fix, "CloseSession", holderArg(holder))
	return r.mem.CloseSession(ctx, holder)
}

func (r *callRecorder) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	r.log.add(r.fix, "Holdings", spaceArg(space))
	return r.mem.Holdings(ctx, space)
}

func (r *callRecorder) ClaimSlot(ctx context.Context, holder string) (int, error) {
	r.log.add(r.fix, "ClaimSlot", holderArg(holder))
	return r.mem.ClaimSlot(ctx, holder)
}

func (r *callRecorder) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	r.log.add(r.fix, "ReleaseSlot", holderArg(holder), slotArg(slot))
	return r.mem.ReleaseSlot(ctx, holder, slot)
}

func (r *callRecorder) Slots(ctx context.Context) (int, []string, error) {
	r.log.add(r.fix, "Slots")
	return r.mem.Slots(ctx)
}

func (r *callRecorder) SetSlots(ctx context.Context, count int) error {
	r.log.add(r.fix, "SetSlots", countArg(count))
	return r.mem.SetSlots(ctx, count)
}

func (r *callRecorder) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	r.log.add(r.fix, "WatchHolders")
	return r.mem.WatchHolders(ctx)
}

// TestChildParallel runs the suite on recorded virtual fixtures and checks that the cases ran in
// parallel, each with its own fixture, and called Peer first. It is child-only:
// TestATSTO34Parallel starts it under -test.parallel 4.
func TestChildParallel(t *testing.T) {
	skipUnlessChild(t)
	if p := flag.Lookup("test.parallel").Value.String(); p != "4" {
		t.Fatalf("-test.parallel is %s, want 4", p)
	}
	log := &callLog{}
	var alive, maxAlive atomic.Int64
	t.Run("suite", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			f, fix := log.newStore(t)
			n := alive.Add(1)
			t.Cleanup(func() { alive.Add(-1) })
			// The first fixture stays alive until a second one exists, which happens only when
			// another case runs alongside it.
			for deadline := time.Now().Add(10 * time.Second); fix == 1 && n < 2 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
				n = alive.Load()
			}
			for m := maxAlive.Load(); n > m && !maxAlive.CompareAndSwap(m, n); m = maxAlive.Load() {
			}
			return f
		})
	})

	if got := log.cases; len(got) != len(caseNames) {
		t.Errorf("newStore ran %d times, want %d", len(got), len(caseNames))
	}
	if got := slices.Sorted(slices.Values(log.cases)); !slices.Equal(got, slices.Sorted(slices.Values(caseNames))) {
		t.Errorf("newStore ran for cases %q, want each of %q once", got, caseNames)
	}
	if m := maxAlive.Load(); m < 2 {
		t.Errorf("at most %d fixture alive at once, want more than one", m)
	}
	first := make(map[int]call)
	peers := make(map[int]int)
	for _, c := range log.calls {
		if _, ok := first[c.fix]; !ok {
			first[c.fix] = c
		}
		if c.method == "Peer" {
			peers[c.fix]++
		}
	}
	for fix, name := range log.cases {
		fix++
		if c, ok := first[fix]; !ok || c.method != "Peer" {
			t.Errorf("%s: first call to its fixture is %v, want Peer()", name, c)
		}
		if peers[fix] != 1 {
			t.Errorf("%s: Peer called %d times, want once", name, peers[fix])
		}
	}
}

// TestATSTO34Parallel: AT-STO-34 (steps written from the text of issue #42). Under
// -test.parallel 4 the suite calls newStore once per case, 64 times, has more than one fixture
// alive at once, and calls Peer before each case's first call to its store.
func TestATSTO34Parallel(t *testing.T) {
	args := []string{"-test.run", "^TestChildParallel$", "-test.v", "-test.count=1", "-test.parallel", "4", "-child"}
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout", time.Until(deadline).String())
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], args...) //nolint:gosec // re-executes this test binary
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		t.Fatalf("child TestChildParallel failed:\n%s", out)
	case err != nil:
		t.Fatalf("running the child TestChildParallel: %v\n%s", err, out)
	}
	if !regexp.MustCompile(`(?m)^--- PASS: TestChildParallel \(`).Match(out) {
		t.Fatalf("child TestChildParallel did not pass:\n%s", out)
	}
}

// lagFor is how long the lagging peer stays stale after each write through Store.
const lagFor = 50 * time.Millisecond

// lagBackend is one memstore shared by a lagStore and a lagPeer. Each write through the lagStore,
// every call of a method that may write, leaves the peer stale for lagFor, showing the state from
// just before that call.
type lagBackend struct {
	mem *memstore.Store

	// staleReads counts the reads the peer answered from a snapshot, refusals the calls it
	// refused while stale.
	staleReads, refusals *atomic.Int64

	mu      sync.Mutex
	until   time.Time              // the peer is stale before until
	snap    lagSnapshot            // what the peer shows while stale
	timings map[string]core.Timing // each holder's Timing as of its last successful KeepAlive
}

// lagSnapshot is the backend's records and slots at one moment.
type lagSnapshot struct {
	holdings map[string][]tenure.Holding
	count    int
	slots    []string
}

func newLagBackend(staleReads, refusals *atomic.Int64) *lagBackend {
	return &lagBackend{
		mem:        memstore.New(nil),
		staleReads: staleReads,
		refusals:   refusals,
		timings:    make(map[string]core.Timing),
	}
}

// take returns the backend's state now. The suite uses only spaces a, b and c.
func (b *lagBackend) take() lagSnapshot {
	ctx := context.Background()
	s := lagSnapshot{holdings: make(map[string][]tenure.Holding)}
	for _, space := range []string{"a", "b", "c"} {
		s.holdings[space], _ = b.mem.Holdings(ctx, space)
	}
	s.count, s.slots, _ = b.mem.Slots(ctx)
	return s
}

// wrote follows a call through Store, before being the state from just before it: it makes the
// peer stale for lagFor, showing before.
func (b *lagBackend) wrote(before lagSnapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.snap = before
	b.until = time.Now().Add(lagFor)
}

// stale returns the snapshot the peer shows and true while it is stale.
func (b *lagBackend) stale() (lagSnapshot, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snap, time.Now().Before(b.until)
}

// keepAlive forwards a KeepAlive and remembers the holder's Timing when it succeeds.
func (b *lagBackend) keepAlive(ctx context.Context, holder string, timing core.Timing) error {
	err := b.mem.KeepAlive(ctx, holder, timing)
	if err == nil {
		b.mu.Lock()
		b.timings[holder] = timing
		b.mu.Unlock()
	}
	return err
}

// lagStore is Store of the lagging fixture: memstore, except that each write leaves the peer
// stale.
type lagStore struct {
	*memstore.Store
	b *lagBackend
}

func (s *lagStore) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	defer s.b.wrote(s.b.take())
	return s.b.keepAlive(ctx, holder, timing)
}

func (s *lagStore) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	defer s.b.wrote(s.b.take())
	return s.Store.Acquire(ctx, holder, space, shard)
}

func (s *lagStore) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	defer s.b.wrote(s.b.take())
	return s.Store.Release(ctx, holder, space, shard, term)
}

func (s *lagStore) CloseSession(ctx context.Context, holder string) error {
	defer s.b.wrote(s.b.take())
	return s.Store.CloseSession(ctx, holder)
}

func (s *lagStore) ClaimSlot(ctx context.Context, holder string) (int, error) {
	defer s.b.wrote(s.b.take())
	return s.Store.ClaimSlot(ctx, holder)
}

func (s *lagStore) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	defer s.b.wrote(s.b.take())
	return s.Store.ReleaseSlot(ctx, holder, slot)
}

func (s *lagStore) SetSlots(ctx context.Context, count int) error {
	defer s.b.wrote(s.b.take())
	return s.Store.SetSlots(ctx, count)
}

// lagPeer is the lagging fixture's peer. While stale it answers reads from the snapshot, refuses
// Acquire with a *tenure.HeldError naming the snapshot's holder when that is another holder, and
// refuses ClaimSlot with tenure.ErrNoSlot. Otherwise, and for calls with a done context or an
// invalid holder, it calls memstore.
type lagPeer struct {
	*memstore.Store
	b *lagBackend
}

// validHolder reports whether memstore accepts holder: not empty, no zero byte, and a member ID
// before any '/'.
func validHolder(holder string) bool {
	id, _, _ := strings.Cut(holder, "/")
	return id != "" && !strings.ContainsRune(holder, 0)
}

func (p *lagPeer) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	return p.b.keepAlive(ctx, holder, timing)
}

func (p *lagPeer) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	if snap, stale := p.b.stale(); stale && ctx.Err() == nil && validHolder(holder) {
		p.b.mu.Lock()
		timing := p.b.timings[holder] // zero for a holder never seen open
		p.b.mu.Unlock()
		for _, h := range snap.holdings[space] {
			if h.Shard == shard && h.Holder != "" && h.Holder != holder {
				p.b.refusals.Add(1)
				return 0, &tenure.HeldError{Holder: h.Holder, RetryAfter: timing.RetryPeriod}
			}
		}
	}
	return p.Store.Acquire(ctx, holder, space, shard)
}

func (p *lagPeer) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	got, err := p.Store.Confirm(ctx, holder, space, shard, term)
	if snap, stale := p.b.stale(); stale && err == nil {
		p.b.staleReads.Add(1)
		got = tenure.Holding{Space: space, Shard: shard}
		for _, h := range snap.holdings[space] {
			if h.Shard == shard {
				got = h
			}
		}
	}
	return got, err
}

func (p *lagPeer) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	got, err := p.Store.Holdings(ctx, space)
	if snap, stale := p.b.stale(); stale && err == nil {
		p.b.staleReads.Add(1)
		got = slices.Clone(snap.holdings[space])
	}
	return got, err
}

func (p *lagPeer) ClaimSlot(ctx context.Context, holder string) (int, error) {
	if _, stale := p.b.stale(); stale && ctx.Err() == nil && validHolder(holder) {
		p.b.refusals.Add(1)
		return -1, tenure.ErrNoSlot
	}
	return p.Store.ClaimSlot(ctx, holder)
}

func (p *lagPeer) Slots(ctx context.Context) (int, []string, error) {
	count, holders, err := p.Store.Slots(ctx)
	if snap, stale := p.b.stale(); stale && err == nil {
		p.b.staleReads.Add(1)
		count, holders = snap.count, slices.Clone(snap.slots)
	}
	return count, holders, err
}

// TestATSTO34LaggingPeer: AT-STO-34 (steps written from the text of issue #42). In real time,
// with a peer that is stale for 50ms after each write through Store and Slack 100ms, every case
// that runs passes. Only the four cases a real-time fixture without SetWall lacks skip, and the
// peer did answer reads from a snapshot and refuse calls while stale.
func TestATSTO34LaggingPeer(t *testing.T) {
	var staleReads, refusals atomic.Int64
	var mu sync.Mutex
	var skipped []string
	t.Run("suite", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			t.Cleanup(func() {
				if t.Skipped() {
					mu.Lock()
					skipped = append(skipped, caseName(t))
					mu.Unlock()
				}
			})
			b := newLagBackend(&staleReads, &refusals)
			return storetest.Fixture{
				Store:        &lagStore{Store: b.mem, b: b},
				Peer:         func() tenure.Store { return &lagPeer{Store: b.mem, b: b} },
				Timing:       realTiming,
				Slack:        100 * time.Millisecond,
				DeleteRecord: b.mem.DeleteRecord,
			}
		})
	})
	want := []string{"Session/ExpiryBoundary", "Term/LostRecordWallBehind", "Term/WallAhead", "Term/WallBehind"}
	if got := slices.Sorted(slices.Values(skipped)); !slices.Equal(got, want) {
		t.Errorf("cases skipped: %q, want %q", got, want)
	}
	if staleReads.Load() == 0 {
		t.Error("the peer answered no read from a snapshot, want it to lag")
	}
	if refusals.Load() == 0 {
		t.Error("the peer refused no call while stale, want it to lag")
	}
	t.Logf("the peer answered %d reads from a snapshot and refused %d calls while stale",
		staleReads.Load(), refusals.Load())
}

// namesHolder matches the holders of STO-840: members m1 to m20, incarnations of 32 hex digits.
var namesHolder = regexp.MustCompile(`^m([1-9]|1[0-9]|20)/[0-9a-f]{32}$`)

// badName returns why a, an argument that reached the store, is not one of STO-840's names, or "".
func badName(a callArg) string {
	switch a.kind {
	case argHolder:
		if !namesHolder.MatchString(a.s) {
			return "a holder other than m1 to m20 with 32 hex digits"
		}
	case argSpace:
		if a.s != "a" && a.s != "b" && a.s != "c" {
			return "a space other than a, b and c"
		}
	case argShard:
		if a.n < 0 || a.n > 19 {
			return "a shard outside 0 to 19"
		}
	case argSlot:
		if a.n < 0 || a.n > 7 {
			return "a slot outside 0 to 7"
		}
	case argCount:
		if a.n < 0 || a.n > 8 {
			return "a slot count above 8, naming slots outside 0 to 7"
		}
	case argOther:
	}
	return ""
}

// TestATSTO38Names: AT-STO-38 (steps written from the text of issue #42). On a recorded virtual
// fixture, every case but Args/Invalid, whose job is to send invalid names, sends the store only
// spaces a, b and c, holders m1 to m20 with 32 hex digits, shards 0 to 19 and slots 0 to 7.
func TestATSTO38Names(t *testing.T) {
	log := &callLog{}
	t.Run("suite", func(t *testing.T) {
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			f, _ := log.newStore(t)
			return f
		})
	})

	reported := make(map[string]bool)
	for _, c := range log.calls {
		name := log.cases[c.fix-1]
		if name == "Args/Invalid" {
			continue
		}
		for _, a := range c.args {
			if why := badName(a); why != "" && !reported[name+why] {
				reported[name+why] = true
				t.Errorf("%s sends %v: %s", name, c, why)
			}
		}
	}
	if len(log.cases) != len(caseNames) {
		t.Errorf("newStore ran %d times, want %d", len(log.cases), len(caseNames))
	}
}
