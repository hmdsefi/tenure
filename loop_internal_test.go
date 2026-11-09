// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// newLoopHarness returns a loop over a new machine of member "a" with holder "a/1", whose node
// has an executor over a scripted store that answers every call with nil. Each step's record is
// appended to recs. The executor is shut down at cleanup.
func newLoopHarness(t *testing.T, recs *[]stepRecord) *loop {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		holder:      "a/1",
		epoch:       time.Now(),
		obs:         nopObserver{},
		ctx:         ctx,
		cancel:      cancel,
		inbox:       newInbox(),
		machineDone: make(chan struct{}),
	}
	st := &scriptStore{auto: func(*scriptCall) (scriptReply, bool) { return scriptReply{}, true }}
	n.exec = newExecutor(ctx, st, core.DefaultTiming, n.epoch, nil)
	t.Cleanup(func() {
		cancel()
		n.exec.wait()
	})
	m := core.NewMachine(core.MachineConfig{Holder: n.holder, Member: "a"})
	return newLoop(n, m, 1, func(r stepRecord) { *recs = append(*recs, r) })
}

// TestLoopTickFirst checks that an input read after wakes have passed is stepped after a Tick
// for each of them, at the wake's reading.
func TestLoopTickFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recs []stepRecord
		l := newLoopHarness(t, &recs)
		l.step(loopInput{in: core.SessionRenewed{}})
		time.Sleep(5 * time.Second) // no goroutine runs the loop: the wakes at 2 s and 4 s pass
		v := core.View{Version: 1, Members: []core.Member{{ID: "a", Slot: -1}}}
		l.step(loopInput{in: core.ViewChanged{View: v}})

		want := []core.Input{
			core.SessionRenewed{},
			core.Tick{Now: 2 * time.Second},
			core.Tick{Now: 4 * time.Second},
			core.ViewChanged{Now: 5 * time.Second, View: v, Wall: l.n.epoch.Add(5 * time.Second)},
		}
		got := make([]core.Input, len(recs))
		for i, r := range recs {
			got[i] = r.Input
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("stepped %#v, want %#v", got, want)
		}
		if pub := l.n.view.Load(); pub == nil || !reflect.DeepEqual(*pub, v) {
			t.Errorf("published view = %v, want %+v", pub, v)
		}
	})
}

// TestLoopExecute checks the actions the loop carries out itself: ExtendDeadline only raises the
// deadline, Wake, Degrade and Restore do nothing, and an action the loop does not know panics.
func TestLoopExecute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recs []stepRecord
		l := newLoopHarness(t, &recs)
		l.execute(core.ExtendDeadline{Deadline: 10 * time.Second})
		l.execute(core.ExtendDeadline{Deadline: 5 * time.Second})
		l.execute(core.Wake{At: time.Second})
		l.execute(core.Degrade{Space: "orders", Shard: 1, Term: 5})
		l.execute(core.Restore{Space: "orders", Shard: 1, Term: 5})
		if got := time.Duration(l.n.deadline.Load()); got != 10*time.Second {
			t.Errorf("deadline = %v, want 10s", got)
		}
		// The loop carries out every action of core, so only a nil action is unknown to it.
		defer func() {
			if r := recover(); r != "tenure: unknown action <nil>" {
				t.Errorf("execute(nil) panicked with %v, want tenure: unknown action <nil>", r)
			}
		}()
		l.execute(nil)
	})
}

// runObserver records the observer's runReturned calls.
type runObserver struct {
	nopObserver
	mu      sync.Mutex
	returns []runReturn
}

// runReturn is one runReturned call.
type runReturn struct {
	sh      *Shard
	err     error
	count   int
	backoff time.Duration
}

func (o *runObserver) runReturned(sh *Shard, err error, count int, backoff time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.returns = append(o.returns, runReturn{sh, err, count, backoff})
}

// TestLoopRunsShards checks StartRun and StopRun as the loop carries them out: StartRun raises
// the deadline, publishes the Shard and starts a supervisor that reports Run's returns to the
// observer; a StopRun of another term changes nothing; StopRun stops the Shard, the supervisor
// then posts RunStopped with it, and stepping that unpublishes the Shard.
func TestLoopRunsShards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recs []stepRecord
		l := newLoopHarness(t, &recs)
		n := l.n
		obs := &runObserver{}
		n.obs = obs
		n.cfg.Timing = core.DefaultTiming
		boom := errors.New("boom")
		n.shards = newShardTable([]Space{{Name: "orders", Shards: 2, Run: func(context.Context, *Shard) error { return boom }}})

		n.stepMu.Lock()
		l.execute(core.StartRun{Space: "orders", Shard: 1, Term: 7, Deadline: 10 * time.Second})
		n.stepMu.Unlock()
		sh := n.shards["orders"].local[1].Load()
		if sh == nil || sh.Space != "orders" || sh.ID != 1 || sh.Term != 7 || !sh.Valid() {
			t.Fatalf("published %+v, want a valid orders/1 at term 7", sh)
		}
		if p := n.shards["orders"].local[0].Load(); p != nil {
			t.Errorf("orders/0 published %+v, want none", p)
		}
		if got := time.Duration(n.deadline.Load()); got != 10*time.Second {
			t.Errorf("deadline = %v, want StartRun's 10s", got)
		}

		time.Sleep(3 * time.Second) // returns at 0 s and 2 s
		obs.mu.Lock()
		got := obs.returns
		obs.mu.Unlock()
		want := []runReturn{{sh, boom, 1, 2 * time.Second}, {sh, boom, 2, 4 * time.Second}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("runReturned calls = %+v, want %+v", got, want)
		}

		n.stepMu.Lock()
		l.execute(core.StopRun{Space: "orders", Shard: 1, Term: 6, Reason: core.StopMoved})
		n.stepMu.Unlock()
		if err := sh.Check(); err != nil {
			t.Errorf("after a StopRun of term 6, Check() = %v, want nil", err)
		}
		n.stepMu.Lock()
		l.execute(core.StopRun{Space: "orders", Shard: 1, Term: 7, Reason: core.StopMoved})
		n.stepMu.Unlock()
		n.sups.Wait()
		if err := sh.Check(); err == nil || err.Error() != "tenure: not owner of orders/1 (term 7): moved" {
			t.Errorf("after StopRun, Check() = %v, want the moved error", err)
		}
		ins, _ := n.inbox.take()
		wantIn := []loopInput{{in: core.RunStopped{Space: "orders", Shard: 1, Term: 7}, sh: sh}}
		if !reflect.DeepEqual(ins, wantIn) {
			t.Fatalf("posted %+v, want %+v", ins, wantIn)
		}
		if p := n.shards["orders"].local[1].Load(); p != sh {
			t.Errorf("published %+v before RunStopped is stepped, want %+v", p, sh)
		}
		l.step(ins[0])
		if p := n.shards["orders"].local[1].Load(); p != nil {
			t.Errorf("published %+v after RunStopped was stepped, want none", p)
		}
	})
}

// TestLoopChainsRuns checks that a StartRun for a new term while the shard's previous Run has
// not returned publishes the new Shard at once but starts its Run only once the previous Run
// has returned, and that the previous run's RunStopped leaves the new Shard published.
func TestLoopChainsRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recs []stepRecord
		l := newLoopHarness(t, &recs)
		n := l.n
		n.cfg.Timing = core.DefaultTiming
		t0 := time.Now()
		var mu sync.Mutex
		starts := map[core.Term]time.Duration{}
		run := func(ctx context.Context, sh *Shard) error {
			mu.Lock()
			starts[sh.Term] = time.Since(t0)
			mu.Unlock()
			<-ctx.Done()
			if sh.Term == 7 {
				time.Sleep(30 * time.Second) // ignores ctx for 30 s
			}
			return nil
		}
		n.shards = newShardTable([]Space{{Name: "orders", Shards: 2, Fenced: true, Run: run}})
		exec := func(a core.Action) {
			n.stepMu.Lock()
			defer n.stepMu.Unlock()
			l.execute(a)
		}
		started := func(term core.Term) (time.Duration, bool) {
			mu.Lock()
			defer mu.Unlock()
			d, ok := starts[term]
			return d, ok
		}

		exec(core.StartRun{Space: "orders", Shard: 1, Term: 7})
		sh7 := n.shards["orders"].local[1].Load()
		time.Sleep(time.Second)
		exec(core.StopRun{Space: "orders", Shard: 1, Term: 7, Reason: core.StopMoved})
		exec(core.StartRun{Space: "orders", Shard: 1, Term: 8})
		sh8 := n.shards["orders"].local[1].Load()
		if sh8 == nil || sh8 == sh7 || sh8.Term != 8 {
			t.Fatalf("published %+v after StartRun of term 8, want its Shard", sh8)
		}

		time.Sleep(30*time.Second - time.Nanosecond)
		synctest.Wait()
		if d, ok := started(8); ok {
			t.Errorf("term 8's Run started at %v, before term 7's Run returned", d)
		}
		if ins, _ := n.inbox.take(); len(ins) != 0 {
			t.Errorf("posted %+v before term 7's Run returned, want nothing", ins)
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if d, ok := started(8); !ok || d != 31*time.Second {
			t.Errorf("term 8's Run started at %v (%t), want 31s, when term 7's returned", d, ok)
		}
		ins, _ := n.inbox.take()
		wantIn := []loopInput{{in: core.RunStopped{Space: "orders", Shard: 1, Term: 7}, sh: sh7}}
		if !reflect.DeepEqual(ins, wantIn) {
			t.Fatalf("posted %+v, want %+v", ins, wantIn)
		}
		l.step(ins[0])
		if p := n.shards["orders"].local[1].Load(); p != sh8 {
			t.Errorf("published %+v after term 7's RunStopped, want term 8's Shard", p)
		}

		exec(core.StopRun{Space: "orders", Shard: 1, Term: 8, Reason: core.StopMoved})
		n.sups.Wait()
	})
}

// TestAssignmentsMatchMachine checks that assignments, which routing takes for a view the machine
// did not apply, gives every space the assignment the machine computes when it applies the same
// view: with a draining member and members outside their JoinAt/LeaveAt window, and with slots.
func TestAssignmentsMatchMachine(t *testing.T) {
	wall := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	spaces := []Space{{Name: "orders", Shards: 8}, {Name: "carts", Shards: 3}}
	views := []struct {
		name string
		v    core.View
	}{
		{"members", core.View{Version: 4, Members: []core.Member{
			{ID: "a", Slot: -1},
			{ID: "b", Slot: -1, Draining: true},
			{ID: "c", Slot: -1, JoinAt: wall.Add(time.Second)},
			{ID: "d", Slot: -1, LeaveAt: wall},
			{ID: "e", Slot: -1, JoinAt: wall.Add(-time.Second), LeaveAt: wall.Add(time.Second)},
		}}},
		{"slots", core.View{Version: 6, Slots: 3, SlotHolders: []string{"a/1", "", "b/2"}, Members: []core.Member{
			{ID: "a", Slot: 0},
			{ID: "b", Slot: 2, Draining: true},
		}}},
	}
	for _, c := range views {
		m := core.NewMachine(core.MachineConfig{Holder: "a/1", Member: "a", Spaces: spaceSpecs(spaces)})
		m.Step(core.ViewChanged{View: c.v, Now: time.Second, Wall: wall})
		if got := m.Status().ViewVersion; got != c.v.Version {
			t.Fatalf("%s: the machine applied version %d, want %d", c.name, got, c.v.Version)
		}
		got := assignments(c.v, wall, spaces)
		for _, sp := range spaces {
			if want := m.Assignment(sp.Name); !reflect.DeepEqual(got[sp.Name], want) {
				t.Errorf("%s: assignments(%s) = %q, want the machine's %q", c.name, sp.Name, got[sp.Name], want)
			}
		}
	}
}

// TestLoopSlotResult checks O8's floor (RUN-210): a stepped SlotClaimed and a stepped
// SlotReleased each raise the merger's floor to the end stamp of their call, so that slot data
// read before either is dropped; a failed claim and the results of other calls leave it.
func TestLoopSlotResult(t *testing.T) {
	var recs []stepRecord
	l := newLoopHarness(t, &recs)
	m := &l.n.merge
	m.post = func(core.View) {}
	m.membership(core.View{Version: 1})

	results := []struct {
		name  string
		r     callResult
		floor uint64
	}{
		{"SlotClaimed", callResult{act: core.ClaimSlot{}, inputs: []core.Input{core.SlotClaimed{Slot: 0}}, start: 3, end: 10}, 10},
		{"failed claim", callResult{act: core.ClaimSlot{}, start: 11, end: 12}, 10},
		{"Released", callResult{act: core.Release{Space: "orders", Shard: 1, Term: 2},
			inputs: []core.Input{core.Released{Space: "orders", Shard: 1, Term: 2}}, start: 13, end: 14}, 10},
		{"SlotReleased", callResult{act: core.ReleaseSlot{Slot: 0},
			inputs: []core.Input{core.SlotReleased{Slot: 0}}, start: 15, end: 20}, 20},
	}
	for _, tt := range results {
		l.slotResult(tt.r)
		m.mu.Lock()
		floor := m.floor
		m.mu.Unlock()
		if floor != tt.floor {
			t.Errorf("after %s: floor %d, want %d", tt.name, floor, tt.floor)
		}
		if _, stale := m.slots(tt.floor-1, 1, nil); !stale {
			t.Errorf("after %s: slot data read at %d not dropped", tt.name, tt.floor-1)
		}
	}
	if _, stale := m.slots(20, 1, nil); stale {
		t.Error("slot data read at the floor dropped")
	}

	// deliver steps no result of an earlier generation and none that reaches a lost machine, so
	// their slot results raise no floor that would hold back the next incarnation's views.
	claimed := func(gen, end uint64) callResult {
		return callResult{gen: gen, act: core.ClaimSlot{}, inputs: []core.Input{core.SlotClaimed{Slot: 0}}, start: end - 1, end: end}
	}
	l.gen = 2
	l.deliver(claimed(1, 30))
	l.status.Session = core.SessionStateLost
	l.deliver(claimed(2, 40))
	m.mu.Lock()
	floor := m.floor
	m.mu.Unlock()
	if floor != 20 {
		t.Errorf("floor %d after slot results that were not stepped, want 20", floor)
	}
	if len(recs) != 0 {
		t.Errorf("stepped %d inputs, want none", len(recs))
	}
}
