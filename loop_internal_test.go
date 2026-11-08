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
		defer func() {
			if r := recover(); r != "tenure: unknown action core.ClaimSlot" {
				t.Errorf("execute(ClaimSlot) panicked with %v, want tenure: unknown action core.ClaimSlot", r)
			}
		}()
		l.execute(core.ClaimSlot{})
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
