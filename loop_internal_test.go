// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"reflect"
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
// deadline, Wake does nothing, and an action the loop does not know panics.
func TestLoopExecute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recs []stepRecord
		l := newLoopHarness(t, &recs)
		l.execute(core.ExtendDeadline{Deadline: 10 * time.Second})
		l.execute(core.ExtendDeadline{Deadline: 5 * time.Second})
		l.execute(core.Wake{At: time.Second})
		if got := time.Duration(l.n.deadline.Load()); got != 10*time.Second {
			t.Errorf("deadline = %v, want 10s", got)
		}
		defer func() {
			if r := recover(); r != "tenure: unknown action core.Acquire" {
				t.Errorf("execute(Acquire) panicked with %v, want tenure: unknown action core.Acquire", r)
			}
		}()
		l.execute(core.Acquire{Space: "orders", Shard: 1})
	})
}
