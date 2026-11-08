// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// newRecoveryHarness returns a loop over a new machine of member "a" with holder "a/1" and the
// space orders of 2 unfenced shards, whose node has an executor over st, a freshness state, a
// readings queue and a resync channel, and draws its next incarnation from rnd. Each step's
// record is appended to recs. Everything is shut down at cleanup.
func newRecoveryHarness(t *testing.T, st Store, rnd []byte, recs *[]stepRecord) *loop {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	spaces := []Space{{Name: "orders", Shards: 2, Run: func(ctx context.Context, _ *Shard) error {
		<-ctx.Done()
		return nil
	}}}
	n := &Node{
		cfg: Config{
			Self:   Member{ID: "a"},
			Spaces: spaces,
			Timing: core.DefaultTiming,
			Rand:   bytes.NewReader(rnd),
		},
		holder:      "a/1",
		epoch:       time.Now(),
		obs:         nopObserver{},
		ctx:         ctx,
		cancel:      cancel,
		inbox:       newInbox(),
		machineDone: make(chan struct{}),
		shards:      newShardTable(spaces),
		readings:    newReadings(),
		resync:      make(chan struct{}, 1),
	}
	n.exec = newExecutor(ctx, st, core.DefaultTiming, n.epoch, nil)
	n.fresh = newFreshness(n.holder, spaces, n.exec.confirmStarted)
	t.Cleanup(func() {
		cancel()
		n.exec.wait()
		n.sups.Wait()
	})
	return newLoop(n, n.newMachine(n.holder), 1, func(r stepRecord) { *recs = append(*recs, r) })
}

// TestRecoveryLossStep checks the step that loses the session and the new incarnation after it.
// At the loss, a Release of the lost holder queued behind an Acquire in flight never goes out, the
// answers of the Acquire calls in flight are discarded and replaced by Acquired with no grant at
// the loss's reading, so the machine is done, and a new incarnation is due rather than the end of
// a drain. recover then draws holder a/abab…, moves to generation 2, clears the session deadline
// and sends KeepAlive for the new holder only; the answer is stepped first, then the latest view
// with the next Version, and a resync of every space is due.
func TestRecoveryLossStep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := &scriptStore{auto: func(c *scriptCall) (scriptReply, bool) {
			return scriptReply{}, c.method == "KeepAlive"
		}}
		var recs []stepRecord
		l := newRecoveryHarness(t, st, bytes.Repeat([]byte{0xab}, incarnationBytes), &recs)
		n := l.n
		v := core.View{Version: 7, Members: []core.Member{{ID: "a", Slot: -1}}}
		l.step(loopInput{in: core.SessionRenewed{}})
		l.step(loopInput{in: core.ViewChanged{View: v}})
		synctest.Wait()
		if got := len(st.open()); got != 2 {
			t.Fatalf("%d calls in flight after the view, want the 2 Acquire calls", got)
		}
		if !n.exec.do(1, "a/1", core.Release{Space: "orders", Shard: 0, Term: 3}) {
			t.Fatal("the Release of generation 1 was not queued")
		}

		time.Sleep(time.Second)
		lossAt := len(recs)
		l.step(loopInput{in: core.SessionFailed{Cause: core.CauseLost}})
		var after []core.Input
		for _, r := range recs[lossAt+1:] {
			after = append(after, r.Input)
		}
		want := []core.Input{
			core.Acquired{Space: "orders", Shard: 0, Now: time.Second},
			core.Acquired{Space: "orders", Shard: 1, Now: time.Second},
		}
		if !reflect.DeepEqual(after, want) {
			t.Errorf("stepped %+v after the loss, want %+v", after, want)
		}
		if !l.status.Done || l.status.Session != core.SessionStateLost {
			t.Errorf("Status after the loss: Done %t, Session %v; want done and lost", l.status.Done, l.status.Session)
		}
		if !l.recoverDue || l.done {
			t.Errorf("recoverDue %t, done %t; want a new incarnation due", l.recoverDue, l.done)
		}

		for _, c := range st.open() {
			c.reply <- scriptReply{term: 5}
		}
		synctest.Wait()
		if rs := n.exec.take(); len(rs) != 0 {
			t.Errorf("results after the loss: %+v, want none", rs)
		}
		for _, c := range st.all() {
			if c.method == "Release" {
				t.Errorf("Release(%s, %s/%d) went out after the loss", c.holder, c.space, c.shard)
			}
		}

		calls := len(st.all())
		n.deadline.Store(int64(time.Hour))
		l.recover()
		holder := "a/" + strings.Repeat("ab", incarnationBytes)
		if n.holder != holder || n.fresh.holder != holder || l.gen != 2 {
			t.Errorf("holder %q, freshness holder %q, generation %d; want %q twice and 2", n.holder, n.fresh.holder,
				l.gen, holder)
		}
		if d := n.deadline.Load(); d != 0 {
			t.Errorf("session deadline %v after recover, want 0", time.Duration(d))
		}
		if !l.opening || len(recs) != lossAt+3 {
			t.Errorf("opening %t, %d steps; want the new machine to wait for its KeepAlive with no step",
				l.opening, len(recs)-lossAt-3)
		}
		synctest.Wait()
		sent := st.all()[calls:]
		if len(sent) != 1 || sent[0].method != "KeepAlive" || sent[0].holder != holder {
			t.Fatalf("calls of recover: %+v, want one KeepAlive(%s)", sent, holder)
		}

		for _, r := range n.exec.take() {
			l.deliver(r)
		}
		var gen2 []core.Input
		for _, r := range recs[lossAt+3:] {
			if r.Gen != 2 {
				t.Errorf("step %+v of generation %d after recover, want 2", r.Input, r.Gen)
			}
			gen2 = append(gen2, r.Input)
		}
		next := v
		next.Version = 8
		want = []core.Input{
			core.SessionRenewed{Now: time.Second, SentAt: time.Second},
			core.ViewChanged{Now: time.Second, View: next, Wall: n.epoch.Add(time.Second)},
		}
		if !reflect.DeepEqual(gen2, want) {
			t.Errorf("generation 2 stepped %+v, want %+v", gen2, want)
		}
		if l.opening || len(n.resync) != 1 {
			t.Errorf("opening %t, %d resync requests; want the machine open and one resync due", l.opening,
				len(n.resync))
		}
	})
}

// TestRecoveryDrain checks RUN-190 at the loop: a machine done after a lost session closes
// machineDone, with no new incarnation, once a drain has been requested, also when the request
// comes while recover waits to draw again after a failed read.
func TestRecoveryDrain(t *testing.T) {
	t.Run("requested before the machine is done", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var recs []stepRecord
			l := newRecoveryHarness(t, &scriptStore{}, nil, &recs)
			l.n.drainDone = make(chan struct{})
			l.step(loopInput{in: core.SessionFailed{Cause: core.CauseLost}})
			select {
			case <-l.n.machineDone:
			default:
				t.Fatal("machineDone open after the loss with a drain requested")
			}
			if l.recoverDue {
				t.Error("a new incarnation is due with a drain requested")
			}
		})
	})
	t.Run("requested while the draw is retried", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var recs []stepRecord
			l := newRecoveryHarness(t, &scriptStore{}, nil, &recs) // every read fails
			l.step(loopInput{in: core.SessionFailed{Cause: core.CauseLost}})
			if !l.recoverDue {
				t.Fatal("no new incarnation due after the loss")
			}
			t0 := time.Now()
			go func() {
				time.Sleep(time.Second)
				l.n.mu.Lock()
				l.n.drainDone = make(chan struct{})
				l.n.mu.Unlock()
			}()
			l.recover()
			if d := time.Since(t0); d != 2*time.Second {
				t.Errorf("recover returned after %v, want 2s, at the second read", d)
			}
			select {
			case <-l.n.machineDone:
			default:
				t.Error("machineDone open after a drain was requested during the draw")
			}
			if l.gen != 1 || l.n.holder != "a/1" {
				t.Errorf("generation %d, holder %q; want no new incarnation", l.gen, l.n.holder)
			}
		})
	})
}

// TestRecoveryFreshness checks the freshness state of a new machine: the holder changes, grants,
// Confirm tags, Confirm counts and a pending resync are cleared, and every floor and last stamp
// is kept.
func TestRecoveryFreshness(t *testing.T) {
	f := newFreshness("a/1", []Space{{Name: "orders", Shards: 2}}, nil)
	f.shards["orders"][0] = shardFresh{floor: 9, last: 12, grant: 4, confirm: 13}
	f.shards["orders"][1] = shardFresh{floor: 3, last: 5, grant: 2}
	f.issued, f.answered, f.wait, f.resyncAt = 4, 3, true, 4

	f.newMachine("a/2")
	want := []shardFresh{{floor: 9, last: 12}, {floor: 3, last: 5}}
	if !reflect.DeepEqual(f.shards["orders"], want) || f.holder != "a/2" {
		t.Errorf("after newMachine: holder %q, shards %+v; want a/2 and %+v", f.holder, f.shards["orders"], want)
	}
	if f.issued != 0 || f.answered != 0 || f.wait || f.resyncAt != 0 {
		t.Errorf("after newMachine: issued %d, answered %d, wait %t, resyncAt %d; want all cleared", f.issued,
			f.answered, f.wait, f.resyncAt)
	}
}

// TestRecoveryLossBatch checks that the loss ends the lost machine's results also within the
// batch the loss came in. The KeepAlive that returns ErrSessionLost and the Acquire of orders/0,
// which returns a grant right after it, are taken together. The loss answers both acquiring
// shards with Acquired with no grant, and the Acquire's own result is not stepped, so the machine
// gets one answer for each Acquire (MCH O3).
func TestRecoveryLossBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := &scriptStore{}
		var recs []stepRecord
		l := newRecoveryHarness(t, st, bytes.Repeat([]byte{0xab}, incarnationBytes), &recs)
		n := l.n
		v := core.View{Version: 7, Members: []core.Member{{ID: "a", Slot: -1}}}
		l.step(loopInput{in: core.SessionRenewed{}})
		l.step(loopInput{in: core.ViewChanged{View: v}})
		n.exec.do(1, "a/1", core.KeepAlive{})
		synctest.Wait()

		calls := st.open()
		if len(calls) != 3 {
			t.Fatalf("%d calls in flight, want 2 Acquire calls and a KeepAlive", len(calls))
		}
		for _, c := range calls {
			if c.method == "KeepAlive" {
				c.reply <- scriptReply{err: ErrSessionLost}
			}
		}
		synctest.Wait()
		for _, c := range calls {
			if c.method == "Acquire" && c.shard == 0 {
				c.reply <- scriptReply{term: 5}
			}
		}
		synctest.Wait()
		rs := n.exec.take()
		if len(rs) != 2 {
			t.Fatalf("%d results, want the KeepAlive's and orders/0's Acquire's", len(rs))
		}
		for _, r := range rs {
			l.deliver(r)
		}

		answers := map[int]int{}
		for _, r := range recs {
			if a, ok := r.Input.(core.Acquired); ok {
				answers[a.Shard]++
				if a.Term != 0 {
					t.Errorf("stepped %+v, want no grant after the loss", a)
				}
			}
		}
		if answers[0] != 1 || answers[1] != 1 {
			t.Errorf("Acquired answers by shard %v, want one for each", answers)
		}
		if !l.status.Done || !l.recoverDue {
			t.Errorf("Done %t, recoverDue %t; want a new incarnation due", l.status.Done, l.recoverDue)
		}
	})
}
