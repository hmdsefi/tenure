// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package membershiptest

import (
	"context"
	"errors"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The cases below are MEM-530's 19 Phase 1 cases. A case that is not about observers watches an
// instance only once it has joined, and before a change undoes an earlier one, it waits until
// every watcher has seen the earlier change, so that no watcher can miss both.

// watchInitial: Watch returns with the current view buffered, and the view comes to show the
// member that joined.
func watchInitial(e *env) {
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
	if e.f.Observer {
		e.watch("x")
	}
}

// watchSorted: members that join out of order are listed sorted by ID.
func watchSorted(e *env) {
	e.join("d")
	w := e.watch("d")
	e.join("b")
	e.join("c")
	e.join("a")
	w.eventually(lists(self(0), self(1), self(2), self(3)))
}

// watchVersions: each change makes a view with a larger Version, and no view comes without a
// change.
func watchVersions(e *env) {
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
	e.join("b")
	w.eventually(lists(self(0), self(1)))
	e.setDraining("b", true)
	w.eventually(lists(self(0), draining(self(1))))
	w.quiet()
}

// watchCoalesce: a view not yet received is replaced by the newest one, so once the changes have
// settled, the next view received is the newest.
func watchCoalesce(e *env) {
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
	e.join("b")
	e.join("c")
	time.Sleep(e.f.Settle)
	want := lists(self(0), self(1), self(2))
	select {
	case v, ok := <-w.ch:
		if !ok {
			e.t.Fatalf("view %d on a: a closed channel, want %s", w.last.Version, want.want)
		}
		w.take(v)
		if !want.ok(v) {
			e.t.Fatalf("view %d on a: %s, want the newest view, with %s", v.Version, fmtMembers(v), want.want)
		}
	default:
		e.t.Fatalf("view %d on a: no new view %v after the changes, want the newest view, with %s",
			w.last.Version, e.f.Settle, want.want)
	}
}

// watchCancel: ending the context of a Watch closes its channel.
func watchCancel(e *env) {
	e.join("a")
	ctx, cancel := context.WithCancel(e.ctx)
	w := e.watchCtx(ctx, "a")
	cancel()
	w.closes("its context ended")
}

// watchMultiple: two watches on one instance both see each change.
func watchMultiple(e *env) {
	e.join("a")
	w1 := e.watch("a")
	w2 := e.watch("a")
	e.join("b")
	w1.eventually(lists(self(0), self(1)))
	w2.eventually(lists(self(0), self(1)))
	e.setDraining("b", true)
	w1.eventually(lists(self(0), draining(self(1))))
	w2.eventually(lists(self(0), draining(self(1))))
}

// watchWithoutJoin: an instance that never joins sees a member join and start draining.
func watchWithoutJoin(e *env) {
	w := e.watch("x")
	e.join("a")
	w.eventually(lists(self(0)))
	e.setDraining("a", true)
	w.eventually(lists(draining(self(0))))
}

// joinVisible: a join reaches every instance.
func joinVisible(e *env) {
	var ws []*watcher
	for _, name := range []string{"a", "b", "c"} {
		e.join(name)
		ws = append(ws, e.watch(name))
	}
	for _, w := range ws {
		w.eventually(lists(self(0), self(1), self(2)))
	}
}

// joinMetaCopy: changing the Meta map passed to Join afterwards changes no view.
func joinMetaCopy(e *env) {
	m := self(0)
	e.joinAs("a", m)
	w := e.watch("a")
	m.Meta["zone"] = "changed"
	m.Meta["extra"] = "added"
	e.join("b")
	w.eventually(lists(self(0), self(1)))
}

// joinTwice: a second Join on an instance that joined is an error, with its own member or
// another.
func joinTwice(e *env) {
	e.join("a")
	a := e.inst("a")
	for _, m := range []core.Member{self(0), self(4)} {
		if err := a.Join(e.ctx, m); err == nil {
			e.t.Fatalf("Join(ctx, %s) = nil, want an error", fmtMember(m))
		}
	}
	w := e.watch("a")
	w.eventually(lists(self(0)))
}

// joinInvalid: Join with an invalid member is an error that wraps core.ErrInvalidMember and
// changes nothing: the instance joins validly afterwards, and no view lists the invalid member.
func joinInvalid(e *env) {
	slash := self(0)
	slash.ID = "m0/x"
	slot := self(0)
	slot.Slot = -2
	a := e.inst("a")
	for _, m := range []core.Member{slash, slot} {
		if err := a.Join(e.ctx, m); !errors.Is(err, core.ErrInvalidMember) {
			e.t.Fatalf("Join(ctx, %s) = %s, want an error wrapping %s", fmtMember(m), errText(err),
				core.ErrInvalidMember)
		}
	}
	e.join("a")
	w := e.watch("a")
	v := w.eventually(lists(self(0)))
	if !omits(slash.ID).ok(v) {
		e.t.Fatalf("view %d on a: %s, want %s", v.Version, fmtMembers(v), omits(slash.ID).want)
	}
}

// joinCanceled: Join with a done context returns the context's error and changes nothing.
func joinCanceled(e *env) {
	e.join("b")
	w := e.watch("b")
	w.eventually(lists(self(1)))
	ctx, cancel := context.WithCancel(e.ctx)
	cancel()
	if err := e.inst("a").Join(ctx, self(0)); !errors.Is(err, context.Canceled) {
		e.t.Fatalf("Join(canceled ctx, %s) = %s, want %s", fmtMember(self(0)), errText(err), context.Canceled)
	}
	w.quiet()
	e.join("a")
	w.eventually(lists(self(0), self(1)))
}

// drainingVisible: Draining set to true and then back to false reaches every instance.
func drainingVisible(e *env) {
	e.join("a")
	wa := e.watch("a")
	e.join("b")
	wb := e.watch("b")
	for _, d := range []bool{true, false} {
		e.setDraining("a", d)
		want := self(0)
		want.Draining = d
		wa.eventually(lists(want, self(1)))
		wb.eventually(lists(want, self(1)))
	}
}

// drainingNoop: setting Draining to the value it has makes no view.
func drainingNoop(e *env) {
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
	e.setDraining("a", false)
	w.quiet()
	e.setDraining("a", true)
	w.eventually(lists(draining(self(0))))
	e.setDraining("a", true)
	w.quiet()
}

// drainingBeforeJoin: SetDraining before Join is an error and changes nothing.
func drainingBeforeJoin(e *env) {
	if err := e.inst("a").SetDraining(e.ctx, true); err == nil {
		e.t.Fatal("SetDraining(ctx, true) = nil, want an error")
	}
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
}

// leaveVisible: a member that leaves leaves the views of every other instance.
func leaveVisible(e *env) {
	e.join("a")
	wa := e.watch("a")
	e.join("b")
	e.join("c")
	wc := e.watch("c")
	all := lists(self(0), self(1), self(2))
	wa.eventually(all)
	wc.eventually(all)
	e.leave("b")
	wa.eventually(omits(self(1).ID))
	wc.eventually(omits(self(1).ID))
}

// leaveClosed: after Leave, Join, SetDraining and Watch return tenure.ErrClosed, the instance's
// channel delivers the view without its member and then closes, and Leave again returns nil.
func leaveClosed(e *env) {
	e.join("a")
	w := e.watch("a")
	w.eventually(lists(self(0)))
	e.leave("a")
	e.closed("a")
	w.closes("Leave returned")
	if gone := omits(self(0).ID); !gone.ok(w.last) {
		e.t.Fatalf("view %d on a: %s and then a closed channel, want %s before the channel closes",
			w.last.Version, fmtMembers(w.last), gone.want)
	}
	e.leave("a")
}

// leaveBeforeJoin: Leave before Join returns nil and closes the instance; Leave again returns
// nil.
func leaveBeforeJoin(e *env) {
	e.leave("a")
	e.closed("a")
	e.leave("a")
}

// propagationEstimate: the estimate is between 0 and Settle.
func propagationEstimate(e *env) {
	p, _ := e.inst("a").(tenure.PropagationEstimator)
	if d := p.Propagation(); d < 0 || d > e.f.Settle {
		e.t.Fatalf("Propagation() = %v, want a duration from 0s to %v", d, e.f.Settle)
	}
}
