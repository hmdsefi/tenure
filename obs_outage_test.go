// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of the outage, loss, stuck and restart events: renewals failing, the session deadline
// passing, a lost session and the new incarnation that follows, fenced shards running degraded,
// and runs that do not stop. They use OBS §10's harness (obs_harness_test.go). The steps of the
// tests carrying an AT-OBS or AT-RUN number were written from the issues' text.

// sessionKinds are the session kinds.
var sessionKinds = []tenure.EventKind{
	tenure.EventSessionOpened, tenure.EventSessionRenewFailed, tenure.EventSessionRenewedLate,
	tenure.EventSessionRecovered, tenure.EventSessionExpired, tenure.EventSessionLost,
	tenure.EventSessionClosed, tenure.EventSessionRestarted,
}

// sessEv returns the session or node event of kind with no shard, slot or other field.
func sessEv(kind tenure.EventKind) tenure.Event {
	return tenure.Event{Kind: kind, Shard: -1, Slot: -1}
}

// kindsOf returns the kind of each event, in order.
func kindsOf(events []tenure.Event) []tenure.EventKind {
	out := make([]tenure.EventKind, len(events))
	for i, e := range events {
		out[i] = e.Kind
	}
	return out
}

// timesOf returns the Time of each event, relative to t0.
func timesOf(events []tenure.Event, t0 time.Time) []time.Duration {
	out := make([]time.Duration, len(events))
	for i, e := range events {
		out[i] = e.Time.Sub(t0)
	}
	return out
}

// wantErrEvents fails t unless events are one event of kind per instant of at (relative to t0),
// with no field set but Err, which matches target.
func wantErrEvents(t *testing.T, events []tenure.Event, kind tenure.EventKind, target error,
	t0 time.Time, at []time.Duration) {
	t.Helper()
	got := ofKinds(events, kind)
	if !slices.Equal(timesOf(got, t0), at) {
		t.Errorf("%v events at %v, want at %v", kind, timesOf(got, t0), at)
	}
	for _, e := range got {
		if !errors.Is(e.Err, target) {
			t.Errorf("%+v: Err %v, want %v", e, e.Err, target)
		}
		e.Err = nil
		if bare(e) != sessEv(kind) {
			t.Errorf("%+v: want no field set but Err", e)
		}
	}
}

// wantOnlyAt fails t unless events hold exactly the bare events want, each at t0 plus at.
func wantOnlyAt(t *testing.T, what string, events []tenure.Event, want []tenure.Event, t0 time.Time,
	at time.Duration) {
	t.Helper()
	if !reflect.DeepEqual(bareAll(events), want) {
		t.Errorf("%s = %+v, want %+v", what, bareAll(events), want)
	}
	for _, e := range events {
		if e.Time != t0.Add(at).UTC() {
			t.Errorf("%s: %+v at T0+%v, want T0+%v", what, e, e.Time.Sub(t0), at)
		}
	}
}

// recordsWith returns the log lines that contain every one of subs.
func recordsWith(lines []string, subs ...string) []string {
	var out []string
	for _, l := range lines {
		ok := true
		for _, s := range subs {
			ok = ok && strings.Contains(l, s)
		}
		if ok {
			out = append(out, l)
		}
	}
	return out
}

// firstTerms returns the term of the first Run of each of the harness's shards on n.
func firstTerms(t *testing.T, n *obsNode) [2]core.Term {
	t.Helper()
	var terms [2]core.Term
	for shard := range 2 {
		runs := n.Runs.Of(obsSpace, shard)
		if len(runs) == 0 {
			t.Fatalf("%s: no Run of %s/%d", n.ID, obsSpace, shard)
		}
		terms[shard] = runs[0].Shard.Term
	}
	return terms
}

// TestATOBS05OutageUnfenced checks an outage of an unfenced space: every KeepAlive from T0+2s
// fails with boom until T0+11s. Each failure emits session.renew_failed{Err: boom}; at the
// deadline, T0+10s, one session.expired, then shard.expired for both shards with their terms;
// their runs return, and their releases emit shard.released{Reason: "expired"} with no handoff.
// The KeepAlive at T0+12s succeeds: session.renewed_late{Duration: 2s}, then
// session.recovered{Count: 5}, and both shards are acquired again in greater terms.
func TestATOBS05OutageUnfenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		t0 := time.Now()
		a := newObsHarness(t, "a").Start("a")
		a.Store.Fail("KeepAlive", errBoom, 0)
		synctest.Wait()
		terms := firstTerms(t, a)

		sleepUntil(t0.Add(11 * time.Second))
		a.Store.Heal("KeepAlive")
		sleepUntil(t0.Add(20 * time.Second))
		events := a.Events.Events()

		wantErrEvents(t, events, tenure.EventSessionRenewFailed, errBoom, t0, secs(2, 4, 6, 8, 10))
		wantOnlyAt(t, "session.expired events", ofKinds(events, tenure.EventSessionExpired),
			[]tenure.Event{sessEv(tenure.EventSessionExpired)}, t0, 10*time.Second)
		late := sessEv(tenure.EventSessionRenewedLate)
		late.Duration = 2 * time.Second
		recovered := sessEv(tenure.EventSessionRecovered)
		recovered.Count = 5
		wantOnlyAt(t, "session.renewed_late events", ofKinds(events, tenure.EventSessionRenewedLate),
			[]tenure.Event{late}, t0, 12*time.Second)
		wantOnlyAt(t, "session.recovered events", ofKinds(events, tenure.EventSessionRecovered),
			[]tenure.Event{recovered}, t0, 12*time.Second)
		if i := indexOf(events, tenure.EventSessionRenewedLate, -1); i < 0 || i+1 >= len(events) ||
			events[i+1].Kind != tenure.EventSessionRecovered {
			t.Errorf("session.renewed_late at %d, want session.recovered right after it: %v", i, kindsOf(events))
		}

		expiredAt := indexOf(events, tenure.EventSessionExpired, -1)
		for shard := range 2 {
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 2 {
				t.Fatalf("s/%d runs %v, want 2", shard, runs)
			}
			wantStopped(t, runs[0], t0.Add(10*time.Second), "expired")
			term, again := terms[shard], runs[1].Shard.Term
			if again <= term || !runs[1].Start.Equal(t0.Add(12*time.Second)) {
				t.Errorf("s/%d: second Run at T0+%v in term %d, want T0+12s in a term above %d",
					shard, runs[1].Start.Sub(t0), again, term)
			}
			want := []tenure.Event{
				shardEv(tenure.EventShardAcquiring, shard, 0, ""),
				shardEv(tenure.EventShardAcquired, shard, term, ""),
				shardEv(tenure.EventShardExpired, shard, term, ""),
				shardEv(tenure.EventShardReleasing, shard, term, ""),
				shardEv(tenure.EventShardReleased, shard, term, "expired"),
				shardEv(tenure.EventShardAcquiring, shard, 0, ""),
				shardEv(tenure.EventShardAcquired, shard, again, ""),
			}
			got := ofShard(events, shard)
			if !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("s/%d events =\n%+v\nwant\n%+v", shard, bareAll(got), want)
				continue
			}
			if at := secs(0, 0, 10, 10, 10, 12, 12); !slices.Equal(timesOf(got, t0), at) {
				t.Errorf("s/%d events at %v, want %v", shard, timesOf(got, t0), at)
			}
			if i := indexOf(events, tenure.EventShardExpired, shard); i < expiredAt {
				t.Errorf("s/%d: shard.expired at %d, session.expired at %d: want the session first", shard, i, expiredAt)
			}
		}
		if i, j := indexOf(events, tenure.EventSessionRecovered, -1), lastIndexOf(events, tenure.EventShardAcquiring); j < i {
			t.Errorf("last shard.acquiring at %d, session.recovered at %d: want the recovery first", j, i)
		}
		for _, k := range []tenure.EventKind{tenure.EventSessionLost, tenure.EventSessionRestarted,
			tenure.EventShardLost, tenure.EventShardDegraded, tenure.EventShardStuck,
			tenure.EventShardHandoffCompleted, tenure.EventShardStopping} {
			if n := count(events, k); n != 0 {
				t.Errorf("%d %v events, want none", n, k)
			}
		}

		lines := a.Log.Lines()
		head := fmt.Sprintf(`"member":"a","holder":"%s"`, obsHolderA)
		for _, want := range []string{
			`{"level":"WARN","msg":"tenure: session renew failed",` + head + `,"error":"boom"}`,
			`{"level":"ERROR","msg":"tenure: session expired",` + head + `}`,
			`{"level":"WARN","msg":"tenure: session renewed late",` + head + `,"late_by":2000000000}`,
			`{"level":"INFO","msg":"tenure: session recovered",` + head + `,"failures":5}`,
			fmt.Sprintf(`{"level":"ERROR","msg":"tenure: shard expired",%s,"space":"s","shard":0,"term":%d}`, head, terms[0]),
		} {
			if !slices.Contains(lines, want) {
				t.Errorf("no log record %s", want)
			}
		}
		a.MustClose()
		wantDelivery(t, a)
		check()
	})
}

// lastIndexOf returns the index of the last event of kind, or -1 when there is none.
func lastIndexOf(events []tenure.Event, kind tenure.EventKind) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == kind {
			return i
		}
	}
	return -1
}

// fenced makes the harness's space fenced.
func fenced(c *tenure.Config) {
	c.Spaces[0].Fenced = true
}

// TestATOBS06OutageFenced checks the outage of TestATOBS05OutageUnfenced in a fenced space: at
// the deadline both shards emit shard.degraded with their terms, and none is expired or stopped:
// the runs keep going and the rows are degraded. After the recovery the shards are confirmed and
// emit shard.recovered with the same terms, and the rows are owned again. The session's own
// deadline still passes: one session.expired at T0+10s.
func TestATOBS06OutageFenced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		t0 := time.Now()
		a := newObsHarness(t, "a").Start("a", fenced)
		a.Store.Fail("KeepAlive", errBoom, 0)
		synctest.Wait()
		terms := firstTerms(t, a)

		sleepUntil(t0.Add(11 * time.Second))
		synctest.Wait()
		events := a.Events.Events()
		degraded := []tenure.Event{
			shardEv(tenure.EventShardDegraded, 0, terms[0], ""),
			shardEv(tenure.EventShardDegraded, 1, terms[1], ""),
		}
		wantOnlyAt(t, "shard.degraded events", ofKinds(events, tenure.EventShardDegraded), degraded, t0, 10*time.Second)
		wantRows(t, a.Node, rowWant{obsSpace, 0, "degraded", obsHolderA, terms[0]},
			rowWant{obsSpace, 1, "degraded", obsHolderA, terms[1]})
		for _, r := range a.Table().Spaces[0].Rows {
			if !r.Running || r.Stuck || r.Stop != "" {
				t.Errorf("row %+v during the outage, want running, not stuck or stopped", r)
			}
		}

		a.Store.Heal("KeepAlive")
		sleepUntil(t0.Add(20 * time.Second))
		events = a.Events.Events()
		recovered := []tenure.Event{
			shardEv(tenure.EventShardRecovered, 0, terms[0], ""),
			shardEv(tenure.EventShardRecovered, 1, terms[1], ""),
		}
		// The Confirm answers come back in any order.
		got := slices.SortedFunc(slices.Values(ofKinds(events, tenure.EventShardRecovered)),
			func(x, y tenure.Event) int { return x.Shard - y.Shard })
		wantOnlyAt(t, "shard.recovered events", got, recovered, t0, 12*time.Second)
		if i, j := indexOf(events, tenure.EventSessionRecovered, -1), indexOf(events, tenure.EventShardRecovered, -1); j < i {
			t.Errorf("shard.recovered at %d, session.recovered at %d: want the session first", j, i)
		}
		wantRows(t, a.Node, rowWant{obsSpace, 0, "owned", obsHolderA, terms[0]},
			rowWant{obsSpace, 1, "owned", obsHolderA, terms[1]})
		wantOnlyAt(t, "session.expired events", ofKinds(events, tenure.EventSessionExpired),
			[]tenure.Event{sessEv(tenure.EventSessionExpired)}, t0, 10*time.Second)
		for _, k := range []tenure.EventKind{tenure.EventShardExpired, tenure.EventShardLost,
			tenure.EventShardStopping, tenure.EventShardReleasing, tenure.EventShardReleased, tenure.EventShardStuck} {
			if n := count(events, k); n != 0 {
				t.Errorf("%d %v events, want none", n, k)
			}
		}
		for shard := range 2 {
			if n := count(ofShard(events, shard), tenure.EventShardAcquired); n != 1 {
				t.Errorf("s/%d: %d shard.acquired events, want 1", shard, n)
			}
		}
		wantRunning(t, a.Runs.Calls())
		if n := len(a.Runs.Calls()); n != 2 {
			t.Errorf("%d Run calls, want 2", n)
		}
		a.MustClose()
		wantDelivery(t, a)
		check()
	})
}

// obsHolderA2 is the holder of member a in its second incarnation.
const obsHolderA2 = "a/12121212121212121212121212121212"

// TestATOBS07SessionLost checks a lost session: the KeepAlive at T0+2s returns ErrSessionLost.
// session.renew_failed and then session.lost carry that error, both shards emit shard.lost with
// their terms, and once their runs have returned shard.released{Reason: "lost"} with no
// handoff. session.restarted{Holder: a/1111…11} follows, then the new incarnation's
// session.opened, and the shards are acquired again at once in greater terms (the memstore ends
// the lost holder's session when the member opens a new one) and owned by the new holder in the
// table. The log records name the first holder up to session.restarted, whose record names the
// new one and the previous one, and the new holder from then on.
func TestATOBS07SessionLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		t0 := time.Now()
		a := newObsHarness(t, "a").Start("a")
		a.Store.Fail("KeepAlive", tenure.ErrSessionLost, 1)
		synctest.Wait()
		terms := firstTerms(t, a)

		sleepUntil(t0.Add(30 * time.Second))
		events := a.Events.Events()
		wantSession := []tenure.EventKind{tenure.EventSessionOpened, tenure.EventSessionRenewFailed,
			tenure.EventSessionLost, tenure.EventSessionRestarted, tenure.EventSessionOpened}
		if got := kindsOf(ofKinds(events, sessionKinds...)); !slices.Equal(got, wantSession) {
			t.Fatalf("session events %v, want %v", got, wantSession)
		}
		wantErrEvents(t, events, tenure.EventSessionRenewFailed, tenure.ErrSessionLost, t0, secs(2))
		wantErrEvents(t, events, tenure.EventSessionLost, tenure.ErrSessionLost, t0, secs(2))
		restarted := sessEv(tenure.EventSessionRestarted)
		restarted.Holder = obsHolderA
		r := indexOf(events, tenure.EventSessionRestarted, -1)
		if got := bare(events[r]); got != restarted || events[r].Time != t0.Add(2*time.Second).UTC() {
			t.Errorf("session.restarted %+v at T0+%v, want %+v at T0+2s", got, events[r].Time.Sub(t0), restarted)
		}
		if o := lastIndexOf(events, tenure.EventSessionOpened); o < r {
			t.Errorf("second session.opened at %d, session.restarted at %d: want the restart first", o, r)
		}
		if got := a.Node.Holder(); got != obsHolderA2 {
			t.Errorf("Holder() = %q, want %q", got, obsHolderA2)
		}

		lost := indexOf(events, tenure.EventSessionLost, -1)
		for shard := range 2 {
			term := terms[shard]
			runs := a.Runs.Of(obsSpace, shard)
			if len(runs) != 2 || runs[1].Shard.Term <= term {
				t.Errorf("s/%d: runs %v, want a second Run in a term above %d", shard, runs, term)
				continue
			}
			wantStopped(t, runs[0], t0.Add(2*time.Second), "lost")
			got := ofShard(events, shard)
			want := []tenure.Event{
				shardEv(tenure.EventShardAcquiring, shard, 0, ""),
				shardEv(tenure.EventShardAcquired, shard, term, ""),
				shardEv(tenure.EventShardLost, shard, term, ""),
				shardEv(tenure.EventShardReleased, shard, term, "lost"),
				shardEv(tenure.EventShardAcquiring, shard, 0, ""),
				shardEv(tenure.EventShardAcquired, shard, runs[1].Shard.Term, ""),
			}
			if !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("s/%d events =\n%+v\nwant\n%+v", shard, bareAll(got), want)
				continue
			}
			if at := secs(0, 0, 2, 2, 2, 2); !slices.Equal(timesOf(got, t0), at) {
				t.Errorf("s/%d events at %v, want %v", shard, timesOf(got, t0), at)
			}
			if i := indexOf(events, tenure.EventShardLost, shard); i < lost {
				t.Errorf("s/%d: shard.lost at %d, session.lost at %d: want the session first", shard, i, lost)
			}
			if i := indexOf(events, tenure.EventShardReleased, shard); i > r {
				t.Errorf("s/%d: shard.released at %d, session.restarted at %d: want the release first", shard, i, r)
			}
		}
		wantRows(t, a.Node, rowWant{obsSpace, 0, "owned", obsHolderA2, a.Runs.Of(obsSpace, 0)[1].Shard.Term},
			rowWant{obsSpace, 1, "owned", obsHolderA2, a.Runs.Of(obsSpace, 1)[1].Shard.Term})
		if n := count(events, tenure.EventShardHandoffCompleted); n != 0 {
			t.Errorf("%d shard.handoff_completed events, want none", n)
		}

		// The records match the events one to one (wantDelivery).
		lines := a.Log.Lines()
		wantDelivery(t, a)
		for i, l := range lines {
			holder := obsHolderA
			if i >= r {
				holder = obsHolderA2
			}
			if !strings.Contains(l, fmt.Sprintf(`"holder":"%s"`, holder)) {
				t.Errorf("record %d %s: want holder %s", i+1, l, holder)
			}
		}
		want := fmt.Sprintf(`{"level":"INFO","msg":"tenure: session restarted","member":"a","holder":"%s","previous":"%s"}`,
			obsHolderA2, obsHolderA)
		if r < len(lines) && lines[r] != want {
			t.Errorf("session.restarted record %s, want %s", lines[r], want)
		}
		if got := recordsWith(lines, `"msg":"tenure: session lost"`); len(got) != 1 ||
			!strings.Contains(got[0], `"level":"ERROR"`) || !strings.Contains(got[0], `"error":"tenure: session lost"`) {
			t.Errorf("session.lost records %v, want one ERROR with the error", got)
		}
		a.MustClose()
		check()
	})
}

// TestATOBS08Stuck checks runs that do not stop: Drain at T0+1s stops both runs, which return
// 15 s after their stop. RenewDeadline after the stop, at T0+11s, each shard emits one
// shard.stuck{Reason: "draining"} with its term, and its row is stopping, running and stuck.
// Once Run returns, at T0+16s, the shard is released, and its row is no longer stuck.
func TestATOBS08Stuck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t, "a").Node("a")
		a.Runs.Script(stopAfter(15 * time.Second))
		t0 := time.Now()
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start(a) = %v, want nil", err)
		}
		synctest.Wait()
		terms := firstTerms(t, a)
		sleepUntil(t0.Add(time.Second))
		drained := make(chan error, 1)
		go func() { drained <- a.Drain(context.Background()) }()

		sleepUntil(t0.Add(11*time.Second - time.Nanosecond))
		synctest.Wait()
		if n := count(a.Events.Events(), tenure.EventShardStuck); n != 0 {
			t.Errorf("%d shard.stuck events before T0+11s, want none", n)
		}
		sleepUntil(t0.Add(11 * time.Second))
		synctest.Wait()
		stuck := []tenure.Event{
			shardEv(tenure.EventShardStuck, 0, terms[0], "draining"),
			shardEv(tenure.EventShardStuck, 1, terms[1], "draining"),
		}
		wantOnlyAt(t, "shard.stuck events", ofKinds(a.Events.Events(), tenure.EventShardStuck), stuck, t0, 11*time.Second)
		for shard, r := range a.Table().Spaces[0].Rows {
			if r.State != "stopping" || !r.Running || !r.Stuck || r.Stop != "draining" || r.Term != terms[shard] {
				t.Errorf("row %+v at T0+11s, want stopping, running and stuck in term %d", r, terms[shard])
			}
		}

		if err := <-drained; err != nil {
			t.Errorf("Drain() = %v, want nil", err)
		}
		synctest.Wait() // the delivery of the drain's last events
		events := a.Events.Events()
		wantOnlyAt(t, "shard.stuck events", ofKinds(events, tenure.EventShardStuck), stuck, t0, 11*time.Second)
		for shard := range 2 {
			run := a.Runs.Of(obsSpace, shard)[0]
			if !run.End.Equal(t0.Add(16 * time.Second)) {
				t.Errorf("%v returned at T0+%v, want T0+16s", run, run.End.Sub(t0))
			}
			got := ofShard(events, shard)
			want := lifecycle(shard, terms[shard], "draining", 15*time.Second)
			want = slices.Insert(want, 3, stuck[shard])
			if !reflect.DeepEqual(bareAll(got), want) {
				t.Errorf("s/%d events =\n%+v\nwant\n%+v", shard, bareAll(got), want)
				continue
			}
			if at := secs(0, 0, 1, 11, 16, 16, 16); !slices.Equal(timesOf(got, t0), at) {
				t.Errorf("s/%d events at %v, want %v", shard, timesOf(got, t0), at)
			}
		}
		for _, r := range a.Table().Spaces[0].Rows {
			if r.Stuck || r.Running || r.State != "idle" {
				t.Errorf("row %+v after the release, want idle, not running or stuck", r)
			}
		}
		a.MustClose()
		wantDelivery(t, a)
		check()
	})
}

// TestATRUN90OutageAndLossEvents checks the runtime's events (RUN-700) through an outage and a
// lost session of one node with the 8 shards of orders, as OnEvent gets them in step order. Every
// KeepAlive fails from T0+2s to T0+11s, so the session expires at T0+10s and is renewed late at
// T0+12s; the KeepAlive of H1 at T0+20s returns ErrSessionLost, and a new incarnation H2 follows.
// The session events are exactly opened, five renew_failed, expired, renewed_late, recovered{5},
// renew_failed and lost (ErrSessionLost), restarted{H1} and opened; and each shard goes through
// acquired, expired, released{expired}, acquired, lost, released{lost} and acquired (by H2, at
// T0+20s), each acquired term above the one before.
func TestATRUN90OutageAndLossEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		r := newShardRig(t)
		cfg, events := r.config(), &eventRec{}
		cfg.Rand, cfg.OnEvent = recoveryRand(), events.OnEvent
		t0 := time.Now()
		n := start(t, context.Background(), cfg)
		r.fs.Fail("KeepAlive", errBoom)
		sleepUntil(t0.Add(11 * time.Second))
		r.fs.Fail("KeepAlive", nil)
		sleepUntil(t0.Add(19 * time.Second))
		loseSession(r.fs)
		sleepUntil(t0.Add(60 * time.Second))
		got := events.Events()

		wantSession := []tenure.EventKind{
			tenure.EventSessionOpened,
			tenure.EventSessionRenewFailed, tenure.EventSessionRenewFailed, tenure.EventSessionRenewFailed,
			tenure.EventSessionRenewFailed, tenure.EventSessionExpired, tenure.EventSessionRenewFailed,
			tenure.EventSessionRenewedLate, tenure.EventSessionRecovered,
			tenure.EventSessionRenewFailed, tenure.EventSessionLost, tenure.EventSessionRestarted,
			tenure.EventSessionOpened,
		}
		session := ofKinds(got, sessionKinds...)
		if !slices.Equal(kindsOf(session), wantSession) {
			t.Fatalf("session events %v, want %v", kindsOf(session), wantSession)
		}
		if at := secs(0, 2, 4, 6, 8, 10, 10, 12, 12, 20, 20, 20, 20); !slices.Equal(timesOf(session, t0), at) {
			t.Errorf("session events at %v, want %v", timesOf(session, t0), at)
		}
		if e := session[8]; e.Count != 5 {
			t.Errorf("session.recovered Count %d, want 5", e.Count)
		}
		if e := session[11]; e.Holder != holder01 {
			t.Errorf("session.restarted Holder %q, want %q", e.Holder, holder01)
		}
		wantErrEvents(t, got, tenure.EventSessionLost, tenure.ErrSessionLost, t0, secs(20))

		wantKinds := []tenure.EventKind{
			tenure.EventShardAcquired, tenure.EventShardExpired, tenure.EventShardReleased,
			tenure.EventShardAcquired, tenure.EventShardLost, tenure.EventShardReleased, tenure.EventShardAcquired,
		}
		watch := []tenure.EventKind{
			tenure.EventShardAcquired, tenure.EventShardDegraded, tenure.EventShardRecovered,
			tenure.EventShardExpired, tenure.EventShardLost, tenure.EventShardStopping, tenure.EventShardStuck,
			tenure.EventShardReleased, tenure.EventShardHandoffCompleted,
		}
		for _, id := range allShards {
			var mine []tenure.Event
			for _, e := range ofKinds(got, watch...) {
				if e.Space == "orders" && e.Shard == id {
					mine = append(mine, e)
				}
			}
			if !slices.Equal(kindsOf(mine), wantKinds) {
				t.Errorf("orders/%d events %v, want %v", id, kindsOf(mine), wantKinds)
				continue
			}
			if mine[2].Reason != "expired" || mine[5].Reason != "lost" {
				t.Errorf("orders/%d released with %q and %q, want expired and lost", id, mine[2].Reason, mine[5].Reason)
			}
			for i, e := range mine {
				if i > 0 && e.Kind == tenure.EventShardAcquired && e.Term <= mine[i-1].Term ||
					i > 0 && e.Kind != tenure.EventShardAcquired && e.Term != mine[i-1].Term {
					t.Errorf("orders/%d events %+v: want each acquired term above the last, the others on it", id, mine)
					break
				}
			}
			if !mine[6].Time.Equal(t0.Add(20 * time.Second).UTC()) {
				t.Errorf("orders/%d acquired by H2 at T0+%v, want T0+20s", id, mine[6].Time.Sub(t0))
			}
		}
		closeNode(t, n)
		check()
	})
}
