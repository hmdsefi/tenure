// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"slices"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
)

// The labels of the mono timers of a run.
const (
	labelRunWrite   = "tenure/driver/run_write"
	labelRunRestart = "tenure/driver/run_restart"
	labelRunTimeout = "tenure/driver/run_timeout"
)

// The ops of tenure.run.
const (
	runOpStart   = "start"
	runOpRaise   = "raise"
	runOpReturn  = "return"
	runOpRestart = "restart"
	runOpStop    = "stop"
	runOpStopped = "stopped"
)

// The texts of a tenure.run return: why the call of the run returned.
const (
	runReturnRejected = "rejected" // the fence rejected a write
	runReturnTimeout  = "timeout"  // the fence did not answer within RetryPeriod
	runReturnCheck    = "check: "  // Check failed, followed by the reason
)

// run is one simulated Run of a shard, which a StartRun of the member's machine starts and its
// StopRun stops (SIM-410, SIM-420). It does what a service's Run does against a fenced
// downstream: each call of it raises the fence of the shard to its term, then writes to the
// shard in its term, one write at a time, at intervals drawn from tenure/run/<member>, uniform
// in [0, RetryPeriod].
//
// Check (RUN-310). Before each write the run checks that it owns the shard: a shard of an
// unfenced space is owned only while the member's clock reads less than its session deadline
// (the latest deadline of a StartRun or an ExtendDeadline), and a shard of a fenced space while
// its machine has not stopped it, degraded or not; a run whose StopRun came owns nothing. When
// Check fails, the call returns without writing, unless the scenario has IgnoreCheck: then the
// run writes all the same.
//
// Returns and restarts (RUN-330). A call returns when the fence rejects one of its writes, or
// when a raise or a write has no answer RetryPeriod after it went out, on the member's clock.
// The runtime's supervisor then restarts the run unless Check fails: with c the count of
// consecutive returns, reset to 0 first when the call lasted at least LeaseDuration and then
// incremented, after min(RetryPeriod·2^(c−1), LeaseDuration). A call that returned because
// Check failed is never restarted: the shard is not owned any more, and its StopRun comes.
//
// Stop (SIM-420). From its StopRun the run owns nothing, and a pending restart is dropped. The
// run takes a stop delay drawn from tenure/run/<member>, uniform in [0, StopDelay], to stop:
// until then it goes on as Check lets it, so with IgnoreCheck it keeps writing. When the delay
// is over, the request it has in flight, if any, is abandoned, as a canceled call is, and in the
// same event the driver steps RunStopped. A reply to an abandoned request is dropped; a write
// sent before may still reach the fence, which is what the fence is for.
//
// A run lives in its member's incarnation: a crash drops its timers with the node's, and a new
// incarnation stops the driver of the old one, its runs with it.
//
// Every step of a run emits tenure.run with op start (at its StartRun), raise (each raise sent,
// with the request's text), return (with the reason as text: rejected, timeout, or "check: "
// and the reason Check failed), restart (when a backoff ends, before the raise), stop (at its
// StopRun) and stopped (right before RunStopped is stepped).
type run struct {
	d      *driver
	space  string
	shard  int
	term   core.Term
	fenced bool

	count  int           // the consecutive returns (RUN-330)
	callAt time.Duration // the reading at which the present call started
	inCall bool          // a call is going on: false once it returned, until its restart

	// req is the ID of the fence request in flight, 0 for none, and reqTimer its timeout.
	req      uint64
	reqTimer *monoTimer
	// next is the timer of the next write, or of the restart after a return.
	next *monoTimer

	stopping  bool            // its StopRun came
	stopTimer *monoTimer      // the end of the stop delay
	reason    core.StopReason // the reason of its StopRun, while stopping
	done      bool            // RunStopped was stepped, or the driver stopped
}

// startRun is StartRun's part of the driver: it raises the session deadline that Check reads to
// a's and starts a run of the shard in a's term.
func (d *driver) startRun(a core.StartRun) {
	d.deadline = max(d.deadline, a.Deadline)
	r := &run{d: d, space: a.Space, shard: a.Shard, term: a.Term, fenced: d.fenced(a.Space)}
	d.runs = append(d.runs, r)
	r.emit("", runOpStart)
	r.call()
}

// stopRun is StopRun's part of the driver: it stops the shard's run in a's term, if one runs
// that has not been told to stop yet.
func (d *driver) stopRun(a core.StopRun) {
	for _, r := range d.runs {
		if r.space == a.Space && r.shard == a.Shard && r.term == a.Term && !r.stopping {
			r.stop(a.Reason)
			return
		}
	}
}

// extendDeadline is ExtendDeadline's part of the driver: Check reads the new deadline.
func (d *driver) extendDeadline(a core.ExtendDeadline) {
	d.deadline = max(d.deadline, a.Deadline)
}

// fenced reports whether the space called name is fenced.
func (d *driver) fenced(name string) bool {
	for _, sp := range d.w.sc.Spaces {
		if sp.Name == name {
			return sp.Fenced
		}
	}
	return false
}

// stopRuns ends every run of the driver at once, with no record and no RunStopped: the driver's
// incarnation is over.
func (d *driver) stopRuns() {
	for _, r := range d.runs {
		r.done = true
		r.cancelTimers()
	}
	d.runs = nil
	d.fenceCalls = nil
}

// runReply takes a reply of the fence: it goes to the run whose request it answers. A reply to a
// request that timed out or was abandoned, or of an earlier incarnation, is dropped.
func (d *driver) runReply(rep reply) {
	id := rep.requestID()
	r := d.fenceCalls[id]
	if r == nil {
		return
	}
	delete(d.fenceCalls, id)
	r.answered(rep)
}

// emit emits the run's tenure.run with op and text.
func (r *run) emit(text, op string) {
	r.d.w.emitRun(r.d.m, text, op, r.space, r.shard, r.term)
}

// now returns the member's monotonic reading.
func (r *run) now() time.Duration { return r.d.m.Mono() }

// call starts a call of the run: it raises the fence of the shard to the run's term.
func (r *run) call() {
	r.inCall = true
	r.callAt = r.now()
	d := r.d
	d.md.lastID++
	req := raiseReq{reqHead: reqHead{ID: d.md.lastID, Holder: d.holder}, Space: r.space, Shard: r.shard, Term: r.term}
	r.emit(req.Describe(), runOpRaise)
	r.send(req)
}

// send sends req to the fence and sets its timeout, RetryPeriod later on the member's clock.
func (r *run) send(req request) {
	d := r.d
	id := req.head().ID
	r.req = id
	if d.fenceCalls == nil {
		d.fenceCalls = make(map[uint64]*run)
	}
	d.fenceCalls[id] = r
	r.reqTimer = d.w.setTimer(d.m.Node, r.now()+d.timing.RetryPeriod, labelRunTimeout, func() {
		delete(d.fenceCalls, id)
		r.req = 0
		r.returned(runReturnTimeout)
	})
	d.w.fw.Net.Send(d.m.Node, d.w.fence.ID(), req)
}

// answered takes the fence's reply to the request in flight.
func (r *run) answered(rep reply) {
	r.req = 0
	r.reqTimer.cancel()
	switch rep := rep.(type) {
	case raisedReply:
		r.waitWrite()
	case writtenReply:
		if !rep.Accepted {
			r.returned(runReturnRejected)
			return
		}
		r.waitWrite()
	}
}

// waitWrite sets the timer of the next write, an interval drawn from tenure/run/<member>,
// uniform in [0, RetryPeriod], from now.
func (r *run) waitWrite() {
	interval := kernel.Uniform(r.d.md.runs, 0, r.d.timing.RetryPeriod)
	r.next = r.d.w.setTimer(r.d.m.Node, r.now()+interval, labelRunWrite, r.write)
}

// write runs at the time of a write: it checks, then writes, or returns when Check fails and
// the scenario does not ignore it.
func (r *run) write() {
	if why := r.check(); why != "" && !r.d.w.sc.IgnoreCheck {
		r.returned(runReturnCheck + why)
		return
	}
	d := r.d
	d.md.lastID++
	r.send(writeReq{reqHead: reqHead{ID: d.md.lastID, Holder: d.holder}, Space: r.space, Shard: r.shard, Term: r.term})
}

// check is the run's Check (RUN-310): "" while the member owns the shard, else why not: the
// reason of its StopRun, or "expired" for a shard of an unfenced space at or after the session
// deadline.
func (r *run) check() string {
	switch {
	case r.stopping:
		return r.reason.String()
	case !r.fenced && r.now() >= r.d.deadline:
		return core.StopExpired.String()
	}
	return ""
}

// returned ends the present call, with why as the text of its tenure.run return, and sets the
// restart after the backoff of RUN-330, unless Check fails: a run that is not owned any more is
// not restarted.
func (r *run) returned(why string) {
	r.inCall = false
	r.emit(why, runOpReturn)
	if r.check() != "" {
		return
	}
	now := r.now()
	r.count = nextReturnCount(r.count, now-r.callAt, r.d.timing.LeaseDuration)
	r.next = r.d.w.setTimer(r.d.m.Node, now+restartBackoff(r.d.timing, r.count), labelRunRestart, func() {
		r.emit("", runOpRestart)
		r.call()
	})
}

// stop is the run's StopRun (SIM-420): it drops a pending restart and sets the end of the stop
// delay.
func (r *run) stop(reason core.StopReason) {
	r.stopping, r.reason = true, reason
	r.emit("", runOpStop)
	if !r.inCall && r.next != nil {
		r.next.cancel()
	}
	delay := time.Duration(0)
	if sd := r.d.w.sc.StopDelay; sd > 0 {
		delay = kernel.Uniform(r.d.md.runs, 0, sd)
	}
	r.stopTimer = r.d.w.setTimer(r.d.m.Node, r.now()+delay, labelRunStop, r.stopped)
}

// stopped runs when the stop delay is over: it abandons the request in flight, emits
// tenure.run stopped and steps RunStopped.
func (r *run) stopped() {
	d := r.d
	if d.stopped || r.done {
		return
	}
	r.done = true
	r.cancelTimers()
	if r.req != 0 {
		delete(d.fenceCalls, r.req)
		r.req = 0
	}
	if i := slices.Index(d.runs, r); i >= 0 {
		d.runs = slices.Delete(d.runs, i, i+1)
	}
	r.emit("", runOpStopped)
	d.step(core.RunStopped{Space: r.space, Shard: r.shard, Term: r.term}, "", "")
}

// cancelTimers cancels the run's pending timers.
func (r *run) cancelTimers() {
	for _, t := range []*monoTimer{r.next, r.reqTimer, r.stopTimer} {
		if t != nil {
			t.cancel()
		}
	}
}

// nextReturnCount returns the count of consecutive returns after a call that lasted lasted,
// with count returns before it (RUN-330): the count starts again from 0 after a call that
// lasted at least lease, and then grows by one.
func nextReturnCount(count int, lasted, lease time.Duration) int {
	if lasted >= lease {
		count = 0
	}
	return count + 1
}

// restartBackoff returns min(RetryPeriod·2^(count−1), LeaseDuration) for count ≥ 1, as the
// runtime's supervisor waits before it restarts a Run (RUN-330), without overflowing: the
// doubling stops once LeaseDuration is reached.
func restartBackoff(t core.Timing, count int) time.Duration {
	b := t.RetryPeriod
	for i := 1; i < count && b < t.LeaseDuration; i++ {
		b *= 2
	}
	return min(b, t.LeaseDuration)
}
