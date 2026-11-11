// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The record kinds the audits read (SIM §8), besides those of steady_test.go.
const (
	kindGrant     = "tenure.grant"
	kindWallStep  = "tenure.wall_step"
	kindReplyLate = "tenure.reply_late"
)

// auditSeeds is the number of seeds of each rule the audits run, each with every record kept.
const auditSeeds = 5

// TestATSIM13TermsGrow checks AT-SIM-13 (SIM-240) under each rule, with every record kept, in
// TestWallClock's world, wall clocks up to an hour off that step by up to 10 min either way,
// with TestCrashRestart's crashes added so that shards change hands throughout the run. The
// steps were written for this task from the issue and the design note, since the original
// acceptance-test text is lost:
//
//  1. Every tenure.grant of a shard has a term above every earlier term of the shard.
//  2. Every term is core.NextTerm of the shard's previous term and the wall reading the grant
//     was decided at: the store's wall clock under the Server rule (no offset and, with no
//     drift, the start of virtual time plus the global time), and the wall reading the cas
//     request carries under the Observer rule.
//  3. Under the Observer rule the run reaches the case the rule is about: over the seeds, at
//     least one grant is decided at a wall reading at or below the shard's previous term (in
//     Unix milliseconds), so the term is the previous one plus one; and at least one grant
//     goes to a member whose wall clock has stepped backwards before it.
func TestATSIM13TermsGrow(t *testing.T) {
	r := rowNamed(t, "TestWallClock")
	r.sc.Faults = rowNamed(t, "TestCrashRestart").sc.Faults
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Seeds = auditSeeds
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			// By seed: a seed that runs twice, for the determinism check, counts once.
			bySeed := map[uint64]termCover{}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				fw.Final("test/terms", func() error {
					c, err := termsGrow(fw.Sim.Records(), w.Scenario().Takeover)
					bySeed[fw.Seed()] = c
					return err
				})
			}))
			var cover termCover
			for _, c := range bySeed {
				cover.add(c)
			}
			t.Logf("%v: %d grants at a wall reading at or below the previous term, %d after a step backwards",
				rule, cover.fromPrev, cover.afterBackStep)
			if t.Failed() || rule != world.Observer {
				return
			}
			if cover.fromPrev == 0 || cover.afterBackStep == 0 {
				t.Errorf("observer: %d grants at a wall reading at or below the previous term, %d after a step backwards; want some of each",
					cover.fromPrev, cover.afterBackStep)
			}
		})
	}
}

// termCover counts the grants of TestATSIM13TermsGrow's step 3.
type termCover struct {
	fromPrev      int // grants whose term is the previous term plus one, the wall reading being at or below it
	afterBackStep int // grants to a member whose wall clock stepped backwards before
}

func (c *termCover) add(o termCover) {
	c.fromPrev += o.fromPrev
	c.afterBackStep += o.afterBackStep
}

// casWall matches the wall reading of a cas request's text, as "wall=3600.000000000s".
var casWall = regexp.MustCompile(` wall=(-?[0-9]+)\.([0-9]{9})s`)

// termsGrow checks steps 1 and 2 of TestATSIM13TermsGrow over a full trace and counts what
// step 3 needs.
func termsGrow(recs []kernel.Record, rule world.Takeover) (termCover, error) {
	var cover termCover
	terms := map[string]core.Term{} // the latest term granted, by shard
	backStep := map[string]bool{}   // the members whose wall clock has stepped backwards, by name
	names := nodeNames(recs)
	for _, r := range recs {
		switch r.Kind {
		case kindWallStep:
			if strings.HasPrefix(attr(r, "delta_ns"), "-") {
				backStep[names[r.Node]] = true
			}
		case kindGrant:
			shard := attr(r, "space") + "/" + attr(r, "shard")
			term, err := strconv.ParseUint(attr(r, "term"), 10, 64)
			if err != nil {
				return cover, fmt.Errorf("grant at %v: term %q: %w", r.At, attr(r, "term"), err)
			}
			prev := terms[shard]
			if core.Term(term) <= prev {
				return cover, fmt.Errorf("%s: term %d granted at %v after term %d", shard, term, r.At, prev)
			}
			wall, err := grantWall(r, rule)
			if err != nil {
				return cover, err
			}
			if want := core.NextTerm(prev, wall); core.Term(term) != want {
				return cover, fmt.Errorf("%s: term %d granted at %v at wall %v after term %d, want NextTerm %d",
					shard, term, r.At, wall.Sub(kernel.Epoch), prev, want)
			}
			if prev > 0 && core.Term(term) == prev+1 && core.NextTerm(0, wall) <= prev { // the wall's milliseconds are at most prev
				cover.fromPrev++
			}
			if member, _, _ := strings.Cut(attr(r, "holder"), "/"); backStep[member] {
				cover.afterBackStep++
			}
			terms[shard] = core.Term(term)
		}
	}
	return cover, nil
}

// grantWall returns the wall reading the grant r was decided at: under the Observer rule the
// one the cas request carries, in r's text; under the Server rule the store's, which has no
// offset, at the global time of r, the store's clock having no drift in the worlds this runs.
func grantWall(r kernel.Record, rule world.Takeover) (time.Time, error) {
	if rule == world.Server {
		return kernel.Epoch.Add(time.Duration(r.At)), nil
	}
	m := casWall.FindStringSubmatch(r.Text)
	if m == nil {
		return time.Time{}, fmt.Errorf("grant at %v: no wall reading in %q", r.At, r.Text)
	}
	sec, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	ns, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	d := time.Duration(sec)*time.Second + time.Duration(ns)
	if strings.HasPrefix(m[1], "-") {
		d = time.Duration(sec)*time.Second - time.Duration(ns)
	}
	return kernel.Epoch.Add(d), nil
}

// nodeNames returns the name of each node, by ID, from the kernel.add_node records of a full
// trace.
func nodeNames(recs []kernel.Record) map[kernel.NodeID]string {
	names := map[kernel.NodeID]string{}
	for _, r := range recs {
		if r.Kind == "kernel.add_node" {
			names[r.Node] = r.Text
		}
	}
	return names
}

// TestATSIM48DriverAudit checks AT-SIM-48 under each rule, with every record kept, over the
// faulty worlds of TestCrashRestart (crashes and restarts, so many incarnations),
// TestPartitions (lost sessions and recoveries, late replies), TestPauses (pauses, after which
// a member's events come due at once) and TestStoreFaults (a store that crashes and is cut
// off, so timeouts and resyncs): every tenure.input,
// tenure.action and tenure.reply_late record of every member is audited against the driver
// contract (MCH O1 to O8, SIM-300 to SIM-340). The steps were written for this task from the
// issue and the design note, since the original acceptance-test text is lost. auditDriver
// lists the audit.
func TestATSIM48DriverAudit(t *testing.T) {
	for _, name := range []string{"TestCrashRestart", "TestPartitions", "TestPauses", "TestStoreFaults"} {
		r := rowNamed(t, name)
		for _, rule := range r.takeovers() {
			t.Run(name+"/"+rule.String(), func(t *testing.T) {
				opts := r.options()
				opts.Seeds = auditSeeds
				opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
				faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
					fw.Final("test/driver_audit", func() error {
						return auditDriver(fw.Sim.Records(), w.Members(), w.Scenario().Timing)
					})
				}))
			})
		}
	}
}

// The parts of the texts the audit reads: a request's or a reply's name and request ID, at the
// start of its text ("keepAlive#45 m4/…", "renewed#56 version=6"), and the version of a granted
// reply's record ("granted#8 book/3 holder=m1/0a term=5 version=9").
var (
	textID       = regexp.MustCompile(`^([a-zA-Z]+)#([0-9]+)(?: |$)`)
	grantVersion = regexp.MustCompile(`^granted#[0-9]+ \S+ holder=\S+ term=[0-9]+ version=([0-9]+)`)
)

// coalescing are the actions of which a driver never has two equal ones queued or in flight
// (O6).
var coalescing = map[string]bool{"Release": true, "ReleaseSlot": true, "ClaimSlot": true, "CloseSession": true}

// auditDriver audits the records of every member of a full trace against the driver contract,
// member by member, in record order. The audit:
//
//  1. Incarnations: the first input after each tenure.boot of a member is a Tick (SIM-300).
//  2. Monotonic readings: within an incarnation, the now_ns of every input is at least that of
//     the input before (MCH O1).
//  3. Wake first: no input but a Tick is stepped at a reading at or after the wake of the
//     latest Wake action, which every step with an armed timer ends with; a step without one
//     disarms the wake (MCH O2, SIM-300).
//  4. Actions follow their input: every tenure.action comes in the event of an input of the
//     same member, after it.
//  5. One answer per call: the inputs whose text is a reply to request N (its name and #N)
//     all come in one event, never after a tenure.reply_late for N; N has at most one
//     tenure.reply_late, and none once answered; and no two actions send the same request ID
//     (SIM-320). A reply that answers several inputs, as a lost reply to an Acquire, steps
//     them in that one event.
//  6. No repeated Release, ReleaseSlot, ClaimSlot or CloseSession: such an action that goes
//     out (its record has the request's text) is never equal, in type and fields, to one that
//     went out earlier in the incarnation and is still in flight: neither answered by an input
//     nor late, and sent less than RetryPeriod of the member's clock before (a request times
//     out RetryPeriod after it goes out, which is no earlier than the step that made it) (MCH
//     O6).
//  7. Versions only grow: within an incarnation, per shard, the version of every stepped
//     HolderChanged is above the latest version the machine has had for the shard, from a
//     HolderChanged or the granted reply of an Acquired, or equal to it for the answer of a
//     Confirm (src=confirm) (MCH O4, SIM-335).
func auditDriver(recs []kernel.Record, members []*world.Member, timing core.Timing) error {
	var errs []error
	for _, m := range members {
		if err := auditMember(recs, m.Node.ID(), m.Name, timing.RetryPeriod); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// auditIncarnation is what auditMember keeps of the present incarnation of a member.
type auditIncarnation struct {
	booted    bool
	first     bool          // the next input is the incarnation's first
	inputs    int           // inputs stepped so far
	lastNow   time.Duration // the now_ns of the latest input
	wake      time.Duration // the at_ns of the latest Wake action, when armed
	armed     bool
	inEvent   bool              // an input of the member was stepped in the present event
	stepNow   time.Duration     // the now_ns of the latest input: the reading of its actions
	out       []coalesced       // the coalescing actions sent, not known done
	delivered map[string]uint64 // by shard, the latest version the machine has had
}

// coalesced is one coalescing action that went out.
type coalesced struct {
	key    string // type and fields
	id     uint64
	sentAt time.Duration
}

// auditRequest is what auditMember keeps of one request ID of a member.
type auditRequest struct {
	answered uint64 // the Seq of the kernel.event of its answer, 0 before one
	late     bool
}

// auditMember audits the records of the member whose node is id and whose name is name; see
// auditDriver. retry is the RetryPeriod.
func auditMember(recs []kernel.Record, id kernel.NodeID, name string, retry time.Duration) error {
	var inc auditIncarnation
	var event uint64                   // the Seq of the present kernel.event
	sent := map[uint64]bool{}          // the request IDs the member's actions sent
	reqs := map[uint64]*auditRequest{} // by request ID, over every incarnation
	request := func(n uint64) *auditRequest {
		if reqs[n] == nil {
			reqs[n] = &auditRequest{}
		}
		return reqs[n]
	}
	fail := func(r kernel.Record, format string, args ...any) error {
		return fmt.Errorf("%s at %v (seq %d): %s", name, r.At, r.Seq, fmt.Sprintf(format, args...))
	}
	for _, r := range recs {
		if r.Kind == kindEvent {
			event = r.Seq
			inc.inEvent = false
			continue
		}
		if r.Node != id {
			continue
		}
		switch r.Kind {
		case kindBoot:
			inc = auditIncarnation{booted: true, first: true, delivered: map[string]uint64{}}
		case kindInput:
			if !inc.booted {
				return fail(r, "an input before the first tenure.boot")
			}
			typ := attr(r, "type")
			now, err := attrNanos(r, "now_ns")
			if err != nil {
				return fail(r, "%v", err)
			}
			// 1. The first input of an incarnation is a Tick.
			if inc.first && typ != "Tick" {
				return fail(r, "the first input of the incarnation is %s, want a Tick", typ)
			}
			// 2. Readings never go back within an incarnation.
			if inc.inputs > 0 && now < inc.lastNow {
				return fail(r, "%s at reading %v after reading %v", typ, now, inc.lastNow)
			}
			// 3. A Tick comes first once the wake is due.
			if typ != "Tick" && inc.armed && now >= inc.wake {
				return fail(r, "%s at reading %v, at or after the wake %v, with no Tick first", typ, now, inc.wake)
			}
			inc.first, inc.inEvent, inc.armed = false, true, false
			inc.inputs++
			inc.lastNow, inc.stepNow = now, now
			// 5. One answer per request, in one event.
			if n, ok := requestID(r.Text); ok {
				q := request(n)
				switch {
				case q.late:
					return fail(r, "%s answers request %d after its reply came late", typ, n)
				case q.answered != 0 && q.answered != event:
					return fail(r, "%s answers request %d again, in a later event", typ, n)
				}
				q.answered = event
				inc.out = doneOut(inc.out, n)
			}
			// 7. Versions only grow.
			if err := auditVersion(&inc, r, typ); err != nil {
				return fail(r, "%v", err)
			}
		case kindAction:
			// 4. Actions follow their input.
			if !inc.inEvent {
				return fail(r, "action %s with no input before it in its event", attr(r, "type"))
			}
			typ := attr(r, "type")
			if typ == "Wake" {
				at, err := attrNanos(r, "at_ns")
				if err != nil {
					return fail(r, "%v", err)
				}
				inc.wake, inc.armed = at, true
			}
			if r.Text == "" {
				continue // no request, or one the lanes dropped
			}
			n, ok := requestID(r.Text)
			if !ok {
				return fail(r, "action %s with the text %q, which names no request", typ, r.Text)
			}
			if sent[n] {
				return fail(r, "request %d sent twice", n)
			}
			sent[n] = true
			// 6. No repeated coalescing action while an equal one is in flight.
			if !coalescing[typ] {
				continue
			}
			key := actionKey(r)
			for _, o := range inc.out {
				if o.key == key && inc.stepNow < o.sentAt+retry && !request(o.id).late && request(o.id).answered == 0 {
					return fail(r, "%s sent as request %d while request %d of it is in flight", key, n, o.id)
				}
			}
			inc.out = append(inc.out, coalesced{key: key, id: n, sentAt: inc.stepNow})
		case kindReplyLate:
			n, err := strconv.ParseUint(attr(r, "req"), 10, 64)
			if err != nil {
				return fail(r, "reply_late req %q: %v", attr(r, "req"), err)
			}
			// 5. At most one late reply, and none for a request answered.
			q := request(n)
			if q.late || q.answered != 0 {
				return fail(r, "late reply %s to request %d, which was answered or late before", attr(r, "type"), n)
			}
			q.late = true
		}
	}
	return nil
}

// auditVersion applies step 7 of the audit to the input r of type typ.
func auditVersion(inc *auditIncarnation, r kernel.Record, typ string) error {
	shard := attr(r, "space") + "/" + attr(r, "shard")
	switch typ {
	case "Acquired":
		if m := grantVersion.FindStringSubmatch(r.Text); m != nil {
			v, err := strconv.ParseUint(m[1], 10, 64)
			if err != nil {
				return err
			}
			inc.delivered[shard] = max(inc.delivered[shard], v)
		}
	case "HolderChanged":
		v, err := strconv.ParseUint(attr(r, "version"), 10, 64)
		if err != nil {
			return fmt.Errorf("HolderChanged version %q: %w", attr(r, "version"), err)
		}
		d := inc.delivered[shard]
		if v < d || v == d && attr(r, "src") != "confirm" {
			return fmt.Errorf("HolderChanged of %s (src=%s) at version %d, the machine had version %d", shard, attr(r, "src"), v, d)
		}
		inc.delivered[shard] = v
	}
	return nil
}

// requestID returns the request ID at the start of text: of the request whose text an
// action's record has, or of the request a reply answers, whose text an input's record has. An
// input's text is that of a reply, a push or a snapshot, never a request's, and only a reply's
// starts with a name and #N.
func requestID(text string) (uint64, bool) {
	m := textID.FindStringSubmatch(text)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseUint(m[2], 10, 64)
	return n, err == nil
}

// doneOut returns out without the action whose request ID is n.
func doneOut(out []coalesced, n uint64) []coalesced {
	for i, o := range out {
		if o.id == n {
			return append(out[:i:i], out[i+1:]...)
		}
	}
	return out
}

// actionKey returns an action's type and fields, as "Release book/3@5".
func actionKey(r kernel.Record) string {
	parts := make([]string, 0, len(r.Attrs))
	for _, a := range r.Attrs {
		parts = append(parts, a.Key+"="+a.Value)
	}
	return strings.Join(parts, " ")
}

// attrNanos returns the attribute key of r, a duration in nanoseconds.
func attrNanos(r kernel.Record, key string) (time.Duration, error) {
	n, err := strconv.ParseInt(attr(r, key), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %s %q: %w", r.Kind, key, attr(r, key), err)
	}
	return time.Duration(n), nil
}
