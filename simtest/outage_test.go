// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// outageAt is when the store is cut off in TestFencedStoreOutage's row (SIM-600).
const outageAt = 40 * time.Second

// outageLengths returns the four outage lengths of TestFencedStoreOutage under the default
// timing, shortest first: RD/2, which no session deadline outlasts; RD + 2·RP, past every
// deadline; LD + 2·RP, past every session at the store; and 4·LD.
func outageLengths() []time.Duration {
	t := core.DefaultTiming
	return []time.Duration{
		t.RenewDeadline / 2,
		t.RenewDeadline + 2*t.RetryPeriod,
		t.LeaseDuration + 2*t.RetryPeriod,
		4 * t.LeaseDuration,
	}
}

// outageScenario returns TestFencedStoreOutage's scenario with an outage of length d: five
// members share the unfenced space book and the fenced space ledger, 8 shards each, and the
// store is cut off from 40 s for d.
func outageScenario(d time.Duration) world.Scenario {
	return world.Scenario{
		Spaces: []core.SpaceSpec{{Name: "book", Shards: 8}, {Name: "ledger", Shards: 8, Fenced: true}},
		Outage: &world.Outage{At: outageAt, For: d},
	}
}

// outageRow returns TestFencedStoreOutage's row with an outage of length d.
func outageRow(t testing.TB, d time.Duration) row {
	t.Helper()
	r := rowNamed(t, "TestFencedStoreOutage")
	r.sc = outageScenario(d)
	return r
}

// TestFencedStoreOutage runs SIM-600's outage row (AT-SIM-31) under each rule at each of the
// four outage lengths: the store is cut off from every member at 40 s and the network heals
// after the outage. Every invariant holds after every event, and the finals pass: every shard
// has an owner B after the heal, the members converge, and tenure/outage holds the unfenced
// space book to stopping at the session deadline and the fenced space ledger to degrading and
// running on until the first sign of loss, then restoring or being lost after the heal (SIM-650).
func TestFencedStoreOutage(t *testing.T) {
	for _, rule := range rowNamed(t, "TestFencedStoreOutage").takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			for _, d := range outageLengths() {
				t.Run(d.String(), func(t *testing.T) {
					r := outageRow(t, d)
					faultline.Run(t, r.options(), r.body(rule, nil))
				})
			}
		})
	}
}

// TestOutageLengths checks the row at every length: the four lengths are those of SIM-600, and
// each scenario passes SetupError for the row's duration with the heal as its recovery time.
func TestOutageLengths(t *testing.T) {
	want := []time.Duration{5 * time.Second, 14 * time.Second, 19 * time.Second, 60 * time.Second}
	got := outageLengths()
	if len(got) != len(want) {
		t.Fatalf("outageLengths() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("outageLengths() = %v, want %v", got, want)
		}
	}
	for _, d := range got {
		r := outageRow(t, d)
		for _, rule := range r.takeovers() {
			sc := r.sc
			sc.Takeover = rule
			if err := world.SetupError(sc, kernel.Time(r.duration)); err != nil {
				t.Errorf("%v, %v: %v", d, rule, err)
			}
		}
	}
}

// TestATSIM36ShortOutage checks AT-SIM-36 under each rule: in TestFencedStoreOutage's world
// with an outage of RD/2, which ends before any session deadline, no machine degrades a shard
// at any time, and none stops a run from the isolation to the end of the run. Every record is
// kept. The steps were written for this task from the issue and the design note, since the
// original acceptance-test text is lost.
func TestATSIM36ShortOutage(t *testing.T) {
	r := outageRow(t, core.DefaultTiming.RenewDeadline/2)
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Seeds = 3
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, _ *world.World) {
				fw.Final("test/short_outage", func() error {
					return shortOutage(fw.Sim.Records(), kernel.Time(outageAt))
				})
			}))
		})
	}
}

// shortOutage returns an error for the first Degrade action of the trace, or the first StopRun
// action at or after at.
func shortOutage(recs []kernel.Record, at kernel.Time) error {
	for _, r := range recs {
		if r.Kind != kindAction {
			continue
		}
		switch typ := attr(r, "type"); {
		case typ == "Degrade":
			return fmt.Errorf("node %d at %v: Degrade %s", r.Node, r.At, shardTerm(r))
		case typ == "StopRun" && r.At >= at:
			return fmt.Errorf("node %d at %v: StopRun %s (reason %s) after the outage began", r.Node, r.At, shardTerm(r), attr(r, "reason"))
		}
	}
	return nil
}

// resyncLength is the outage of TestATSIM17Resync: RD + RP. The members renew in step, every
// RP from 0, so the renewals sent at 40 s are lost and each session's last renewal reaches the
// store just after 38 s: every session deadline passes at about 48 s, and the store would end
// every session at about 53 s. A heal at 52 s therefore comes after every deadline and before
// any session ends, so every member degrades its fenced shards and then resyncs on a live
// session. None of the row's four lengths does both: RD/2 heals before the deadlines, and RD +
// 2·RP and longer after the store ended every session, so no session renews after the heal.
func resyncLength() time.Duration {
	return core.DefaultTiming.RenewDeadline + core.DefaultTiming.RetryPeriod
}

// TestATSIM17Resync checks AT-SIM-17 under each rule, with every record kept, in
// TestFencedStoreOutage's world with an outage of RD + RP (resyncLength):
//
//  1. every incarnation whose session is renewed after the heal, after a failed renewal, then
//     sends holdings for each space, in name order (book, then ledger), and the store answers
//     at least one request per space; no holdings request goes out after the heal but in such
//     a resync;
//  2. every record stepped from a holdings answer (src=holdings) has a version above the
//     version delivered for its shard before it: that of the incarnation's latest
//     HolderChanged of the shard, or of the grant its Acquired came from;
//  3. every Degrade is of the fenced space ledger, and the degraded run then restores, with a
//     Restore whose step is the HolderChanged (src=confirm) of the answer of that shard's
//     latest Confirm action, naming the run's holder and term, or is lost: its StopRun comes,
//     or its incarnation's session is lost;
//  4. at least one member resyncs, and at least one shard restores, in each run.
//
// The steps were written for this task from the issue, SIM-340 and the design note, since the
// original acceptance-test text is lost.
func TestATSIM17Resync(t *testing.T) {
	r := outageRow(t, resyncLength())
	heal := kernel.Time(outageAt + resyncLength())
	for _, rule := range r.takeovers() {
		t.Run(rule.String(), func(t *testing.T) {
			opts := r.options()
			opts.Seeds = 3
			opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
			faultline.Run(t, opts, r.body(rule, func(fw *faultline.World, w *world.World) {
				fw.Final("test/resync", func() error {
					return resyncAudit(fw.Sim.Records(), w.Members(), w.Store().ID(), heal, []string{"book", "ledger"})
				})
			}))
		})
	}
}

// incarnation is what resyncAudit keeps of one incarnation of a member.
type incarnation struct {
	name      string
	holder    string
	failed    bool              // a SessionFailed since the last renewal
	resyncing bool              // its resync after the heal began
	spaces    []string          // the spaces of its holdings requests since then, repeats collapsed
	sent      []string          // the request IDs of those holdings requests, in order
	delivered map[string]uint64 // by shard: the version delivered
	confirm   map[string]string // by shard: the request ID of the latest Confirm action
	degraded  map[string]string // by shard: the term of a degraded run neither restored nor lost
	degrades  []string          // the shards in degraded, in the order they degraded
	lost      bool              // its session is lost
}

// unresolved returns the first shard the incarnation degraded and neither restored nor lost.
func (inc *incarnation) unresolved() (string, bool) {
	if inc.lost {
		return "", false
	}
	for _, key := range inc.degrades {
		if _, ok := inc.degraded[key]; ok {
			return key, true
		}
	}
	return "", false
}

// resyncAudit checks AT-SIM-17's clauses over a full trace of a run whose outage heals at
// heal; spaces are the space names in order.
func resyncAudit(recs []kernel.Record, members []*world.Member, store kernel.NodeID, heal kernel.Time, spaces []string) error {
	byNode := map[kernel.NodeID]*incarnation{}
	var all []*incarnation
	names := map[kernel.NodeID]string{}
	byName := map[string]kernel.NodeID{}
	for _, m := range members {
		names[m.Node.ID()] = m.Name
		byName[m.Name] = m.Node.ID()
	}
	answered := map[kernel.NodeID]map[string]bool{} // by member node: the holdings requests answered
	var stepIn kernel.Record                        // the latest tenure.input: the step of the actions after it
	restores := 0
	for _, r := range recs {
		if r.Kind == "net.send" && r.Node == store {
			if p := attr(r, "payload"); strings.HasPrefix(p, "holdings#") {
				id := byName[attr(r, "to")]
				if answered[id] == nil {
					answered[id] = map[string]bool{}
				}
				answered[id][requestID(p)] = true
			}
			continue
		}
		name, ok := names[r.Node]
		if !ok {
			continue
		}
		inc := byNode[r.Node]
		if r.Kind == kindBoot {
			inc = &incarnation{name: name, holder: attr(r, "holder"), delivered: map[string]uint64{},
				confirm: map[string]string{}, degraded: map[string]string{}}
			byNode[r.Node] = inc
			all = append(all, inc)
			continue
		}
		if inc == nil {
			continue
		}
		var err error
		switch r.Kind {
		case "net.send":
			err = inc.send(r, heal)
		case kindInput:
			stepIn = r
			err = inc.input(r, heal)
		case kindAction:
			var restored bool
			restored, err = inc.action(r, stepIn)
			if restored {
				restores++
			}
		}
		if err != nil {
			return err
		}
	}
	resyncs := 0
	for _, inc := range all {
		if key, ok := inc.unresolved(); ok {
			return fmt.Errorf("%s: %s degraded %s@%s and neither restored nor lost it", inc.name, inc.holder, key, inc.degraded[key])
		}
		if !inc.resyncing {
			continue
		}
		resyncs++
		if len(inc.spaces) < len(spaces) || !slices.Equal(inc.spaces[:len(spaces)], spaces) {
			return fmt.Errorf("%s: %s sent holdings for %v after the heal, want %v first", inc.name, inc.holder, inc.spaces, spaces)
		}
		got := 0
		for _, id := range inc.sent {
			if answered[byName[inc.name]][id] {
				got++
			}
		}
		if got < len(spaces) {
			return fmt.Errorf("%s: %s had %d of its %d holdings requests answered after the heal, want one per space",
				inc.name, inc.holder, got, len(inc.sent))
		}
	}
	if resyncs == 0 || restores == 0 {
		return fmt.Errorf("%d incarnations resynced and %d shards restored after the heal, want both above 0", resyncs, restores)
	}
	return nil
}

// send takes a message the incarnation's node sent: a holdings request must be its own and
// part of a resync that began after the heal.
func (inc *incarnation) send(r kernel.Record, heal kernel.Time) error {
	p := attr(r, "payload")
	if !strings.HasPrefix(p, "holdings#") {
		return nil
	}
	f := strings.Fields(p)
	if len(f) != 3 || f[1] != inc.holder {
		return fmt.Errorf("%s at %v: holdings request %q, want one of %s", inc.name, r.At, p, inc.holder)
	}
	if !inc.resyncing {
		if r.At > heal {
			return fmt.Errorf("%s at %v: holdings request %q with no resync after the heal", inc.name, r.At, p)
		}
		return nil
	}
	inc.sent = append(inc.sent, requestID(p))
	if n := len(inc.spaces); n == 0 || inc.spaces[n-1] != f[2] {
		inc.spaces = append(inc.spaces, f[2])
	}
	return nil
}

// input takes a tenure.input of the incarnation.
func (inc *incarnation) input(r kernel.Record, heal kernel.Time) error {
	key := attr(r, "space") + "/" + attr(r, "shard")
	switch attr(r, "type") {
	case "SessionFailed":
		inc.failed = true
		inc.lost = inc.lost || attr(r, "cause") == "lost"
	case "SessionRenewed":
		if inc.failed && r.At > heal {
			inc.resyncing = true
		}
		inc.failed = false
	case "Acquired":
		if v, ok := textVersion(r.Text); ok && attr(r, "term") != "0" {
			inc.delivered[key] = max(inc.delivered[key], v)
		}
	case "HolderChanged":
		v, err := strconv.ParseUint(attr(r, "record_version"), 10, 64)
		if err != nil {
			return fmt.Errorf("%s at %v: HolderChanged %s without a record_version", inc.name, r.At, key)
		}
		if attr(r, "src") == "holdings" && v <= inc.delivered[key] {
			return fmt.Errorf("%s at %v: holdings record %s of version %d stepped, %d delivered", inc.name, r.At, key, v, inc.delivered[key])
		}
		inc.delivered[key] = v
	}
	return nil
}

// action takes a tenure.action of the incarnation, of the step whose input is in, and reports
// whether it restored a degraded run.
func (inc *incarnation) action(r, in kernel.Record) (bool, error) {
	key := attr(r, "space") + "/" + attr(r, "shard")
	switch attr(r, "type") {
	case "Confirm":
		inc.confirm[key] = requestID(r.Text)
	case "Degrade":
		if attr(r, "space") != "ledger" {
			return false, fmt.Errorf("%s at %v: Degrade %s of an unfenced space", inc.name, r.At, shardTerm(r))
		}
		inc.degraded[key] = attr(r, "term")
		inc.degrades = append(inc.degrades, key)
	case "Restore":
		if in.Node != r.Node || attr(in, "type") != "HolderChanged" || attr(in, "src") != "confirm" ||
			attr(in, "holder") != inc.holder || attr(in, "term") != attr(r, "term") {
			return false, fmt.Errorf("%s at %v: Restore %s, not from a confirm answer naming %s at its term", inc.name, r.At, shardTerm(r), inc.holder)
		}
		if got, want := requestID(in.Text), inc.confirm[key]; got != want {
			return false, fmt.Errorf("%s at %v: Restore %s from the answer of confirm #%s, the latest is #%s", inc.name, r.At, shardTerm(r), got, want)
		}
		delete(inc.degraded, key)
		return true, nil
	case "StopRun":
		if inc.degraded[key] == attr(r, "term") {
			delete(inc.degraded, key)
		}
	}
	return false, nil
}

// requestID returns the request ID of a message text such as "holdings#12 m1/0a book" or
// "confirmed#8 ledger/3 …": the digits after the first "#".
func requestID(text string) string {
	_, rest, ok := strings.Cut(text, "#")
	if !ok {
		return ""
	}
	id, _, _ := strings.Cut(rest, " ")
	return id
}

// textVersion returns the record version in a granted answer's text, "… version=9".
func textVersion(text string) (uint64, bool) {
	_, rest, ok := strings.Cut(text, " version=")
	if !ok {
		return 0, false
	}
	v, _, _ := strings.Cut(rest, " ")
	n, err := strconv.ParseUint(v, 10, 64)
	return n, err == nil
}
