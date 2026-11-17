// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// The router rows (AT-RTR-13): two routers that each apply the store's views a delay drawn from
// [2·Delta, routerLag] after the store made them, over an unfenced and a fenced space, so that
// the requests the members serve in the fenced one are writes the fence judges.
var (
	routerSpaces = []core.SpaceSpec{{Name: "book", Shards: 8}, {Name: "ledger", Shards: 8, Fenced: true}}
	routerCut    = &world.RouterCut{At: 20 * time.Second, For: time.Minute}
)

const (
	routerCount = 2
	routerLag   = 3 * time.Second
	// vacuityRuns is the fewest runs of a rule over which runRouterRow judges whether the
	// checks were vacuous: -short's seed count. A replay of one seed is not judged.
	vacuityRuns = faultline.ShortSeeds
)

// routerScenario returns sc with the router rows' spaces and routers.
func routerScenario(sc world.Scenario) world.Scenario {
	sc.Spaces = routerSpaces
	sc.Routers = routerCount
	sc.RouterLag = routerLag
	return sc
}

// churnFaults returns the churn of the router rows: TestCrashRestart's crashes of the active
// members, which leave the view and join it again as they crash and come back.
func churnFaults() *fault.Random {
	return &fault.Random{MaxDown: 2, Rules: []fault.Rule{
		{Kind: fault.KindCrash, Target: "members", Every: 10 * time.Second, MinFor: time.Second, MaxFor: 20 * time.Second},
	}}
}

// routerTotals is the sum of the routers' counts over every run of a rule.
type routerTotals struct {
	runs                                       int
	requests, served, sends, resent            int
	bounded, converged, corrections, refused   int
	fallbacks, rejected, views, snapshotsTicks int
}

// add adds the counts of one run.
func (t *routerTotals) add(stats []world.RouterStats) {
	t.runs++
	for _, s := range stats {
		t.requests += s.Requests
		t.served += s.Served
		t.sends += s.Sends
		t.resent += s.Resent
		t.bounded += s.Bounded
		t.converged += s.Converged
		t.corrections += s.Corrections
		t.refused += s.Refused
		t.fallbacks += s.Fallbacks
		t.rejected += s.Rejected
		t.views += s.Views
		t.snapshotsTicks += s.Snapshots - s.Views
	}
}

// runRouterRow runs the router row name under each rule. Every invariant of the world holds
// after every event, the routers' among them (world/router.go):
//
//   - tenure/router/bound: a request sent while some member serves its shard, every member
//     that answered it naming a member that serves it, is served within three sends;
//   - tenure/router/converged: a request sent while the router's view equals every active
//     member's view, which shows no vacant slot, and the members route its shard to one member
//     that serves it, takes one send;
//   - tenure/router/owner: a member that serves a request owns its shard.
//
// The existing invariants hold too, g3 judging the requests the fenced space's owners serve
// as writes. Then, over all the seeds of each rule, at least vacuityRuns of them, the checks
// were not vacuous: the bound applied to some requests, some requests were sent in a converged
// state, some took more than one send, and the routers kept corrections.
func runRouterRow(t *testing.T, name string) {
	r := rowNamed(t, name)
	totals := map[world.Takeover]*routerTotals{}
	for _, rule := range r.takeovers() {
		totals[rule] = &routerTotals{}
	}
	runRow(t, r, func(fw *faultline.World, w *world.World) {
		sum := totals[w.Scenario().Takeover]
		fw.Final("test/router_stats", func() error {
			sum.add(w.RouterStats())
			return nil
		})
	})
	if t.Failed() {
		return
	}
	for _, rule := range r.takeovers() {
		s := totals[rule]
		t.Logf("%s/%v: %d runs, %d requests, %d served, %d sends, %d resent, %d bounded, %d converged, "+
			"%d corrections kept, %d refused, %d fallback sends, %d fenced writes rejected, %d views, %d ticks",
			name, rule, s.runs, s.requests, s.served, s.sends, s.resent, s.bounded, s.converged,
			s.corrections, s.refused, s.fallbacks, s.rejected, s.views, s.snapshotsTicks)
		switch {
		case s.runs == 0:
			t.Errorf("%v: no run reported the routers' counts", rule)
		case s.runs < vacuityRuns:
			t.Logf("%v: %d runs, too few to judge whether the checks were vacuous", rule, s.runs)
		case s.bounded == 0:
			t.Errorf("%v: the bound of three sends applied to no request", rule)
		case s.converged == 0:
			t.Errorf("%v: no request was sent in a converged state", rule)
		case s.resent == 0:
			t.Errorf("%v: no served request took more than one send", rule)
		case s.corrections == 0:
			t.Errorf("%v: no correction was kept", rule)
		}
	}
}

// TestATRTR13RouterSteady is AT-RTR-13 in TestSteady's world: five members with no fault, two
// routers whose views lag the store's by up to 3 s, under each rule. The steps were written for
// this task from the issue's text, since the original acceptance-test text is lost.
func TestATRTR13RouterSteady(t *testing.T) {
	runRouterRow(t, "TestATRTR13RouterSteady")
}

// TestATRTR13RouterRollout is AT-RTR-13 in TestSlotRollout's world: a rolling deploy with five
// slots replaces every member once while the routers lag, under each rule. The steps were
// written for this task from the issue's text.
func TestATRTR13RouterRollout(t *testing.T) {
	runRouterRow(t, "TestATRTR13RouterRollout")
}

// TestATRTR13RouterChurn is AT-RTR-13 with churn and no slots: TestCrashRestart's crashes make
// members leave the view and join it again, and shards move each time, while the routers lag,
// under each rule. The steps were written for this task from the issue's text.
func TestATRTR13RouterChurn(t *testing.T) {
	runRouterRow(t, "TestATRTR13RouterChurn")
}

// TestATRTR13RouterPartition is TestATRTR13RouterChurn with the routers cut off from the store's
// views from 20 s to 80 s, so their views stop while members crash and come back; at the end of
// the cut each router reads the store's latest view again. The steps were written for this
// task from the issue's text.
func TestATRTR13RouterPartition(t *testing.T) {
	runRouterRow(t, "TestATRTR13RouterPartition")
}
