// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The tests of Node.Table on running nodes. They use OBS §10's harness (obs_harness_test.go).
// The steps of the tests carrying an AT-OBS number were written from the issue's text.

// rowOf returns the row of shard of space in tab, and fails t when tab has none.
func rowOf(t *testing.T, tab tenure.Table, space string, shard int) tenure.TableRow {
	t.Helper()
	for _, sp := range tab.Spaces {
		if sp.Name == space && shard >= 0 && shard < len(sp.Rows) {
			return sp.Rows[shard]
		}
	}
	t.Fatalf("Table() has no row %s/%d: %+v", space, shard, tab.Spaces)
	return tenure.TableRow{}
}

// tableSpace returns the space of 4 shards, keyed by their numbers, that AT-OBS-19's nodes serve.
func tableSpace(c *tenure.Config) {
	c.Spaces[0].Shards = 4
	c.Spaces[0].ShardOf = func(key string) int {
		s, _ := strconv.Atoi(key)
		return s
	}
}

// lastRenewal returns the start of the last KeepAlive call of n that succeeded, in UTC.
func lastRenewal(t *testing.T, n *obsNode) time.Time {
	t.Helper()
	calls := n.FS.Calls("KeepAlive")
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Done && calls[i].Err == nil {
			return calls[i].Start.UTC()
		}
	}
	t.Fatalf("%s: no KeepAlive succeeded", n.ID)
	return time.Time{}
}

// TestATOBS19Table checks the table of a node with a neighbour. a starts alone and acquires the 4
// shards of s, then b joins and takes two of them. 60 s after the start, a's Table shows: a's
// identity and holder; a live session whose deadline is RenewDeadline after the last accepted
// renewal; the view of both members; for each shard a.Route's member, and either an owned row of
// a's with a's holder, the run's term and Running, or an idle row with b's holder and term and Stop
// "moved". Its maps and slices are copies.
func TestATOBS19Table(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		h := newObsHarness(t)
		t0 := time.Now()
		a := h.Start("a", tableSpace, func(c *tenure.Config) { c.Self.Meta = map[string]string{"zone": "z1"} })
		time.Sleep(time.Second)
		b := h.Start("b", tableSpace)
		sleepUntil(t0.Add(60 * time.Second))
		synctest.Wait()

		tab := a.Table()
		if want := time.Now().UTC(); !tab.Taken.Equal(want) || tab.Taken.Location() != time.UTC {
			t.Errorf("Taken = %v, want %v in UTC", tab.Taken, want)
		}
		wantSelf := tenure.Member{ID: "a", Addr: "a:1", Meta: map[string]string{"zone": "z1"}, Slot: -1}
		if !reflect.DeepEqual(tab.Member, wantSelf) {
			t.Errorf("Member = %+v, want %+v", tab.Member, wantSelf)
		}
		if tab.Holder != obsHolderA || tab.Draining || tab.Closed || !tab.SessionLive {
			t.Errorf("Holder, Draining, Closed, SessionLive = %s, %t, %t, %t, want %s, false, false, true",
				tab.Holder, tab.Draining, tab.Closed, tab.SessionLive, obsHolderA)
		}
		if want := lastRenewal(t, a); !tab.LastRenewal.Equal(want) {
			t.Errorf("LastRenewal = %v, want the last KeepAlive's start %v", tab.LastRenewal, want)
		}
		if tab.SessionDeadline != tab.LastRenewal.Add(10*time.Second) {
			t.Errorf("SessionDeadline = %v, want LastRenewal + 10s = %v", tab.SessionDeadline, tab.LastRenewal.Add(10*time.Second))
		}
		view, _ := a.PublishedView()
		if tab.ViewVersion != view.Version || tab.ViewVersion == 0 {
			t.Errorf("ViewVersion = %d, want the published view's %d", tab.ViewVersion, view.Version)
		}
		wantMembers := []tenure.Member{wantSelf, {ID: "b", Addr: "b:1", Slot: -1}}
		if !reflect.DeepEqual(tab.Members, wantMembers) {
			t.Errorf("Members = %+v, want %+v", tab.Members, wantMembers)
		}
		if tab.Slots != 0 || tab.Slot != -1 || len(tab.SlotHolders) != 0 {
			t.Errorf("Slots, Slot, SlotHolders = %d, %d, %v, want 0, -1, none", tab.Slots, tab.Slot, tab.SlotHolders)
		}
		if len(tab.Spaces) != 1 || tab.Spaces[0].Name != obsSpace || tab.Spaces[0].Shards != 4 ||
			tab.Spaces[0].Fenced || len(tab.Spaces[0].Rows) != 4 {
			t.Fatalf("Spaces = %+v, want s with 4 unfenced shards", tab.Spaces)
		}

		owners := map[string]int{}
		for id, row := range tab.Spaces[0].Rows {
			route, err := a.Route(obsSpace, strconv.Itoa(id))
			if err != nil {
				t.Fatalf("Route(s, %d) = %v", id, err)
			}
			if row.Shard != id || row.Route != route.ID || row.Slot != -1 {
				t.Errorf("row %d: Shard, Route, Slot = %d, %q, %d, want %d, %q, -1", id, row.Shard, row.Route, row.Slot, id, route.ID)
			}
			owners[row.Route]++
			var want tenure.TableRow
			switch row.Route {
			case "a":
				runs := a.Runs.Of(obsSpace, id)
				if len(runs) != 1 {
					t.Fatalf("a's runs of s/%d = %v, want 1", id, runs)
				}
				want = tenure.TableRow{Shard: id, Route: "a", Slot: -1, State: "owned", Holder: obsHolderA,
					Term: runs[0].Shard.Term, Since: runs[0].Start.UTC(), Running: true}
			case "b":
				aRuns, bRuns := a.Runs.Of(obsSpace, id), b.Runs.Of(obsSpace, id)
				if len(aRuns) != 1 || !aRuns[0].Done || len(bRuns) != 1 {
					t.Fatalf("runs of s/%d: a %v, b %v, want a returned one and b one", id, aRuns, bRuns)
				}
				rel := onShard(a.FS.Calls("Release"), obsSpace, id)
				if len(rel) != 1 {
					t.Fatalf("a's Release calls of s/%d = %v, want 1", id, rel)
				}
				want = tenure.TableRow{Shard: id, Route: "b", Slot: -1, State: "idle", Holder: obsHolderB,
					Term: bRuns[0].Shard.Term, Since: rel[0].End.UTC(), Stop: "moved"}
				if want.Term <= aRuns[0].Shard.Term {
					t.Errorf("b's term %d of s/%d is not above a's %d", want.Term, id, aRuns[0].Shard.Term)
				}
			default:
				t.Fatalf("row %d routes to %q", id, row.Route)
			}
			if !reflect.DeepEqual(row, want) {
				t.Errorf("row %d = %+v, want %+v", id, row, want)
			}
		}
		if owners["a"] != 2 || owners["b"] != 2 {
			t.Errorf("rows by route %v, want 2 for a and 2 for b", owners)
		}

		// The table shares nothing with the node.
		tab.Member.Meta["zone"] = "x"
		tab.Members[0].Meta["zone"] = "x"
		tab.Members[1].ID = "x"
		tab.Spaces[0].Rows[0].State = "x"
		if again := a.Table(); again.Member.Meta["zone"] != "z1" || again.Members[0].Meta["zone"] != "z1" ||
			again.Members[1].ID != "b" || again.Spaces[0].Rows[0].State == "x" {
			t.Errorf("Table() after changing an earlier one = %+v", again)
		}
		if view, _ := a.PublishedView(); view.Members[0].Meta["zone"] != "z1" {
			t.Errorf("the published view's Meta = %v, want zone z1", view.Members[0].Meta)
		}

		b.MustClose()
		a.MustClose()
		check()
	})
}

// TestATOBS20TableAfterClose checks the table after Close. After a completed drain: Closed,
// Draining, not live, every row idle and not running, with Stop "draining" and no holder, at the
// term released. After a Close whose ctx was done already: the rows are idle, but a Run that has
// not returned is still Running until it returns, with Stop "closed".
func TestATOBS20TableAfterClose(t *testing.T) {
	t.Run("Drained", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Start("a")
			synctest.Wait()
			a.MustClose()
			tab := a.Table()
			if !tab.Closed || !tab.Draining || tab.SessionLive {
				t.Errorf("Closed, Draining, SessionLive = %t, %t, %t, want true, true, false", tab.Closed, tab.Draining, tab.SessionLive)
			}
			if tab.Holder != obsHolderA || tab.Member.ID != "a" {
				t.Errorf("Holder, Member.ID = %s, %s, want %s, a", tab.Holder, tab.Member.ID, obsHolderA)
			}
			for id := range 2 {
				runs := a.Runs.Of(obsSpace, id)
				if len(runs) != 1 {
					t.Fatalf("runs of s/%d = %v, want 1", id, runs)
				}
				row := rowOf(t, tab, obsSpace, id)
				if row.State != "idle" || row.Running || row.Stuck || row.Stop != "draining" ||
					row.Holder != "" || row.Term != runs[0].Shard.Term {
					t.Errorf("row %d = %+v, want idle, not running, Stop draining, no holder, term %d", id, row, runs[0].Shard.Term)
				}
			}
			if again := a.Table(); !reflect.DeepEqual(again.Spaces, tab.Spaces) || !again.Closed {
				t.Errorf("a second Table() after Close = %+v, want the same rows", again)
			}
			check()
		})
	})

	t.Run("Deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			a := newObsHarness(t).Node("a")
			hold := make(chan struct{})
			a.Runs.Script(func(ctx context.Context, _ *tenure.Shard, _ int) error {
				<-ctx.Done()
				<-hold
				return context.Cause(ctx)
			})
			if err := a.Start(context.Background()); err != nil {
				t.Fatalf("Start() = %v", err)
			}
			synctest.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := a.Close(ctx); err == nil {
				t.Fatal("Close(done ctx) = nil, want an error")
			}
			tab := a.Table()
			if !tab.Closed || !tab.Draining || tab.SessionLive {
				t.Errorf("Closed, Draining, SessionLive = %t, %t, %t, want true, true, false", tab.Closed, tab.Draining, tab.SessionLive)
			}
			for id := range 2 {
				row := rowOf(t, tab, obsSpace, id)
				if row.State != "idle" || !row.Running || row.Stop != "closed" || row.Holder != obsHolderA || row.Since != time.Now().UTC() {
					t.Errorf("row %d = %+v, want idle since now, running, Stop closed, holder %s", id, row, obsHolderA)
				}
			}

			close(hold)
			synctest.Wait()
			for id := range 2 {
				if row := rowOf(t, a.Table(), obsSpace, id); row.Running || row.Stuck || row.State != "idle" {
					t.Errorf("row %d once Run returned = %+v, want idle and not running", id, row)
				}
			}
			check()
		})
	})
}

// TestTableMembershipOnly checks the table of a node with neither store nor spaces: its identity,
// no session, the view, no slot and no spaces; after Close, Closed and Draining.
func TestTableMembershipOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		a := newObsHarness(t).Start("a", func(c *tenure.Config) {
			c.Store, c.Spaces = nil, nil
			c.Self.Meta = map[string]string{"role": "web"}
		})
		synctest.Wait()

		tab := a.Table()
		self := tenure.Member{ID: "a", Addr: "a:1", Meta: map[string]string{"role": "web"}, Slot: -1}
		view, _ := a.PublishedView()
		want := tenure.Table{
			Taken:       time.Now().UTC(),
			Member:      self,
			Holder:      obsHolderA,
			ViewVersion: view.Version,
			Members:     []tenure.Member{self},
			Slot:        -1,
			SlotHolders: view.SlotHolders,
			Spaces:      []tenure.TableSpace{},
		}
		if !reflect.DeepEqual(tab, want) || tab.ViewVersion == 0 {
			t.Errorf("Table() = %+v, want %+v", tab, want)
		}
		tab.Members[0].Meta["role"] = "x"
		if again := a.Table(); again.Members[0].Meta["role"] != "web" || again.Member.Meta["role"] != "web" {
			t.Errorf("Table() after changing an earlier one = %+v", again)
		}

		a.MustClose()
		tab = a.Table()
		if !tab.Closed || !tab.Draining || tab.SessionLive || tab.Holder != obsHolderA || len(tab.Spaces) != 0 {
			t.Errorf("Table() after Close = %+v, want Closed, Draining, not live, no spaces", tab)
		}
		check()
	})
}

// rowWant is what wantRows checks of one row: its State, Holder and Term.
type rowWant struct {
	space  string
	shard  int
	state  string
	holder string
	term   core.Term
}

// wantRows fails t unless n's Table holds each row as want says.
func wantRows(t *testing.T, n *tenure.Node, want ...rowWant) {
	t.Helper()
	tab := n.Table()
	for _, w := range want {
		r := rowOf(t, tab, w.space, w.shard)
		if r.State != w.state || r.Holder != w.holder || r.Term != w.term {
			t.Errorf("row %s/%d = %+v, want State %s, Holder %q, Term %d", w.space, w.shard, r, w.state, w.holder, w.term)
		}
	}
}
