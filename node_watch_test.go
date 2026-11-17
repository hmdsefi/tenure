// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The tests of Node.Watch (RTR-730): nodes with slots on one memstore and one static list.

// watchNode starts member id with slots slots on ms and g, and the space orders.
func watchNode(t *testing.T, ms *memstore.Store, g *group, id string, slots int) *tenure.Node {
	t.Helper()
	runs := &runLog{}
	cfg := nodeConfig(id, g.Absent(id), ms, orders(runs.Run))
	cfg.Slots = slots
	return start(t, context.Background(), cfg)
}

// nextView receives from ch until it gets a view for which ok holds, failing t when ch closes
// or none comes within a minute. It returns that view.
func nextView(t *testing.T, ch <-chan core.View, want string, ok func(core.View) bool) core.View {
	t.Helper()
	timeout := time.After(time.Minute)
	for {
		select {
		case v, open := <-ch:
			if !open {
				t.Fatalf("Watch channel closed, want a view with %s", want)
			}
			if ok(v) {
				return v
			}
		case <-timeout:
			t.Fatalf("no view with %s within a minute", want)
		}
	}
}

// wantWatchClosed fails t unless ch is closed now, with no view left in it.
func wantWatchClosed(t *testing.T, ch <-chan core.View, why string) {
	t.Helper()
	select {
	case v, open := <-ch:
		if open {
			t.Fatalf("Watch channel holds view %d after %s, want it closed", v.Version, why)
		}
	default:
		t.Fatalf("Watch channel open after %s, want it closed", why)
	}
}

// TestNodeWatchFollows checks that Node.Watch holds the published view at once and follows
// joins, drains and slot changes, each view equal to the one the node publishes, with no
// goroutine left after Close.
func TestNodeWatchFollows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		ms := memstore.New(nil)
		g := newGroup(t)
		n1 := watchNode(t, ms, g, "n1", 2)
		ch, err := n1.Watch(context.Background())
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		select {
		case v := <-ch:
			if pub := published(t, n1); v.Version != pub.Version || !sameView(v, pub) {
				t.Fatalf("first view %d: %s, want the published view %d: %s", v.Version,
					describeView(v, true), pub.Version, describeView(pub, true))
			}
		default:
			t.Fatal("Watch channel empty after Start, want the published view at once")
		}
		nextView(t, ch, "n1 in slot 0", func(v core.View) bool {
			return len(v.Members) == 1 && v.Slots == 2 && v.Members[0].Slot == 0
		})

		n2 := watchNode(t, ms, g, "n2", 2)
		nextView(t, ch, "n1 and n2, each in a slot", func(v core.View) bool {
			return len(v.Members) == 2 && v.Members[0].Slot >= 0 && v.Members[1].Slot >= 0
		})
		if err := n2.Drain(context.Background()); err != nil {
			t.Fatalf("n2: Drain() = %v, want nil", err)
		}
		nextView(t, ch, "n2 draining", func(v core.View) bool {
			return len(v.Members) == 2 && v.Members[1].Draining
		})
		if err := n1.SetSlots(context.Background(), 3); err != nil {
			t.Fatalf("SetSlots(3) = %v, want nil", err)
		}
		v := nextView(t, ch, "3 slots", func(v core.View) bool { return v.Slots == 3 })
		if pub := published(t, n1); v.Version > pub.Version {
			t.Errorf("view %d is newer than the published view %d", v.Version, pub.Version)
		}
		closeNode(t, n2)
		v = nextView(t, ch, "n1 alone", func(v core.View) bool { return len(v.Members) == 1 })
		synctest.Wait()
		if len(ch) > 0 {
			v = <-ch
		}
		if pub := published(t, n1); v.Version != pub.Version || !sameView(v, pub) {
			t.Errorf("latest view %d: %s, want the published view %d: %s", v.Version,
				describeView(v, true), pub.Version, describeView(pub, true))
		}
		closeNode(t, n1)
		for range ch { // the views published during Close, then the close
		}
		check()
	})
}

// TestNodeWatchLatest checks that a reader that receives nothing while views are published
// finds the latest one only, and that a Watch started before any change gets each later one.
func TestNodeWatchLatest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		g := newGroup(t)
		n1 := startN1(t, g.Absent("n1"))
		ch, err := n1.Watch(context.Background())
		if err != nil {
			t.Fatalf("Watch() = %v, want nil", err)
		}
		for _, id := range []string{"n2", "n3", "n4", "n5"} {
			g.Join(core.Member{ID: id, Addr: addrOf(id)})
			synctest.Wait()
		}
		if got := len(ch); got != 1 {
			t.Fatalf("Watch channel holds %d views, want 1", got)
		}
		v := <-ch
		pub := published(t, n1)
		if v.Version != pub.Version || len(v.Members) != 5 {
			t.Errorf("slow reader got view %d with %d members, want the latest, %d with 5", v.Version,
				len(v.Members), pub.Version)
		}
		select {
		case v := <-ch:
			t.Errorf("Watch channel holds view %d after the latest, want nothing", v.Version)
		default:
		}
		closeNode(t, n1)
		for range ch { // the views published during Close, then the close
		}
		check()
	})
}

// TestNodeWatchClose checks that a Watch channel closes when its ctx ends, while another stays
// open, that every channel closes when the node closes, by a drained Close and by a Close whose
// ctx ended first, and that Watch then returns ErrClosed, or ctx's error for a done ctx.
func TestNodeWatchClose(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "drained"
		if force {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				ms := memstore.New(nil)
				g := newGroup(t)
				n1 := watchNode(t, ms, g, "n1", 0)
				ctx, cancel := context.WithCancel(context.Background())
				ended, err := n1.Watch(ctx)
				if err != nil {
					t.Fatalf("Watch(ctx) = %v, want nil", err)
				}
				open, err := n1.Watch(context.Background())
				if err != nil {
					t.Fatalf("Watch() = %v, want nil", err)
				}
				<-ended
				cancel()
				synctest.Wait()
				wantWatchClosed(t, ended, "its ctx ended")
				if _, err := n1.Watch(ctx); !errors.Is(err, context.Canceled) {
					t.Errorf("Watch(done ctx) = %v, want context.Canceled", err)
				}

				closeCtx := context.Background()
				if force {
					done, stop := context.WithCancel(context.Background())
					stop()
					closeCtx = done
				}
				err = n1.Close(closeCtx)
				if force != (err != nil) {
					t.Fatalf("Close() = %v, want an error %t", err, force)
				}
				for range open { // views published during Close, then the close
				}
				if ch, err := n1.Watch(context.Background()); ch != nil || !errors.Is(err, tenure.ErrClosed) {
					t.Errorf("Watch() after Close = %v, %v, want nil, ErrClosed", ch, err)
				}
				check()
			})
		})
	}
}

// TestNodeWatchDuringClose checks that Watch returns ErrClosed once Close has begun, before it
// returns.
func TestNodeWatchDuringClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		m := newGroup(t).Absent("n1")
		n1 := startN1(t, m)
		release := m.Block("Leave")
		done := make(chan error, 1)
		go func() { done <- n1.Close(context.Background()) }()
		synctest.Wait() // Close waits for Leave
		if ch, err := n1.Watch(context.Background()); ch != nil || !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Watch() during Close = %v, %v, want nil, ErrClosed", ch, err)
		}
		release()
		if err := <-done; err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		check()
	})
}
