// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
)

// The tests of what Config.OnEvent may do. They use OBS §10's harness (obs_harness_test.go). The
// steps of the tests carrying an AT-OBS number were written from the issue's text.

// obsPanicChild is the environment variable that makes TestATOBS24Callbacks run as its own child
// process, whose OnEvent panics.
const obsPanicChild = "TENURE_OBS24_CHILD"

// TestATOBS24Callbacks checks OnEvent's callbacks into the node. An OnEvent that calls Table and
// RecentEvents on every event, while a and b hand shards over and close, deadlocks nothing: every
// call returns, RecentEvents holds the event at hand, and node.closed sees a closed table. A
// child process whose OnEvent panics with "boom" exits non-zero with "panic: boom" on stderr,
// matched by substring, since a coverage build adds lines of its own.
func TestATOBS24Callbacks(t *testing.T) {
	if os.Getenv(obsPanicChild) == "1" {
		obsPanicNode(t)
		return
	}

	t.Run("TableFromOnEvent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			h := newObsHarness(t)
			var bad []string
			var mu sync.Mutex
			fail := func(msg string) { // t.Error from the delivery goroutine, kept for the end
				mu.Lock()
				defer mu.Unlock()
				bad = append(bad, msg)
			}
			var calls atomic.Int64
			callBack := func(n *obsNode) {
				var node atomic.Pointer[tenure.Node]
				release := n.Events.BlockFrom(1) // until the node is known, from the first event on
				n.Events.Hook(func(_ int, e tenure.Event) {
					calls.Add(1)
					tab := node.Load().Table()
					recent := node.Load().RecentEvents()
					if len(recent) == 0 || recent[len(recent)-1].Seq < e.Seq {
						fail(n.ID + ": RecentEvents in OnEvent does not hold the event at hand")
					}
					if tab.Holder != n.Holder {
						fail(n.ID + ": Table in OnEvent names another holder")
					}
					// The table is updated before the step's events are emitted, so it is never
					// behind the event at hand.
					switch e.Kind {
					case tenure.EventViewChanged:
						if tab.ViewVersion < e.Version {
							fail(n.ID + ": Table at view.changed has an older view")
						}
					case tenure.EventShardAcquired:
						if row := tab.Spaces[0].Rows[e.Shard]; row.Term < e.Term {
							fail(n.ID + ": Table at shard.acquired has an older term")
						}
					case tenure.EventNodeClosed:
						if !tab.Closed {
							fail(n.ID + ": Table at node.closed is not closed")
						}
					}
				})
				if err := n.Start(context.Background()); err != nil {
					t.Fatalf("Start(%s) = %v", n.ID, err)
				}
				node.Store(n.Node)
				release()
			}
			a, b := h.Node("a"), h.Node("b")
			callBack(a)
			time.Sleep(time.Second)
			callBack(b)
			time.Sleep(30 * time.Second)
			b.MustClose()
			a.MustClose()

			if got := calls.Load(); got != int64(len(a.Events.Calls())+len(b.Events.Calls())) || got == 0 {
				t.Errorf("%d OnEvent calls ran the callbacks, want all %d", got, len(a.Events.Calls())+len(b.Events.Calls()))
			}
			for _, n := range []*obsNode{a, b} {
				for _, c := range n.Events.Calls() {
					if !c.Done {
						t.Errorf("%s: OnEvent call with %v never returned", n.ID, c.Event.Kind)
					}
				}
				wantDelivery(t, n)
			}
			for _, s := range bad {
				t.Error(s)
			}
			check()
		})
	})

	t.Run("PanicCrashes", func(t *testing.T) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestATOBS24Callbacks$") //nolint:gosec // the test binary itself
		cmd.Env = append(os.Environ(), obsPanicChild+"=1")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err := cmd.Run()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() == 0 {
			t.Fatalf("child exited with %v, want a non-zero exit; stderr:\n%s", err, stderr.String())
		}
		if !strings.Contains(stderr.String(), "panic: boom") {
			t.Errorf("child's stderr does not contain %q:\n%s", "panic: boom", stderr.String())
		}
	})
}

// obsPanicNode is TestATOBS24Callbacks's child process: a node whose OnEvent panics with "boom",
// which must end the process.
func obsPanicNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newObsHarness(t).Node("a")
		a.Events.Hook(func(int, tenure.Event) { panic("boom") })
		if err := a.Start(context.Background()); err != nil {
			t.Fatalf("Start() = %v", err)
		}
		time.Sleep(time.Second)
		a.MustClose()
		t.Error("OnEvent's panic did not crash the process")
	})
}
