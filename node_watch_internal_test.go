// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// TestViewWatchSend checks one Watch channel's sends: a newer view replaces one not yet
// received, and a view of the Version sent last or an older one is dropped, received or not, so
// that a Watch that loads the view publish has just stored does not get it twice.
func TestViewWatchSend(t *testing.T) {
	w := &viewWatch{ch: make(chan core.View, 1)}
	recv := func() (core.View, bool) {
		select {
		case v := <-w.ch:
			return v, true
		default:
			return core.View{}, false
		}
	}

	w.send(core.View{Version: 4})
	w.send(core.View{Version: 5, Slots: 1})
	w.send(core.View{Version: 5, Slots: 2}) // the same Version again
	w.send(core.View{Version: 3})           // an older one
	if v, ok := recv(); !ok || v.Version != 5 || v.Slots != 1 {
		t.Fatalf("received %d (Slots %d), %t, want view 5 with Slots 1", v.Version, v.Slots, ok)
	}
	if v, ok := recv(); ok {
		t.Fatalf("received view %d after the latest, want nothing", v.Version)
	}

	w.send(core.View{Version: 5, Slots: 3}) // received already
	if v, ok := recv(); ok {
		t.Fatalf("received view %d again, want nothing", v.Version)
	}
	w.send(core.View{Version: 6})
	if v, ok := recv(); !ok || v.Version != 6 {
		t.Fatalf("received %d, %t, want view 6", v.Version, ok)
	}
}

// TestNodeWatchCloseRace checks a Watch that Close overtakes: one that saw no Close begun yet but
// finds the channels of Watch closed returns ErrClosed, and a channel opened before they closed
// holds the published view and is closed.
func TestNodeWatchCloseRace(t *testing.T) {
	n := &Node{}
	n.view.Store(&core.View{Version: 7})
	ch, err := n.Watch(context.Background())
	if err != nil {
		t.Fatalf("Watch() = %v, want nil", err)
	}

	n.closeWatches() // Close's step, with n.closing read as false by the next Watch
	if got, err := n.Watch(context.Background()); got != nil || !errors.Is(err, ErrClosed) {
		t.Errorf("Watch() after the channels closed = %v, %v, want nil, ErrClosed", got, err)
	}
	if v, open := <-ch; !open || v.Version != 7 {
		t.Fatalf("first receive = view %d, open %t, want view 7", v.Version, open)
	}
	select {
	case v, open := <-ch:
		if open {
			t.Fatalf("Watch channel holds view %d after Close, want it closed", v.Version)
		}
	default:
		t.Fatal("Watch channel open after Close, want it closed")
	}
}
