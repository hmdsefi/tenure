// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestMergeView checks RUN-220's merge: holders cut or padded to the count, each member's Slot
// the lowest slot whose holder's ID part is the member's ID, else -1, and Version as given.
func TestMergeView(t *testing.T) {
	members := func(slots ...int) []core.Member {
		ms := []core.Member{{ID: "n1", Addr: "a1"}, {ID: "n2", Addr: "a2"}, {ID: "n3", Addr: "a3"}}
		for i := range ms {
			ms[i].Slot = slots[i]
		}
		return ms
	}
	tests := []struct {
		name    string
		in      core.View
		count   int
		holders []string
		want    core.View
	}{
		{
			name: "no slots",
			in:   core.View{Version: 7, Members: members(2, -1, 0)},
			want: core.View{Version: 7, Members: members(-1, -1, -1)},
		},
		{
			name:    "no slots, holders cut",
			in:      core.View{Version: 7, Members: members(-1, -1, -1)},
			holders: []string{"n1/a"},
			want:    core.View{Version: 7, Members: members(-1, -1, -1)},
		},
		{
			name:    "holders padded",
			in:      core.View{Version: 3, Members: members(-1, -1, -1)},
			count:   3,
			holders: []string{"n2/aa"},
			want:    core.View{Version: 3, Members: members(-1, 0, -1), Slots: 3, SlotHolders: []string{"n2/aa", "", ""}},
		},
		{
			name:    "holders cut",
			in:      core.View{Version: 3, Members: members(-1, -1, -1)},
			count:   2,
			holders: []string{"", "n3/x", "n1/y", "n2/z"},
			want:    core.View{Version: 3, Members: members(-1, -1, 1), Slots: 2, SlotHolders: []string{"", "n3/x"}},
		},
		{
			name:    "lowest slot wins",
			in:      core.View{Version: 9, Members: members(-1, -1, -1)},
			count:   4,
			holders: []string{"", "n1/b", "n2/c", "n1/a"},
			want:    core.View{Version: 9, Members: members(1, 2, -1), Slots: 4, SlotHolders: []string{"", "n1/b", "n2/c", "n1/a"}},
		},
		{
			name:    "holder without slash and unknown holder",
			in:      core.View{Version: 1, Members: members(0, 1, 2)},
			count:   3,
			holders: []string{"n3", "x/1", "n10/1"},
			want:    core.View{Version: 1, Members: members(-1, -1, 0), Slots: 3, SlotHolders: []string{"n3", "x/1", "n10/1"}},
		},
		{
			name:  "no members",
			in:    core.View{Version: 2},
			count: 1,
			want:  core.View{Version: 2, Slots: 1, SlotHolders: []string{""}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Members = append([]core.Member(nil), tt.in.Members...)
			holders := append([]string(nil), tt.holders...)
			got := mergeView(in, tt.count, holders)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeView(%+v, %d, %q) = %+v, want %+v", tt.in, tt.count, tt.holders, got, tt.want)
			}
			if !reflect.DeepEqual(in.Members, tt.in.Members) {
				t.Errorf("mergeView changed the input members to %+v", in.Members)
			}
			if len(got.Members) > 0 && &got.Members[0] == &in.Members[0] {
				t.Error("mergeView shares the members slice with its input")
			}
			if len(got.SlotHolders) > 0 && len(holders) > 0 && &got.SlotHolders[0] == &holders[0] {
				t.Error("mergeView shares the holders slice with its input")
			}
			if err := got.Validate(); err != nil {
				t.Errorf("mergeView(...).Validate() = %v, want nil", err)
			}
		})
	}
}

// TestMergerVersions checks the merged view's Version: the membership view's plus the slot
// changes posted so far, so that it grows with every post; and O8's floor: no post from slot data
// read before the floor, posted views from such reads stale, and a repost once a later read comes.
func TestMergerVersions(t *testing.T) {
	var posts []core.View
	m := merger{post: func(v core.View) { posts = append(posts, v) }}
	mem := func(version uint64) core.View {
		return core.View{Version: version, Members: []core.Member{{ID: "a", Slot: -1}, {ID: "b", Slot: -1}}}
	}
	wantPosts := func(step string, versions ...uint64) {
		t.Helper()
		var got []uint64
		for _, v := range posts {
			got = append(got, v.Version)
		}
		if !reflect.DeepEqual(got, versions) {
			t.Fatalf("%s: posted Versions %v, want %v", step, got, versions)
		}
	}

	if taken, first := m.membership(mem(5)); !taken || !first {
		t.Fatalf("membership(5) = %t, %t, want taken first", taken, first)
	}
	wantPosts("first view", 5)
	if v, stale := m.slots(0, 2, []string{"a/1"}); v != 6 || stale {
		t.Errorf("slots(new data) = %d, %t, want 6", v, stale)
	}
	wantPosts("new slot data", 5, 6)
	if got := posts[1]; got.Slots != 2 || !reflect.DeepEqual(got.SlotHolders, []string{"a/1", ""}) || got.Members[0].Slot != 0 {
		t.Errorf("merged view = %+v, want a on slot 0 of 2", got)
	}
	if v, stale := m.slots(1, 2, []string{"a/1", ""}); v != 6 || stale {
		t.Errorf("slots(same data) = %d, %t, want 6", v, stale)
	}
	wantPosts("same slot data", 5, 6)
	if taken, _ := m.membership(mem(5)); taken {
		t.Error("membership(5) taken twice")
	}
	m.membership(mem(7))
	wantPosts("new membership view", 5, 6, 8)

	m.raise(10)
	if !m.stale(8) || m.stale(9) {
		t.Errorf("stale(8), stale(9) = %t, %t, want true, false", m.stale(8), m.stale(9))
	}
	m.membership(mem(9))
	wantPosts("membership view over slot data older than the floor", 5, 6, 8)
	if _, stale := m.slots(9, 2, []string{"", "b/1"}); !stale {
		t.Error("slots(read before the floor) not stale")
	}
	wantPosts("read before the floor", 5, 6, 8)
	if v, stale := m.slots(11, 2, []string{"a/1", ""}); v != 11 || stale {
		t.Errorf("slots(read after the floor, same data) = %d, %t, want 11", v, stale)
	}
	wantPosts("read after the floor", 5, 6, 8, 11)
	if got := posts[3]; got.Members[0].Slot != 0 {
		t.Errorf("repost = %+v, want membership view 9 with a on slot 0", got)
	}
	m.raise(11) // the latest post came from a read at the floor: nothing turns stale
	if m.stale(11) {
		t.Error("stale(11) after a floor at its read")
	}
	if v, _ := m.slots(13, 3, nil); v != 12 {
		t.Errorf("slots(new count) = %d, want 12", v)
	}
	wantPosts("new count", 5, 6, 8, 11, 12)
}

// slotsDouble is a SlotStore whose Slots and SetSlots calls follow a script. Its other methods
// are not called.
type slotsDouble struct {
	SlotStore
	mu       sync.Mutex
	count    int
	err      error // of every Slots call
	setErr   error // of every SetSlots call
	failRead int   // when > 0, the read of that number fails with boom
	reads    []time.Time
}

var errSlotsBoom = errors.New("boom")

func (s *slotsDouble) Slots(context.Context) (int, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, time.Now())
	if len(s.reads) == s.failRead {
		return 0, nil, errSlotsBoom
	}
	return s.count, nil, s.err
}

func (s *slotsDouble) SetSlots(_ context.Context, count int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setErr != nil {
		return s.setErr
	}
	s.count = count
	return nil
}

// script makes later Slots calls return count and err.
func (s *slotsDouble) script(count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count, s.err = count, err
}

// readsSince returns the start of each Slots call, relative to t0.
func (s *slotsDouble) readsSince(t0 time.Time) []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []time.Duration
	for _, r := range s.reads {
		out = append(out, r.Sub(t0))
	}
	return out
}

// slotsObserver records the observer's slot calls.
type slotsObserver struct {
	nopObserver
	mu       sync.Mutex
	failed   []error
	mismatch [][2]int
}

func (o *slotsObserver) slotsFailed(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = append(o.failed, err)
}

func (o *slotsObserver) slotCountMismatch(configured, stored int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.mismatch = append(o.mismatch, [2]int{configured, stored})
}

// newSlotsNode returns a node with an executor, a recording observer and a merger that records
// its posts and holds membership view 1, for the refresher and Start's slot reads. The node's
// internal context ends at cleanup.
func newSlotsNode(t *testing.T, slots int) (*Node, *slotsObserver, func() []core.View) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	obs := &slotsObserver{}
	n := &Node{cfg: Config{Slots: slots, Timing: core.DefaultTiming}, obs: obs, ctx: ctx, cancel: cancel}
	st := &scriptStore{auto: func(*scriptCall) (scriptReply, bool) { return scriptReply{}, true }}
	n.exec = newExecutor(ctx, st, core.DefaultTiming, time.Now(), nil)
	var mu sync.Mutex
	var posts []core.View
	n.merge.post = func(v core.View) {
		mu.Lock()
		defer mu.Unlock()
		posts = append(posts, v)
	}
	n.merge.membership(core.View{Version: 1})
	t.Cleanup(func() {
		cancel()
		n.exec.wait()
		n.wg.Wait()
	})
	return n, obs, func() []core.View {
		mu.Lock()
		defer mu.Unlock()
		return append([]core.View(nil), posts...)
	}
}

// secsOf returns ss seconds each.
func secsOf(ss ...float64) []time.Duration {
	out := make([]time.Duration, len(ss))
	for i, s := range ss {
		out[i] = time.Duration(s * float64(time.Second))
	}
	return out
}

// TestRefresher checks the refresher's reads (RUN-210): every RetryPeriod after the previous
// read's start, at once for refresh and refreshWait; refreshWait returns the Version that holds
// the data read, or the read's error, which is reported while the data is kept.
func TestRefresher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, obs, posts := newSlotsNode(t, 0)
		st := &slotsDouble{count: 1}
		r := newRefresher(n, st)
		t0 := time.Now()
		n.wg.Go(func() { r.run(t0) })

		time.Sleep(3 * time.Second)
		if got, want := st.readsSince(t0), secsOf(2); !reflect.DeepEqual(got, want) {
			t.Errorf("reads at %v, want %v", got, want)
		}
		if version, err := r.refreshWait(context.Background()); version != 2 || err != nil {
			t.Errorf("refreshWait() = %d, %v, want 2, nil", version, err)
		}
		if got, want := st.readsSince(t0), secsOf(2, 3); !reflect.DeepEqual(got, want) {
			t.Errorf("reads at %v, want %v", got, want)
		}

		st.script(2, errSlotsBoom)
		if _, err := r.refreshWait(context.Background()); !errors.Is(err, errSlotsBoom) {
			t.Errorf("refreshWait() = %v, want boom", err)
		}
		st.script(3, nil)
		r.refresh()
		synctest.Wait()
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if got, want := st.readsSince(t0), secsOf(2, 3, 3, 3, 5, 7); !reflect.DeepEqual(got, want) {
			t.Errorf("reads at %v, want %v", got, want)
		}
		var counts []int
		for _, v := range posts() {
			counts = append(counts, v.Slots)
		}
		if want := []int{0, 1, 3}; !reflect.DeepEqual(counts, want) {
			t.Errorf("posted counts %v, want %v: the failed read changes nothing", counts, want)
		}
		obs.mu.Lock()
		if len(obs.failed) != 1 || !errors.Is(obs.failed[0], errSlotsBoom) {
			t.Errorf("slotsFailed calls %v, want boom once", obs.failed)
		}
		obs.mu.Unlock()

		// A negative count breaks the SlotStore contract: the read fails and changes nothing.
		st.script(-1, nil)
		if _, err := r.refreshWait(context.Background()); err == nil || err.Error() != "negative slot count -1" {
			t.Errorf("refreshWait() over a count of -1 = %v, want negative slot count -1", err)
		}
		if ps := posts(); len(ps) != 3 || ps[2].Slots != 3 {
			t.Errorf("%d posts, the latest with %d slots; want 3 posts, the latest with 3", len(ps), ps[len(ps)-1].Slots)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := r.refreshWait(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("refreshWait(canceled ctx) = %v, want nil or context.Canceled", err)
		}
		n.cancel()
		if _, err := r.refreshWait(context.Background()); !errors.Is(err, ErrClosed) {
			t.Errorf("refreshWait() after the node stopped = %v, want ErrClosed", err)
		}
	})
}

// TestReadSlots checks Start's slot reads (RUN-230): a stored count of 0 is seeded with a
// positive Config.Slots and read again; another positive count is kept and reported; a failure
// names its step.
func TestReadSlots(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		st         *slotsDouble // nil: a store with no slots
		wantCount  int
		wantReads  int
		wantErr    string
		mismatches [][2]int
	}{
		{name: "seeded", configured: 3, wantCount: 3, wantReads: 2},
		{name: "no slots", wantReads: 1},
		{name: "same count", configured: 3, st: &slotsDouble{count: 3}, wantCount: 3, wantReads: 1},
		{name: "other count", configured: 2, st: &slotsDouble{count: 3}, wantCount: 3, wantReads: 1,
			mismatches: [][2]int{{2, 3}}},
		{name: "count without Config.Slots", st: &slotsDouble{count: 3}, wantCount: 3, wantReads: 1},
		{name: "first read fails", configured: 3, st: &slotsDouble{failRead: 1}, wantErr: "tenure: read slots: boom"},
		{name: "seeding fails", configured: 3, st: &slotsDouble{setErr: errSlotsBoom}, wantErr: "tenure: seed slots: boom"},
		{name: "second read fails", configured: 3, st: &slotsDouble{failRead: 2}, wantErr: "tenure: seed slots: boom"},
		{name: "negative count", configured: 3, st: &slotsDouble{count: -1}, wantErr: "tenure: read slots: negative slot count -1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n, obs, posts := newSlotsNode(t, tt.configured)
				st := tt.st
				if st == nil {
					st = &slotsDouble{}
				}
				_, err := n.readSlots(context.Background(), st)
				if tt.wantErr != "" {
					boom := strings.HasSuffix(tt.wantErr, "boom")
					if err == nil || err.Error() != tt.wantErr || errors.Is(err, errSlotsBoom) != boom {
						t.Errorf("readSlots() = %v, want %q", err, tt.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("readSlots() = %v, want nil", err)
				}
				if got := len(st.readsSince(time.Now())); got != tt.wantReads {
					t.Errorf("%d Slots reads, want %d", got, tt.wantReads)
				}
				ps := posts()
				if last := ps[len(ps)-1]; last.Slots != tt.wantCount {
					t.Errorf("latest merged view has %d slots, want %d", last.Slots, tt.wantCount)
				}
				obs.mu.Lock()
				defer obs.mu.Unlock()
				if !reflect.DeepEqual(obs.mismatch, tt.mismatches) {
					t.Errorf("slotCountMismatch calls %v, want %v", obs.mismatch, tt.mismatches)
				}
			})
		})
	}
}
