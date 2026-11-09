// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// member returns the member id of the tests: with an address, a label and no slot.
func member(id string) core.Member {
	return core.Member{ID: id, Addr: "10.0.0.7:8080", Meta: map[string]string{"zone": "a"}, Slot: -1}
}

// leaveAtEnd calls Leave on a when the test ends, before the test's check that a's goroutines
// stopped.
func leaveAtEnd(t testing.TB, a *Adapter) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Leave(ctx); err != nil {
			t.Errorf("Leave: %v", err)
		}
	})
}

// mustJoin calls Join and fails the test on an error. The test leaves when it ends.
func mustJoin(t testing.TB, a *Adapter, m core.Member) {
	t.Helper()
	if err := a.Join(t.Context(), m); err != nil {
		t.Fatalf("Join %s: %v", m.ID, err)
	}
	leaveAtEnd(t, a)
}

// sentLease returns the Lease of a fake client's create or update action.
func sentLease(x k8stesting.Action) *coordinationv1.Lease {
	o, _ := x.(k8stesting.CreateAction)
	l, _ := o.GetObject().(*coordinationv1.Lease)
	return l
}

// fakeGet reads Lease name from the fake client; nil when there is none.
func fakeGet(t testing.TB, client *fake.Clientset, name string) *coordinationv1.Lease {
	t.Helper()
	l, err := client.CoordinationV1().Leases("default").Get(t.Context(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return l
}

// withoutAnnotation returns the annotations of l without key.
func withoutAnnotation(l *coordinationv1.Lease, key string) map[string]string {
	a := maps.Clone(l.Annotations)
	delete(a, key)
	return a
}

// sameButRecord reports whether after differs from before in no field but its member record
// and its resourceVersion.
func sameButRecord(before, after *coordinationv1.Lease) bool {
	return before.UID == after.UID && maps.Equal(before.Labels, after.Labels) &&
		maps.Equal(withoutAnnotation(before, annotationMember), withoutAnnotation(after, annotationMember)) &&
		equalSpec(before.Spec, after.Spec)
}

// equalSpec reports whether two Lease specs are equal.
func equalSpec(a, b coordinationv1.LeaseSpec) bool {
	x := &coordinationv1.Lease{Spec: a}
	y := &coordinationv1.Lease{Spec: b}
	return holderOf(x) == holderOf(y) && equalMicro(a.AcquireTime, b.AcquireTime) && equalMicro(a.RenewTime, b.RenewTime) &&
		equalInt32(a.LeaseDurationSeconds, b.LeaseDurationSeconds) && equalInt32(a.LeaseTransitions, b.LeaseTransitions)
}

func equalMicro(a, b *metav1.MicroTime) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(b)
}

func equalInt32(a, b *int32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// TestK8S600Join checks Join: the errors it returns without a request; the member Lease it
// creates (labels, member record, renewTime, the Adapter's duration, no holder) or the update
// of only the record of a Lease there is; the informer and heartbeat it starts; and a failed
// write that leaves the Adapter not joined, with no informer.
func TestK8S600Join(t *testing.T) {
	ctx := t.Context()
	name := memberName("g", "pod-0")

	t.Run("checks", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		done, cancel := context.WithCancel(ctx)
		cancel()
		if err := a.Join(done, member("pod-0")); !errors.Is(err, context.Canceled) {
			t.Errorf("Join with a done ctx = %v, want context.Canceled", err)
		}
		long := strings.Repeat("a", 254-len("g-member-"))
		year10000 := member("pod-0")
		year10000.JoinAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		yearNeg := member("pod-0")
		yearNeg.LeaveAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)
		badAddr := member("pod-0")
		badAddr.Addr = "\xff"
		badMeta := member("pod-0")
		badMeta.Meta = map[string]string{"k": "\xfe"}
		slash := member("m0/x")
		slot := member("pod-0")
		slot.Slot = -2
		tests := []struct {
			m    core.Member
			want string
		}{
			{core.Member{}, "k8s: Join: core: invalid member: empty ID"},
			{slash, `k8s: Join: core: invalid member: ID "m0/x" contains '/'`},
			{slot, `k8s: Join: core: invalid member "pod-0": Slot -2 is below -1`},
			{member("Pod-0"), `k8s: Join: core: invalid member "Pod-0": ID must be a DNS-1123 subdomain`},
			{member(long), fmt.Sprintf(`k8s: Join: core: invalid member %q: ID makes Lease name g-member-%s longer than 253 characters`, long, long)},
			{year10000, `k8s: Join: core: invalid member "pod-0": JoinAt year 10000 is outside [0, 9999]`},
			{yearNeg, `k8s: Join: core: invalid member "pod-0": LeaveAt year -1 is outside [0, 9999]`},
			{badAddr, `k8s: Join: core: invalid member "pod-0": Addr is not valid UTF-8`},
			{badMeta, `k8s: Join: core: invalid member "pod-0": Meta["k"] is not valid UTF-8`},
		}
		for _, tc := range tests {
			err := a.Join(ctx, tc.m)
			if !errors.Is(err, core.ErrInvalidMember) || err.Error() != tc.want {
				t.Errorf("Join(%q) = %v, want %s", tc.m.ID, err, tc.want)
			}
		}
		if got := client.Actions(); len(got) != 0 {
			t.Fatalf("requests %v, want none", got)
		}
		if a.runningCache() != nil {
			t.Fatal("an informer runs after failed Joins")
		}

		mustJoin(t, a, member("pod-0"))
		before := len(writes(client))
		for _, m := range []core.Member{member("pod-0"), member("pod-1")} {
			if err := a.Join(ctx, m); err == nil || err.Error() != "k8s: already joined" {
				t.Errorf("second Join(%s) = %v, want k8s: already joined", m.ID, err)
			}
		}
		if err := a.Join(ctx, core.Member{}); !errors.Is(err, core.ErrInvalidMember) {
			t.Errorf("Join of an invalid member after Join = %v, want core.ErrInvalidMember first", err)
		}
		if err := a.Leave(ctx); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		after := len(writes(client))
		for _, m := range []core.Member{member("pod-0"), {}} {
			if err := a.Join(ctx, m); !errors.Is(err, tenure.ErrClosed) {
				t.Errorf("Join(%q) after Leave = %v, want tenure.ErrClosed", m.ID, err)
			}
		}
		if got := len(writes(client)); got != after || after != before+1 {
			t.Errorf("writes: %d after Join, %d after Leave, %d at the end; want one write, by Leave", before, after, got)
		}
	})

	t.Run("create", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		m := member("pod-0")
		m.Draining = true
		m.JoinAt = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
		before := time.Now().Add(-time.Second)
		mustJoin(t, a, m)
		m.Meta["zone"] = "b" // the Adapter keeps its own copy

		creates := actions(client, "create")
		if len(creates) != 1 || len(writes(client)) != 1 {
			t.Fatalf("writes %v, want one create", writes(client))
		}
		l := sentLease(creates[0])
		want := `{"v":1,"id":"pod-0","addr":"10.0.0.7:8080","meta":{"zone":"a"},"draining":true,"joinAt":"2026-11-02T12:00:00Z"}`
		s := l.Spec
		switch {
		case l.Name != name || !maps.Equal(l.Labels, leaseLabels("g", kindMember, "")):
			t.Errorf("created %s with labels %v, want %s with %v", l.Name, l.Labels, name, leaseLabels("g", kindMember, ""))
		case len(l.Annotations) != 1 || l.Annotations[annotationMember] != want:
			t.Errorf("annotations %v, want only tenure.member %s", l.Annotations, want)
		case s.HolderIdentity != nil || s.AcquireTime != nil:
			t.Errorf("holder %v, acquireTime %v; want none", s.HolderIdentity, s.AcquireTime)
		case s.RenewTime == nil || s.RenewTime.Before(&metav1.MicroTime{Time: before}) || s.RenewTime.After(time.Now()):
			t.Errorf("renewTime %v, want the time of Join", s.RenewTime)
		case s.LeaseDurationSeconds == nil || *s.LeaseDurationSeconds != 3:
			t.Errorf("leaseDurationSeconds %v, want 3", s.LeaseDurationSeconds)
		case s.LeaseTransitions == nil || *s.LeaseTransitions != 0:
			t.Errorf("leaseTransitions %v, want 0", s.LeaseTransitions)
		}
		if c := a.runningCache(); c == nil || c.refs != 1 {
			t.Errorf("informer after Join: %+v, want one run with 1 hold", c)
		}
		// The heartbeat runs: within two RetryPeriods it moves renewTime on.
		deadline := time.Now().Add(2 * T.RetryPeriod)
		for len(actions(client, "update")) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := actions(client, "update"); len(got) == 0 || !sentLease(got[0]).Spec.RenewTime.After(s.RenewTime.Time) {
			t.Fatalf("updates %v in two RetryPeriods, want a heartbeat", got)
		}
		// SetDraining encodes the copy Join kept, not the caller's map.
		if err := a.SetDraining(ctx, false); err != nil {
			t.Fatalf("SetDraining: %v", err)
		}
		if l := fakeGet(t, client, name); !strings.Contains(l.Annotations[annotationMember], `"meta":{"zone":"a"}`) {
			t.Errorf("record after SetDraining %s, want meta zone a", l.Annotations[annotationMember])
		}
	})

	t.Run("update", func(t *testing.T) {
		client := newFake()
		old := encodeMember(core.Member{ID: "pod-0", Addr: "old"})
		l := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Labels:      map[string]string{labelGroup: "g", labelKind: kindMember, "example.com/keep": "1"},
				Annotations: map[string]string{"example.com/keep": "2", annotationMember: old},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       new("pod-0/7"),
				AcquireTime:          new(metav1.NewMicroTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))),
				RenewTime:            new(metav1.NewMicroTime(time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC))),
				LeaseDurationSeconds: new(int32(15)),
				LeaseTransitions:     new(int32(4)),
			},
		}
		l, err := client.CoordinationV1().Leases("default").Create(ctx, l, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		client.ClearActions()
		a := fakeAdapter(t, client)
		mustJoin(t, a, member("pod-0"))
		updates := actions(client, "update")
		if len(updates) != 1 || len(writes(client)) != 1 || len(actions(client, "get")) != 0 {
			t.Fatalf("requests %v, want one update and no GET", client.Actions())
		}
		sent := sentLease(updates[0])
		if sent.ResourceVersion != l.ResourceVersion || !sameButRecord(l, sent) ||
			sent.Annotations[annotationMember] != encodeMember(member("pod-0")) {
			t.Errorf("update sent %+v, want the Lease at resourceVersion %s with only its record changed", sent, l.ResourceVersion)
		}
		// The Lease names a holder: the heartbeat writes nothing.
		time.Sleep(3 * T.RetryPeriod)
		if got := writes(client); len(got) != 1 {
			t.Errorf("writes %v, want no heartbeat while the Lease names a holder", got)
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		client := newFake()
		foreign := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{labelGroup: "other", labelKind: kindMember}}}
		if _, err := client.CoordinationV1().Leases("default").Create(ctx, foreign, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create: %v", err)
		}
		client.ClearActions()
		a := fakeAdapter(t, client)
		const want = "k8s: lease g-member-pod-0 is not a member lease of group g"
		if err := a.Join(ctx, member("pod-0")); err == nil || err.Error() != want {
			t.Errorf("Join on a foreign Lease = %v, want %s", err, want)
		}
		if got := append(actions(client, "update"), actions(client, "delete")...); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after Join failed")
		}
		if err := a.SetDraining(ctx, true); err == nil || err.Error() != "k8s: not joined" {
			t.Errorf("SetDraining after a failed Join = %v, want k8s: not joined", err)
		}
	})

	t.Run("write error", func(t *testing.T) {
		client := newFake()
		internal := apierrors.NewInternalError(errors.New("injected"))
		var failing atomic.Bool
		failing.Store(true)
		client.PrependReactor("create", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			return failing.Load(), nil, internal
		})
		a := fakeAdapter(t, client)
		want := "k8s: join g-member-pod-0: " + internal.Error()
		if err := a.Join(ctx, member("pod-0")); err == nil || err.Error() != want {
			t.Errorf("Join with a failing create = %v, want %s", err, want)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after Join failed")
		}
		failing.Store(false)
		// Not joined: Join may be called again.
		mustJoin(t, a, member("pod-0"))
	})
}

// TestK8S640SetDraining checks SetDraining: its errors without a request, no request when the
// value is unchanged, and an update of only the member record otherwise, also re-creating a
// deleted Lease.
func TestK8S640SetDraining(t *testing.T) {
	ctx := t.Context()
	name := memberName("g", "pod-0")
	client := newFake()
	a := fakeAdapter(t, client)

	done, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.SetDraining(done, true); !errors.Is(err, context.Canceled) {
		t.Errorf("SetDraining with a done ctx = %v, want context.Canceled", err)
	}
	if err := a.SetDraining(ctx, true); err == nil || err.Error() != "k8s: not joined" {
		t.Errorf("SetDraining before Join = %v, want k8s: not joined", err)
	}
	if got := client.Actions(); len(got) != 0 {
		t.Fatalf("requests %v, want none", got)
	}

	m := member("pod-0")
	mustJoin(t, a, m)
	// The heartbeat writes nothing while a session holds the Lease, so only SetDraining writes.
	mustKeepAlive(t, a, "pod-0/1")
	closeAtEnd(t, a, "pod-0/1")
	client.ClearActions()
	if err := a.SetDraining(ctx, false); err != nil {
		t.Fatalf("SetDraining(false) when not draining: %v", err)
	}
	if got := writes(client); len(got) != 0 {
		t.Fatalf("writes %v, want none for an unchanged value", got)
	}
	for _, d := range []bool{true, false} {
		before := fakeGet(t, client, name)
		client.ClearActions()
		if err := a.SetDraining(ctx, d); err != nil {
			t.Fatalf("SetDraining(%v): %v", d, err)
		}
		got := writes(client)
		if len(got) != 1 || got[0].GetVerb() != "update" {
			t.Fatalf("SetDraining(%v): writes %v, want one update", d, got)
		}
		sent := sentLease(got[0])
		want := m
		want.Draining = d
		if sent.ResourceVersion != before.ResourceVersion || !sameButRecord(before, sent) ||
			sent.Annotations[annotationMember] != encodeMember(want) {
			t.Errorf("SetDraining(%v) sent %+v, want %s at resourceVersion %s with only its record changed",
				d, sent, encodeMember(want), before.ResourceVersion)
		}
		client.ClearActions()
		if err := a.SetDraining(ctx, d); err != nil || len(writes(client)) != 0 {
			t.Errorf("SetDraining(%v) again = %v with writes %v, want nil and none", d, err, writes(client))
		}
	}

	// A deleted Lease is created again, with the record and no holder.
	if err := client.CoordinationV1().Leases("default").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	waitForgotten(t, a, name)
	if err := a.SetDraining(ctx, true); err != nil {
		t.Fatalf("SetDraining on a deleted Lease: %v", err)
	}
	m.Draining = true
	l := fakeGet(t, client, name)
	if l == nil || l.Annotations[annotationMember] != encodeMember(m) || holderOf(l) != "" {
		t.Errorf("Lease after SetDraining on a deleted one: %+v, want the record %s and no holder", l, encodeMember(m))
	}

	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	client.ClearActions()
	if err := a.SetDraining(ctx, false); !errors.Is(err, tenure.ErrClosed) {
		t.Errorf("SetDraining after Leave = %v, want tenure.ErrClosed", err)
	}
	if got := client.Actions(); len(got) != 0 {
		t.Errorf("requests %v, want none", got)
	}
}

// logRecorder is a slog.Handler that keeps the records it handles.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *logRecorder) WithGroup(string) slog.Handler            { return h }

func (h *logRecorder) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

// warnings returns the messages of the Warn records, each with its err attribute.
func (h *logRecorder) warnings() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Level != slog.LevelWarn {
			continue
		}
		msg := r.Message
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "err" {
				msg += ": " + a.Value.String()
			}
			return true
		})
		out = append(out, msg)
	}
	return out
}

// TestK8S250HeartbeatWrites checks the heartbeat on a cluster: without a session, one update of
// renewTime alone every RetryPeriod (6 ± 1 in 3 s) and no other request; none while a session
// holds the Lease; after a conflict, a GET and the next update from what it read; a deleted
// Lease created again within 1 s; other errors logged at Warn.
func TestK8S250HeartbeatWrites(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	logs := &logRecorder{}
	a, rec := c.adapter(t, g, WithLogger(slog.New(logs)))
	name := memberName(g, "pod-0")
	mustJoin(t, a, member("pod-0"))
	ownUpdates := func(r request) bool { return on(verbUpdate, name)(r) && r.status == 200 }

	t.Run("without a session", func(t *testing.T) {
		before := rawGet(t, c, name)
		rec.reset()
		time.Sleep(3*time.Second + T.RetryPeriod/2)
		got := rec.filter(ownUpdates)
		if n := len(got); n < 5 || n > 7 {
			t.Errorf("%d heartbeat updates in 3s, want 6 ± 1: %v", n, requestStrings(rec.requests()))
		}
		if others := rec.filter(func(r request) bool { return named(r) && !ownUpdates(r) }); len(others) != 0 {
			t.Errorf("other requests %v, want none", requestStrings(others))
		}
		prev := before.Spec.RenewTime
		for i, r := range got {
			sent := r.lease().DeepCopy()
			rt := sent.Spec.RenewTime
			sent.Spec.RenewTime = before.Spec.RenewTime
			if !sameButRecord(before, sent) || sent.Annotations[annotationMember] != before.Annotations[annotationMember] {
				t.Errorf("update %d changed more than renewTime: %+v, before %+v", i, sent, before)
			}
			if rt == nil || !rt.After(prev.Time) {
				t.Errorf("update %d renewTime %v, want after %v", i, rt, prev)
			}
			prev = rt
		}
	})

	t.Run("with a session", func(t *testing.T) {
		r := startRenewer(t, a, "pod-0/1")
		rec.waitFor(t, time.Second, func(r request) bool {
			return ownUpdates(r) && r.lease().Spec.HolderIdentity != nil
		})
		rec.reset()
		time.Sleep(3 * time.Second)
		renewals, errs := r.stop()
		got := rec.filter(ownUpdates)
		for _, u := range got {
			if holderOf(u.lease()) != "pod-0/1" {
				t.Errorf("update without the holder while a session holds the Lease: %+v", u.lease().Spec)
			}
		}
		if len(errs) != 0 || len(got) > renewals+1 {
			t.Errorf("%d updates for %d renewals, errors %v; want the renewals' updates alone", len(got), renewals, errs)
		}
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		l := rawGet(t, c, name)
		if l == nil || holderOf(l) != "" || l.Annotations[annotationMember] == "" {
			t.Fatalf("Lease after CloseSession %+v, want the holder cleared and the record kept", l)
		}
		rec.reset()
		u := rec.waitFor(t, 2*T.RetryPeriod, ownUpdates)
		if u.lease().Spec.HolderIdentity != nil {
			t.Errorf("first update after CloseSession %+v, want a heartbeat", u.lease().Spec)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		rec.bump(name, time.Time{})
		rec.reset()
		rec.waitFor(t, 3*T.RetryPeriod, ownUpdates)
		got := rec.filter(named)
		want := []string{"UPDATE " + name + " 409", "GET " + name + " 200", "UPDATE " + name + " 200"}
		if s := requestStrings(got); !slices.Equal(s, want) {
			t.Fatalf("requests %v, want %v", s, want)
		}
		if rv := got[2].lease().ResourceVersion; rv != got[0].bumpedTo.ResourceVersion {
			t.Errorf("update after the conflict sent resourceVersion %s, want the bumped %s", rv, got[0].bumpedTo.ResourceVersion)
		}
		if gap := got[2].sent.Sub(got[1].done); gap < T.RetryPeriod/2 {
			t.Errorf("update %v after the GET, want it on the next beat", gap)
		}
	})

	t.Run("recreation", func(t *testing.T) {
		before := rawGet(t, c, name)
		rec.reset()
		deleted := time.Now()
		if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		created := rec.waitFor(t, 2*time.Second, func(r request) bool { return on(verbCreate, "")(r) && r.status == 201 })
		if d := created.done.Sub(deleted); d > time.Second {
			t.Errorf("Lease created again %v after its deletion, want within 1s", d)
		}
		l := rawGet(t, c, name)
		s := l.Spec
		if l.UID == before.UID || !maps.Equal(l.Labels, leaseLabels(g, kindMember, "")) ||
			l.Annotations[annotationMember] != before.Annotations[annotationMember] || s.HolderIdentity != nil ||
			s.RenewTime == nil || *s.LeaseDurationSeconds != 3 {
			t.Errorf("re-created Lease %+v, want labels, the record, renewTime, duration 3 and no holder", l)
		}
	})

	t.Run("recreation after a session", func(t *testing.T) {
		// The last state the Adapter wrote names a holder; the Lease is then deleted and the
		// session closed, which finds nothing to write. The informer saw the deletion, so the
		// heartbeat creates the Lease again.
		mustKeepAlive(t, a, "pod-0/2")
		if err := a.SetDraining(ctx, true); err != nil {
			t.Fatalf("SetDraining: %v", err)
		}
		deleted := time.Now()
		if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		waitForgotten(t, a, name)
		if err := a.CloseSession(ctx, "pod-0/2"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		rec.reset()
		created := rec.waitFor(t, 2*time.Second, func(r request) bool { return on(verbCreate, "")(r) && r.status == 201 })
		if d := created.done.Sub(deleted); d > time.Second {
			t.Errorf("Lease created again %v after its deletion, want within 1s", d)
		}
		l := rawGet(t, c, name)
		m := member("pod-0")
		m.Draining = true
		if l == nil || holderOf(l) != "" || l.Annotations[annotationMember] != encodeMember(m) {
			t.Errorf("re-created Lease %+v, want the draining record and no holder", l)
		}
		if err := a.SetDraining(ctx, false); err != nil {
			t.Fatalf("SetDraining: %v", err)
		}
	})

	t.Run("other errors", func(t *testing.T) {
		off := rec.fail(on(verbUpdate, name))
		time.Sleep(2*T.RetryPeriod + T.RetryPeriod/2)
		off()
		warned := logs.warnings()
		prefix := "k8s: heartbeat failed: k8s: heartbeat " + name + ": "
		if len(warned) < 1 || !strings.HasPrefix(warned[0], prefix) {
			t.Fatalf("warnings %v, want %s…", warned, prefix)
		}
		rec.reset()
		rec.waitFor(t, 2*T.RetryPeriod, ownUpdates)
	})
}

// TestK8S650Leave checks Leave: before Join it closes the Adapter without a request; it stops
// the heartbeat and deletes the member Lease with UID and resourceVersion preconditions, or
// removes only the member record while a session holds the Lease; later calls return nil; the
// informer stops; a done ctx changes nothing; a failed write still closes the Adapter.
func TestK8S650Leave(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()

	closed := func(t *testing.T, a *Adapter, rec *recorder) {
		t.Helper()
		rec.reset()
		if err := a.Join(ctx, member("pod-9")); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("Join after Leave = %v, want tenure.ErrClosed", err)
		}
		if err := a.SetDraining(ctx, true); !errors.Is(err, tenure.ErrClosed) {
			t.Errorf("SetDraining after Leave = %v, want tenure.ErrClosed", err)
		}
		if err := a.Leave(ctx); err != nil {
			t.Errorf("Leave again = %v, want nil", err)
		}
		if got := rec.requests(); len(got) != 0 {
			t.Errorf("requests %v, want none", requestStrings(got))
		}
		waitStopped(t, a)
	}

	t.Run("before Join", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		if err := a.Leave(ctx); err != nil {
			t.Fatalf("Leave before Join: %v", err)
		}
		if got := rec.requests(); len(got) != 0 {
			t.Errorf("requests %v, want none", requestStrings(got))
		}
		closed(t, a, rec)
	})

	t.Run("without a session", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		name := memberName(g, "pod-0")
		if err := a.Join(ctx, member("pod-0")); err != nil {
			t.Fatalf("Join: %v", err)
		}
		// Leave right after a heartbeat, so none is in flight.
		rec.waitFor(t, 2*T.RetryPeriod, func(r request) bool { return on(verbUpdate, name)(r) && r.status == 200 })
		rec.reset()
		if err := a.Leave(ctx); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		l := rec.filter(on(verbDelete, name))
		got := rec.filter(named)
		if len(got) != 1 || len(l) != 1 || got[0].status != 200 {
			t.Fatalf("requests %v, want one DELETE 200", requestStrings(got))
		}
		pre := l[0].deleteOptions().Preconditions
		if pre == nil || pre.UID == nil || pre.ResourceVersion == nil {
			t.Fatalf("delete preconditions %+v, want UID and resourceVersion", pre)
		}
		// The preconditions held: they were the Lease's last state.
		if rawGet(t, c, name) != nil {
			t.Error("the member Lease still exists")
		}
		time.Sleep(2 * T.RetryPeriod)
		if got := rec.filter(named); len(got) != 1 {
			t.Errorf("requests %v after Leave, want no heartbeat", requestStrings(got))
		}
		closed(t, a, rec)
	})

	t.Run("with a session", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		name := memberName(g, "pod-1")
		if err := a.Join(ctx, member("pod-1")); err != nil {
			t.Fatalf("Join: %v", err)
		}
		mustKeepAlive(t, a, "pod-1/1")
		before := rawGet(t, c, name)
		// The informer sees nothing until the session has renewed after Leave.
		release := rec.stall(on(verbWatch, ""))
		rec.reset()
		if err := a.Leave(ctx); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Fatalf("requests %v, want [UPDATE %s 200]", s, name)
		}
		l := rawGet(t, c, name)
		if _, ok := l.Annotations[annotationMember]; ok || !equalSpec(before.Spec, l.Spec) || !maps.Equal(before.Labels, l.Labels) {
			t.Errorf("Lease after Leave %+v, want only the record removed from %+v", l, before)
		}
		// The session goes on, renewing from the Lease Leave wrote; closing it now deletes the
		// Lease.
		rec.reset()
		mustKeepAlive(t, a, "pod-1/1")
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Errorf("renewal after Leave: requests %v, want [UPDATE %s 200]", s, name)
		}
		release()
		rec.reset()
		if err := a.CloseSession(ctx, "pod-1/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "DELETE "+name+" 200" {
			t.Errorf("CloseSession after Leave: requests %v, want [DELETE %s 200]", s, name)
		}
		closed(t, a, rec)
	})

	t.Run("done ctx", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		if err := a.Join(ctx, member("pod-2")); err != nil {
			t.Fatalf("Join: %v", err)
		}
		done, cancel := context.WithCancel(ctx)
		cancel()
		rec.reset()
		if err := a.Leave(done); !errors.Is(err, context.Canceled) {
			t.Errorf("Leave with a done ctx = %v, want context.Canceled", err)
		}
		if err := a.SetDraining(ctx, true); err != nil {
			t.Errorf("SetDraining after a Leave with a done ctx = %v, want nil: still joined", err)
		}
		if err := a.Leave(ctx); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		closed(t, a, rec)
	})

	t.Run("failed write", func(t *testing.T) {
		a, rec := c.adapter(t, g)
		name := memberName(g, "pod-3")
		if err := a.Join(ctx, member("pod-3")); err != nil {
			t.Fatalf("Join: %v", err)
		}
		off := rec.fail(on(verbDelete, name))
		err := a.Leave(ctx)
		off()
		if err == nil || !strings.HasPrefix(err.Error(), "k8s: leave "+name+": ") {
			t.Fatalf("Leave with a failing DELETE = %v, want k8s: leave %s: …", err, name)
		}
		closed(t, a, rec)
		if rawGet(t, c, name) == nil {
			t.Error("the member Lease is gone, want it left to expire")
		}
	})
}

// TestATK8S10SessionFields checks the session fields of a member Lease on a cluster: after Join
// and a first KeepAlive the Lease names the holder with leaseTransitions 1 and acquireTime equal
// to renewTime, and keeps the member record; each renewal is one update; without Join the
// session's Lease is created with leaseTransitions 0; invalid timings are rejected with no
// request. The acceptance test's steps were written from the issue's text: the original text
// is unavailable.
func TestATK8S10SessionFields(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)

	t.Run("after Join", func(t *testing.T) {
		name := memberName(g, "pod-0")
		mustJoin(t, a, member("pod-0"))
		record := rawGet(t, c, name).Annotations[annotationMember]
		rec.reset()
		mustKeepAlive(t, a, "pod-0/1")
		closeAtEnd(t, a, "pod-0/1")
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Fatalf("opening on the joined Lease: requests %v, want [UPDATE %s 200]", s, name)
		}
		l := rawGet(t, c, name)
		checkSessionFields(t, l, g, "pod-0/1", 1)
		if l.Annotations[annotationMember] != record {
			t.Errorf("member record %s, want it unchanged: %s", l.Annotations[annotationMember], record)
		}
		for i := range 3 {
			before := l
			rec.reset()
			mustKeepAlive(t, a, "pod-0/1")
			got := rec.filter(named)
			if s := requestStrings(got); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
				t.Fatalf("renewal %d: requests %v, want [UPDATE %s 200]", i, s, name)
			}
			if rv := got[0].lease().ResourceVersion; rv != before.ResourceVersion {
				t.Errorf("renewal %d sent resourceVersion %s, want %s", i, rv, before.ResourceVersion)
			}
			l = rawGet(t, c, name)
			if !before.Spec.RenewTime.Before(l.Spec.RenewTime) || !l.Spec.AcquireTime.Equal(before.Spec.AcquireTime) ||
				*l.Spec.LeaseTransitions != 1 || holderOf(l) != "pod-0/1" || l.Annotations[annotationMember] != record {
				t.Errorf("renewal %d: Lease %+v, want only renewTime moved on from %v", i, l, before.Spec.RenewTime)
			}
		}
	})

	t.Run("without Join", func(t *testing.T) {
		b, _ := c.adapter(t, g)
		mustKeepAlive(t, b, "pod-1/1")
		closeAtEnd(t, b, "pod-1/1")
		l := rawGet(t, c, memberName(g, "pod-1"))
		checkSessionFields(t, l, g, "pod-1/1", 0)
		if _, ok := l.Annotations[annotationMember]; ok {
			t.Errorf("annotations %v, want no member record without Join", l.Annotations)
		}
	})

	t.Run("invalid timings", func(t *testing.T) {
		bad := core.Timing{LeaseDuration: time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: time.Second, SlotGrace: time.Second}
		halfSecond := T
		halfSecond.LeaseDuration = 2500 * time.Millisecond
		rec.reset()
		for _, tc := range []struct {
			timing core.Timing
			want   string
		}{
			{bad, "k8s: KeepAlive: core: invalid timing: LeaseDuration 1s must be at least 21/19 times RenewDeadline 2s"},
			{halfSecond, "k8s: KeepAlive: LeaseDuration 2.5s is not a whole number of seconds"},
		} {
			for _, h := range []string{"pod-0/1", "pod-2/1"} {
				if err := a.KeepAlive(ctx, h, tc.timing); err == nil || err.Error() != tc.want {
					t.Errorf("KeepAlive(%s, %+v) = %v, want %s", h, tc.timing, err, tc.want)
				}
			}
		}
		if got := rec.filter(func(r request) bool { return named(r) || r.verb == verbCreate }); len(got) != 0 {
			t.Errorf("requests %v, want none", requestStrings(got))
		}
	})
}

// leaseHistory records every resourceVersion of Lease name, through the raw client, from its
// start until the test ends.
type leaseHistory struct {
	mu  sync.Mutex
	rvs []string
}

func watchHistory(t *testing.T, c *cluster, name string) *leaseHistory {
	t.Helper()
	w, err := c.leases().Watch(t.Context(), metav1.ListOptions{FieldSelector: "metadata.name=" + name})
	if err != nil {
		t.Fatalf("watch %s: %v", name, err)
	}
	h := &leaseHistory{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range w.ResultChan() {
			l, ok := e.Object.(*coordinationv1.Lease)
			if !ok || e.Type == watch.Bookmark {
				continue
			}
			h.mu.Lock()
			h.rvs = append(h.rvs, l.ResourceVersion)
			h.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		w.Stop()
		<-done
	})
	return h
}

// has reports whether rv was a resourceVersion of the Lease, waiting up to 2 s for the watch.
func (h *leaseHistory) has(rv string) bool {
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.mu.Lock()
		ok := slices.Contains(h.rvs, rv)
		h.mu.Unlock()
		if ok || time.Now().After(deadline) {
			return ok
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rawChange changes Lease name with f through the raw client, retrying on a conflict with the
// Adapter's writes.
func rawChange(t *testing.T, c *cluster, name string, f func(l *coordinationv1.Lease)) {
	t.Helper()
	for range 10 {
		l := rawGet(t, c, name)
		f(l)
		_, err := c.leases().Update(t.Context(), l, metav1.UpdateOptions{})
		if err == nil {
			return
		}
		if !apierrors.IsConflict(err) {
			t.Fatalf("update %s: %v", name, err)
		}
	}
	t.Fatalf("update %s: 10 conflicts", name)
}

// TestATK8S14CarriedOverFields checks that every write of a member Lease keeps the fields its
// operation does not name: a foreign label and annotation survive Join, the opening and the
// renewals of sessions, a renewal after a conflict, SetDraining, the heartbeat, CloseSession
// and Leave; and every update carries the resourceVersion of the state it was decided on. The
// acceptance test's steps were written from the issue's text: the original text is
// unavailable.
func TestATK8S14CarriedOverFields(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	name := memberName(g, "pod-0")
	history := watchHistory(t, c, name)
	a, rec := c.adapter(t, g)
	b, recB := c.adapter(t, g)

	mustJoin(t, a, member("pod-0"))
	rawChange(t, c, name, func(l *coordinationv1.Lease) {
		l.Labels["example.com/keep"] = "label"
		l.Annotations["example.com/keep"] = "annotation"
	})
	kept := func(step string) {
		t.Helper()
		l := rawGet(t, c, name)
		if l == nil || l.Labels["example.com/keep"] != "label" || l.Annotations["example.com/keep"] != "annotation" {
			t.Fatalf("after %s: Lease %+v, want the foreign label and annotation", step, l)
		}
	}
	// waitHeartbeat waits for the next update without a holder: a heartbeat.
	waitHeartbeat := func() {
		t.Helper()
		beat := func(r request) bool {
			return on(verbUpdate, name)(r) && r.status == 200 && r.lease().Spec.HolderIdentity == nil
		}
		n := rec.count(beat)
		deadline := time.Now().Add(2 * T.RetryPeriod)
		for rec.count(beat) == n {
			if time.Now().After(deadline) {
				t.Fatalf("no heartbeat in %v: %v", 2*T.RetryPeriod, requestStrings(rec.requests()))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	waitHeartbeat()
	kept("the heartbeat")
	mustKeepAlive(t, a, "pod-0/1")
	kept("opening a session")
	mustKeepAlive(t, a, "pod-0/1")
	kept("a renewal")
	rec.bump(name, time.Time{})
	mustKeepAlive(t, a, "pod-0/1")
	kept("a renewal after a conflict")
	if err := a.SetDraining(ctx, true); err != nil {
		t.Fatalf("SetDraining: %v", err)
	}
	kept("SetDraining")
	if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	kept("CloseSession")
	waitHeartbeat()
	kept("the heartbeat after CloseSession")
	mustKeepAlive(t, b, "pod-0/2")
	closeAtEnd(t, b, "pod-0/2")
	kept("opening a session in another Adapter")
	if err := a.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	kept("Leave")
	if l := rawGet(t, c, name); holderOf(l) != "pod-0/2" || l.Annotations[annotationMember] != "" {
		t.Errorf("after Leave: holder %q, annotations %v; want pod-0/2 and no record", holderOf(l), l.Annotations)
	}

	updates := append(rec.filter(on(verbUpdate, name)), recB.filter(on(verbUpdate, name))...)
	if len(updates) == 0 {
		t.Fatal("no update recorded")
	}
	for _, u := range updates {
		if rv := u.lease().ResourceVersion; rv == "" || !history.has(rv) {
			t.Errorf("%v sent resourceVersion %q, want one the Lease had", u, rv)
		}
	}
	if n := rec.count(func(r request) bool { return on(verbUpdate, name)(r) && r.bumped && r.status == 409 }); n != 1 {
		t.Errorf("%d bumped updates with 409, want the renewal's", n)
	}
}
