// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// renewer is the Renewer fixture: it calls KeepAlive for one holder every RetryPeriod of T
// (500 ms), the first call at once, until it is stopped or a call returns an error matching
// tenure.ErrSessionLost.
type renewer struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	renewals int // calls that returned nil
	errs     []error
}

// startRenewer starts a renewer of holder on a. It is stopped when the test ends.
func startRenewer(t testing.TB, a *Adapter, holder string) *renewer {
	ctx, cancel := context.WithCancel(context.Background())
	r := &renewer{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		tick := time.NewTicker(T.RetryPeriod)
		defer tick.Stop()
		for {
			err := a.KeepAlive(ctx, holder, T)
			r.mu.Lock()
			switch {
			case err == nil:
				r.renewals++
			case ctx.Err() == nil:
				r.errs = append(r.errs, err)
			}
			r.mu.Unlock()
			if errors.Is(err, tenure.ErrSessionLost) {
				return
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() { r.stop() })
	return r
}

// stop stops the renewer and returns how many of its calls returned nil, and the errors of the
// others, but for those that the stop ended.
func (r *renewer) stop() (renewals int, errs []error) {
	r.cancel()
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renewals, r.errs
}

// TestK8S200KeepAliveChecks checks what KeepAlive and CloseSession return without a request:
// ctx.Err(), then the holder's and the timing's errors, the error of an ended holder and of an
// expired session, which ends it; and that CloseSession never returns ErrSessionLost. It also
// checks that a session expiring during its renewal is not renewed, that a failed renewal with
// a shorter duration brings the expiry forward, that a new incarnation ends the session of the
// one it replaces in the same Adapter, and that CloseSession ends the holder when its ctx ends
// while it waits for the member Lease.
func TestK8S200KeepAliveChecks(t *testing.T) {
	ctx := t.Context()
	bad := core.Timing{LeaseDuration: time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: time.Second, SlotGrace: time.Second}
	halfSecond := T
	halfSecond.LeaseDuration = 2500 * time.Millisecond

	t.Run("arguments", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		done, cancel := context.WithCancel(ctx)
		cancel()
		tests := []struct {
			name   string
			ctx    context.Context
			holder string
			timing core.Timing
			want   string
		}{
			{name: "ctx done", ctx: done, holder: "", timing: bad, want: "context canceled"},
			{name: "empty holder", holder: "", timing: bad, want: "k8s: KeepAlive: empty holder"},
			{name: "zero byte", holder: "pod-0/\x00", want: `k8s: KeepAlive: holder "pod-0/\x00": contains a zero byte`},
			{name: "no slash", holder: "pod-0", want: `k8s: KeepAlive: holder "pod-0": has no "/"`},
			{name: "empty incarnation", holder: "pod-0/", want: `k8s: KeepAlive: holder "pod-0/": has an empty incarnation`},
			{name: "two slashes", holder: "pod-0/1/2", want: `k8s: KeepAlive: holder "pod-0/1/2": has a "/" in its incarnation`},
			{name: "bad member", holder: "Pod-0/1", want: `k8s: KeepAlive: holder "Pod-0/1": member ID must be a DNS-1123 subdomain`},
			{
				name: "invalid timing", holder: "pod-0/1", timing: bad,
				want: "k8s: KeepAlive: core: invalid timing: LeaseDuration 1s must be at least 21/19 times RenewDeadline 2s",
			},
			{name: "timing of 2.5s", holder: "pod-0/1", timing: halfSecond, want: "k8s: KeepAlive: LeaseDuration 2.5s is not a whole number of seconds"},
		}
		for _, tc := range tests {
			c := tc.ctx
			if c == nil {
				c = ctx
			}
			if err := a.KeepAlive(c, tc.holder, tc.timing); err == nil || err.Error() != tc.want {
				t.Errorf("%s: KeepAlive = %v, want %s", tc.name, err, tc.want)
			}
		}
		if err := a.CloseSession(done, ""); !errors.Is(err, context.Canceled) {
			t.Errorf("CloseSession with a done ctx = %v, want context.Canceled", err)
		}
		const want = `k8s: CloseSession: holder "pod-0": has no "/"`
		if err := a.CloseSession(ctx, "pod-0"); err == nil || err.Error() != want {
			t.Errorf("CloseSession(pod-0) = %v, want %s", err, want)
		}
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	})

	t.Run("ended", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		// Closed before it ever had a session.
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		client.ClearActions()
		const want = "k8s: session pod-0/1 ended: tenure: session lost"
		for range 2 {
			if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
				t.Errorf("KeepAlive of a closed holder = %v, want %s", err, want)
			}
		}
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Errorf("CloseSession of an ended holder = %v, want nil", err)
		}
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
		// Another incarnation of the member is another holder.
		if err := a.KeepAlive(ctx, "pod-0/2", T); err != nil {
			t.Errorf("KeepAlive pod-0/2 = %v, want nil", err)
		}
		if err := a.CloseSession(ctx, "pod-0/2"); err != nil {
			t.Errorf("CloseSession pod-0/2: %v", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		clk := newFakeClock()
		client := newFake()
		a := fakeAdapter(t, client, withClock(clk.now))
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		clk.advance(T.LeaseDuration - time.Nanosecond)
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive just before the expiry = %v, want nil", err)
		}
		clk.advance(T.LeaseDuration)
		client.ClearActions()
		want := "k8s: session pod-0/1 expired: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive at the expiry = %v, want %s", err, want)
		}
		want = "k8s: session pod-0/1 ended: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive after the expiry = %v, want %s", err, want)
		}
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after the only session expired")
		}
		// CloseSession still clears the member Lease, which names the holder.
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if got := actions(client, "delete"); len(got) != 1 {
			t.Errorf("deletes %v, want 1", got)
		}
	})

	t.Run("expired during the renewal", func(t *testing.T) {
		// The session expires between KeepAlive's check and the second write of a conflict: no
		// write is sent once the session has expired.
		clk := newFakeClock()
		client := newFake()
		a := fakeAdapter(t, client, withClock(clk.now))
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		clk.advance(T.LeaseDuration - time.Millisecond)
		first := true
		client.PrependReactor("update", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			if !first {
				return false, nil, nil
			}
			first = false
			clk.advance(time.Millisecond)
			return true, nil, apierrors.NewConflict(leasesResource.GroupResource(), memberName("g", "pod-0"), errors.New("injected"))
		})
		client.ClearActions()
		want := "k8s: session pod-0/1 expired: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive expiring during its conflict = %v, want %s", err, want)
		}
		if got := writes(client); len(got) != 1 {
			t.Errorf("writes %v, want the first update alone", got)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after the only session expired")
		}
		// The Lease still names the holder: CloseSession deletes it.
		client.ClearActions()
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if got := actions(client, "delete"); len(got) != 1 {
			t.Errorf("deletes %v, want 1", got)
		}
	})

	t.Run("failed renewal with a shorter duration", func(t *testing.T) {
		// A failed renewal may have been written with its shorter duration, which the session's
		// expiry then follows.
		clk := newFakeClock()
		client := newFake()
		a := fakeAdapter(t, client, withClock(clk.now))
		closeAtEnd(t, a, "pod-0/1")
		if err := a.KeepAlive(ctx, "pod-0/1", core.DefaultTiming); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		failed := false
		client.PrependReactor("update", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			if failed {
				return false, nil, nil
			}
			failed = true
			return true, nil, apierrors.NewInternalError(errors.New("injected"))
		})
		if err := a.KeepAlive(ctx, "pod-0/1", T); err == nil || errors.Is(err, tenure.ErrSessionLost) {
			t.Fatalf("KeepAlive with a failing update = %v, want the update's error", err)
		}
		clk.advance(T.LeaseDuration)
		want := "k8s: session pod-0/1 expired: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive %v after the failed renewal = %v, want %s", T.LeaseDuration, err, want)
		}
	})

	t.Run("replaced in this adapter", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, "pod-0/1", "pod-0/2")
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive pod-0/1: %v", err)
		}
		if err := a.KeepAlive(ctx, "pod-0/2", T); err != nil {
			t.Fatalf("KeepAlive pod-0/2: %v", err)
		}
		if c := a.runningCache(); c == nil || c.refs != 1 {
			t.Errorf("cache %+v, want one with the new incarnation's hold alone", c)
		}
		client.ClearActions()
		const want = "k8s: session pod-0/1 ended: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive of the replaced incarnation = %v, want %s", err, want)
		}
		// Its CloseSession leaves the Lease, which names the new incarnation, alone.
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Errorf("CloseSession pod-0/1: %v", err)
		}
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	})

	t.Run("close while the lease is busy", func(t *testing.T) {
		// CloseSession whose ctx ends while another write of the member Lease is in flight ends
		// the holder all the same.
		client := newFake()
		a := fakeAdapter(t, client)
		closeAtEnd(t, a, "pod-0/1")
		if err := a.KeepAlive(ctx, "pod-0/1", T); err != nil {
			t.Fatalf("KeepAlive: %v", err)
		}
		unlock, err := a.lockLease(ctx, memberName("g", "pod-0"))
		if err != nil {
			t.Fatalf("lockLease: %v", err)
		}
		cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		err = a.CloseSession(cctx, "pod-0/1")
		unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("CloseSession = %v, want context.DeadlineExceeded", err)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after the only session was closed")
		}
		client.ClearActions()
		const want = "k8s: session pod-0/1 ended: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive after the CloseSession = %v, want %s", err, want)
		}
		if got := client.Actions(); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		client := newFake()
		a := fakeAdapter(t, client)
		name := memberName("g", "pod-0")
		foreign := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{labelGroup: "other", labelKind: kindMember}}}
		foreign, err := client.CoordinationV1().Leases("default").Create(ctx, foreign, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		client.ClearActions()
		want := "k8s: lease g-member-pod-0 is not a member lease of group g"
		if err := a.KeepAlive(ctx, "pod-0/1", T); err == nil || err.Error() != want {
			t.Errorf("KeepAlive on a foreign Lease = %v, want %s", err, want)
		}
		// The informer does not see the Lease, which lacks the group's label, so a create is
		// tried, and fails.
		if got := append(actions(client, "update"), actions(client, "delete")...); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		l, err := client.CoordinationV1().Leases("default").Get(ctx, name, metav1.GetOptions{})
		if err != nil || l.ResourceVersion != foreign.ResourceVersion {
			t.Errorf("the foreign Lease after KeepAlive: %v, %v; want it unchanged", l, err)
		}
		if a.runningCache() != nil {
			t.Error("the informer still runs after the session failed to open")
		}
	})
}

// TestK8S330Conflicts checks the compare-and-swap writes: an update carries its base's
// resourceVersion, a delete its UID and resourceVersion; Conflict and AlreadyExists lead to a
// GET and a new decision, NotFound of an update or delete to a decision on no Lease; at most 3
// writes are sent; decisions' errors and other errors end the write.
func TestK8S330Conflicts(t *testing.T) {
	ctx := t.Context()
	name := memberName("g", "pod-0")
	type setup struct {
		client *fake.Clientset
		a      *Adapter
		cur    *coordinationv1.Lease // the Lease stored
	}
	newSetup := func(t *testing.T) setup {
		client := newFake()
		l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: leaseLabels("g", kindMember, "")}}
		l, err := client.CoordinationV1().Leases("default").Create(ctx, l, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		client.ClearActions()
		return setup{client: client, a: New(client, "default", "g"), cur: l}
	}
	// annotate decides to set annotation n to the number of decisions, and records the states
	// decided on.
	type decisions struct {
		states []*coordinationv1.Lease
	}
	annotate := func(d *decisions) decider {
		return func(cur *coordinationv1.Lease) (change, error) {
			d.states = append(d.states, cur)
			if cur == nil {
				return change{lease: &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Labels: leaseLabels("g", kindMember, "")}}}, nil
			}
			l := cur.DeepCopy()
			if l.Annotations == nil {
				l.Annotations = map[string]string{}
			}
			l.Annotations["n"] = fmt.Sprint(len(d.states))
			return change{lease: l}, nil
		}
	}
	verbs := func(client *fake.Clientset) string {
		var out []string
		for _, x := range actions(client, "") {
			out = append(out, x.GetVerb())
		}
		return strings.Join(out, " ")
	}
	stale := func(s setup) *coordinationv1.Lease {
		old := s.cur.DeepCopy()
		l := s.cur.DeepCopy()
		l.Annotations = map[string]string{"other": "1"}
		if _, err := s.client.CoordinationV1().Leases("default").Update(ctx, l, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update: %v", err)
		}
		s.client.ClearActions()
		return old
	}

	t.Run("update", func(t *testing.T) {
		s := newSetup(t)
		var d decisions
		got, err := s.a.writeLease(ctx, "test", name, s.cur, annotate(&d))
		if err != nil || got == nil || got.Annotations["n"] != "1" {
			t.Fatalf("writeLease = %v, %v; want the Lease with n=1", got, err)
		}
		if v := verbs(s.client); v != "update" {
			t.Errorf("requests %q, want update", v)
		}
		update, _ := actions(s.client, "update")[0].(k8stesting.UpdateAction)
		sent, _ := update.GetObject().(*coordinationv1.Lease)
		if sent.ResourceVersion != s.cur.ResourceVersion || sent.Name != name {
			t.Errorf("update sent %s at resourceVersion %q, want %s at %q", sent.Name, sent.ResourceVersion, name, s.cur.ResourceVersion)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		s := newSetup(t)
		old := stale(s)
		var d decisions
		got, err := s.a.writeLease(ctx, "test", name, old, annotate(&d))
		if err != nil || got == nil || got.Annotations["n"] != "2" || got.Annotations["other"] != "1" {
			t.Fatalf("writeLease = %v, %v; want the Lease with n=2 and other=1", got, err)
		}
		if v := verbs(s.client); v != "update get update" {
			t.Errorf("requests %q, want update get update", v)
		}
		if len(d.states) != 2 || d.states[1].Annotations["other"] != "1" {
			t.Errorf("decided on %v, want the stale state and then the GET's", d.states)
		}
	})

	t.Run("three conflicts", func(t *testing.T) {
		s := newSetup(t)
		conflict := apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, name, errors.New("injected"))
		s.client.PrependReactor("update", resourceLeases, func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, conflict
		})
		var d decisions
		_, err := s.a.writeLease(ctx, "test", name, s.cur, annotate(&d))
		want := "k8s: lease g-member-pod-0: 3 conflicts: " + conflict.Error()
		if err == nil || err.Error() != want || !apierrors.IsConflict(errors.Unwrap(err)) {
			t.Errorf("writeLease = %v, want %s", err, want)
		}
		if v := verbs(s.client); v != "update get update get update" {
			t.Errorf("requests %q, want three updates with a GET between each", v)
		}
	})

	t.Run("already exists", func(t *testing.T) {
		s := newSetup(t)
		var d decisions
		got, err := s.a.writeLease(ctx, "test", name, nil, annotate(&d))
		if err != nil || got == nil || got.Annotations["n"] != "2" || got.UID != s.cur.UID {
			t.Fatalf("writeLease = %v, %v; want the existing Lease with n=2", got, err)
		}
		if v := verbs(s.client); v != "create get update" {
			t.Errorf("requests %q, want create get update", v)
		}
	})

	t.Run("update of a deleted lease", func(t *testing.T) {
		s := newSetup(t)
		if err := s.client.CoordinationV1().Leases("default").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		s.client.ClearActions()
		var d decisions
		got, err := s.a.writeLease(ctx, "test", name, s.cur, annotate(&d))
		if err != nil || got == nil || got.UID == s.cur.UID {
			t.Fatalf("writeLease = %v, %v; want a new Lease", got, err)
		}
		if v := verbs(s.client); v != "update create" || len(d.states) != 2 || d.states[1] != nil {
			t.Errorf("requests %q deciding on %v, want update create deciding on the stale state and then none", v, d.states)
		}
	})

	t.Run("delete", func(t *testing.T) {
		s := newSetup(t)
		old := stale(s)
		var states []*coordinationv1.Lease
		got, err := s.a.writeLease(ctx, "test", name, old, func(cur *coordinationv1.Lease) (change, error) {
			states = append(states, cur)
			return change{del: true}, nil
		})
		if err != nil || got != nil {
			t.Fatalf("writeLease = %v, %v; want nil, nil", got, err)
		}
		if v := verbs(s.client); v != "delete get delete" {
			t.Fatalf("requests %q, want delete get delete", v)
		}
		for i, x := range actions(s.client, "delete") {
			del, _ := x.(k8stesting.DeleteActionImpl)
			pre := del.DeleteOptions.Preconditions
			if pre == nil || pre.UID == nil || *pre.UID != states[i].UID || *pre.ResourceVersion != states[i].ResourceVersion {
				t.Errorf("delete %d preconditions %+v, want UID %s and resourceVersion %s", i, pre, states[i].UID, states[i].ResourceVersion)
			}
		}
		if _, err := s.client.CoordinationV1().Leases("default").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Errorf("GET after the delete = %v, want NotFound", err)
		}
	})

	t.Run("delete of a deleted lease", func(t *testing.T) {
		s := newSetup(t)
		if err := s.client.CoordinationV1().Leases("default").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		s.client.ClearActions()
		got, err := s.a.writeLease(ctx, "test", name, s.cur, func(cur *coordinationv1.Lease) (change, error) {
			return change{del: cur != nil}, nil
		})
		if err != nil || got != nil || verbs(s.client) != "delete" {
			t.Errorf("writeLease = %v, %v with requests %q; want nil, nil with one delete", got, err, verbs(s.client))
		}
	})

	t.Run("no change", func(t *testing.T) {
		s := newSetup(t)
		got, err := s.a.writeLease(ctx, "test", name, s.cur, func(*coordinationv1.Lease) (change, error) { return change{}, nil })
		if err != nil || got != nil || len(s.client.Actions()) != 0 {
			t.Errorf("writeLease = %v, %v with requests %v; want nil, nil, none", got, err, s.client.Actions())
		}
	})

	t.Run("decision error", func(t *testing.T) {
		s := newSetup(t)
		old := stale(s)
		want := errors.New("no")
		var d decisions
		_, err := s.a.writeLease(ctx, "test", name, old, func(cur *coordinationv1.Lease) (change, error) {
			if len(d.states) == 1 {
				return change{}, want
			}
			return annotate(&d)(cur)
		})
		if err != want {
			t.Errorf("writeLease = %v, want the decision's error", err)
		}
		if v := verbs(s.client); v != "update get" {
			t.Errorf("requests %q, want update get", v)
		}
	})

	t.Run("other errors", func(t *testing.T) {
		for _, verb := range []string{"update", "get"} {
			s := newSetup(t)
			old := stale(s)
			internal := apierrors.NewInternalError(errors.New("injected"))
			s.client.PrependReactor(verb, resourceLeases, func(action k8stesting.Action) (bool, runtime.Object, error) {
				if verb == "update" || action.GetVerb() == "get" {
					return true, nil, internal
				}
				return false, nil, nil
			})
			var d decisions
			_, err := s.a.writeLease(ctx, "test", name, old, annotate(&d))
			want := "k8s: test g-member-pod-0: " + internal.Error()
			if err == nil || err.Error() != want || !apierrors.IsInternalError(errors.Unwrap(err)) {
				t.Errorf("%s error: writeLease = %v, want %s", verb, err, want)
			}
			wantVerbs := map[string]string{"update": "update", "get": "update get"}[verb]
			if v := verbs(s.client); v != wantVerbs {
				t.Errorf("%s error: requests %q, want %q", verb, v, wantVerbs)
			}
		}
	})
}

// closeAtEnd ends the sessions of holders on a when the test ends, before the test's check that
// a's informer stopped.
func closeAtEnd(t testing.TB, a *Adapter, holders ...string) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, h := range holders {
			if err := a.CloseSession(ctx, h); err != nil {
				t.Errorf("CloseSession %s: %v", h, err)
			}
		}
	})
}

// rawGet reads Lease name through the raw client; nil when there is none.
func rawGet(t testing.TB, c *cluster, name string) *coordinationv1.Lease {
	t.Helper()
	l, err := c.leases().Get(t.Context(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("GET %s: %v", name, err)
	}
	return l
}

// rawUpdate changes Lease name with f through the raw client and returns the stored Lease.
func rawUpdate(t testing.TB, c *cluster, name string, f func(l *coordinationv1.Lease)) *coordinationv1.Lease {
	t.Helper()
	l := rawGet(t, c, name)
	if l == nil {
		t.Fatalf("no lease %s", name)
	}
	f(l)
	l, err := c.leases().Update(t.Context(), l, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update %s: %v", name, err)
	}
	return l
}

// mustKeepAlive calls KeepAlive and fails the test on an error.
func mustKeepAlive(t testing.TB, a *Adapter, holder string) {
	t.Helper()
	if err := a.KeepAlive(t.Context(), holder, T); err != nil {
		t.Fatalf("KeepAlive %s: %v", holder, err)
	}
}

// requestStrings returns the String of each request.
func requestStrings(rs []request) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.String()
	}
	return out
}

// named matches the recorded requests on a named object: no LIST, WATCH or CREATE.
func named(r request) bool { return r.name != "" }

// checkSessionFields checks the session fields of member Lease l: labels, holder,
// acquireTime == renewTime, leaseDurationSeconds 3 and leaseTransitions.
func checkSessionFields(t testing.TB, l *coordinationv1.Lease, g, holder string, transitions int32) {
	t.Helper()
	s := l.Spec
	switch {
	case l.Labels[labelGroup] != g || l.Labels[labelKind] != kindMember:
		t.Errorf("labels %v, want %s=%s and %s=member", l.Labels, labelGroup, g, labelKind)
	case holderOf(l) != holder:
		t.Errorf("holderIdentity %q, want %q", holderOf(l), holder)
	case s.AcquireTime == nil || s.RenewTime == nil || !s.AcquireTime.Equal(s.RenewTime):
		t.Errorf("acquireTime %v, renewTime %v; want equal", s.AcquireTime, s.RenewTime)
	case s.LeaseDurationSeconds == nil || *s.LeaseDurationSeconds != 3:
		t.Errorf("leaseDurationSeconds %v, want 3", s.LeaseDurationSeconds)
	case s.LeaseTransitions == nil || *s.LeaseTransitions != transitions:
		t.Errorf("leaseTransitions %v, want %d", s.LeaseTransitions, transitions)
	}
}

// TestK8S210OpenSession checks how a first KeepAlive opens a session on a cluster: it creates
// the member Lease without Join; takes over a Lease without holder, or one of another
// incarnation, which loses its session; renews a Lease that names the holder already; and
// never writes a foreign Lease. The session's Lease takes its duration from the KeepAlive's
// Timing, not from the Adapter's.
func TestK8S210OpenSession(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g, WithTiming(core.DefaultTiming))
	b, _ := c.adapter(t, g)
	d, recD := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1", "pod-1/1", "pod-3/1")
	closeAtEnd(t, b, "pod-0/2")
	closeAtEnd(t, d, "pod-0/2")
	name0 := memberName(g, "pod-0")

	t.Run("create without Join", func(t *testing.T) {
		before := time.Now().Add(-time.Second)
		mustKeepAlive(t, a, "pod-0/1")
		creates := rec.filter(on(verbCreate, ""))
		if len(creates) != 1 || creates[0].status != 201 || creates[0].lease().Name != name0 {
			t.Fatalf("creates %v, want one 201 of %s", creates, name0)
		}
		if got := rec.filter(named); len(got) != 0 {
			t.Errorf("requests on named Leases %v, want none", got)
		}
		l := rawGet(t, c, name0)
		checkSessionFields(t, l, g, "pod-0/1", 0)
		if _, ok := l.Annotations[annotationMember]; ok || l.Spec.RenewTime.Before(&metav1.MicroTime{Time: before}) {
			t.Errorf("annotations %v, renewTime %v; want no member record and a current time", l.Annotations, l.Spec.RenewTime)
		}
	})

	t.Run("lease without holder", func(t *testing.T) {
		name := memberName(g, "pod-1")
		l := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Labels:      map[string]string{labelGroup: g, labelKind: kindMember, "example.com/keep": "1"},
				Annotations: map[string]string{"example.com/keep": "1"},
			},
			Spec: coordinationv1.LeaseSpec{LeaseTransitions: new(int32(2))},
		}
		l, err := c.leases().Create(ctx, l, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		waitObserved(t, a, name, l.ResourceVersion)
		rec.reset()
		mustKeepAlive(t, a, "pod-1/1")
		got := rec.filter(named)
		if s := requestStrings(got); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Fatalf("requests %v, want [UPDATE %s 200]", s, name)
		}
		if rv := got[0].lease().ResourceVersion; rv != l.ResourceVersion {
			t.Errorf("update sent resourceVersion %s, want %s", rv, l.ResourceVersion)
		}
		l = rawGet(t, c, name)
		checkSessionFields(t, l, g, "pod-1/1", 3)
		if l.Labels["example.com/keep"] != "1" || l.Annotations["example.com/keep"] != "1" {
			t.Errorf("labels %v, annotations %v; want the foreign ones kept", l.Labels, l.Annotations)
		}
	})

	t.Run("another incarnation", func(t *testing.T) {
		before := rawGet(t, c, name0)
		mustKeepAlive(t, b, "pod-0/2")
		l := rawGet(t, c, name0)
		checkSessionFields(t, l, g, "pod-0/2", 1)
		if l.UID != before.UID {
			t.Errorf("UID %s, want the Lease's %s", l.UID, before.UID)
		}
		const want = "k8s: session pod-0/1 lost: replaced by pod-0/2: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-0/1", T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive of the replaced incarnation = %v, want %s", err, want)
		}
	})

	t.Run("lease naming the holder", func(t *testing.T) {
		before := rawGet(t, c, name0)
		mustKeepAlive(t, d, "pod-0/2")
		if s := requestStrings(recD.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name0+" 200" {
			t.Fatalf("requests %v, want [UPDATE %s 200]", s, name0)
		}
		l := rawGet(t, c, name0)
		s := l.Spec
		if holderOf(l) != "pod-0/2" || !s.AcquireTime.Equal(before.Spec.AcquireTime) || *s.LeaseTransitions != 1 ||
			!before.Spec.RenewTime.Before(s.RenewTime) {
			t.Errorf("after a renewal by another Adapter: holder %q, acquireTime %v, transitions %d, renewTime %v; "+
				"want pod-0/2, %v, 1 and after %v", holderOf(l), s.AcquireTime, *s.LeaseTransitions, s.RenewTime,
				before.Spec.AcquireTime, before.Spec.RenewTime)
		}
	})

	t.Run("foreign lease", func(t *testing.T) {
		name := memberName(g, "pod-3")
		l, err := c.leases().Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		rec.reset()
		want := fmt.Sprintf("k8s: lease %s is not a member lease of group %s", name, g)
		if err := a.KeepAlive(ctx, "pod-3/1", T); err == nil || err.Error() != want {
			t.Errorf("KeepAlive on a foreign Lease = %v, want %s", err, want)
		}
		if got := append(rec.filter(on(verbUpdate, "")), rec.filter(on(verbDelete, ""))...); len(got) != 0 {
			t.Errorf("writes %v, want none", got)
		}
		if after := rawGet(t, c, name); after.ResourceVersion != l.ResourceVersion {
			t.Errorf("the foreign Lease changed: %+v", after)
		}
	})
}

// TestK8S220RenewOutcomes checks renewals on a cluster: one update from the last own state,
// every RetryPeriod with the Renewer; the session lost when the Lease was deleted, names
// another holder or none, then ended with no request; and an error of the API server that
// keeps the session.
func TestK8S220RenewOutcomes(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1", "pod-4/1", "pod-5/1", "pod-6/1")

	t.Run("renewal", func(t *testing.T) {
		name := memberName(g, "pod-0")
		mustKeepAlive(t, a, "pod-0/1")
		before := rawGet(t, c, name)
		rec.reset()
		mustKeepAlive(t, a, "pod-0/1")
		got := rec.filter(named)
		if s := requestStrings(got); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Fatalf("requests %v, want [UPDATE %s 200]", s, name)
		}
		if rv := got[0].lease().ResourceVersion; rv != before.ResourceVersion {
			t.Errorf("update sent resourceVersion %s, want %s", rv, before.ResourceVersion)
		}
		l := rawGet(t, c, name)
		if !before.Spec.RenewTime.Before(l.Spec.RenewTime) || !l.Spec.AcquireTime.Equal(before.Spec.AcquireTime) ||
			*l.Spec.LeaseTransitions != 0 || holderOf(l) != "pod-0/1" {
			t.Errorf("renewed Lease %+v, want only renewTime moved on from %v", l.Spec, before.Spec.RenewTime)
		}

		// The Renewer: one update per renewal, no GET.
		rec.reset()
		r := startRenewer(t, a, "pod-0/1")
		time.Sleep(2 * time.Second)
		renewals, errs := r.stop()
		updates := rec.count(func(r request) bool { return on(verbUpdate, name)(r) && r.status == 200 })
		if len(errs) != 0 || renewals < 4 || renewals > 6 || updates != renewals || rec.count(on(verbGet, "")) != 0 {
			t.Errorf("Renewer for 2s: %d renewals, errors %v, %d updates, %d GETs; want 5±1 renewals, no error, an update each, no GET",
				renewals, errs, updates, rec.count(on(verbGet, "")))
		}

		// A new Timing's duration is written.
		t4 := T
		t4.LeaseDuration, t4.SlotGrace = 4*time.Second, 4*time.Second
		if err := a.KeepAlive(ctx, "pod-0/1", t4); err != nil {
			t.Fatalf("KeepAlive with a 4s LeaseDuration: %v", err)
		}
		if l := rawGet(t, c, name); *l.Spec.LeaseDurationSeconds != 4 {
			t.Errorf("leaseDurationSeconds %d, want 4", *l.Spec.LeaseDurationSeconds)
		}
		mustKeepAlive(t, a, "pod-0/1")
	})

	t.Run("renewal from the latest state", func(t *testing.T) {
		// Another writer changed the Lease and the informer saw it: the renewal starts from that
		// state, in one update that keeps the other writer's change.
		name := memberName(g, "pod-5")
		mustKeepAlive(t, a, "pod-5/1")
		l := rawUpdate(t, c, name, func(l *coordinationv1.Lease) {
			l.Annotations = map[string]string{"example.com/keep": "1"}
		})
		waitObserved(t, a, name, l.ResourceVersion)
		rec.reset()
		mustKeepAlive(t, a, "pod-5/1")
		got := rec.filter(named)
		if s := requestStrings(got); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Fatalf("requests %v, want [UPDATE %s 200]", s, name)
		}
		if rv := got[0].lease().ResourceVersion; rv != l.ResourceVersion {
			t.Errorf("update sent resourceVersion %s, want the other writer's %s", rv, l.ResourceVersion)
		}
		if l := rawGet(t, c, name); l.Annotations["example.com/keep"] != "1" {
			t.Errorf("annotations %v, want the other writer's kept", l.Annotations)
		}
	})

	t.Run("serialized writes", func(t *testing.T) {
		// Concurrent KeepAlives of one holder write its member Lease one at a time: each update
		// starts from the one before, and none conflicts.
		name := memberName(g, "pod-6")
		mustKeepAlive(t, a, "pod-6/1")
		rec.reset()
		off := rec.delay(name, 200*time.Millisecond)
		defer off()
		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i := range errs {
			wg.Go(func() { errs[i] = a.KeepAlive(ctx, "pod-6/1", T) })
		}
		wg.Wait()
		off()
		if err := errors.Join(errs...); err != nil {
			t.Fatalf("concurrent KeepAlives: %v", err)
		}
		updates := on(verbUpdate, name)
		inFlight := rec.maxInFlight(updates)
		renewed := rec.count(func(r request) bool { return updates(r) && r.status == 200 })
		if inFlight != 1 || renewed != 3 {
			t.Errorf("%d updates in flight at most, %d of them 200; want 1 and 3", inFlight, renewed)
		}
		if got := rec.filter(named); len(got) != 3 {
			t.Errorf("requests %v, want 3 updates", requestStrings(got))
		}
	})

	lost := []struct {
		name   string
		member string
		change func(name string)
		reason string
	}{
		{
			name: "lease deleted", member: "pod-1", reason: "lease deleted",
			change: func(name string) {
				if err := c.leases().Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
					t.Fatalf("delete: %v", err)
				}
			},
		},
		{
			name: "replaced", member: "pod-2", reason: "replaced by pod-2/2",
			change: func(name string) {
				rawUpdate(t, c, name, func(l *coordinationv1.Lease) { l.Spec.HolderIdentity = new("pod-2/2") })
			},
		},
		{
			name: "holder cleared", member: "pod-3", reason: "holder cleared",
			change: func(name string) {
				rawUpdate(t, c, name, func(l *coordinationv1.Lease) { l.Spec.HolderIdentity = nil })
			},
		},
	}
	for _, tc := range lost {
		t.Run(tc.name, func(t *testing.T) {
			h, name := tc.member+"/1", memberName(g, tc.member)
			mustKeepAlive(t, a, h)
			tc.change(name)
			want := fmt.Sprintf("k8s: session %s lost: %s: tenure: session lost", h, tc.reason)
			if err := a.KeepAlive(ctx, h, T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
				t.Fatalf("KeepAlive = %v, want %s", err, want)
			}
			rec.reset()
			want = fmt.Sprintf("k8s: session %s ended: tenure: session lost", h)
			if err := a.KeepAlive(ctx, h, T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
				t.Errorf("KeepAlive after the loss = %v, want %s", err, want)
			}
			if got := rec.filter(named); len(got) != 0 {
				t.Errorf("requests %v, want none", got)
			}
		})
	}

	t.Run("error keeps the session", func(t *testing.T) {
		name := memberName(g, "pod-4")
		mustKeepAlive(t, a, "pod-4/1")
		off := rec.fail(on(verbUpdate, name))
		err := a.KeepAlive(ctx, "pod-4/1", T)
		off()
		if err == nil || !strings.HasPrefix(err.Error(), "k8s: renew "+name+": ") || errors.Is(err, tenure.ErrSessionLost) {
			t.Fatalf("KeepAlive with a failing update = %v, want k8s: renew %s: …", err, name)
		}
		rec.reset()
		mustKeepAlive(t, a, "pod-4/1")
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Errorf("requests after the failure %v, want [UPDATE %s 200]", s, name)
		}
	})
}

// TestK8S240CloseSessionMember checks steps 2 and 3 of CloseSession on a cluster: the member
// Lease naming the holder is deleted with UID and resourceVersion preconditions, or has only its
// holder cleared when it carries a member record; a Lease naming another holder is left; the
// holder ends, also when unknown; and the informer stops within 1 s of the last session's end.
func TestK8S240CloseSessionMember(t *testing.T) {
	c := newCluster(t)
	g := group()
	ctx := t.Context()
	a, rec := c.adapter(t, g)
	b, recB := c.adapter(t, g)

	ended := func(t *testing.T, a *Adapter, rec *recorder, h string) {
		t.Helper()
		rec.reset()
		want := fmt.Sprintf("k8s: session %s ended: tenure: session lost", h)
		if err := a.KeepAlive(ctx, h, T); !errors.Is(err, tenure.ErrSessionLost) || err.Error() != want {
			t.Errorf("KeepAlive after CloseSession = %v, want %s", err, want)
		}
		if err := a.CloseSession(ctx, h); err != nil {
			t.Errorf("CloseSession again = %v, want nil", err)
		}
		if got := rec.filter(named); len(got) != 0 {
			t.Errorf("requests %v, want none", got)
		}
	}

	t.Run("delete", func(t *testing.T) {
		name := memberName(g, "pod-0")
		mustKeepAlive(t, a, "pod-0/1")
		l := rawGet(t, c, name)
		rec.reset()
		if err := a.CloseSession(ctx, "pod-0/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		got := rec.filter(named)
		if s := requestStrings(got); len(s) != 1 || s[0] != "DELETE "+name+" 200" {
			t.Fatalf("requests %v, want [DELETE %s 200]", s, name)
		}
		if o := got[0].deleteOptions(); o == nil || o.Preconditions == nil || o.Preconditions.UID == nil ||
			*o.Preconditions.UID != l.UID || o.Preconditions.ResourceVersion == nil || *o.Preconditions.ResourceVersion != l.ResourceVersion {
			t.Errorf("delete options %+v, want UID %s and resourceVersion %s", o, l.UID, l.ResourceVersion)
		}
		if rawGet(t, c, name) != nil {
			t.Error("the member Lease still exists")
		}
		ended(t, a, rec, "pod-0/1")
	})

	t.Run("member record", func(t *testing.T) {
		name := memberName(g, "pod-1")
		mustKeepAlive(t, a, "pod-1/1")
		record := encodeMember(core.Member{ID: "pod-1"})
		before := rawUpdate(t, c, name, func(l *coordinationv1.Lease) {
			l.Annotations = map[string]string{annotationMember: record}
			l.Labels["example.com/keep"] = "1"
		})
		waitObserved(t, a, name, before.ResourceVersion)
		rec.reset()
		if err := a.CloseSession(ctx, "pod-1/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if s := requestStrings(rec.filter(named)); len(s) != 1 || s[0] != "UPDATE "+name+" 200" {
			t.Errorf("requests %v, want [UPDATE %s 200]", s, name)
		}
		l := rawGet(t, c, name)
		if l == nil || l.UID != before.UID || holderOf(l) != "" || l.Annotations[annotationMember] != record ||
			l.Labels["example.com/keep"] != "1" || !l.Spec.RenewTime.Equal(before.Spec.RenewTime) {
			t.Fatalf("member Lease after CloseSession %+v, want the Lease with only its holder cleared", l)
		}
		ended(t, a, rec, "pod-1/1")
	})

	t.Run("unknown holder", func(t *testing.T) {
		name := memberName(g, "pod-2")
		mustKeepAlive(t, a, "pod-2/1")
		recB.reset()
		if err := b.CloseSession(ctx, "pod-2/1"); err != nil {
			t.Fatalf("CloseSession by another Adapter: %v", err)
		}
		if s := requestStrings(recB.filter(named)); len(s) != 2 || s[0] != "GET "+name+" 200" || s[1] != "DELETE "+name+" 200" {
			t.Errorf("requests %v, want [GET %s 200 DELETE %s 200]", s, name, name)
		}
		if rawGet(t, c, name) != nil {
			t.Error("the member Lease still exists")
		}
		ended(t, b, recB, "pod-2/1")
		const want = "k8s: session pod-2/1 lost: lease deleted: tenure: session lost"
		if err := a.KeepAlive(ctx, "pod-2/1", T); err == nil || err.Error() != want {
			t.Errorf("KeepAlive by the first Adapter = %v, want %s", err, want)
		}
	})

	t.Run("lease names another holder", func(t *testing.T) {
		name := memberName(g, "pod-3")
		mustKeepAlive(t, a, "pod-3/1")
		before := rawGet(t, c, name)
		recB.reset()
		if err := b.CloseSession(ctx, "pod-3/2"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		if s := requestStrings(recB.filter(named)); len(s) != 1 || s[0] != "GET "+name+" 200" {
			t.Errorf("requests %v, want [GET %s 200]", s, name)
		}
		if l := rawGet(t, c, name); l.ResourceVersion != before.ResourceVersion {
			t.Errorf("the Lease changed: %+v", l)
		}
		ended(t, b, recB, "pod-3/2")
	})

	t.Run("informer stops", func(t *testing.T) {
		if a.runningCache() == nil {
			t.Fatal("no informer runs with the session of pod-3/1 open")
		}
		if err := a.CloseSession(ctx, "pod-3/1"); err != nil {
			t.Fatalf("CloseSession: %v", err)
		}
		waitStopped(t, a)
	})
}

// TestATK8S12RenewalConflict checks a renewal that conflicts: the update gets 409, the Adapter
// reads the Lease and updates it again from what it read, with renewTime 1 µs past the value
// written in between. The acceptance test's steps were written from the issue's text: the
// original text is unavailable.
func TestATK8S12RenewalConflict(t *testing.T) {
	c := newCluster(t)
	g := group()
	a, rec := c.adapter(t, g)
	closeAtEnd(t, a, "pod-0/1")
	name := memberName(g, "pod-0")
	mustKeepAlive(t, a, "pod-0/1")

	raw := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	rec.bump(name, raw)
	rec.reset()
	mustKeepAlive(t, a, "pod-0/1")
	got := rec.filter(named)
	want := []string{"UPDATE " + name + " 409", "GET " + name + " 200", "UPDATE " + name + " 200"}
	if s := requestStrings(got); !slices.Equal(s, want) {
		t.Fatalf("requests %v, want %v", s, want)
	}
	bumped := got[0].bumpedTo
	if !got[0].bumped || bumped == nil || !bumped.Spec.RenewTime.Time.Equal(raw) {
		t.Fatalf("the first update was not bumped to renewTime %v: %+v", raw, bumped)
	}
	// The second update starts from what the GET read: the bumped Lease.
	wantTime := bumped.Spec.RenewTime.Add(time.Microsecond)
	second := got[2].lease()
	if rt := second.Spec.RenewTime; rt == nil || !rt.Time.Equal(wantTime) || second.ResourceVersion != bumped.ResourceVersion {
		t.Errorf("second update: renewTime %v at resourceVersion %s, want %v at %s", rt, second.ResourceVersion, wantTime, bumped.ResourceVersion)
	}
	l := rawGet(t, c, name)
	if !l.Spec.RenewTime.Time.Equal(wantTime) || l.Annotations[bumpAnnotation] != "1" || holderOf(l) != "pod-0/1" {
		t.Errorf("Lease after the renewal: renewTime %v, annotations %v, holder %q; want %v, the bump kept, pod-0/1",
			l.Spec.RenewTime, l.Annotations, holderOf(l), wantTime)
	}
}
