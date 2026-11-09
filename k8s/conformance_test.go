// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/membershiptest"
	"github.com/hmdsefi/tenure/storetest"
)

// settle is the membershiptest fixture's Settle: a change reaches another Adapter's views through
// its informer's watch, within milliseconds on kind; a RetryPeriod of T leaves room for a loaded
// runner. Settle 0 fails Watch/Coalesce, whose changes land after the Settle it waits.
var settle = T.RetryPeriod

// newClientFunc returns a new recorded client of the test's cluster.
type newClientFunc func(t *testing.T) (kubernetes.Interface, *recorder)

// TestATK8S35Conformance runs the storetest and membershiptest conformance suites against
// Adapters on admin clients, in real time with timing T: each storetest case on a group of its
// own, with a peer Adapter on a second client, a Slack of 500 ms and DeleteRecord deleting the
// shard Lease; each membershiptest case on a group of its own, every instance an Adapter on a
// client of its own, as an Observer. The acceptance test's steps were written from the issue's
// text: the original text is unavailable.
func TestATK8S35Conformance(t *testing.T) {
	c := newCluster(t)
	t.Parallel()
	runConformance(t, c, func(t *testing.T) (kubernetes.Interface, *recorder) { return c.client(t) })
}

// TestATK8S34RBAC applies rbac.yaml in the test's namespace and runs the conformance suites as
// TestATK8S35Conformance does, every Adapter on a client that impersonates the service account
// tenure, which only the shipped Role allows anything: no request of theirs gets 403 Forbidden.
// It also checks the manifest's objects and that the service account may not read pods, so the
// clients are limited to the Role. The acceptance test's steps were written from the issue's
// text: the original text is unavailable.
func TestATK8S34RBAC(t *testing.T) {
	c := newCluster(t)
	t.Parallel()
	applyRBAC(t, c)
	user := "system:serviceaccount:" + c.ns + ":tenure"
	runConformance(t, c, func(t *testing.T) (kubernetes.Interface, *recorder) { return c.clientAs(t, user) })
}

// runConformance runs both suites, at the same time, on Adapters of namespace c.ns on clients
// from newClient. Each Adapter stops when its case ends, and no request of its client may have
// got 403 Forbidden.
func runConformance(t *testing.T, c *cluster, newClient newClientFunc) {
	t.Run("storetest", func(t *testing.T) {
		t.Parallel()
		storetest.Run(t, func(t *testing.T) storetest.Fixture {
			g := group()
			return storetest.Fixture{
				Store:  conformanceAdapter(t, c, newClient, g),
				Peer:   func() tenure.Store { return conformanceAdapter(t, c, newClient, g) },
				Timing: T,
				Slack:  500 * time.Millisecond,
				DeleteRecord: func(space string, shard int) {
					name := shardName(g, space, shard)
					if err := c.leases().Delete(t.Context(), name, metav1.DeleteOptions{}); err != nil {
						t.Fatalf("delete %s: %v", name, err)
					}
				},
			}
		})
	})
	t.Run("membershiptest", func(t *testing.T) {
		t.Parallel()
		membershiptest.Run(t, func(t *testing.T, _ []string) membershiptest.Fixture {
			g := group()
			return membershiptest.Fixture{
				New:      func() tenure.Membership { return conformanceAdapter(t, c, newClient, g) },
				Observer: true,
				Settle:   settle,
			}
		})
	})
}

// conformanceAdapter returns an Adapter of group g in c.ns with timing T, on a new client from
// newClient. When the case ends, it closes every session the Adapter knows and leaves, checks
// that the Adapter's goroutines stop, and that no request of its client got 403 Forbidden.
func conformanceAdapter(t *testing.T, c *cluster, newClient newClientFunc, g string) *Adapter {
	t.Helper()
	cl, rec := newClient(t)
	t.Cleanup(func() {
		for _, r := range rec.filter(func(r request) bool { return r.status == http.StatusForbidden }) {
			t.Errorf("request %s %v got 403 Forbidden", r, r.query)
		}
	})
	a := New(cl, c.ns, g, WithTiming(T))
	if a.err != nil {
		t.Fatalf("New: %v", a.err)
	}
	t.Cleanup(func() {
		stopAdapter(t, a)
		waitStopped(t, a)
	})
	return a
}

// stopAdapter closes every session a knows that has not ended, in no particular order, and
// leaves, so that a's goroutines stop.
func stopAdapter(t *testing.T, a *Adapter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a.mu.Lock()
	var holders []string
	for h, s := range a.sessions {
		if !s.ended {
			holders = append(holders, h)
		}
	}
	a.mu.Unlock()
	for _, h := range holders {
		if err := a.CloseSession(ctx, h); err != nil {
			t.Errorf("CloseSession(%s) at the end: %v", h, err)
		}
	}
	if err := a.Leave(ctx); err != nil {
		t.Errorf("Leave at the end: %v", err)
	}
}

// applyRBAC creates the objects of rbac.yaml in c.ns, with c.ns for the namespace default,
// after checking that the manifest holds K8S-850's Role, ServiceAccount and RoleBinding, in that
// order. It waits until the service account may list Leases.
func applyRBAC(t *testing.T, c *cluster) {
	t.Helper()
	ctx := t.Context()
	manifest, err := os.ReadFile("rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	manifest = bytes.ReplaceAll(manifest, []byte("namespace: default\n"), []byte("namespace: "+c.ns+"\n"))
	var objs []runtime.Object
	for doc := range strings.SplitSeq(string(manifest), "---\n") {
		obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(doc), nil, nil)
		if err != nil {
			t.Fatalf("decode rbac.yaml: %v", err)
		}
		objs = append(objs, obj)
	}
	meta := metav1.ObjectMeta{Name: "tenure", Namespace: c.ns}
	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: meta,
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "delete"},
		}},
	}
	account := &corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: meta}
	binding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: meta,
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "tenure", Namespace: c.ns}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "tenure"},
	}
	if want := []runtime.Object{role, account, binding}; !reflect.DeepEqual(objs, want) {
		t.Fatalf("rbac.yaml holds %+v, want %+v", objs, want)
	}
	// The manifest holds exactly these objects: create them.
	if _, err := c.raw.RbacV1().Roles(c.ns).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Role: %v", err)
	}
	if _, err := c.raw.CoreV1().ServiceAccounts(c.ns).Create(ctx, account, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create ServiceAccount: %v", err)
	}
	if _, err := c.raw.RbacV1().RoleBindings(c.ns).Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create RoleBinding: %v", err)
	}

	// The API server's authorizer learns of the binding through its own informers.
	sa, _ := c.clientAs(t, "system:serviceaccount:"+c.ns+":tenure")
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := sa.CoordinationV1().Leases(c.ns).List(ctx, metav1.ListOptions{})
		if err == nil {
			break
		}
		if !apierrors.IsForbidden(err) || time.Now().After(deadline) {
			t.Fatalf("list Leases as the service account: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := sa.CoreV1().Pods(c.ns).List(ctx, metav1.ListOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("list pods as the service account = %v, want 403 Forbidden", err)
	}
}
