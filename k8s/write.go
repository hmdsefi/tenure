// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"fmt"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// maxWriteAttempts is how many writes writeLease sends at most for one call (K8S-330).
const maxWriteAttempts = 3

// change is a decision's write: create or update lease, delete the Lease decided on, or, as
// the zero change, nothing.
type change struct {
	// lease is the whole Lease to create, when there is none, or to update. writeLease sets its
	// name and, for an update, the resourceVersion of the state decided on.
	lease *coordinationv1.Lease
	// del deletes the Lease decided on, with its UID and resourceVersion as preconditions.
	del bool
}

// decider decides the write to Lease name from cur, its current state as far as the Adapter
// knows, nil when it has none. It builds the Lease to write from a deep copy of cur, never cur
// itself, changing only the fields its operation names, so foreign labels and annotations
// survive. An error ends the write with that error and nothing written.
type decider func(cur *coordinationv1.Lease) (change, error)

// writeLease writes Lease name by compare-and-swap (K8S-330): it decides from cur, the state
// the caller knows, then creates the Lease when cur is nil or updates the whole object with
// cur's resourceVersion, or deletes it with cur's UID and resourceVersion as preconditions. When
// the write fails because the Lease changed (Conflict, AlreadyExists, or NotFound for an update
// or a delete), it reads the Lease and decides again, sending at most maxWriteAttempts writes;
// then it returns "k8s: lease <name>: 3 conflicts: <last error>". It returns the Lease written,
// which it notes in the running cache (noteRead), nil when it deleted the Lease or decided on no
// change. Other errors of the API server come back as "k8s: <op> <name>: <error>"; errors of
// decide come back as they are.
func (a *Adapter) writeLease(ctx context.Context, op, name string, cur *coordinationv1.Lease, decide decider) (*coordinationv1.Lease, error) {
	leases := a.leases()
	for attempt := 1; ; attempt++ {
		ch, err := decide(cur)
		if err != nil {
			return nil, err
		}
		var written *coordinationv1.Lease
		sent := a.now()
		switch {
		case ch.del && cur != nil:
			pre := metav1.Preconditions{UID: &cur.UID, ResourceVersion: &cur.ResourceVersion}
			err = leases.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &pre})
		case ch.lease == nil:
			return nil, nil
		case cur == nil:
			ch.lease.Name, ch.lease.ResourceVersion = name, ""
			written, err = leases.Create(ctx, ch.lease, metav1.CreateOptions{})
		default:
			ch.lease.Name, ch.lease.ResourceVersion = name, cur.ResourceVersion
			written, err = leases.Update(ctx, ch.lease, metav1.UpdateOptions{})
		}
		if err == nil {
			// The cache knows the write at once, before the informer tells: a decision on the
			// cache, such as whether a replaced incarnation still holds its member Lease, must not
			// meet an older state of a Lease this Adapter wrote.
			a.noteRead(written)
			a.noteOwnWrite(cur, written, sent)
			return written, nil
		}
		gone := cur != nil && apierrors.IsNotFound(err)
		if !gone && !apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("k8s: %s %s: %w", op, name, err)
		}
		if attempt == maxWriteAttempts {
			return nil, &conflictsError{name: name, err: err}
		}
		if gone {
			cur = nil
			continue
		}
		if cur, err = a.getLease(ctx, name); err != nil {
			return nil, fmt.Errorf("k8s: %s %s: %w", op, name, err)
		}
	}
}

// conflictsError is writeLease's error when maxWriteAttempts writes failed because the Lease
// changed: "k8s: lease <name>: 3 conflicts: <last error>".
type conflictsError struct {
	name string
	err  error // the last write's error
}

func (e *conflictsError) Error() string {
	return fmt.Sprintf("k8s: lease %s: %d conflicts: %v", e.name, maxWriteAttempts, e.err)
}

func (e *conflictsError) Unwrap() error { return e.err }

// getLease reads Lease name from the API server: nil, with no error, when there is none.
func (a *Adapter) getLease(ctx context.Context, name string) (*coordinationv1.Lease, error) {
	l, err := a.leases().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return l, err
}

// leases returns the client of the Leases of the Adapter's namespace.
func (a *Adapter) leases() coordinationv1client.LeaseInterface {
	return a.client.CoordinationV1().Leases(a.namespace)
}
