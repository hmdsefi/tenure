// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hmdsefi/tenure"
)

func TestNotOwnerErrorMessage(t *testing.T) {
	cases := []struct {
		route tenure.Member
		want  string
	}{
		{tenure.Member{ID: "n2", Addr: "a2"}, "tenure: not owner of orders/3: route to n2 (a2)"},
		{tenure.Member{ID: "n2"}, "tenure: not owner of orders/3: route to n2"},
		{tenure.Member{}, "tenure: not owner of orders/3: no route"},
	}
	for _, c := range cases {
		var err error = &tenure.NotOwnerError{Space: "orders", Shard: 3, Route: c.route}
		if got := err.Error(); got != c.want {
			t.Errorf("Error() = %q, want %q", got, c.want)
		}
		wrapped := fmt.Errorf("serve: %w", err)
		if !errors.Is(wrapped, tenure.ErrNotOwner) {
			t.Errorf("errors.Is(%q, ErrNotOwner) = false, want true", wrapped)
		}
		if errors.Is(wrapped, tenure.ErrNoMembers) {
			t.Errorf("errors.Is(%q, ErrNoMembers) = true, want false", wrapped)
		}
		var target *tenure.NotOwnerError
		if !errors.As(wrapped, &target) || target.Route.ID != c.route.ID {
			t.Errorf("errors.As(%q) = %v, %+v, want the *NotOwnerError", wrapped, target != nil, target)
		}
	}
}

func TestErrorValues(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{tenure.ErrNotOwner, "tenure: not owner"},
		{tenure.ErrNoMembers, "tenure: no members"},
		{tenure.ErrDraining, "tenure: draining"},
	}
	for _, c := range cases {
		if got := c.err.Error(); got != c.want {
			t.Errorf("Error() = %q, want %q", got, c.want)
		}
	}
	all := []error{tenure.ErrNoSlot, tenure.ErrSessionLost, tenure.ErrClosed, tenure.ErrNotOwner, tenure.ErrNoMembers, tenure.ErrDraining}
	for i, a := range all {
		for j, b := range all {
			if (i == j) != errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", a, b, errors.Is(a, b), i == j)
			}
		}
	}
}
