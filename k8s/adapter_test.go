// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/hmdsefi/tenure/core"
)

// T is the timing of the adapter's tests: 3 s / 2 s / 500 ms, and a SlotGrace of 3 s.
var T = core.Timing{
	LeaseDuration: 3 * time.Second,
	RenewDeadline: 2 * time.Second,
	RetryPeriod:   500 * time.Millisecond,
	SlotGrace:     3 * time.Second,
}

// adapterCalls has one row per exported method of *Adapter, as TestATK8S01ConfigErrors checks: a
// call with valid arguments that returns the method's error.
var adapterCalls = []struct {
	method string
	call   func(ctx context.Context, a *Adapter) error
}{
	{"CheckNames", func(_ context.Context, a *Adapter) error { return a.CheckNames([]string{"markets"}, "pod-0") }},
	{"CheckTiming", func(_ context.Context, a *Adapter) error { return a.CheckTiming(T) }},
	{"KeepAlive", func(ctx context.Context, a *Adapter) error { return a.KeepAlive(ctx, "pod-0/1", T) }},
	{"CloseSession", func(ctx context.Context, a *Adapter) error { return a.CloseSession(ctx, "pod-0/1") }},
	{"Acquire", func(ctx context.Context, a *Adapter) error {
		_, err := a.Acquire(ctx, "pod-0/1", "markets", 0)
		return err
	}},
	{"Release", func(ctx context.Context, a *Adapter) error { return a.Release(ctx, "pod-0/1", "markets", 0, 1) }},
	{"Confirm", func(ctx context.Context, a *Adapter) error {
		_, err := a.Confirm(ctx, "pod-0/1", "markets", 0, 1)
		return err
	}},
	{"Holdings", func(ctx context.Context, a *Adapter) error {
		_, err := a.Holdings(ctx, "markets")
		return err
	}},
	{"WatchHolders", func(ctx context.Context, a *Adapter) error {
		ch, err := a.WatchHolders(ctx)
		if ch != nil {
			return fmt.Errorf("WatchHolders = a channel, want nil with %w", err)
		}
		return err
	}},
	{"ClaimSlot", func(ctx context.Context, a *Adapter) error {
		slot, err := a.ClaimSlot(ctx, "pod-0/1")
		if slot != -1 {
			return fmt.Errorf("slot %d, want -1 with %w", slot, err)
		}
		return err
	}},
	{"ReleaseSlot", func(ctx context.Context, a *Adapter) error { return a.ReleaseSlot(ctx, "pod-0/1", 0) }},
	{"Slots", func(ctx context.Context, a *Adapter) error {
		count, holders, err := a.Slots(ctx)
		if count != 0 || holders != nil {
			return fmt.Errorf("Slots = %d, %q, want 0, nil with %w", count, holders, err)
		}
		return err
	}},
	{"SetSlots", func(ctx context.Context, a *Adapter) error { return a.SetSlots(ctx, 3) }},
	{"Join", func(ctx context.Context, a *Adapter) error { return a.Join(ctx, core.Member{ID: "pod-0", Slot: -1}) }},
	{"SetDraining", func(ctx context.Context, a *Adapter) error { return a.SetDraining(ctx, true) }},
	{"Leave", func(ctx context.Context, a *Adapter) error { return a.Leave(ctx) }},
	{"Watch", func(ctx context.Context, a *Adapter) error {
		ch, err := a.Watch(ctx)
		if ch != nil {
			return fmt.Errorf("a channel with %w", err)
		}
		return err
	}},
}

// TestATK8S01ConfigErrors checks New's configuration error, which every method returns first
// without a request, the options' defaults, and CheckTiming. The acceptance test's steps were
// written from the issue's text: the original text is unavailable.
func TestATK8S01ConfigErrors(t *testing.T) {
	ctx := t.Context()

	t.Run("every method", func(t *testing.T) {
		typ := reflect.TypeFor[*Adapter]()
		want := make([]string, 0, typ.NumMethod())
		for m := range typ.Methods() {
			want = append(want, m.Name)
		}
		got := make([]string, 0, len(adapterCalls))
		for _, c := range adapterCalls {
			got = append(got, c.method)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("adapterCalls has rows for %q, want one row for each exported method of *Adapter: %q", got, want)
		}
	})

	t.Run("nil client", func(t *testing.T) {
		for _, a := range []*Adapter{
			New(nil, "default", "g"),
			New(nil, "Bad NS", "BAD", WithRecordConcurrency(0)),
		} {
			for _, c := range adapterCalls {
				if err := c.call(ctx, a); err == nil || err.Error() != "k8s: nil client" {
					t.Errorf("%s with a nil client = %v, want k8s: nil client", c.method, err)
				}
			}
		}
	})

	long := strings.Repeat("a", 63)
	bad := core.Timing{LeaseDuration: time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: time.Second, SlotGrace: time.Second}
	halfSecond := T
	halfSecond.LeaseDuration = 2500 * time.Millisecond
	tests := []struct {
		name      string
		namespace string
		group     string
		opts      []Option
		want      string
	}{
		{name: "empty namespace", namespace: "", group: "g", want: `k8s: namespace "" is not a DNS-1123 label`},
		{name: "upper-case namespace", namespace: "Default", group: "g", want: `k8s: namespace "Default" is not a DNS-1123 label`},
		{name: "namespace with a dot", namespace: "a.b", group: "g", want: `k8s: namespace "a.b" is not a DNS-1123 label`},
		{name: "namespace of 64", namespace: long + "a", group: "g", want: `k8s: namespace "` + long + `a" is not a DNS-1123 label`},
		{name: "namespace before group", namespace: "-ns", group: "G", want: `k8s: namespace "-ns" is not a DNS-1123 label`},
		{name: "empty group", namespace: "default", group: "", want: `k8s: group "" is not a DNS-1123 label of at most 40 characters`},
		{name: "upper-case group", namespace: "default", group: "T1", want: `k8s: group "T1" is not a DNS-1123 label of at most 40 characters`},
		{name: "group ending in -", namespace: "default", group: "g-", want: `k8s: group "g-" is not a DNS-1123 label of at most 40 characters`},
		{name: "group of 41", namespace: "default", group: long[:41], want: `k8s: group "` + long[:41] + `" is not a DNS-1123 label of at most 40 characters`},
		{
			name: "group before options", namespace: "default", group: "a_b", opts: []Option{WithRecordConcurrency(0)},
			want: `k8s: group "a_b" is not a DNS-1123 label of at most 40 characters`,
		},
		{
			name: "invalid timing", namespace: "default", group: "g", opts: []Option{WithTiming(bad)},
			want: "k8s: WithTiming: core: invalid timing: LeaseDuration 1s must be at least 21/19 times RenewDeadline 2s",
		},
		{
			name: "timing of 2.5s", namespace: "default", group: "g", opts: []Option{WithTiming(halfSecond)},
			want: "k8s: WithTiming: LeaseDuration 2.5s is not a whole number of seconds",
		},
		{name: "record concurrency 0", namespace: "default", group: "g", opts: []Option{WithRecordConcurrency(0)}, want: "k8s: WithRecordConcurrency(0): want at least 1"},
		{name: "record concurrency -3", namespace: "default", group: "g", opts: []Option{WithRecordConcurrency(-3)}, want: "k8s: WithRecordConcurrency(-3): want at least 1"},
		{name: "member GC -1s", namespace: "default", group: "g", opts: []Option{WithMemberGC(-time.Second)}, want: "k8s: WithMemberGC(-1s): want 0 or more"},
		{name: "watch latency -1ms", namespace: "default", group: "g", opts: []Option{WithWatchLatency(-time.Millisecond)}, want: "k8s: WithWatchLatency(-1ms): want 0 or more"},
		{
			name: "first option wins", namespace: "default", group: "g",
			opts: []Option{WithLogger(nil), WithMemberGC(0), WithWatchLatency(-time.Second), WithRecordConcurrency(0)},
			want: "k8s: WithWatchLatency(-1s): want 0 or more",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			a := New(client, tc.namespace, tc.group, tc.opts...)
			for _, c := range adapterCalls {
				if err := c.call(ctx, a); err == nil || err.Error() != tc.want {
					t.Errorf("%s = %v, want %s", c.method, err, tc.want)
				}
			}
			if n := len(client.Actions()); n != 0 {
				t.Errorf("%d requests, want none: %v", n, client.Actions())
			}
		})
	}

	t.Run("invalid timing wraps ErrInvalidTiming", func(t *testing.T) {
		err := New(fake.NewClientset(), "default", "g", WithTiming(bad)).CheckTiming(T)
		if !errors.Is(err, core.ErrInvalidTiming) {
			t.Errorf("CheckTiming = %v, want an error matching core.ErrInvalidTiming", err)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		a := New(fake.NewClientset(), "default", "g")
		if a.err != nil {
			t.Fatalf("New: %v", a.err)
		}
		if a.timing != core.DefaultTiming || a.recordConcurrency != 4 || a.memberGC != time.Hour ||
			a.watchLatency != time.Second || a.logger == nil {
			t.Errorf("defaults: timing %+v, record concurrency %d, member GC %v, watch latency %v, logger %v; "+
				"want %+v, 4, 1h0m0s, 1s, a logger", a.timing, a.recordConcurrency, a.memberGC, a.watchLatency, a.logger, core.DefaultTiming)
		}
	})

	t.Run("options", func(t *testing.T) {
		l := slog.New(slog.DiscardHandler)
		client := fake.NewClientset()
		a := New(client, "tenure-0123abcd", "g0123abcd", WithTiming(T), WithRecordConcurrency(2), WithMemberGC(0),
			WithWatchLatency(0), WithLogger(l))
		if a.err != nil {
			t.Fatalf("New: %v", a.err)
		}
		if a.client != client || a.namespace != "tenure-0123abcd" || a.group != "g0123abcd" || a.timing != T ||
			a.recordConcurrency != 2 || a.memberGC != 0 || a.watchLatency != 0 || a.logger != l {
			t.Errorf("New with options = %+v", a)
		}
		b := New(client, "default", strings.Repeat("g", 40), WithLogger(nil), WithRecordConcurrency(1))
		if b.err != nil {
			t.Fatalf("New with a group of 40, WithLogger(nil) and WithRecordConcurrency(1): %v", b.err)
		}
		if b.recordConcurrency != 1 {
			t.Errorf("record concurrency %d, want 1", b.recordConcurrency)
		}
		if b.logger == nil {
			t.Fatal("WithLogger(nil) left no logger, want one that discards")
		}
		b.logger.Error("discarded")
		if n := len(client.Actions()); n != 0 {
			t.Errorf("%d requests, want none: %v", n, client.Actions())
		}
	})

	t.Run("CheckTiming", func(t *testing.T) {
		client := fake.NewClientset()
		a := New(client, "default", "g")
		for _, tm := range []core.Timing{T, core.DefaultTiming} {
			if err := a.CheckTiming(tm); err != nil {
				t.Errorf("CheckTiming(%+v) = %v, want nil", tm, err)
			}
		}
		const want = "k8s: LeaseDuration 2.5s is not a whole number of seconds"
		if err := a.CheckTiming(halfSecond); err == nil || err.Error() != want {
			t.Errorf("CheckTiming with a 2.5s LeaseDuration = %v, want %s", err, want)
		}
		// Start validates the Timing; CheckTiming only adds the whole-second rule.
		if err := a.CheckTiming(bad); err != nil {
			t.Errorf("CheckTiming(%+v) = %v, want nil: it does not repeat Validate", bad, err)
		}
		if n := len(client.Actions()); n != 0 {
			t.Errorf("%d requests, want none: %v", n, client.Actions())
		}
	})
}
