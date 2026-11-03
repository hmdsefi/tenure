// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"testing"
	"time"
)

func TestATCOR05DefaultTiming(t *testing.T) {
	want := Timing{
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		SlotGrace:     60 * time.Second,
	}
	if DefaultTiming != want {
		t.Fatalf("DefaultTiming = %+v, want %+v", DefaultTiming, want)
	}
	if err := DefaultTiming.Validate(); err != nil {
		t.Fatalf("DefaultTiming.Validate() = %v, want nil", err)
	}
	if got := DefaultTiming.DriftBound(); got != 0.2 {
		t.Fatalf("DefaultTiming.DriftBound() = %v, want 0.2", got)
	}
}

func TestATCOR06TimingValidate(t *testing.T) {
	const rangeErr = "must be in (0s, 24h0m0s]"
	leaseErr := "core: invalid timing: LeaseDuration 0s " + rangeErr
	with := func(f func(*Timing)) Timing {
		tm := DefaultTiming
		f(&tm)
		return tm
	}
	tests := []struct {
		name string
		t    Timing
		want string // "" means nil
	}{
		{"default", DefaultTiming, ""},
		{"lease zero", with(func(t *Timing) { t.LeaseDuration = 0 }), leaseErr},
		{"retry negative", with(func(t *Timing) { t.RetryPeriod = -time.Second }),
			"core: invalid timing: RetryPeriod -1s " + rangeErr},
		{"grace above 24h", with(func(t *Timing) { t.SlotGrace = 24*time.Hour + 1 }),
			"core: invalid timing: SlotGrace 24h0m0.000000001s " + rangeErr},
		{"renew zero", with(func(t *Timing) { t.RenewDeadline = 0 }),
			"core: invalid timing: RenewDeadline 0s " + rangeErr},
		{"lease and retry zero", with(func(t *Timing) { t.LeaseDuration, t.RetryPeriod = 0, 0 }), leaseErr},
		{"zero value", Timing{}, leaseErr},

		{"renew 6s retry 5s", with(func(t *Timing) { t.RenewDeadline, t.RetryPeriod = 6*time.Second, 5*time.Second }),
			"core: invalid timing: RenewDeadline 6s must be greater than 1.2 times RetryPeriod 5s"},
		{"renew 6s retry 4999ms", with(func(t *Timing) { t.RenewDeadline, t.RetryPeriod = 6*time.Second, 4999*time.Millisecond }), ""},
		{"retry 8s", with(func(t *Timing) { t.RetryPeriod = 8 * time.Second }), ""},

		{"lease 11s", with(func(t *Timing) { t.LeaseDuration = 11 * time.Second }),
			"core: invalid timing: LeaseDuration 11s must be at least 21/19 times RenewDeadline 10s"},
		{"lease 21s renew 19s", with(func(t *Timing) { t.LeaseDuration, t.RenewDeadline = 21*time.Second, 19*time.Second }), ""},
		{"lease 20999ms renew 19s", with(func(t *Timing) { t.LeaseDuration, t.RenewDeadline = 20999*time.Millisecond, 19*time.Second }),
			"core: invalid timing: LeaseDuration 20.999s must be at least 21/19 times RenewDeadline 19s"},

		{"grace 14s", with(func(t *Timing) { t.SlotGrace = 14 * time.Second }),
			"core: invalid timing: SlotGrace 14s must be at least LeaseDuration 15s"},
		{"grace 15s", with(func(t *Timing) { t.SlotGrace = 15 * time.Second }), ""},

		// Check order (COR-070): the first failing check wins.
		{"checks 2 and 3 fail", Timing{LeaseDuration: 11 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 10 * time.Second, SlotGrace: 60 * time.Second},
			"core: invalid timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 10s"},
		{"checks 3 and 4 fail", Timing{LeaseDuration: 11 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, SlotGrace: 5 * time.Second},
			"core: invalid timing: LeaseDuration 11s must be at least 21/19 times RenewDeadline 10s"},
		{"checks 2 and 4 fail", Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 10 * time.Second, SlotGrace: 5 * time.Second},
			"core: invalid timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 10s"},
		{"checks 2, 3 and 4 fail", Timing{LeaseDuration: 11 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 10 * time.Second, SlotGrace: 5 * time.Second},
			"core: invalid timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 10s"},
		{"check 1 before check 3", Timing{LeaseDuration: 11 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second, SlotGrace: 0},
			"core: invalid timing: SlotGrace 0s " + rangeErr},
		{"check 1 before check 2", Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 0, SlotGrace: 60 * time.Second},
			"core: invalid timing: RetryPeriod 0s " + rangeErr},

		{"largest values", Timing{LeaseDuration: 24 * time.Hour, RenewDeadline: 20 * time.Hour, RetryPeriod: time.Hour, SlotGrace: 24 * time.Hour}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.t.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Fatalf("Validate() = %q, want %q", err.Error(), tt.want)
			}
			if !errors.Is(err, ErrInvalidTiming) {
				t.Fatalf("Validate() = %v, does not match ErrInvalidTiming", err)
			}
		})
	}
}

func TestATCOR07DriftBound(t *testing.T) {
	s := time.Second
	tests := []struct {
		name string
		t    Timing
		want float64
	}{
		{"default", DefaultTiming, 0.2},
		{"21s 19s", Timing{LeaseDuration: 21 * s, RenewDeadline: 19 * s}, float64(2e9) / float64(40e9)},
		{"137s 107s", Timing{LeaseDuration: 137 * s, RenewDeadline: 107 * s}, float64(30e9) / float64(244e9)},
		{"equal", Timing{LeaseDuration: 10 * s, RenewDeadline: 10 * s}, 0},
		{"lease below renew", Timing{LeaseDuration: 5 * s, RenewDeadline: 10 * s}, 0},
		{"zero value", Timing{}, 0},
		{"renew zero", Timing{LeaseDuration: 10 * s}, 0},
		{"lease zero", Timing{RenewDeadline: 10 * s}, 0},
		{"negative lease", Timing{LeaseDuration: -10 * s, RenewDeadline: -20 * s}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.DriftBound(); got != tt.want {
				t.Fatalf("DriftBound() = %v, want %v", got, tt.want)
			}
		})
	}

	// The bound is strict: the inequality of architecture 7.5 holds just inside it.
	rho := 0.199999
	if 10*(1+rho) >= 15*(1-rho) {
		t.Fatalf("10*(1+ρ) < 15*(1-ρ) fails at ρ = %v", rho)
	}
	if rho >= DefaultTiming.DriftBound() {
		t.Fatalf("ρ = %v is not below DriftBound() = %v", rho, DefaultTiming.DriftBound())
	}
}
