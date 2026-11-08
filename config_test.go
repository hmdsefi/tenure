// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

var (
	_ tenure.NameChecker   = (*checkingStore)(nil)
	_ tenure.TimingChecker = (*checkingStore)(nil)
	_ tenure.NameChecker   = (*checkingMembership)(nil)
	_ tenure.TimingChecker = (*checkingMembership)(nil)
)

// noRun is a Space.Run that returns at once.
func noRun(context.Context, *tenure.Shard) error { return nil }

// configDoubles are the doubles a configuration test checks a Config against.
type configDoubles struct {
	fs *recStore
	sm *recMembership
}

// newConfigDoubles returns fs over a new memstore and sm for member n1 of a one-member group.
func newConfigDoubles(t *testing.T) configDoubles {
	t.Helper()
	return configDoubles{
		fs: newRecStore(memstore.New(nil)),
		sm: newGroup(t, core.Member{ID: "n1", Slot: -1}).Node("n1"),
	}
}

// config returns a valid Config over d's checking doubles, whose checkers accept everything:
// member n1 and one space "orders" of 4 shards.
func (d configDoubles) config() tenure.Config {
	return tenure.Config{
		Self:       tenure.Member{ID: "n1", Addr: "10.0.0.1:8080"},
		Membership: d.sm.Checking(nil, nil),
		Store:      d.fs.Checking(nil, nil),
		Spaces:     []tenure.Space{{Name: "orders", Shards: 4, Run: noRun}},
	}
}

// wantNoCalls fails t when fs or sm recorded a call.
func (d configDoubles) wantNoCalls(t *testing.T) {
	t.Helper()
	if c := d.fs.Calls(); len(c) != 0 {
		t.Errorf("fs recorded %v, want no call", c)
	}
	if c := d.sm.Calls(); len(c) != 0 {
		t.Errorf("sm recorded %v, want no call", c)
	}
}

// wantInvalid fails t unless err's text is "tenure: invalid config: " + reason and, when is is not
// nil, err matches is.
func wantInvalid(t *testing.T, err error, reason string, is error) {
	t.Helper()
	if want := "tenure: invalid config: " + reason; err == nil || err.Error() != want {
		t.Errorf("error = %v, want %s", err, want)
	}
	if is != nil && !errors.Is(err, is) {
		t.Errorf("errors.Is(%v, %v) = false, want true", err, is)
	}
}

func TestATRUN01InvalidConfig(t *testing.T) {
	t.Run("Rejections", func(t *testing.T) {
		rows := []struct {
			name   string
			mutate func(cfg *tenure.Config, d configDoubles)
			reason string
			is     error
		}{
			// Check 1: Self.
			{"empty ID", func(c *tenure.Config, _ configDoubles) { c.Self.ID = "" },
				`Self: core: invalid member: empty ID`, core.ErrInvalidMember},
			{"ID with slash", func(c *tenure.Config, _ configDoubles) { c.Self.ID = "n/1" },
				`Self: core: invalid member: ID "n/1" contains '/'`, core.ErrInvalidMember},
			{"ID with zero byte", func(c *tenure.Config, _ configDoubles) { c.Self.ID = "n\x001" },
				`Self: core: invalid member: ID "n\x001" contains a zero byte`, core.ErrInvalidMember},
			{"LeaveAt before JoinAt", func(c *tenure.Config, _ configDoubles) {
				c.Self.JoinAt = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
				c.Self.LeaveAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			}, `Self: core: invalid member "n1": LeaveAt is not after JoinAt`, core.ErrInvalidMember},
			{"Self before everything", func(c *tenure.Config, _ configDoubles) {
				*c = tenure.Config{Self: tenure.Member{ID: ""}, Slots: -1, Spaces: []tenure.Space{{}}}
			}, `Self: core: invalid member: empty ID`, core.ErrInvalidMember},

			// Check 2: reserved Meta keys, the lowest one named.
			{"reserved Meta key", func(c *tenure.Config, _ configDoubles) {
				c.Self.Meta = map[string]string{"zone": "a", "tenure.split": "1", "tenure.b": "", "tenure.weight": "2"}
			}, `Self.Meta: key "tenure.b" is reserved`, nil},
			{"Meta before Membership", func(c *tenure.Config, _ configDoubles) {
				c.Self.Meta = map[string]string{"tenure.": ""}
				c.Membership = nil
			}, `Self.Meta: key "tenure." is reserved`, nil},

			// Check 3: Membership.
			{"nil Membership", func(c *tenure.Config, _ configDoubles) { c.Membership = nil },
				`Membership: nil`, nil},
			{"Membership before Timing", func(c *tenure.Config, _ configDoubles) {
				c.Membership = nil
				c.Timing = core.Timing{LeaseDuration: time.Second}
			}, `Membership: nil`, nil},

			// Check 4: Timing, after the defaults.
			{"RetryPeriod too long", func(c *tenure.Config, _ configDoubles) {
				c.Timing = core.Timing{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 9 * time.Second, SlotGrace: time.Minute}
			}, `Timing: core: invalid timing: RenewDeadline 10s must be greater than 1.2 times RetryPeriod 9s`, core.ErrInvalidTiming},
			{"default SlotGrace below LeaseDuration", func(c *tenure.Config, _ configDoubles) {
				c.Timing = core.Timing{LeaseDuration: 90 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second}
			}, `Timing: core: invalid timing: SlotGrace 1m0s must be at least LeaseDuration 1m30s`, core.ErrInvalidTiming},
			{"negative field", func(c *tenure.Config, _ configDoubles) {
				c.Timing = core.DefaultTiming
				c.Timing.LeaseDuration = -time.Second
			}, `Timing: core: invalid timing: LeaseDuration -1s must be in (0s, 24h0m0s]`, core.ErrInvalidTiming},

			// Check 5: Slots.
			{"negative Slots", func(c *tenure.Config, _ configDoubles) { c.Slots = -1 },
				`Slots: -1 is below 0`, nil},
			{"Slots before Spaces and Store", func(c *tenure.Config, _ configDoubles) {
				c.Slots = -3
				c.Spaces[0].Name = ""
				c.Store = nil
			}, `Slots: -3 is below 0`, nil},

			// Check 6: invalid space names.
			{"empty space name", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Name = "" },
				`Spaces[0].Name: invalid name ""`, nil},
			{"space name with slash", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Name = "a/b" },
				`Spaces[0].Name: invalid name "a/b"`, nil},
			{"space name with zero byte", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Name = "a\x00b" },
				`Spaces[0].Name: invalid name "a\x00b"`, nil},
			{"invalid name before Shards and Run", func(c *tenure.Config, _ configDoubles) {
				c.Spaces[0] = tenure.Space{Name: "/"}
			}, `Spaces[0].Name: invalid name "/"`, nil},

			// Check 7: reserved space names.
			{"reserved space name", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Name = "slots" },
				`Spaces[0].Name: reserved name "slots"`, nil},
			{"reserved name before duplicate", func(c *tenure.Config, _ configDoubles) {
				c.Spaces = []tenure.Space{
					{Name: "member", Shards: 1, Run: noRun},
					{Name: "member", Shards: 1, Run: noRun},
				}
			}, `Spaces[0].Name: reserved name "member"`, nil},

			// Check 8: duplicate space names.
			{"duplicate space name", func(c *tenure.Config, _ configDoubles) {
				c.Spaces = []tenure.Space{
					{Name: "orders", Shards: 1, Run: noRun},
					{Name: "markets", Shards: 1, Run: noRun},
					{Name: "orders", Shards: 2, Run: noRun},
				}
			}, `Spaces[2].Name: duplicate name "orders"`, nil},
			{"duplicate before Shards", func(c *tenure.Config, _ configDoubles) {
				c.Spaces = append(c.Spaces, tenure.Space{Name: "orders"})
			}, `Spaces[1].Name: duplicate name "orders"`, nil},

			// Check 9: Shards.
			{"zero Shards", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Shards = 0 },
				`Spaces[0].Shards: 0 is below 1`, nil},
			{"negative Shards", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Shards = -2 },
				`Spaces[0].Shards: -2 is below 1`, nil},
			{"Shards before Run", func(c *tenure.Config, _ configDoubles) {
				c.Spaces[0].Shards = 0
				c.Spaces[0].Run = nil
			}, `Spaces[0].Shards: 0 is below 1`, nil},

			// Check 10: Run.
			{"nil Run", func(c *tenure.Config, _ configDoubles) { c.Spaces[0].Run = nil },
				`Spaces[0].Run: nil`, nil},
			{"space by space", func(c *tenure.Config, _ configDoubles) {
				c.Spaces = []tenure.Space{
					{Name: "orders", Shards: 1},
					{Name: "", Shards: 1, Run: noRun},
				}
			}, `Spaces[0].Run: nil`, nil},
			{"Run before Store", func(c *tenure.Config, _ configDoubles) {
				c.Spaces[0].Run = nil
				c.Store = nil
			}, `Spaces[0].Run: nil`, nil},

			// Check 11: Store.
			{"nil Store with spaces", func(c *tenure.Config, _ configDoubles) { c.Store = nil },
				`Store: nil`, nil},
			{"nil Store with slots", func(c *tenure.Config, _ configDoubles) {
				c.Store = nil
				c.Spaces = nil
				c.Slots = 1
			}, `Store: nil`, nil},

			// Check 12: SlotStore.
			{"Store without slots", func(c *tenure.Config, d configDoubles) {
				c.Store = d.fs.StoreOnly()
				c.Slots = 3
			}, `Store: tenure_test.storeOnly is not a SlotStore`, nil},
		}
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					check := baseline(t)
					d := newConfigDoubles(t)
					cfg := d.config()
					row.mutate(&cfg, d)
					got, err := tenure.CheckConfig(cfg)
					wantInvalid(t, err, row.reason, row.is)
					if !reflect.DeepEqual(got, tenure.Config{}) {
						t.Errorf("CheckConfig returned %+v with the error, want the zero Config", got)
					}
					d.wantNoCalls(t)
					check()
				})
			})
		}
	})

	t.Run("PassingNames", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			d := newConfigDoubles(t)
			names := []string{
				"orders", "Slot", "SLOTS", "slots2", "slot-0", "members", "generations", "requests",
				"tenure", "tenure.x", "a.b_c-d", "with space", "café", "\\",
			}
			spaces := make([]tenure.Space, len(names))
			for i, n := range names {
				spaces[i] = tenure.Space{Name: n, Shards: 1, Run: noRun}
			}
			cfg := tenure.Config{
				Self: tenure.Member{ID: "n1", Meta: map[string]string{
					"tenure": "", "tenurex.y": "", "Tenure.x": "", "app.tenure.x": "", "tenure_x": "",
				}},
				Membership: d.sm,
				Store:      d.fs,
				Slots:      2,
				Spaces:     spaces,
			}
			got, err := tenure.CheckConfig(cfg)
			if err != nil {
				t.Fatalf("CheckConfig = %v, want nil", err)
			}
			gotNames := make([]string, len(got.Spaces))
			for i, sp := range got.Spaces {
				gotNames[i] = sp.Name
			}
			if !reflect.DeepEqual(gotNames, names) || !reflect.DeepEqual(got.Self.Meta, cfg.Self.Meta) {
				t.Errorf("CheckConfig changed the spaces or the Meta: %+v", got)
			}
			d.wantNoCalls(t)
			check()
		})
	})

	t.Run("Defaults", func(t *testing.T) {
		if tenure.DefaultSlotGrace != core.DefaultTiming.SlotGrace {
			t.Errorf("DefaultSlotGrace = %v, want core.DefaultTiming.SlotGrace %v", tenure.DefaultSlotGrace, core.DefaultTiming.SlotGrace)
		}
		custom := core.Timing{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 4 * time.Second, SlotGrace: 2 * time.Minute}
		noGrace := custom
		noGrace.SlotGrace = 0
		withGrace := noGrace
		withGrace.SlotGrace = tenure.DefaultSlotGrace
		src := strings.NewReader("0123456789abcdef")
		cases := []struct {
			name       string
			timing     core.Timing
			rand       io.Reader
			wantTiming core.Timing
			wantRand   io.Reader
		}{
			{"zero Timing", core.Timing{}, nil, core.DefaultTiming, rand.Reader},
			{"zero SlotGrace", noGrace, nil, withGrace, rand.Reader},
			{"set Timing and Rand", custom, src, custom, src},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					check := baseline(t)
					d := newConfigDoubles(t)
					cfg := tenure.Config{
						Self:       tenure.Member{ID: "n1", Addr: "10.0.0.1:8080", Slot: -5, Draining: true},
						Membership: d.sm,
						Store:      d.fs,
						Spaces:     []tenure.Space{{Name: "orders", Shards: 4, Fenced: true, Run: noRun}},
						Timing:     tc.timing,
						Rand:       tc.rand,
					}
					got, err := tenure.CheckConfig(cfg)
					if err != nil {
						t.Fatalf("CheckConfig = %v, want nil", err)
					}
					if got.Timing != tc.wantTiming {
						t.Errorf("Timing = %+v, want %+v", got.Timing, tc.wantTiming)
					}
					if got.Rand != tc.wantRand {
						t.Errorf("Rand = %v, want %v", got.Rand, tc.wantRand)
					}
					if want := (tenure.Member{ID: "n1", Addr: "10.0.0.1:8080", Slot: -1}); !reflect.DeepEqual(got.Self, want) {
						t.Errorf("Self = %+v, want %+v (Slot -5 passes and becomes -1)", got.Self, want)
					}
					if got.Membership != cfg.Membership || got.Store != cfg.Store || got.Slots != 0 ||
						len(got.Spaces) != 1 || got.Spaces[0].Name != "orders" || got.Spaces[0].Shards != 4 ||
						!got.Spaces[0].Fenced || got.Spaces[0].Run == nil {
						t.Errorf("CheckConfig = %+v, want cfg with only the defaults changed", got)
					}
					d.wantNoCalls(t)
					check()
				})
			})
		}
	})

	t.Run("MembershipOnly", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			d := newConfigDoubles(t)
			if _, err := tenure.CheckConfig(tenure.Config{Self: tenure.Member{ID: "n1"}, Membership: d.sm}); err != nil {
				t.Fatalf("CheckConfig of a membership-only Config = %v, want nil", err)
			}
			d.wantNoCalls(t)
			check()
		})
	})

	t.Run("Copies", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			d := newConfigDoubles(t)
			cfg := d.config()
			cfg.Self.Meta = map[string]string{"zone": "a"}
			got, err := tenure.CheckConfig(cfg)
			if err != nil {
				t.Fatalf("CheckConfig = %v, want nil", err)
			}
			cfg.Self.Meta["zone"] = "b"
			cfg.Spaces[0].Name = "changed"
			if got.Self.Meta["zone"] != "a" || got.Spaces[0].Name != "orders" {
				t.Errorf("the result shares Meta or Spaces with cfg: %+v", got)
			}
			check()
		})
	})
}

func TestATRUN02NameAndTimingCheckers(t *testing.T) {
	boom := errors.New("boom")
	timing := core.Timing{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 4 * time.Second}
	applied := timing
	applied.SlotGrace = tenure.DefaultSlotGrace
	names := []string{"orders", "markets"}
	sNames := fmt.Sprintf("store CheckNames(%v, n1)", names)
	mNames := fmt.Sprintf("membership CheckNames(%v, n1)", names)
	sTiming := fmt.Sprintf("store CheckTiming(%v)", applied)
	mTiming := fmt.Sprintf("membership CheckTiming(%v)", applied)

	cases := []struct {
		name                      string
		storeNames, storeTiming   error
		memberNames, memberTiming error
		reason                    string // "" for no error
		calls                     []string
	}{
		{name: "store refuses names", storeNames: boom, reason: "Store: boom",
			calls: []string{sNames}},
		{name: "membership refuses names", memberNames: boom, reason: "Membership: boom",
			calls: []string{sNames, mNames}},
		{name: "store refuses timing", storeTiming: boom, reason: "Store: boom",
			calls: []string{sNames, mNames, sTiming}},
		{name: "membership refuses timing", memberTiming: boom, reason: "Membership: boom",
			calls: []string{sNames, mNames, sTiming, mTiming}},
		{name: "both accept",
			calls: []string{sNames, mNames, sTiming, mTiming}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				d := newConfigDoubles(t)
				spaces := make([]tenure.Space, len(names))
				for i, n := range names {
					spaces[i] = tenure.Space{Name: n, Shards: 2, Run: noRun}
				}
				cfg := tenure.Config{
					Self:       tenure.Member{ID: "n1"},
					Membership: d.sm.Checking(tc.memberNames, tc.memberTiming),
					Store:      d.fs.Checking(tc.storeNames, tc.storeTiming),
					Slots:      1,
					Spaces:     spaces,
					Timing:     timing,
				}
				_, err := tenure.CheckConfig(cfg)
				if tc.reason == "" {
					if err != nil {
						t.Errorf("CheckConfig = %v, want nil", err)
					}
				} else {
					wantInvalid(t, err, tc.reason, boom)
				}
				if got := checkerCalls(d); !reflect.DeepEqual(got, tc.calls) {
					t.Errorf("calls = %q, want %q", got, tc.calls)
				}
				check()
			})
		})
	}

	// Without spaces the checkers get nil names; without a store only the membership's run.
	t.Run("both accept without spaces", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			check := baseline(t)
			d := newConfigDoubles(t)
			cfg := tenure.Config{Self: tenure.Member{ID: "n1"}, Membership: d.sm.Checking(nil, nil)}
			if _, err := tenure.CheckConfig(cfg); err != nil {
				t.Fatalf("CheckConfig = %v, want nil", err)
			}
			c := d.sm.Calls()
			if len(c) != 2 || c[0].Method != "CheckNames" || c[1].Method != "CheckTiming" ||
				!reflect.DeepEqual(c[0].Args, []any{[]string(nil), "n1"}) ||
				!reflect.DeepEqual(c[1].Args, []any{core.DefaultTiming}) {
				t.Errorf("sm recorded %v, want CheckNames([], n1) with nil spaces, then CheckTiming(DefaultTiming)", c)
			}
			if c := d.fs.Calls(); len(c) != 0 {
				t.Errorf("fs recorded %v, want no call", c)
			}
			check()
		})
	})
}

// checkerCalls returns the calls fs and sm recorded, in start order, each as "store" or
// "membership" followed by the call's method and arguments.
func checkerCalls(d configDoubles) []string {
	var out []string
	store := map[uint64]bool{}
	for _, c := range d.fs.Calls() {
		store[c.Seq] = true
	}
	for _, c := range callsInOrder(d.fs.Calls(), d.sm.Calls()) {
		who := "membership"
		if store[c.Seq] {
			who = "store"
		}
		args := make([]string, len(c.Args))
		for i, a := range c.Args {
			args[i] = fmt.Sprint(a)
		}
		out = append(out, who+" "+c.Method+"("+strings.Join(args, ", ")+")")
	}
	return out
}

func TestATSPL03ReservedMetaKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		check := baseline(t)
		d := newConfigDoubles(t)
		cfg := d.config()
		cfg.Self.Meta = map[string]string{"tenure.x": "1"}
		_, err := tenure.CheckConfig(cfg)
		if want := `tenure: invalid config: Self.Meta: key "tenure.x" is reserved`; err == nil || err.Error() != want {
			t.Errorf("CheckConfig = %v, want %s", err, want)
		}
		d.wantNoCalls(t)
		check()
	})
}

func TestATSPL47ReservedSpaceNames(t *testing.T) {
	for _, name := range []string{"generation", "member", "slot", "slots", "request"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				check := baseline(t)
				d := newConfigDoubles(t)
				cfg := tenure.Config{
					Self:       tenure.Member{ID: "n1"},
					Membership: d.sm,
					Store:      d.fs,
					Spaces:     []tenure.Space{{Name: name, Shards: 1, Run: noRun}},
				}
				_, err := tenure.CheckConfig(cfg)
				wantInvalid(t, err, fmt.Sprintf("Spaces[0].Name: reserved name %q", name), nil)
				d.wantNoCalls(t)
				check()
			})
		})
	}
}
