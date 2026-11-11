// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package world holds the simulated worlds the simulations run in: a store, a fenced
// downstream and the members that share the shards of some spaces.
//
// A Scenario describes one world: how many members it has, the spaces and the timing, how far
// the clocks and the network may stray, and the faults. From a Scenario alone, ComputeBound
// gives the longest time a shard may stay without an owner once the faults have stopped, and
// AssertsG2 says whether the timing still promises that no two members own a shard at once.
//
// The package is deterministic: it starts no goroutine, takes no lock, reads no wall clock and
// takes every random choice from the seed of the run, so a scenario and a seed give one run.
package world

import (
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/tenure/core"
)

// Takeover is the rule by which a shard whose holder stopped renewing its session is taken.
type Takeover uint8

const (
	// Server is the rule of a store that judges sessions by its own clock: the store ends a
	// session it has not seen renewed for the session's lease duration, and grants the
	// session's shards to the next member that asks. It is the zero value.
	Server Takeover = iota
	// Observer is the rule of a store that judges nothing: a member that wants a shard watches
	// the holder's session, and takes the shard with a compare-and-swap once its own clock has
	// seen the session unchanged for the lease duration.
	Observer
)

// String returns "server" or "observer", and "Takeover(7)" for a value that is neither.
func (t Takeover) String() string {
	switch t {
	case Server:
		return "server"
	case Observer:
		return "observer"
	default:
		return "Takeover(" + strconv.Itoa(int(t)) + ")"
	}
}

// Mutation names a bug that a build with planted bugs puts into the world, to show that the
// checks catch it. An ordinary build plants none, whatever the Mutation.
type Mutation uint8

const (
	// MutationNone plants no bug. It is the zero value.
	MutationNone Mutation = iota
	// GrantHeld makes the store grant a shard that a live session still holds.
	GrantHeld
	// SkipRaise makes a run write to the fenced downstream without raising the fence first.
	SkipRaise
	// LateDeadline makes a member place its session deadline later than its renewals allow.
	LateDeadline
	// ReadConfirm makes the store answer a confirmation without writing the record.
	ReadConfirm
)

// String returns the name of the constant, such as "MutationNone" or "GrantHeld", and
// "Mutation(9)" for a value that has none.
func (m Mutation) String() string {
	switch m {
	case MutationNone:
		return "MutationNone"
	case GrantHeld:
		return "GrantHeld"
	case SkipRaise:
		return "SkipRaise"
	case LateDeadline:
		return "LateDeadline"
	case ReadConfirm:
		return "ReadConfirm"
	default:
		return "Mutation(" + strconv.Itoa(int(m)) + ")"
	}
}

// Rollout replaces every member once, one after another, as a rolling deploy does.
type Rollout struct {
	// Start is when the first replacement becomes active.
	Start time.Duration
	// Gap is the time from one replacement becoming active to the next.
	Gap time.Duration
	// Surge is how long a replacement and the member it replaces run side by side: the member
	// starts to drain Surge after its replacement became active.
	Surge time.Duration
}

// Outage cuts the store off from every other node once.
type Outage struct {
	// At is when the outage starts.
	At time.Duration
	// For is how long the outage lasts.
	For time.Duration
}

// The values that stand for a zero field of a Scenario.
const (
	defaultMembers = 5
	defaultSpace   = "book"
	defaultShards  = 16
	defaultDelta   = 5 * time.Millisecond
)

// roleMembers is the fault target that names the active members and their replacements.
const roleMembers = "members"

// Scenario describes a simulated world. The zero value is a usable scenario: five members
// share the 16 shards of one unfenced space under the Server rule, with core.DefaultTiming,
// exact clocks, a 5 ms network and no faults.
type Scenario struct {
	// Takeover is the rule by which shards are taken from a holder that stopped renewing.
	Takeover Takeover
	// Members is the number of members, named m1 to mN. 0 means 5.
	Members int
	// Spaces lists the spaces. nil means one unfenced space, "book", of 16 shards.
	Spaces []core.SpaceSpec
	// Timing is the session timing of every member. The zero Timing means core.DefaultTiming.
	Timing core.Timing
	// Slots is the store's slot count at the start. With 0 the store has no slots, and shards
	// are placed by member ID.
	Slots int
	// Rho bounds the initial clock drifts: each node's drift is drawn from [-Rho, Rho] parts
	// per million.
	Rho int32
	// Delta is the largest one-way delay of a message. 0 means 5 ms.
	Delta time.Duration
	// WallSkew bounds the initial wall-clock offsets: each member's offset is drawn from
	// [-WallSkew, WallSkew].
	WallSkew time.Duration
	// WallSteps, when positive, makes each member's wall clock step during the run, by up to
	// WallSteps either way.
	WallSteps time.Duration
	// StopDelay is the longest time a run takes to stop once it is told to: each stop takes a
	// time drawn from [0, StopDelay].
	StopDelay time.Duration
	// EndAfter, when positive, makes the store end every session EndAfter after it opened.
	// The recovery time is raised to the latest end of a session opened at the start, so a
	// run with EndAfter should end before 2·EndAfter, when the recovered sessions would end in
	// their turn. A scenario with EndAfter does not assert that no two members own a shard at
	// once.
	EndAfter time.Duration
	// Faults plans random faults, or none when it is nil. The world plans a copy of it and
	// leaves the caller's value as it is.
	Faults *fault.Random
	// Rollout, when it is not nil, replaces every member once.
	Rollout *Rollout
	// Outage, when it is not nil, cuts the store off once.
	Outage *Outage
	// SetSlotsAt and SetSlotsTo make m1 set the store's slot count to SetSlotsTo at the time
	// SetSlotsAt.
	SetSlotsAt time.Duration
	SetSlotsTo int
	// NoLiveness leaves out the final check that every shard has an owner once the bound of
	// ComputeBound has passed after the faults.
	NoLiveness bool
	// IgnoreCheck makes the runs keep writing after the check they make before each write
	// says that they must stop.
	IgnoreCheck bool
	// Mutation is the bug that a build with planted bugs puts into the world.
	Mutation Mutation
}

// withDefaults returns sc with every zero field that has a default set to it: Members,
// Spaces (nil only: an empty list stays empty), Timing (the zero Timing only) and Delta. It
// copies nothing else and checks nothing: a value out of range stays as it is, and applying it
// again changes nothing more.
func (sc Scenario) withDefaults() Scenario {
	if sc.Members == 0 {
		sc.Members = defaultMembers
	}
	if sc.Spaces == nil {
		sc.Spaces = []core.SpaceSpec{{Name: defaultSpace, Shards: defaultShards}}
	}
	if sc.Timing == (core.Timing{}) {
		sc.Timing = core.DefaultTiming
	}
	if sc.Delta == 0 {
		sc.Delta = defaultDelta
	}
	return sc
}

// million converts parts per million: a clock with a drift of ρ ppm counts million ± ρ
// nanoseconds in million nanoseconds of global time.
const million = 1_000_000

// MaxDrift returns the largest clock drift the scenario allows, in parts per million: the
// larger of Rho and the largest Magnitude, whatever its sign, of the clock-drift rules of
// Faults. It is never negative, and a Magnitude beyond the range of int32 counts as the
// largest int32.
func (sc Scenario) MaxDrift() int32 {
	rho := max(sc.Rho, 0)
	if sc.Faults == nil {
		return rho
	}
	for _, rule := range sc.Faults.Rules {
		if rule.Kind != fault.KindClockDrift {
			continue
		}
		m := rule.Magnitude
		if m < 0 {
			m = -m // still negative for the smallest int64
		}
		if m < 0 || m > math.MaxInt32 {
			return math.MaxInt32
		}
		rho = max(rho, int32(m))
	}
	return rho
}

// ComputeBound returns the scenario's bound: the longest time a shard may stay without an
// owner after the faults stopped. With LD and RP the scenario's LeaseDuration and RetryPeriod,
// δ its Delta, ρ its MaxDrift and the zero fields at their defaults, it is
//
//	(LD + 3·RP + StopDelay + G + k·δ) · 10⁶ / (10⁶ − ρ)
//
// nanoseconds, rounded up. The session of the old holder runs out (LD); the member that takes
// the shard needs a renewal, a snapshot of the store and a retry (3·RP); a run that moves
// stops first (StopDelay); and the messages of the exchange cross the network: k is 9 one-way
// delays under the Server rule and 18 under the Observer rule, whose takeover is three
// requests. G is the timing's SlotGrace when the store has slots (Slots > 0), the time after
// which a vacant slot leaves the assignment, and 0 otherwise. The last factor turns the sum,
// counted on the slowest clock the scenario allows, into global time.
//
// The result is exact: it is computed in integers that cannot overflow. A scenario whose
// clocks may stand still (ρ of 10⁶ or more) has no bound, and ComputeBound returns the largest
// Duration for it, as it does for a bound beyond that.
func ComputeBound(sc Scenario) time.Duration {
	sc = sc.withDefaults()
	hops := int64(9)
	if sc.Takeover == Observer {
		hops = 18
	}
	grace := time.Duration(0)
	if sc.Slots > 0 {
		grace = sc.Timing.SlotGrace
	}

	sum := big.NewInt(int64(sc.Timing.LeaseDuration))
	sum.Add(sum, product(3, int64(sc.Timing.RetryPeriod)))
	sum.Add(sum, big.NewInt(int64(sc.StopDelay)))
	sum.Add(sum, big.NewInt(int64(grace)))
	sum.Add(sum, product(hops, int64(sc.Delta)))

	rate := million - int64(sc.MaxDrift()) // what the slowest clock counts in 10⁶ ns
	if rate <= 0 {
		return math.MaxInt64
	}
	// Euclidean division by a positive number rounds down; a remainder rounds the bound up.
	bound, rest := new(big.Int).DivMod(sum.Mul(sum, big.NewInt(million)), big.NewInt(rate), new(big.Int))
	if rest.Sign() != 0 {
		bound.Add(bound, big.NewInt(1))
	}
	switch {
	case bound.IsInt64():
		return time.Duration(bound.Int64())
	case bound.Sign() < 0:
		return math.MinInt64
	default:
		return math.MaxInt64
	}
}

// AssertsG2 reports whether the scenario keeps the timing's promise that no two members own a
// shard at once, so that a run of it may assert that. Three things break the promise:
//
//   - clocks that drift too far: with LD and RD the timing's LeaseDuration and RenewDeadline
//     and ρ the scenario's MaxDrift, the promise needs ρ·(LD + RD) < 10⁶·(LD − RD), which is
//     ρ strictly below the timing's DriftBound. The comparison is exact: its products cannot
//     overflow;
//   - members that pause: Faults has a pause rule whose Target is "" (any member) or
//     "members". A paused member cannot stop its runs at its deadline. A rule that pauses only
//     another role, such as "store" or "fence", breaks nothing;
//   - sessions that end out of band: EndAfter is positive. The store ends a session the
//     moment EndAfter has passed and may grant its shards at once, while the holder, which has
//     not yet seen the end, owns them until its deadline.
//
// The zero fields count at their defaults.
func (sc Scenario) AssertsG2() bool {
	sc = sc.withDefaults()
	ld, rd := big.NewInt(int64(sc.Timing.LeaseDuration)), big.NewInt(int64(sc.Timing.RenewDeadline))
	drifted := new(big.Int).Add(ld, rd)
	drifted.Mul(drifted, big.NewInt(int64(sc.MaxDrift())))
	margin := new(big.Int).Sub(ld, rd)
	margin.Mul(margin, big.NewInt(million))
	if drifted.Cmp(margin) >= 0 || sc.EndAfter > 0 {
		return false
	}
	if sc.Faults == nil {
		return true
	}
	for _, rule := range sc.Faults.Rules {
		if rule.Kind == fault.KindPause && (rule.Target == "" || rule.Target == roleMembers) {
			return false
		}
	}
	return true
}

// product returns a·b, which cannot overflow.
func product(a, b int64) *big.Int {
	p := big.NewInt(a)
	return p.Mul(p, big.NewInt(b))
}
