// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// ErrInvalidAssigner is wrapped by the errors NewAssigner returns, except a timing error,
// which wraps ErrInvalidTiming.
var ErrInvalidAssigner = errors.New("core: invalid assigner")

// AssignerSpace names one space an Assigner places, with its shard count.
//
// Write AssignerSpace values with keyed fields: later versions add fields.
type AssignerSpace struct {
	// Name identifies the space. It is non-empty and distinct within an Assigner.
	Name string

	// Shards is the number of shards in the space, at least 1.
	Shards int
}

// Assigner computes the assignment of a process that follows a group's views without being a
// member that holds a slot, such as a router: for each space, the placement key of every shard,
// as a Machine of the same group that holds no slot and does not drain computes it.
//
// The rules are the machine's. A view is ignored when it is not valid or its version is older
// than the applied one; a view of the same version applies. Eligibility is judged at the view's
// wall-clock time advanced by the readings since the view was applied. A slot that the applied
// views have shown vacant for Timing.SlotGrace, by the Assigner's readings, drops out of the
// placement keys until a view shows it held. The assignment is recomputed when a view is
// applied and when a slot's vacancy expires or ends, and at no other reading: a member's JoinAt
// or LeaveAt that passes between two such readings takes effect at the next one, as on a
// member.
//
// An Assigner reads no clock and starts nothing: the caller feeds it views and readings, which
// never go backwards. It is not safe for concurrent use.
type Assigner struct {
	// spaces is a private copy of the spaces sorted by name, and assignments[i] the assignment of
	// spaces[i].
	spaces      []AssignerSpace
	assignments [][]string

	// timing is the validated timing.
	timing Timing

	// now is the latest reading.
	now time.Duration

	// view is the applied view, the zero View before the first, wall is its wall-clock time
	// without a monotonic reading, and appliedAt is the reading at which it was applied.
	view      View
	wall      time.Time
	appliedAt time.Duration

	// vacancy holds the vacancy streak of each slot of the applied view (MCH-310).
	vacancy []vacancy

	// keys holds the placement keys of the last computation, nil for none.
	keys []string

	// dirty reports that the assignments no longer match the applied view and the vacancies.
	dirty bool
}

// NewAssigner returns an Assigner for spaces, with no view applied, every shard's key "" and
// its clock at reading 0. Every space has a non-empty, distinct Name and at least one shard.
// The zero Timing means DefaultTiming. NewAssigner returns an error wrapping
// ErrInvalidAssigner for a bad space, and the error of Timing.Validate, unchanged, for a bad
// timing. It copies spaces and does not modify it.
func NewAssigner(spaces []AssignerSpace, timing Timing) (*Assigner, error) {
	seen := make(map[string]bool, len(spaces))
	for i, s := range spaces {
		switch {
		case s.Name == "":
			return nil, fmt.Errorf("%w: spaces[%d]: empty Name", ErrInvalidAssigner, i)
		case s.Shards < 1:
			return nil, fmt.Errorf("%w: space %q: Shards %d is below 1", ErrInvalidAssigner, s.Name, s.Shards)
		case seen[s.Name]:
			return nil, fmt.Errorf("%w: duplicate space %q", ErrInvalidAssigner, s.Name)
		}
		seen[s.Name] = true
	}
	if timing == (Timing{}) {
		timing = DefaultTiming
	}
	if err := timing.Validate(); err != nil {
		return nil, err
	}
	a := &Assigner{spaces: slices.Clone(spaces), timing: timing}
	slices.SortFunc(a.spaces, func(x, y AssignerSpace) int { return cmp.Compare(x.Name, y.Name) }) //tenure:totalorder names are distinct
	a.assignments = make([][]string, len(a.spaces))
	for i, s := range a.spaces {
		a.assignments[i] = make([]string, s.Shards)
	}
	return a, nil
}

// ApplyView applies v, read at the monotonic reading reading and at the wall-clock time wall,
// and reports whether the assignment of any space changed. A view that is not valid, or whose
// version is older than the applied one, changes nothing but the clock. ApplyView panics if
// reading is before an earlier reading.
func (a *Assigner) ApplyView(v View, reading time.Duration, wall time.Time) bool {
	a.advance(reading)
	if v.Version >= a.view.Version && v.Validate() == nil {
		a.view = v
		a.wall, a.appliedAt = wall.Round(0), a.now
		a.dirty = true
		a.trackVacancy()
	}
	return a.step()
}

// Tick moves the clock to reading and reports whether the assignment of any space changed:
// a slot's vacancy may have expired. Tick panics if reading is before an earlier reading.
func (a *Assigner) Tick(reading time.Duration) bool {
	a.advance(reading)
	return a.step()
}

// NextWake returns the earliest reading at which Tick can change an assignment, and true, or
// 0 and false when no later reading can: the next expiry of a slot's vacancy.
func (a *Assigner) NextWake() (time.Duration, bool) {
	var at time.Duration
	armed := false
	for _, vac := range a.vacancy {
		if vac.streak && !vac.expired {
			if t := vac.since + a.timing.SlotGrace; !armed || t < at {
				at, armed = t, true
			}
		}
	}
	return at, armed
}

// Assignment returns the placement key of each shard of the named space, indexed by shard
// number, or nil when the Assigner has no such space. The key is "" for a shard with no key, so
// every key is "" before the first view. Callers must not modify the result. A changed
// assignment is stored in a new slice, so an earlier result never changes.
func (a *Assigner) Assignment(space string) []string {
	i, ok := slices.BinarySearchFunc(a.spaces, space, func(s AssignerSpace, name string) int {
		return cmp.Compare(s.Name, name)
	})
	if !ok {
		return nil
	}
	return a.assignments[i]
}

// Keys returns the placement keys of the current assignments, as PlacementKeys returns them,
// or nil when there are none. Callers must not modify the result. Changed keys are stored in a
// new slice, so an earlier result never changes.
func (a *Assigner) Keys() []string {
	return a.keys
}

// advance moves the clock to reading, at most maxReading, so that every expiry fits in a
// Duration. It panics if reading is before the clock.
func (a *Assigner) advance(reading time.Duration) {
	reading = min(reading, maxReading)
	if reading < a.now {
		panic(fmt.Errorf("core: Assigner: reading %v is before %v", reading, a.now))
	}
	a.now = reading
}

// step expires the vacancies that are due and recomputes the assignments if anything changed
// them, as phases B and C of a machine's step do. It reports whether an assignment changed.
func (a *Assigner) step() bool {
	for s := range a.vacancy {
		vac := &a.vacancy[s]
		if vac.streak && !vac.expired && a.now >= vac.since+a.timing.SlotGrace {
			vac.expired = true
			a.dirty = true
		}
	}
	if !a.dirty {
		return false
	}
	a.dirty = false
	keys, _ := PlacementKeys(a.view, a.wall.Add(a.now-a.appliedAt), a.expired())
	if slices.Equal(keys, a.keys) {
		return false
	}
	a.keys = keys
	changed := false
	for i, s := range a.spaces {
		next := make([]string, s.Shards)
		for shard, k := range Balanced(s.Name, s.Shards, keys) {
			if k >= 0 {
				next[shard] = keys[k]
			}
		}
		if !slices.Equal(next, a.assignments[i]) {
			a.assignments[i] = next
			changed = true
		}
	}
	return changed
}

// trackVacancy applies the view just applied to the vacancy streaks (MCH-310), as a machine
// that holds no slot does: a slot the view shows vacant starts a streak at the view's reading
// unless it has one, a slot the view shows held ends its streak, and slots the view does not
// count lose theirs. Ending an expired streak recomputes the assignments.
func (a *Assigner) trackVacancy() {
	v := &a.view
	for s := v.Slots; s < len(a.vacancy); s++ {
		a.endVacancy(s)
	}
	a.vacancy = a.vacancy[:min(len(a.vacancy), v.Slots)]
	for len(a.vacancy) < v.Slots {
		a.vacancy = append(a.vacancy, vacancy{})
	}
	for s, holder := range v.SlotHolders {
		switch {
		case holder != "":
			a.endVacancy(s)
		case !a.vacancy[s].streak:
			a.vacancy[s] = vacancy{streak: true, since: a.appliedAt}
		}
	}
}

// endVacancy ends the vacancy streak of slot s, if it has one.
func (a *Assigner) endVacancy(s int) {
	if a.vacancy[s].expired {
		a.dirty = true
	}
	a.vacancy[s] = vacancy{}
}

// expired returns the expiry marks of the applied view's slots as PlacementKeys takes them: nil
// for a view without slots, and otherwise one entry per slot.
func (a *Assigner) expired() []bool {
	if a.view.Slots == 0 {
		return nil
	}
	out := make([]bool, a.view.Slots)
	for s := range out {
		out[s] = a.vacancy[s].expired
	}
	return out
}
