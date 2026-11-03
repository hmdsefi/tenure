// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"math"
	"time"
)

// Term numbers one acquisition of a shard. A store grants a new, larger term on every
// acquisition and never on renewal. Term 0 is never granted and means no term.
type Term uint64

// NextTerm returns the term a store grants on an acquisition: the larger of prev+1 and wall
// in Unix milliseconds, where a time before 1970 counts as 0. prev is the shard record's
// previous term, or 0 when the store has no record. NextTerm panics if prev is the largest
// Term.
func NextTerm(prev Term, wall time.Time) Term {
	if prev == math.MaxUint64 {
		panic(errors.New("core: NextTerm: term overflow"))
	}
	next := prev + 1
	if ms := wall.UnixMilli(); ms > 0 && Term(ms) > next {
		return Term(ms)
	}
	return next
}
