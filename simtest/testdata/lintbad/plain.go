// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build ignore

package lintbad

import (
	"math/rand/v2"
	"time"
)

// plain imports math/rand/v2 under its own name, rand, and uses every function of the package
// that draws from the source no seed controls.
func plain(r *rand.Rand) {
	_ = rand.ExpFloat64()              // want: use of math/rand/v2.ExpFloat64
	_ = rand.Float32()                 // want: use of math/rand/v2.Float32
	_ = rand.Float64()                 // want: use of math/rand/v2.Float64
	_ = rand.Int()                     // want: use of math/rand/v2.Int
	_ = rand.Int32()                   // want: use of math/rand/v2.Int32
	_ = rand.Int32N(10)                // want: use of math/rand/v2.Int32N
	_ = rand.Int64()                   // want: use of math/rand/v2.Int64
	_ = rand.Int64N(10)                // want: use of math/rand/v2.Int64N
	_ = rand.IntN(10)                  // want: use of math/rand/v2.IntN
	_ = rand.N(time.Second)            // want: use of math/rand/v2.N
	_ = rand.NormFloat64()             // want: use of math/rand/v2.NormFloat64
	_ = rand.Perm(10)                  // want: use of math/rand/v2.Perm
	rand.Shuffle(0, func(i, j int) {}) // want: use of math/rand/v2.Shuffle
	_ = rand.Uint()                    // want: use of math/rand/v2.Uint
	_ = rand.Uint32()                  // want: use of math/rand/v2.Uint32
	_ = rand.Uint32N(10)               // want: use of math/rand/v2.Uint32N
	_ = rand.Uint64()                  // want: use of math/rand/v2.Uint64
	_ = rand.Uint64N(10)               // want: use of math/rand/v2.Uint64N
	_ = rand.UintN(10)                 // want: use of math/rand/v2.UintN

	// A generator's methods break nothing, called, as method values or as method expressions.
	seeded := rand.New(rand.NewPCG(1, 2))
	draw, intN := r.Uint64, (*rand.Rand).IntN
	_ = seeded.IntN(10) + intN(r, 10) + int(draw())
}

// nested breaks the rules inside closures, inside a goroutine and in the operand of a defer.
func nested() {
	run := func() {
		go func() { // want: go statement
			time.Sleep(time.Second) // want: use of time.Sleep
			func() {
				go nested() // want: go statement
			}()
		}()
	}
	run()
	defer time.Sleep(time.Second)          // want: use of time.Sleep
	clocks := []func() time.Time{time.Now} // want: use of time.Now
	_ = (time.Now)().Add(time.Second)      // want: use of time.Now
	_ = clocks
}
