// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build ignore

// Package lintbad breaks the rules of the simulation harness. Each line that breaks a rule
// ends with a comment that names the rule, and the lines around it look alike and break none.
// The package is a fixture of the test of the rules: nothing builds it.
package lintbad

import (
	crand "crypto/rand" // want: import of crypto/rand
	"math/rand"         // want: import of math/rand
	randv2 "math/rand/v2"
	"sync"        // want: import of sync
	"sync/atomic" // want: import of sync/atomic
	"time"
)

// clock has a field and a method with the names of forbidden functions.
type clock struct {
	Now   time.Duration
	Sleep func(time.Duration)
}

// Since is a method, not time.Since.
func (c clock) Since(d time.Duration) time.Duration { return c.Now - d }

var (
	mu      sync.Mutex
	counter atomic.Int64
)

// allowed uses time's types and constants, the constructors and types of math/rand/v2, and
// the methods of a generator.
func allowed(r *randv2.Rand, src randv2.Source) time.Duration {
	var (
		pcg    *randv2.PCG     = randv2.NewPCG(1, 2)
		chacha *randv2.ChaCha8 = randv2.NewChaCha8([32]byte{})
		zipf   *randv2.Zipf    = randv2.NewZipf(r, 2, 1, 10)
	)
	seeded := randv2.New(pcg)
	n := r.IntN(10) + seeded.IntN(10) + randv2.New(chacha).IntN(10) + randv2.New(src).IntN(10)
	seeded.Shuffle(n, func(i, j int) {})
	c := clock{Now: time.Second + time.Duration(zipf.Uint64())}
	c.Sleep = func(time.Duration) {}
	c.Sleep(time.Millisecond)
	return c.Since(time.Duration(n) * time.Millisecond).Round(time.Microsecond)
}

// forbidden breaks each rule.
func forbidden(buf []byte) {
	go allowed(nil, nil) // want: go statement
	done := make(chan struct{})
	go func() { // want: go statement
		close(done)
	}()
	<-done

	start := time.Now()                 // want: use of time.Now
	_ = time.Since(start)               // want: use of time.Since
	_ = time.Until(start)               // want: use of time.Until
	<-time.After(time.Second)           // want: use of time.After
	time.Sleep(time.Second)             // want: use of time.Sleep
	timer := time.NewTimer(time.Second) // want: use of time.NewTimer
	timer.Stop()
	ticker := time.NewTicker(time.Second) // want: use of time.NewTicker
	ticker.Stop()
	<-time.Tick(time.Second)               // want: use of time.Tick
	time.AfterFunc(time.Second, func() {}) // want: use of time.AfterFunc
	// The function as a value reaches the wall clock as a call does.
	now := time.Now // want: use of time.Now
	_ = now().Add(time.Second)

	_ = randv2.IntN(10)                       // want: use of math/rand/v2.IntN
	_ = randv2.N[int64](10)                   // want: use of math/rand/v2.N
	randv2.Shuffle(len(buf), func(i, j int) { // want: use of math/rand/v2.Shuffle
		buf[i], buf[j] = buf[j], buf[i]
	})
	draw := randv2.Uint64 // want: use of math/rand/v2.Uint64
	_ = draw()

	// The imports of these two packages are the findings, not their uses.
	_ = rand.Intn(3)
	_, _ = crand.Read(buf)
	mu.Lock()
	counter.Add(1)
	mu.Unlock()
}
