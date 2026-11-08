// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// The Race cases check that a store stays consistent under concurrent calls: of racing Acquires
// by different holders at most one is granted, a Release or CloseSession racing an Acquire ends
// in one of the two orders, and the terms of a shard grow however its grants interleave. Each
// race starts its calls together, every goroutine blocking on one channel the case closes.

// raceRounds is how many races the round-based cases run.
const raceRounds = 20

// raceTwoAcquirers: two holders, one per client, race to acquire each of twenty shards, all
// forty calls at once, round after round, the winners releasing their shards between rounds: of
// each shard's two Acquires exactly one is granted a term, and the other is refused with a
// *tenure.HeldError naming the winner.
func raceTwoAcquirers(e *env) {
	const shards = 20
	h1, h2 := holder(1, 1), holder(2, 1)
	won := make([]string, shards)
	terms := make([]core.Term, shards)
	for round := range raceRounds / 2 {
		e.open(h1)
		e.open(h2)
		for shard, w := range won {
			if w != "" {
				e.release(w, "a", shard, terms[shard])
			}
		}
		for shard := range shards {
			e.rec(h1, "a", shard, "", terms[shard])
			e.rec(h2, "a", shard, "", terms[shard])
		}

		hs := []string{h1, h2}
		if round%2 == 1 {
			hs = []string{h2, h1}
		}
		res := make([][]acqResult, shards)
		var calls []func()
		for shard := range shards {
			res[shard] = make([]acqResult, len(hs))
			for i, h := range hs {
				c := e.client(h)
				calls = append(calls, func() {
					term, err := c.Acquire(e.ctx, h, "a", shard)
					res[shard][i] = acqResult{h, term, err}
				})
			}
		}
		together(calls...)
		for shard := range shards {
			won[shard], terms[shard] = e.oneWinner("a", shard, res[shard])
		}
		for shard := range shards {
			e.rec(h1, "a", shard, won[shard], terms[shard])
			e.rec(h2, "a", shard, won[shard], terms[shard])
		}
	}
}

// raceManyAcquirers: eight holders race to acquire one shard, round after round, the winner
// releasing it between rounds: each round exactly one is granted, at a larger term than the
// round before, and every other is refused with a *tenure.HeldError naming the winner.
func raceManyAcquirers(e *env) {
	hs := make([]string, 8)
	for i := range hs {
		hs[i] = holder(i+1, 1)
	}
	var (
		w    string
		term core.Term
	)
	for round := range raceRounds / 2 {
		for _, h := range hs {
			e.open(h)
		}
		if w != "" {
			e.release(w, "a", 0, term)
		}
		for _, h := range hs {
			e.rec(h, "a", 0, "", term)
		}
		order := slices.Concat(hs[round%len(hs):], hs[:round%len(hs)])
		w, term = e.oneWinner("a", 0, e.raceAcquire("a", 0, order...))
		e.rec(hs[0], "a", 0, w, term)
		e.rec(hs[1], "a", 0, w, term)
	}
}

// raceReleaseAcquire: a holder releasing its shard races another holder acquiring it, round
// after round with the two in turn: either the Acquire comes first and is refused with a
// *tenure.HeldError naming the releaser, leaving the record with no holder once the Release
// lands, or it comes second and is granted a term above the released one. Either way the record
// ends naming at most the one holder.
func raceReleaseAcquire(e *env) {
	h1, h2 := holder(1, 1), holder(2, 1)
	e.open(h1)
	e.open(h2)
	owner, other := h1, h2
	term := e.acq(owner, "a", 0)
	for range raceRounds {
		e.open(h1)
		e.open(h2)
		e.rec(owner, "a", 0, owner, term)
		e.rec(other, "a", 0, owner, term)

		var relErr error
		var got acqResult
		oc, ac := e.client(owner), e.client(other)
		together(
			func() { relErr = oc.Release(e.ctx, owner, "a", 0, term) },
			func() {
				t, err := ac.Acquire(e.ctx, other, "a", 0)
				got = acqResult{other, t, err}
			},
		)
		if relErr != nil {
			e.t.Fatalf("Release(%q, %q, %d, %d) = %s, want nil", owner, "a", 0, term, errText(relErr))
		}
		if got.err == nil {
			if got.term <= term {
				e.t.Fatalf("Acquire(%q, %q, %d) = %d, nil, want a term above %d, nil", other, "a", 0,
					got.term, term)
			}
			term = got.term
			e.terms[shardKey{"a", 0}] = term
			e.rec(owner, "a", 0, other, term)
			e.rec(other, "a", 0, other, term)
		} else {
			e.refusedBy(got, "a", 0, owner)
			e.rec(owner, "a", 0, "", term)
			e.rec(other, "a", 0, "", term)
			term = e.acq(other, "a", 0)
		}
		owner, other = other, owner
	}
}

// raceCloseAcquire: CloseSession of a holder races another holder acquiring the closed holder's
// shard, round after round with a new incarnation of the closed holder's member: either the
// Acquire comes first and is refused with a *tenure.HeldError naming the closed holder, leaving
// the record with no holder at its term, or it comes second and is granted a term above the
// closed holder's. Either way the closed holder's session has ended.
func raceCloseAcquire(e *env) {
	h2 := holder(2, 1)
	for round := range raceRounds {
		h1 := holder(1, round+1)
		shard := round
		e.open(h1)
		e.open(h2)
		t1 := e.acq(h1, "a", shard)
		e.rec(h2, "a", shard, h1, t1)

		var closeErr error
		var got acqResult
		cc, ac := e.client(h1), e.client(h2)
		together(
			func() { closeErr = cc.CloseSession(e.ctx, h1) },
			func() {
				t, err := ac.Acquire(e.ctx, h2, "a", shard)
				got = acqResult{h2, t, err}
			},
		)
		if closeErr != nil {
			e.t.Fatalf("CloseSession(%q) = %s, want nil", h1, errText(closeErr))
		}
		e.lost(h1)
		if got.err == nil {
			if got.term <= t1 {
				e.t.Fatalf("Acquire(%q, %q, %d) = %d, nil, want a term above %d, nil", h2, "a", shard,
					got.term, t1)
			}
			e.terms[shardKey{"a", shard}] = got.term
			e.rec(h1, "a", shard, h2, got.term)
			e.rec(h2, "a", shard, h2, got.term)
			continue
		}
		e.refusedBy(got, "a", shard, h1)
		e.rec(h1, "a", shard, "", t1)
		e.rec(h2, "a", shard, "", t1)
		e.acq(h2, "a", shard)
	}
}

// raceTerms: four holders, two per client, each renew their session, acquire one of three shards
// and release it again, over and over, all at once: no two grants of a shard share a term, a
// grant that starts after another of the same shard has returned has the larger term, and every
// record ends with no holder at the largest term granted.
func raceTerms(e *env) {
	const holders, shards, cycles = 4, 3, 25
	hs := make([]string, holders)
	for i := range hs {
		hs[i] = holder(i+1, 1)
		e.open(hs[i])
	}

	// grant is one granted Acquire, with the times it was called and returned.
	type grant struct {
		holder     string
		shard      int
		term       core.Term
		start, end time.Time
	}
	grants := make([][]grant, holders)
	fails := make([]string, holders)
	calls := make([]func(), holders)
	for i, h := range hs {
		c, tm := e.client(h), e.timingOf(h)
		calls[i] = func() {
			for n := range cycles {
				if err := c.KeepAlive(e.ctx, h, tm); err != nil {
					fails[i] = fmt.Sprintf("KeepAlive(%q, %v) = %s, want nil", h, tm, errText(err))
					return
				}
				shard := (i + n) % shards
				start := time.Now()
				term, err := c.Acquire(e.ctx, h, "a", shard)
				end := time.Now()
				var he *tenure.HeldError
				switch {
				case err == nil && term != 0:
					grants[i] = append(grants[i], grant{h, shard, term, start, end})
					if err := c.Release(e.ctx, h, "a", shard, term); err != nil {
						fails[i] = fmt.Sprintf("Release(%q, %q, %d, %d) = %s, want nil", h, "a", shard,
							term, errText(err))
						return
					}
				case term != 0 || !errors.As(err, &he):
					fails[i] = fmt.Sprintf("Acquire(%q, %q, %d) = %d, %s, want a term, nil or 0, a *tenure.HeldError",
						h, "a", shard, term, errText(err))
					return
				}
			}
		}
	}
	together(calls...)
	for _, msg := range fails {
		if msg != "" {
			e.t.Fatal(msg)
		}
	}

	top := make([]core.Term, shards)
	var all []grant
	for _, gs := range grants {
		all = append(all, gs...)
	}
	for i, a := range all {
		top[a.shard] = max(top[a.shard], a.term)
		for _, b := range all[i+1:] {
			if a.shard != b.shard {
				continue
			}
			if a.term == b.term {
				e.t.Fatalf("Acquire(%q, %q, %d) = %d, nil and Acquire(%q, %q, %d) = %d, nil, want two different terms",
					a.holder, "a", a.shard, a.term, b.holder, "a", b.shard, b.term)
			}
			first, second := a, b
			if b.end.Before(a.start) {
				first, second = b, a
			} else if !a.end.Before(b.start) {
				continue
			}
			if second.term < first.term {
				e.t.Fatalf("Acquire(%q, %q, %d) = %d, nil after Acquire(%q, %q, %d) = %d, nil, want a term above %d, nil",
					second.holder, "a", second.shard, second.term, first.holder, "a", first.shard,
					first.term, first.term)
			}
		}
	}
	for shard, t := range top {
		e.terms[shardKey{"a", shard}] = t
		e.rec(hs[0], "a", shard, "", t)
		e.rec(hs[1], "a", shard, "", t)
	}
}

// together runs each call in a goroutine of its own and returns once every call has returned.
// The calls start together: each goroutine blocks on one channel, which together closes once
// every goroutine is waiting on it.
func together(calls ...func()) {
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(calls))
	for _, call := range calls {
		done.Go(func() {
			ready.Done()
			<-start
			call()
		})
	}
	ready.Wait()
	close(start)
	done.Wait()
}

// acqResult is what one racing Acquire returned.
type acqResult struct {
	holder string
	term   core.Term
	err    error
}

// raceAcquire has each holder in hs call Acquire of the shard at once, through its own client,
// and returns the results in the order of hs.
func (e *env) raceAcquire(space string, shard int, hs ...string) []acqResult {
	res := make([]acqResult, len(hs))
	calls := make([]func(), len(hs))
	for i, h := range hs {
		c := e.client(h)
		calls[i] = func() {
			term, err := c.Acquire(e.ctx, h, space, shard)
			res[i] = acqResult{h, term, err}
		}
	}
	together(calls...)
	return res
}

// oneWinner checks that of the racing Acquires of the shard in res exactly one was granted, at a
// term above every term the case has seen for the shard, and that every other was refused with a
// *tenure.HeldError naming the winner. It returns the winner and its term.
func (e *env) oneWinner(space string, shard int, res []acqResult) (string, core.Term) {
	e.t.Helper()
	won := -1
	for i, r := range res {
		if r.err != nil {
			continue
		}
		if won >= 0 {
			e.t.Fatalf("%s, want one term and %d *tenure.HeldError", fmtAcqs(space, shard, res), len(res)-1)
		}
		won = i
	}
	if won < 0 {
		e.t.Fatalf("%s, want one term and %d *tenure.HeldError", fmtAcqs(space, shard, res), len(res)-1)
	}
	w := res[won]
	key := shardKey{space, shard}
	if prev := e.terms[key]; w.term == 0 || w.term <= prev {
		want := "a term"
		if prev != 0 {
			want = fmt.Sprintf("a term above %d", prev)
		}
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, nil, want %s, nil", w.holder, space, shard, w.term, want)
	}
	for i, r := range res {
		if i != won {
			e.refusedBy(r, space, shard, w.holder)
		}
	}
	e.terms[key] = w.term
	return w.holder, w.term
}

// refusedBy checks that r, an Acquire of the shard, returned 0 and a *tenure.HeldError naming by,
// with the RetryPeriod of the acquirer's Timing as RetryAfter.
func (e *env) refusedBy(r acqResult, space string, shard int, by string) {
	e.t.Helper()
	want := &tenure.HeldError{Holder: by, RetryAfter: e.timingOf(r.holder).RetryPeriod}
	var he *tenure.HeldError
	if r.term != 0 || !errors.As(r.err, &he) || *he != *want {
		e.t.Fatalf("Acquire(%q, %q, %d) = %d, %s, want 0, %s", r.holder, space, shard, r.term,
			errText(r.err), want)
	}
}

// fmtAcqs formats racing Acquires of the shard and their results.
func fmtAcqs(space string, shard int, res []acqResult) string {
	parts := make([]string, len(res))
	for i, r := range res {
		parts[i] = fmt.Sprintf("Acquire(%q, %q, %d) = %d, %s", r.holder, space, shard, r.term, errText(r.err))
	}
	return strings.Join(parts, "; ")
}
