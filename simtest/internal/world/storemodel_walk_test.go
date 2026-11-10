// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// walkHolders are the holders of the walks: two incarnations of m1 and of m2, so that sessions
// replace each other, and a holder with no "/".
var walkHolders = []string{"m1/a", "m1/b", "m2/a", "m2/b", "m3/a", "m4"}

// walker drives one store model through a seeded walk of requests and expire calls, and checks
// after every call what must hold whatever the requests: the outbox's order, terms and versions,
// the holders records name, the sessions, the next end and the view.
type walker struct {
	t        *testing.T
	rule     Takeover
	endAfter time.Duration
	rng      *rand.Rand
	m        *storeModel
	chk      *check.Checker

	// at and wall are the readings of the next call.
	at   time.Duration
	wall time.Time
	// trace holds the text of every call, reply and effect, to compare two runs of a seed.
	trace []string

	// version is the version of the latest record write seen, terms the latest term of each
	// shard's record, view the latest view.
	version uint64
	terms   map[string]core.Term
	view    core.View
}

// TestStoreModelWalk runs seeded random walks under both rules, with and without EndAfter, and
// checks after every call that terms grow per shard (the g1 check passes), that every record
// write takes the next version and that nothing else does, that a grant goes to a live sender
// and that under the server rule it never takes a shard from a live session (the g4 check
// passes), that a record names only a holder that opened a session, that each member has at
// most one session that has not ended, that no session is past its end, that nextEnd is the
// earliest end, and that the view is the one its content gives, with a new version exactly when
// that content changes, as the last effect, in slices of the effect's own. It also checks each
// reply against the state before the call: lost exactly when the sender has no live session,
// the term from core.NextTerm of the deciding wall (the store's for acquire, the request's for
// cas), held with the sender's RetryPeriod, cas and casSlot granting exactly when the version
// is the expected one, confirm writing exactly under the observer rule at the record's term,
// renewals storing the timing and growing the version by one, getSession telling what the store
// had, closeSession leaving no record or slot naming the sender, a grant's evidence, and every
// session end's cause and time. Each walk runs twice and must give the same text.
func TestStoreModelWalk(t *testing.T) {
	seeds := uint64(200)
	if testing.Short() {
		seeds = 40
	}
	for _, rule := range []Takeover{Server, Observer} {
		for _, endAfter := range []time.Duration{0, 20 * time.Second} {
			t.Run(rule.String()+"/endAfter="+endAfter.String(), func(t *testing.T) {
				for seed := range seeds {
					first := walk(t, rule, endAfter, seed)
					if second := walk(t, rule, endAfter, seed); !slices.Equal(first, second) {
						t.Fatalf("seed %d: two runs differ", seed)
					}
				}
			})
		}
	}
}

// walk runs one walk of 200 calls and returns its trace.
func walk(t *testing.T, rule Takeover, endAfter time.Duration, seed uint64) []string {
	t.Helper()
	w := &walker{
		t: t, rule: rule, endAfter: endAfter,
		rng:   rand.New(rand.NewPCG(seed, uint64(rule))), //nolint:gosec // G404: a test's seeded walk.
		terms: make(map[string]core.Term),
		wall:  time.UnixMilli(1_000_000),
	}
	w.m = newStoreModel(rule, w.rng.IntN(3), endAfter)
	w.view = w.m.currentView()
	w.chk = check.New(check.Config{Now: func() kernel.Time { return kernel.Time(w.at) }})
	for range 200 {
		w.step()
		if w.t.Failed() {
			w.t.Fatalf("seed %d, rule %v, endAfter %v:\n%s", seed, rule, endAfter, strings.Join(w.trace, "\n"))
		}
	}
	return w.trace
}

// step makes one call and checks the model after it.
func (w *walker) step() {
	switch w.rng.IntN(8) {
	case 0, 1, 2:
		w.at += time.Duration(w.rng.IntN(4000)) * time.Millisecond
	case 3:
		w.at -= time.Duration(w.rng.IntN(1000)) * time.Millisecond // the store keeps its time
	case 4:
		// The node's timer: a reading at the next end, or a nanosecond before it.
		if at, ok := w.m.nextEnd(); ok {
			w.at = at - time.Duration(w.rng.IntN(2))
		}
	}
	w.wall = w.wall.Add(time.Duration(w.rng.IntN(3000)-1000) * time.Millisecond)

	pre := w.capture()
	req := w.request()
	var rep reply
	if req == nil {
		w.m.expire(w.at)
		w.trace = append(w.trace, "expire "+msgDuration(w.at))
	} else {
		rep = w.m.handle(w.at, w.wall, req)
		w.trace = append(w.trace, msgDuration(w.at)+" "+req.Describe()+" -> "+rep.Describe())
	}
	effects := w.m.takeEffects()
	for _, e := range effects {
		w.trace = append(w.trace, "\t"+effectText(e))
	}
	w.checkEffects(req, rep, effects)
	if req != nil {
		w.checkReply(pre, req, rep, effects)
	}
	w.checkEnds(pre, req, effects)
	w.checkSlots(pre.slots, pre.slotVersion)
	w.checkSessions()
	w.checkView(effects)
}

// state is what the walk keeps of the model before a call.
type state struct {
	sessions    map[string]storeSession
	slots       map[int]slotRecord
	slotVersion uint64
	slotCount   int
	records     map[string]tenure.Holding
}

// record returns the shard's record before the call, as storeModel.record does.
func (s state) record(space string, shard int) tenure.Holding {
	if rec, ok := s.records[msgShard(space, shard)]; ok {
		return rec
	}
	return tenure.Holding{Space: space, Shard: shard}
}

// capture returns a copy of the model's sessions, records and slots.
func (w *walker) capture() state {
	s := state{
		records:     make(map[string]tenure.Holding),
		sessions:    make(map[string]storeSession, len(w.m.sessions)),
		slots:       maps.Clone(w.m.slots),
		slotVersion: w.m.slotVersion,
		slotCount:   w.m.slotCount,
	}
	for h, sess := range w.m.sessions {
		s.sessions[h] = *sess
	}
	for _, rec := range w.m.allRecords() {
		s.records[msgShard(rec.Space, rec.Shard)] = rec
	}
	return s
}

// needsSession reports whether the request needs the sender's live session, which the store's
// clock judges under both rules.
func needsSession(req request) bool {
	switch req.(type) {
	case keepAliveReq, acquireReq, confirmReq, claimSlotReq, casReq, casSlotReq:
		return true
	}
	return false
}

// checkReply checks the reply of a request against the state before it: what each request
// must answer, and the evidence of a grant.
func (w *walker) checkReply(pre state, req request, rep reply, effects []effect) {
	w.t.Helper()
	sender := req.head().Holder
	live := !w.m.gone(sender)
	if _, lost := rep.(lostReply); lost == live && needsSession(req) {
		w.fail("%s: lost %v, sender live %v", rep.Describe(), lost, live)
	}
	if s := w.m.sessions[sender]; live && needsSession(req) && w.m.now >= s.renewed+s.timing.LeaseDuration {
		w.fail("%s: the sender's session is past its expiry %v", rep.Describe(), s.renewed+s.timing.LeaseDuration)
	}
	wrote := false
	var grant *grantEffect
	for _, e := range effects {
		switch e := e.(type) {
		case recordEffect:
			wrote = true
		case grantEffect:
			grant = &e
		}
	}
	switch r := req.(type) {
	case keepAliveReq:
		w.checkKeepAlive(pre, r, rep)
	case acquireReq:
		prev := pre.record(r.Space, r.Shard)
		switch rep := rep.(type) {
		case grantedReply:
			if want := core.NextTerm(prev.Term, w.wall); rep.Record.Term != want {
				w.fail("%s: term %d, want %d", rep.Describe(), rep.Record.Term, want)
			}
			w.checkEvidence(grant, prev, w.m.now-pre.sessions[prev.Holder].renewed)
		case heldReply:
			if rep.Record.Holder == sender || w.m.gone(rep.Record.Holder) {
				w.fail("%s: held by %s", rep.Describe(), rep.Record.Holder)
			}
			if rep.RetryAfter != w.m.sessions[sender].timing.RetryPeriod {
				w.fail("%s: retry, want the sender's %v", rep.Describe(), w.m.sessions[sender].timing.RetryPeriod)
			}
		}
	case confirmReq:
		prev := pre.record(r.Space, r.Shard)
		want := live && w.rule == Observer && prev.Version != 0 && prev.Holder == sender && prev.Term == r.Term
		if wrote != want {
			w.fail("%s: wrote %v, want %v", rep.Describe(), wrote, want)
		}
		if c, ok := rep.(confirmedReply); ok && c.Record != w.m.record(r.Space, r.Shard) {
			w.fail("%s: not the record", rep.Describe())
		}
	case closeSessionReq:
		for _, rec := range w.m.allRecords() {
			if rec.Holder == sender {
				w.fail("%s still names %s after its closeSession", msgRecord(rec), sender)
			}
		}
		for slot, rec := range w.m.slots {
			if rec.Holder == sender {
				w.fail("slot %d still names %s after its closeSession", slot, sender)
			}
		}
		if live {
			w.fail("%s is live after its closeSession", sender)
		}
	case casReq:
		prev := pre.record(r.Space, r.Shard)
		_, granted := rep.(grantedReply)
		_, stale := rep.(staleReply)
		if granted != (live && prev.Version == r.Expect) || stale != (live && prev.Version != r.Expect) {
			w.fail("%s: record version %d, expected %d", rep.Describe(), prev.Version, r.Expect)
		}
		if g, ok := rep.(grantedReply); ok {
			if want := core.NextTerm(prev.Term, r.Wall); g.Record.Term != want {
				w.fail("%s: term %d, want %d from the cas's wall", rep.Describe(), g.Record.Term, want)
			}
			w.checkEvidence(grant, prev, r.Unseen)
		}
	case casSlotReq:
		prev := pre.slots[r.Slot]
		_, claimed := rep.(slotClaimedReply)
		if want := live && r.Slot < pre.slotCount && prev.Version == r.Expect; claimed != want {
			w.fail("%s: claimed %v, want %v", rep.Describe(), claimed, want)
		}
	case getSessionReq:
		want := sessionInfo{Holder: r.Of, Ended: true}
		if s, ok := pre.sessions[r.Of]; ok {
			want = sessionInfo{Holder: r.Of, Version: s.version, Lease: s.timing.LeaseDuration, Ended: s.ended}
		}
		if got, ok := rep.(sessionReply); !ok || got.Session != want {
			w.fail("%s: want %+v", rep.Describe(), want)
		}
	}
}

// checkKeepAlive checks the reply of a keepAliveReq.
func (w *walker) checkKeepAlive(pre state, r keepAliveReq, rep reply) {
	w.t.Helper()
	s := w.m.sessions[r.Holder]
	old, known := pre.sessions[r.Holder]
	renewed, ok := rep.(renewedReply)
	switch {
	case !ok:
		return
	case renewed.Opened == known:
		w.fail("%s: opened %v for a holder known %v", rep.Describe(), renewed.Opened, known)
	case renewed.Opened && renewed.Version != 1, !renewed.Opened && renewed.Version != old.version+1:
		w.fail("%s: version after %d", rep.Describe(), old.version)
	case s.renewed != w.m.now || s.timing != r.Timing || s.version != renewed.Version:
		w.fail("%s: session %+v at %v", rep.Describe(), *s, w.m.now)
	}
}

// checkEvidence checks the grant effect against the record it wrote over and the unseen time
// the deciding clock had: Ended for a session that ended other than by the store's clock,
// Lease for the LeaseDuration of Prev's session.
func (w *walker) checkEvidence(g *grantEffect, prev tenure.Holding, unseen time.Duration) {
	w.t.Helper()
	if g == nil {
		w.fail("a grant with no grant effect")
		return
	}
	want := grantEffect{Record: g.Record, Prev: prev.Holder}
	if prev.Holder != "" {
		s := w.m.sessions[prev.Holder]
		want.Ended = s.ended && s.cause != causeExpired
		want.Unseen = unseen
		want.Lease = s.timing.LeaseDuration
	}
	if *g != want {
		w.fail("grant effect %+v, want %+v", *g, want)
	}
}

// checkEnds checks every session end of the call: the session had reached the earlier of its
// ends (its expiry when the store's clock judges it, its EndAfter end), the cause names that
// end, a sender's own expiry is late, and a replacement comes from a keepAlive that opened a
// session of the same member.
func (w *walker) checkEnds(pre state, req request, effects []effect) {
	w.t.Helper()
	sender := ""
	if req != nil {
		sender = req.head().Holder
	}
	for _, e := range effects {
		se, ok := e.(sessionEffect)
		if !ok || se.Op != sessionEnd {
			continue
		}
		s := pre.sessions[se.Holder]
		own := se.Holder == sender && needsSession(req)
		if se.Cause == causeReplaced {
			_, open := req.(keepAliveReq)
			if _, known := pre.sessions[sender]; !open || known || sessionMember(sender) != sessionMember(se.Holder) {
				w.fail("%s replaced at %v", se.Holder, req)
			}
			continue
		}
		byClock := own || w.rule == Server
		at, cause, found := s.renewed+s.timing.LeaseDuration, causeExpired, byClock
		if cut := s.opened + w.endAfter; w.endAfter > 0 && (!byClock || cut < at) {
			at, cause, found = cut, causeEndAfter, true
		}
		if own && cause == causeExpired {
			cause = causeLate
		}
		if !found || w.m.now < at || se.Cause != cause {
			w.fail("%s ended %s at %v; want %s at %v (found %v)", se.Holder, se.Cause, w.m.now, cause, at, found)
		}
	}
}

// request returns a random request, or nil for an expire call.
func (w *walker) request() request {
	head := reqHead{ID: uint64(len(w.trace)), Holder: walkHolders[w.rng.IntN(len(walkHolders))]}
	space, shard := []string{"book", "cart"}[w.rng.IntN(2)], w.rng.IntN(3)
	rec := w.m.record(space, shard)
	term := rec.Term
	if term == 0 || w.rng.IntN(4) == 0 {
		term = core.Term(w.rng.Uint64N(3) + 1)
	}
	expect := rec.Version
	if w.rng.IntN(3) == 0 {
		expect = w.rng.Uint64N(w.m.version + 1)
	}
	slot := w.rng.IntN(4)
	slotExpect := w.m.slot(slot).Version
	if w.rng.IntN(3) == 0 {
		slotExpect = w.rng.Uint64N(w.m.slotVersion + 1)
	}
	unseen := time.Duration(w.rng.IntN(20)) * time.Second
	timing := []core.Timing{core.DefaultTiming, timingShort, timingB}[w.rng.IntN(3)]

	switch w.rng.IntN(16) {
	case 0, 1, 2:
		return keepAliveReq{head, timing}
	case 3, 4:
		return acquireReq{head, space, shard}
	case 5:
		return releaseReq{head, space, shard, term}
	case 6:
		return confirmReq{head, space, shard, term}
	case 7:
		if w.rng.IntN(3) == 0 {
			return closeSessionReq{head}
		}
		return holdingsReq{head, space}
	case 8:
		return claimSlotReq{head}
	case 9:
		return releaseSlotReq{head, slot}
	case 10:
		return setSlotsReq{head, w.rng.IntN(4)}
	case 11:
		if w.rng.IntN(2) == 0 {
			return getShardReq{head, space, shard}
		}
		return getSlotReq{head, slot}
	case 12:
		return getSessionReq{head, walkHolders[w.rng.IntN(len(walkHolders))]}
	case 13, 14:
		// Members send cas and casSlot under the observer rule only.
		if w.rule == Server {
			return acquireReq{head, space, shard}
		}
		if w.rng.IntN(2) == 0 {
			return casSlotReq{head, slot, slotExpect, unseen}
		}
		return casReq{head, space, shard, expect, w.wall, unseen}
	}
	return nil
}

// fail reports a broken invariant.
func (w *walker) fail(format string, args ...any) {
	w.t.Helper()
	w.t.Errorf(format, args...)
}

// checkEffects checks the outbox of one call.
func (w *walker) checkEffects(req request, rep reply, effects []effect) {
	w.t.Helper()
	writes := 0
	for i, e := range effects {
		switch e := e.(type) {
		case viewEffect:
			if i != len(effects)-1 {
				w.fail("a view effect before the last effect")
			}
		case recordEffect:
			writes++
			key := msgShard(e.Record.Space, e.Record.Shard)
			if e.Record.Version != w.version+1 {
				w.fail("%s: version %d after %d", key, e.Record.Version, w.version)
			}
			w.version = e.Record.Version
			if e.Record.Term < w.terms[key] {
				w.fail("%s: term %d after %d", key, e.Record.Term, w.terms[key])
			}
			w.terms[key] = e.Record.Term
			if e.Record != w.m.record(e.Record.Space, e.Record.Shard) && !laterWrite(effects[i+1:], e.Record) {
				w.fail("%s: effect %s is not the record", key, msgRecord(e.Record))
			}
		case grantEffect:
			w.checkGrant(req, effects, i, e)
		}
	}
	if w.m.version != w.version {
		w.fail("store version %d, last write %d", w.m.version, w.version)
	}
	switch rep.(type) {
	case lostReply, heldReply, staleReply:
		if writes > 0 {
			w.fail("%s wrote %d records", rep.Describe(), writes)
		}
	}
	if _, ok := req.(confirmReq); ok && w.rule == Server && writes > 0 {
		w.fail("a confirm wrote under the server rule")
	}
	for _, rec := range w.m.allRecords() {
		if rec.Holder != "" && (w.m.sessions[rec.Holder] == nil || w.m.sessions[rec.Holder].version == 0) {
			w.fail("%s names %s, which never opened a session", msgRecord(rec), rec.Holder)
		}
	}
}

// laterWrite reports whether a later effect of the call writes the same shard's record again.
func laterWrite(effects []effect, rec tenure.Holding) bool {
	for _, e := range effects {
		if r, ok := e.(recordEffect); ok && r.Record.Space == rec.Space && r.Record.Shard == rec.Shard {
			return true
		}
	}
	return false
}

// checkGrant checks the grant effect effects[i] of req.
func (w *walker) checkGrant(req request, effects []effect, i int, g grantEffect) {
	w.t.Helper()
	if i+1 >= len(effects) || effects[i+1] != effect(recordEffect{g.Record}) {
		w.fail("grant %s not followed by its record", msgRecord(g.Record))
	}
	if g.Record.Holder != req.head().Holder || w.m.gone(g.Record.Holder) {
		w.fail("grant %s to %s, which is not the live sender", msgRecord(g.Record), g.Record.Holder)
	}
	if w.rule == Server && g.Prev != "" && g.Prev != g.Record.Holder && !w.m.gone(g.Prev) {
		w.fail("grant %s while %s is live", msgRecord(g.Record), g.Prev)
	}
	w.chk.Grant(check.Grant{
		Space: g.Record.Space, Shard: g.Record.Shard, Term: g.Record.Term, Holder: g.Record.Holder,
		Prev: g.Prev, Ended: g.Ended, Unseen: g.Unseen, Lease: g.Lease,
	})
	if err := w.chk.Err("g1"); err != nil {
		w.fail("%v", err)
	}
	// Under the observer rule the evidence is the acquirer's, which the walk draws at random.
	if err := w.chk.Err("g4"); err != nil && w.rule == Server {
		w.fail("%v", err)
	}
}

// checkSlots checks the slot records the call wrote: each takes a slot version larger than
// every earlier one, and names no holder or one that has a session that has not ended.
func (w *walker) checkSlots(before map[int]slotRecord, beforeVersion uint64) {
	w.t.Helper()
	seen := make(map[uint64]bool)
	for _, slot := range slices.Sorted(maps.Keys(w.m.slots)) {
		rec := w.m.slots[slot]
		if rec == before[slot] {
			continue
		}
		if rec.Version <= beforeVersion || rec.Version > w.m.slotVersion || seen[rec.Version] {
			w.fail("slot %d: version %d, earlier largest %d", slot, rec.Version, beforeVersion)
		}
		seen[rec.Version] = true
		if rec.Holder != "" && w.m.gone(rec.Holder) {
			w.fail("slot %d written for %s, which has no session", slot, rec.Holder)
		}
	}
	if len(w.m.slots) < len(before) {
		w.fail("a slot record was removed")
	}
}

// checkSessions checks the sessions, their ends and nextEnd.
func (w *walker) checkSessions() {
	w.t.Helper()
	var alive []string
	members := make(map[string]bool)
	var first time.Duration
	found := false
	for _, holder := range slices.Sorted(maps.Keys(w.m.sessions)) {
		s := w.m.sessions[holder]
		if s.ended {
			continue
		}
		alive = append(alive, holder)
		if id := sessionMember(holder); members[id] {
			w.fail("two sessions of %s have not ended", id)
		} else {
			members[id] = true
		}
		var ends []time.Duration
		if w.rule == Server {
			ends = append(ends, s.renewed+s.timing.LeaseDuration)
		}
		if w.endAfter > 0 {
			ends = append(ends, s.opened+w.endAfter)
		}
		if len(ends) == 0 {
			continue
		}
		end := slices.Min(ends)
		if end <= w.m.now {
			w.fail("%s has not ended at %v, its end %v", holder, w.m.now, end)
		}
		if !found || end < first {
			first, found = end, true
		}
	}
	if !slices.Equal(alive, w.m.alive) {
		w.fail("alive %q, want %q", w.m.alive, alive)
	}
	if at, ok := w.m.nextEnd(); ok != found || at != first {
		w.fail("nextEnd %v %v, want %v %v", at, ok, first, found)
	}
}

// checkView checks the view against its content, computed here on its own, and its version.
func (w *walker) checkView(effects []effect) {
	w.t.Helper()
	want := core.View{Slots: w.m.slotCount}
	var ids []string
	for _, holder := range w.m.alive {
		ids = append(ids, sessionMember(holder))
	}
	slices.Sort(ids)
	if w.m.slotCount > 0 {
		want.SlotHolders = make([]string, w.m.slotCount)
		for slot := range want.SlotHolders {
			if h := w.m.slots[slot].Holder; h != "" && !w.m.gone(h) {
				want.SlotHolders[slot] = h
			}
		}
	}
	for _, id := range ids {
		member := core.Member{ID: id, Slot: -1}
		for slot, h := range want.SlotHolders {
			if h != "" && sessionMember(h) == id {
				member.Slot = slot
				break
			}
		}
		want.Members = append(want.Members, member)
	}
	got := w.m.currentView()
	if err := got.Validate(); err != nil {
		w.fail("view: %v", err)
	}
	if !sameView(got, want) {
		w.fail("view %s, want %s", modelViewText(got), modelViewText(want))
	}
	var pushed *viewEffect
	for _, e := range effects {
		if v, ok := e.(viewEffect); ok {
			if pushed != nil {
				w.fail("two view effects in one call")
			}
			pushed = &v
		}
	}
	switch {
	case pushed == nil && (got.Version != w.view.Version || !sameView(got, w.view)):
		w.fail("view %s changed from %s with no view effect", modelViewText(got), modelViewText(w.view))
	case pushed != nil && (pushed.View.Version != w.view.Version+1 || sameView(pushed.View, w.view) ||
		!sameView(pushed.View, got) || pushed.View.Version != got.Version):
		w.fail("view effect %s after %s, the store's %s", modelViewText(pushed.View), modelViewText(w.view), modelViewText(got))
	case pushed != nil && !slices.Equal(pushed.Sessions, w.m.sessionList()):
		w.fail("view effect sessions %v, want %v", pushed.Sessions, w.m.sessionList())
	}
	if pushed != nil {
		for i := range pushed.View.Members {
			pushed.View.Members[i].ID = "scribbled"
		}
		for i := range pushed.View.SlotHolders {
			pushed.View.SlotHolders[i] = "scribbled"
		}
		if !sameView(w.m.currentView(), got) {
			w.fail("the view effect shares its slices with the store")
		}
	}
	w.view = got
	w.trace = append(w.trace, "\tview="+strconv.FormatUint(got.Version, 10))
}
