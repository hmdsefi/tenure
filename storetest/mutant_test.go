// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package storetest_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/storetest"
)

// mutants is the mutant matrix (STO-850): each defective store, by name, with the cases its
// defect must make fail. "none" is the baseline with no defect, which must pass every case.
var mutants = []struct {
	name   string
	defect string
	fails  []string
}{
	{"none", "no defect", nil},
	{"racy-acquire", "Acquire reads and writes the record in two lock sections", []string{"Race/TwoAcquirers"}},
	{"expire-strict", "a session expires at > instead of >= its lease", []string{"Session/ExpiryBoundary"}},
	{"version-restart", "versions count per record, restarting at 1", []string{"Holdings/Version"}},
	{"acquirer-lease", "expiry is judged by the acquirer's lease", []string{"Session/HolderLease"}},
	{"renew-ignored", "KeepAlive does not renew a live session", []string{"Session/RenewalKeepsRecords"}},
	{"late-renewal", "KeepAlive renews an expired session", []string{"Session/LateRenewal"}},
	{"no-replace", "a new incarnation leaves the old one live", []string{"Session/Replace"}},
	{"reopen", "KeepAlive reopens an ended session", []string{"Session/ReplacedForGood"}},
	{"retry-holder", "RetryAfter comes from the holder's Timing", []string{"Session/RetryAfter"}},
	{"close-keeps", "CloseSession leaves the holder's records", []string{"Session/Close"}},
	{"close-unknown", "CloseSession of an unknown holder does not block it", []string{"Session/CloseUnknown"}},
	{"term-wall-only", "a term is the wall time in ms, ignoring the previous term", []string{"Term/WallBehind"}},
	{"term-prev-only", "a term is the previous term + 1, ignoring the wall", []string{"Term/WallAhead"}},
	{"no-regrant", "Acquire refuses the holder's own re-grant", []string{"Acquire/Reacquire"}},
	{"release-any-term", "Release ignores the term", []string{"Release/WrongTerm"}},
	{"release-any-holder", "Release ignores the holder", []string{"Release/WrongHolder"}},
	{"release-zero-term", "clearing a record also zeroes its term", []string{"Release/Match"}},
	{"close-all", "CloseSession also clears other holders' records", []string{"CloseSession/Stale"}},
	{"holdings-unsorted", "Holdings returns records in reverse shard order", []string{"Holdings/Order"}},
	{"confirm-term", "Confirm reports the caller's term instead of the record's", []string{"Confirm/AfterTakeover"}},
	{"confirm-no-session", "Confirm does not check the session", []string{"Confirm/NoSession"}},
	{"slot-highest", "ClaimSlot claims the highest free slot", []string{"Slots/Claim"}},
	{"slot-sticky", "ClaimSlot never reuses a dead holder's slot", []string{"Slots/Expired"}},
	{"slot-retired-free", "a retired slot does not block ClaimSlot", []string{"Slots/Retired"}},
	{"watch-drop", "every second watch value is dropped", []string{"Watch/Order"}},
	{"watch-no-close", "a watch channel stays open when its context ends", []string{"Watch/Cancel"}},
	{"watch-one", "only the first watch channel receives", []string{"Watch/Multiple"}},
	{"ctx-ignored", "every method ignores a done context", []string{"Context/Canceled"}},
	{"args-unchecked", "a negative shard is accepted", []string{"Args/Invalid"}},
}

// mutantFailLine is the form of every log line of a mutant run (STO-820).
var mutantFailLine = regexp.MustCompile(`^\s+\S+\.go:\d+: \w+\(.*\) = .*, want .*$`)

// coverLine matches the line a child test binary built with -cover prints after its results.
var coverLine = regexp.MustCompile(`^coverage: (\d+\.\d+% of statements|\[no statements\])`)

// TestMutant runs the suite on the virtual fixture over each mutant. It is child-only: every
// mutant but none fails.
func TestMutant(t *testing.T) {
	skipUnlessChild(t)
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			storetest.Run(t, func(*testing.T) storetest.Fixture {
				clock := newTestClock()
				st := newMutant(clock, m.name)
				return storetest.Fixture{
					Store:        st,
					Peer:         func() tenure.Store { return st },
					Timing:       core.DefaultTiming,
					Advance:      clock.advance,
					SetWall:      clock.setWall,
					DeleteRecord: st.DeleteRecord,
				}
			})
		})
	}
}

// TestMutants: AT-STO-39 and the mutant parts of the acceptance tests the matrix covers (steps
// written from the text of issue #41). Each mutant's run of the suite fails, with each of its
// cases among the failures and every log line of the form "<Method>(<args>) = <got>, want
// <want>"; the none baseline passes every case.
func TestMutants(t *testing.T) {
	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			parent := "TestMutant/" + m.name
			r := runChild(t, "^TestMutant$/^"+regexp.QuoteMeta(m.name)+"$")
			if strings.Contains(r.out, "DATA RACE") || strings.Contains(r.out, "panic:") {
				t.Fatalf("child %s raced or panicked:\n%s", parent, r.out)
			}
			if got := r.under(parent); !slices.Equal(got, caseNames) {
				t.Fatalf("child %s started %q, want %q\n%s", parent, got, caseNames, r.out)
			}
			if m.name == "none" {
				if r.failed {
					t.Errorf("child %s failed, want it to pass:\n%s", parent, r.out)
				}
				for _, name := range caseNames {
					if got := r.results[parent+"/"+name]; got != "PASS" {
						t.Errorf("%s: %s, want PASS", name, got)
					}
				}
				return
			}
			if !r.failed {
				t.Errorf("child %s passed, want it to fail (%s)", parent, m.defect)
			}
			for _, name := range m.fails {
				if got := r.results[parent+"/"+name]; got != "FAIL" {
					t.Errorf("%s (%s): %s: %s, want FAIL", m.name, m.defect, name, got)
				}
			}
			for _, name := range slices.Sorted(maps.Keys(r.logs)) {
				for _, l := range r.logs[name] {
					if !mutantFailLine.MatchString(l.line) {
						t.Errorf("%s logs %q, want a line matching %s", name, l.line, mutantFailLine)
					}
				}
			}
			// A line that is no "===", "---" or log line, such as the second line of a
			// message with a newline, would escape the check above. A child built with
			// -cover also prints its coverage line.
			for line := range strings.Lines(r.out) {
				line = strings.TrimRight(line, "\r\n")
				if !runLine.MatchString(line) && !resultLine.MatchString(line) &&
					!logLine.MatchString(line) && !coverLine.MatchString(line) &&
					line != "PASS" && line != "FAIL" {
					t.Errorf("child %s prints %q, want only test, result and log lines", parent, line)
				}
			}
		})
	}
}

// mutant is a test-only tenure.Store, tenure.SlotStore and tenure.Watcher that follows
// memstore's rules except for one defect, chosen by name. Every access is under mu, also in the
// racy defect, so a run fails on cases rather than on race reports.
type mutant struct {
	clock  *testClock
	defect string

	mu       sync.Mutex
	now      time.Duration
	seen     bool
	version  uint64
	sessions map[string]*mutSession
	opened   map[string]string
	records  map[string]map[int]tenure.Holding
	slots    map[int]string
	count    int
	watchers []*mutWatcher
}

type mutSession struct {
	timing  core.Timing
	renewed time.Duration
	ended   bool // found expired
	gone    bool // replaced or closed
}

type mutWatcher struct {
	ch   chan tenure.Holding
	stop func() bool
	sent int
}

func newMutant(clock *testClock, defect string) *mutant {
	return &mutant{
		clock:    clock,
		defect:   defect,
		sessions: make(map[string]*mutSession),
		opened:   make(map[string]string),
		records:  make(map[string]map[int]tenure.Holding),
		slots:    make(map[int]string),
	}
}

// is reports whether the mutant has the named defect.
func (s *mutant) is(defect string) bool { return s.defect == defect }

// ctxErr is ctx.Err(), unless the defect ignores contexts.
func (s *mutant) ctxErr(ctx context.Context) error {
	if s.is("ctx-ignored") {
		return nil
	}
	return ctx.Err()
}

func (s *mutant) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	if err := s.ctxErr(ctx); err != nil {
		return err
	}
	if err := mutCheckHolder(holder); err != nil {
		return err
	}
	if err := timing.Validate(); err != nil {
		return fmt.Errorf("mutant: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	sess, ok := s.sessions[holder]
	if !ok {
		member, _, _ := strings.Cut(holder, "/")
		if prev, ok := s.opened[member]; ok && !s.is("no-replace") {
			s.sessions[prev].gone = true
		}
		s.opened[member] = holder
		s.sessions[holder] = &mutSession{timing: timing, renewed: now}
		return nil
	}
	switch {
	case s.is("reopen"):
		sess.ended, sess.gone = false, false
	case s.is("late-renewal") && !sess.gone:
		sess.ended = false
	case !s.live(sess, now):
		return tenure.ErrSessionLost
	}
	sess.timing = timing
	if !s.is("renew-ignored") {
		sess.renewed = now
	}
	return nil
}

func (s *mutant) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	if err := s.ctxErr(ctx); err != nil {
		return 0, err
	}
	if err := s.checkRecordArgs(holder, space, shard); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	sess := s.sessions[holder]
	if sess == nil || !s.live(sess, now) {
		return 0, tenure.ErrSessionLost
	}
	rec, ok := s.records[space][shard]
	if ok && rec.Holder == holder && s.is("no-regrant") {
		return 0, &tenure.HeldError{Holder: holder, RetryAfter: sess.timing.RetryPeriod}
	}
	if ok && rec.Holder != "" && rec.Holder != holder {
		if other := s.sessions[rec.Holder]; other != nil && s.otherLive(other, sess, now) {
			retry := sess.timing.RetryPeriod
			if s.is("retry-holder") {
				retry = other.timing.RetryPeriod
			}
			return 0, &tenure.HeldError{Holder: rec.Holder, RetryAfter: retry}
		}
	}
	if s.is("racy-acquire") {
		// The decision is taken; let another Acquire take its own before this one writes.
		s.mu.Unlock()
		runtime.Gosched()
		s.mu.Lock()
	}
	wall := s.clock.Wall()
	var term core.Term
	switch {
	case s.is("term-wall-only"):
		term = core.Term(max(wall.UnixMilli(), 1))
	case s.is("term-prev-only"):
		term = rec.Term + 1
	default:
		term = core.NextTerm(rec.Term, wall)
	}
	s.write(tenure.Holding{Space: space, Shard: shard, Holder: holder, Term: term}, false)
	return term, nil
}

// otherLive reports whether other, the session a record names, keeps the record from acquirer.
func (s *mutant) otherLive(other, acquirer *mutSession, now time.Duration) bool {
	if s.is("acquirer-lease") {
		return !other.ended && !other.gone && now-other.renewed < acquirer.timing.LeaseDuration
	}
	return s.live(other, now)
}

func (s *mutant) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	if err := s.ctxErr(ctx); err != nil {
		return err
	}
	if err := s.checkRecordArgs(holder, space, shard); err != nil {
		return err
	}
	if term == 0 {
		return errors.New("mutant: term 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[space][shard]
	if !ok || rec.Holder == "" {
		return nil
	}
	if (rec.Holder == holder || s.is("release-any-holder")) && (rec.Term == term || s.is("release-any-term")) {
		s.clear(rec)
	}
	return nil
}

func (s *mutant) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	if err := s.ctxErr(ctx); err != nil {
		return tenure.Holding{}, err
	}
	if err := s.checkRecordArgs(holder, space, shard); err != nil {
		return tenure.Holding{}, err
	}
	if term == 0 {
		return tenure.Holding{}, errors.New("mutant: term 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	if sess := s.sessions[holder]; !s.is("confirm-no-session") && (sess == nil || !s.live(sess, now)) {
		return tenure.Holding{}, tenure.ErrSessionLost
	}
	rec, ok := s.records[space][shard]
	if !ok {
		rec = tenure.Holding{Space: space, Shard: shard}
	}
	if s.is("confirm-term") {
		rec.Term = term
	}
	return rec, nil
}

func (s *mutant) CloseSession(ctx context.Context, holder string) error {
	if err := s.ctxErr(ctx); err != nil {
		return err
	}
	if err := mutCheckHolder(holder); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[holder]; ok {
		sess.gone = true
	} else if !s.is("close-unknown") {
		s.sessions[holder] = &mutSession{gone: true}
	}
	if !s.is("close-keeps") {
		for _, space := range slices.Sorted(maps.Keys(s.records)) {
			for _, shard := range slices.Sorted(maps.Keys(s.records[space])) {
				rec := s.records[space][shard]
				if rec.Holder == holder || (rec.Holder != "" && s.is("close-all")) {
					s.clear(rec)
				}
			}
		}
	}
	for slot, h := range s.slots {
		if h == holder {
			delete(s.slots, slot)
		}
	}
	return nil
}

func (s *mutant) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	if err := s.ctxErr(ctx); err != nil {
		return nil, err
	}
	if err := mutCheckSpace(space); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Collect(maps.Values(s.records[space]))
	slices.SortFunc(out, func(a, b tenure.Holding) int { return cmp.Compare(a.Shard, b.Shard) })
	if s.is("holdings-unsorted") {
		slices.Reverse(out)
	}
	return out, nil
}

// DeleteRecord removes the shard's record and its term history, as memstore's does.
func (s *mutant) DeleteRecord(space string, shard int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[space][shard]; ok {
		s.write(tenure.Holding{Space: space, Shard: shard}, true)
	}
}

func (s *mutant) ClaimSlot(ctx context.Context, holder string) (int, error) {
	if err := s.ctxErr(ctx); err != nil {
		return -1, err
	}
	if err := mutCheckHolder(holder); err != nil {
		return -1, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	if sess := s.sessions[holder]; sess == nil || !s.live(sess, now) {
		return -1, tenure.ErrSessionLost
	}
	retired := false
	for _, slot := range slices.Sorted(maps.Keys(s.slots)) {
		if s.slots[slot] != holder {
			continue
		}
		if slot < s.count {
			return slot, nil
		}
		retired = true
	}
	if retired && !s.is("slot-retired-free") {
		return -1, tenure.ErrNoSlot
	}
	order := make([]int, s.count)
	for i := range order {
		order[i] = i
	}
	if s.is("slot-highest") {
		slices.Reverse(order)
	}
	for _, slot := range order {
		_, taken := s.slots[slot]
		if sticky := taken && s.is("slot-sticky"); s.slotFree(slot, now) && !sticky {
			s.slots[slot] = holder
			return slot, nil
		}
	}
	return -1, tenure.ErrNoSlot
}

func (s *mutant) ReleaseSlot(ctx context.Context, holder string, slot int) error {
	if err := s.ctxErr(ctx); err != nil {
		return err
	}
	if err := mutCheckHolder(holder); err != nil {
		return err
	}
	if slot < 0 || slot > 9999 {
		return fmt.Errorf("mutant: slot %d is outside [0, 9999]", slot)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots[slot] == holder {
		delete(s.slots, slot)
	}
	return nil
}

func (s *mutant) Slots(ctx context.Context) (int, []string, error) {
	if err := s.ctxErr(ctx); err != nil {
		return 0, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return 0, nil, nil
	}
	now := s.tick()
	holders := make([]string, s.count)
	for slot := range holders {
		if !s.slotFree(slot, now) {
			holders[slot] = s.slots[slot]
		}
	}
	return s.count, holders, nil
}

func (s *mutant) SetSlots(ctx context.Context, count int) error {
	if err := s.ctxErr(ctx); err != nil {
		return err
	}
	if count < 0 || count > 10000 {
		return fmt.Errorf("mutant: slot count %d is outside [0, 10000]", count)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.count = count
	return nil
}

func (s *mutant) WatchHolders(ctx context.Context) (<-chan tenure.Holding, error) {
	if err := s.ctxErr(ctx); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	w := &mutWatcher{ch: make(chan tenure.Holding, 1024), stop: func() bool { return false }}
	s.watchers = append(s.watchers, w)
	if !s.is("watch-no-close") {
		w.stop = context.AfterFunc(ctx, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.closeWatcher(w)
		})
	}
	return w.ch, nil
}

// tick reads the clock and returns the largest reading seen.
func (s *mutant) tick() time.Duration {
	if m := s.clock.Monotonic(); !s.seen || m > s.now {
		s.now, s.seen = m, true
	}
	return s.now
}

// live reports whether sess is live at now, ending it when it has expired.
func (s *mutant) live(sess *mutSession, now time.Duration) bool {
	expired := now-sess.renewed >= sess.timing.LeaseDuration
	if s.is("expire-strict") {
		expired = now-sess.renewed > sess.timing.LeaseDuration
	}
	if !sess.ended && !sess.gone && expired {
		sess.ended = true
	}
	return !sess.ended && !sess.gone
}

// clear clears rec's holder, keeping its term.
func (s *mutant) clear(rec tenure.Holding) {
	rec.Holder = ""
	if s.is("release-zero-term") {
		rec.Term = 0
	}
	s.write(rec, false)
}

// write commits a shard-record write and delivers it.
func (s *mutant) write(rec tenure.Holding, remove bool) {
	if s.is("version-restart") {
		rec.Version = s.records[rec.Space][rec.Shard].Version + 1
	} else {
		s.version++
		rec.Version = s.version
	}
	if remove {
		delete(s.records[rec.Space], rec.Shard)
		if len(s.records[rec.Space]) == 0 {
			delete(s.records, rec.Space)
		}
	} else {
		if s.records[rec.Space] == nil {
			s.records[rec.Space] = make(map[int]tenure.Holding)
		}
		s.records[rec.Space][rec.Shard] = rec
	}
	for i, w := range slices.Clone(s.watchers) {
		if i > 0 && s.is("watch-one") {
			break
		}
		w.sent++
		if w.sent%2 == 0 && s.is("watch-drop") {
			continue
		}
		select {
		case w.ch <- rec:
		default:
			s.closeWatcher(w)
		}
	}
}

// closeWatcher closes w's channel once and forgets w.
func (s *mutant) closeWatcher(w *mutWatcher) {
	i := slices.Index(s.watchers, w)
	if i < 0 {
		return
	}
	s.watchers = slices.Delete(s.watchers, i, i+1)
	close(w.ch)
	w.stop()
}

// slotFree reports whether slot has no record or names a holder whose session is not live.
func (s *mutant) slotFree(slot int, now time.Duration) bool {
	h, ok := s.slots[slot]
	if !ok {
		return true
	}
	sess := s.sessions[h]
	return sess == nil || !s.live(sess, now)
}

func (s *mutant) checkRecordArgs(holder, space string, shard int) error {
	if err := mutCheckHolder(holder); err != nil {
		return err
	}
	if err := mutCheckSpace(space); err != nil {
		return err
	}
	if shard < 0 && !s.is("args-unchecked") {
		return fmt.Errorf("mutant: shard %d is negative", shard)
	}
	return nil
}

func mutCheckHolder(holder string) error {
	id, _, _ := strings.Cut(holder, "/")
	switch {
	case holder == "":
		return errors.New("mutant: empty holder")
	case strings.IndexByte(holder, 0) >= 0:
		return fmt.Errorf("mutant: holder %q contains a zero byte", holder)
	case id == "":
		return fmt.Errorf("mutant: holder %q has an empty member ID", holder)
	}
	return nil
}

func mutCheckSpace(space string) error {
	switch {
	case space == "":
		return errors.New("mutant: empty space")
	case strings.IndexByte(space, '/') >= 0:
		return fmt.Errorf("mutant: space %q contains '/'", space)
	case strings.IndexByte(space, 0) >= 0:
		return fmt.Errorf("mutant: space %q contains a zero byte", space)
	}
	return nil
}
