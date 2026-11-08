// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package memstore is an in-memory tenure.Store, tenure.SlotStore and tenure.Watcher for tests,
// local development and simulations.
//
// A Store keeps everything in one process and takes its time from a Clock the caller supplies,
// so a test can drive expiry and terms step by step. Takeover follows the store's own clock: a
// session expires once its holder's LeaseDuration has passed on the Store's monotonic time
// since the session's last renewal. That time is the largest Monotonic reading the Store has
// seen, so it never moves back, and a step of the wall clock never expires a session. New terms
// come from core.NextTerm and the Clock's wall time.
//
// A Store keeps rules stricter than the tenure.Store contract asks for. Expiry is always judged
// on the Store's clock, never by the acquirer. Confirm is a plain read and never writes. Each
// grant, clear and deletion of an existing record takes the next value of one counter shared by
// every record, so record versions go up by exactly 1 per write; slot writes take none. Every
// such write reaches each open watch channel, or closes it when its buffer is full, before the
// method that makes it returns. A call that returns an error has changed nothing, whatever the
// error.
//
// Every method takes one lock for the whole Store, so a Store is safe for concurrent use, also
// inside a testing/synctest bubble.
package memstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Store is an in-memory tenure.Store, tenure.SlotStore and tenure.Watcher. Create one with New;
// the zero value is not usable.
type Store struct {
	clock Clock

	mu sync.Mutex

	// now is the largest Monotonic reading seen; seen reports that there has been one.
	now  time.Duration
	seen bool

	// version is the store-wide counter of shard-record writes.
	version uint64

	// sessions holds every holder the Store has heard of, also those whose session has ended,
	// so that an ended session never reopens.
	sessions map[string]*session

	// opened maps a member ID to the holder whose session of that member opened last. Opening a
	// session ends every other one of the member, so no other holder of it can be live.
	opened map[string]string

	// records holds the shard records by space and shard.
	records map[string]map[int]tenure.Holding

	// slots maps a slot to the holder its record names; a slot with no record has no entry.
	// Records at or above slotCount are retired slots and stay until released or cleared.
	slots     map[int]string
	slotCount int

	// watchers holds the open WatchHolders channels in registration order.
	watchers []*watcher
}

// session is one holder's session.
type session struct {
	// timing is the Timing of the last successful KeepAlive.
	timing core.Timing

	// renewed is the Store's time at the last successful KeepAlive.
	renewed time.Duration

	// ended reports that the session has ended for good.
	ended bool
}

// New returns an empty Store that reads time from clock. A nil clock is the system clock: the
// monotonic time since New was called, and time.Now for wall time.
func New(clock Clock) *Store {
	if clock == nil {
		clock = systemClock{t0: time.Now()}
	}
	return &Store{
		clock:    clock,
		sessions: make(map[string]*session),
		opened:   make(map[string]string),
		records:  make(map[string]map[int]tenure.Holding),
		slots:    make(map[int]string),
	}
}

// KeepAlive opens a session for a holder the Store has never heard of, ending every other
// session of the same member ID, or renews the live session of holder and stores timing as its
// Timing. It returns tenure.ErrSessionLost when the session has ended, also when the renewal
// comes at or after the session's expiry.
func (s *Store) KeepAlive(ctx context.Context, holder string, timing core.Timing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkHolder(holder); err != nil {
		return err
	}
	if err := timing.Validate(); err != nil {
		return fmt.Errorf("memstore: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	sess, ok := s.sessions[holder]
	if !ok {
		member := memberID(holder)
		if prev, ok := s.opened[member]; ok {
			s.sessions[prev].ended = true
		}
		s.opened[member] = holder
		s.sessions[holder] = &session{timing: timing, renewed: now}
		return nil
	}
	if !s.live(sess, now) {
		return tenure.ErrSessionLost
	}
	sess.timing = timing
	sess.renewed = now
	return nil
}

// Acquire grants the shard to holder and returns the new term when the record is absent, has
// no holder, names a holder whose session is not live, or names holder itself. The term is
// core.NextTerm of the record's term, or of 0 when there is no record, and the Clock's wall
// time. Otherwise it returns 0 and a *tenure.HeldError naming the record's holder, with the
// RetryPeriod of holder's Timing as RetryAfter. It returns tenure.ErrSessionLost when holder has
// no live session.
func (s *Store) Acquire(ctx context.Context, holder, space string, shard int) (core.Term, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := checkRecordArgs(holder, space, shard); err != nil {
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
	if ok && rec.Holder != "" && rec.Holder != holder {
		if other := s.sessions[rec.Holder]; other != nil && s.live(other, now) {
			return 0, &tenure.HeldError{Holder: rec.Holder, RetryAfter: sess.timing.RetryPeriod}
		}
	}
	term := core.NextTerm(rec.Term, s.clock.Wall())
	s.write(tenure.Holding{Space: space, Shard: shard, Holder: holder, Term: term}, false)
	return term, nil
}

// Release clears the record's holder, keeping its term, when the record names holder at term.
// Any other record, or none, is left as it is, and Release still returns nil. It does not look
// at the session of holder.
func (s *Store) Release(ctx context.Context, holder, space string, shard int, term core.Term) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkRecordArgs(holder, space, shard); err != nil {
		return err
	}
	if term == 0 {
		return errors.New("memstore: term 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[space][shard]; ok && rec.Holder == holder && rec.Term == term {
		rec.Holder = ""
		s.write(rec, false)
	}
	return nil
}

// Confirm returns the shard's record and never writes one. An absent record reads as
// tenure.Holding{Space: space, Shard: shard}. term is checked to be non-zero and is otherwise
// unused. It returns tenure.ErrSessionLost when holder has no live session.
func (s *Store) Confirm(ctx context.Context, holder, space string, shard int, term core.Term) (tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return tenure.Holding{}, err
	}
	if err := checkRecordArgs(holder, space, shard); err != nil {
		return tenure.Holding{}, err
	}
	if term == 0 {
		return tenure.Holding{}, errors.New("memstore: term 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.tick()
	if sess := s.sessions[holder]; sess == nil || !s.live(sess, now) {
		return tenure.Holding{}, tenure.ErrSessionLost
	}
	if rec, ok := s.records[space][shard]; ok {
		return rec, nil
	}
	return tenure.Holding{Space: space, Shard: shard}, nil
}

// CloseSession ends the session of holder for good, also for a holder the Store has never heard
// of or whose session has already ended. It clears the holder of every shard record that names
// it, space by space and shard by shard in order, keeping the terms, and removes every slot
// record that names it, also those at or above the slot count. It returns nil.
func (s *Store) CloseSession(ctx context.Context, holder string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkHolder(holder); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[holder]; ok {
		sess.ended = true
	} else {
		s.sessions[holder] = &session{ended: true}
	}
	for _, space := range slices.Sorted(maps.Keys(s.records)) {
		for _, shard := range slices.Sorted(maps.Keys(s.records[space])) {
			if rec := s.records[space][shard]; rec.Holder == holder {
				rec.Holder = ""
				s.write(rec, false)
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

// Holdings returns every record of space, also those with no holder, sorted by shard, in a new
// slice.
func (s *Store) Holdings(ctx context.Context, space string) ([]tenure.Holding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkSpace(space); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Collect(maps.Values(s.records[space]))
	slices.SortFunc(out, func(a, b tenure.Holding) int { return cmp.Compare(a.Shard, b.Shard) })
	return out, nil
}

// DeleteRecord removes the shard's record and with it the shard's term history, as if the
// backend had lost it: the next grant computes its term from 0. Removing a record counts as a
// write of it and reaches watchers as tenure.Holding{Space: space, Shard: shard, Version: v},
// v being the deletion's version. DeleteRecord of an absent record does nothing. It is meant for
// tests.
func (s *Store) DeleteRecord(space string, shard int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[space][shard]; ok {
		s.write(tenure.Holding{Space: space, Shard: shard}, true)
	}
}

// tick reads the monotonic clock and returns the Store's time: the largest reading seen.
func (s *Store) tick() time.Duration {
	if m := s.clock.Monotonic(); !s.seen || m > s.now {
		s.now, s.seen = m, true
	}
	return s.now
}

// live reports whether sess is live at now. A session found expired, now - renewed >= its own
// LeaseDuration, is ended for good here; the Store's time never moves back, so it could not be
// live again anyway.
func (s *Store) live(sess *session, now time.Duration) bool {
	if !sess.ended && now-sess.renewed >= sess.timing.LeaseDuration {
		sess.ended = true
	}
	return !sess.ended
}

// write commits one shard-record write: a grant, a clear or, with remove set, the deletion of
// an existing record. Every such write goes through here. It takes the next value of the
// store-wide version and stamps it on rec, stores rec as the record of (rec.Space, rec.Shard) or
// removes that record, and delivers rec to the watchers.
func (s *Store) write(rec tenure.Holding, remove bool) {
	s.version++
	rec.Version = s.version
	if remove {
		delete(s.records[rec.Space], rec.Shard)
		if len(s.records[rec.Space]) == 0 {
			delete(s.records, rec.Space)
		}
	} else {
		shards := s.records[rec.Space]
		if shards == nil {
			shards = make(map[int]tenure.Holding)
			s.records[rec.Space] = shards
		}
		shards[rec.Shard] = rec
	}
	s.deliver(rec)
}

// memberID returns the member ID of holder: the text before the first "/", or all of holder
// when it has none.
func memberID(holder string) string {
	id, _, _ := strings.Cut(holder, "/")
	return id
}

// checkHolder checks a holder argument.
func checkHolder(holder string) error {
	switch {
	case holder == "":
		return errors.New("memstore: empty holder")
	case strings.IndexByte(holder, 0) >= 0:
		return fmt.Errorf("memstore: holder %q contains a zero byte", holder)
	case memberID(holder) == "":
		return fmt.Errorf("memstore: holder %q has an empty member ID", holder)
	}
	return nil
}

// checkSpace checks a space argument.
func checkSpace(space string) error {
	switch {
	case space == "":
		return errors.New("memstore: empty space")
	case strings.IndexByte(space, '/') >= 0:
		return fmt.Errorf("memstore: space %q contains '/'", space)
	case strings.IndexByte(space, 0) >= 0:
		return fmt.Errorf("memstore: space %q contains a zero byte", space)
	}
	return nil
}

// checkRecordArgs checks the holder, space and shard arguments of a record call, in that order.
func checkRecordArgs(holder, space string, shard int) error {
	if err := checkHolder(holder); err != nil {
		return err
	}
	if err := checkSpace(space); err != nil {
		return err
	}
	if shard < 0 {
		return fmt.Errorf("memstore: shard %d is negative", shard)
	}
	return nil
}
