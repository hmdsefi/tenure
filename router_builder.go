// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// staleSource is implemented by a Membership that can tell that its views may be out of date,
// such as a watch-only source that has lost its connection. Router.Stale reads it.
type staleSource interface {
	Stale() bool
}

// start calls m.Watch once and starts the builder on its channel (RTR-300). It returns Watch's
// error as "tenure: NewRouter: watch: " and the error.
func (r *Router) start(m Membership) error {
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := m.Watch(ctx)
	switch {
	case err != nil:
		cancel()
		return fmt.Errorf("tenure: NewRouter: watch: %w", err)
	case ch == nil:
		cancel()
		return errors.New("tenure: NewRouter: watch: nil channel")
	}
	r.source, r.cancel, r.done = m, cancel, make(chan struct{})
	go r.build(ctx, ch)
	return nil
}

// build is the builder, the Router's one goroutine (RTR-300 to RTR-340). It applies the latest
// view of ch, ticks the Assigner at its next wake, and after ch closes or a Watch fails marks the
// Router stale and calls Watch again RetryPeriod later. It never calls Join, SetDraining or
// Leave. It returns when ctx is done; after Watch returns ErrClosed it watches no more, but
// keeps ticking.
func (r *Router) build(ctx context.Context, ch <-chan core.View) {
	defer close(r.done)
	wake := time.NewTimer(time.Hour)
	wake.Stop()
	defer wake.Stop()
	retry := time.NewTimer(time.Hour)
	retry.Stop()
	defer retry.Stop()

	for {
		r.armWake(wake)
		select {
		case v, ok := <-ch:
			if ok {
				v, ok = latestView(v, ch)
				r.applyNow(v)
			}
			if !ok { // the watch has ended: stale until a view of the next one (RTR-340)
				ch = nil
				r.setStale(true)
				retry.Reset(r.opts.timing.RetryPeriod)
				continue
			}
			r.setStale(false)
		case <-wake.C:
			r.tickNow()
		case <-retry.C:
			next, err := r.source.Watch(ctx)
			switch {
			case ctx.Err() != nil:
				return
			case errors.Is(err, ErrClosed):
				// The source is gone for good: keep the last snapshot, stale.
			case err != nil || next == nil:
				retry.Reset(r.opts.timing.RetryPeriod)
			default:
				ch = next
			}
		case <-ctx.Done():
			return
		}
	}
}

// setStale records whether the Router is stale and reports a change with router.stale or
// router.fresh.
func (r *Router) setStale(stale bool) {
	if r.stale.Swap(stale) == stale {
		return
	}
	if stale {
		r.events.emit(RouterEvent{Kind: RouterEventStale, Shard: -1, Err: errWatchClosed})
		return
	}
	r.events.emit(RouterEvent{Kind: RouterEventFresh, Version: r.Version(), Shard: -1})
}

// latestView returns the latest view ch holds right now, v when it holds none, and whether ch
// is still open. A Membership's channel holds one view, so this matters only for a source that
// buffers more (RTR-310).
func latestView(v core.View, ch <-chan core.View) (core.View, bool) {
	for {
		select {
		case next, ok := <-ch:
			if !ok {
				return v, false
			}
			v = next
		default:
			return v, true
		}
	}
}

// applyNow applies v at the clock's current reading.
func (r *Router) applyNow(v core.View) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wall, reading := r.clock()
	if r.testApplyHook != nil {
		r.testApplyHook(v)
	}
	r.apply(v, reading, wall)
}

// tickNow ticks the Assigner at the clock's current reading.
func (r *Router) tickNow() {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, reading := r.clock()
	r.tick(reading)
}

// armWake arms wake for the Assigner's next wake, or stops it when there is none (RTR-330).
func (r *Router) armWake(wake *time.Timer) {
	r.mu.Lock()
	at, ok := r.assigner.NextWake()
	_, reading := r.clock()
	r.mu.Unlock()
	wake.Stop()
	if ok {
		wake.Reset(max(at-reading, 0))
	}
}

// clock returns the clock's wall-clock time and its reading since the epoch, which never goes
// back: a reading before the last one counts as the last. The caller holds r.mu.
func (r *Router) clock() (time.Time, time.Duration) {
	now := r.opts.now()
	return now, max(now.Sub(r.epoch), r.reading)
}

// Stale reports whether the Router's table may be out of date: its watch of the Membership has
// ended or failed and no view has come since, or the Membership reports itself stale through a
// method Stale() bool. A stale Router keeps routing with its last table.
func (r *Router) Stale() bool {
	if r.stale.Load() {
		return true
	}
	s, ok := r.source.(staleSource)
	return ok && s.Stale()
}

// Subscribe returns a channel that receives the version of each new snapshot: right away the
// current one, once there has been a view, and then each later one. The channel holds one
// version, and a newer one replaces one not yet received, so a slow reader sees the latest. Close
// closes it; after Close, Subscribe returns a closed channel.
func (r *Router) Subscribe() <-chan uint64 {
	ch := make(chan uint64, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.snap.Load()
	if s.closed {
		close(ch)
		return ch
	}
	if s.version > 0 {
		ch <- s.version
	}
	r.subs = append(r.subs, ch)
	return ch
}

// sendVersion puts version in ch, which has buffer 1 and only senders that hold the Router's
// mu, replacing a version not yet received.
func sendVersion(ch chan uint64, version uint64) {
	select {
	case <-ch:
	default:
	}
	ch <- version
}

// Close stops the Router (RTR-350): lookups return ErrClosed from then on, Ready returns
// ErrClosed, the builder and its timers stop, and every channel of Subscribe is closed. Then it
// emits router.closed, the last event, and waits until every event has been delivered. It
// returns once every goroutine of the Router has returned, and returns nil; later calls do the
// same.
func (r *Router) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		closed := *r.snap.Load()
		closed.closed = true
		r.snap.Store(&closed)
		for _, ch := range r.subs {
			close(ch)
		}
		r.subs = nil
		r.mu.Unlock()
		close(r.closed)
		r.cancel()
		<-r.done
		r.events.close(RouterEvent{Kind: RouterEventClosed, Version: closed.version, Shard: -1})
	})
	<-r.done
	r.events.wait()
	return nil
}
