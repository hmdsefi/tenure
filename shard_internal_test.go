// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestATRUN32StopError checks the stop error's text and identity, and the zero Shard. Its steps
// were written from the issue's text.
func TestATRUN32StopError(t *testing.T) {
	sh := newShard("orders", 3, 42, true, time.Time{}, nil)
	if err := sh.Check(); err != nil {
		t.Fatalf("Check() of an owned shard = %v, want nil", err)
	}

	err := sh.stop(core.StopMoved.String())
	const want = "tenure: not owner of orders/3 (term 42): moved"
	if err == nil || err.Error() != want {
		t.Fatalf("stop(moved) = %v, want %q", err, want)
	}
	if !errors.Is(err, ErrNotOwner) {
		t.Errorf("errors.Is(%v, ErrNotOwner) = false, want true", err)
	}
	if errors.Is(err, ErrNoMembers) {
		t.Errorf("errors.Is(%v, ErrNoMembers) = true, want false", err)
	}
	if got := sh.stop(core.StopLost.String()); got != err {
		t.Errorf("second stop(lost) = %v, want the first stop's error %v", got, err)
	}
	if got := sh.stop("closed"); got != err {
		t.Errorf("third stop(closed) = %v, want the first stop's error %v", got, err)
	}
	for i := range 1000 {
		if got := sh.Check(); got != err {
			t.Fatalf("Check() call %d = %v (%p), want the stop error %p", i, got, got, err)
		}
	}
	if sh.Valid() {
		t.Error("Valid() of a stopped shard = true, want false")
	}

	var zero Shard
	var nilShard *Shard
	for name, s := range map[string]*Shard{"zero": &zero, "nil": nilShard, "literal": {Space: "orders", ID: 3, Term: 42}} {
		for range 1000 {
			if got := s.Check(); got != ErrNotOwner {
				t.Fatalf("%s Shard: Check() = %v, want ErrNotOwner", name, got)
			}
		}
		if s.Valid() {
			t.Errorf("%s Shard: Valid() = true, want false", name)
		}
	}
}

// TestATRUN33CheckAllocs checks that Check and Valid allocate nothing. Its steps were written
// from the issue's text.
func TestATRUN33CheckAllocs(t *testing.T) {
	var deadline atomic.Int64
	deadline.Store(int64(time.Hour))
	stopped := newShard("orders", 5, 9, false, time.Now(), &deadline)
	stopErr := stopped.stop(core.StopDraining.String())
	cases := []struct {
		name string
		sh   *Shard
		want bool
	}{
		{"owned fenced", newShard("markets", 1, 7, true, time.Time{}, nil), true},
		{"owned unfenced", newShard("orders", 2, 8, false, time.Now(), &deadline), true},
		{"stopped", stopped, false},
	}
	for _, c := range cases {
		var err error
		valid := false
		allocs := testing.AllocsPerRun(1000, func() {
			err = c.sh.Check()
			valid = c.sh.Valid()
		})
		if allocs != 0 {
			t.Errorf("%s: Check and Valid allocate %v times, want 0", c.name, allocs)
		}
		if (err == nil) != c.want || valid != c.want {
			t.Errorf("%s: Check() = %v, Valid() = %v, want owned %v", c.name, err, valid, c.want)
		}
		if !c.want && err != stopErr {
			t.Errorf("%s: Check() = %v, want the stop error %v", c.name, err, stopErr)
		}
	}
}

func TestCheckUnfencedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		epoch := time.Now()
		time.Sleep(3 * time.Second)
		var deadline atomic.Int64
		deadline.Store(int64(10 * time.Second))
		unfenced := newShard("orders", 4, 11, false, epoch, &deadline)
		fenced := newShard("markets", 6, 12, true, epoch, &deadline)

		time.Sleep(7*time.Second - time.Nanosecond) // deadline − 1 ns
		if err := unfenced.Check(); err != nil {
			t.Fatalf("unfenced Check() at deadline − 1 ns = %v, want nil", err)
		}
		if err := fenced.Check(); err != nil {
			t.Fatalf("fenced Check() at deadline − 1 ns = %v, want nil", err)
		}

		time.Sleep(time.Nanosecond) // the deadline
		expired := unfenced.Check()
		const want = "tenure: not owner of orders/4 (term 11): expired"
		if expired == nil || expired.Error() != want {
			t.Fatalf("unfenced Check() at the deadline = %v, want %q", expired, want)
		}
		if !errors.Is(expired, ErrNotOwner) {
			t.Errorf("errors.Is(%v, ErrNotOwner) = false, want true", expired)
		}
		if unfenced.Valid() {
			t.Error("unfenced Valid() at the deadline = true, want false")
		}
		if got := unfenced.Check(); got != expired {
			t.Errorf("second Check() = %p, want the same error %p", got, expired)
		}
		if err := fenced.Check(); err != nil {
			t.Errorf("fenced Check() at the deadline = %v, want nil", err)
		}

		time.Sleep(time.Hour)
		if err := fenced.Check(); err != nil {
			t.Errorf("fenced Check() an hour after the deadline = %v, want nil", err)
		}

		// A later deadline makes the unfenced shard owned again, until it is stopped.
		deadline.Store(int64(2 * time.Hour))
		if err := unfenced.Check(); err != nil {
			t.Errorf("unfenced Check() after the deadline moved = %v, want nil", err)
		}
		deadline.Store(int64(time.Second))
		if got := unfenced.stop(core.StopExpired.String()); got != expired {
			t.Errorf("stop(expired) = %v (%p), want Check's prebuilt error %p", got, got, expired)
		}
		deadline.Store(int64(3 * time.Hour))
		if got := unfenced.Check(); got != expired {
			t.Errorf("Check() after stop(expired) = %v, want %v", got, expired)
		}
	})
}

// superLog records what a supervisor did, as offsets from the bubble's start.
type superLog struct {
	t0       time.Time
	calls    []time.Duration
	counts   []int
	backoffs []time.Duration
	errs     []error
	stopped  []time.Duration
}

func newSuperLog() *superLog { return &superLog{t0: time.Now()} }

func (l *superLog) since() time.Duration { return time.Since(l.t0) }

func (l *superLog) onReturn(err error, count int, backoff time.Duration) {
	l.errs = append(l.errs, err)
	l.counts = append(l.counts, count)
	l.backoffs = append(l.backoffs, backoff)
}

func (l *superLog) stop() { l.stopped = append(l.stopped, l.since()) }

// start runs supervise in a goroutine and returns a channel closed when it has returned.
func (l *superLog) start(ctx context.Context, sh *Shard, run func(context.Context, *Shard) error, prev <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise(ctx, sh, run, prev, core.DefaultTiming, l.onReturn, l.stop)
	}()
	return done
}

func seconds(s ...float64) []time.Duration {
	out := make([]time.Duration, len(s))
	for i, v := range s {
		out[i] = time.Duration(v * float64(time.Second))
	}
	return out
}

func TestSupervisorBackoff(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name     string
		first    time.Duration // how long the first call runs
		calls    int           // the last call blocks until ctx is done
		want     []time.Duration
		counts   []int
		backoffs []time.Duration
	}{
		{"immediate returns", 0, 6, seconds(0, 2, 6, 14, 29, 44), []int{1, 2, 3, 4, 5}, seconds(2, 4, 8, 15, 15)},
		{"20 s first call", 20 * time.Second, 4, seconds(0, 22, 26, 34), []int{1, 2, 3}, seconds(2, 4, 8)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := newSuperLog()
				ctx, cancel := context.WithCancelCause(context.Background())
				sh := newShard("orders", 1, 3, true, time.Time{}, nil)
				run := func(ctx context.Context, got *Shard) error {
					if got != sh {
						t.Errorf("run got shard %p, want %p", got, sh)
					}
					l.calls = append(l.calls, l.since())
					switch len(l.calls) {
					case 1:
						time.Sleep(c.first)
					case c.calls:
						<-ctx.Done()
						return ctx.Err()
					}
					return boom
				}
				done := l.start(ctx, sh, run, nil)
				time.Sleep(100 * time.Second)
				cancel(sh.stop("moved"))
				<-done

				if !slices.Equal(l.calls, c.want) {
					t.Errorf("calls at %v, want %v", l.calls, c.want)
				}
				if !slices.Equal(l.counts, c.counts) {
					t.Errorf("onReturn counts %v, want %v", l.counts, c.counts)
				}
				if !slices.Equal(l.backoffs, c.backoffs) {
					t.Errorf("onReturn backoffs %v, want %v", l.backoffs, c.backoffs)
				}
				for i, err := range l.errs {
					if err != boom {
						t.Errorf("onReturn error %d = %v, want %v", i, err, boom)
					}
				}
				if !slices.Equal(l.stopped, seconds(100)) {
					t.Errorf("stopped at %v, want once at 100s", l.stopped)
				}
			})
		})
	}
}

func TestRestartBackoff(t *testing.T) {
	timing := core.Timing{LeaseDuration: 20 * time.Hour, RenewDeadline: 10 * time.Hour, RetryPeriod: time.Second, SlotGrace: 24 * time.Hour}
	for count, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 17: 65536 * time.Second, 18: 20 * time.Hour, 1 << 30: 20 * time.Hour} {
		if got := restartBackoff(timing, count); got != want {
			t.Errorf("restartBackoff(%d) = %v, want %v", count, got, want)
		}
	}
}

func TestSupervisorStopDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newSuperLog()
		ctx, cancel := context.WithCancelCause(context.Background())
		sh := newShard("orders", 2, 5, false, l.t0, new(atomic.Int64))
		run := func(context.Context, *Shard) error {
			l.calls = append(l.calls, l.since())
			return nil
		}
		done := l.start(ctx, sh, run, nil)
		time.Sleep(3 * time.Second) // in the 4 s backoff after the second call
		cancel(sh.stop("moved"))
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("supervise has not returned at the instant ctx was canceled")
		}
		if !slices.Equal(l.calls, seconds(0, 2)) {
			t.Errorf("calls at %v, want [0s 2s]", l.calls)
		}
		if !slices.Equal(l.counts, []int{1, 2}) {
			t.Errorf("onReturn counts %v, want [1 2]", l.counts)
		}
		if !slices.Equal(l.stopped, seconds(3)) {
			t.Errorf("stopped at %v, want once at 3s", l.stopped)
		}
		time.Sleep(time.Minute)
		if len(l.calls) != 2 {
			t.Errorf("calls after the stop: %v, want none", l.calls[2:])
		}
	})
}

func TestSupervisorStopsAfterReturn(t *testing.T) {
	t.Run("run returns at the cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := newSuperLog()
			ctx, cancel := context.WithCancelCause(context.Background())
			sh := newShard("orders", 3, 6, true, time.Time{}, nil)
			run := func(ctx context.Context, _ *Shard) error {
				l.calls = append(l.calls, l.since())
				<-ctx.Done()
				return errors.New("canceled")
			}
			done := l.start(ctx, sh, run, nil)
			time.Sleep(5 * time.Second)
			cancel(sh.stop("draining"))
			<-done
			if !slices.Equal(l.calls, seconds(0)) {
				t.Errorf("calls at %v, want [0s]", l.calls)
			}
			if len(l.counts) != 0 {
				t.Errorf("onReturn called %d times, want 0: a return after the cancel is not a restart", len(l.counts))
			}
			if !slices.Equal(l.stopped, seconds(5)) {
				t.Errorf("stopped at %v, want once at 5s", l.stopped)
			}
		})
	})
	t.Run("run returns after the cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := newSuperLog()
			ctx, cancel := context.WithCancelCause(context.Background())
			sh := newShard("orders", 3, 6, true, time.Time{}, nil)
			run := func(ctx context.Context, _ *Shard) error {
				l.calls = append(l.calls, l.since())
				<-ctx.Done()
				time.Sleep(30 * time.Second) // a stuck Run
				return nil
			}
			done := l.start(ctx, sh, run, nil)
			time.Sleep(5 * time.Second)
			cancel(sh.stop("moved"))
			<-done
			if !slices.Equal(l.stopped, seconds(35)) {
				t.Errorf("stopped at %v, want once at 35s, when run returned", l.stopped)
			}
			if len(l.calls) != 1 || len(l.counts) != 0 {
				t.Errorf("calls %v and %d onReturn calls, want one call and none", l.calls, len(l.counts))
			}
		})
	})
	t.Run("ctx done before the first call", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := newSuperLog()
			ctx, cancel := context.WithCancelCause(context.Background())
			sh := newShard("orders", 3, 6, true, time.Time{}, nil)
			cancel(sh.stop("moved"))
			run := func(context.Context, *Shard) error {
				l.calls = append(l.calls, l.since())
				return nil
			}
			<-l.start(ctx, sh, run, nil)
			if len(l.calls) != 0 {
				t.Errorf("calls at %v, want none", l.calls)
			}
			if !slices.Equal(l.stopped, seconds(0)) {
				t.Errorf("stopped at %v, want once at 0s", l.stopped)
			}
		})
	})
}

func TestSupervisorWaitsForPrevious(t *testing.T) {
	t.Run("chained supervisors", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			first, second := newSuperLog(), newSuperLog()
			var running, most atomic.Int32
			// run blocks until ctx is done and returns 2 s later.
			run := func(l *superLog) func(context.Context, *Shard) error {
				return func(ctx context.Context, _ *Shard) error {
					l.calls = append(l.calls, l.since())
					if n := running.Add(1); n > most.Load() {
						most.Store(n)
					}
					defer running.Add(-1)
					<-ctx.Done()
					time.Sleep(2 * time.Second)
					return nil
				}
			}
			ctx1, cancel1 := context.WithCancelCause(context.Background())
			sh1 := newShard("orders", 7, 20, true, time.Time{}, nil)
			done1 := first.start(ctx1, sh1, run(first), nil)

			time.Sleep(5 * time.Second)
			cancel1(sh1.stop("lost"))
			// The next ownership of the shard, from another machine, starts at once.
			ctx2, cancel2 := context.WithCancelCause(context.Background())
			sh2 := newShard("orders", 7, 21, true, time.Time{}, nil)
			done2 := second.start(ctx2, sh2, run(second), done1)

			time.Sleep(10 * time.Second)
			cancel2(sh2.stop("closed"))
			<-done2

			if !slices.Equal(first.calls, seconds(0)) || !slices.Equal(first.stopped, seconds(7)) {
				t.Errorf("first supervisor: calls %v, stopped %v, want [0s] and [7s]", first.calls, first.stopped)
			}
			if !slices.Equal(second.calls, seconds(7)) {
				t.Errorf("second supervisor: calls at %v, want [7s], when the first Run returned", second.calls)
			}
			if !slices.Equal(second.stopped, seconds(17)) {
				t.Errorf("second supervisor: stopped at %v, want [17s]", second.stopped)
			}
			if got := most.Load(); got != 1 {
				t.Errorf("at most %d Runs of the shard at a time, want 1", got)
			}
		})
	})
	t.Run("ctx done while waiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := newSuperLog()
			prev := make(chan struct{})
			ctx, cancel := context.WithCancelCause(context.Background())
			sh := newShard("orders", 0, 1, true, time.Time{}, nil)
			run := func(context.Context, *Shard) error {
				l.calls = append(l.calls, l.since())
				return nil
			}
			done := l.start(ctx, sh, run, prev)
			time.Sleep(2 * time.Second)
			cancel(sh.stop("moved"))
			time.Sleep(3 * time.Second)
			if len(l.stopped) != 0 {
				t.Fatalf("stopped at %v while the previous supervisor runs, want not yet", l.stopped)
			}
			close(prev)
			<-done
			if len(l.calls) != 0 {
				t.Errorf("calls at %v, want none", l.calls)
			}
			if !slices.Equal(l.stopped, seconds(5)) {
				t.Errorf("stopped at %v, want once at 5s, when prev closed", l.stopped)
			}
		})
	})
}

// TestStopThenCancel checks that a Run whose context is done already sees the stop error, from
// Check and as the context's cause.
func TestStopThenCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newSuperLog()
		var deadline atomic.Int64
		deadline.Store(int64(time.Hour))
		sh := newShard("orders", 3, 42, false, l.t0, &deadline)
		ctx, cancel := context.WithCancelCause(context.Background())
		var checked, cause error
		run := func(ctx context.Context, sh *Shard) error {
			<-ctx.Done()
			checked, cause = sh.Check(), context.Cause(ctx)
			return cause
		}
		done := l.start(ctx, sh, run, nil)
		time.Sleep(4 * time.Second)
		stopErr := sh.stop(core.StopMoved.String())
		cancel(stopErr)
		<-done

		const want = "tenure: not owner of orders/3 (term 42): moved"
		if checked != stopErr || cause != stopErr {
			t.Fatalf("Run saw Check() = %v and cause %v, want the stop error %v for both", checked, cause, stopErr)
		}
		if cause.Error() != want || !errors.Is(cause, ErrNotOwner) {
			t.Errorf("cause = %q, errors.Is(cause, ErrNotOwner) = %v, want %q and true", cause, errors.Is(cause, ErrNotOwner), want)
		}
		if got := sh.stop("closed"); got != stopErr {
			t.Errorf("stop(closed) after the stop = %v, want the first error %v", got, stopErr)
		}

		other := newShard("orders", 5, 9, true, time.Time{}, nil)
		if got, want := other.stop("closed").Error(), "tenure: not owner of orders/5 (term 9): closed"; got != want {
			t.Errorf("stop(closed) = %q, want %q", got, want)
		}
	})
}
