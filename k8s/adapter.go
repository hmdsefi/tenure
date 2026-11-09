// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// Defaults of the options (K8S-010).
const (
	defaultRecordConcurrency = 4
	defaultMemberGC          = time.Hour
	defaultWatchLatency      = time.Second
)

// maxGroupLen is the longest group name: every Lease name of the group then fits.
const maxGroupLen = 40

var (
	_ tenure.NameChecker   = (*Adapter)(nil)
	_ tenure.TimingChecker = (*Adapter)(nil)
)

// Adapter keeps one group in the Leases of one namespace. Create one with New; the zero value
// is not usable.
type Adapter struct {
	client    kubernetes.Interface
	namespace string
	group     string

	// err is the first configuration problem New found; every method returns it first.
	err error

	timing            core.Timing
	recordConcurrency int
	memberGC          time.Duration
	logger            *slog.Logger
	watchLatency      time.Duration

	// records limits the record operations in flight to recordConcurrency (records.go).
	records *semaphore
	// terms keeps the terms of deleted shard Leases for the Adapter's life (records.go).
	terms shardTerms

	// now is the Adapter's clock: its readings carry the monotonic clock that observations
	// rest on, and give the wall times written to Leases. Tests replace it.
	now func() time.Time

	// mu guards the fields below it.
	mu sync.Mutex
	// cache is the running informer, nil while none runs (cache.go).
	cache *leaseCache
	// runs counts the informer goroutines that have not ended yet.
	runs sync.WaitGroup
	// sessions is the session table, by holder (session.go).
	sessions map[string]*session
	// leaseLocks serializes the writes of each member Lease, by Lease name (session.go).
	leaseLocks map[string]chan struct{}
}

// Option configures an Adapter. Pass options to New.
type Option struct {
	apply func(a *Adapter) error
}

// WithTiming sets the Timing the Adapter uses for its own member Lease: its duration while
// the member has joined but holds no session, and the interval of the writes that keep it
// alive. A session's Lease uses the Timing passed with the session's renewals instead. The
// default is core.DefaultTiming. The Timing must be valid, and its LeaseDuration a whole
// number of seconds.
func WithTiming(t core.Timing) Option {
	return Option{apply: func(a *Adapter) error {
		err := t.Validate()
		if err == nil {
			err = checkWholeSeconds(t)
		}
		if err != nil {
			return fmt.Errorf("k8s: WithTiming: %w", err)
		}
		a.timing = t
		return nil
	}}
}

// WithRecordConcurrency sets how many shard and slot record writes the Adapter runs at once;
// further writes wait their turn in arrival order. n must be at least 1; the default is 4.
func WithRecordConcurrency(n int) Option {
	return Option{apply: func(a *Adapter) error {
		if n < 1 {
			return fmt.Errorf("k8s: WithRecordConcurrency(%d): want at least 1", n)
		}
		a.recordConcurrency = n
		return nil
	}}
}

// WithMemberGC sets how long a member Lease must stay unchanged, as this Adapter observes it,
// before the Adapter deletes it; it waits at least twice the Lease's duration. 0 turns the
// deletion off. d must not be negative; the default is one hour.
func WithMemberGC(d time.Duration) Option {
	return Option{apply: func(a *Adapter) error {
		if d < 0 {
			return fmt.Errorf("k8s: WithMemberGC(%v): want 0 or more", d)
		}
		a.memberGC = d
		return nil
	}}
}

// WithLogger sets the logger of the Adapter. nil, and the default, discard every record.
func WithLogger(l *slog.Logger) Option {
	return Option{apply: func(a *Adapter) error {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}
		a.logger = l
		return nil
	}}
}

// WithWatchLatency sets how long a change the API server has accepted may take to reach the
// Adapter's watch, the allowance the Adapter adds to its estimates of how long a change takes
// to reach every member. d must not be negative; the default is one second.
func WithWatchLatency(d time.Duration) Option {
	return Option{apply: func(a *Adapter) error {
		if d < 0 {
			return fmt.Errorf("k8s: WithWatchLatency(%v): want 0 or more", d)
		}
		a.watchLatency = d
		return nil
	}}
}

// New returns an Adapter that keeps group in the Leases of namespace through client. It makes
// no request and never fails: when client is nil, namespace is not a DNS-1123 label, group is
// not a DNS-1123 label of at most 40 characters, or an option is invalid, every method of the
// Adapter returns an error that describes the first of these problems, in that order, with
// options in argument order.
func New(client kubernetes.Interface, namespace, group string, opts ...Option) *Adapter {
	a := &Adapter{
		client:            client,
		namespace:         namespace,
		group:             group,
		timing:            core.DefaultTiming,
		recordConcurrency: defaultRecordConcurrency,
		memberGC:          defaultMemberGC,
		logger:            slog.New(slog.DiscardHandler),
		watchLatency:      defaultWatchLatency,
		now:               time.Now,
		sessions:          map[string]*session{},
		leaseLocks:        map[string]chan struct{}{},
	}
	// K8S-010: the checks in order; the first failure is the configuration error.
	switch {
	case client == nil:
		a.err = errors.New("k8s: nil client")
	case !isDNS1123Label(namespace):
		a.err = fmt.Errorf("k8s: namespace %q is not a DNS-1123 label", namespace)
	case !isGroup(group):
		a.err = fmt.Errorf("k8s: group %q is not a DNS-1123 label of at most %d characters", group, maxGroupLen)
	}
	for _, o := range opts {
		if o.apply == nil {
			continue
		}
		if err := o.apply(a); err != nil && a.err == nil {
			a.err = err
		}
	}
	a.records = newSemaphore(a.recordConcurrency)
	return a
}

// CheckNames returns the Adapter's configuration error, if any. Otherwise it returns an error
// for the first space, in order, that the Adapter cannot hold, and then for a member ID it
// cannot hold. It makes no request.
func (a *Adapter) CheckNames(spaces []string, member string) error {
	if a.err != nil {
		return a.err
	}
	for _, s := range spaces {
		if d := spaceDetail(s); d != "" {
			return fmt.Errorf("k8s: invalid space %q: %s", s, d)
		}
	}
	if d := memberDetail(a.group, member); d != "" {
		return fmt.Errorf("k8s: invalid member ID %q: %s", member, d)
	}
	return nil
}

// CheckTiming returns the Adapter's configuration error, if any. Otherwise it returns an error
// when the LeaseDuration of t is not a whole number of seconds, as a Lease stores its duration
// in seconds. It does not repeat t.Validate and makes no request.
func (a *Adapter) CheckTiming(t core.Timing) error {
	if a.err != nil {
		return a.err
	}
	if err := checkWholeSeconds(t); err != nil {
		return fmt.Errorf("k8s: %w", err)
	}
	return nil
}

// checkWholeSeconds reports a LeaseDuration that a Lease's leaseDurationSeconds cannot hold
// (K8S-045).
func checkWholeSeconds(t core.Timing) error {
	if t.LeaseDuration%time.Second != 0 {
		return fmt.Errorf("LeaseDuration %v is not a whole number of seconds", t.LeaseDuration)
	}
	return nil
}
