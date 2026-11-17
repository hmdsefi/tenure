// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package viewhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
)

// DefaultWait is the wait a Membership asks a Handler for when WithWait does not set one.
const DefaultWait = 30 * time.Second

// ClientOption configures a Membership that NewMembership returns.
type ClientOption func(*clientConfig)

// clientConfig is what the ClientOptions set.
type clientConfig struct {
	client     *http.Client
	wait       time.Duration
	backoff    bool // WithBackoff was given
	backoffMin time.Duration
	backoffMax time.Duration
	timing     core.Timing
}

// WithHTTPClient sets the HTTP client that sends the polls. The default is http.DefaultClient.
// Each poll's context bounds it to the wait plus the timing's RenewDeadline.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(cfg *clientConfig) { cfg.client = c }
}

// WithWait sets how long each poll asks the Handler to hold it when the view has not changed.
// The default is DefaultWait, and a wait above MaxWait is cut to MaxWait.
func WithWait(d time.Duration) ClientOption {
	return func(cfg *clientConfig) { cfg.wait = d }
}

// WithBackoff sets the pause after a failed poll: initial after the first failure in a row,
// doubling after each further one up to maximum. The default is the timing's RetryPeriod and
// LeaseDuration.
func WithBackoff(initial, maximum time.Duration) ClientOption {
	return func(cfg *clientConfig) { cfg.backoff, cfg.backoffMin, cfg.backoffMax = true, initial, maximum }
}

// WithTiming sets the timing of the group. Its RenewDeadline is how much longer than the wait a
// poll may take before the Membership gives up on the endpoint, and how much longer than the
// wait the Membership goes without a successful poll before it reports itself stale. The default
// is core.DefaultTiming.
func WithTiming(t core.Timing) ClientOption {
	return func(cfg *clientConfig) { cfg.timing = t }
}

// Membership is a tenure.Membership that only watches: it follows the views that Handlers serve
// at one or more endpoints, and never joins the group. A Router can follow a group through it
// without access to the group's store. Its methods are safe for concurrent use.
//
// While any channel of Watch is open, one goroutine long-polls one endpoint at a time: it starts
// at a random endpoint and keeps it while it answers. After a failed poll, an answer that is
// neither a valid view nor 304 Not Modified, or no answer within the wait plus RenewDeadline, it
// pauses for the backoff and moves to the next endpoint, whose view it accepts even when it is
// older than the last: the members' views can differ briefly. The goroutine stops when the last
// channel closes.
//
// The views carry the Membership's own Version counter, and a new view comes only when the
// members, their slots, the slot count or the slot holders change. Join, SetDraining and Leave
// return an error that matches tenure.ErrWatchOnly.
type Membership struct {
	urls []string // each endpoint's view URL
	cfg  clientConfig

	mu      sync.Mutex
	watches map[*clientWatch]struct{} // every open channel of Watch
	running bool                      // a follower runs for the open channels
	gen     uint64                    // the generation of the latest follower started
	stop    context.CancelFunc        // stops the running follower
	ready   chan struct{}             // closed when the running follower has its first view
	cur     *core.View                // the running follower's latest view; nil before its first
	next    int                       // the endpoint the next follower starts at
	version uint64                    // the Version of the latest view delivered
	lastOK  time.Time                 // when a poll last succeeded; zero before the first
	lastErr error                     // the latest failure, nil before the first
}

// clientWatch is one channel that Watch returned.
type clientWatch struct {
	ch chan core.View // buffer 1
}

// send puts v on w's channel, replacing a view not yet received. The caller holds m.mu, under
// which alone views are sent, so it never blocks.
func (w *clientWatch) send(v core.View) {
	select {
	case <-w.ch:
	default:
	}
	w.ch <- v
}

// NewMembership returns a watch-only Membership that follows the views served at endpoints:
// each endpoint is the base URL under which a Handler serves "view", such as
// "http://relay.example:8080/tenure" for a Handler at "/tenure/view"; one trailing slash is
// ignored. A Kubernetes Service URL serves as one endpoint.
//
// NewMembership fails when endpoints is empty or an endpoint is not an absolute http or https
// URL, when the HTTP client is nil, the wait is not positive, the backoff's initial pause is not
// positive or is above its maximum, or the timing is not valid. It sends no request: the polls
// start with the first Watch.
func NewMembership(endpoints []string, opts ...ClientOption) (*Membership, error) {
	cfg := clientConfig{client: http.DefaultClient, wait: DefaultWait, timing: core.DefaultTiming}
	for _, o := range opts {
		o(&cfg)
	}
	if len(endpoints) == 0 {
		return nil, errors.New("viewhttp: NewMembership: no endpoints")
	}
	urls := make([]string, len(endpoints))
	for i, e := range endpoints {
		u, err := url.Parse(e)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("viewhttp: NewMembership: endpoint %q is not an http or https URL", e)
		}
		urls[i] = strings.TrimSuffix(e, "/") + "/view"
	}
	if err := cfg.timing.Validate(); err != nil {
		return nil, fmt.Errorf("viewhttp: NewMembership: %w", err)
	}
	if !cfg.backoff {
		cfg.backoffMin, cfg.backoffMax = cfg.timing.RetryPeriod, cfg.timing.LeaseDuration
	}
	switch {
	case cfg.client == nil:
		return nil, errors.New("viewhttp: NewMembership: nil HTTP client")
	case cfg.wait <= 0:
		return nil, fmt.Errorf("viewhttp: NewMembership: wait %v is not positive", cfg.wait)
	case cfg.backoffMin <= 0 || cfg.backoffMax < cfg.backoffMin:
		return nil, fmt.Errorf("viewhttp: NewMembership: backoff %v to %v is not a positive range",
			cfg.backoffMin, cfg.backoffMax)
	}
	cfg.wait = min(cfg.wait, MaxWait)
	return &Membership{
		urls:    urls,
		cfg:     cfg,
		watches: make(map[*clientWatch]struct{}),
		next:    rand.IntN(len(urls)), //nolint:gosec // G404: spreads clients over endpoints, no secret
	}, nil
}

// Join returns an error that matches tenure.ErrWatchOnly, or ctx's error when ctx is done.
func (m *Membership) Join(ctx context.Context, _ core.Member) error {
	return refuse(ctx, "Join")
}

// SetDraining returns an error that matches tenure.ErrWatchOnly, or ctx's error when ctx is
// done.
func (m *Membership) SetDraining(ctx context.Context, _ bool) error {
	return refuse(ctx, "SetDraining")
}

// Leave returns an error that matches tenure.ErrWatchOnly, or ctx's error when ctx is done. It
// closes no channel.
func (m *Membership) Leave(ctx context.Context) error {
	return refuse(ctx, "Leave")
}

// refuse returns the error of a write method.
func refuse(ctx context.Context, method string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("viewhttp: %s: %w", method, tenure.ErrWatchOnly)
}

// Watch returns a channel that holds the current view and receives each later one. It holds one
// view, and a newer view replaces one not yet received. The channel is closed when ctx is done.
//
// When no channel of the Membership is open, Watch starts the polls. It returns once the
// Membership has a view, at once when it has one, and fails when ctx ends first, with ctx's
// error, or when the wait plus RenewDeadline passes first, with an error that wraps the latest
// failed poll's error, if any.
func (m *Membership) Watch(ctx context.Context) (<-chan core.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := &clientWatch{ch: make(chan core.View, 1)}
	m.mu.Lock()
	if !m.running {
		m.startLocked()
	}
	if m.cur != nil {
		w.send(*m.cur)
	}
	m.watches[w] = struct{}{}
	ready := m.ready
	m.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { m.endWatch(w) })

	limit := m.cfg.wait + m.cfg.timing.RenewDeadline
	t := time.NewTimer(limit)
	defer t.Stop()
	select {
	case <-ready:
		return w.ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
	}
	stop()
	m.endWatch(w)
	m.mu.Lock()
	last := m.lastErr
	m.mu.Unlock()
	if last == nil {
		return nil, fmt.Errorf("viewhttp: Watch: no view within %v", limit)
	}
	return nil, fmt.Errorf("viewhttp: Watch: no view within %v: %w", limit, last)
}

// Stale reports whether no poll has succeeded for the wait plus RenewDeadline, or none has
// succeeded yet. The views of a stale Membership may be old; it keeps its last view meanwhile.
func (m *Membership) Stale() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastOK.IsZero() || time.Since(m.lastOK) > m.cfg.wait+m.cfg.timing.RenewDeadline
}

// startLocked starts a follower. The caller holds m.mu.
func (m *Membership) startLocked() {
	ctx, cancel := context.WithCancel(context.Background())
	m.gen++
	m.running, m.stop, m.ready, m.cur = true, cancel, make(chan struct{}), nil
	gen, start := m.gen, m.next
	go m.follow(ctx, gen, start)
}

// endWatch closes w's channel unless it is closed already, and stops the follower when no
// channel is left open.
func (m *Membership) endWatch(w *clientWatch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.watches[w]; !ok {
		return
	}
	delete(m.watches, w)
	close(w.ch)
	if len(m.watches) == 0 && m.running {
		m.stop()
		m.running, m.cur = false, nil
	}
}

// follow is the follower of generation gen: it polls the endpoints, starting at index i, until
// ctx is done.
func (m *Membership) follow(ctx context.Context, gen uint64, i int) {
	var (
		tag     string // the ETag of the latest view of the current endpoint
		backoff time.Duration
	)
	for {
		v, newTag, err := m.poll(ctx, m.urls[i], tag)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			backoff = 0
			if v != nil {
				tag = newTag
			}
			m.succeeded(gen, v)
			continue
		}
		i, tag = (i+1)%len(m.urls), ""
		backoff = min(max(2*backoff, m.cfg.backoffMin), m.cfg.backoffMax)
		m.failed(gen, i, err)
		t := time.NewTimer(backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return
		}
	}
}

// poll sends one long poll to u with the ETag tag, if any. It returns the view of a 200 answer
// and its ETag, or a nil view for 304 Not Modified, or the failure.
func (m *Membership) poll(ctx context.Context, u, tag string) (*core.View, string, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.wait+m.cfg.timing.RenewDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u+"?wait="+m.cfg.wait.String(), nil)
	if err != nil {
		return nil, "", err
	}
	if tag != "" {
		req.Header.Set("If-None-Match", tag)
	}
	resp, err := m.cfg.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotModified && tag != "":
		return nil, tag, nil
	case resp.StatusCode != http.StatusOK:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, "", fmt.Errorf("viewhttp: GET %s: %s: %s", u, resp.Status, strings.TrimSpace(string(msg)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("viewhttp: GET %s: read body: %w", u, err)
	}
	v, err := decodeView(body)
	if err != nil {
		return nil, "", fmt.Errorf("viewhttp: GET %s: decode view: %w", u, err)
	}
	if err := v.Validate(); err != nil {
		return nil, "", fmt.Errorf("viewhttp: GET %s: %w", u, err)
	}
	return &v, resp.Header.Get("ETag"), nil
}

// succeeded records a successful poll of the follower of generation gen, and delivers v, the
// view it got (nil for 304), unless its content equals the latest view's.
func (m *Membership) succeeded(gen uint64, v *core.View) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen || !m.running {
		return
	}
	m.lastOK = time.Now()
	if v == nil || (m.cur != nil && sameContent(*m.cur, *v)) {
		return
	}
	first := m.cur == nil
	m.version++
	v.Version = m.version
	m.cur = v
	for w := range m.watches {
		w.send(*v)
	}
	if first {
		close(m.ready)
	}
}

// failed records a failed poll of the follower of generation gen, which moves on to endpoint i.
func (m *Membership) failed(gen uint64, i int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen {
		return
	}
	m.lastErr, m.next = err, i
}

// sameContent reports whether a and b list the same members, with the same data and slots, and
// the same slot count and holders.
func sameContent(a, b core.View) bool {
	return slices.EqualFunc(a.Members, b.Members, func(x, y core.Member) bool {
		return x.ID == y.ID && x.Addr == y.Addr && x.Draining == y.Draining && x.Slot == y.Slot &&
			maps.Equal(x.Meta, y.Meta) && x.JoinAt.Equal(y.JoinAt) && x.LeaveAt.Equal(y.LeaveAt)
	}) && a.Slots == b.Slots && slices.Equal(a.SlotHolders, b.SlotHolders)
}
