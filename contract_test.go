// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/memstore"
)

// The driver contract (MCH O1–O8, RUN §5's mapping: O1 RUN-110; O2 RUN-120; O3 RUN-150, RUN-340;
// O4 and O5 RUN-175; O6 RUN-130, RUN-140, RUN-160; O7 RUN-130, RUN-180, RUN-530; O8 RUN-210),
// checked on running nodes under randomized store faults.
//
// The scenario runs three nodes, n1, n2 and n3, on one memstore and one group, each through an fs
// of its own, with Config.Slots 2, so that one of them is a standby. Each serves orders, 8 shards
// without fencing, and ledger, 8 fenced shards, for 300 s of bubble time. The faults come from
// math/rand/v2's rand.NewPCG(seed, 0), for seeds 1 to 20:
//
//   - outages: every call of one node's fs fails at once or hangs until its ctx ends, for 3 s to
//     13 s, so that the session goes late but is never lost at the store; an outage also breaks
//     the node's store watch;
//   - store errors: one method of one node's fs fails for 2 s to 10 s;
//   - delays: one method of one node's fs waits up to 3 s before it goes to the store, or goes to
//     the store and holds the answer up to 3 s, past the call's deadline at times;
//   - stale states: the store watch of every node re-delivers an earlier value now and then, and
//     lags the store by up to 8 s for 10 s to 40 s, in commit order;
//   - a lost session: from one instant on, the store answers every KeepAlive, Acquire, Confirm and
//     ClaimSlot of one node's holder with ErrSessionLost; the node's watch lags 8 s from then on,
//     and 3 s after the first grant to its next holder comes an outage (trap) that leaves a slow
//     Holdings answer showing the record after the shards' Confirm went out;
//   - a paused membership: one node's views are held back for 5 s to 40 s;
//   - a SetSlots to 1 or 3 through one node, a Drain of another node and a forced Close of a third:
//     a Close whose ctx is done already or ends within 5 s, before the drain can complete.
//
// The other nodes are closed at the end. Runs stop late now and then, some long enough to be
// stuck, and the first Run of some shards returns an error and is restarted. Everything is
// recorded through WithStepHook and the doubles, whose Seq stamps order the steps, the calls, the
// watch values and the runs across the nodes.
//
// The 20 runs are made once, by the first test that needs them, and each test checks its
// obligation on every run. A failure names the seed, the node and the step.

// The scenario's parameters.
const (
	contractSeeds  = 20
	contractLength = 300 * time.Second
)

// contractIDs are the members of the scenario.
var contractIDs = []string{"n1", "n2", "n3"}

// errUnreachable is the error of the calls of a node during an outage that fails them at once.
var errUnreachable = errors.New("unreachable")

// faultKind says what a fault does to a call.
type faultKind int

const (
	faultFail  faultKind = iota // the call fails at once
	faultHang                   // the call waits for its ctx and returns ctx's error
	faultDelay                  // the call waits d, or until its ctx ends, before it goes on
	faultLate                   // the call goes on, and its answer waits d, or until its ctx ends
	faultStuck                  // the call goes on, and its answer waits d, past its ctx's end
)

// fault is one fault of one node's fs: from from to to after the scenario's start, every call
// of method, or of every method when method is "" (an outage), meets kind.
type fault struct {
	from, to time.Duration
	method   string
	kind     faultKind
	d        time.Duration
}

// watchLag is the method name of a fault that makes the store watch lag by d: no call has it.
const watchLag = "WatchLag"

// faultMethods are the store methods that errors and delays hit.
var faultMethods = []string{"KeepAlive", "Acquire", "Release", "Confirm", "CloseSession", "Holdings", "ClaimSlot",
	"ReleaseSlot", "Slots", "WatchHolders", "SetSlots"}

// lostMethods are the methods that answer ErrSessionLost once the store has lost the holder's
// session; Release, ReleaseSlot and CloseSession do not look at the session.
var lostMethods = []string{"KeepAlive", "Acquire", "Confirm", "ClaimSlot"}

// contractPlan is what a seed draws: the faults of each node and the instants of the scenario's
// events, as offsets from its start.
type contractPlan struct {
	faults [3][]fault

	lossNode int
	lossAt   time.Duration

	pauseNode         int
	pauseAt, pauseFor time.Duration

	slotsNode int
	slotsAt   time.Duration
	slotsTo   int

	drainNode int
	drainAt   time.Duration

	closeNode int
	closeAt   time.Duration
	closeWait time.Duration // 0: Close's ctx is done at the call
}

// drawPlan draws the plan of seed from rng.
func drawPlan(rng *rand.Rand) contractPlan {
	sec := func(lo, hi float64) time.Duration {
		return time.Duration((lo + rng.Float64()*(hi-lo)) * float64(time.Second))
	}
	var p contractPlan
	for i := range p.faults {
		for range 1 + rng.IntN(2) {
			from := sec(10, 270)
			kind := faultFail
			if rng.IntN(2) == 0 {
				kind = faultHang
			}
			p.faults[i] = append(p.faults[i], fault{from: from, to: from + sec(3, 13), kind: kind})
		}
		for range 4 {
			from := sec(10, 280)
			p.faults[i] = append(p.faults[i], fault{from: from, to: from + sec(2, 10),
				method: faultMethods[rng.IntN(len(faultMethods))], kind: faultFail})
		}
		for range 4 {
			from := sec(10, 280)
			kind := faultDelay
			if rng.IntN(2) == 0 {
				kind = faultLate
			}
			p.faults[i] = append(p.faults[i], fault{from: from, to: from + sec(5, 20),
				method: faultMethods[rng.IntN(len(faultMethods))], kind: kind, d: sec(0.1, 3)})
		}
		for range 2 {
			from := sec(10, 280)
			p.faults[i] = append(p.faults[i], fault{from: from, to: from + sec(10, 40), method: watchLag,
				kind: faultDelay, d: sec(0.5, 8)})
		}
	}
	p.lossNode, p.lossAt = rng.IntN(3), sec(40, 200)
	p.pauseNode, p.pauseAt, p.pauseFor = rng.IntN(3), sec(20, 200), sec(5, 40)
	p.slotsNode, p.slotsAt, p.slotsTo = rng.IntN(3), sec(60, 140), []int{1, 3}[rng.IntN(2)]
	p.drainNode, p.drainAt = rng.IntN(3), sec(150, 230)
	p.closeNode, p.closeAt = (p.drainNode+1+rng.IntN(2))%3, sec(150, 260)
	if rng.IntN(2) == 0 {
		p.closeWait = sec(0.5, 5)
	}
	// Slow slot reads while slots change hands, so that some read before a claim or a release
	// answers after its result.
	for i := range p.faults {
		for _, at := range []time.Duration{p.lossAt, p.slotsAt, p.drainAt, p.closeAt} {
			p.faults[i] = append(p.faults[i], fault{from: at, to: at + 40*time.Second, method: "Slots",
				kind: faultStuck, d: 2500 * time.Millisecond})
		}
	}
	return p
}

// watchSend is one value a node's store watch handed to the node, with the stamp taken when it
// was sent: the node counts a watch value as read when it receives it (RUN-175).
type watchSend struct {
	h           tenure.Holding
	seq         uint64
	at          time.Time
	redelivered bool
}

// contractNode is one node of the scenario while it runs.
type contractNode struct {
	id   string
	t0   time.Time
	fs   *recStore
	sm   *recMembership
	runs *runLog
	log  *stepLog
	n    *tenure.Node

	startSeq uint64

	mu      sync.Mutex
	faults  []fault
	changed chan struct{} // closed and replaced when a fault is added
	rng     *rand.Rand    // shared by the nodes of a run; guarded by rngMu
	rngMu   *sync.Mutex
	lost    map[string]bool
	sends   []watchSend
	trapped bool // the outage after the first grant of a new incarnation has been added

	closeSeq uint64 // the stamp taken right after Close returned
	closeErr error
}

// faultAt returns the fault that hits a call of method now: an outage before any other.
func (cn *contractNode) faultAt(method string) (fault, bool) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	now := time.Since(cn.t0)
	var found fault
	ok := false
	for _, f := range cn.faults {
		if now < f.from || now >= f.to || (f.method != "" && f.method != method) {
			continue
		}
		if f.method == "" {
			return f, true
		}
		if !ok {
			found, ok = f, true
		}
	}
	return found, ok
}

// nextOutage returns the start of the next outage of the node, and false when none is left, and
// a channel closed when a fault is added.
func (cn *contractNode) nextOutage() (time.Time, bool, <-chan struct{}) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	now := time.Since(cn.t0)
	var next time.Duration
	ok := false
	for _, f := range cn.faults {
		if f.method == "" && f.from > now && (!ok || f.from < next) {
			next, ok = f.from, true
		}
	}
	return cn.t0.Add(next), ok, cn.changed
}

// addFault adds f to the node's faults.
func (cn *contractNode) addFault(f fault) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.faults = append(cn.faults, f)
	close(cn.changed)
	cn.changed = make(chan struct{})
}

// trap adds, after the first grant to the holder of a new incarnation, an outage of 9 s that
// starts 3 s later and hangs every call. The store watch lags 8 s from the loss on, so the outage
// breaks the watch before it delivers the grant, and the record naming the new holder reaches the
// node first through a resync or a Confirm, while its fenced shards are degraded or just after.
func (cn *contractNode) trap(holder string) {
	cn.mu.Lock()
	first := !cn.trapped && len(cn.lost) > 0 && !cn.lost[holder]
	cn.trapped = cn.trapped || first
	cn.mu.Unlock()
	if first {
		now := time.Since(cn.t0)
		cn.addFault(fault{from: now + 3*time.Second, to: now + 10*time.Second, kind: faultHang})
		cn.addFault(fault{from: now + 3*time.Second, to: now + 13*time.Second, method: "KeepAlive", kind: faultFail})
		cn.addFault(fault{from: now + 3*time.Second, to: now + 30*time.Second, method: "Holdings", kind: faultStuck,
			d: 2500 * time.Millisecond})
		cn.addFault(fault{from: now + 3*time.Second, to: now + 30*time.Second, method: "Confirm", kind: faultLate,
			d: 1900 * time.Millisecond})
	}
}

// loseSession makes the store lose the session of holder from now on.
func (cn *contractNode) loseSession(holder string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.lost[holder] = true
}

// isLost reports whether the store has lost the session of holder.
func (cn *contractNode) isLost(holder string) bool {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return cn.lost[holder]
}

// chance reports true one time in n.
func (cn *contractNode) chance(n int) bool {
	cn.rngMu.Lock()
	defer cn.rngMu.Unlock()
	return cn.rng.IntN(n) == 0
}

// intn returns a number drawn from [0, n).
func (cn *contractNode) intn(n int) int {
	cn.rngMu.Lock()
	defer cn.rngMu.Unlock()
	return cn.rng.IntN(n)
}

// sleepCtx waits d or until ctx ends, and returns ctx's error in the second case.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// answer returns the Answer of method: the store loses a lost holder's calls, a fault hits the
// call, or it goes to the memstore.
func (cn *contractNode) answer(method string) answerFunc {
	return func(ctx context.Context, args []any) ([]any, error) {
		if slices.Contains(lostMethods, method) && cn.isLost(argAt[string](args, 0)) {
			return nil, tenure.ErrSessionLost
		}
		res, err := cn.faulty(ctx, method, func(ctx context.Context) ([]any, error) {
			return cn.fs.Forward(ctx, method, args)
		})
		if method == "Acquire" && err == nil {
			cn.trap(argAt[string](args, 0))
		}
		return res, err
	}
}

// faulty runs forward under the fault that hits a call of method now, if any.
func (cn *contractNode) faulty(ctx context.Context, method string, forward func(context.Context) ([]any, error)) ([]any, error) {
	f, ok := cn.faultAt(method)
	if !ok {
		return forward(ctx)
	}
	switch f.kind {
	case faultFail:
		if f.method == "" {
			return nil, errUnreachable
		}
		return nil, errBoom
	case faultHang:
		if err := sleepCtx(ctx, time.Until(cn.t0.Add(f.to))); err != nil {
			return nil, err
		}
		return nil, errUnreachable // the outage is over, but its calls fail
	case faultDelay:
		if err := sleepCtx(ctx, f.d); err != nil {
			return nil, err
		}
		return forward(ctx)
	case faultStuck:
		res, err := forward(ctx)
		time.Sleep(f.d)
		return res, err
	default: // faultLate
		res, err := forward(ctx)
		if e := sleepCtx(ctx, f.d); e != nil {
			return nil, e
		}
		return res, err
	}
}

// watch is the Answer of WatchHolders: under the faults, a channel that relays the memstore's.
func (cn *contractNode) watch(ctx context.Context, args []any) ([]any, error) {
	return cn.faulty(ctx, "WatchHolders", func(ctx context.Context) ([]any, error) {
		res, err := cn.fs.Forward(ctx, "WatchHolders", args)
		if err != nil {
			return nil, err
		}
		out := make(chan tenure.Holding, 1024)
		go cn.relay(resultAt[<-chan tenure.Holding](res, 0), out)
		return []any{(<-chan tenure.Holding)(out)}, nil
	})
}

// relay hands each value of in to out, in order, as late as a watch lag says, and one time in
// four also an earlier value again. It closes out when in closes, at the start of the node's next
// outage, and when out is full, as a store that cannot deliver every write does; values it holds
// then are lost.
func (cn *contractNode) relay(in <-chan tenure.Holding, out chan tenure.Holding) {
	defer close(out)
	var brk <-chan time.Time
	brkTimer := time.NewTimer(time.Hour)
	defer brkTimer.Stop()
	var changed <-chan struct{}
	arm := func() {
		at, ok, ch := cn.nextOutage()
		changed, brk = ch, nil
		brkTimer.Stop()
		if ok {
			brkTimer.Reset(time.Until(at))
			brk = brkTimer.C
		}
	}
	arm()
	type held struct {
		h           tenure.Holding
		due         time.Time
		redelivered bool
	}
	var queue []held
	var sent []tenure.Holding
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		for len(queue) > 0 && !queue[0].due.After(now) {
			q := queue[0]
			queue = queue[1:]
			if !cn.send(out, watchSend{h: q.h, seq: seq(), at: time.Now(), redelivered: q.redelivered}) {
				return
			}
			if !q.redelivered {
				sent = append(sent, q.h)
			}
		}
		var due <-chan time.Time
		if len(queue) > 0 {
			timer.Reset(time.Until(queue[0].due))
			due = timer.C
		}
		select {
		case h, ok := <-in:
			if !ok {
				return
			}
			at := now
			if f, ok := cn.faultAt(watchLag); ok {
				at = at.Add(f.d)
			}
			if n := len(queue); n > 0 && queue[n-1].due.After(at) {
				at = queue[n-1].due
			}
			queue = append(queue, held{h: h, due: at})
			if len(sent) > 0 && cn.chance(4) {
				queue = append(queue, held{h: sent[cn.intn(len(sent))], due: at, redelivered: true})
			}
		case <-due:
		case <-changed:
			arm()
		case <-brk:
			return
		}
		timer.Stop()
	}
}

// send hands s to out without waiting, records it, and reports false when out is full.
func (cn *contractNode) send(out chan tenure.Holding, s watchSend) bool {
	select {
	case out <- s.h:
	default:
		return false
	}
	cn.mu.Lock()
	cn.sends = append(cn.sends, s)
	cn.mu.Unlock()
	return true
}

// contractRunScript is the Run of the scenario: it blocks until its ctx is done and returns the
// cause, late by 0 s to 12 s depending on the shard and the term, so that some runs are stuck; the
// first Run of every fourth shard returns an error after 1 s instead and is restarted.
func contractRunScript(ctx context.Context, sh *tenure.Shard, earlier int) error {
	if earlier == 0 && sh.ID%4 == 1 {
		t := time.NewTimer(time.Second)
		select {
		case <-t.C:
			return errBoom
		case <-ctx.Done():
			t.Stop()
		}
	} else {
		<-ctx.Done()
	}
	late := []time.Duration{0, 0, time.Second, 3 * time.Second, 12 * time.Second}
	i := sh.ID % len(late)
	if sh.Term%2 == 1 {
		i = (i + 2) % len(late)
	}
	time.Sleep(late[i])
	return context.Cause(ctx)
}

// nodeRecord is what one node of a run did.
type nodeRecord struct {
	id       string
	epoch    time.Time
	startSeq uint64 // the stamp taken right before Start

	steps []tenure.StepRecord
	seqs  []uint64 // the hook's stamp of each step

	calls  []call // the store calls, in start order
	mcalls []call // the membership calls, in start order
	runs   []runCall
	sends  []watchSend

	holders []string // the holder of each generation, from 1

	closeSeq uint64 // the stamp taken right after Close returned
	forced   bool   // Close returned an error: its ctx ended first
}

// contractRun is one run of the scenario.
type contractRun struct {
	seed  uint64
	plan  contractPlan
	nodes []*nodeRecord
	ok    bool
}

// contractCache holds the runs, made once.
var contractCache struct {
	once sync.Once
	runs []*contractRun
}

// contractRuns returns the runs of seeds 1 to 20, making them the first time as subtests of t.
func contractRuns(t *testing.T) []*contractRun {
	t.Helper()
	contractCache.once.Do(func() {
		for seed := uint64(1); seed <= contractSeeds; seed++ {
			r := &contractRun{seed: seed}
			t.Run("scenario/seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) { runContract(t, r) })
				r.ok = !t.Failed()
			})
			contractCache.runs = append(contractCache.runs, r)
		}
	})
	for _, r := range contractCache.runs {
		if !r.ok {
			t.Fatalf("seed %d: the scenario failed", r.seed)
		}
	}
	return contractCache.runs
}

// eachNode runs f on every node of every run, as a subtest named by the seed and the node.
func eachNode(t *testing.T, f func(t *testing.T, r *contractRun, nr *nodeRecord)) {
	t.Helper()
	for _, r := range contractRuns(t) {
		for _, nr := range r.nodes {
			t.Run(fmt.Sprintf("seed=%d/%s", r.seed, nr.id), func(t *testing.T) { f(t, r, nr) })
		}
	}
}

// runContract runs the scenario of r.seed and records it in r.
func runContract(t *testing.T, r *contractRun) {
	check := baseline(t)
	rng := rand.New(rand.NewPCG(r.seed, 0)) //nolint:gosec // G404: a test's seeded faults.
	p := drawPlan(rng)
	r.plan = p
	var rngMu sync.Mutex
	ms := memstore.New(nil)
	g := newGroup(t)
	t0 := time.Now()

	nodes := make([]*contractNode, len(contractIDs))
	for i, id := range contractIDs {
		sleepUntil(t0.Add(time.Duration(i) * time.Second))
		cn := &contractNode{id: id, t0: t0, faults: p.faults[i], changed: make(chan struct{}), fs: newRecStore(ms),
			sm: g.Absent(id), runs: &runLog{}, log: &stepLog{}, rng: rng, rngMu: &rngMu, lost: make(map[string]bool)}
		cn.runs.Script(contractRunScript)
		for _, m := range faultMethods {
			if m != "WatchHolders" {
				cn.fs.Answer(m, cn.answer(m))
			}
		}
		cn.fs.Answer("WatchHolders", cn.watch)
		cfg := tenure.Config{
			Self:       tenure.Member{ID: id, Addr: addrOf(id)},
			Membership: cn.sm,
			Store:      cn.fs.Watching(),
			Slots:      2,
			Spaces: []tenure.Space{
				{Name: "orders", Shards: 8, Run: cn.runs.Run},
				{Name: "ledger", Shards: 8, Fenced: true, Run: cn.runs.Run},
			},
			Rand: recoveryRand(),
		}
		cn.startSeq = seq()
		cn.n = start(t, cn.log.ctx(context.Background()), cfg)
		nodes[i] = cn
	}

	var wg sync.WaitGroup
	closeNode := func(cn *contractNode, ctx context.Context) {
		cn.closeErr = cn.n.Close(ctx)
		cn.closeSeq = seq()
	}
	type event struct {
		at time.Duration
		do func()
	}
	events := []event{
		{p.lossAt, func() {
			cn := nodes[p.lossNode]
			cn.addFault(fault{from: p.lossAt, to: p.lossAt + time.Minute, method: watchLag, kind: faultDelay,
				d: 8 * time.Second})
			cn.loseSession(cn.n.Holder())
		}},
		{p.pauseAt, func() { nodes[p.pauseNode].sm.Pause() }},
		{p.pauseAt + p.pauseFor, func() { nodes[p.pauseNode].sm.Resume() }},
		{p.slotsAt, func() {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = nodes[p.slotsNode].n.SetSlots(ctx, p.slotsTo) // faults may fail it
			})
		}},
		{p.drainAt, func() {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				_ = nodes[p.drainNode].n.Drain(ctx) // it may outlast ctx; Close finishes it
			})
		}},
		{p.closeAt, func() {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), p.closeWait)
				defer cancel()
				closeNode(nodes[p.closeNode], ctx)
			})
		}},
	}
	slices.SortStableFunc(events, func(a, b event) int { return int(a.at - b.at) })
	for _, e := range events {
		sleepUntil(t0.Add(e.at))
		e.do()
	}
	sleepUntil(t0.Add(contractLength))
	wg.Wait()
	for _, cn := range nodes {
		if cn.closeSeq != 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		closeNode(cn, ctx)
		cancel()
		if cn.closeErr != nil {
			t.Errorf("%s: Close() at the end = %v, want nil", cn.id, cn.closeErr)
		}
	}
	time.Sleep(30 * time.Second) // runs that outlive a forced Close return
	synctest.Wait()

	for _, cn := range nodes {
		steps, seqs := cn.log.records()
		nr := &nodeRecord{
			id: cn.id, epoch: cn.n.Epoch(), startSeq: cn.startSeq,
			steps: steps, seqs: seqs,
			calls: cn.fs.Calls(), mcalls: cn.sm.Calls(), runs: cn.runs.Calls(),
			closeSeq: cn.closeSeq, forced: cn.closeErr != nil,
		}
		cn.mu.Lock()
		nr.sends = slices.Clone(cn.sends)
		cn.mu.Unlock()
		for _, c := range nr.calls {
			if c.Method == "KeepAlive" {
				if h := argAt[string](c.Args, 0); !slices.Contains(nr.holders, h) {
					nr.holders = append(nr.holders, h)
				}
			}
		}
		r.nodes = append(r.nodes, nr)
	}
	check()
}

// holder returns the holder of generation gen.
func (nr *nodeRecord) holder(gen uint64) string {
	if gen == 0 || gen > uint64(len(nr.holders)) {
		return ""
	}
	return nr.holders[gen-1]
}

// before returns the stamp taken before step i began: the hook's stamp of the step before it, or
// the stamp taken before Start for the first.
func (nr *nodeRecord) before(i int) uint64 {
	if i == 0 {
		return nr.startSeq
	}
	return nr.seqs[i-1]
}

// at returns the bubble time of step i.
func (nr *nodeRecord) at(i int) time.Time { return nr.epoch.Add(nr.steps[i].Now) }

// where names step i for failure messages.
func (nr *nodeRecord) where(i int) string {
	s := nr.steps[i]
	return fmt.Sprintf("step %d (gen %d, at %v, seq %d) %T%+v", i, s.Gen, s.Now, nr.seqs[i], s.Input, s.Input)
}

// matcher hands out calls of a node, each at most once.
type matcher struct {
	calls []call
	used  []bool
}

// newMatcher returns a matcher of calls.
func newMatcher(calls []call) *matcher {
	return &matcher{calls: calls, used: make([]bool, len(calls))}
}

// take marks the first call not handed out yet for which ok holds, and reports whether there
// was one.
func (m *matcher) take(ok func(c call) bool) bool {
	for i, c := range m.calls {
		if !m.used[i] && ok(c) {
			m.used[i] = true
			return true
		}
	}
	return false
}

// holderOfCall returns the holder argument of a call of holderCalls' methods, "" for another.
func holderOfCall(c call) string {
	if !slices.Contains(holderCalls, c.Method) {
		return ""
	}
	return argAt[string](c.Args, 0)
}

// inputNow returns the Now field of an input.
func inputNow(in core.Input) time.Duration {
	return time.Duration(reflect.ValueOf(in).FieldByName("Now").Int())
}

// TestDriverContractO1Readings checks O1 (RUN-110): every step's Now is in [0, 2^62) and never
// below the one before; the SentAt of each SessionRenewed is the start of its KeepAlive call, a
// call of the generation's holder that succeeded before the step, less the epoch; and each
// ViewChanged's Wall is the epoch plus its Now.
func TestDriverContractO1Readings(t *testing.T) {
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		renewals := newMatcher(nr.calls)
		for i, s := range nr.steps {
			if s.Now < 0 || s.Now >= 1<<62 {
				t.Errorf("%s: Now outside [0, 2^62)", nr.where(i))
			}
			if inputNow(s.Input) != s.Now {
				t.Errorf("%s: the input's Now is not the step's %v", nr.where(i), s.Now)
			}
			if i > 0 && s.Now < nr.steps[i-1].Now {
				t.Errorf("%s: Now below the step before's %v", nr.where(i), nr.steps[i-1].Now)
			}
			switch in := s.Input.(type) {
			case core.ViewChanged:
				if !in.Wall.Equal(nr.epoch.Add(in.Now)) {
					t.Errorf("%s: Wall %v, want Epoch+Now %v", nr.where(i), in.Wall, nr.epoch.Add(in.Now))
				}
			case core.SessionRenewed:
				h, start := nr.holder(s.Gen), nr.epoch.Add(in.SentAt)
				if !renewals.take(func(c call) bool {
					return c.Method == "KeepAlive" && argAt[string](c.Args, 0) == h && c.Done && c.Err == nil &&
						c.EndSeq < nr.seqs[i] && c.Start.Equal(start)
				}) {
					t.Errorf("%s: no KeepAlive(%s) of its own that started at Epoch+SentAt and succeeded before it",
						nr.where(i), h)
				}
				if !s.SentWall.Equal(start) {
					t.Errorf("%s: SentWall %v, want Epoch+SentAt %v", nr.where(i), s.SentWall, start)
				}
			}
		}
	})
}

// TestDriverContractO2Ticks checks O2 (RUN-120): after a step that returns Wake{At: w}, the next
// step of the machine whose Now is at or after w is Tick{Now: w}; and every Tick is at the wake
// of the step before it.
func TestDriverContractO2Ticks(t *testing.T) {
	ticks := 0
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		for i := 1; i < len(nr.steps); i++ {
			prev, s := nr.steps[i-1], nr.steps[i]
			if prev.Gen != s.Gen {
				if _, ok := s.Input.(core.Tick); ok {
					t.Errorf("%s: a Tick as a new machine's first step", nr.where(i))
				}
				continue
			}
			w, armed := wakeOf(prev.Actions)
			tick, isTick := s.Input.(core.Tick)
			switch {
			case isTick && (!armed || tick.Now != w):
				t.Errorf("%s: a Tick, but the step before woke at %v (armed %t)", nr.where(i), w, armed)
			case armed && s.Now >= w && !isTick:
				t.Errorf("%s: at or after the wake %v of the step before, want Tick{Now: %v}", nr.where(i), w, w)
			case isTick:
				ticks++
			}
		}
	})
	if ticks == 0 {
		t.Error("no Tick stepped in any run")
	}
}

// shardKey names one shard of one generation.
type shardKey struct {
	gen   uint64
	space string
	shard int
}

// TestDriverContractO3Answers checks O3 (RUN-150, RUN-340): each Acquire gets one answer,
// Acquired or AcquireFailed, after it, and every Acquire is answered unless Close's ctx ended
// first; each StopRun gets one RunStopped, stepped after every Run of its term returned; and
// Released, SessionRenewed, SessionFailed{CauseLost} and SessionClosed are each stepped only after
// a store result of the generation's holder that they answer, one result per step.
func TestDriverContractO3Answers(t *testing.T) {
	var answered, stopped int
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		acquires := make(map[shardKey]int)
		type runKey struct {
			shardKey
			term core.Term
		}
		stops := make(map[runKey]int)
		results := newMatcher(nr.calls)
		answer := func(i int, k shardKey) {
			if acquires[k] == 0 {
				t.Errorf("%s: an answer with no Acquire out", nr.where(i))
				return
			}
			acquires[k]--
			answered++
		}
		for i, s := range nr.steps {
			h := nr.holder(s.Gen)
			switch in := s.Input.(type) {
			case core.Acquired:
				answer(i, shardKey{s.Gen, in.Space, in.Shard})
			case core.AcquireFailed:
				answer(i, shardKey{s.Gen, in.Space, in.Shard})
			case core.RunStopped:
				k := runKey{shardKey{s.Gen, in.Space, in.Shard}, in.Term}
				if stops[k] == 0 {
					t.Errorf("%s: a RunStopped with no StopRun out", nr.where(i))
				} else {
					stops[k]--
					stopped++
				}
				for _, c := range nr.runs {
					if c.Shard.Space == in.Space && c.Shard.ID == in.Shard && c.Shard.Term == in.Term &&
						(!c.Done || c.EndSeq > nr.seqs[i]) {
						t.Errorf("%s: before %v returned", nr.where(i), c)
					}
				}
			case core.Released:
				if !results.take(func(c call) bool {
					return c.Method == "Release" && c.Done && c.Err == nil && c.EndSeq < nr.seqs[i] &&
						holderOfCall(c) == h && argAt[string](c.Args, 1) == in.Space &&
						argAt[int](c.Args, 2) == in.Shard && argAt[core.Term](c.Args, 3) == in.Term
				}) {
					t.Errorf("%s: no Release(%s, %s, %d, %d) succeeded before it", nr.where(i), h, in.Space, in.Shard, in.Term)
				}
			case core.SessionRenewed:
				if !results.take(func(c call) bool {
					return c.Method == "KeepAlive" && c.Done && c.Err == nil && c.EndSeq < nr.seqs[i] && holderOfCall(c) == h
				}) {
					t.Errorf("%s: no KeepAlive(%s) succeeded before it", nr.where(i), h)
				}
			case core.SessionFailed:
				if in.Cause != core.CauseLost {
					break
				}
				if !results.take(func(c call) bool {
					return c.Done && errors.Is(c.Err, tenure.ErrSessionLost) && c.EndSeq < nr.seqs[i] && holderOfCall(c) == h
				}) {
					t.Errorf("%s: no call of %s returned ErrSessionLost before it", nr.where(i), h)
				}
			case core.SessionClosed:
				if !results.take(func(c call) bool {
					return c.Method == "CloseSession" && c.Done && c.EndSeq < nr.seqs[i] && holderOfCall(c) == h &&
						(c.Err == nil || errors.Is(c.Err, tenure.ErrSessionLost))
				}) {
					t.Errorf("%s: no CloseSession(%s) succeeded before it", nr.where(i), h)
				}
			}
			for _, a := range s.Actions {
				switch a := a.(type) {
				case core.Acquire:
					acquires[shardKey{s.Gen, a.Space, a.Shard}]++
				case core.StopRun:
					stops[runKey{shardKey{s.Gen, a.Space, a.Shard}, a.Term}]++
				}
			}
		}
		if nr.forced {
			return // a forced Close stops driving the machine: what is out stays unanswered
		}
		for k, n := range acquires {
			if n != 0 {
				t.Errorf("gen %d: %d Acquire(%s/%d) never answered", k.gen, n, k.space, k.shard)
			}
		}
		for k, n := range stops {
			if n != 0 {
				t.Errorf("gen %d: %d StopRun(%s/%d, term %d) with no RunStopped", k.gen, n, k.space, k.shard, k.term)
			}
		}
	})
	if answered == 0 || stopped == 0 {
		t.Errorf("%d Acquire answers and %d RunStopped in all runs, want some of each", answered, stopped)
	}
}

// recordKey names one record state of one shard: its holder and term, which a memstore writes
// once each.
type recordKey struct {
	space  string
	shard  int
	holder string
	term   core.Term
}

// readings returns every record state the nodes of r read, with its version: from the answers of
// Holdings and Confirm, and from the store watches. It fails t when a state carries two versions.
func readingVersions(t *testing.T, r *contractRun) map[recordKey]uint64 {
	t.Helper()
	out := make(map[recordKey]uint64)
	add := func(h tenure.Holding) {
		if h.Version == 0 || h.Term == 0 {
			return
		}
		k := recordKey{h.Space, h.Shard, h.Holder, h.Term}
		if v, ok := out[k]; ok && v != h.Version {
			t.Errorf("seed %d: %+v read with versions %d and %d", r.seed, k, v, h.Version)
		}
		out[k] = h.Version
	}
	for _, nr := range r.nodes {
		for _, c := range nr.calls {
			if !c.Done || c.Err != nil {
				continue
			}
			switch c.Method {
			case "Holdings":
				for _, h := range resultAt[[]tenure.Holding](c.Result, 0) {
					add(h)
				}
			case "Confirm":
				add(resultAt[tenure.Holding](c.Result, 0))
			}
		}
		for _, s := range nr.sends {
			add(s.h)
		}
	}
	return out
}

// TestDriverContractO4Freshness checks O4 (RUN-175): per shard, the record versions behind the
// HolderChanged steps of a node increase, also across generations, although the store watches
// re-deliver stale states; a version equal to the one before is stepped only as the answer of a
// Confirm call of the generation's holder; and a step of no record comes before any record.
func TestDriverContractO4Freshness(t *testing.T) {
	var stepped, stale int
	for _, r := range contractRuns(t) {
		versions := readingVersions(t, r)
		for _, nr := range r.nodes {
			t.Run(fmt.Sprintf("seed=%d/%s", r.seed, nr.id), func(t *testing.T) {
				for _, s := range nr.sends {
					if s.redelivered {
						stale++
					}
				}
				last := make(map[[2]any]uint64)
				confirms := newMatcher(nr.calls)
				for i, s := range nr.steps {
					in, ok := s.Input.(core.HolderChanged)
					if !ok {
						continue
					}
					stepped++
					key := [2]any{in.Space, in.Shard}
					v := uint64(0)
					if in.Holder != "" || in.Term != 0 {
						if v, ok = versions[recordKey{in.Space, in.Shard, in.Holder, in.Term}]; !ok {
							t.Errorf("%s: no reading of that record", nr.where(i))
							continue
						}
					}
					prev := last[key]
					switch {
					case v == 0 && prev != 0:
						t.Errorf("%s: no record, after version %d", nr.where(i), prev)
					case v != 0 && v < prev:
						t.Errorf("%s: version %d, after version %d", nr.where(i), v, prev)
					case v != 0 && v == prev:
						h := nr.holder(s.Gen)
						if !confirms.take(func(c call) bool {
							if c.Method != "Confirm" || !c.Done || c.Err != nil || c.EndSeq > nr.seqs[i] || holderOfCall(c) != h {
								return false
							}
							got := resultAt[tenure.Holding](c.Result, 0)
							return got.Space == in.Space && got.Shard == in.Shard && got.Version == v
						}) {
							t.Errorf("%s: version %d again, but not as a Confirm answer", nr.where(i), v)
						}
					}
					last[key] = max(prev, v)
				}
			})
		}
	}
	if stepped == 0 || stale == 0 {
		t.Errorf("%d HolderChanged steps and %d stale states re-delivered in all runs, want some of each", stepped, stale)
	}
}

// TestDriverContractO5Confirm checks O5 (RUN-175): each Restore comes after the call of the
// shard's latest Confirm action started (Confirm calls are never dropped and go out in action
// order on the shard's lane, so the k-th action is the k-th call), and its step is a HolderChanged
// naming the generation's holder at the run's term that some reading of that record, started
// after that call and ended before the step, shows: a Confirm answer, a Holdings answer or a
// value of the store watch.
func TestDriverContractO5Confirm(t *testing.T) {
	restores := 0
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		actions := make(map[shardKey]int) // Confirm actions so far
		for i, s := range nr.steps {
			for _, a := range s.Actions {
				switch a := a.(type) {
				case core.Confirm:
					actions[shardKey{s.Gen, a.Space, a.Shard}]++
				case core.Restore:
					restores++
					checkRestore(t, nr, i, a, actions[shardKey{s.Gen, a.Space, a.Shard}])
				}
			}
		}
	})
	if restores == 0 {
		t.Error("no Restore in any run")
	}
}

// checkRestore checks the Restore a of step i, after k Confirm actions for its shard.
func checkRestore(t *testing.T, nr *nodeRecord, i int, a core.Restore, k int) {
	t.Helper()
	h, at := nr.holder(nr.steps[i].Gen), nr.seqs[i]
	isConfirm := func(c call) bool {
		return c.Method == "Confirm" && holderOfCall(c) == h && argAt[string](c.Args, 1) == a.Space &&
			argAt[int](c.Args, 2) == a.Shard
	}
	var confirms []call
	for _, c := range nr.calls {
		if isConfirm(c) {
			confirms = append(confirms, c)
		}
	}
	if k == 0 || len(confirms) < k || confirms[k-1].Seq > at {
		t.Errorf("%s: %+v before the call of Confirm action %d went out (%d calls)", nr.where(i), a, k, len(confirms))
		return
	}
	from, fromAt := confirms[k-1].Seq, confirms[k-1].Start
	in, ok := nr.steps[i].Input.(core.HolderChanged)
	if !ok || in.Space != a.Space || in.Shard != a.Shard || in.Holder != h || in.Term != a.Term {
		t.Errorf("%s: %+v not on a HolderChanged naming %s at term %d", nr.where(i), a, h, a.Term)
		return
	}
	shows := func(x tenure.Holding) bool {
		return x.Space == a.Space && x.Shard == a.Shard && x.Holder == h && x.Term == a.Term
	}
	// A reading at the instant the Confirm call started counts as after it: the node stamps the
	// call when it hands it over, which the double does not see, and at one instant nothing else
	// orders the two.
	after := func(seq uint64, start time.Time) bool { return seq > from || start.Equal(fromAt) }
	for _, c := range nr.calls {
		if !c.Done || c.Err != nil || !after(c.Seq, c.Start) || c.EndSeq > at {
			continue
		}
		switch {
		case isConfirm(c) && shows(resultAt[tenure.Holding](c.Result, 0)):
			return
		case c.Method == "Holdings" && slices.ContainsFunc(resultAt[[]tenure.Holding](c.Result, 0), shows):
			return
		}
	}
	for _, s := range nr.sends {
		if after(s.seq, s.at) && s.seq < at && shows(s.h) {
			return
		}
	}
	t.Errorf("%s: %+v, but no reading of the record started after %v and ended before it", nr.where(i), a,
		confirms[k-1])
}

// actionText returns the record a store action works on, and its canonical text with its
// arguments; ok is false for anything else.
func actionText(a core.Action) (lane, text string, ok bool) {
	switch a := a.(type) {
	case core.KeepAlive:
		return "session", "KeepAlive", true
	case core.CloseSession:
		return "session", "CloseSession", true
	case core.ClaimSlot:
		return "slot", "ClaimSlot", true
	case core.ReleaseSlot:
		return "slot", "ReleaseSlot " + strconv.Itoa(a.Slot), true
	case core.Acquire:
		return a.Space + "/" + strconv.Itoa(a.Shard), "Acquire", true
	case core.Release:
		return a.Space + "/" + strconv.Itoa(a.Shard), fmt.Sprintf("Release %d", a.Term), true
	case core.Confirm:
		return a.Space + "/" + strconv.Itoa(a.Shard), fmt.Sprintf("Confirm %d", a.Term), true
	}
	return "", "", false
}

// callText is actionText for a recorded call of a store action.
func callText(c call) (lane, text string) {
	switch c.Method {
	case "KeepAlive", "CloseSession":
		return "session", c.Method
	case "ClaimSlot":
		return "slot", c.Method
	case "ReleaseSlot":
		return "slot", "ReleaseSlot " + strconv.Itoa(argAt[int](c.Args, 1))
	case "Acquire":
		return argAt[string](c.Args, 1) + "/" + strconv.Itoa(argAt[int](c.Args, 2)), c.Method
	case "Release", "Confirm":
		return argAt[string](c.Args, 1) + "/" + strconv.Itoa(argAt[int](c.Args, 2)),
			fmt.Sprintf("%s %d", c.Method, argAt[core.Term](c.Args, 3))
	}
	return "", ""
}

// laneAction is one store action of a machine on one record.
type laneAction struct {
	step  int
	text  string
	call  *call // the call that carried it out, nil while none
	moved bool  // a later action of the lane has a call
}

// TestDriverContractO6OrderAndRepeats checks O6 (RUN-130, RUN-140, RUN-160): per record (the
// session, the slots, each shard) and generation, the store calls of the generation's holder are
// its machine's actions in order, each call carrying out a distinct action whose step began before
// the call started: the driver repeats no call and reorders none. An action left without a call is
// a KeepAlive while an identical one has not started, a Release, ReleaseSlot, ClaimSlot or
// CloseSession while an identical one is out, or one of the last actions on the record of a
// machine whose session was lost or of a node whose Close ended early. A holder's first KeepAlive
// opens its session before its machine steps, and is no action.
func TestDriverContractO6OrderAndRepeats(t *testing.T) {
	var matched, dropped int
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		type laneOf struct {
			gen  uint64
			lane string
		}
		lanes := make(map[laneOf][]*laneAction)
		var keys []laneOf
		lostGen := make(map[uint64]bool)
		for i, s := range nr.steps {
			if s.After.Session == core.SessionStateLost {
				lostGen[s.Gen] = true
			}
			for _, a := range s.Actions {
				if lane, text, ok := actionText(a); ok {
					k := laneOf{s.Gen, lane}
					if lanes[k] == nil {
						keys = append(keys, k)
					}
					lanes[k] = append(lanes[k], &laneAction{step: i, text: text})
				}
			}
		}
		calls := make(map[laneOf][]call)
		for gen := uint64(1); gen <= uint64(len(nr.holders)); gen++ {
			opened := false
			for _, c := range nr.calls {
				if holderOfCall(c) != nr.holder(gen) {
					continue
				}
				if c.Method == "KeepAlive" && !opened {
					opened = true
					continue
				}
				lane, _ := callText(c)
				k := laneOf{gen, lane}
				if lanes[k] == nil && calls[k] == nil {
					keys = append(keys, k)
				}
				calls[k] = append(calls[k], c)
			}
		}
		for _, k := range keys {
			acts, next := lanes[k], 0
			for ci := range calls[k] {
				c := &calls[k][ci]
				_, text := callText(*c)
				j := next
				for j < len(acts) && (acts[j].text != text || nr.before(acts[j].step) > c.Seq) {
					j++
				}
				if j == len(acts) {
					t.Errorf("gen %d %s: %v at %v (seq %d) carries out no action of its machine in order", k.gen, k.lane, c,
						c.Start.Sub(nr.epoch), c.Seq)
					continue
				}
				acts[j].call = c
				for _, a := range acts[next:j] {
					a.moved = true
				}
				next = j + 1
				matched++
			}
			for j, a := range acts {
				if a.call != nil {
					continue
				}
				dropped++
				if !a.moved && (lostGen[k.gen] || nr.forced) {
					continue // dropped at the loss, or at a forced Close
				}
				at := nr.at(a.step)
				if !slices.ContainsFunc(acts[:j], func(e *laneAction) bool {
					if e.call == nil || e.text != a.text {
						return false
					}
					switch {
					case a.text == "KeepAlive":
						return !e.call.Start.Before(at) // still queued
					case a.text == "Acquire" || strings.HasPrefix(a.text, "Confirm"):
						return false // never dropped
					}
					return !e.call.Done || !e.call.End.Before(at) // still queued or in flight
				}) {
					t.Errorf("gen %d %s: %s of %s has no call, and no identical call was out then", k.gen, k.lane, a.text,
						nr.where(a.step))
				}
			}
		}
	})
	if matched == 0 || dropped == 0 {
		t.Errorf("%d calls carried out actions and %d actions dropped in all runs, want some of each", matched, dropped)
	}
}

// TestDriverContractO7Closing checks O7 (RUN-130, RUN-180, RUN-530): per generation, the
// CloseSession calls of its holder never outnumber its machine's CloseSession actions; no call of
// the holder of a machine that lost its session starts after the step of the loss; and no call of
// the store or the membership starts after Close returned, also when Close's ctx ended first.
func TestDriverContractO7Closing(t *testing.T) {
	var forced, losses int
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		actions := make(map[uint64]int)
		loss := make(map[uint64]uint64) // the stamp of the step of the loss, by generation
		for i, s := range nr.steps {
			for _, a := range s.Actions {
				if _, ok := a.(core.CloseSession); ok {
					actions[s.Gen]++
				}
			}
			if s.Before.Session != core.SessionStateLost && s.After.Session == core.SessionStateLost {
				loss[s.Gen] = nr.seqs[i]
				losses++
			}
		}
		for gen := uint64(1); gen <= uint64(len(nr.holders)); gen++ {
			h, n := nr.holder(gen), 0
			for _, c := range nr.calls {
				if holderOfCall(c) != h {
					continue
				}
				if c.Method == "CloseSession" {
					n++
				}
				if at, ok := loss[gen]; ok && c.Seq > at {
					t.Errorf("gen %d: %v started after the loss", gen, c)
				}
			}
			if n > actions[gen] {
				t.Errorf("gen %d: %d CloseSession calls of %s for %d actions", gen, n, h, actions[gen])
			}
		}
		if nr.forced {
			forced++
		}
		for _, c := range slices.Concat(nr.calls, nr.mcalls) {
			if c.Seq > nr.closeSeq {
				t.Errorf("%v started after Close returned (forced %t)", c, nr.forced)
			}
		}
	})
	if forced == 0 || losses == 0 {
		t.Errorf("%d forced Closes and %d lost sessions in all runs, want some of each", forced, losses)
	}
}

// TestDriverContractO8Views checks O8 (RUN-210), in its strong form: no view stepped into a live
// machine after a SlotClaimed{s} shows s vacant or held by another, until the machine gives s
// back (ReleaseSlot{s}) or steps SlotReleased{s}; and none after a SlotReleased{s} shows s held by
// the machine's holder, until it claims again. A view that no longer counts s shows nothing of it.
// While the machine's session is live, the store's session is too, so its slot data cannot have
// moved on.
func TestDriverContractO8Views(t *testing.T) {
	var claims, views int
	eachNode(t, func(t *testing.T, _ *contractRun, nr *nodeRecord) {
		var gen uint64
		var claimed, released map[int]bool
		for i, s := range nr.steps {
			if s.Gen != gen {
				gen, claimed, released = s.Gen, make(map[int]bool), make(map[int]bool)
			}
			h := nr.holder(gen)
			switch in := s.Input.(type) {
			case core.SlotClaimed:
				claimed[in.Slot], released[in.Slot] = true, false
				claims++
			case core.SlotReleased:
				claimed[in.Slot], released[in.Slot] = false, true
			case core.ViewChanged:
				if s.After.Session != core.SessionStateLive {
					break
				}
				v := in.View
				for slot, on := range claimed {
					if on && slot < v.Slots && v.SlotHolders[slot] != h {
						t.Errorf("%s: slot %d held by %q after SlotClaimed{%d}", nr.where(i), slot, v.SlotHolders[slot], slot)
					}
				}
				for slot, on := range released {
					if on && slot < v.Slots && v.SlotHolders[slot] == h {
						t.Errorf("%s: slot %d still ours after SlotReleased{%d}", nr.where(i), slot, slot)
					}
				}
				views++
			}
			for _, a := range s.Actions {
				switch a := a.(type) {
				case core.ReleaseSlot:
					claimed[a.Slot] = false
				case core.ClaimSlot:
					clear(released)
				}
			}
		}
	})
	if claims == 0 || views == 0 {
		t.Errorf("%d SlotClaimed and %d views checked in all runs, want some of each", claims, views)
	}
}
