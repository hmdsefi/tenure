// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/simtest/internal/check"
)

// Routers (AT-RTR-13, RTR-400 to RTR-430). A router is a model of a tenure.Router in event
// mode: it starts no goroutine, and everything it does happens in the events of the run.
//
// Views. A router watches the store, the membership source of the world: every new view of
// the store reaches it its own delay later, drawn once per router from [2·Delta, RouterLag],
// so it never sees a view before the members the store pushed it to, and lags them by up to
// the delay. It applies each view, with each member's Slot as the runtime merges it (RUN-220),
// to a core.Assigner at the global time, which is its monotonic reading, and ticks the
// Assigner when a slot's vacancy expires. Each applied view, and each tick that changes an
// assignment, is a new snapshot, whose version counts them (D10). Under the Observer rule the
// store's view keeps a member that stopped renewing, which the members' own views drop: such a
// router lags for as long as the member is gone, and corrections cover it. A RouterCut drops
// the views the store makes while it lasts and every delivery due then; at its end each router
// reads the store's latest view again, which reaches it its delay later.
//
// Corrections (RTR-400, RTR-420). A correction names the member that serves one shard, and
// records the snapshot version and the time it was made. A lookup of the shard returns it
// while the version is the router's and it is younger than the correction TTL, the scenario's
// LeaseDuration; otherwise the table's member. A correction is kept only for a member of the
// router's view.
//
// Requests. Every routerEvery each router sends one request for a random key of a random
// space, to the member its lookup names, as routehttp.Transport would (RTR-810), all in the
// event of the request: the member answers from its state in that event. A member serves the
// request when it runs the key's shard and the run's check passes, as Node.Local does; else
// it answers 421 with the member its own route names (its machine's assignment and its latest
// view, as Node.Route), or 503 when it knows none. A member that is not active, is down or
// paused, or has no view yet does not answer, as a dial error. After a 421 the router corrects
// the shard and sends to its lookup again, or to the named member when the correction was
// refused; after a 503 or no answer it sends to its fallback, a random member that would
// answer, as a Kubernetes Service sends to a ready pod. It stops after routerSends sends: one
// and MaxRedirects (2) resends. A request a member of a fenced space serves writes in the
// run's term; the fence accepts it when the term is at least the shard's raised term, and the
// checker judges an accepted write as it judges every other (g3).
//
// Checks, after every event (tenure/router/bound, tenure/router/converged,
// tenure/router/owner):
//
//   - bound (RTR-430): a request sent while some member serves its shard reaches a member that
//     serves it within routerSends sends, when every member that answered it named a member
//     that serves the shard: the owner named by the member's own view, as RTR-430 states the
//     bound. A member whose view differs from the owner's answers otherwise, and that is the
//     members' disagreement, not the router's;
//   - converged (AT-RTR-13): a request sent while the router's view equals the latest view of
//     every active member, the view shows no vacant slot, every active member's route names
//     one member for the shard, that member serves it, and the router holds no live
//     correction of the shard naming another member, takes exactly one send. A vacant slot is
//     left out because each process expires a vacancy at its own readings, and a live
//     correction because it lapses only at the router's next snapshot or its TTL (see
//     convergedFor);
//   - owner: a member that serves a request owns the shard by the checker's ownership, so a
//     router never makes a member serve a shard it does not hold.

// The labels and the stream of the routers.
const (
	labelRouterView    = "tenure/router/view"
	labelRouterTick    = "tenure/router/tick"
	labelRouterRequest = "tenure/router/request"
	labelRouterCut     = "tenure/router/cut"
	labelRouterHeal    = "tenure/router/heal"
	streamRouter       = "tenure/router/"
)

// The names of the routers' invariants.
const (
	invariantRouterBound     = invariantPrefix + "router/bound"
	invariantRouterConverged = invariantPrefix + "router/converged"
	invariantRouterOwner     = invariantPrefix + "router/owner"
)

const (
	// routerEvery is the time between two requests of a router.
	routerEvery = 50 * time.Millisecond
	// routerSends is the most sends of a request: the first, then MaxRedirects (2) resends
	// (RTR-430).
	routerSends = 3
)

// RouterStats counts what one router did in a run.
type RouterStats struct {
	// Name is the router's name, "r1" for the first.
	Name string
	// Delay is the router's view delay.
	Delay time.Duration
	// Views counts the views the router applied, and Snapshots its snapshots: one per view
	// applied and one per tick that changed an assignment.
	Views     int
	Snapshots int
	// Requests counts the requests, Served those a member served, and Sends the sends of all
	// of them. Resent counts the served requests that took more than one send.
	Requests int
	Served   int
	Sends    int
	Resent   int
	// Bounded counts the requests the bound of three sends applied to, and Converged those
	// sent in a converged state, which must take one send.
	Bounded   int
	Converged int
	// Corrections counts the corrections kept, Refused those refused, and Fallbacks the sends
	// to the fallback.
	Corrections int
	Refused     int
	Fallbacks   int
	// Rejected counts the served requests of a fenced space whose write the fence rejected.
	Rejected int
}

// correction is a correction of one shard (RTR-400): the member that serves it, and the
// snapshot version and the reading at which it was made.
type correction struct {
	member  string
	version uint64
	at      time.Duration
}

// routerTable is a router's table, without the world: its Assigner, its view and its
// corrections, at readings the caller gives.
type routerTable struct {
	assigner *core.Assigner
	ttl      time.Duration // the correction TTL

	view    core.View // the applied view, with each member's Slot; valid when viewed
	viewed  bool
	version uint64 // the snapshot version, 0 before the first view

	fixes map[spaceShard]correction // looked up only
}

// newRouterTable returns the table of a router of spaces under timing, with no view applied.
// The correction TTL is timing's LeaseDuration.
func newRouterTable(spaces []core.SpaceSpec, timing core.Timing) *routerTable {
	as := make([]core.AssignerSpace, len(spaces))
	for i, sp := range spaces {
		as[i] = core.AssignerSpace{Name: sp.Name, Shards: sp.Shards}
	}
	a, err := core.NewAssigner(as, timing)
	if err != nil {
		panic(fmt.Sprintf("world: router: %v", err)) // the setup checked the spaces and the timing
	}
	return &routerTable{assigner: a, ttl: timing.LeaseDuration, fixes: make(map[spaceShard]correction)}
}

// apply applies the view v, with each member's Slot, at the reading now, and reports whether it
// did: a view older than the applied one is ignored. An applied view is a new snapshot.
func (t *routerTable) apply(v core.View, now time.Duration) bool {
	if t.viewed && v.Version < t.view.Version {
		return false
	}
	t.assigner.ApplyView(v, now, kernel.Epoch.Add(now))
	t.view, t.viewed = v, true
	t.version++
	return true
}

// tick moves the table's clock to now and reports whether an assignment changed, which is a
// new snapshot.
func (t *routerTable) tick(now time.Duration) bool {
	if !t.assigner.Tick(now) {
		return false
	}
	t.version++
	return true
}

// lookup returns the member that the table routes the shard to at the reading now: the
// shard's live correction, else the member the assignment names. It returns "" and why when
// there is none: "no view yet", "no placement keys", "slot-1 is vacant" or "m4 is not in the
// view", the details of RTR-230.
func (t *routerTable) lookup(space string, shard int, now time.Duration) (member, why string) {
	if c, ok := t.live(space, shard, now); ok {
		return c, ""
	}
	delete(t.fixes, spaceShard{space, shard})
	if !t.viewed {
		return "", "no view yet"
	}
	return routeOf(t.view, t.assigner.Assignment(space), shard, -1, "")
}

// live returns the member of the shard's correction, and true, while the correction is live
// at the reading now: it was made in the present snapshot and is younger than the TTL.
func (t *routerTable) live(space string, shard int, now time.Duration) (string, bool) {
	c, ok := t.fixes[spaceShard{space, shard}]
	if !ok || c.version != t.version || now-c.at >= t.ttl {
		return "", false
	}
	return c.member, true
}

// correct records that member serves the shard, at the reading now, and reports whether it
// kept the correction: only a member of the router's view is kept (RTR-400).
func (t *routerTable) correct(space string, shard int, member string, now time.Duration) bool {
	if !t.viewed || !hasMember(t.view, member) {
		return false
	}
	t.fixes[spaceShard{space, shard}] = correction{member: member, version: t.version, at: now}
	return true
}

// vacancy reports whether the applied view shows a slot vacant.
func (t *routerTable) vacancy() bool {
	return slices.Contains(t.view.SlotHolders, "")
}

// routeOf returns the member that view v and assign, an assignment computed from it, route the
// shard to, as the runtime's routes.route does: the shard's placement key, and the member
// whose ID it is or, for a slot key, the member whose ID is the ID part of the slot's holder.
// held is the slot the router holds, -1 for none, which routes to self. It returns "" and why
// when there is none.
func routeOf(v core.View, assign []string, shard int, held int, self string) (member, why string) {
	key := ""
	if shard >= 0 && shard < len(assign) {
		key = assign[shard]
	}
	if key == "" {
		return "", "no placement keys"
	}
	id := key
	if s, ok := slotOfKey(v, key); ok {
		switch h := v.SlotHolders[s]; {
		case s == held:
			id = self
		case h == "":
			return "", key + " is vacant"
		default:
			id = sessionMember(h)
		}
	}
	if !hasMember(v, id) {
		return "", id + " is not in the view"
	}
	return id, ""
}

// slotOfKey returns the slot a placement key slot-<i> names, with i one of v's slots, and false
// for any other key.
func slotOfKey(v core.View, key string) (int, bool) {
	if v.Slots == 0 {
		return 0, false
	}
	digits, ok := strings.CutPrefix(key, "slot-")
	if !ok {
		return 0, false
	}
	s, err := strconv.Atoi(digits)
	if err != nil || s < 0 || s >= v.Slots {
		return 0, false
	}
	return s, true
}

// hasMember reports whether v lists the member id.
func hasMember(v core.View, id string) bool {
	return slices.ContainsFunc(v.Members, func(m core.Member) bool { return m.ID == id })
}

// answerKind is how a member answers a request.
type answerKind uint8

const (
	// answerServed: the member runs the shard and serves the request.
	answerServed answerKind = iota
	// answerMisdirected: 421 with the member the answering member's route names.
	answerMisdirected
	// answerUnavailable: 503, the member knows no member that serves the shard.
	answerUnavailable
	// answerNone: no answer, as a dial error.
	answerNone
)

// answer is a member's answer to one send of a request.
type answer struct {
	kind answerKind
	// route is the member a misdirected answer names.
	route string
	// holder and term are those of the run that served the request.
	holder string
	term   core.Term
}

// hop is one send of a request: the member it went to, whether that was the fallback, and
// the answer.
type hop struct {
	to       string
	fallback bool
	ans      answer
}

// String returns the hop as the path of tenure.route prints it: "m3 served", "m3 421 m2",
// "m3 503" or "m3 none", after "fallback " for a send to the fallback.
func (h hop) String() string {
	s := h.to
	if h.fallback {
		s = "fallback " + s
	}
	switch h.ans.kind {
	case answerServed:
		return s + " served"
	case answerMisdirected:
		return s + " 421 " + h.ans.route
	case answerUnavailable:
		return s + " 503"
	default:
		return s + " none"
	}
}

// path returns the hops as tenure.route prints them, joined by commas.
func path(hops []hop) string {
	parts := make([]string, len(hops))
	for i, h := range hops {
		parts[i] = h.String()
	}
	return strings.Join(parts, ", ")
}

// served reports whether the last hop was served.
func served(hops []hop) bool {
	return len(hops) > 0 && hops[len(hops)-1].ans.kind == answerServed
}

// sendRequest sends one request for the shard through the table t at the reading now, as
// routehttp.Transport does (RTR-810), and returns its sends: to the table's member, or to the
// fallback when the table has none; after a 421, to the table's member once the table took the
// correction, else to the member the 421 named; after a 503 or no answer, to the fallback. It
// stops when a member serves the request, after routerSends sends, or when fallback has no
// member. ask is a member's answer; fallback returns the member a send to the fallback goes
// to, and false when there is none.
func sendRequest(t *routerTable, space string, shard int, now time.Duration,
	ask func(member string) answer, fallback func() (string, bool), onCorrect func(kept bool)) []hop {
	hops := make([]hop, 0, routerSends)
	to, _ := t.lookup(space, shard, now)
	toFallback := to == ""
	for len(hops) < routerSends {
		if toFallback {
			var ok bool
			if to, ok = fallback(); !ok {
				break
			}
		}
		a := ask(to)
		hops = append(hops, hop{to: to, fallback: toFallback, ans: a})
		switch a.kind {
		case answerServed:
			return hops
		case answerMisdirected:
			kept := t.correct(space, shard, a.route, now)
			if onCorrect != nil {
				onCorrect(kept)
			}
			to = a.route
			if kept {
				to, _ = t.lookup(space, shard, now)
			}
			toFallback = to == ""
		default:
			toFallback = true
		}
	}
	return hops
}

// router is one router of the world.
type router struct {
	w     *World
	name  string
	delay time.Duration
	rand  *rand.Rand // tenure/router/<name>
	table *routerTable

	cut  bool           // the router is cut off from the store's views
	wake kernel.EventID // the tick at the Assigner's NextWake; 0 for none

	stats RouterStats
}

// routerWatch holds the routers of a world and the first violation of each of their checks.
type routerWatch struct {
	routers []*router
	errs    map[string]error // by invariant name
}

// installRouters adds the scenario's routers, with their view delays drawn in order, makes them
// watch the store, schedules their requests and the RouterCut, and registers the routers'
// invariants. A scenario without routers installs nothing.
func (w *World) installRouters() {
	if w.sc.Routers == 0 {
		return
	}
	rw := &routerWatch{errs: make(map[string]error)}
	w.routers = rw
	lo := 2 * w.sc.Delta
	hi := max(lo, w.sc.RouterLag)
	for i := 1; i <= w.sc.Routers; i++ {
		name := "r" + strconv.Itoa(i)
		r := &router{
			w:     w,
			name:  name,
			rand:  w.sim.Rand(streamRouter + name),
			table: newRouterTable(w.sc.Spaces, w.sc.Timing),
		}
		r.delay = lo + time.Duration(r.rand.Int64N(int64(hi-lo)+1))
		r.stats = RouterStats{Name: name, Delay: r.delay}
		rw.routers = append(rw.routers, r)
		first := kernel.Time(r.rand.Int64N(int64(routerEvery)))
		w.sim.At(first, labelRouterRequest, r.request)
	}
	w.storeSrv.onView = w.routersWatch
	if c := w.sc.RouterCut; c != nil {
		w.sim.At(kernel.Time(c.At), labelRouterCut, func() { w.cutRouters(true) })
		w.sim.At(kernel.Time(c.At+c.For), labelRouterHeal, func() { w.cutRouters(false) })
	}
	for _, name := range []string{invariantRouterBound, invariantRouterConverged, invariantRouterOwner} {
		w.fw.Invariant(name, func() error { return rw.errs[name] })
	}
}

// RouterStats returns what each router did so far, in the order of the routers, and nil for a
// world without routers.
func (w *World) RouterStats() []RouterStats {
	if w.routers == nil {
		return nil
	}
	out := make([]RouterStats, len(w.routers.routers))
	for i, r := range w.routers.routers {
		out[i] = r.stats
	}
	return out
}

// routersWatch is the store's onView: each router that is not cut off gets v its delay later.
func (w *World) routersWatch(v core.View) {
	for _, r := range w.routers.routers {
		if !r.cut {
			r.send(v)
		}
	}
}

// send schedules the delivery of v to r, its delay from now.
func (r *router) send(v core.View) {
	r.w.sim.At(r.w.sim.Now().Add(r.delay), labelRouterView, func() { r.deliver(v) })
}

// cutRouters cuts every router off from the store's views, or ends the cut: then each router
// reads the store's latest view again.
func (w *World) cutRouters(cut bool) {
	for _, r := range w.routers.routers {
		r.cut = cut
		w.emitRouterCut(r, cut)
		if !cut {
			r.send(w.storeSrv.model.currentView())
		}
	}
}

// now returns the router's reading: the global time.
func (r *router) now() time.Duration { return time.Duration(r.w.sim.Now()) }

// deliver applies the store's view v, unless the router is cut off, and keeps the tick at the
// Assigner's next wake.
func (r *router) deliver(v core.View) {
	if r.cut {
		return
	}
	if !r.table.apply(withSlots(v), r.now()) {
		return
	}
	r.stats.Views++
	r.stats.Snapshots++
	r.w.emitRouterView(r, "view")
	r.rearm()
}

// rearm keeps the tick at the Assigner's NextWake.
func (r *router) rearm() {
	if r.wake != 0 {
		r.w.sim.Cancel(r.wake)
		r.wake = 0
	}
	if at, ok := r.table.assigner.NextWake(); ok {
		r.wake = r.w.sim.At(max(kernel.Time(at), r.w.sim.Now()), labelRouterTick, r.onTick)
	}
}

// onTick ticks the Assigner; an assignment that changed is a new snapshot.
func (r *router) onTick() {
	r.wake = 0
	if r.table.tick(r.now()) {
		r.stats.Snapshots++
		r.w.emitRouterView(r, "tick")
	}
	r.rearm()
}

// request sends one request for a random key of a random space, checks it, and schedules the
// next request.
func (r *router) request() {
	w := r.w
	sp := w.sc.Spaces[r.rand.IntN(len(w.sc.Spaces))]
	key := "k" + strconv.FormatUint(r.rand.Uint64(), 16)
	shard := core.KeyShard(key, sp.Shards)
	holders := w.servingMembers(sp.Name, shard)
	conv := w.convergedFor(r.table, sp.Name, shard, r.now())

	hops := sendRequest(r.table, sp.Name, shard, r.now(),
		func(id string) answer { return w.ask(id, sp.Name, shard) },
		r.fallback,
		func(kept bool) {
			if kept {
				r.stats.Corrections++
			} else {
				r.stats.Refused++
			}
		})
	r.count(hops)
	result := "failed"
	if served(hops) {
		result = "served"
		r.write(sp, shard, hops[len(hops)-1].ans)
	}
	w.emitRoute(r, sp.Name, shard, len(hops), result, path(hops))
	r.judge(sp.Name, shard, holders, conv, hops)
	w.sim.At(w.sim.Now().Add(routerEvery), labelRouterRequest, r.request)
}

// count adds a request's sends to the router's counts.
func (r *router) count(hops []hop) {
	r.stats.Requests++
	r.stats.Sends += len(hops)
	for _, h := range hops {
		if h.fallback {
			r.stats.Fallbacks++
		}
	}
	if served(hops) {
		r.stats.Served++
		if len(hops) > 1 {
			r.stats.Resent++
		}
	}
}

// write is the write of a served request of the space sp: in a fenced space the fence judges
// it by the shard's raised term, and the checker judges an accepted one (g3).
func (r *router) write(sp core.SpaceSpec, shard int, a answer) {
	if !sp.Fenced {
		return
	}
	if a.term < r.w.fenceSrv.model.raisedOf(sp.Name, shard) {
		r.stats.Rejected++
		return
	}
	r.w.checker.Accepted(check.Write{Space: sp.Name, Shard: shard, Term: a.term, Holder: a.holder})
}

// judge decides the routers' checks for one request: holders are the members that served the
// shard when it was sent, conv whether it was sent in a converged state.
func (r *router) judge(space string, shard int, holders []string, conv bool, hops []hop) {
	errs := r.w.routers.errs
	fail := func(name, format string, args ...any) {
		if errs[name] == nil {
			errs[name] = fmt.Errorf("router: %s: %s/%d: "+format, append([]any{r.name, space, shard}, args...)...)
		}
	}
	if len(holders) > 0 && namedHolders(hops, holders) {
		r.stats.Bounded++
		if !served(hops) {
			fail(invariantRouterBound, "not served in %d sends while %s serves it: %s",
				routerSends, strings.Join(holders, ", "), path(hops))
		}
	}
	if conv {
		r.stats.Converged++
		if len(hops) != 1 || !served(hops) {
			fail(invariantRouterConverged, "%d sends in a converged state, want 1 send that is served: %s",
				len(hops), path(hops))
		}
	}
	if served(hops) {
		a := hops[len(hops)-1].ans
		if !slices.Contains(r.w.checker.Owners(space, shard), a.holder) {
			fail(invariantRouterOwner, "served by %s, which does not own it", a.holder)
		}
	}
}

// namedHolders reports whether every member that answered a send of the request named one of
// holders: it served the request, or its 421 named one of them. A 503 names none.
func namedHolders(hops []hop, holders []string) bool {
	for _, h := range hops {
		switch h.ans.kind {
		case answerMisdirected:
			if !slices.Contains(holders, h.ans.route) {
				return false
			}
		case answerUnavailable:
			return false
		}
	}
	return true
}

// fallback returns a random member that would answer a request, as a Service sends to a ready
// pod, and false when there is none.
func (r *router) fallback() (string, bool) {
	var ready []string
	for _, m := range r.w.members {
		if r.w.answering(m) != nil {
			ready = append(ready, m.Name)
		}
	}
	if len(ready) == 0 {
		return "", false
	}
	return ready[r.rand.IntN(len(ready))], true
}

// answering returns the driver of m when m answers requests: m is active and up, and drives its
// present incarnation, which has stepped a view. It returns nil otherwise.
func (w *World) answering(m *Member) *driver {
	if m.phase != Active || m.Node.State() != kernel.NodeUp {
		return nil
	}
	d := w.drivers.current(m)
	if d == nil || d.stopped || d.holder != m.holder || !d.showing {
		return nil
	}
	return d
}

// memberNamed returns the member whose name is name, or nil.
func (w *World) memberNamed(name string) *Member {
	for _, m := range w.members {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// ask returns the answer of the member called id to a request for the shard.
func (w *World) ask(id, space string, shard int) answer {
	m := w.memberNamed(id)
	if m == nil {
		return answer{kind: answerNone}
	}
	d := w.answering(m)
	if d == nil {
		return answer{kind: answerNone}
	}
	if run := d.serving(space, shard); run != nil {
		return answer{kind: answerServed, holder: d.holder, term: run.term}
	}
	if to, _ := d.route(space, shard); to != "" {
		return answer{kind: answerMisdirected, route: to}
	}
	return answer{kind: answerUnavailable}
}

// servingMembers returns the members that serve the shard now, in the order of their nodes.
func (w *World) servingMembers(space string, shard int) []string {
	var out []string
	for _, m := range w.members {
		if d := w.answering(m); d != nil && d.serving(space, shard) != nil {
			out = append(out, m.Name)
		}
	}
	return out
}

// convergedFor reports whether a request for the shard through t, at the reading now, is sent
// in a converged state: t has a view that shows no vacant slot; every active member answers
// requests and its latest view equals t's; every one of them routes the shard to one member;
// that member serves it; and t holds no live correction of the shard that names another
// member. A correction made while the members routed the shard elsewhere stays live until the
// router's next snapshot or its TTL (RTR-420), also when the members' views come back to the
// router's, as when a member that left comes back under the same ID: the router has not
// converged until it lapses.
func (w *World) convergedFor(t *routerTable, space string, shard int, now time.Duration) bool {
	if !t.viewed || t.vacancy() {
		return false
	}
	owner := ""
	for _, m := range w.members {
		if m.phase != Active {
			continue
		}
		d := w.answering(m)
		if d == nil || !sameView(d.shown, t.view) {
			return false
		}
		to, _ := d.route(space, shard)
		switch {
		case to == "":
			return false
		case owner == "":
			owner = to
		case to != owner:
			return false
		}
	}
	if owner == "" {
		return false
	}
	if c, ok := t.live(space, shard, now); ok && c != owner {
		return false
	}
	m := w.memberNamed(owner)
	if m == nil {
		return false
	}
	d := w.answering(m)
	return d != nil && d.serving(space, shard) != nil
}

// serving returns the run of the driver that serves the shard: one that has not stopped and
// whose check passes, as Node.Local asks of a Shard. It returns nil when there is none.
func (d *driver) serving(space string, shard int) *run {
	for _, r := range d.runs {
		if r.space == space && r.shard == shard && !r.done && r.check() == "" {
			return r
		}
	}
	return nil
}

// route returns the member that the driver's member routes the shard to, as Node.Route does:
// by its machine's assignment and its latest view, with the slot its machine holds routed to
// itself. It returns "" and why when there is none.
func (d *driver) route(space string, shard int) (member, why string) {
	held := -1
	if d.shown.Slots > 0 {
		// The machine's Status copies every shard's state, so the held slot is read once per
		// step of the machine.
		if !d.heldRead || d.heldStep != d.steps {
			d.heldSlot, d.heldStep, d.heldRead = d.status().Slot, d.steps, true
		}
		held = d.heldSlot
	}
	return routeOf(d.shown, d.machine.Assignment(space), shard, held, d.m.Name)
}
