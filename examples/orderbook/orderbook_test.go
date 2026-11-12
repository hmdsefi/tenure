// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/examples/internal/fencedb"
)

// raiseEvent is one call of the raised hook: node n raised the fence of a shard to a term.
type raiseEvent struct {
	node  int
	shard int
	term  core.Term
}

// commitEvent is one call of the commit hook on node n.
type commitEvent struct {
	node int
	attempt
}

// observer collects what the engine hooks of every node see.
type observer struct {
	mu       sync.Mutex
	raises   []raiseEvent
	attempts []commitEvent
}

// hook installs the observer's hooks into the orderbook of node n.
func (ob *observer) hook(n int, o *orderbook) {
	o.raised = func(shard int, term core.Term) {
		ob.mu.Lock()
		defer ob.mu.Unlock()
		ob.raises = append(ob.raises, raiseEvent{n, shard, term})
	}
	o.commit = func(shard int, term core.Term, id string, err error) {
		ob.mu.Lock()
		defer ob.mu.Unlock()
		ob.attempts = append(ob.attempts, commitEvent{n, attempt{shard, term, id, err}})
	}
}

// staleFrom returns the commit attempts of node n that the fence rejected.
func (ob *observer) staleFrom(n int) []commitEvent {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	var out []commitEvent
	for _, a := range ob.attempts {
		if a.node == n && errors.Is(a.err, fencedb.ErrStale) {
			out = append(out, a)
		}
	}
	return out
}

// raisedAbove reports whether some node raised the fence of a shard to a term above term.
func (ob *observer) raisedAbove(shard int, term core.Term) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return slices.ContainsFunc(ob.raises, func(r raiseEvent) bool { return r.shard == shard && r.term > term })
}

// snapshot returns copies of the recorded events.
func (ob *observer) snapshot() ([]raiseEvent, []commitEvent) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return slices.Clone(ob.raises), slices.Clone(ob.attempts)
}

// obCluster starts a cluster of three nodes over db, with the hooks of ob when it is not nil,
// and closes it when the test is over.
func obCluster(t *testing.T, ctx context.Context, db *fencedb.DB, ob *observer) (*demo.Cluster, []*orderbook) {
	t.Helper()
	books := newBooks(db, nodeCount)
	if ob != nil {
		for i, o := range books {
			ob.hook(i, o)
		}
	}
	c, err := startCluster(ctx, books)
	if err != nil {
		t.Fatalf("start the cluster: %v", err)
	}
	t.Cleanup(func() {
		for _, cut := range c.Cuts {
			cut.Heal()
		}
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.Close(cctx); err != nil {
			t.Errorf("close the cluster: %v", err)
		}
	})
	return c, books
}

// answer is one HTTP answer of a node.
type answer struct {
	status int
	header http.Header
	body   []byte
}

// postOrder sends one order to url, once: it follows no route and retries nothing.
func postOrder(ctx context.Context, hc *http.Client, url string, body []byte) (answer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/orders", bytes.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return answer{resp.StatusCode, resp.Header, b}, err
}

// orderJSON returns the JSON of a valid order.
func orderJSON(t *testing.T, o order) []byte {
	t.Helper()
	b, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// loadIDs returns the IDs that a load with the prefix, the clients and the orders posts.
func loadIDs(prefix string, clients, orders int) []string {
	var ids []string
	for i := range clients {
		n := orders / clients
		if i < orders%clients {
			n++
		}
		for k := range n {
			ids = append(ids, fmt.Sprintf("%s%d-%d", prefix, i, k))
		}
	}
	return ids
}

// pollUntil polls cond every 5 ms until it holds, and fails the test when ctx ends first.
func pollUntil(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestOrderbook is the property the example exists to show. Three nodes serve 16 shards of
// markets over HTTP; clients post orders; n3 is drained; n2 is cut from the store, keeps running
// because the space is fenced, and keeps taking orders until the fence has rejected one of its
// commits; then everything heals. The journal must be a single-owner history: its commits never
// go back to a lower term, every rejected commit shows in the hooks and not in the history, every
// order that got a 200 is journaled once, and the drained node owns nothing while its shards were
// raised again with higher terms. The steps are the four assertions of EXM-250 (AT-EXM-09),
// written from the issue's text.
func TestOrderbook(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Second)
	defer cancel()

	db := fencedb.New()
	ob := new(observer)
	c, books := obCluster(t, ctx, db, ob)
	hc := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(hc.CloseIdleConnections)

	if _, err := waitOwners(ctx, c, nil); err != nil {
		t.Fatalf("wait for the owners of the shards: %v", err)
	}
	const cut, drainedNode = 1, 2 // n2 is cut, n3 is drained
	stay := []string{c.URL(0), c.URL(1)}

	accepted := make(map[string]bool) // IDs of the orders that got a 200

	// Phase 1: orders through n1 and n2 while the three nodes serve.
	one := &load{hc: hc, urls: stay, clients: 4, orders: 120, seed: 1}
	if err := one.run(ctx); err != nil {
		t.Fatalf("first orders: %v", err)
	}
	for _, id := range loadIDs("c", 4, 120) {
		accepted[id] = true
	}

	// Phase 2: drain n3. Remember the shards it held and their terms.
	held := make(map[int]core.Term)
	before := owners(c)
	if before == nil {
		t.Fatal("a shard has no owner")
	}
	for shard, o := range before {
		if o.id == memberID(drainedNode) {
			held[shard] = core.Term(o.term)
		}
	}
	if len(held) == 0 {
		t.Fatalf("%s holds no shard before its drain", memberID(drainedNode))
	}
	if err := c.Nodes[drainedNode].Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := waitOwners(ctx, c, func(m map[int]owner) bool { return !ownedBy(m, memberID(drainedNode)) }); err != nil {
		t.Fatalf("wait for the shards of the drained node to move: %v", err)
	}

	// Phase 3: cut n2, keep sending it orders directly until the fence rejected one of its
	// commits, while other clients go on through n1.
	c.Cuts[cut].Cut()
	if err := c.Groups[cut].SetDraining(ctx, true); err != nil {
		t.Fatalf("mark the cut node as draining in the group: %v", err)
	}
	two := &load{hc: hc, urls: stay[:1], clients: 2, orders: 60, seed: 2, prefix: "d"}
	twoDone := make(chan error, 1)
	go func() { twoDone <- two.run(ctx) }()

	staleCtx, staleCancel := context.WithTimeout(ctx, 25*time.Second)
	defer staleCancel()
	var rejected []string // IDs posted to n2 that did not get a 200
	for i := 0; len(ob.staleFrom(cut)) == 0; i++ {
		select {
		case <-staleCtx.Done():
			t.Fatal("the fence rejected no commit of the cut node")
		case <-time.After(5 * time.Millisecond):
		}
		market := marketName(i % marketCount)
		if _, err := c.Nodes[cut].Local(marketsSpace, market); err != nil {
			continue // n2 does not believe it owns the shard
		}
		id := fmt.Sprintf("x-%d", i)
		a, err := postOrder(staleCtx, hc, c.URL(cut), orderJSON(t, order{ID: id, Market: market,
			Side: []string{sideBuy, sideSell}[i%2], Price: 90 + int64(i%21), Qty: 1 + int64(i%10)}))
		if err != nil {
			t.Fatalf("post %s to the cut node: %v", id, err)
		}
		switch a.status {
		case http.StatusOK:
			accepted[id] = true
		case http.StatusServiceUnavailable:
			if a.header.Get("Retry-After") != "1" {
				t.Errorf("503 for %s without Retry-After: 1: %v", id, a.header)
			}
			rejected = append(rejected, id)
		case http.StatusMisdirectedRequest:
		default:
			t.Fatalf("post %s to the cut node: status %d: %s", id, a.status, a.body)
		}
	}

	// Phase 4: heal, let the clients finish, and stop the nodes so that the journal rests.
	c.Cuts[cut].Heal()
	if err := <-twoDone; err != nil {
		t.Fatalf("orders during the cut: %v", err)
	}
	for _, id := range loadIDs("d", 2, 60) {
		accepted[id] = true
	}
	pollUntil(t, ctx, "the shards of the drained node to be raised again", func() bool {
		for shard, term := range held {
			if !ob.raisedAbove(shard, term) {
				return false
			}
		}
		return true
	})

	// The drained node owns nothing, and no Local of it names it as owner.
	for _, sp := range c.Nodes[drainedNode].Table().Spaces {
		for _, row := range sp.Rows {
			if row.State == "owned" || row.State == "degraded" || row.Running {
				t.Errorf("after its drain %s still runs shard %d: state %q, running %v",
					memberID(drainedNode), row.Shard, row.State, row.Running)
			}
		}
	}
	for i := range marketCount {
		if _, err := c.Nodes[drainedNode].Local(marketsSpace, marketName(i)); !errors.Is(err, tenure.ErrNotOwner) {
			t.Errorf("Local(%s) on the drained node = %v, want ErrNotOwner", marketName(i), err)
		}
	}

	cctx, ccancel := context.WithTimeout(ctx, 20*time.Second)
	defer ccancel()
	if err := c.Close(cctx); err != nil {
		t.Fatalf("close the cluster: %v", err)
	}
	raises, attempts := ob.snapshot()

	// Read every journal and history once.
	type shardState struct {
		lines   [][]byte
		history []fencedb.Commit
	}
	shards := make([]shardState, marketShards)
	for s := range shards {
		if err := db.View(marketsSpace, s, func(tx *fencedb.Tx) error {
			shards[s].lines = tx.Entries(journalLog)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		shards[s].history = db.History(marketsSpace, s)
	}

	// Assertion 1: every shard's history is a single-owner history, and its journal replays to
	// the fills it holds.
	for s, st := range shards {
		for i := 1; i < len(st.history); i++ {
			if st.history[i].Term < st.history[i-1].Term {
				t.Errorf("shard %d: commit %d has term %d after term %d", s, st.history[i].Seq,
					st.history[i].Term, st.history[i-1].Term)
			}
		}
		if err := newEngine(books[0], s, 0).replay(st.lines); err != nil {
			t.Errorf("shard %d: the journal does not replay: %v", s, err)
		}
		if len(st.lines) != len(st.history) {
			t.Errorf("shard %d: %d journal entries and %d commits", s, len(st.lines), len(st.history))
		}
	}

	// Assertion 2: the fence rejected commits of the cut node. Each rejected attempt carries a
	// term below one that some node raised, failed with ErrStale, and is not in the history: the
	// history holds exactly the attempts that succeeded.
	stale := 0
	committed := make(map[engineKey]int) // successful attempts per shard and term
	for _, a := range attempts {
		switch {
		case a.err == nil:
			committed[engineKey{a.shard, a.term}]++
		case errors.Is(a.err, fencedb.ErrStale):
			stale++
			if !slices.ContainsFunc(raises, func(r raiseEvent) bool { return r.shard == a.shard && r.term > a.term }) {
				t.Errorf("order %s on shard %d at term %d was rejected, but no one raised the fence above it",
					a.id, a.shard, a.term)
			}
		default:
			t.Errorf("order %s on shard %d at term %d failed with %v, not ErrStale", a.id, a.shard, a.term, a.err)
		}
	}
	if len(ob.staleFrom(cut)) == 0 {
		t.Errorf("no commit of %s was rejected by the fence (%d rejected in all)", memberID(cut), stale)
	}
	inHistory := make(map[engineKey]int)
	for s, st := range shards {
		for _, cm := range st.history {
			inHistory[engineKey{s, cm.Term}]++
		}
	}
	if !mapsEqual(committed, inHistory) {
		t.Errorf("successful commit attempts per shard and term %v differ from the history %v", committed, inHistory)
	}

	// Assertion 3: every order that got a 200 is journaled exactly once, and nothing else is.
	count := make(map[string]int)
	for s, st := range shards {
		for _, en := range journalOf(t, st.lines) {
			count[en.Order.ID]++
			if got := core.KeyShard(en.Order.Market, marketShards); got != s {
				t.Errorf("order %s of %s is journaled in shard %d, not %d", en.Order.ID, en.Order.Market, s, got)
			}
		}
	}
	for id := range accepted {
		if count[id] != 1 {
			t.Errorf("order %s got a 200 and is journaled %d times", id, count[id])
		}
	}
	for id, n := range count {
		if !accepted[id] {
			t.Errorf("order %s is journaled %d times but never got a 200", id, n)
		}
	}
	for _, id := range rejected {
		if count[id] != 0 {
			t.Errorf("order %s got a 503 but is journaled", id)
		}
	}

	// Assertion 4 (the table and Local of the drained node are checked above): the shards it
	// held were raised again, by a node that was not it, with a higher term.
	for shard, term := range held {
		ok := slices.ContainsFunc(raises, func(r raiseEvent) bool {
			return r.shard == shard && r.term > term && r.node != drainedNode
		})
		if !ok {
			t.Errorf("shard %d of the drained node (term %d) was not raised again with a higher term", shard, term)
		}
	}
	t.Logf("%d orders accepted, %d rejected by a 503, %d commits rejected by the fence (%d from %s)",
		len(accepted), len(rejected), stale, len(ob.staleFrom(cut)), memberID(cut))
}

// TestCommand runs the program in process: two tables and the summary, every order accepted.
// It matches the output by substring, as the program may be built with coverage.
func TestCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := execute(ctx, []string{"-orders", "60", "-clients", "3", "-seed", "7"}, &out); err != nil {
		t.Fatalf("execute: %v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{"before the drain of n3:", "after the drain of n3:", "shard  owner  term", "orders accepted: 60\n", "fills: ", "shards moved: "} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
	if n := strings.Count(s, "shard  owner  term"); n != 2 {
		t.Errorf("output has %d tables, want 2", n)
	}

	if err := execute(ctx, []string{"-orders", "0"}, io.Discard); err == nil {
		t.Error("execute with -orders 0 succeeded")
	}
}

// TestCommandShort runs the program with so few orders that the clients may end before the
// program has seen half of them accepted: it must still drain, print both tables and the summary.
func TestCommandShort(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	for i := range 10 {
		var out bytes.Buffer
		if err := execute(ctx, []string{"-orders", "1", "-clients", "1"}, &out); err != nil {
			t.Fatalf("run %d: execute: %v\n%s", i, err, out.String())
		}
		if s := out.String(); strings.Count(s, "shard  owner  term") != 2 || !strings.Contains(s, "orders accepted: 1\n") {
			t.Fatalf("run %d: output lacks two tables or the summary:\n%s", i, s)
		}
	}
}

// TestOrderEntry checks the answers of POST /orders: 200 with fills, 409 for a repeated ID,
// 400 for a body that is not a valid order, 421 with the owner's address from a member that does
// not own the shard, 503 with Retry-After from an engine that refuses orders, and 405 for other
// methods.
func TestOrderEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db := fencedb.New()
	c, books := obCluster(t, ctx, db, nil)
	hc := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(hc.CloseIdleConnections)
	if _, err := waitOwners(ctx, c, nil); err != nil {
		t.Fatal(err)
	}

	const market = "mkt-07"
	shard := core.KeyShard(market, marketShards)
	ownerIdx := -1
	var sh *tenure.Shard
	for i, n := range c.Nodes {
		if s, err := n.Local(marketsSpace, market); err == nil {
			ownerIdx, sh = i, s
		}
	}
	if ownerIdx < 0 {
		t.Fatalf("no node owns shard %d", shard)
	}
	owner := c.URL(ownerIdx)
	other := c.URL((ownerIdx + 1) % nodeCount)

	post := func(url string, body []byte) answer {
		t.Helper()
		a, err := postOrder(ctx, hc, url, body)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	sell := order{ID: "s1", Market: market, Side: sideSell, Price: 100, Qty: 5}
	buy := order{ID: "b1", Market: market, Side: sideBuy, Price: 101, Qty: 3}

	if a := post(owner, orderJSON(t, sell)); a.status != http.StatusOK || strings.Contains(string(a.body), `"maker"`) {
		t.Fatalf("resting order: status %d, body %s; want 200 without fills", a.status, a.body)
	}
	a := post(owner, orderJSON(t, buy))
	var res result
	if err := json.Unmarshal(a.body, &res); err != nil || a.status != http.StatusOK {
		t.Fatalf("crossing order: status %d, body %s, err %v", a.status, a.body, err)
	}
	if want := []fill{{Maker: "s1", Taker: "b1", Price: 100, Qty: 3}}; res.ID != "b1" || !slices.Equal(res.Fills, want) {
		t.Errorf("crossing order: answer %+v, want id b1 and fills %v", res, want)
	}

	if a := post(owner, orderJSON(t, buy)); a.status != http.StatusConflict {
		t.Errorf("repeated order ID: status %d, want 409", a.status)
	}

	for name, body := range map[string]string{
		"not JSON":      `{`,
		"unknown field": `{"id":"e1","market":"mkt-07","side":"buy","price":1,"qty":1,"extra":1}`,
		"two orders":    `{"id":"e2","market":"mkt-07","side":"buy","price":1,"qty":1}{}`,
		"bad side":      `{"id":"e3","market":"mkt-07","side":"hold","price":1,"qty":1}`,
		"zero price":    `{"id":"e4","market":"mkt-07","side":"buy","price":0,"qty":1}`,
		"no market":     `{"id":"e5","market":"","side":"buy","price":1,"qty":1}`,
		"no ID":         `{"market":"mkt-07","side":"buy","price":1,"qty":1}`,
	} {
		if a := post(owner, []byte(body)); a.status != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, a.status)
		}
	}
	if a := post(other, orderJSON(t, order{ID: "o1", Market: market, Side: sideBuy, Price: 1, Qty: 1})); a.status != http.StatusMisdirectedRequest ||
		a.header.Get(routeHeader) != c.Addrs[ownerIdx] {
		t.Errorf("order sent to a member that does not own the shard: status %d, %s %q; want 421 and %q",
			a.status, routeHeader, a.header.Get(routeHeader), c.Addrs[ownerIdx])
	}

	// The same order through demo.Post lands on the owner.
	if res, err := demo.Post(ctx, hc, other+"/orders", orderJSON(t, order{ID: "o2", Market: market, Side: sideBuy, Price: 1, Qty: 1})); err != nil ||
		res.Status != http.StatusOK || res.Sends != 2 {
		t.Errorf("Post through a member that does not own the shard: %+v, %v; want 200 in 2 sends", res, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, owner+"/orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /orders: status %d, want 405", resp.StatusCode)
	}

	// An engine that refuses orders answers 503 with Retry-After, and journals nothing more.
	e := books[ownerIdx].engine(sh.ID, sh.Term)
	if e == nil {
		t.Fatal("the owner has no engine")
	}
	e.close()
	before := len(journal(t, db, sh.ID))
	a = post(owner, orderJSON(t, order{ID: "r1", Market: market, Side: sideBuy, Price: 1, Qty: 1}))
	if a.status != http.StatusServiceUnavailable || a.header.Get("Retry-After") != "1" {
		t.Errorf("order to a refusing engine: status %d, Retry-After %q; want 503 and 1", a.status, a.header.Get("Retry-After"))
	}
	if after := len(journal(t, db, sh.ID)); after != before {
		t.Errorf("the journal grew from %d to %d entries on a refused order", before, after)
	}
}

// mapsEqual reports whether two maps hold the same keys with the same counts.
func mapsEqual(a, b map[engineKey]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// journalOf decodes journal lines.
func journalOf(t *testing.T, lines [][]byte) []entry {
	t.Helper()
	out := make([]entry, 0, len(lines))
	for _, l := range lines {
		var en entry
		if err := json.Unmarshal(l, &en); err != nil {
			t.Fatalf("journal line %q: %v", l, err)
		}
		out = append(out, en)
	}
	return out
}
