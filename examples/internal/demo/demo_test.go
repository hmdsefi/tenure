// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package demo_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/core"
	"github.com/hmdsefi/tenure/examples/internal/demo"
	"github.com/hmdsefi/tenure/memstore"
)

const (
	testSpace  = "orders"
	testShards = 8
)

// testCtx returns a context that ends when the test is over or after d, whichever is first.
func testCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

// noLeaks fails the test when a goroutine started during it outlives it, once the cleanups ran.
func noLeaks(t *testing.T) {
	t.Helper()
	before := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, before) })
}

// waitFor polls cond every 10 ms until it holds or ctx ends.
func waitFor(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// owned returns the shards of testSpace that node n runs, from its table.
func owned(n *tenure.Node) []int {
	var out []int
	for _, sp := range n.Table().Spaces {
		if sp.Name != testSpace {
			continue
		}
		for _, row := range sp.Rows {
			if row.State == "owned" {
				out = append(out, row.Shard)
			}
		}
	}
	return out
}

// testSpaces is the one space of the cluster tests, whose Run blocks until the shard moves.
func testSpaces(int) []tenure.Space {
	return []tenure.Space{{
		Name:   testSpace,
		Shards: testShards,
		Fenced: true,
		Run: func(ctx context.Context, _ *tenure.Shard) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}}
}

// startCluster starts a cluster for the test and closes it when the test is over.
func startCluster(t *testing.T, opts demo.Options) *demo.Cluster {
	t.Helper()
	c, err := demo.Start(testCtx(t, 20*time.Second), opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		for _, cut := range c.Cuts {
			cut.Heal()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// get fetches url and returns the body.
func get(ctx context.Context, hc *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// TestATEXM05DemoCluster checks the demo cluster: listeners and member IDs, routes that name
// listener addresses, the drain of one node, and a store cut that returns ErrCut when the call's
// context ends and heals. The steps were written from the issue's text.
func TestATEXM05DemoCluster(t *testing.T) {
	noLeaks(t)
	ctx := testCtx(t, 60*time.Second)
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	c := startCluster(t, demo.Options{
		Spaces: testSpaces,
		Serve: func(i int, _ *tenure.Node) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, "node %d", i)
			})
		},
	})

	// Three nodes, one listener each, IDs n1…n3, member address = listener address.
	if len(c.Nodes) != 3 || len(c.Cuts) != 3 || len(c.Addrs) != 3 {
		t.Fatalf("%d nodes, %d cuts, %d addresses; want 3 of each", len(c.Nodes), len(c.Cuts), len(c.Addrs))
	}
	if c.Store == nil {
		t.Fatal("Store is nil")
	}
	for i, n := range c.Nodes {
		m := n.Table().Member
		if want := fmt.Sprintf("n%d", i+1); m.ID != want {
			t.Errorf("node %d has ID %q, want %q", i, m.ID, want)
		}
		if m.Addr != c.Addrs[i] {
			t.Errorf("node %d has address %q, want its listener %q", i, m.Addr, c.Addrs[i])
		}
		if want := "http://" + c.Addrs[i]; c.URL(i) != want {
			t.Errorf("URL(%d) = %q, want %q", i, c.URL(i), want)
		}
		if got, err := get(ctx, hc, c.URL(i)); err != nil || got != fmt.Sprintf("node %d", i) {
			t.Errorf("GET %s = %q, %v; want the handler of node %d", c.URL(i), got, err, i)
		}
	}
	if len(slices.Compact(slices.Sorted(slices.Values(c.Addrs)))) != 3 {
		t.Errorf("addresses %v are not three distinct ones", c.Addrs)
	}

	// Every shard is owned by exactly one node, and routes name listener addresses.
	totalOwned := func() int {
		total := 0
		for _, n := range c.Nodes {
			total += len(owned(n))
		}
		return total
	}
	waitFor(t, ctx, "every shard to be owned", func() bool { return totalOwned() == testShards })
	for i, n := range c.Nodes {
		for k := range 40 {
			m, err := n.Route(testSpace, fmt.Sprintf("key-%d", k))
			if err != nil {
				t.Fatalf("n%d Route: %v", i+1, err)
			}
			if !slices.Contains(c.Addrs, m.Addr) {
				t.Errorf("n%d routes key-%d to %s at %q, not a listener of the cluster %v", i+1, k, m.ID, m.Addr, c.Addrs)
			}
		}
	}

	// After n3 drains, only n1 and n2 own shards and route.
	if err := c.Nodes[2].Drain(ctx); err != nil {
		t.Fatalf("Drain n3: %v", err)
	}
	waitFor(t, ctx, "n1 and n2 to own every shard", func() bool {
		return len(owned(c.Nodes[0]))+len(owned(c.Nodes[1])) == testShards
	})
	if got := owned(c.Nodes[2]); len(got) != 0 {
		t.Errorf("n3 owns %v after its drain", got)
	}
	for i, n := range c.Nodes[:2] {
		waitFor(t, ctx, "the view of n"+fmt.Sprint(i+1)+" to leave out n3", func() bool {
			for k := range 40 {
				m, err := n.Route(testSpace, fmt.Sprintf("key-%d", k))
				if err != nil || (m.ID != "n1" && m.ID != "n2") {
					return false
				}
			}
			return true
		})
	}

	// A cut store: the call blocks until its context ends, then matches ErrCut and the context's
	// error; after Heal calls succeed.
	cut := c.Cuts[0]
	cut.Cut()
	callCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	start := time.Now()
	_, err := cut.Holdings(callCtx, testSpace)
	cancel()
	if !errors.Is(err, demo.ErrCut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Holdings on a cut store = %v, want ErrCut and DeadlineExceeded", err)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("a cut call returned after %v, want it to wait for its context", d)
	}
	// Another node's store is not cut.
	if _, err := c.Cuts[1].Holdings(ctx, testSpace); err != nil {
		t.Errorf("Holdings on the store of n2 = %v, want nil", err)
	}
	cut.Heal()
	hs, err := cut.Holdings(ctx, testSpace)
	if err != nil {
		t.Fatalf("Holdings after Heal: %v", err)
	}
	if len(hs) != testShards {
		t.Errorf("Holdings lists %d records, want %d", len(hs), testShards)
	}
}

// TestCutStore covers the CutStore on its own: each call is held while cut, a held call goes on
// after Heal, a done context never blocks, and the watch keeps its channel open and delivers in
// order after Heal.
func TestCutStore(t *testing.T) {
	noLeaks(t)
	ctx := testCtx(t, 30*time.Second)
	mem := memstore.New(nil)
	cut := demo.NewCutStore(mem)

	timing := demo.TestTiming()
	if err := cut.KeepAlive(ctx, "h/1", timing); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	cut.Cut()
	cut.Cut() // cutting twice is one cut

	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, 50*time.Millisecond)
	}
	calls := map[string]func(ctx context.Context) error{
		"KeepAlive": func(ctx context.Context) error { return cut.KeepAlive(ctx, "h/1", timing) },
		"Acquire": func(ctx context.Context) error {
			_, err := cut.Acquire(ctx, "h/1", testSpace, 0)
			return err
		},
		"Release": func(ctx context.Context) error { return cut.Release(ctx, "h/1", testSpace, 0, 1) },
		"Confirm": func(ctx context.Context) error {
			_, err := cut.Confirm(ctx, "h/1", testSpace, 0, 1)
			return err
		},
		"CloseSession": func(ctx context.Context) error { return cut.CloseSession(ctx, "h/2") },
		"Holdings": func(ctx context.Context) error {
			_, err := cut.Holdings(ctx, testSpace)
			return err
		},
		"WatchHolders": func(ctx context.Context) error {
			_, err := cut.WatchHolders(ctx)
			return err
		},
	}
	for name, call := range calls {
		c, cancel := short()
		err := call(c)
		cancel()
		if !errors.Is(err, demo.ErrCut) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s on a cut store = %v, want ErrCut and DeadlineExceeded", name, err)
		}
	}
	// Nothing reached the store: the shard has no record.
	if hs, _ := mem.Holdings(ctx, testSpace); len(hs) != 0 {
		t.Errorf("the store holds %v, want nothing written while cut", hs)
	}

	// A context that is canceled already also returns at once.
	done, cancel := context.WithCancel(ctx)
	cancel()
	if err := cut.KeepAlive(done, "h/1", timing); !errors.Is(err, demo.ErrCut) || !errors.Is(err, context.Canceled) {
		t.Errorf("KeepAlive with a canceled context = %v, want ErrCut and Canceled", err)
	}

	// A call held by the cut goes on after Heal and returns the store's answer.
	var term core.Term
	acquired := make(chan error, 1)
	go func() {
		var err error
		term, err = cut.Acquire(ctx, "h/1", testSpace, 0)
		acquired <- err
	}()
	select {
	case err := <-acquired:
		t.Fatalf("Acquire returned %v while the store was cut", err)
	case <-time.After(100 * time.Millisecond):
	}
	cut.Heal()
	cut.Heal() // healing twice is fine
	if err := <-acquired; err != nil {
		t.Fatalf("Acquire after Heal: %v", err)
	}
	if term == 0 {
		t.Error("Acquire after Heal returned term 0")
	}

	// The watch: open while healed, quiet while cut, in order after Heal.
	wctx, wcancel := context.WithCancel(ctx)
	ch, err := cut.WatchHolders(wctx)
	if err != nil {
		t.Fatalf("WatchHolders: %v", err)
	}
	recv := func() (tenure.Holding, bool) {
		select {
		case h, ok := <-ch:
			return h, ok
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the watch")
			return tenure.Holding{}, false
		}
	}
	if err := cut.Release(ctx, "h/1", testSpace, 0, term); err != nil {
		t.Fatal(err)
	}
	if h, ok := recv(); !ok || h.Holder != "" || h.Term != term {
		t.Fatalf("watch delivered %+v, %v while healed; want the release of term %d", h, ok, term)
	}
	cut.Cut()
	var terms []core.Term
	for range 3 {
		term, err = mem.Acquire(ctx, "h/1", testSpace, 0)
		if err != nil {
			t.Fatal(err)
		}
		terms = append(terms, term)
		if err := mem.Release(ctx, "h/1", testSpace, 0, term); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case h, ok := <-ch:
		t.Fatalf("watch delivered %+v, %v while cut", h, ok)
	case <-time.After(100 * time.Millisecond):
	}
	cut.Heal()
	for i, want := range terms {
		if h, ok := recv(); !ok || h.Holder != "h/1" || h.Term != want {
			t.Fatalf("watch value %d after Heal = %+v, %v; want the grant of term %d", i, h, ok, want)
		}
		if h, ok := recv(); !ok || h.Holder != "" || h.Term != want {
			t.Fatalf("watch value %d after Heal = %+v, %v; want the release of term %d", i, h, ok, want)
		}
	}
	// The channel closes when its context ends.
	wcancel()
	waitFor(t, ctx, "the watch channel to close", func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	})

	// A store that is not a Watcher cannot be watched through the cut.
	if _, err := demo.NewCutStore(noWatch{mem}).WatchHolders(ctx); err == nil {
		t.Error("WatchHolders over a store without a watch succeeded")
	}
}

// noWatch hides the Watcher of a memstore.
type noWatch struct{ tenure.Store }

// TestStartOptions covers the defaults and the failures of Start.
func TestStartOptions(t *testing.T) {
	noLeaks(t)
	ctx := testCtx(t, 30*time.Second)

	t.Run("two nodes and no spaces", func(t *testing.T) {
		c := startCluster(t, demo.Options{Nodes: 2})
		if len(c.Nodes) != 2 {
			t.Fatalf("%d nodes, want 2", len(c.Nodes))
		}
		// With no Serve every listener answers 404.
		hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
		body, err := get(ctx, hc, c.URL(1)+"/x")
		if err != nil || body != "404 page not found\n" {
			t.Errorf("GET without a handler = %q, %v; want the 404 page", body, err)
		}
	})

	t.Run("negative node count", func(t *testing.T) {
		if _, err := demo.Start(ctx, demo.Options{Nodes: -1}); err == nil {
			t.Error("Start with -1 nodes succeeded")
		}
	})

	t.Run("failed start tears down", func(t *testing.T) {
		// The third node's space is invalid, so Start fails after two nodes started and three
		// listeners opened; the leak check of the test covers the rest.
		_, err := demo.Start(ctx, demo.Options{
			Spaces: func(i int) []tenure.Space {
				if i == 2 {
					return []tenure.Space{{Name: "", Shards: 1}}
				}
				return testSpaces(i)
			},
		})
		if err == nil {
			t.Fatal("Start with an invalid space succeeded")
		}
	})

	t.Run("failed start frees the listeners", func(t *testing.T) {
		// Start with a context that is done: the first step fails and nothing stays open.
		done, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := demo.Start(done, demo.Options{}); err == nil {
			t.Fatal("Start with a canceled context succeeded")
		}
	})

	t.Run("serve gets the node", func(t *testing.T) {
		seen := make(chan string, 3)
		startCluster(t, demo.Options{
			Nodes: 3,
			Serve: func(i int, n *tenure.Node) http.Handler {
				seen <- fmt.Sprintf("%d:%s", i, n.Table().Member.ID)
				return nil
			},
		})
		got := []string{<-seen, <-seen, <-seen}
		slices.Sort(got)
		if want := []string{"0:n1", "1:n2", "2:n3"}; !slices.Equal(got, want) {
			t.Errorf("Serve was called with %v, want %v", got, want)
		}
	})

	t.Run("close is repeatable", func(t *testing.T) {
		c, err := demo.Start(ctx, demo.Options{Nodes: 1})
		if err != nil {
			t.Fatal(err)
		}
		addr := c.Addrs[0]
		if err := c.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := c.Close(ctx); err != nil {
			t.Errorf("second Close: %v", err)
		}
		// The listener is closed: the port refuses connections.
		var d net.Dialer
		if conn, err := d.DialContext(ctx, "tcp", addr); err == nil {
			_ = conn.Close()
			t.Errorf("%s still accepts connections after Close", addr)
		}
	})
}

// n2Draining reports whether node n's latest view lists member n2 as draining.
func n2Draining(n *tenure.Node) bool {
	for _, m := range n.Table().Members {
		if m.ID == "n2" {
			return m.Draining
		}
	}
	return false
}

// TestCutMembership checks that a cut node does not see the group change while the other nodes
// do, and sees it after Heal.
func TestCutMembership(t *testing.T) {
	noLeaks(t)
	ctx := testCtx(t, 30*time.Second)
	c := startCluster(t, demo.Options{Nodes: 3, Spaces: testSpaces})

	c.Cuts[1].Cut()
	if err := c.Groups[1].SetDraining(ctx, true); err != nil {
		t.Fatalf("SetDraining: %v", err)
	}
	waitFor(t, ctx, "n1 to see n2 draining", func() bool { return n2Draining(c.Nodes[0]) })
	waitFor(t, ctx, "n3 to see n2 draining", func() bool { return n2Draining(c.Nodes[2]) })
	time.Sleep(200 * time.Millisecond)
	if n2Draining(c.Nodes[1]) {
		t.Error("the cut node n2 sees the group change")
	}

	c.Cuts[1].Heal()
	waitFor(t, ctx, "n2 to see itself draining after Heal", func() bool { return n2Draining(c.Nodes[1]) })
}
