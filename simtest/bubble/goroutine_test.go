// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package bubble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/debughttp"
)

// The tests of the real runtime in faultline's goroutine mode (SIM-720): the harness's group as
// processes m1 to m3, with every store and fence call gated through Process.Do, crashed and cut
// off at random. A goroutine-mode seed replays on a best-effort basis (SIM-710), so the tests
// check invariants only and skip under the determinism check (DET-510, AT-DET-18).

// skipDeterminism is DET-510's message: why the goroutine-mode tests skip under
// FAULTLINE_CHECK_DETERMINISM=1.
const skipDeterminism = "simtest: goroutine mode replays on a best-effort basis; skipped under FAULTLINE_CHECK_DETERMINISM=1"

// skipUnderDeterminismCheck skips t when FAULTLINE_CHECK_DETERMINISM=1, which would run every
// passing seed twice and compare trace hashes that goroutine mode does not keep stable.
func skipUnderDeterminismCheck(t *testing.T) {
	t.Helper()
	if os.Getenv("FAULTLINE_CHECK_DETERMINISM") == "1" {
		t.Skip(skipDeterminism)
	}
}

// The length of a goroutine-mode run and of its recovery window at the end, in which no fault
// starts: the faults stop at runFor-quiet, which leaves more than settle for the group to
// converge.
const (
	runFor = 40 * time.Second
	quiet  = 15 * time.Second
)

// goroutineSeeds is the number of seeds of each goroutine-mode test (AT-SIM-40); -short runs
// faultline.ShortSeeds of them.
const goroutineSeeds = 50

// goroutineOptions are the options of the random goroutine-mode tests. Drain is shorter than
// faultline's default: at the end every process closes its node at once.
func goroutineOptions() faultline.Options {
	return faultline.Options{
		Mode:     faultline.ModeGoroutine,
		Seeds:    goroutineSeeds,
		Duration: runFor,
		Drain:    10 * time.Second,
	}
}

// TestGoroutineCrash crashes one member at a time at random for up to 2·LeaseDuration, and
// restarts it with a new incarnation.
func TestGoroutineCrash(t *testing.T) {
	skipUnderDeterminismCheck(t)
	faultline.Run(t, goroutineOptions(), func(w *faultline.World) {
		pw := newProcWorld(w, options{})
		w.Plan(&fault.Random{MaxDown: 1, Quiet: quiet, Rules: []fault.Rule{
			{Kind: fault.KindCrash, Target: roleMembers, Every: 5 * time.Second, MinFor: time.Second, MaxFor: 2 * leaseDuration},
		}})
		pw.check(runFor - quiet)
	})
}

// TestGoroutineStoreCuts isolates a member (from the store and the fence) or the store (from
// every member) at random, for up to 2·LeaseDuration and LeaseDuration.
func TestGoroutineStoreCuts(t *testing.T) {
	skipUnderDeterminismCheck(t)
	faultline.Run(t, goroutineOptions(), func(w *faultline.World) {
		pw := newProcWorld(w, options{})
		w.Plan(&fault.Random{Quiet: quiet, Rules: []fault.Rule{
			{Kind: fault.KindIsolate, Target: roleMembers, Every: 5 * time.Second, MinFor: time.Second, MaxFor: 2 * leaseDuration},
			{Kind: fault.KindIsolate, Target: roleStore, Every: 10 * time.Second, MinFor: time.Second, MaxFor: leaseDuration},
		}})
		pw.check(runFor - quiet)
	})
}

// gateProbe is one probe of TestATSIM45Gates: when it ran, which incarnation's gate it used, the
// op, and what came of it.
type gateProbe struct {
	at   time.Duration
	inc  int
	op   string
	err  error
	ran  bool
	took time.Duration
}

func (p gateProbe) String() string {
	out := "ran"
	if !p.ran {
		out = "not run"
	}
	return fmt.Sprintf("%v inc %d %s: %v, %s, took %v", p.at, p.inc, p.op, p.err, out, p.took)
}

// TestATSIM45Gates checks the gate of a process (AT-SIM-45): a call runs when the member and the
// callee reach each other both ways; a cut of either direction, or an isolation, refuses it
// with the callee's unreachable error; and once the member's node crashed, its incarnation's gate
// refuses every call at once with faultline.ErrCrashed, also after the node restarted, while the
// new incarnation's gate lets calls through. A refused call does not run. The steps were written
// for this test from the issue's text, the original acceptance test being unavailable.
func TestATSIM45Gates(t *testing.T) {
	skipUnderDeterminismCheck(t)
	sec := func(s float64) kernel.Time { return kernel.Time(s * float64(time.Second)) }
	opts := faultline.Options{Mode: faultline.ModeGoroutine, Seeds: 1, Duration: 15 * time.Second}
	faultline.Run(t, opts, func(w *faultline.World) {
		pn := newProcNet(w)
		start := time.Now()
		var (
			mu      sync.Mutex
			probes  []gateProbe
			boots   int
			oldGate gateFunc
		)
		probe := func(inc int, gate gateFunc, op string) {
			ran := false
			before := time.Now()
			err := gate(op, func() { ran = true })
			mu.Lock()
			defer mu.Unlock()
			probes = append(probes, gateProbe{at: before.Sub(start), inc: inc, op: op, err: err, ran: ran, took: time.Since(before)})
		}
		probeAt := func(at kernel.Time, inc int, gate gateFunc) {
			time.Sleep(time.Duration(at) - time.Since(start))
			probe(inc, gate, "store.KeepAlive")
			probe(inc, gate, "fence.write")
		}
		w.AddProcess("m1", func(ctx context.Context, p *faultline.Process) error {
			gate := pn.gate(p)
			mu.Lock()
			boots++
			inc := boots
			mu.Unlock()
			if inc == 1 {
				for _, at := range []float64{0.5, 1.5, 3.5, 5.5, 7.5, 9.5, 10.5} {
					probeAt(sec(at), inc, gate)
				}
				<-ctx.Done()
				probe(inc, gate, "store.KeepAlive")
				probe(inc, gate, "fence.write")
				mu.Lock()
				oldGate = gate
				mu.Unlock()
				return nil
			}
			probeAt(sec(12.5), inc, gate)
			mu.Lock()
			old := oldGate
			mu.Unlock()
			probe(1, old, "store.KeepAlive")
			probe(1, old, "fence.write")
			<-ctx.Done()
			return nil
		})
		w.Plan(fault.Script(
			fault.Event{At: sec(1), Kind: fault.KindCut, Node: "m1", Peer: storeNode},
			fault.Event{At: sec(2), Kind: fault.KindHealLink, Node: "m1", Peer: storeNode},
			fault.Event{At: sec(3), Kind: fault.KindCut, Node: storeNode, Peer: "m1"},
			fault.Event{At: sec(4), Kind: fault.KindHealLink, Node: storeNode, Peer: "m1"},
			fault.Event{At: sec(5), Kind: fault.KindCut, Node: "m1", Peer: fenceNode},
			fault.Event{At: sec(6), Kind: fault.KindHealLink, Node: "m1", Peer: fenceNode},
			fault.Event{At: sec(7), Kind: fault.KindCut, Node: fenceNode, Peer: "m1"},
			fault.Event{At: sec(8), Kind: fault.KindHealLink, Node: fenceNode, Peer: "m1"},
			fault.Event{At: sec(9), Kind: fault.KindIsolate, Node: "m1"},
			fault.Event{At: sec(10), Kind: fault.KindHeal},
			fault.Event{At: sec(11), Kind: fault.KindCrash, Node: "m1"},
			fault.Event{At: sec(12), Kind: fault.KindRestart, Node: "m1"},
		))

		type want struct {
			at  float64
			inc int
			op  string
			err error // nil: the call runs
		}
		const keepAlive, write = "store.KeepAlive", "fence.write"
		wants := []want{
			{0.5, 1, keepAlive, nil}, {0.5, 1, write, nil}, // connected
			{1.5, 1, keepAlive, errStoreUnreachable}, {1.5, 1, write, nil}, // m1 -> store cut
			{3.5, 1, keepAlive, errStoreUnreachable}, {3.5, 1, write, nil}, // store -> m1 cut
			{5.5, 1, keepAlive, nil}, {5.5, 1, write, errFenceUnreachable}, // m1 -> fence cut
			{7.5, 1, keepAlive, nil}, {7.5, 1, write, errFenceUnreachable}, // fence -> m1 cut
			{9.5, 1, keepAlive, errStoreUnreachable}, {9.5, 1, write, errFenceUnreachable}, // m1 isolated
			{10.5, 1, keepAlive, nil}, {10.5, 1, write, nil}, // healed
			{11, 1, keepAlive, faultline.ErrCrashed}, {11, 1, write, faultline.ErrCrashed}, // crashed
			{12.5, 2, keepAlive, nil}, {12.5, 2, write, nil}, // restarted
			{12.5, 1, keepAlive, faultline.ErrCrashed}, {12.5, 1, write, faultline.ErrCrashed}, // the old gate
		}
		w.Final("AT-SIM-45 gates", func() error {
			mu.Lock()
			defer mu.Unlock()
			var errs []error
			if len(probes) != len(wants) {
				errs = append(errs, fmt.Errorf("%d probes, want %d", len(probes), len(wants)))
			}
			for i, p := range probes {
				if i >= len(wants) {
					break
				}
				wt := wants[i]
				ok := p.at == time.Duration(sec(wt.at)) && p.inc == wt.inc && p.op == wt.op &&
					errors.Is(p.err, wt.err) && (p.err == nil) == p.ran && p.took == 0
				if !ok {
					errs = append(errs, fmt.Errorf("probe %d: %v; want at %vs inc %d %s: %v, run %t, at once",
						i, p, wt.at, wt.inc, wt.op, wt.err, wt.err == nil))
				}
			}
			return errors.Join(errs...)
		})
	})
}

// TestEventRecordAttrs checks eventAttrs against package debughttp: for every event a node
// emits, in a group that converges and then loses a member, the record's attributes after seq
// are the keys and values of the event's object in debughttp's events body after seq, time and
// kind, in the same order.
func TestEventRecordAttrs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		var (
			mu    sync.Mutex
			attrs = map[string]map[uint64][]kernel.Attr{} // member -> seq -> attributes after seq
		)
		for _, m := range h.members {
			attrs[m.id] = map[uint64][]kernel.Attr{}
		}
		for _, m := range h.members {
			id := m.id
			if _, err := m.startWith(context.Background(), hooks{onEvent: func(e tenure.Event) {
				a := eventAttrs(e)
				if a[0] != recAttr("seq", fmt.Sprint(e.Seq)) {
					t.Errorf("%s: event %d: first attribute %v, want seq", id, e.Seq, a[0])
				}
				mu.Lock()
				defer mu.Unlock()
				attrs[id][e.Seq] = a[1:]
			}}); err != nil {
				t.Fatalf("start %s: %v", id, err)
			}
		}
		h.sleepUntil(disturbAt)
		h.member("m2").crash()
		h.sleepUntil(2 * disturbAt)

		compared := 0
		for _, id := range []string{"m1", "m3"} {
			objs := debugEvents(t, h.member(id).inc.node)
			mu.Lock()
			for seq, want := range objs {
				got, ok := attrs[id][seq]
				if !ok {
					continue // not delivered to OnEvent yet
				}
				compared++
				if !slices.Equal(got, want) {
					t.Errorf("%s: event %d: record attributes %v, debughttp has %v", id, seq, got, want)
				}
			}
			mu.Unlock()
		}
		if compared == 0 {
			t.Error("no event compared")
		}
		h.finish(disturbAt)
	})
}

// debugEvents returns the events of n in debughttp's events body: for each seq, the keys and
// values after seq, time and kind, in order, numbers in decimal.
func debugEvents(t *testing.T, n *tenure.Node) map[uint64][]kernel.Attr {
	t.Helper()
	rec := httptest.NewRecorder()
	debughttp.Handler(n).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /events: %d %s", rec.Code, rec.Body)
	}
	var body struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	out := map[uint64][]kernel.Attr{}
	for _, raw := range body.Events {
		kv, err := orderedObject(raw)
		if err != nil {
			t.Fatalf("event %s: %v", raw, err)
		}
		if len(kv) < 3 || kv[0].Key != "seq" || kv[1].Key != "time" || kv[2].Key != "kind" {
			t.Fatalf("event %s: does not start with seq, time and kind", raw)
		}
		var seq uint64
		if _, err := fmt.Sscan(kv[0].Value, &seq); err != nil {
			t.Fatalf("event %s: seq: %v", raw, err)
		}
		out[seq] = kv[3:]
	}
	return out
}

// orderedObject returns the members of a flat JSON object in order, each value as a string:
// strings as they are, numbers as written.
func orderedObject(raw []byte) ([]kernel.Attr, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("not an object: %v %v", tok, err)
	}
	var out []kernel.Attr
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, err
		}
		val, err := dec.Token()
		if err != nil {
			return nil, err
		}
		var s string
		switch v := val.(type) {
		case string:
			s = v
		case json.Number:
			s = v.String()
		case nil:
			s = "null"
		default:
			return nil, fmt.Errorf("key %v: value %v is not a string or a number", key, val)
		}
		out = append(out, kernel.Attr{Key: fmt.Sprint(key), Value: s})
	}
	if _, err := dec.Token(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return out, nil
}
