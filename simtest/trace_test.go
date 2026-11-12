// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package simtest

import (
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/tenure/simtest/internal/world"
)

// replaySeed is the seed of TestSteady's server row that TestTraceReplay runs twice.
const replaySeed = 0x1

// traceReplaySkip is why TestTraceReplay skips in a run without FAULTLINE_CHECK_DETERMINISM=1.
const traceReplaySkip = "simtest: TestTraceReplay runs a seed twice and so runs only with FAULTLINE_CHECK_DETERMINISM=1, like every same-seed check"

// traceRun is what one run of TestTraceReplay keeps: every record and the trace hash.
type traceRun struct {
	records []kernel.Record
	hash    uint64
}

// TestTraceReplay checks SIM-610 (AT-SIM-32): one seed of TestSteady's server row, run twice
// with every record kept, gives the same records, field by field (Seq, At, Node, Inc, Kind,
// Cause, Text and Attrs), and the same trace hash. The first difference is reported. The
// steps were written for this task from the issue and the design note, since the original
// acceptance-test text is lost.
//
// Like every check that runs a seed twice, it runs only with FAULTLINE_CHECK_DETERMINISM=1, as
// CI's determinism step sets it, and skips otherwise (AT-DET-16): state kept outside the World,
// such as a package-level counter, makes two runs of a seed differ, and only that step is to
// fail on it.
func TestTraceReplay(t *testing.T) {
	if os.Getenv("FAULTLINE_CHECK_DETERMINISM") != "1" {
		t.Skip(traceReplaySkip)
	}
	r := rowNamed(t, "TestSteady")
	opts := r.options() // CheckDeterminism off: the test compares the two runs itself
	opts.Trace = kernel.TraceConfig{Level: kernel.TraceFull}
	// Each subtest keeps the trace of its first run only: FAULTLINE_CHECK_DETERMINISM=1, which
	// this test needs, runs the seed a second time with the hash alone.
	var runs [2]*traceRun
	for i := range runs {
		t.Run(fmt.Sprintf("run%d", i+1), func(t *testing.T) {
			faultline.RunSeeds(t, opts, r.body(world.Server, func(fw *faultline.World, _ *world.World) {
				fw.Final("test/keep_trace", func() error {
					if runs[i] == nil {
						runs[i] = &traceRun{records: cloneRecords(fw.Sim.Records()), hash: fw.Sim.TraceHash()}
					}
					return nil
				})
			}), replaySeed)
		})
	}
	if t.Failed() {
		return
	}
	for i, run := range runs {
		if run == nil || len(run.records) == 0 {
			t.Fatalf("run %d kept no records", i+1)
		}
	}
	if err := firstDifference(runs[0].records, runs[1].records); err != nil {
		t.Errorf("seed %#x run twice: %v", uint64(replaySeed), err)
	}
	if runs[0].hash != runs[1].hash {
		t.Errorf("seed %#x run twice: trace hashes %#016x and %#016x", uint64(replaySeed), runs[0].hash, runs[1].hash)
	}
}

// cloneRecords returns a deep copy of recs.
func cloneRecords(recs []kernel.Record) []kernel.Record {
	out := slices.Clone(recs)
	for i := range out {
		out[i].Attrs = slices.Clone(out[i].Attrs)
	}
	return out
}

// firstDifference compares two traces record by record and field by field, and returns an
// error for the first field that differs, or for the first record one trace has and the other
// lacks; nil when they are equal.
func firstDifference(a, b []kernel.Record) error {
	for i := range min(len(a), len(b)) {
		x, y := a[i], b[i]
		var field string
		var got, want any
		switch {
		case x.Seq != y.Seq:
			field, got, want = "Seq", x.Seq, y.Seq
		case x.At != y.At:
			field, got, want = "At", x.At, y.At
		case x.Node != y.Node:
			field, got, want = "Node", x.Node, y.Node
		case x.Inc != y.Inc:
			field, got, want = "Inc", x.Inc, y.Inc
		case x.Kind != y.Kind:
			field, got, want = "Kind", fmt.Sprintf("%q", x.Kind), fmt.Sprintf("%q", y.Kind)
		case x.Cause != y.Cause:
			field, got, want = "Cause", x.Cause, y.Cause
		case x.Text != y.Text:
			field, got, want = "Text", fmt.Sprintf("%q", x.Text), fmt.Sprintf("%q", y.Text)
		case !slices.Equal(x.Attrs, y.Attrs):
			field, got, want = "Attrs", fmt.Sprintf("%v", x.Attrs), fmt.Sprintf("%v", y.Attrs)
		default:
			continue
		}
		return fmt.Errorf("record %d (%s at %v) differs in %s: %v, then %v", i+1, x.Kind, x.At, field, got, want)
	}
	switch {
	case len(a) > len(b):
		return fmt.Errorf("the first trace has %d records, the second %d; the first extra is %s at %v", len(a), len(b), a[len(b)].Kind, a[len(b)].At)
	case len(b) > len(a):
		return fmt.Errorf("the first trace has %d records, the second %d; the first extra is %s at %v", len(a), len(b), b[len(a)].Kind, b[len(a)].At)
	}
	return nil
}

// TestFirstDifference checks that firstDifference finds equal traces equal and reports the
// first differing field, and a missing record, with its position.
func TestFirstDifference(t *testing.T) {
	base := []kernel.Record{
		{Seq: 1, At: 0, Kind: "kernel.start", Text: "start"},
		{Seq: 2, At: 5, Node: 2, Inc: 1, Kind: "tenure.input", Cause: 1, Text: "Tick", Attrs: []kernel.Attr{{Key: "type", Value: "Tick"}}},
		{Seq: 3, At: 7, Node: 3, Inc: 1, Kind: "tenure.action", Cause: 2, Text: "StartRun"},
	}
	if err := firstDifference(base, cloneRecords(base)); err != nil {
		t.Errorf("equal traces: %v", err)
	}
	tests := []struct {
		name   string
		change func(recs []kernel.Record) []kernel.Record
		want   string
	}{
		{"seq", func(r []kernel.Record) []kernel.Record { r[1].Seq = 9; return r },
			"record 2 (tenure.input at 0.000000005s) differs in Seq: 2, then 9"},
		{"at", func(r []kernel.Record) []kernel.Record { r[1].At = 6; return r },
			"record 2 (tenure.input at 0.000000005s) differs in At: 0.000000005s, then 0.000000006s"},
		{"node", func(r []kernel.Record) []kernel.Record { r[2].Node = 4; return r },
			"record 3 (tenure.action at 0.000000007s) differs in Node: 3, then 4"},
		{"inc", func(r []kernel.Record) []kernel.Record { r[2].Inc = 2; return r },
			"record 3 (tenure.action at 0.000000007s) differs in Inc: 1, then 2"},
		{"kind", func(r []kernel.Record) []kernel.Record { r[0].Kind = "kernel.end"; return r },
			`record 1 (kernel.start at 0.000000000s) differs in Kind: "kernel.start", then "kernel.end"`},
		{"cause", func(r []kernel.Record) []kernel.Record { r[2].Cause = 1; return r },
			"record 3 (tenure.action at 0.000000007s) differs in Cause: 2, then 1"},
		{"text", func(r []kernel.Record) []kernel.Record { r[2].Text = "StopRun"; return r },
			`record 3 (tenure.action at 0.000000007s) differs in Text: "StartRun", then "StopRun"`},
		{"attrs", func(r []kernel.Record) []kernel.Record { r[1].Attrs[0].Value = "Acquired"; return r },
			"record 2 (tenure.input at 0.000000005s) differs in Attrs: [{type Tick}], then [{type Acquired}]"},
		{"first of two", func(r []kernel.Record) []kernel.Record { r[1].Text, r[2].Text = "x", "y"; return r },
			`record 2 (tenure.input at 0.000000005s) differs in Text: "Tick", then "x"`},
		{"shorter", func(r []kernel.Record) []kernel.Record { return r[:2] },
			"the first trace has 3 records, the second 2; the first extra is tenure.action at 0.000000007s"},
		{"longer", func(r []kernel.Record) []kernel.Record {
			return append(r, kernel.Record{Seq: 4, At: 8, Kind: "kernel.end"})
		},
			"the first trace has 3 records, the second 4; the first extra is kernel.end at 0.000000008s"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := firstDifference(base, tc.change(cloneRecords(base)))
			if err == nil || err.Error() != tc.want {
				t.Errorf("firstDifference = %v, want %q", err, tc.want)
			}
		})
	}
}
