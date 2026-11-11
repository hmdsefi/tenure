// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"
)

// TestTwoWayLinks checks that a link rule slows both directions of its link: every fault.link
// and fault.link_reset of the store's links comes with its mirror right after it, the members'
// messages to the store and the store's to them both take longer than the default network
// allows, none takes longer than the slow link allows, the link faults end by R, and the other
// rules keep faultline's recovery start at R.
func TestTwoWayLinks(t *testing.T) {
	sc := Scenario{Delta: 150 * time.Millisecond, Faults: random(
		fault.Rule{Kind: fault.KindCrash, Every: 10 * time.Second, MaxFor: time.Second, Target: roleMembers},
		fault.Rule{
			Kind: fault.KindLink, Target: storeName, Every: 2 * time.Second, MaxFor: 5 * time.Second,
			Link: &simnet.Link{Latency: 100 * time.Millisecond, Jitter: 50 * time.Millisecond},
		},
	)}
	runWorld(t, fullTrace(60*time.Second), sc, func(fw *faultline.World, w *World) {
		fw.Final("test/links", func() error {
			t := fw.T()
			if fw.RecoveryStart() != w.Recovery() {
				t.Errorf("faultline's RecoveryStart %v, want R %v", fw.RecoveryStart(), w.Recovery())
			}
			if got := w.Scenario().Faults.Rules; len(got) != 2 || got[1].Kind != fault.KindLink {
				t.Errorf("Scenario().Faults.Rules = %+v, want both rules in order", got)
			}
			var links []kernel.Record
			slow := map[string]int{} // by direction, the deliveries slower than the default network allows
			for _, r := range fw.Sim.Records() {
				switch r.Kind {
				case "fault.link", "fault.link_reset":
					links = append(links, r)
				case "net.deliver":
					from, to := attrOf(r, "from"), attrOf(r, "to")
					if from != storeName && to != storeName {
						continue
					}
					d, err := time.ParseDuration(attrOf(r, "latency_ns") + "ns")
					if err != nil {
						t.Fatalf("latency_ns of %q: %v", r.Text, err)
					}
					if d > 150*time.Millisecond {
						t.Errorf("%s took %v, more than the slow link's 150 ms", r.Text, d)
					}
					if d > 5*time.Millisecond {
						slow[from+"->"+to]++
					}
				}
			}
			if len(links) == 0 || len(links)%2 != 0 {
				t.Fatalf("%d link records, want pairs", len(links))
			}
			for i := 0; i < len(links); i += 2 {
				a, b := links[i], links[i+1]
				if a.Kind != b.Kind || a.At != b.At || !mirrored(a.Text, b.Text) {
					t.Errorf("link records %q at %v and %q at %v are not a fault and its mirror", a.Text, a.At, b.Text, b.At)
				}
				if a.At > w.Recovery() {
					t.Errorf("%q at %v, after R %v", a.Text, a.At, w.Recovery())
				}
			}
			var toStore, fromStore int
			for dir, n := range slow {
				if strings.HasSuffix(dir, "->"+storeName) {
					toStore += n
				} else {
					fromStore += n
				}
			}
			if toStore == 0 || fromStore == 0 {
				t.Errorf("%d slow messages to the store, %d from it; want some of each", toStore, fromStore)
			}
			return nil
		})
	})
}

// mirrored reports whether two link record texts, as "link store -> m2: …" and "link m2 ->
// store: …", name the two directions of one link.
func mirrored(a, b string) bool {
	ends := func(s string) (string, string) {
		_, rest, _ := strings.Cut(s, " ")
		rest, _, _ = strings.Cut(rest, ":")
		from, to, _ := strings.Cut(rest, " -> ")
		return from, strings.TrimSpace(to)
	}
	af, at := ends(a)
	bf, bt := ends(b)
	return af != "" && af == bt && at == bf
}

// attrOf returns the value of the attribute key of r, or "".
func attrOf(r kernel.Record, key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}
