// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package tenure

import (
	"testing"
	"time"

	"github.com/hmdsefi/tenure/core"
)

// TestTableStepRows checks two rules of tableStep that a running node rarely meets. A Released
// older than the record the row shows changes neither its holder nor its term. A new machine, which
// reports every shard idle at reading 0, leaves an idle row's Since as it was, while a row that
// becomes idle takes the step's time.
func TestTableStepRows(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n := &Node{holder: "a/1", epoch: epoch}
	n.tab = newTableState(Config{Spaces: []Space{{Name: "s", Shards: 2}}}, n.holder, epoch)
	l := &loop{n: n}
	status := func(states ...core.ShardStatus) core.Status {
		return core.Status{Slot: -1, Spaces: []core.SpaceStatus{{Name: "s", Shards: states}}}
	}
	idleAt := func(d time.Duration) core.ShardStatus { return core.ShardStatus{State: core.ShardStateIdle, Since: d} }
	owned := core.ShardStatus{State: core.ShardStateOwned, Since: time.Second}

	l.tableStep(stepRecord{Input: core.HolderChanged{Space: "s", Shard: 0, Holder: "b/2", Term: 7}, After: status(idleAt(0), owned)})
	l.tableStep(stepRecord{Input: core.Released{Space: "s", Shard: 0, Term: 5}, After: status(idleAt(0), owned)})
	if r := n.tab.spaces[0].rows[0]; r.Holder != "b/2" || r.Term != 7 {
		t.Errorf("row 0 after an older Released = %+v, want holder b/2, term 7", r)
	}
	l.tableStep(stepRecord{Input: core.Released{Space: "s", Shard: 0, Term: 7}, After: status(idleAt(0), idleAt(3*time.Second))})
	if r := n.tab.spaces[0].rows[0]; r.Holder != "" || r.Term != 7 {
		t.Errorf("row 0 after Released of its term = %+v, want no holder, term 7", r)
	}
	if r := n.tab.spaces[0].rows[1]; r.State != "idle" || !r.Since.Equal(epoch.Add(3*time.Second)) {
		t.Errorf("row 1 once released = %+v, want idle since epoch+3s", r)
	}

	// A new machine: every shard idle at reading 0.
	l.tableStep(stepRecord{Input: core.Tick{}, After: status(idleAt(0), idleAt(0))})
	if r := n.tab.spaces[0].rows[1]; r.State != "idle" || !r.Since.Equal(epoch.Add(3*time.Second)) {
		t.Errorf("row 1 after a new machine's step = %+v, want idle since epoch+3s still", r)
	}
}
