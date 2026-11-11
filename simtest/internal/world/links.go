// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package world

import (
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
)

// plannerLinks is the name of the planner of a scenario's link rules, and so its stream is
// fault/links.
const plannerLinks = "links"

// twoWayLinks plans the link rules of a scenario's Faults so that each fault slows both
// directions of its link. faultline's Random draws a link fault's node from the rule's target
// and its peer from the servers, the members, and changes the link from the node to the peer
// alone. A rule that targets the store would then slow only what the store sends; with
// twoWayLinks the members' requests to the store are as slow as its replies and pushes.
//
// It runs the rules as a Random of its own, whose draws come from fault/links, and injects the
// mirror of every link and link_reset event it makes right after it. Its Random ends its faults
// at until, the start of the quiet window, as the scenario's other rules do.
type twoWayLinks struct {
	plan  *fault.Random
	until kernel.Time
}

// Name returns the planner's name, "links".
func (p *twoWayLinks) Name() string { return plannerLinks }

// Start starts the link rules' Random on a copy of ctx whose Inject also injects the mirror of
// each link event, and whose Until is the start of the quiet window.
func (p *twoWayLinks) Start(ctx *fault.PlanContext) {
	c := *ctx
	c.Until = min(ctx.Until, p.until)
	inject := ctx.Inject
	c.Inject = func(e fault.Event) {
		inject(e)
		if e.Kind == fault.KindLink || e.Kind == fault.KindLinkReset {
			e.Node, e.Peer = e.Peer, e.Node
			inject(e)
		}
	}
	p.plan.Start(&c)
}

// splitLinks returns rules without their link rules, and the link rules, each in order.
func splitLinks(rules []fault.Rule) (others, links []fault.Rule) {
	for _, r := range rules {
		if r.Kind == fault.KindLink {
			links = append(links, r)
		} else {
			others = append(others, r)
		}
	}
	return others, links
}
