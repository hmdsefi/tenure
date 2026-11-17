// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package viewhttp relays the views of a tenure group over HTTP, so that many routers can follow
// a group without each of them watching the membership store.
//
// Handler serves the views of a Source, such as a running *tenure.Node, at a path ending in
// "/view", and holds long polls until the view changes. Views change only when members, drains
// or slots change, so the relay sends almost nothing in between, and the store's load no longer
// grows with the number of routers.
//
// A node serves its merged views next to its other handlers:
//
//	mux := http.NewServeMux()
//	mux.Handle("/tenure/view", viewhttp.Handler(node))
//
// A router polls GET /tenure/view?wait=30s with the ETag of the last answer in If-None-Match,
// and gets the next view as soon as there is one, or 304 Not Modified when the wait ends.
package viewhttp
