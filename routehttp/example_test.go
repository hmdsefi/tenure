// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package routehttp_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/hmdsefi/tenure"
	"github.com/hmdsefi/tenure/routehttp"
)

// A handler that takes requests only for the keys its node owns.
func Example() {
	// local stands in for Node.Local: this member, n1, does not own the key, n2 does.
	local := func(space, key string) error {
		return &tenure.NotOwnerError{
			Space: space, Shard: len(key) % 8, Route: tenure.Member{ID: "n2", Addr: "10.0.0.2:8080"},
		}
	}
	const self = "n1"

	handler := func(w http.ResponseWriter, r *http.Request) {
		if err := local("orders", r.URL.Query().Get("key")); err != nil {
			var notOwner *tenure.NotOwnerError
			if errors.As(err, &notOwner) && notOwner.Route.ID != self {
				routehttp.Misdirected(w, notOwner.Route)
				return
			}
			routehttp.Unavailable(w, time.Second)
			return
		}
		// Serve the request.
	}

	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodPost, "/orders?key=k1", nil))
	fmt.Println(w.Code, w.Header().Get(routehttp.RouteHeader), w.Header().Get(routehttp.UnprocessedHeader))
	// Output: 421 10.0.0.2:8080 1
}
