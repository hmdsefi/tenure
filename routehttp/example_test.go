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
		err := local("orders", r.URL.Query().Get("key"))
		var notOwner *tenure.NotOwnerError
		switch {
		case err == nil:
			// Serve the request.
		case errors.As(err, &notOwner) && notOwner.Route.ID != "" && notOwner.Route.ID != self:
			routehttp.Misdirected(w, notOwner.Route)
		case errors.As(err, &notOwner), errors.Is(err, tenure.ErrClosed):
			routehttp.Unavailable(w, time.Second)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodPost, "/orders?key=k1", nil))
	fmt.Println(w.Code, w.Header().Get(routehttp.RouteHeader), w.Header().Get(routehttp.UnprocessedHeader))
	// Output: 421 10.0.0.2:8080 1
}
