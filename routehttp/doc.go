// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package routehttp is the HTTP side of routing requests to the members of a tenure group: the
// answers a member gives to a request for a key it does not own, the rules by which a caller
// may send such a request again, and Transport, a caller that sends each request to the member
// that owns its key.
//
// # The protocol
//
// A member checks every request with Node.Local before it acts on it. When the member does not
// own the key, it acts on nothing and answers one of two ways:
//
//   - 421 Misdirected Request, with a Tenure-Route header that holds the host:port of the member
//     that owns the key in this member's view. Misdirected writes it.
//   - 503 Service Unavailable, with a Retry-After header in whole seconds, when this member knows
//     no owner that can serve the key yet, for example while a shard moves. Unavailable writes it.
//
// Both answers carry Tenure-Unprocessed: 1. The header is a promise: the request had no effect,
// so the caller may send it again, whatever its method, and nothing happens twice. A server may
// send Tenure-Unprocessed only when that holds. A member that has begun to act on a request, or
// that cannot tell, answers without it, and a 503 without it is final for the caller: it is not
// sent again, because the first send may have had an effect. A 421 always means the request was
// not taken, so the caller may follow its Tenure-Route.
//
// A caller that follows the protocol:
//
//   - on a 421, sends the same request to the address in Tenure-Route, a bounded number of
//     times, so that two members whose views disagree cannot bounce a request forever;
//   - on a 503 with Tenure-Unprocessed, waits about Retry-After and sends it again;
//   - on any other answer, is done.
//
// It sets Tenure-Hops on each send: the number of earlier sends of the same request that moved
// it to another member, 0 on the first. A member that forwards requests itself can use the
// count to stop a loop.
//
// A server uses the helpers in its handlers:
//
//	sh, err := node.Local("orders", key)
//	if err != nil {
//		var notOwner *tenure.NotOwnerError
//		if errors.As(err, &notOwner) && notOwner.Route.ID != self {
//			routehttp.Misdirected(w, notOwner.Route)
//			return
//		}
//		routehttp.Unavailable(w, time.Second)
//		return
//	}
//
// # Sending requests to their owners
//
// A caller with a tenure.Router sends each request straight to the member that owns its key
// through a Transport, which follows the protocol above for it. Use it as the Transport of an
// http.Client:
//
//	client := &http.Client{Transport: &routehttp.Transport{
//		Router:   router,
//		Space:    "orders",
//		Key:      func(r *http.Request) (string, error) { return r.Header.Get("Order-Key"), nil },
//		Fallback: "orders.default.svc:8080",
//	}}
//
// The request's own host is never used: the Transport replaces it with the owner's address, so
// a URL such as "http://orders/orders" will do. Fallback, when set, takes requests that the
// Router has no member for and requests whose member does not accept connections; any member
// behind it answers them or redirects them.
//
// Key picks the key of a request. It can read a header, as above, or the URL:
//
//	Key: func(r *http.Request) (string, error) {
//		id, ok := strings.CutPrefix(r.URL.Path, "/orders/")
//		if !ok || id == "" {
//			return "", errors.New("no order ID in the path")
//		}
//		return id, nil
//	},
//
// Key must not read the body, which the Transport still has to send. A caller whose key is in
// the body reads the body first, keeps the bytes so that the Transport can send them again, and
// passes the key along in the request's context:
//
//	type keyCtx struct{}
//
//	body, _ := json.Marshal(order)
//	ctx = context.WithValue(ctx, keyCtx{}, order.ID)
//	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://orders/orders", bytes.NewReader(body))
//
//	Key: func(r *http.Request) (string, error) {
//		if key, ok := r.Context().Value(keyCtx{}).(string); ok {
//			return key, nil
//		}
//		return "", errors.New("request has no key")
//	},
//
// http.NewRequest sets GetBody for a bytes.Reader, so the Transport can send such a request
// again without buffering it.
//
// # A routing proxy
//
// tenure ships no proxy, but one that sends each request to its owner takes a few lines: an
// httputil.ReverseProxy whose Transport is a Transport. The proxy buffers bodies of up to
// MaxBufferedBody bytes so that it can send them again:
//
//	proxy := &httputil.ReverseProxy{
//		Rewrite: func(pr *httputil.ProxyRequest) {
//			pr.SetXForwarded()
//			pr.Out.URL.Scheme = "http"
//			pr.Out.URL.Host = "orders" // replaced by the owner's address
//		},
//		Transport: &routehttp.Transport{
//			Router:   router,
//			Space:    "orders",
//			Key:      func(r *http.Request) (string, error) { return r.Header.Get("Order-Key"), nil },
//			Fallback: "orders.default.svc:8080",
//		},
//	}
//	log.Fatal(http.ListenAndServe(":8080", proxy))
package routehttp
