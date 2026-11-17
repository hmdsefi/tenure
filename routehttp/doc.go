// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package routehttp is the HTTP side of routing requests to the members of a tenure group: the
// answers a member gives to a request for a key it does not own, and the rules by which a caller
// may send such a request again.
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
package routehttp
