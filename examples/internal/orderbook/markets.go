// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package orderbook

import (
	"fmt"
	"math/rand/v2"
)

const (
	// MarketCount is the number of markets the demo clients trade: mkt-00 to mkt-31.
	MarketCount = 32

	minPrice, maxPrice = 90, 110
	maxQty             = 10
)

// MarketName returns the name of market i.
func MarketName(i int) string {
	return fmt.Sprintf("mkt-%02d", i)
}

// RandomOrder returns a valid order named id that rng picks: one of the MarketCount markets,
// a price from 90 to 110 and a quantity from 1 to 10, then a side. The same rng state gives the
// same order.
func RandomOrder(rng *rand.Rand, id string) Order {
	o := Order{
		ID:     id,
		Market: MarketName(rng.IntN(MarketCount)),
		Side:   SideBuy,
		Price:  minPrice + rng.Int64N(maxPrice-minPrice+1),
		Qty:    1 + rng.Int64N(maxQty),
	}
	if rng.IntN(2) == 0 {
		o.Side = SideSell
	}
	return o
}
