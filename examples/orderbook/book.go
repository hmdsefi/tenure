// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Order sides.
const (
	sideBuy  = "buy"
	sideSell = "sell"
)

// order is a limit order: Qty units of Market at Price or better. Price and Qty are integers.
type order struct {
	ID     string `json:"id"`
	Market string `json:"market"`
	Side   string `json:"side"`
	Price  int64  `json:"price"`
	Qty    int64  `json:"qty"`
}

// errInvalidOrder is wrapped by every error validate returns.
var errInvalidOrder = errors.New("orderbook: invalid order")

// validate reports whether o can be matched: it has an ID and a market, a side of buy or sell,
// and a positive price and quantity.
func (o order) validate() error {
	switch {
	case o.ID == "":
		return fmt.Errorf("%w: empty id", errInvalidOrder)
	case o.Market == "":
		return fmt.Errorf("%w: %s: empty market", errInvalidOrder, o.ID)
	case o.Side != sideBuy && o.Side != sideSell:
		return fmt.Errorf("%w: %s: side %q is neither buy nor sell", errInvalidOrder, o.ID, o.Side)
	case o.Price <= 0:
		return fmt.Errorf("%w: %s: price %d is not positive", errInvalidOrder, o.ID, o.Price)
	case o.Qty <= 0:
		return fmt.Errorf("%w: %s: qty %d is not positive", errInvalidOrder, o.ID, o.Qty)
	}
	return nil
}

// fill is one trade of an incoming order (the taker) against a resting one (the maker), at the
// maker's price.
type fill struct {
	Maker string `json:"maker"`
	Taker string `json:"taker"`
	Price int64  `json:"price"`
	Qty   int64  `json:"qty"`
}

// resting is the open part of an order that sits in a book.
type resting struct {
	id    string
	price int64
	qty   int64
}

// book is the two sides of one market. bids are ordered from the highest price down and asks
// from the lowest up; within a price the older order comes first (price-time priority). The
// zero value is an empty book.
type book struct {
	bids []resting
	asks []resting
}

// match takes o against the opposite side while prices cross, best price first and oldest first
// within a price, filling at the maker's price and never more than either side has left. What is
// left of o then rests in the book. It returns the fills in the order they happened, never nil.
// o must be valid.
func (b *book) match(o order) []fill {
	opposite, same := &b.asks, &b.bids
	crosses := func(maker int64) bool { return maker <= o.Price }
	worse := func(r resting) bool { return r.price < o.Price } // r ranks behind o on the same side
	if o.Side == sideSell {
		opposite, same = &b.bids, &b.asks
		crosses = func(maker int64) bool { return maker >= o.Price }
		worse = func(r resting) bool { return r.price > o.Price }
	}

	fills := []fill{}
	left := o.Qty
	used := 0 // makers at the head of opposite that are used up
	for used < len(*opposite) && left > 0 && crosses((*opposite)[used].price) {
		m := &(*opposite)[used]
		q := min(left, m.qty)
		fills = append(fills, fill{Maker: m.id, Taker: o.ID, Price: m.price, Qty: q})
		left -= q
		m.qty -= q
		if m.qty == 0 {
			used++
		}
	}
	*opposite = (*opposite)[used:]

	if left > 0 {
		// o goes behind every order with the same or a better price.
		i := slices.IndexFunc(*same, worse)
		if i < 0 {
			i = len(*same)
		}
		*same = slices.Insert(*same, i, resting{id: o.ID, price: o.Price, qty: left})
	}
	return fills
}

// clone returns a copy that shares nothing with b.
func (b *book) clone() *book {
	return &book{bids: slices.Clone(b.bids), asks: slices.Clone(b.asks)}
}

// String lists the book as "bids id@price*qty ... | asks id@price*qty ...", best first.
func (b *book) String() string {
	side := func(name string, rs []resting) string {
		var sb strings.Builder
		sb.WriteString(name)
		for _, r := range rs {
			sb.WriteString(" " + r.id + "@" + strconv.FormatInt(r.price, 10) + "*" + strconv.FormatInt(r.qty, 10))
		}
		return sb.String()
	}
	return side("bids", b.bids) + " | " + side("asks", b.asks)
}
