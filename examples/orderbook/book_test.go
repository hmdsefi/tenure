// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"slices"
	"testing"
)

// mirror returns the order with its side swapped.
func mirror(o order) order {
	if o.Side == sideBuy {
		o.Side = sideSell
	} else {
		o.Side = sideBuy
	}
	return o
}

// ord makes an order of the market "m".
func ord(id, side string, price, qty int64) order {
	return order{ID: id, Market: "m", Side: side, Price: price, Qty: qty}
}

// run puts the orders into a new book in turn and returns the book and each order's fills.
func run(t *testing.T, orders ...order) (*book, [][]fill) {
	t.Helper()
	b := new(book)
	var all [][]fill
	for _, o := range orders {
		if err := o.validate(); err != nil {
			t.Fatalf("order %s: %v", o.ID, err)
		}
		fills := b.match(o)
		if fills == nil {
			t.Fatalf("order %s: match returned nil fills", o.ID)
		}
		all = append(all, fills)
	}
	return b, all
}

func checkFills(t *testing.T, what string, got, want []fill) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s: fills = %v, want %v", what, got, want)
	}
}

func checkBook(t *testing.T, b *book, want string) {
	t.Helper()
	if got := b.String(); got != want {
		t.Errorf("book = %q, want %q", got, want)
	}
}

// TestATEXM07Book steps were written for this test from the text of issue #107: a crossing
// order fills at the maker's price and rests with what is left; at one price the older resting
// order fills first; an order that does not cross rests and fills nothing. Each case also runs
// with the sides swapped.
func TestATEXM07Book(t *testing.T) {
	t.Run("crossing order fills at the maker's price and the remainder rests", func(t *testing.T) {
		// A buy at 105 meets sells at 100 (qty 3) and 102 (qty 4): it takes both at their own
		// prices, then rests with the 3 it could not fill.
		b, fills := run(t,
			ord("s1", sideSell, 100, 3),
			ord("s2", sideSell, 102, 4),
			ord("s3", sideSell, 110, 5),
			ord("b1", sideBuy, 105, 10),
		)
		checkFills(t, "s1", fills[0], []fill{})
		checkFills(t, "b1", fills[3], []fill{
			{Maker: "s1", Taker: "b1", Price: 100, Qty: 3},
			{Maker: "s2", Taker: "b1", Price: 102, Qty: 4},
		})
		checkBook(t, b, "bids b1@105*3 | asks s3@110*5")

		// The mirror: a sell at 95 takes the buys at 100 and 98 at their prices.
		b, fills = run(t,
			ord("b1", sideBuy, 100, 3),
			ord("b2", sideBuy, 98, 4),
			ord("b3", sideBuy, 90, 5),
			ord("s1", sideSell, 95, 10),
		)
		checkFills(t, "mirror s1", fills[3], []fill{
			{Maker: "b1", Taker: "s1", Price: 100, Qty: 3},
			{Maker: "b2", Taker: "s1", Price: 98, Qty: 4},
		})
		checkBook(t, b, "bids b3@90*5 | asks s1@95*3")
	})

	t.Run("at equal price the older resting order fills first", func(t *testing.T) {
		for _, first := range []order{ord("r1", sideSell, 100, 2), ord("r1", sideBuy, 100, 2)} {
			// r2 is the same side as r1 and arrived later: r1 fills first, then r2 for the rest.
			second := first
			second.ID = "r2"
			taker := mirror(first)
			taker.ID = "t1"
			taker.Qty = 3
			b, fills := run(t, first, second, taker)
			checkFills(t, "t1 against "+first.Side+"s", fills[2], []fill{
				{Maker: "r1", Taker: "t1", Price: 100, Qty: 2},
				{Maker: "r2", Taker: "t1", Price: 100, Qty: 1},
			})
			var rest string
			if first.Side == sideSell {
				rest = "bids | asks r2@100*1"
			} else {
				rest = "bids r2@100*1 | asks"
			}
			checkBook(t, b, rest)
		}
	})

	t.Run("an order that does not cross rests and fills nothing", func(t *testing.T) {
		// A buy at 99 against a sell at 100 does not cross, and nor does a sell at 101 against
		// the buy.
		b, fills := run(t,
			ord("s1", sideSell, 100, 5),
			ord("b1", sideBuy, 99, 5),
			ord("s2", sideSell, 101, 2),
			ord("b2", sideBuy, 98, 1),
		)
		for i, f := range fills {
			checkFills(t, "order "+string(rune('0'+i)), f, []fill{})
		}
		checkBook(t, b, "bids b1@99*5 b2@98*1 | asks s1@100*5 s2@101*2")
	})
}

// TestBookPriority covers what the three acceptance cases leave open: price beats time, time
// breaks ties within a price, a resting order is consumed in pieces, and a book is not changed by
// the copy a match ran on.
func TestBookPriority(t *testing.T) {
	b, fills := run(t,
		ord("s1", sideSell, 101, 1),
		ord("s2", sideSell, 100, 1), // better price, arrived later
		ord("s3", sideSell, 100, 1),
		ord("b1", sideBuy, 101, 2),
	)
	checkFills(t, "b1", fills[3], []fill{
		{Maker: "s2", Taker: "b1", Price: 100, Qty: 1},
		{Maker: "s3", Taker: "b1", Price: 100, Qty: 1},
	})
	checkBook(t, b, "bids | asks s1@101*1")

	// A partial fill leaves the maker at the head of its price with the rest.
	b, _ = run(t, ord("s1", sideSell, 100, 5), ord("s2", sideSell, 100, 5), ord("b1", sideBuy, 100, 2))
	checkBook(t, b, "bids | asks s1@100*3 s2@100*5")

	// A buy at a better price than a resting buy goes ahead of it.
	b, _ = run(t, ord("b1", sideBuy, 99, 1), ord("b2", sideBuy, 100, 1), ord("b3", sideBuy, 99, 1))
	checkBook(t, b, "bids b2@100*1 b1@99*1 b3@99*1 | asks")

	// clone shares nothing with the original.
	c := b.clone()
	c.match(ord("s1", sideSell, 90, 3))
	checkBook(t, b, "bids b2@100*1 b1@99*1 b3@99*1 | asks")
	checkBook(t, c, "bids | asks")
}

func TestOrderValidate(t *testing.T) {
	good := ord("o1", sideBuy, 1, 1)
	if err := good.validate(); err != nil {
		t.Fatalf("valid order: %v", err)
	}
	for name, o := range map[string]order{
		"no id":      {Market: "m", Side: sideBuy, Price: 1, Qty: 1},
		"no market":  {ID: "o", Side: sideBuy, Price: 1, Qty: 1},
		"bad side":   {ID: "o", Market: "m", Side: "hold", Price: 1, Qty: 1},
		"zero price": {ID: "o", Market: "m", Side: sideBuy, Qty: 1},
		"neg price":  {ID: "o", Market: "m", Side: sideSell, Price: -1, Qty: 1},
		"zero qty":   {ID: "o", Market: "m", Side: sideSell, Price: 1},
	} {
		if err := o.validate(); err == nil {
			t.Errorf("%s: validate returned nil", name)
		}
	}
}
