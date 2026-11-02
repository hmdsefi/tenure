// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

func f() {}

func g(ch chan int) { // want DT002
	go f()   // want DT002
	ch <- 1  // want DT002
	_ = <-ch // want DT002
}

func h() {
	select {} // want DT002
}
