// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package fixture

import "fmt"

var _ fmt.Stringer

func f(err error) {
	fmt.Println("x") // want DT004
	fmt.Fprint(nil)  // want DT004
	_ = fmt.Errorf("e: %w", err)
	_ = fmt.Sprintf("%d", 1)
	println("x") // want DT004
	print("x")   // want DT004
}
