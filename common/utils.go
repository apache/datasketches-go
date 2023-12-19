/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"math"
	"math/bits"
	"strconv"
)

// InvPow2 returns 2^(-e).
func InvPow2(e int) float64 {
	if (e | 1024 - e - 1) < 0 {
		panic("e cannot be negative or greater than 1023: " + strconv.Itoa(e))
	}
	return math.Float64frombits((1023 - uint64(e)) << 52)
}

// CeilPowerOf2 returns the smallest power of 2 greater than or equal to n.
func CeilPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	topIntPwrOf2 := 1 << 30
	if n >= topIntPwrOf2 {
		return topIntPwrOf2
	}
	return int(math.Pow(2, math.Ceil(math.Log2(float64(n)))))
}

func ExactLog2OfLong(powerOf2 uint64) int {
	if !isLongPowerOf2(powerOf2) {
		panic("Argument 'powerOf2' must be a positive power of 2.")
	}
	return bits.TrailingZeros64(powerOf2)
}

// isLongPowerOf2 returns true if the given number is a power of 2.
func isLongPowerOf2(powerOf2 uint64) bool {
	return powerOf2 > 0 && (powerOf2&(powerOf2-1)) == 0
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
