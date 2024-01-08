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

package internal

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
)

const (
	InverseGolden = float64(0.6180339887498949025)
)

const (
	DEFAULT_UPDATE_SEED = uint64(9001)
)

const (
	DSketchTestGenerateGo = "DSKETCH_TEST_GENERATE_GO"
)

const (
	JavaPath = "../serialization_test_data/java_generated_files"
	CppPath  = "../serialization_test_data/cpp_generated_files"
	GoPath   = "../serialization_test_data/go_generated_files"
)

// GetShortLE gets a short value from a byte array in little endian format.
func GetShortLE(array []byte, offset int) int {
	return int(array[offset]&0xFF) | (int(array[offset+1]&0xFF) << 8)
}

// PutShortLE puts a short value into a byte array in little endian format.
func PutShortLE(array []byte, offset int, value int) {
	array[offset] = byte(value)
	array[offset+1] = byte(value >> 8)
}

// InvPow2 returns 2^(-e).
func InvPow2(e int) (float64, error) {
	if (e | 1024 - e - 1) < 0 {
		return 0, fmt.Errorf("e cannot be negative or greater than 1023: " + strconv.Itoa(e))
	}
	return math.Float64frombits((1023 - uint64(e)) << 52), nil
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

func ExactLog2(powerOf2 int) (int, error) {
	if !IsPowerOf2(powerOf2) {
		return 0, fmt.Errorf("argument 'powerOf2' must be a positive power of 2")
	}
	return bits.TrailingZeros64(uint64(powerOf2)), nil
}

// IsPowerOf2 returns true if the given number is a power of 2.
func IsPowerOf2(powerOf2 int) bool {
	return powerOf2 > 0 && (powerOf2&(powerOf2-1)) == 0
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
